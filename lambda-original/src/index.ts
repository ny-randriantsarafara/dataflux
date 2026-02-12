import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import type { Context } from 'aws-lambda';
import Knex from 'knex';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { scanSegment, type DynamoRow } from './scan-dynamodb';
import { buildPictureFromDynamoRows } from './build-picture';
import { savePictures } from './save-picture';
import { log, formatDbError } from './logger';

import 'dotenv/config';

type SegmentKeys = Record<string, string | null>;
type MigrationContext = Pick<Context, 'getRemainingTimeInMillis'>;

export type MigrationEvent = {
  batchSize?: number;
  totalSegments?: number;
  maxPictureId?: number;
  segmentKeys?: SegmentKeys;
};

export type MigrationResult = {
  totalScanned: number;
  totalInserted: number;
  totalSkipped: number;
  totalErrors: number;
  completed: boolean;
  resumeEvent?: MigrationEvent;
};

type SegmentResult = {
  segment: number;
  scanned: number;
  inserted: number;
  skipped: number;
  errors: string[];
  lastKey?: Record<string, AttributeValue>;
};

const TIMEOUT_BUFFER_MS = 60_000;

const getKnexClient = () => {
  const useSsl = process.env.DB_SSL === 'true';
  const sslConfig = useSsl ? { rejectUnauthorized: false } : false;

  return Knex({
    client: 'pg',
    connection: {
      host: process.env.SERVER,
      port: Number(process.env.PORT),
      user: process.env.PG_USER,
      password: process.env.PASSWORD,
      database: process.env.DATABASE,
      application_name: 'infinity-cms-2-migrate-dynamodb-pictures',
      ssl: sslConfig,
    },
    pool: {
      min: 2,
      max: 10,
    },
    log: {
      warn(message: string) {
        log.knex.warn(message);
      },
      error(message: string) {
        log.knex.error(message);
      },
      deprecate(message: string) {
        log.knex.warn(message);
      },
      debug() {},
    },
  });
};

const getDynamoTableName = (): string => {
  const tableName = process.env.DYNAMODB_PICTURE_TABLE_NAME;
  if (!tableName) {
    throw new Error('DYNAMODB_PICTURE_TABLE_NAME environment variable is required');
  }
  return tableName;
};

export const groupRowsByPictureId = (rows: DynamoRow[]): DynamoRow[][] => {
  const rowsByPictureId = new Map<number, DynamoRow[]>();

  for (const row of rows) {
    const existingRows = rowsByPictureId.get(row.pictureId);
    if (existingRows) {
      rowsByPictureId.set(row.pictureId, [...existingRows, row]);
      continue;
    }

    rowsByPictureId.set(row.pictureId, [row]);
  }

  return Array.from(rowsByPictureId.values());
};

const processPage = async (
  rows: DynamoRow[],
  knexClient: Knex.Knex,
  batchSize: number,
  maxPictureId: number
): Promise<{ inserted: number; skipped: number }> => {
  const eligible = rows.filter((r) => r.pictureId < maxPictureId);
  const skipped = rows.length - eligible.length;

  if (eligible.length === 0) {
    return { inserted: 0, skipped };
  }

  const groupedRows = groupRowsByPictureId(eligible);
  const pictures = groupedRows.map((pictureRows) => buildPictureFromDynamoRows(pictureRows));
  let inserted = 0;

  for (let i = 0; i < pictures.length; i += batchSize) {
    const batch = pictures.slice(i, i + batchSize);
    inserted += await savePictures(knexClient, batch);
  }

  return { inserted, skipped };
};

const processSegment = async (
  dynamoClient: DynamoDB,
  tableName: string,
  knexClient: Knex.Knex,
  segment: number,
  totalSegments: number,
  batchSize: number,
  maxPictureId: number,
  startKey: Record<string, AttributeValue> | undefined,
  shouldStop: () => boolean
): Promise<SegmentResult> => {
  let scanned = 0;
  let inserted = 0;
  let skipped = 0;
  const errors: string[] = [];
  let lastKey: Record<string, AttributeValue> | undefined;

  const scanner = scanSegment(dynamoClient, tableName, segment, totalSegments, startKey);

  log.segment(segment, startKey ? 'resuming...' : 'starting...');

  for await (const page of scanner) {
    if (shouldStop()) {
      lastKey = page.lastKey;
      log.segment(segment, `stopping early (timeout approaching)`);
      break;
    }

    scanned += page.scannedCount;

    try {
      const result = await processPage(page.rows, knexClient, batchSize, maxPictureId);
      inserted += result.inserted;
      skipped += result.skipped;
    } catch (err) {
      const detail = formatDbError(err);
      log.segmentError(segment, 'INSERT_FAILED', `at ${scanned.toLocaleString('en-US')} items\n${detail}`);
      errors.push(detail.split('\n')[0].trim());
    }

    lastKey = page.lastKey;

    if (scanned % 100_000 < page.scannedCount) {
      log.segmentProgress(segment, { scanned, inserted, skipped, errors: errors.length });
    }
  }

  log.segmentProgress(segment, { scanned, inserted, skipped, errors: errors.length });
  log.segment(segment, 'done');
  return { segment, scanned, inserted, skipped, errors, lastKey };
};

export const handler = async (event: MigrationEvent = {}, context?: MigrationContext): Promise<MigrationResult> => {
  const { batchSize = 500, totalSegments = 10, maxPictureId = 20_000_000, segmentKeys = {} } = event;
  const startTime = Date.now();

  log.migration.start({
    batchSize,
    totalSegments,
    maxPictureId,
    resuming: Object.keys(segmentKeys).length > 0,
  });

  const dynamoClient = new DynamoDB({});
  const tableName = getDynamoTableName();
  const knexClient = getKnexClient();

  const shouldStop = () => {
    if (!context?.getRemainingTimeInMillis) {
      return false;
    }
    return context.getRemainingTimeInMillis() < TIMEOUT_BUFFER_MS;
  };

  try {
    const segments = Array.from({ length: totalSegments }, (_, i) => i).filter((seg) => {
      const key = segmentKeys[seg.toString()];
      return key !== null;
    });

    const segmentResults = await Promise.all(
      segments.map((seg) => {
        const startKeyStr = segmentKeys[seg.toString()];
        const startKey = startKeyStr ? JSON.parse(startKeyStr) : undefined;
        return processSegment(
          dynamoClient,
          tableName,
          knexClient,
          seg,
          totalSegments,
          batchSize,
          maxPictureId,
          startKey,
          shouldStop
        );
      })
    );

    const totalScanned = segmentResults.reduce((sum, r) => sum + r.scanned, 0);
    const totalInserted = segmentResults.reduce((sum, r) => sum + r.inserted, 0);
    const totalSkipped = segmentResults.reduce((sum, r) => sum + r.skipped, 0);
    const allErrors = segmentResults.flatMap((r) => r.errors);

    const incompleteSegments = segmentResults.filter((r) => r.lastKey);
    const completed = incompleteSegments.length === 0;

    if (completed) {
      await knexClient.raw("SELECT setval('picture_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM picture))");
      log.success('PostgreSQL sequence reset');
    }

    const resumeSegmentKeys: SegmentKeys = {};
    for (const seg of segmentResults) {
      resumeSegmentKeys[seg.segment.toString()] = seg.lastKey ? JSON.stringify(seg.lastKey) : null;
    }

    const resumeEvent: MigrationEvent | undefined = completed
      ? undefined
      : { batchSize, totalSegments, maxPictureId, segmentKeys: resumeSegmentKeys };

    const result: MigrationResult = {
      totalScanned,
      totalInserted,
      totalSkipped,
      totalErrors: allErrors.length,
      completed,
      resumeEvent,
    };

    log.migration.summary({
      scanned: totalScanned,
      inserted: totalInserted,
      skipped: totalSkipped,
      errors: allErrors.length,
      completed,
      elapsed: Date.now() - startTime,
    });

    if (resumeEvent) {
      log.info('Resume event saved — re-run to continue');
    }

    return result;
  } finally {
    await knexClient.destroy();
    log.info('Connection pool closed');
  }
};
