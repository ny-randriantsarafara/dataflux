import Knex from 'knex';
import { S3Client } from '@aws-sdk/client-s3';
import type { MigrationProfile, RunnerConfig, Checkpoint } from './types';
import { listExportFiles, readExportFile } from './read-s3-dump';
import { loadCheckpoint, saveCheckpoint, deleteCheckpoint } from './checkpoint';
import { log, formatDbError } from './logger';

export type RunResult = {
  totalScanned: number;
  totalInserted: number;
  totalSkipped: number;
  totalErrors: number;
  filesProcessed: number;
  filesTotal: number;
  completed: boolean;
};

const getKnexClient = (config: RunnerConfig) => {
  const sslConfig = config.dbConfig.ssl ? { rejectUnauthorized: false } : false;

  return Knex({
    client: 'pg',
    connection: {
      host: config.dbConfig.host,
      port: config.dbConfig.port,
      user: config.dbConfig.user,
      password: config.dbConfig.password,
      database: config.dbConfig.database,
      application_name: `migrate-dynamodb-to-pg`,
      ssl: sslConfig,
    },
    pool: { min: 2, max: 10 },
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

export const run = async <TRaw, TInsert>(
  profile: MigrationProfile<TRaw, TInsert>,
  config: RunnerConfig,
  shouldStop: () => boolean
): Promise<RunResult> => {
  const startTime = Date.now();

  const s3Client = new S3Client({ region: config.awsRegion });
  const knexClient = getKnexClient(config);

  try {
    // 1. List all export files
    log.info(`Listing S3 files: s3://${config.s3Bucket}/${config.s3Prefix}`);
    const allFiles = await listExportFiles(s3Client, config.s3Bucket, config.s3Prefix);

    if (allFiles.length === 0) {
      log.warn('No .json.gz files found under the given S3 prefix');
      return { totalScanned: 0, totalInserted: 0, totalSkipped: 0, totalErrors: 0, filesProcessed: 0, filesTotal: 0, completed: true };
    }

    // 2. Load checkpoint
    const checkpoint = loadCheckpoint(config.logDir, profile.name);
    const processedSet = new Set(checkpoint?.processedFiles ?? []);
    const filesToProcess = allFiles.filter((f) => !processedSet.has(f));

    log.migration.start(profile.name, {
      batchSize: config.profileConfig.batchSize,
      maxId: config.profileConfig.maxId,
      fileCount: filesToProcess.length,
      resuming: processedSet.size > 0,
    });

    // 3. Process files one at a time
    let totalScanned = 0;
    let totalInserted = 0;
    let totalSkipped = 0;
    let totalErrors = 0;
    let filesProcessed = processedSet.size;

    for (const fileKey of filesToProcess) {
      if (shouldStop()) {
        log.info('Graceful shutdown — saving checkpoint');
        break;
      }

      const shortName = fileKey.split('/').pop() ?? fileKey;
      log.file(shortName, 'processing...');

      let fileScanned = 0;
      let fileInserted = 0;
      let fileSkipped = 0;
      let fileErrors = 0;

      // Collect all rows from the file, then group + transform + upsert
      const rows: TRaw[] = [];

      try {
        for await (const rawItem of readExportFile(s3Client, config.s3Bucket, fileKey)) {
          const parsed = profile.parseItem(rawItem);
          if (!parsed) {
            fileSkipped++;
            continue;
          }

          if (profile.filter && !profile.filter(parsed, config.profileConfig)) {
            fileSkipped++;
            continue;
          }

          rows.push(parsed);
          fileScanned++;
        }
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        log.error(`Failed to read ${shortName}: ${detail}`);
        fileErrors++;
        totalErrors++;
        continue;
      }

      // Group and transform
      const groups = profile.groupRows(rows);
      const records = groups.map((group) => profile.transform(group));

      // Batch upsert
      const { batchSize } = config.profileConfig;
      for (let i = 0; i < records.length; i += batchSize) {
        if (shouldStop()) break;

        const batch = records.slice(i, i + batchSize);
        try {
          const count = await profile.upsert(knexClient, batch);
          fileInserted += count;
        } catch (err) {
          const detail = formatDbError(err);
          log.error(`Upsert failed in ${shortName}: ${detail}`);
          fileErrors++;
        }
      }

      // Update checkpoint after each file
      processedSet.add(fileKey);
      filesProcessed++;
      totalScanned += fileScanned;
      totalInserted += fileInserted;
      totalSkipped += fileSkipped;
      totalErrors += fileErrors;

      log.fileProgress(shortName, {
        scanned: fileScanned,
        inserted: fileInserted,
        skipped: fileSkipped,
        errors: fileErrors,
      });

      const currentCheckpoint: Checkpoint = {
        processedFiles: Array.from(processedSet),
        profileConfig: config.profileConfig,
      };
      saveCheckpoint(config.logDir, profile.name, currentCheckpoint);
    }

    // 4. Completion
    const completed = filesProcessed === allFiles.length && !shouldStop();

    if (completed && profile.onComplete) {
      await profile.onComplete(knexClient);
      log.success('Post-migration hook completed');
    }

    if (completed) {
      deleteCheckpoint(config.logDir, profile.name);
    }

    log.migration.summary({
      scanned: totalScanned,
      inserted: totalInserted,
      skipped: totalSkipped,
      errors: totalErrors,
      completed,
      elapsed: Date.now() - startTime,
      filesProcessed,
      filesTotal: allFiles.length,
    });

    return { totalScanned, totalInserted, totalSkipped, totalErrors, filesProcessed, filesTotal: allFiles.length, completed };
  } finally {
    await knexClient.destroy();
    log.info('Connection pool closed');
  }
};
