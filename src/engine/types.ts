import type { Knex } from 'knex';

/**
 * Contract that each migration profile implements.
 * The engine is fully generic — it delegates all data-specific logic to the profile.
 *
 * TRaw  = the parsed row type (e.g. DynamoRow for pictures)
 * TInsert = the shape inserted into PG (e.g. PictureInsert)
 */
export type MigrationProfile<TRaw = unknown, TInsert = unknown> = {
  /** Profile name — used in logs, checkpoint filenames, CLI */
  name: string;

  /** Parse a raw DynamoDB JSON item into a typed row, or null to skip */
  parseItem: (raw: Record<string, unknown>) => TRaw | null;

  /** Group parsed rows by their natural key (e.g. pictureId). Each group becomes one insert. */
  groupRows: (rows: TRaw[]) => TRaw[][];

  /** Transform a group of related rows into one insertable record */
  transform: (rows: TRaw[]) => TInsert;

  /** Upsert a batch of records into PG. Return count of rows written. */
  upsert: (knex: Knex, batch: TInsert[]) => Promise<number>;

  /** Optional: called once after all files are processed (e.g. reset PG sequences) */
  onComplete?: (knex: Knex) => Promise<void>;

  /** Optional: filter predicate applied to each parsed row */
  filter?: (row: TRaw, config: ProfileConfig) => boolean;
};

export type ProfileConfig = {
  batchSize: number;
  maxId?: number;
  [key: string]: unknown;
};

export type RunnerConfig = {
  s3Bucket: string;
  s3Prefix: string;
  awsRegion: string;
  profileConfig: ProfileConfig;
  dbConfig: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    ssl: boolean;
  };
  logDir: string;
};

export type Checkpoint = {
  processedFiles: string[];
  profileConfig: ProfileConfig;
};
