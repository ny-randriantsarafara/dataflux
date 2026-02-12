import type { TargetDialect } from '../dialects/target';

/**
 * Metrics hook for monitoring migration progress.
 * Called at various points during the migration run.
 */
export type MetricsHook = {
  /** Called when migration starts */
  onStart?: (params: {
    profileName: string;
    fileCount: number;
    batchSize: number;
    resuming: boolean;
  }) => void;

  /** Called after each file is processed */
  onFileComplete?: (params: {
    fileIndex: number;
    fileTotal: number;
    fileName: string;
    scanned: number;
    inserted: number;
    skipped: number;
    errors: number;
  }) => void;

  /** Called periodically with running totals */
  onProgress?: (params: {
    scanned: number;
    inserted: number;
    skipped: number;
    errors: number;
    filesProcessed: number;
    filesTotal: number;
  }) => void;

  /** Called when migration completes (success or failure) */
  onComplete?: (params: {
    profileName: string;
    scanned: number;
    inserted: number;
    skipped: number;
    errors: number;
    filesProcessed: number;
    filesTotal: number;
    completed: boolean;
    elapsedMs: number;
  }) => void;
};

/**
 * Target config passed to profile for creating target dialect
 */
export type TargetRunnerConfig = {
  type: string;
  [key: string]: unknown;
};

/**
 * Contract that each migration profile implements.
 * The engine is fully generic — it delegates all data-specific logic to the profile.
 *
 * TRaw  = the parsed row type (e.g. DynamoRow for pictures)
 * TInsert = the shape inserted into target (e.g. PictureInsert)
 */
export type MigrationProfile<TRaw = unknown, TInsert = unknown> = {
  /** Profile name — used in logs, checkpoint filenames, CLI */
  name: string;

  /** Parse a raw source item into a typed row, or null to skip */
  parseItem: (raw: Record<string, unknown>) => TRaw | null;

  /** Group parsed rows by their natural key (e.g. pictureId). Each group becomes one insert. */
  groupRows: (rows: TRaw[]) => TRaw[][];

  /** Transform a group of related rows into one insertable record */
  transform: (rows: TRaw[]) => TInsert;

  /** Create a target dialect with profile-specific configuration */
  createTarget: (config: TargetRunnerConfig) => TargetDialect;

  /** Upsert a batch of records into the target. Return count of rows written. */
  upsert: (target: TargetDialect, batch: TInsert[]) => Promise<number>;

  /** Optional: called once after all files are processed */
  onComplete?: (target: TargetDialect) => Promise<void>;

  /** Optional: filter predicate applied to each parsed row */
  filter?: (row: TRaw, config: ProfileConfig) => boolean;
};

export type ProfileConfig = {
  batchSize: number;
  maxId?: number;
  [key: string]: unknown;
};

export type RunnerConfig = {
  /** Source dialect configuration */
  sourceConfig: {
    type: string;
    [key: string]: unknown;
  };
  /** Target dialect configuration - passed to profile's createTarget */
  targetConfig: TargetRunnerConfig;
  profileConfig: ProfileConfig;
  logDir: string;
  /** Optional metrics hook for monitoring */
  metrics?: MetricsHook;
};

export type Checkpoint = {
  processedFiles: string[];
  profileConfig: ProfileConfig;
};
