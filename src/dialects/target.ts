import type { Knex } from 'knex';

/**
 * Target dialect interface.
 * Implement this to write data to any target (PostgreSQL, OpenSearch, etc.)
 */
export interface TargetDialect {
  /** Unique name for logging and diagnostics */
  readonly name: string;

  /** Get the underlying query builder (for custom SQL in profiles) */
  getClient(): Knex;

  /** Upsert a batch of records using default conflict handling */
  upsert<T>(batch: T[]): Promise<number>;

  /** Optional: called once after all files are processed (e.g., reset sequences, refresh indexes) */
  onComplete?(): Promise<void>;

  /** Optional: cleanup resources when done */
  close?(): Promise<void>;
}

/**
 * Configuration for target dialects
 */
export type TargetConfig =
  | { type: 'postgresql'; host: string; port: number; user: string; password: string; database: string; ssl: boolean }
  | { type: 'opensearch'; endpoint: string; index: string; apiKey?: string }
  | { type: 'custom'; [key: string]: unknown };
