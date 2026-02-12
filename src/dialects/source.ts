/**
 * Source dialect interface.
 * Implement this to read data from any source (S3, PostgreSQL, OpenSearch, etc.)
 */
export interface SourceDialect {
  /** Unique name for logging and diagnostics */
  readonly name: string;

  /** List all data files to process */
  listFiles(): Promise<string[]>;

  /** Stream records from a single file/key */
  streamRecords(fileKey: string): AsyncGenerator<Record<string, unknown>>;

  /** Optional: cleanup resources when done */
  close?(): Promise<void>;
}

/**
 * Configuration for source dialects
 */
export type SourceConfig =
  | { type: 's3-dynamodb'; bucket: string; prefix: string; region: string }
  | { type: 'postgresql'; connectionString: string; query: string }
  | { type: 'custom'; [key: string]: unknown };
