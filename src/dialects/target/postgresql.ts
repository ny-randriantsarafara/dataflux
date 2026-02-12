import Knex, { type Knex as KnexType } from 'knex';
import type { TargetDialect, TargetConfig } from '../target';
import { registerTarget } from '../target-registry';

/**
 * PostgreSQL target dialect.
 * Writes to PostgreSQL using Knex with batch upserts.
 */
class PostgreSQLTarget implements TargetDialect {
  readonly name = 'postgresql';

  readonly client: KnexType;
  private readonly tableName: string;
  private readonly columns: readonly string[];

  constructor(
    config: TargetConfig,
    tableName: string,
    columns: readonly string[]
  ) {
    if (config.type !== 'postgresql') {
      throw new Error('Invalid config type for PostgreSQL target');
    }

    const sslConfig = config.ssl ? { rejectUnauthorized: false } : false;

    this.client = Knex({
      client: 'pg',
      connection: {
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        database: config.database,
        application_name: 'migrate-dynamodb-to-pg',
        ssl: sslConfig,
      },
      pool: { min: 2, max: 10 },
      log: {
        warn(message: string) {
          console.warn('[knex warn]', message);
        },
        error(message: string) {
          console.error('[knex error]', message);
        },
        deprecate(message: string) {
          console.warn('[knex deprecate]', message);
        },
        debug() {},
      },
    });

    this.tableName = tableName;
    this.columns = columns;
  }

  getClient(): KnexType {
    return this.client;
  }

  async upsert<T>(batch: T[]): Promise<number> {
    if (batch.length === 0) {
      return 0;
    }

    // Build the INSERT ... ON CONFLICT query
    const columnList = this.columns.map((c) => `"${c}"`).join(', ');
    const rowPlaceholders = this.columns.map(() => '?').join(', ');
    const valuesPlaceholder = Array.from({ length: batch.length }, () => `(${rowPlaceholders})`).join(', ');

    // Flatten all values from the batch
    const bindings: unknown[] = [];
    for (const record of batch) {
      const recordAny = record as Record<string, unknown>;
      for (const col of this.columns) {
        const value = recordAny[col];
        // Handle JSON serialization for JSON columns
        if (typeof value === 'object' && value !== null) {
          bindings.push(JSON.stringify(value));
        } else {
          bindings.push(value);
        }
      }
    }

    // For now, use a simple upsert - profiles can extend this with custom onConflict logic
    const query = `
      INSERT INTO ${this.tableName} (${columnList})
      VALUES ${valuesPlaceholder}
      ON CONFLICT DO NOTHING
    `;

    const result = await this.client.raw(query, bindings);
    return result.rowCount ?? batch.length;
  }

  async onComplete(): Promise<void> {
    // Profiles can override this for custom post-migration logic
  }

  async close(): Promise<void> {
    await this.client.destroy();
  }
}

/**
 * Create a PostgreSQL target dialect with custom table/column configuration.
 * This is used by profiles that need specific table mappings.
 */
export const createPostgreSQLTarget = (
  config: TargetConfig,
  tableName: string,
  columns: readonly string[]
): TargetDialect => {
  return new PostgreSQLTarget(config, tableName, columns);
};

// Register with default table (can be overridden via createPostgreSQLTarget)
registerTarget('postgresql', (config) => new PostgreSQLTarget(config, 'unknown_table', []));
