# DynamoDB to PostgreSQL Migration Engine

A production-grade migration tool for importing DynamoDB S3 exports into PostgreSQL. Designed for large datasets that exceed Lambda timeout limits, with resume-from-failure capability and memory-efficient streaming.

Now supports pluggable source and target dialects — the pipeline is source/target agnostic.

## Why

AWS DynamoDB exports to S3 produce gzipped JSON files that can be hundreds of gigabytes. Importing these into PostgreSQL requires:

- **Streaming** to avoid memory exhaustion
- **Idempotency** to handle failures and re-runs safely
- **Long-running processes** that exceed Lambda's 15-minute timeout
- **Progress visibility** for multi-hour migrations

This tool addresses these with a profile-based architecture that can be extended to any DynamoDB collection.

## Features

| Feature | Implementation |
|---------|----------------|
| **Resume capability** | Per-file checkpointing with JSON bookmark |
| **Memory efficiency** | Streaming S3 reads, line-by-line parsing |
| **Data integrity** | `ON CONFLICT DO UPDATE` with smart merging |
| **Error resilience** | Binary split retry, individual item skip |
| **Observability** | Structured logging with progress counters + metrics hook |
| **Extensibility** | Dialect-based architecture for new sources/targets |
| **Production-ready** | Daemon mode, graceful shutdown, SSL support |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI (cli.ts)                           │
│   Commands: run, start, stop, status, logs, reset               │
│   Options: --source-type, --target-type                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Migration Engine                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Runner     │──│  Checkpoint  │──│ Batch Retry  │          │
│  │  (runner.ts)│  │(checkpoint.ts)│ │ (batch.ts)   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Dialects (pluggable)                  │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │SourceDialect│  │SourceDialect│  │SourceDialect│    │  │
│  │  │ (s3-dynamo)│  │ (postgresql)│  │   (custom)  │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │TargetDialect│  │TargetDialect│  │TargetDialect│    │  │
│  │  │ (postgresql)│  │ (opensearch)│  │   (custom)  │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Migration Profile                          │
│  ┌────────────┐ ┌────────────┐ ┌───────────┐ ┌──────────────┐ │
│  │ parseItem  │→│ groupRows  │→│ transform │→│    upsert    │ │
│  └────────────┘ └────────────┘ └───────────┘ └──────────────┘ │
│                                                                 │
│  Optional: filter, onComplete                                   │
└─────────────────────────────────────────────────────────────────┘
```

## How It Works

1. Creates source and target dialects from configuration
2. Lists files from source dialect
3. Loads checkpoint - skips already-processed files
4. For each file: stream → parse → group → transform → upsert
5. Saves checkpoint after each file (resume-safe)
6. On completion: runs profile's `onComplete` hook, deletes checkpoint

### Idempotency Layers

| Layer | Mechanism |
|-------|-----------|
| File level | Checkpoint file tracks processed keys |
| Record level | `ON CONFLICT DO UPDATE` with merge logic |
| Batch level | Binary split isolates failing records |

### Binary Split Retry

When a batch insert fails, the tool recursively splits it in half until the problematic record is isolated:

```
Batch of 500 fails
  → Split into 250 + 250
    → Left half succeeds
    → Right half fails
      → Split into 125 + 125
        → ...
          → Log and skip individual failing record
```

This ensures one bad record doesn't stop the entire migration.

## Quick Start

```bash
# Install
pnpm install

# Configure
cp .env.example .env
# Fill in S3_BUCKET, S3_PREFIX, SERVER, PG_USER, PASSWORD, etc.

# Run (foreground, resumable)
tsx src/cli.ts --profile pictures run

# Or use npm scripts (pictures profile)
pnpm migrate
```

## CLI Reference

```
tsx src/cli.ts --profile <name> <command> [options]
```

| Command  | Description |
|----------|-------------|
| `run`    | Run in foreground. Resumes from bookmark if available. |
| `start`  | Run as background daemon |
| `stop`   | Stop the daemon |
| `status` | Check daemon status |
| `logs`   | Tail daemon logs (`-f` to follow) |
| `reset`  | Delete bookmark, start fresh |

| Option             | Description                           | Default       |
|--------------------|---------------------------------------|--------------|
| `-p, --profile`    | Migration profile name (required)     |              |
| `-b, --batch-size` | DB insert batch size                 | 500          |
| `-m, --max-id`     | Upper ID limit filter                | none         |
| `--s3-bucket`      | S3 bucket (or env `S3_BUCKET`)      |              |
| `--s3-prefix`      | S3 key prefix (or env `S3_PREFIX`)  |              |
| `--source-type`    | Source dialect type                  | s3-dynamodb  |
| `--target-type`    | Target dialect type                  | postgresql   |

## Usage Examples

### Foreground Run (interactive, local or EC2)

```bash
# Default settings
tsx src/cli.ts --profile pictures run

# With tuning: smaller batches, ID limit, custom S3 location
tsx src/cli.ts --profile pictures run -b 100 -m 1000000 --s3-bucket my-bucket --s3-prefix exports/2026-02/
```

Ctrl+C triggers graceful shutdown: finishes current batch, saves checkpoint, exits.

### Resume After Interruption

```bash
# Just run again — bookmark is loaded automatically
tsx src/cli.ts --profile pictures run
```

Skips already-processed S3 files and continues from where it left off.

### Background Daemon (EC2 / long-running)

```bash
tsx src/cli.ts --profile pictures start          # start daemon
tsx src/cli.ts --profile pictures status         # check if running
tsx src/cli.ts --profile pictures logs -f       # follow logs in real-time
tsx src/cli.ts --profile pictures logs -n 200   # last 200 lines
tsx src/cli.ts --profile pictures stop           # graceful stop + checkpoint
```

Each profile gets its own files: `migrate-pictures.pid`, `migrate-pictures.log`, `migrate-pictures.bookmark.json`.

### Reset and Re-run from Scratch

```bash
tsx src/cli.ts --profile pictures reset    # delete bookmark
tsx src/cli.ts --profile pictures run      # processes all files again (idempotent)
```

### Dry-run on a Small Subset

```bash
tsx src/cli.ts --profile pictures run --max-id 1000 -b 50
```

### Typical EC2 Workflow (end-to-end)

```bash
# 1. Dry-run with a small subset
tsx src/cli.ts --profile pictures run --max-id 1000 -b 50

# 2. Full run as daemon
tsx src/cli.ts --profile pictures start -b 500

# 3. Monitor
tsx src/cli.ts --profile pictures logs -f

# 4. If something goes wrong — stop, fix, resume
tsx src/cli.ts --profile pictures stop
# ... fix issue ...
tsx src/cli.ts --profile pictures start -b 500  # resumes automatically

# 5. Verify completion (logs show COMPLETED summary)
tsx src/cli.ts --profile pictures logs -n 20
```

### npm Script Shortcuts (pictures profile)

```bash
pnpm migrate              # run foreground
pnpm migrate:start        # start daemon
pnpm migrate:stop         # stop daemon
pnpm migrate:status       # check status
pnpm migrate:logs         # follow logs
pnpm migrate:reset       # delete bookmark
```

## Adding a New Migration Profile

The engine is collection-agnostic. To migrate a different DynamoDB table, create a profile under `src/profiles/<name>/`.

### 1. `parse.ts`

```ts
export type MyRow = { /* your parsed row shape */ };

export const parseItem = (raw: Record<string, unknown>): MyRow | null => {
  // Validate and return typed row, or null to skip
};

export const groupRows = (rows: MyRow[]): MyRow[][] => {
  // Group rows by natural key (e.g. by ID)
};
```

### 2. `transform.ts`

```ts
export type MyInsert = { /* your target insert shape */ };

export const transform = (rows: MyRow[]): MyInsert => {
  // Convert grouped rows into one target record
};
```

### 3. `upsert.ts`

```ts
import type { TargetDialect } from '../../dialects/target';

export const upsert = async (target: TargetDialect, batch: MyInsert[]): Promise<number> => {
  // Get Knex client for custom SQL
  const knex = target.getClient();

  // INSERT ... ON CONFLICT ... DO UPDATE
  // Return count of rows written
};
```

### 4. `index.ts`

```ts
import type { TargetDialect } from '../../dialects/target';
import type { MigrationProfile } from '../../engine/types';

const myProfile: MigrationProfile<MyRow, MyInsert> = {
  name: 'my-collection',
  parseItem,
  groupRows,
  transform,
  upsert,
  // optional: filter, onComplete
};

export default myProfile;
```

### 5. Register in `src/profiles/registry.ts`

```ts
import myProfile from './my-collection';

const profiles = {
  pictures: picturesProfile,
  'my-collection': myProfile,  // add here
};
```

Then run: `tsx src/cli.ts --profile my-collection run`

## Adding a New Source Dialect

To add a new source (e.g., PostgreSQL → OpenSearch):

1. Create `src/dialects/source/my-source.ts`:

```ts
import type { SourceDialect, SourceConfig } from '../source';
import { registerSource } from '../source-registry';

class MySource implements SourceDialect {
  readonly name = 'my-source';

  constructor(config: SourceConfig) {
    // validate config type
  }

  async listFiles(): Promise<string[]> {
    // return list of file keys to process
  }

  async *streamRecords(fileKey: string): AsyncGenerator<Record<string, unknown>> {
    // yield records from the source
  }

  async close(): Promise<void> {
    // cleanup
  }
}

registerSource('my-source', (config) => new MySource(config));
```

2. Add config type to `SourceConfig` in `src/dialects/source.ts`:

```ts
export type SourceConfig =
  | { type: 's3-dynamodb'; bucket: string; prefix: string; region: string }
  | { type: 'my-source'; /* your config fields */ }
  | { type: 'custom'; [key: string]: unknown };
```

3. Use with CLI: `--source-type my-source`

## Adding a New Target Dialect

To add a new target (e.g., OpenSearch):

1. Create `src/dialects/target/my-target.ts`:

```ts
import type { TargetDialect, TargetConfig } from '../target';
import { registerTarget } from '../target-registry';

class MyTarget implements TargetDialect {
  readonly name = 'my-target';

  constructor(config: TargetConfig) {
    // validate config type
  }

  async upsert<T>(batch: T[]): Promise<number> {
    // write batch to target
    return batch.length;
  }

  async onComplete(): Promise<void> {
    // post-migration hook
  }

  async close(): Promise<void> {
    // cleanup
  }
}

registerTarget('my-target', (config) => new MyTarget(config));
```

2. Add config type to `TargetConfig` in `src/dialects/target.ts`

3. Use with CLI: `--target-type my-target`

## Metrics Hook

For programmatic monitoring, pass a `metrics` hook in the runner config:

```ts
import { run } from './engine/runner';

const config: RunnerConfig = {
  sourceConfig: { type: 's3-dynamodb', bucket: '...', prefix: '...', region: '...' },
  targetConfig: { type: 'postgresql', host: '...', port: 5432, user: '...', password: '...', database: '...', ssl: true },
  profileConfig: { batchSize: 500 },
  logDir: '/tmp',
  metrics: {
    onStart: ({ profileName, fileCount, batchSize, resuming }) => {
      console.log(`Starting ${profileName}: ${fileCount} files, batch=${batchSize}, resuming=${resuming}`);
    },
    onFileComplete: ({ fileName, scanned, inserted, errors }) => {
      console.log(`Completed ${fileName}: scanned=${scanned}, inserted=${inserted}, errors=${errors}`);
    },
    onProgress: ({ scanned, inserted, filesProcessed, filesTotal }) => {
      // e.g., emit to Prometheus, CloudWatch, etc.
    },
    onComplete: ({ profileName, completed, elapsedMs }) => {
      console.log(`Finished ${profileName}: ${completed ? 'SUCCESS' : 'INCOMPLETE'} in ${elapsedMs}ms`);
    },
  },
};

await run(profile, config, () => false);
```

## Running on EC2

```bash
# SSH into EC2 instance
# Clone the repo, install deps
git clone <repo-url>
cd migrate-dynamodb-to-pg
pnpm install

# Configure
cp .env.example .env
vim .env

# Run as daemon
tsx src/cli.ts --profile pictures start

# Monitor
tsx src/cli.ts --profile pictures logs -f

# Check status
tsx src/cli.ts --profile pictures status
```

**EC2 instance requirements:**
- IAM role with `s3:GetObject` and `s3:ListBucket` on the export bucket
- Network access to the PG instance (VPC / security groups)

## Environment Variables

| Setting    | Env var          | Default      |
|------------|------------------|--------------|
| S3 bucket  | `S3_BUCKET`      | (required)   |
| S3 prefix  | `S3_PREFIX`      | (required)   |
| AWS region | `AWS_REGION`     | eu-west-1    |
| PG host    | `SERVER`         | (required)   |
| PG port    | `PORT`           | 5432         |
| PG user    | `PG_USER`        | (required)   |
| PG pass    | `PASSWORD`       | (required)   |
| PG db      | `DATABASE`       | infinityCMS  |
| SSL        | `DB_SSL`         | true         |
| Log dir    | `MIGRATE_LOG_DIR`| cwd          |

CLI flags take precedence over environment variables.

## Operational Notes

### Memory Footprint

Memory usage is bounded by batch size and individual record size. The tool never loads entire files into memory - it streams line-by-line.

### Performance

Typical throughput: 5,000–20,000 records per second depending on:
- Target connection latency
- Record size and complexity
- Source proximity
- Batch size tuning

### Log Format

```
2024-01-15T10:30:00.000Z INFO [pictures] Starting migration
2024-01-15T10:30:01.234Z INFO [pictures] Processing file [3/42]: export-001.json.gz
2024-01-15T10:30:05.678Z INFO [pictures] Inserted 500 records (batch 1) in 234ms
2024-01-15T10:45:00.000Z INFO [pictures] Migration complete: 500,000 records in 15m
```

## Roadmap

- [ ] Parallel file processing for multi-file exports
- [ ] Progress bar with ETA estimation
- [ ] Configurable retry backoff strategies
- [ ] Built-in OpenSearch target dialect
- [ ] Built-in PostgreSQL source dialect
- [ ] Docker image with entrypoint

## Contributing

### Development Setup

```bash
pnpm install
pnpm test
pnpm lint
```

### Guidelines

- Keep the engine generic; domain-specific logic belongs in profiles
- Add tests for profile parsing and transformation logic
- Maintain backward compatibility with the `MigrationProfile` contract
- When adding dialects, provide clear config types

### Commit Convention

```
feat: add parallel file processing
fix: handle S3 throttling errors
docs: update profile creation guide
refactor: extract batch retry logic
feat(dialect): add OpenSearch target dialect
```

## License

MIT
