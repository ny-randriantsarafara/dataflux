# migrate-dynamodb-to-pg

Collection-agnostic migration tool for DynamoDB exports (S3) → PostgreSQL.

## Quick start

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

## CLI

```
tsx src/cli.ts --profile <name> <command> [options]
```

| Command  | Description                         |
|----------|-------------------------------------|
| `run`    | Run in foreground (default). Resumes from bookmark if available. |
| `start`  | Run as background daemon            |
| `stop`   | Stop the daemon                     |
| `status` | Check daemon status                 |
| `logs`   | Tail daemon logs (`-f` to follow)   |
| `reset`  | Delete bookmark, start fresh        |

| Option            | Description                        | Default     |
|-------------------|------------------------------------|-------------|
| `-p, --profile`   | Migration profile name (required)  |             |
| `-b, --batch-size`| DB insert batch size               | 500         |
| `-m, --max-id`    | Upper ID limit filter              | none        |
| `--s3-bucket`     | S3 bucket (or env `S3_BUCKET`)     |             |
| `--s3-prefix`     | S3 key prefix (or env `S3_PREFIX`) |             |

## How it works

1. Lists `.json.gz` files from `s3://{bucket}/{prefix}/`
2. Loads checkpoint → skips already-processed files
3. For each file: stream → gunzip → parse → group → transform → upsert
4. Saves checkpoint after each file (resume-safe)
5. On completion: runs profile's `onComplete` hook, deletes checkpoint

Upserts are idempotent (`ON CONFLICT ... DO UPDATE`). Safe to rerun.

## Adding a new migration profile

Create `src/profiles/<name>/` with these files:

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
export type MyInsert = { /* your PG insert shape */ };

export const transform = (rows: MyRow[]): MyInsert => {
  // Convert grouped rows into one PG record
};
```

### 3. `upsert.ts`

```ts
export const upsert = async (knex: Knex, batch: MyInsert[]): Promise<number> => {
  // INSERT ... ON CONFLICT ... DO UPDATE
  // Return count of rows written
};
```

### 4. `index.ts`

```ts
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

## Usage examples

### Foreground run (interactive, local or EC2)

```bash
# Default settings
tsx src/cli.ts --profile pictures run

# With tuning: smaller batches, ID limit, custom S3 location
tsx src/cli.ts --profile pictures run -b 100 -m 1000000 --s3-bucket my-bucket --s3-prefix exports/2026-02/
```

Ctrl+C triggers graceful shutdown: finishes current batch, saves checkpoint, exits.

### Resume after interruption

```bash
# Just run again — bookmark is loaded automatically
tsx src/cli.ts --profile pictures run
```

Skips already-processed S3 files and continues from where it left off.

### Background daemon (EC2 / long-running)

```bash
tsx src/cli.ts --profile pictures start          # start daemon
tsx src/cli.ts --profile pictures status          # check if running
tsx src/cli.ts --profile pictures logs -f         # follow logs in real-time
tsx src/cli.ts --profile pictures logs -n 200     # last 200 lines
tsx src/cli.ts --profile pictures stop            # graceful stop + checkpoint
```

Each profile gets its own files: `migrate-pictures.pid`, `migrate-pictures.log`, `migrate-pictures.bookmark.json`.

### Reset and re-run from scratch

```bash
tsx src/cli.ts --profile pictures reset    # delete bookmark
tsx src/cli.ts --profile pictures run      # processes all files again (idempotent)
```

### Dry-run on a small subset

```bash
tsx src/cli.ts --profile pictures run --max-id 1000 -b 50
```

### Typical EC2 workflow (end-to-end)

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

### npm script shortcuts (pictures profile)

```bash
pnpm migrate              # run foreground
pnpm migrate:start        # start daemon
pnpm migrate:stop         # stop daemon
pnpm migrate:status       # check status
pnpm migrate:logs         # follow logs
pnpm migrate:reset        # delete bookmark
```

### Running a different profile

```bash
# Once you add a "videos" profile
tsx src/cli.ts --profile videos run
tsx src/cli.ts --profile videos start
tsx src/cli.ts --profile videos logs -f
```

### Environment variable override order

CLI flags take precedence over env vars, which take precedence over defaults.

| Setting    | CLI flag       | Env var          | Default      |
|------------|----------------|------------------|--------------|
| S3 bucket  | `--s3-bucket`  | `S3_BUCKET`      | (required)   |
| S3 prefix  | `--s3-prefix`  | `S3_PREFIX`      | (required)   |
| Batch size | `-b`           | —                | 500          |
| Max ID     | `-m`           | —                | none         |
| AWS region | —              | `AWS_REGION`     | eu-west-1    |
| PG host    | —              | `SERVER`         | (required)   |
| PG port    | —              | `PORT`           | 5432         |
| PG user    | —              | `PG_USER`        | (required)   |
| PG pass    | —              | `PASSWORD`       | (required)   |
| PG db      | —              | `DATABASE`       | infinityCMS  |
| SSL        | —              | `DB_SSL`         | true         |
| Log dir    | —              | `MIGRATE_LOG_DIR`| cwd          |

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

EC2 instance needs:
- IAM role with `s3:GetObject` and `s3:ListBucket` on the export bucket
- Network access to the PG instance (VPC / security groups)
