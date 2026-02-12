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
