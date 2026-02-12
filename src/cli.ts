import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { getProfile, listProfiles } from './profiles/registry';
import { run } from './engine/runner';
import { deleteCheckpoint } from './engine/checkpoint';
import type { RunnerConfig } from './engine/types';

import 'dotenv/config';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    profile: { type: 'string', short: 'p' },
    'batch-size': { type: 'string', short: 'b' },
    'max-id': { type: 'string', short: 'm' },
    's3-bucket': { type: 'string' },
    's3-prefix': { type: 'string' },
    'source-type': { type: 'string', default: 's3-dynamodb' },
    'target-type': { type: 'string', default: 'postgresql' },
    follow: { type: 'boolean', short: 'f', default: false },
    lines: { type: 'string', short: 'n', default: '50' },
  },
});

const command = positionals[0] ?? 'run';
const profileName = values.profile;
const BASE_DIR = process.env.MIGRATE_LOG_DIR ?? process.cwd();

const requireProfile = (): string => {
  if (!profileName) {
    console.error(`Error: --profile is required. Available profiles: ${listProfiles().join(', ')}`);
    process.exit(1);
  }
  return profileName;
};

const getPidFile = (name: string): string => path.join(BASE_DIR, `migrate-${name}.pid`);
const getLogFile = (name: string): string => path.join(BASE_DIR, `migrate-${name}.log`);

const isProcessRunning = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const parseOptionalInt = (raw: unknown): number | undefined => {
  if (typeof raw !== 'string') return undefined;
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) return undefined;
  return parsed;
};

const buildRunnerConfig = (name: string): RunnerConfig => {
  const sourceType = values['source-type'] as string;
  const targetType = values['target-type'] as string;

  // Source config
  if (sourceType === 's3-dynamodb') {
    const s3Bucket = (values['s3-bucket'] as string) ?? process.env.S3_BUCKET;
    const s3Prefix = (values['s3-prefix'] as string) ?? process.env.S3_PREFIX;

    if (!s3Bucket) {
      console.error('Error: S3_BUCKET env var or --s3-bucket is required');
      process.exit(1);
    }
    if (!s3Prefix) {
      console.error('Error: S3_PREFIX env var or --s3-prefix is required');
      process.exit(1);
    }

    return {
      sourceConfig: {
        type: 's3-dynamodb',
        bucket: s3Bucket,
        prefix: s3Prefix,
        region: process.env.AWS_REGION ?? 'eu-west-1',
      },
      targetConfig: buildTargetConfig(targetType),
      profileConfig: {
        batchSize: parseOptionalInt(values['batch-size']) ?? 500,
        maxId: parseOptionalInt(values['max-id']),
      },
      logDir: BASE_DIR,
    };
  }

  // Add other source types here
  console.error(`Error: Unknown source type "${sourceType}"`);
  process.exit(1);
};

const buildTargetConfig = (targetType: string): RunnerConfig['targetConfig'] => {
  if (targetType === 'postgresql') {
    const host = process.env.SERVER;
    const user = process.env.PG_USER;
    const password = process.env.PASSWORD;

    if (!host || !user || !password) {
      console.error('Error: SERVER, PG_USER, PASSWORD env vars are required');
      process.exit(1);
    }

    return {
      type: 'postgresql',
      host,
      port: Number(process.env.PORT ?? '5432'),
      user,
      password,
      database: process.env.DATABASE ?? 'infinityCMS',
      ssl: process.env.DB_SSL !== 'false',
    };
  }

  // Add other target types here
  console.error(`Error: Unknown target type "${targetType}"`);
  process.exit(1);
};

const runCommand = async (): Promise<void> => {
  const name = requireProfile();
  const profile = getProfile(name);
  const config = buildRunnerConfig(name);

  const abortController = new AbortController();
  const shouldStop = () => abortController.signal.aborted;

  const onSignal = () => {
    if (abortController.signal.aborted) return;
    abortController.abort();
    console.info('\nGraceful shutdown requested — waiting for current batch to finish...');
  };

  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  await run(profile, config, shouldStop);

  process.off('SIGINT', onSignal);
  process.off('SIGTERM', onSignal);
};

const buildCliArgs = (): string[] => {
  const args: string[] = [];
  if (profileName) args.push('--profile', profileName);
  if (typeof values['batch-size'] === 'string') args.push('--batch-size', values['batch-size']);
  if (typeof values['max-id'] === 'string') args.push('--max-id', values['max-id']);
  if (typeof values['s3-bucket'] === 'string') args.push('--s3-bucket', values['s3-bucket']);
  if (typeof values['s3-prefix'] === 'string') args.push('--s3-prefix', values['s3-prefix']);
  if (typeof values['source-type'] === 'string') args.push('--source-type', values['source-type']);
  if (typeof values['target-type'] === 'string') args.push('--target-type', values['target-type']);
  return args;
};

const startCommand = (): void => {
  const name = requireProfile();
  const pidFile = getPidFile(name);

  if (fs.existsSync(pidFile)) {
    const existingPid = parseInt(fs.readFileSync(pidFile, 'utf-8').trim(), 10);
    if (isProcessRunning(existingPid)) {
      console.error(`Daemon already running (PID ${existingPid}). Use "stop" first.`);
      process.exit(1);
    }
    fs.unlinkSync(pidFile);
  }

  const logFile = getLogFile(name);
  const logFd = fs.openSync(logFile, 'a');
  const cliPath = path.resolve(__dirname, 'cli.ts');

  const child = spawn('tsx', [cliPath, 'run', ...buildCliArgs()], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: process.env,
    cwd: BASE_DIR,
  });

  fs.writeFileSync(pidFile, String(child.pid));
  child.unref();
  fs.closeSync(logFd);

  console.info(`Daemon started (PID ${child.pid})`);
  console.info(`Logs:     ${logFile}`);
  console.info(`PID file: ${pidFile}`);
};

const stopCommand = (): void => {
  const name = requireProfile();
  const pidFile = getPidFile(name);

  if (!fs.existsSync(pidFile)) {
    console.info('No daemon running (no PID file).');
    return;
  }

  const pid = parseInt(fs.readFileSync(pidFile, 'utf-8').trim(), 10);

  if (!isProcessRunning(pid)) {
    console.info(`Daemon not running (stale PID ${pid}). Cleaning up.`);
    fs.unlinkSync(pidFile);
    return;
  }

  process.kill(pid, 'SIGTERM');
  console.info(`Sent SIGTERM to PID ${pid}.`);
  fs.unlinkSync(pidFile);
};

const statusCommand = (): void => {
  const name = requireProfile();
  const pidFile = getPidFile(name);

  if (!fs.existsSync(pidFile)) {
    console.info('No daemon running (no PID file).');
    return;
  }

  const pid = parseInt(fs.readFileSync(pidFile, 'utf-8').trim(), 10);

  if (isProcessRunning(pid)) {
    console.info(`Daemon is running (PID ${pid}).`);
  } else {
    console.info(`Daemon is not running (stale PID file for PID ${pid}).`);
    fs.unlinkSync(pidFile);
  }
};

const logsCommand = (): void => {
  const name = requireProfile();
  const logFile = getLogFile(name);

  if (!fs.existsSync(logFile)) {
    console.error(`No log file found at ${logFile}`);
    process.exit(1);
  }

  const lines = typeof values.lines === 'string' ? values.lines : '50';

  const tailArgs = ['-n', lines];
  if (values.follow === true) tailArgs.push('-f');
  tailArgs.push(logFile);

  const tail = spawn('tail', tailArgs, { stdio: 'inherit' });

  tail.on('close', (code) => {
    process.exit(code ?? 0);
  });
};

const resetCommand = (): void => {
  const name = requireProfile();
  if (deleteCheckpoint(BASE_DIR, name)) {
    console.info('Bookmark deleted. Next run will start from the beginning.');
  } else {
    console.info('No bookmark to delete.');
  }
};

const printUsage = (): void => {
  console.info(`
Usage: tsx src/cli.ts --profile <name> <command> [options]

Available profiles: ${listProfiles().join(', ')}

Commands:
  run      Run migration in foreground (default)
             Automatically resumes from bookmark if available
  start    Start migration as daemon
  stop     Stop running daemon
  status   Check daemon status
  logs     Show daemon logs
  reset    Delete bookmark and start fresh next run

Options:
  -p, --profile <name>       Migration profile (required)
  -b, --batch-size <n>       DB insert batch size (default: 500)
  -m, --max-id <n>           Upper limit filter (profile-specific)
  --s3-bucket <name>          S3 bucket (or env S3_BUCKET)
  --s3-prefix <prefix>       S3 key prefix (or env S3_PREFIX)
  --source-type <type>        Source dialect type (default: s3-dynamodb)
  --target-type <type>        Target dialect type (default: postgresql)
  -f, --follow               Follow log output (logs command)
  -n, --lines <n>            Number of log lines to show (default: 50)

Environment:
  S3_BUCKET          S3 bucket containing the DynamoDB export
  S3_PREFIX          S3 key prefix for the export files
  AWS_REGION         AWS region (default: eu-west-1)
  SERVER             PostgreSQL host
  PORT               PostgreSQL port (default: 5432)
  PG_USER            PostgreSQL user
  PASSWORD           PostgreSQL password
  DATABASE           PostgreSQL database (default: infinityCMS)
  DB_SSL             Use SSL (default: true)
  MIGRATE_LOG_DIR    Directory for PID, log, bookmark files (default: cwd)
`);
};

const main = async (): Promise<void> => {
  switch (command) {
    case 'run':
      await runCommand();
      break;
    case 'start':
      startCommand();
      break;
    case 'stop':
      stopCommand();
      break;
    case 'status':
      statusCommand();
      break;
    case 'logs':
      logsCommand();
      break;
    case 'reset':
      resetCommand();
      break;
    default:
      printUsage();
      process.exit(1);
  }
};

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
