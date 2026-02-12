import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { handler } from './index';
import type { MigrationEvent } from './index';

const BASE_DIR = process.env.MIGRATE_LOG_DIR ?? process.cwd();
const PID_FILE = path.join(BASE_DIR, 'migrate-dynamodb-pictures.pid');
const LOG_FILE = path.join(BASE_DIR, 'migrate-dynamodb-pictures.log');
const BOOKMARK_FILE = path.join(BASE_DIR, 'migrate-dynamodb-pictures.bookmark.json');

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    'batch-size': { type: 'string', short: 'b' },
    'total-segments': { type: 'string', short: 's' },
    'max-picture-id': { type: 'string', short: 'm' },
    follow: { type: 'boolean', short: 'f', default: false },
    lines: { type: 'string', short: 'n', default: '50' },
  },
});

const command = positionals[0] ?? 'run';

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

const buildEventFromArgs = (): MigrationEvent => {
  const batchSize = parseOptionalInt(values['batch-size']);
  const totalSegments = parseOptionalInt(values['total-segments']);
  const maxPictureId = parseOptionalInt(values['max-picture-id']);

  return {
    batchSize,
    totalSegments,
    maxPictureId,
  };
};

const buildCliArgs = (): string[] => {
  const args: string[] = [];
  if (typeof values['batch-size'] === 'string') args.push('--batch-size', values['batch-size']);
  if (typeof values['total-segments'] === 'string') args.push('--total-segments', values['total-segments']);
  if (typeof values['max-picture-id'] === 'string') args.push('--max-picture-id', values['max-picture-id']);
  return args;
};

const isSegmentKeys = (value: unknown): value is Record<string, string | null> => {
  if (value === null || typeof value !== 'object') return false;
  if (Array.isArray(value)) return false;

  const entries = Object.entries(value);
  for (const [, v] of entries) {
    if (typeof v === 'string') continue;
    if (v === null) continue;
    return false;
  }

  return true;
};

const parseMigrationEvent = (raw: unknown): MigrationEvent | undefined => {
  if (raw === null || typeof raw !== 'object') return undefined;
  if (Array.isArray(raw)) return undefined;

  const record = raw as Record<string, unknown>;
  const batchSize = typeof record.batchSize === 'number' ? record.batchSize : undefined;
  const totalSegments = typeof record.totalSegments === 'number' ? record.totalSegments : undefined;
  const maxPictureId = typeof record.maxPictureId === 'number' ? record.maxPictureId : undefined;
  const segmentKeys = isSegmentKeys(record.segmentKeys) ? record.segmentKeys : undefined;

  return {
    batchSize,
    totalSegments,
    maxPictureId,
    segmentKeys,
  };
};

const loadBookmark = (): MigrationEvent | undefined => {
  if (!fs.existsSync(BOOKMARK_FILE)) return undefined;
  const raw = fs.readFileSync(BOOKMARK_FILE, 'utf-8');
  const parsed: unknown = JSON.parse(raw);
  return parseMigrationEvent(parsed);
};

const saveBookmark = (event: MigrationEvent): void => {
  fs.writeFileSync(BOOKMARK_FILE, JSON.stringify(event, null, 2));
};

const deleteBookmark = (): boolean => {
  if (!fs.existsSync(BOOKMARK_FILE)) return false;
  fs.unlinkSync(BOOKMARK_FILE);
  return true;
};

const run = async (): Promise<void> => {
  const argsEvent = buildEventFromArgs();
  const bookmark = loadBookmark();

  const event: MigrationEvent = (() => {
    if (!bookmark) return argsEvent;
    return { ...bookmark, ...argsEvent, segmentKeys: bookmark.segmentKeys };
  })();

  if (bookmark) {
    console.info('Resuming from bookmark...');
  }

  const abortController = new AbortController();
  const fakeContext = {
    getRemainingTimeInMillis: () => (abortController.signal.aborted ? 0 : Infinity),
  };

  const onSignal = () => {
    if (abortController.signal.aborted) return;
    abortController.abort();
    console.info('\nGraceful shutdown requested — waiting for current pages to finish...');
  };

  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  const result = await handler(event, fakeContext);

  process.off('SIGINT', onSignal);
  process.off('SIGTERM', onSignal);

  if (result.completed) {
    deleteBookmark();
  } else if (result.resumeEvent) {
    saveBookmark(result.resumeEvent);
    console.info(`Bookmark saved to ${BOOKMARK_FILE}`);
  }
};

const start = (): void => {
  if (fs.existsSync(PID_FILE)) {
    const existingPid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10);
    if (isProcessRunning(existingPid)) {
      console.error(`Daemon already running (PID ${existingPid}). Use "stop" first.`);
      process.exit(1);
    }
    fs.unlinkSync(PID_FILE);
  }

  const logFd = fs.openSync(LOG_FILE, 'a');
  const cliPath = path.resolve(__dirname, 'cli.ts');

  const child = spawn('tsx', [cliPath, 'run', ...buildCliArgs()], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: process.env,
    cwd: BASE_DIR,
  });

  fs.writeFileSync(PID_FILE, String(child.pid));
  child.unref();
  fs.closeSync(logFd);

  console.info(`Daemon started (PID ${child.pid})`);
  console.info(`Logs:     ${LOG_FILE}`);
  console.info(`PID file: ${PID_FILE}`);
};

const stop = (): void => {
  if (!fs.existsSync(PID_FILE)) {
    console.info('No daemon running (no PID file).');
    return;
  }

  const pid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10);

  if (!isProcessRunning(pid)) {
    console.info(`Daemon not running (stale PID ${pid}). Cleaning up.`);
    fs.unlinkSync(PID_FILE);
    return;
  }

  process.kill(pid, 'SIGTERM');
  console.info(`Sent SIGTERM to PID ${pid}.`);
  fs.unlinkSync(PID_FILE);
};

const status = (): void => {
  if (!fs.existsSync(PID_FILE)) {
    console.info('No daemon running (no PID file).');
    return;
  }

  const pid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10);

  if (isProcessRunning(pid)) {
    console.info(`Daemon is running (PID ${pid}).`);
  } else {
    console.info(`Daemon is not running (stale PID file for PID ${pid}).`);
    fs.unlinkSync(PID_FILE);
  }
};

const logs = (): void => {
  if (!fs.existsSync(LOG_FILE)) {
    console.error(`No log file found at ${LOG_FILE}`);
    process.exit(1);
  }

  const lines = typeof values.lines === 'string' ? values.lines : '50';

  const tailArgs = ['-n', lines];
  if (values.follow === true) tailArgs.push('-f');
  tailArgs.push(LOG_FILE);

  const tail = spawn('tail', tailArgs, { stdio: 'inherit' });

  tail.on('close', (code) => {
    process.exit(code ?? 0);
  });
};

const reset = (): void => {
  if (deleteBookmark()) {
    console.info('Bookmark deleted. Next run will start from the beginning.');
  } else {
    console.info('No bookmark to delete.');
  }
};

const printUsage = (): void => {
  console.info(`
Usage: tsx cli.ts <command> [options]

Commands:
  run      Run migration in foreground (default)
             Automatically resumes from bookmark if available
  start    Start migration as daemon
  stop     Stop running daemon
  status   Check daemon status
  logs     Show daemon logs
  reset    Delete bookmark and start fresh next run

Options:
  -b, --batch-size <n>       DB insert batch size (default: 500)
  -s, --total-segments <n>   DynamoDB scan parallelism (default: 10)
                             Lower = fewer deadlocks (try 2-4 locally)
  -m, --max-picture-id <n>   Upper limit on picture ID (default: 20000000)
  -f, --follow               Follow log output (logs command)
  -n, --lines <n>            Number of log lines to show (default: 50)

Environment:
  MIGRATE_LOG_DIR    Directory for PID and log files (default: cwd)
`);
};

const main = async (): Promise<void> => {
  switch (command) {
    case 'run':
      await run();
      break;
    case 'start':
      start();
      break;
    case 'stop':
      stop();
      break;
    case 'status':
      status();
      break;
    case 'logs':
      logs();
      break;
    case 'reset':
      reset();
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
