type AnsiColor = {
  reset: string;
  dim: string;
  bold: string;
  red: string;
  green: string;
  yellow: string;
  blue: string;
  cyan: string;
  magenta: string;
};

const COLORS: Readonly<AnsiColor> = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
};

const SEGMENT_COLORS = [
  COLORS.blue,
  COLORS.cyan,
  COLORS.magenta,
  COLORS.green,
  COLORS.yellow,
  COLORS.blue,
  COLORS.cyan,
  COLORS.magenta,
  COLORS.green,
  COLORS.yellow,
];

const pad = (n: number, len = 2): string => String(n).padStart(len, '0');

const timestamp = (): string => {
  const d = new Date();
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
};

const formatNumber = (n: number): string => n.toLocaleString('en-US');

const segmentColor = (segment: number): string => SEGMENT_COLORS[segment % SEGMENT_COLORS.length];

export const log = {
  info: (message: string) => {
    console.info(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${message}`);
  },

  success: (message: string) => {
    console.info(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${COLORS.green}${message}${COLORS.reset}`);
  },

  warn: (message: string) => {
    console.warn(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${COLORS.yellow}WARN${COLORS.reset}  ${message}`);
  },

  error: (message: string) => {
    console.error(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${COLORS.red}ERR${COLORS.reset}   ${message}`);
  },

  segment: (segment: number, message: string) => {
    const color = segmentColor(segment);
    const tag = `${color}seg-${segment}${COLORS.reset}`;
    console.info(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${tag}  ${message}`);
  },

  segmentError: (segment: number, errorType: string, detail: string) => {
    const color = segmentColor(segment);
    const tag = `${color}seg-${segment}${COLORS.reset}`;
    console.error(
      `${COLORS.dim}${timestamp()}${COLORS.reset}  ${tag}  ${COLORS.red}${errorType}${COLORS.reset}  ${detail}`
    );
  },

  segmentProgress: (segment: number, stats: { scanned: number; inserted: number; skipped: number; errors: number }) => {
    const color = segmentColor(segment);
    const tag = `${color}seg-${segment}${COLORS.reset}`;
    const parts: string[] = [
      `scanned ${COLORS.bold}${formatNumber(stats.scanned)}${COLORS.reset}`,
      `inserted ${COLORS.green}${formatNumber(stats.inserted)}${COLORS.reset}`,
    ];

    if (stats.skipped > 0) {
      parts.push(`skipped ${COLORS.yellow}${formatNumber(stats.skipped)}${COLORS.reset}`);
    }

    if (stats.errors > 0) {
      parts.push(`errors ${COLORS.red}${formatNumber(stats.errors)}${COLORS.reset}`);
    }
    console.info(`${COLORS.dim}${timestamp()}${COLORS.reset}  ${tag}  ${parts.join('  ')}`);
  },

  migration: {
    start: (config: { batchSize: number; totalSegments: number; maxPictureId: number; resuming: boolean }) => {
      const resuming = config.resuming ? `${COLORS.yellow}yes${COLORS.reset}` : 'no';
      const lines = [
        '',
        `${COLORS.bold}Migration started${COLORS.reset}`,
        `  batch size:     ${formatNumber(config.batchSize)}`,
        `  segments:       ${config.totalSegments}`,
        `  max picture id: ${formatNumber(config.maxPictureId)}`,
        `  resuming:       ${resuming}`,
        '',
      ];
      console.info(lines.join('\n'));
    },

    summary: (stats: {
      scanned: number;
      inserted: number;
      skipped: number;
      errors: number;
      completed: boolean;
      elapsed: number;
    }) => {
      const elapsed = (() => {
        if (stats.elapsed < 60_000) {
          return `${(stats.elapsed / 1000).toFixed(1)}s`;
        }

        const minutes = Math.floor(stats.elapsed / 60_000);
        const seconds = Math.round((stats.elapsed % 60_000) / 1000);
        return `${minutes}m ${seconds}s`;
      })();

      const status = (() => {
        if (stats.completed) {
          return `${COLORS.green}${COLORS.bold}COMPLETED${COLORS.reset}`;
        }

        return `${COLORS.yellow}${COLORS.bold}INCOMPLETE${COLORS.reset}`;
      })();

      const errors = (() => {
        if (stats.errors > 0) {
          return `${COLORS.red}${formatNumber(stats.errors)}${COLORS.reset}`;
        }
        return `0${COLORS.reset}`;
      })();

      const lines = [
        '',
        `${COLORS.dim}${'─'.repeat(50)}${COLORS.reset}`,
        `  ${status}  ${COLORS.dim}(${elapsed})${COLORS.reset}`,
        '',
        `  scanned:  ${COLORS.bold}${formatNumber(stats.scanned)}${COLORS.reset}`,
        `  inserted: ${COLORS.green}${formatNumber(stats.inserted)}${COLORS.reset}`,
        `  skipped:  ${COLORS.yellow}${formatNumber(stats.skipped)}${COLORS.reset}`,
        `  errors:   ${errors}`,
        `${COLORS.dim}${'─'.repeat(50)}${COLORS.reset}`,
        '',
      ];
      console.info(lines.join('\n'));
    },
  },

  db: (action: string, count: number, elapsed: number) => {
    const tag = `${COLORS.dim}db${COLORS.reset}`;
    const time = (() => {
      if (elapsed > 1000) {
        return `${COLORS.yellow}${elapsed}ms${COLORS.reset}`;
      }
      return `${COLORS.dim}${elapsed}ms${COLORS.reset}`;
    })();
    console.info(
      `${COLORS.dim}${timestamp()}${COLORS.reset}  ${tag}     ${action} ${COLORS.bold}${formatNumber(count)}${
        COLORS.reset
      } rows  ${time}`
    );
  },

  knex: {
    warn: (message: string) => {
      log.warn(`[knex] ${message}`);
    },
    error: (message: string) => {
      log.error(`[knex] ${message}`);
    },
  },
};

interface PgError {
  severity?: string;
  code?: string;
  detail?: string;
  constraint?: string;
  table?: string;
  hint?: string;
  message?: string;
}

const isPgError = (err: unknown): err is PgError =>
  err !== null && typeof err === 'object' && 'severity' in err && 'code' in err;

export const formatDbError = (err: unknown): string => {
  if (!isPgError(err)) {
    const msg = err instanceof Error ? err.message : String(err);
    return msg.split('\n')[0].slice(0, 200);
  }

  const fields: Array<[string, string | undefined]> = [
    ['code', err.code],
    ['severity', err.severity],
    ['detail', err.detail],
    ['constraint', err.constraint],
    ['table', err.table],
    ['hint', err.hint],
  ];

  const padding = '                      ';
  return fields
    .filter(([, v]) => v)
    .map(([k, v]) => `${padding}${COLORS.dim}${k.padEnd(12)}${COLORS.reset}${v}`)
    .join('\n');
};
