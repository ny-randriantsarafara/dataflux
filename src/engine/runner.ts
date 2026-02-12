import type { MigrationProfile, RunnerConfig, Checkpoint, MetricsHook } from './types';
import { loadCheckpoint, saveCheckpoint, deleteCheckpoint } from './checkpoint';
import { log, formatDbError } from './logger';
import { createSource } from '../dialects/source-registry';
import { createTarget } from '../dialects/target-registry';

// Import dialects to register them
import '../dialects/source/s3-dynamodb';
import '../dialects/target/postgresql';

export type RunResult = {
  totalScanned: number;
  totalInserted: number;
  totalSkipped: number;
  totalErrors: number;
  filesProcessed: number;
  filesTotal: number;
  completed: boolean;
};

const emitMetrics = (
  config: RunnerConfig,
  name: keyof MetricsHook,
  params: Parameters<NonNullable<MetricsHook[keyof MetricsHook]>>[0]
): void => {
  const hook = config.metrics?.[name];
  if (hook) {
    // @ts-expect-error - dynamic metric call
    hook(params);
  }
};

export const run = async <TRaw, TInsert>(
  profile: MigrationProfile<TRaw, TInsert>,
  config: RunnerConfig,
  shouldStop: () => boolean
): Promise<RunResult> => {
  const startTime = Date.now();

  const source = createSource(config.sourceConfig as never);
  const target = createTarget(config.targetConfig as never);

  try {
    // 1. List all export files
    log.info(`Listing source files via ${source.name}`);
    const allFiles = await source.listFiles();
    log.info(`Found ${allFiles.length} files`);

    if (allFiles.length === 0) {
      log.warn('No files found');
      return {
        totalScanned: 0,
        totalInserted: 0,
        totalSkipped: 0,
        totalErrors: 0,
        filesProcessed: 0,
        filesTotal: 0,
        completed: true,
      };
    }

    // 2. Load checkpoint
    const checkpoint = loadCheckpoint(config.logDir, profile.name);
    const processedSet = new Set(checkpoint?.processedFiles ?? []);
    const filesToProcess = allFiles.filter((f) => !processedSet.has(f));

    if (processedSet.size > 0) {
      log.info(`Skipping ${processedSet.size} already-processed files`);
    }

    const { batchSize } = config.profileConfig;
    const resuming = processedSet.size > 0;

    log.migration.start(profile.name, {
      batchSize,
      maxId: config.profileConfig.maxId,
      fileCount: filesToProcess.length,
      resuming,
    });

    emitMetrics(config, 'onStart', {
      profileName: profile.name,
      fileCount: filesToProcess.length,
      batchSize,
      resuming,
    });

    // 3. Process files one at a time
    let totalScanned = 0;
    let totalInserted = 0;
    let totalSkipped = 0;
    let totalErrors = 0;
    let filesProcessed = processedSet.size;

    let fileIndex = processedSet.size;

    for (const fileKey of filesToProcess) {
      if (shouldStop()) {
        log.info('Graceful shutdown — saving checkpoint');
        break;
      }

      fileIndex++;
      const shortName = fileKey.split('/').pop() ?? fileKey;
      log.fileCounter(fileIndex, allFiles.length, shortName, 'reading...');

      let fileScanned = 0;
      let fileInserted = 0;
      let fileSkipped = 0;
      let fileErrors = 0;

      // Collect all rows from the file, then group + transform + upsert
      const rows: TRaw[] = [];

      try {
        for await (const rawItem of source.streamRecords(fileKey)) {
          const parsed = profile.parseItem(rawItem);
          if (!parsed) {
            fileSkipped++;
            continue;
          }

          if (profile.filter && !profile.filter(parsed, config.profileConfig)) {
            fileSkipped++;
            continue;
          }

          rows.push(parsed);
          fileScanned++;

          if (fileScanned % batchSize === 0) {
            log.fileCounter(
              fileIndex,
              allFiles.length,
              shortName,
              `parsed ${fileScanned.toLocaleString('en-US')} rows...`
            );
          }
        }
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        log.error(`Failed to read ${shortName}: ${detail}`);
        fileErrors++;
        totalErrors++;
        continue;
      }

      // Group and transform
      const groups = profile.groupRows(rows);
      const records = groups.map((group) => profile.transform(group));
      log.fileCounter(
        fileIndex,
        allFiles.length,
        shortName,
        `parsed ${fileScanned.toLocaleString('en-US')} rows, grouped into ${records.length.toLocaleString('en-US')} records`
      );

      // Batch upsert
      for (let i = 0; i < records.length; i += batchSize) {
        if (shouldStop()) break;

        const batch = records.slice(i, i + batchSize);
        try {
          const count = await profile.upsert(target, batch);
          fileInserted += count;
        } catch (err) {
          const detail = formatDbError(err);
          log.error(`Upsert failed in ${shortName}: ${detail}`);
          fileErrors++;
        }
      }

      // Update checkpoint after each file
      processedSet.add(fileKey);
      filesProcessed++;
      totalScanned += fileScanned;
      totalInserted += fileInserted;
      totalSkipped += fileSkipped;
      totalErrors += fileErrors;

      log.fileCounter(
        fileIndex,
        allFiles.length,
        shortName,
        `done — scanned ${fileScanned.toLocaleString('en-US')}, inserted ${fileInserted.toLocaleString('en-US')}`
      );

      const currentCheckpoint: Checkpoint = {
        processedFiles: Array.from(processedSet),
        profileConfig: config.profileConfig,
      };
      saveCheckpoint(config.logDir, profile.name, currentCheckpoint);
      log.fileCounter(fileIndex, allFiles.length, shortName, 'checkpoint saved');

      log.runningTotal({ scanned: totalScanned, inserted: totalInserted, skipped: totalSkipped, errors: totalErrors });

      emitMetrics(config, 'onFileComplete', {
        fileIndex,
        fileTotal: allFiles.length,
        fileName: shortName,
        scanned: fileScanned,
        inserted: fileInserted,
        skipped: fileSkipped,
        errors: fileErrors,
      });

      emitMetrics(config, 'onProgress', {
        scanned: totalScanned,
        inserted: totalInserted,
        skipped: totalSkipped,
        errors: totalErrors,
        filesProcessed,
        filesTotal: allFiles.length,
      });
    }

    // 4. Completion
    const completed = filesProcessed === allFiles.length && !shouldStop();

    if (completed && profile.onComplete) {
      await profile.onComplete(target);
      log.success('Post-migration hook completed');
    }

    if (completed) {
      deleteCheckpoint(config.logDir, profile.name);
    }

    const elapsedMs = Date.now() - startTime;

    log.migration.summary({
      scanned: totalScanned,
      inserted: totalInserted,
      skipped: totalSkipped,
      errors: totalErrors,
      completed,
      elapsed: elapsedMs,
      filesProcessed,
      filesTotal: allFiles.length,
    });

    emitMetrics(config, 'onComplete', {
      profileName: profile.name,
      scanned: totalScanned,
      inserted: totalInserted,
      skipped: totalSkipped,
      errors: totalErrors,
      filesProcessed,
      filesTotal: allFiles.length,
      completed,
      elapsedMs,
    });

    return {
      totalScanned,
      totalInserted,
      totalSkipped,
      totalErrors,
      filesProcessed,
      filesTotal: allFiles.length,
      completed,
    };
  } finally {
    if (target.close) {
      await target.close();
    }
    if (source.close) {
      await source.close();
    }
    log.info('Connections closed');
  }
};
