import { log, formatDbError } from './logger';

/**
 * Generic retry strategy: on failure, split the batch in half and retry each half.
 * Recurses until individual items, then logs and skips the failing item.
 *
 * @param items      - The batch to insert
 * @param insertFn   - The actual insert function (profile-specific)
 * @param itemLabel  - A function to produce a log-friendly label for a single item (e.g. "picture id=123")
 */
export const insertWithRetry = async <T>(
  items: T[],
  insertFn: (batch: T[]) => Promise<number>,
  itemLabel: (item: T) => string
): Promise<number> => {
  try {
    return await insertFn(items);
  } catch (err) {
    if (items.length === 1) {
      const detail = formatDbError(err);
      log.warn(`Skipping ${itemLabel(items[0])}\n${detail}`);
      return 0;
    }

    const mid = Math.ceil(items.length / 2);
    const left = items.slice(0, mid);
    const right = items.slice(mid);

    log.warn(`Batch of ${items.length} failed, splitting into ${left.length} + ${right.length}`);

    const [leftCount, rightCount] = await Promise.all([
      insertWithRetry(left, insertFn, itemLabel),
      insertWithRetry(right, insertFn, itemLabel),
    ]);

    return leftCount + rightCount;
  }
};
