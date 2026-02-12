import { z } from 'zod';

// Coerce strings to numbers (DynamoDB N type can unmarshall to strings)
const numericField = z.union([z.number(), z.string().transform(Number)]);
const optionalNumericField = z.union([z.number(), z.string().transform(Number)]).optional().nullable();

const DynamoRowSchema = z.object({
  id: z.string(),
  pictureId: numericField,
  languageId: numericField,
  url: z.string(),
  caption: z.string(),
  agencyId: numericField,
  formatId: numericField,
  formatPictureId: optionalNumericField,
  originalWidth: optionalNumericField,
  originalHeight: optionalNumericField,
  x: optionalNumericField,
  y: optionalNumericField,
});

export type DynamoRow = z.infer<typeof DynamoRowSchema>;

let errorLogCount = 0;
const MAX_ERROR_LOGS = 10;

export const parseItem = (raw: Record<string, unknown>): DynamoRow | null => {
  // Ensure caption defaults to empty string
  const withDefaults = {
    ...raw,
    caption: raw.caption ?? '',
  };

  const result = DynamoRowSchema.safeParse(withDefaults);
  if (!result.success) {
    // Log first few validation errors to help debug schema mismatches
    if (errorLogCount < MAX_ERROR_LOGS) {
      console.error(`[parse] Validation failed for record:`, JSON.stringify(raw).slice(0, 200));
      console.error(`[parse] Errors:`, JSON.stringify(result.error.issues, null, 2));
      errorLogCount++;
      if (errorLogCount === MAX_ERROR_LOGS) {
        console.error(`[parse] Suppressing further validation errors...`);
      }
    }
    return null;
  }
  return result.data;
};

export const groupRowsByPictureId = (rows: DynamoRow[]): DynamoRow[][] => {
  const rowsByPictureId = new Map<number, DynamoRow[]>();

  for (const row of rows) {
    const existingRows = rowsByPictureId.get(row.pictureId);
    if (existingRows) {
      rowsByPictureId.set(row.pictureId, [...existingRows, row]);
      continue;
    }

    rowsByPictureId.set(row.pictureId, [row]);
  }

  return Array.from(rowsByPictureId.values());
};
