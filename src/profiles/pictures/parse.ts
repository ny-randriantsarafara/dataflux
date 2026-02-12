import { z } from 'zod';

const DynamoRowSchema = z.object({
  id: z.string(),
  pictureId: z.number(),
  languageId: z.number(),
  url: z.string(),
  caption: z.string(),
  agencyId: z.number(),
  formatId: z.number(),
  formatPictureId: z.number().optional(),
  originalWidth: z.number().optional(),
  originalHeight: z.number().optional(),
  x: z.number().optional(),
  y: z.number().optional(),
});

export type DynamoRow = z.infer<typeof DynamoRowSchema>;

export const parseItem = (raw: Record<string, unknown>): DynamoRow | null => {
  // Ensure caption defaults to empty string
  const withDefaults = {
    ...raw,
    caption: raw.caption ?? '',
  };

  const result = DynamoRowSchema.safeParse(withDefaults);
  if (!result.success) {
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
