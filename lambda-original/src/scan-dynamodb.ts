import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import { DynamoDB, ScanCommand } from '@aws-sdk/client-dynamodb';
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

type DynamoDBItem = Record<string, AttributeValue>;

export type ScanPage = {
  rows: DynamoRow[];
  lastKey?: Record<string, AttributeValue>;
  scannedCount: number;
};

const parseNumberAttribute = (value: AttributeValue | undefined): number | undefined => {
  if (!value?.N) return undefined;
  const parsed = Number(value.N);
  return isNaN(parsed) ? undefined : parsed;
};

const parseStringAttribute = (value: AttributeValue | undefined): string | undefined => {
  return value?.S;
};

const parseDynamoItem = (item: DynamoDBItem): DynamoRow | null => {
  const raw = {
    id: parseStringAttribute(item.id),
    pictureId: parseNumberAttribute(item.pictureId),
    languageId: parseNumberAttribute(item.languageId),
    url: parseStringAttribute(item.url),
    caption: parseStringAttribute(item.caption) ?? '',
    agencyId: parseNumberAttribute(item.agencyId),
    formatId: parseNumberAttribute(item.formatId),
    formatPictureId: parseNumberAttribute(item.formatPictureId),
    originalWidth: parseNumberAttribute(item.originalWidth),
    originalHeight: parseNumberAttribute(item.originalHeight),
    x: parseNumberAttribute(item.x),
    y: parseNumberAttribute(item.y),
  };

  const result = DynamoRowSchema.safeParse(raw);
  if (!result.success) {
    console.warn(`Skipping invalid DynamoDB item: ${JSON.stringify(raw)}, errors: ${result.error.message}`);
    return null;
  }
  return result.data;
};

const parseItems = (items: DynamoDBItem[]): DynamoRow[] => {
  const rows: DynamoRow[] = [];
  for (const item of items) {
    const row = parseDynamoItem(item);
    if (row) {
      rows.push(row);
    }
  }
  return rows;
};

export async function* scanSegment(
  client: DynamoDB,
  tableName: string,
  segment: number,
  totalSegments: number,
  startKey?: Record<string, AttributeValue>
): AsyncGenerator<ScanPage> {
  let exclusiveStartKey = startKey;

  do {
    const response = await client.send(
      new ScanCommand({
        TableName: tableName,
        Segment: segment,
        TotalSegments: totalSegments,
        ExclusiveStartKey: exclusiveStartKey,
      })
    );

    const rows = response.Items ? parseItems(response.Items as DynamoDBItem[]) : [];
    const lastKey = response.LastEvaluatedKey as Record<string, AttributeValue> | undefined;

    yield { rows, lastKey, scannedCount: response.Items?.length ?? 0 };

    exclusiveStartKey = lastKey;
  } while (exclusiveStartKey);
}
