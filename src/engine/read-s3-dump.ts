import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { createGunzip } from 'node:zlib';
import { Readable } from 'node:stream';
import { createInterface } from 'node:readline';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import type { AttributeValue } from '@aws-sdk/client-dynamodb';

/**
 * List all export data files from an S3 prefix.
 * DynamoDB Export to S3 places data files under {prefix}/AWSDynamoDB/{exportId}/data/*.json.gz
 * We list everything under the prefix and filter for .json.gz files.
 */
export const listExportFiles = async (
  s3Client: S3Client,
  bucket: string,
  prefix: string
): Promise<string[]> => {
  const files: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    if (response.Contents) {
      for (const object of response.Contents) {
        if (object.Key && object.Key.endsWith('.json.gz')) {
          files.push(object.Key);
        }
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  files.sort();
  return files;
};

/**
 * Stream and parse a single S3 export file.
 * Each line is a JSON object: { "Item": { "id": { "S": "..." }, ... } }
 * We unmarshall the DynamoDB typed attributes into plain JS objects.
 */
export async function* readExportFile(
  s3Client: S3Client,
  bucket: string,
  key: string
): AsyncGenerator<Record<string, unknown>> {
  const response = await s3Client.send(
    new GetObjectCommand({ Bucket: bucket, Key: key })
  );

  if (!response.Body) {
    return;
  }

  const bodyStream = response.Body as Readable;
  const gunzip = createGunzip();
  const decompressed = bodyStream.pipe(gunzip);

  const rl = createInterface({
    input: decompressed,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const parsed = JSON.parse(trimmed);
      const item = parsed.Item ?? parsed;
      const unmarshalled = unmarshall(item as Record<string, AttributeValue>);
      yield unmarshalled;
    } catch {
      // Skip malformed lines
    }
  }
}
