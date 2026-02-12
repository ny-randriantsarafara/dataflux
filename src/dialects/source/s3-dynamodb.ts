import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { createGunzip } from 'node:zlib';
import { Readable } from 'node:stream';
import { createInterface } from 'node:readline';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import type { SourceDialect, SourceConfig } from '../source';
import { registerSource } from '../source-registry';

/**
 * S3 DynamoDB source dialect.
 * Reads from S3 buckets containing DynamoDB Export to S3 format (gzipped JSON lines).
 */
class S3DynamoDBSource implements SourceDialect {
  readonly name = 's3-dynamodb';

  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(config: SourceConfig) {
    if (config.type !== 's3-dynamodb') {
      throw new Error('Invalid config type for S3 DynamoDB source');
    }
    this.client = new S3Client({ region: config.region });
    this.bucket = config.bucket;
    this.prefix = config.prefix;
  }

  async listFiles(): Promise<string[]> {
    const files: string[] = [];
    let continuationToken: string | undefined;

    do {
      const response = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: this.prefix,
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
  }

  async *streamRecords(fileKey: string): AsyncGenerator<Record<string, unknown>> {
    const response = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: fileKey })
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

  async close(): Promise<void> {
    // S3 client doesn't need explicit cleanup
  }
}

// Register the dialect
registerSource('s3-dynamodb', (config: SourceConfig) => new S3DynamoDBSource(config));
