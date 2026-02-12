import type { TargetDialect, TargetConfig } from '../../dialects/target';
import type { MigrationProfile, TargetRunnerConfig } from '../../engine/types';
import { parseItem, groupRowsByPictureId, type DynamoRow } from './parse';
import { buildPictureFromDynamoRows, type PictureInsert } from './transform';
import { savePictures } from './upsert';
import { createPostgreSQLTarget } from '../../dialects/target/postgresql';

const TABLE_NAME = 'picture';

const requiredString = (value: unknown, name: string): string => {
  if (typeof value !== 'string' || value === '') {
    throw new Error(`Invalid ${name}: expected non-empty string`);
  }
  return value;
};

const COLUMNS: ReadonlyArray<string> = [
  'id',
  'uuid',
  'path',
  'imageType',
  'description_i18n',
  'agencyId',
  'focalPointX',
  'focalPointY',
  'width',
  'height',
  'crops',
  'taxonomy',
  'type',
  'createdBy',
  'createdAt',
  'updatedAt',
  'variants',
];

const picturesProfile: MigrationProfile<DynamoRow, PictureInsert> = {
  name: 'pictures',

  parseItem,

  groupRows: groupRowsByPictureId,

  transform: buildPictureFromDynamoRows,

  createTarget: (config: TargetRunnerConfig): TargetDialect => {
    const required = ['host', 'port', 'user', 'password', 'database'] as const;
    for (const key of required) {
      if (config[key] === undefined || config[key] === null || config[key] === '') {
        throw new Error(`Missing required target config: ${key}`);
      }
    }

    const port = config.port;
    const portNum = typeof port === 'number' ? port : Number(port);
    if (!Number.isInteger(portNum) || portNum <= 0 || portNum > 65535) {
      throw new Error(`Invalid port: ${port}`);
    }

    // Extract validated string values (TS narrows based on our checks)
    const host = requiredString(config.host, 'host');
    const user = requiredString(config.user, 'user');
    const password = requiredString(config.password, 'password');
    const database = requiredString(config.database, 'database');

    const targetConfig: TargetConfig = {
      type: 'postgresql',
      host,
      port: portNum,
      user,
      password,
      database,
      ssl: config.ssl !== false,
    };

    return createPostgreSQLTarget(targetConfig, TABLE_NAME, COLUMNS);
  },

  upsert: savePictures,

  filter: (row: DynamoRow, config) => {
    if (config.maxId !== undefined && row.pictureId >= config.maxId) {
      return false;
    }
    return true;
  },

  onComplete: async (target: TargetDialect) => {
    const knex = target.getClient();
    await knex.raw("SELECT setval('picture_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM picture))");
  },
};

export default picturesProfile;
