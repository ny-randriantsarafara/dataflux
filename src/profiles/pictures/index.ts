import type { TargetDialect, TargetConfig } from '../../dialects/target';
import type { MigrationProfile, TargetRunnerConfig } from '../../engine/types';
import { parseItem, groupRowsByPictureId, type DynamoRow } from './parse';
import { buildPictureFromDynamoRows, type PictureInsert } from './transform';
import { savePictures } from './upsert';
import { createPostgreSQLTarget } from '../../dialects/target/postgresql';

const TABLE_NAME = 'picture';

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
    const targetConfig: TargetConfig = {
      type: 'postgresql',
      host: String(config.host),
      port: Number(config.port),
      user: String(config.user),
      password: String(config.password),
      database: String(config.database),
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
