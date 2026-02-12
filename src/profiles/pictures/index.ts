import type { Knex } from 'knex';
import type { MigrationProfile, ProfileConfig } from '../../engine/types';
import { parseItem, groupRowsByPictureId, type DynamoRow } from './parse';
import { buildPictureFromDynamoRows, type PictureInsert } from './transform';
import { savePictures } from './upsert';

const picturesProfile: MigrationProfile<DynamoRow, PictureInsert> = {
  name: 'pictures',

  parseItem,

  groupRows: groupRowsByPictureId,

  transform: buildPictureFromDynamoRows,

  upsert: savePictures,

  filter: (row: DynamoRow, config: ProfileConfig) => {
    if (config.maxId !== undefined && row.pictureId >= config.maxId) {
      return false;
    }
    return true;
  },

  onComplete: async (knex: Knex) => {
    await knex.raw("SELECT setval('picture_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM picture))");
  },
};

export default picturesProfile;
