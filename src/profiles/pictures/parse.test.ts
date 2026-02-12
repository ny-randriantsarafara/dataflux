import { groupRowsByPictureId, type DynamoRow } from './parse';

const createRow = (overrides: Partial<DynamoRow>): DynamoRow => {
  return {
    id: overrides.id ?? '1-6-68',
    pictureId: overrides.pictureId ?? 1,
    languageId: overrides.languageId ?? 6,
    url: overrides.url ?? '/img/2013/07/11/1.jpg',
    caption: overrides.caption ?? 'caption',
    agencyId: overrides.agencyId ?? 227,
    formatId: overrides.formatId ?? 68,
    formatPictureId: overrides.formatPictureId,
    originalWidth: overrides.originalWidth,
    originalHeight: overrides.originalHeight,
    x: overrides.x,
    y: overrides.y,
  };
};

describe('groupRowsByPictureId', () => {
  it('groups rows by picture id while keeping row order per picture', () => {
    const rows = [
      createRow({ id: '10-6-85', pictureId: 10, formatId: 85 }),
      createRow({ id: '20-6-85', pictureId: 20, formatId: 85 }),
      createRow({ id: '10-6-72', pictureId: 10, formatId: 72 }),
      createRow({ id: '20-6-72', pictureId: 20, formatId: 72 }),
    ];

    const groupedRows = groupRowsByPictureId(rows);

    expect(groupedRows).toHaveLength(2);
    expect(groupedRows[0].map((row) => row.id)).toEqual(['10-6-85', '10-6-72']);
    expect(groupedRows[1].map((row) => row.id)).toEqual(['20-6-85', '20-6-72']);
  });
});
