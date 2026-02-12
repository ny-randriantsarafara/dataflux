import { buildPictureFromDynamoRows, mergePictureVariants } from './build-picture';
import { DynamoRow } from './scan-dynamodb';

const createRow = (overrides: Partial<DynamoRow>): DynamoRow => {
  return {
    id: overrides.id ?? '1057245-6-68',
    pictureId: overrides.pictureId ?? 1057245,
    languageId: overrides.languageId ?? 6,
    url: overrides.url ?? '/img/2013/07/11/1057245.jpg',
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

describe('build-picture', () => {
  it('prefers format 85 as primary row when available', () => {
    const rows = [
      createRow({ id: '1057245-6-72', formatId: 72, url: '/img/2013/07/11/from-72.jpg' }),
      createRow({ id: '1057245-6-85', formatId: 85, url: '/img/2013/07/11/from-85.jpg' }),
    ];

    const picture = buildPictureFromDynamoRows(rows);

    expect(picture.path).toEqual('2013/07/11/from-85.jpg');
  });

  it('falls back to the first row when format 85 is missing', () => {
    const rows = [
      createRow({ id: '1057245-6-72', formatId: 72, url: '/img/2013/07/11/from-first.jpg' }),
      createRow({ id: '1057245-6-68', formatId: 68, url: '/img/2013/07/11/from-second.jpg' }),
    ];

    const picture = buildPictureFromDynamoRows(rows);

    expect(picture.path).toEqual('2013/07/11/from-first.jpg');
  });

  it('builds resolved paths for known formats and keeps unknown format metadata unresolved', () => {
    const rows = [
      createRow({ id: '1057245-6-85', formatId: 85, formatPictureId: 74654029 }),
      createRow({ id: '1057245-6-72', formatId: 72, formatPictureId: 74654026 }),
      createRow({ id: '1057245-6-999', formatId: 999, formatPictureId: 74654030 }),
    ];

    const picture = buildPictureFromDynamoRows(rows);

    expect(picture.variants).toEqual([
      {
        formatId: 85,
        formatPictureId: 74654029,
        width: 2560,
        height: 1440,
        path: '2013/07/11/1057245-74654029-2560-1440.jpg',
      },
      {
        formatId: 72,
        formatPictureId: 74654026,
        width: 640,
        height: 480,
        path: '2013/07/11/1057245-74654026-640-480.jpg',
      },
      {
        formatId: 999,
        formatPictureId: 74654030,
        width: null,
        height: null,
        path: null,
      },
    ]);
  });

  it('keeps existing non-null path when merging duplicate variant keys', () => {
    const existing = [
      {
        formatId: 85,
        formatPictureId: 74654029,
        width: 2560,
        height: 1440,
        path: '2013/07/11/1057245-74654029-2560-1440.jpg',
      },
    ];
    const incoming = [
      {
        formatId: 85,
        formatPictureId: 74654029,
        width: null,
        height: null,
        path: null,
      },
    ];

    const merged = mergePictureVariants(existing, incoming);

    expect(merged).toEqual(existing);
  });
});
