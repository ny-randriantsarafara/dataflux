import type { TargetDialect } from '../../dialects/target';
import { mergePictureVariants, type PictureInsert } from './transform';
import { insertWithRetry } from '../../engine/batch';
import { log } from '../../engine/logger';

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

const buildValuesPlaceholder = (count: number): string => {
  const row = `(${COLUMNS.map(() => '?').join(', ')})`;
  return Array.from({ length: count }, () => row).join(', ');
};

const buildBindings = (pictures: PictureInsert[]): unknown[] =>
  pictures.flatMap((p) => [
    p.id,
    p.uuid,
    p.path,
    p.imageType,
    JSON.stringify(p.description_i18n),
    p.agencyId,
    p.focalPointX,
    p.focalPointY,
    p.width,
    p.height,
    p.crops,
    p.taxonomy,
    p.type,
    p.createdBy,
    p.createdAt,
    p.updatedAt,
    JSON.stringify(p.variants),
  ]);

const insertBatch = async (target: TargetDialect, pictures: PictureInsert[]): Promise<number> => {
  const knex = target.getClient();
  const columnList = COLUMNS.map((c) => `"${c}"`).join(', ');
  const values = buildValuesPlaceholder(pictures.length);
  const bindings = buildBindings(pictures);

  const query = `
    INSERT INTO ${TABLE_NAME} (${columnList})
    VALUES ${values}
    ON CONFLICT (id) DO UPDATE SET
      "description_i18n" = COALESCE("${TABLE_NAME}"."description_i18n"::jsonb, '{}'::jsonb) || EXCLUDED."description_i18n"::jsonb,
      "agencyId" = EXCLUDED."agencyId",
      "focalPointX" = EXCLUDED."focalPointX",
      "focalPointY" = EXCLUDED."focalPointY",
      taxonomy = EXCLUDED.taxonomy,
      path = CASE
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(EXCLUDED."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN EXCLUDED.path
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE("${TABLE_NAME}"."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN "${TABLE_NAME}".path
        ELSE COALESCE(EXCLUDED.path, "${TABLE_NAME}".path)
      END,
      "imageType" = CASE
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(EXCLUDED."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN EXCLUDED."imageType"
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE("${TABLE_NAME}"."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN "${TABLE_NAME}"."imageType"
        ELSE COALESCE(EXCLUDED."imageType", "${TABLE_NAME}"."imageType")
      END,
      width = CASE
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(EXCLUDED."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN EXCLUDED.width
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE("${TABLE_NAME}"."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN "${TABLE_NAME}".width
        ELSE COALESCE(EXCLUDED.width, "${TABLE_NAME}".width)
      END,
      height = CASE
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE(EXCLUDED."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN EXCLUDED.height
        WHEN EXISTS (
          SELECT 1 FROM jsonb_array_elements(COALESCE("${TABLE_NAME}"."variants", '[]'::jsonb)) v
          WHERE (v->>'formatId')::text = '85' AND v->>'path' IS NOT NULL
        ) THEN "${TABLE_NAME}".height
        ELSE COALESCE(EXCLUDED.height, "${TABLE_NAME}".height)
      END,
      "variants" = (
        WITH merged_variants AS (
          SELECT
            variant,
            format_id_key,
            format_picture_id_key,
            ROW_NUMBER() OVER (
              PARTITION BY
                format_id_key,
                format_picture_id_key
              ORDER BY
                CASE WHEN variant->>'path' IS NOT NULL THEN 1 ELSE 0 END DESC,
                source_order DESC,
                variant_order DESC
            ) AS row_num
          FROM (
            SELECT
              value AS variant,
              COALESCE(value->>'formatId', 'null') AS format_id_key,
              COALESCE(value->>'formatPictureId', 'null') AS format_picture_id_key,
              0 AS source_order,
              ordinality AS variant_order
            FROM jsonb_array_elements(COALESCE("${TABLE_NAME}"."variants", '[]'::jsonb)) WITH ORDINALITY
            UNION ALL
            SELECT
              value AS variant,
              COALESCE(value->>'formatId', 'null') AS format_id_key,
              COALESCE(value->>'formatPictureId', 'null') AS format_picture_id_key,
              1 AS source_order,
              ordinality AS variant_order
            FROM jsonb_array_elements(COALESCE(EXCLUDED."variants", '[]'::jsonb)) WITH ORDINALITY
          ) all_variants
        )
        SELECT COALESCE(jsonb_agg(variant ORDER BY format_id_key, format_picture_id_key), '[]'::jsonb)
        FROM merged_variants
        WHERE row_num = 1
      )
  `;

  const result = await knex.raw(query, bindings);
  return result.rowCount ?? pictures.length;
};

const deduplicateById = (pictures: PictureInsert[]): PictureInsert[] => {
  const map = new Map<number, PictureInsert>();

  for (const pic of pictures) {
    const existing = map.get(pic.id);
    if (existing) {
      const mergedPicture = {
        ...existing,
        description_i18n: { ...existing.description_i18n, ...pic.description_i18n },
        variants: mergePictureVariants(existing.variants, pic.variants),
      };
      map.set(pic.id, mergedPicture);
      continue;
    }

    map.set(pic.id, { ...pic });
  }

  return Array.from(map.values());
};

export const savePictures = async (target: TargetDialect, pictures: PictureInsert[]): Promise<number> => {
  if (pictures.length === 0) {
    return 0;
  }

  const deduplicated = deduplicateById(pictures);
  const start = Date.now();

  const inserted = await insertWithRetry(
    deduplicated,
    (batch) => insertBatch(target, batch),
    (pic) => `picture id=${pic.id}`
  );

  const elapsed = Date.now() - start;
  log.db('inserted', inserted, elapsed);

  return inserted;
};
