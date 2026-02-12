import { v5 as uuidv5 } from 'uuid';
import { Domain } from '@/infinity/shared/constants/domain';
import { ImageType } from '@/infinity/shared/types/image-types';
import type { DynamoRow } from './scan-dynamodb';

const NAMESPACE = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
const FORMAT_85_ID = 85;
const KNOWN_FORMAT_DIMENSIONS: Record<number, { width: number; height: number }> = {
  85: { width: 2560, height: 1440 },
  72: { width: 640, height: 480 },
  68: { width: 310, height: 310 },
};

export type PictureVariant = {
  formatId: number | null;
  formatPictureId: number | null;
  width: number | null;
  height: number | null;
  path: string | null;
};

export type PictureInsert = {
  id: number;
  uuid: string;
  path: string;
  imageType: ImageType;
  description_i18n: Record<string, string>;
  agencyId: number;
  focalPointX: number | null;
  focalPointY: number | null;
  width: number | null;
  height: number | null;
  crops: string;
  taxonomy: string;
  type: string;
  createdBy: string;
  createdAt: Date;
  updatedAt: Date;
  variants: PictureVariant[];
};

const netsportLanguageIdMap: Record<number, string> = Object.values(Domain).reduce(
  (acc, domain) => ({
    ...acc,
    [domain.netsportLanguageId]: domain.languageId,
  }),
  {} as Record<number, string>
);

const getImageTypeFromExtension = (path: string): ImageType => {
  const ext = path.split('.').pop()?.toLowerCase();
  switch (ext) {
    case 'jpg':
    case 'jpeg':
      return ImageType.IMAGE_JPEG;
    case 'png':
      return ImageType.IMAGE_PNG;
    case 'gif':
      return ImageType.IMAGE_GIF;
    default:
      return ImageType.IMAGE_JPEG;
  }
};

const stripImgPrefix = (url: string): string => {
  return url.replace(/^\/img\//, '');
};

const getFileExtension = (picturePath: string): string => {
  const picturePathParts = picturePath.split('.');
  if (picturePathParts.length <= 1) {
    return '';
  }

  return `.${picturePathParts[picturePathParts.length - 1]}`;
};

const buildVariantPath = (picturePath: string, formatPictureId: number, formatId: number): string | null => {
  const dimensions = KNOWN_FORMAT_DIMENSIONS[formatId];
  if (!dimensions) {
    return null;
  }

  const extension = getFileExtension(picturePath);
  if (!extension) {
    return null;
  }
  const picturePathWithoutExtension = picturePath.substring(0, picturePath.length - extension.length);
  return `${picturePathWithoutExtension}-${formatPictureId}-${dimensions.width}-${dimensions.height}${extension}`;
};

const buildPictureVariantFromRow = (row: DynamoRow): PictureVariant => {
  const formatPictureId = row.formatPictureId ?? null;
  if (formatPictureId === null) {
    return {
      formatId: row.formatId ?? null,
      formatPictureId: null,
      width: null,
      height: null,
      path: null,
    };
  }

  const picturePath = stripImgPrefix(row.url);
  const variantPath = buildVariantPath(picturePath, formatPictureId, row.formatId);
  const dimensions = KNOWN_FORMAT_DIMENSIONS[row.formatId];
  if (!variantPath || !dimensions) {
    return {
      formatId: row.formatId ?? null,
      formatPictureId,
      width: null,
      height: null,
      path: null,
    };
  }

  return {
    formatId: row.formatId,
    formatPictureId,
    width: dimensions.width,
    height: dimensions.height,
    path: variantPath,
  };
};

const getVariantKey = ({ formatId, formatPictureId }: PictureVariant): string => {
  return `${formatId ?? 'null'}-${formatPictureId ?? 'null'}`;
};

export const mergePictureVariants = (existing: PictureVariant[], incoming: PictureVariant[]): PictureVariant[] => {
  const variantsByKey = new Map<string, PictureVariant>();
  const allVariants = [...existing, ...incoming];

  for (const variant of allVariants) {
    const key = getVariantKey(variant);
    const currentVariant = variantsByKey.get(key);
    if (!currentVariant) {
      variantsByKey.set(key, variant);
      continue;
    }

    if (currentVariant.path && !variant.path) {
      continue;
    }

    variantsByKey.set(key, variant);
  }

  return Array.from(variantsByKey.values());
};

const buildDescriptionI18n = (rows: DynamoRow[]): Record<string, string> => {
  const descriptions: Record<string, string> = {};

  for (const row of rows) {
    const languageId = netsportLanguageIdMap[row.languageId];
    if (!languageId) {
      continue;
    }
    if (!row.caption) {
      continue;
    }
    descriptions[languageId] = row.caption;
  }

  if (!descriptions.en) {
    descriptions.en = '';
  }

  return descriptions;
};

const findOriginalDimensions = (rows: DynamoRow[]): { width: number | null; height: number | null } => {
  for (const row of rows) {
    if (row.originalWidth !== undefined && row.originalHeight !== undefined) {
      return { width: row.originalWidth, height: row.originalHeight };
    }
  }
  return { width: null, height: null };
};

const buildPictureFromRow = (primaryRow: DynamoRow, allRows: DynamoRow[]): PictureInsert => {
  const pictureId = primaryRow.pictureId;
  const descriptionI18n = buildDescriptionI18n(allRows);
  const path = stripImgPrefix(primaryRow.url);
  const variants = mergePictureVariants([], allRows.map(buildPictureVariantFromRow));
  const { width, height } = findOriginalDimensions(allRows);
  const now = new Date();

  return {
    id: pictureId,
    uuid: uuidv5(pictureId.toString(), NAMESPACE),
    path,
    imageType: getImageTypeFromExtension(path),
    description_i18n: descriptionI18n,
    agencyId: primaryRow.agencyId,
    focalPointX: null,
    focalPointY: null,
    width,
    height,
    crops: JSON.stringify([]),
    taxonomy: JSON.stringify([]),
    type: 'MEDIA',
    createdBy: 'migration-script',
    createdAt: now,
    updatedAt: now,
    variants,
  };
};

export const buildPictureFromDynamoRows = (rows: DynamoRow[]): PictureInsert => {
  if (rows.length === 0) {
    throw new Error('Cannot build picture from empty rows array');
  }

  const primaryRow = rows.find((r) => r.formatId === FORMAT_85_ID);
  if (!primaryRow) {
    const fallbackRow = rows[0];
    return buildPictureFromRow(fallbackRow, rows);
  }

  return buildPictureFromRow(primaryRow, rows);
};

export const buildPictureFromSingleRow = (row: DynamoRow): PictureInsert => {
  const languageId = netsportLanguageIdMap[row.languageId];
  const descriptionI18n: Record<string, string> = {};
  if (languageId && row.caption) {
    descriptionI18n[languageId] = row.caption;
  }
  if (!descriptionI18n.en) {
    descriptionI18n.en = '';
  }

  const path = stripImgPrefix(row.url);
  const variants = mergePictureVariants([], [buildPictureVariantFromRow(row)]);
  const now = new Date();

  return {
    id: row.pictureId,
    uuid: uuidv5(row.pictureId.toString(), NAMESPACE),
    path,
    imageType: getImageTypeFromExtension(path),
    description_i18n: descriptionI18n,
    agencyId: row.agencyId,
    focalPointX: null,
    focalPointY: null,
    width: row.originalWidth ?? null,
    height: row.originalHeight ?? null,
    crops: JSON.stringify([]),
    taxonomy: JSON.stringify([]),
    type: 'MEDIA',
    createdBy: 'migration-script',
    createdAt: now,
    updatedAt: now,
    variants,
  };
};
