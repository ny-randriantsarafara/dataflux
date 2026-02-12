/**
 * Inlined from infinity-cms-2 backend/src/shared/constants/domain.ts
 * Maps DynamoDB netsportLanguageId → CMS languageId
 */
export const netsportLanguageIdMap: Record<number, string> = {
  0: 'en',
  1: 'de',
  3: 'fr',
  4: 'it',
  5: 'nl',
  6: 'es',
  9: 'tr',
  11: 'da',
  13: 'nb',
  14: 'pl',
  15: 'ru',
  16: 'ro',
  17: 'hu',
};

/**
 * Inlined from infinity-cms-2 backend/src/shared/types/image-types.ts
 */
export enum ImageType {
  IMAGE_JPEG = 'image/jpeg',
  IMAGE_GIF = 'image/gif',
  IMAGE_PNG = 'image/png',
}
