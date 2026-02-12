import fs from 'node:fs';
import path from 'node:path';
import type { Checkpoint, ProfileConfig } from './types';

const getBookmarkPath = (logDir: string, profileName: string): string => {
  return path.join(logDir, `migrate-${profileName}.bookmark.json`);
};

export const loadCheckpoint = (logDir: string, profileName: string): Checkpoint | undefined => {
  const filepath = getBookmarkPath(logDir, profileName);
  if (!fs.existsSync(filepath)) return undefined;

  const raw = fs.readFileSync(filepath, 'utf-8');
  const parsed: unknown = JSON.parse(raw);

  if (!isValidCheckpoint(parsed)) return undefined;

  return { processedFiles: parsed.processedFiles, profileConfig: parsed.profileConfig };
};

const isValidCheckpoint = (value: unknown): value is { processedFiles: string[]; profileConfig: ProfileConfig } => {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  if (!keys.includes('processedFiles') || !keys.includes('profileConfig')) return false;

  const record = value as Record<string, unknown>;
  if (!Array.isArray(record.processedFiles)) return false;
  if (!record.processedFiles.every((f): f is string => typeof f === 'string')) return false;
  if (typeof record.profileConfig !== 'object' || record.profileConfig === null || Array.isArray(record.profileConfig)) return false;

  const config = record.profileConfig as Record<string, unknown>;
  if (typeof config.batchSize !== 'number') return false;
  return true;
};

export const saveCheckpoint = (logDir: string, profileName: string, checkpoint: Checkpoint): void => {
  const filepath = getBookmarkPath(logDir, profileName);
  fs.writeFileSync(filepath, JSON.stringify(checkpoint, null, 2));
};

export const deleteCheckpoint = (logDir: string, profileName: string): boolean => {
  const filepath = getBookmarkPath(logDir, profileName);
  if (!fs.existsSync(filepath)) return false;
  fs.unlinkSync(filepath);
  return true;
};
