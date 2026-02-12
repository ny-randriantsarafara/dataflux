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

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;

  const record = parsed as Record<string, unknown>;
  const processedFiles = Array.isArray(record.processedFiles) ? (record.processedFiles as string[]) : [];
  const profileConfig = typeof record.profileConfig === 'object' ? (record.profileConfig as ProfileConfig) : undefined;

  if (!profileConfig) return undefined;

  return { processedFiles, profileConfig };
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
