import type { MigrationProfile } from '../engine/types';
import picturesProfile from './pictures';

/**
 * Register all migration profiles here.
 * To add a new migration, create a profile under src/profiles/<name>/
 * and add it to this map.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const profiles: Record<string, MigrationProfile<any, any>> = {
  pictures: picturesProfile,
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getProfile = (name: string): MigrationProfile<any, any> => {
  const profile = profiles[name];
  if (!profile) {
    const available = Object.keys(profiles).join(', ');
    throw new Error(`Unknown profile "${name}". Available profiles: ${available}`);
  }
  return profile;
};

export const listProfiles = (): string[] => Object.keys(profiles);
