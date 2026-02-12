import type { SourceDialect, SourceConfig } from './source';

type SourceDialectFactory = (config: SourceConfig) => SourceDialect;

const sources: Record<string, SourceDialectFactory> = {};

/**
 * Register a source dialect factory.
 * Call this in each dialect implementation to register itself.
 */
export const registerSource = (type: string, factory: SourceDialectFactory): void => {
  sources[type] = factory;
};

/**
 * Create a source dialect from configuration
 */
export const createSource = (config: SourceConfig): SourceDialect => {
  const factory = sources[config.type];
  if (!factory) {
    const available = Object.keys(sources).join(', ');
    throw new Error(`Unknown source type "${config.type}". Available: ${available}`);
  }
  return factory(config);
};

/**
 * List all registered source types
 */
export const listSourceTypes = (): string[] => Object.keys(sources);
