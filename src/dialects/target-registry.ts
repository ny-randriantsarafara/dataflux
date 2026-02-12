import type { TargetDialect, TargetConfig } from './target';

type TargetDialectFactory = (config: TargetConfig) => TargetDialect;

const targets: Record<string, TargetDialectFactory> = {};

/**
 * Register a target dialect factory.
 * Call this in each dialect implementation to register itself.
 */
export const registerTarget = (type: string, factory: TargetDialectFactory): void => {
  targets[type] = factory;
};

/**
 * Create a target dialect from configuration
 */
export const createTarget = (config: TargetConfig): TargetDialect => {
  const factory = targets[config.type];
  if (!factory) {
    const available = Object.keys(targets).join(', ');
    throw new Error(`Unknown target type "${config.type}". Available: ${available}`);
  }
  return factory(config);
};

/**
 * List all registered target types
 */
export const listTargetTypes = (): string[] => Object.keys(targets);
