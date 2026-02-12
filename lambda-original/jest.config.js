module.exports = {
  resetMocks: true,
  modulePathIgnorePatterns: ['build', 'dist'],
  preset: 'ts-jest/presets/js-with-ts-esm',
  moduleNameMapper: {
    '^@/infinity/(.*)$': '<rootDir>/../../backend/src/$1',
    '^@/(.*)$': ['<rootDir>/src/$1', '<rootDir>/../../backend/src/$1'],
  },
};
