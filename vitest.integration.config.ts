import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
    environment: 'node',
    globals: true,
    setupFiles: ['tests/setup-env.ts'],
    fileParallelism: false,
    testTimeout: 30_000,
  },
});
