import { describe, expect, it } from 'vitest';
import { advisoryLockKey } from '../../src/hash.js';

describe('advisoryLockKey', () => {
  it('matches the published FNV-1a 64-bit test vector for the empty string', () => {
    // FNV-1a of "" is exactly the offset basis, unsigned: 14695981039346656037.
    // As a signed 64-bit value (Postgres bigint's range): subtract 2^64.
    // 14695981039346656037 - 18446744073709551616 = -3750763034362895579
    expect(advisoryLockKey('')).toBe(-3750763034362895579n);
  });

  it('matches the published FNV-1a 64-bit test vector for "a"', () => {
    // The canonical FNV-1a 64 test vector for "a" is 0xaf63dc4c8601ec8c
    // unsigned = 12638187200555641996.
    // 12638187200555641996 - 18446744073709551616 = -5808556873153909620
    expect(advisoryLockKey('a')).toBe(-5808556873153909620n);
  });

  it('is deterministic', () => {
    expect(advisoryLockKey('order-123')).toBe(advisoryLockKey('order-123'));
  });

  it('returns a value within Postgres bigint range', () => {
    const min = -(2n ** 63n);
    const max = 2n ** 63n - 1n;
    for (const s of ['', 'a', 'order-123', 'a-very-long-aggregate-id-' + 'x'.repeat(500)]) {
      const key = advisoryLockKey(s);
      expect(key >= min && key <= max).toBe(true);
    }
  });

  it('differs for different inputs (not a guarantee, but true for these)', () => {
    expect(advisoryLockKey('order-123')).not.toBe(advisoryLockKey('order-124'));
  });
});
