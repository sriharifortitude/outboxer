// A stable, deterministic 64-bit hash from an aggregate id to the
// signed bigint key pg_advisory_lock expects. FNV-1a: simple, no
// dependency, and this is the one place in the whole library where the
// hash function's exact output actually matters (see
// docs/adr/0003-per-aggregate-ordering-with-advisory-locks.md) -- two
// different aggregate ids hashing to the same lock key would only cost
// throughput (one waits for the other unnecessarily), but the reverse
// bug -- treating two different hashes as needing separate locks when
// they should be the same -- can't happen since the hash is a pure
// function of the input.
const FNV_OFFSET_BASIS = 14695981039346656037n;
const FNV_PRIME = 1099511628211n;
const MASK_64 = 0xffffffffffffffffn;
const SIGNED_64_MAX = 0x7fffffffffffffffn;
const WRAP = 0x10000000000000000n;

/** FNV-1a over the UTF-8 bytes of s, returned as a signed 64-bit BigInt
 *  (Postgres's bigint range) -- the type pg_advisory_lock takes. */
export function advisoryLockKey(s: string): bigint {
  let hash = FNV_OFFSET_BASIS;
  const bytes = new TextEncoder().encode(s);
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK_64;
  }
  return hash > SIGNED_64_MAX ? hash - WRAP : hash;
}
