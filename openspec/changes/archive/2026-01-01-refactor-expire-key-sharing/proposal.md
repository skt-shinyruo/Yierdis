# Change: Refactor expires index to share canonical key bytes with store

## Why
Redis uses a single canonical key object for both the main keyspace dictionary and the expires dictionary. This lets the
TTL index reference keys without duplicating key memory and keeps the relationship “one key object, multiple indexes”.

Yierdis currently stores TTL entries in a separate `ByteArrayKeyspace<Long>` (`expires`) using the command-provided
`byte[]` as the key. Since request decoding allocates new `byte[]` for each argument, the same logical key can exist as:
- one `byte[]` stored in `store`, and
- a different `byte[]` stored in `expires`,

even though they compare equal by contents.

This diverges from Redis’ implementation model and increases heap usage for TTL-heavy workloads.

## What Changes
- Add a keyspace primitive to retrieve the **canonical stored key reference** for a logical key.
- Ensure `YierdisDb` stores expiration entries using the canonical key reference from `store`, so `store` and `expires`
  share the same `byte[]` instance for a key whenever possible.
- (Optional repair) If an existing expires entry is keyed by a non-canonical `byte[]`, move it to the canonical key when
  updating TTL, so the running process converges to the Redis-style invariant.

External RESP2 semantics and supported command set MUST remain unchanged.

## Scope
### In scope
- Internal refactor only: key identity sharing between `store` and `expires`.
- Unit tests validating key reference sharing behavior.

### Out of scope (non-goals)
- Off-heap key storage.
- RESP decoder changes (still allocates `byte[]` per arg).
- New commands or behavior changes.

## Impact
- Affected code:
  - `yierdis-server/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Tests: add a targeted test to assert `expires` uses the same `byte[]` reference as `store` for the same logical key.

## Risks & Mitigations
- **Increased CPU due to extra lookups**: keep canonicalization O(1) in the hash table; repair only when a mismatch is detected.
- **Rehash interaction**: the canonical lookup must be compatible with incremental rehashing and remain allocation-free.

