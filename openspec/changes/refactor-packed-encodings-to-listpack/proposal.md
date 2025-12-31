# Change: Refactor packed encodings to listpack-like storage

## Why
Yierdis currently models Redis-style internal encodings (e.g. “packed”/“listpack” vs “hashtable”/“skiplist”), but most
“packed” variants still store each element as its own Java object (`byte[]`, `byte[][]`, `double[]` + `byte[][]`, etc).
This diverges from Redis’ core memory layout strategy where small composite values are stored in a single contiguous
buffer (listpack / quicklist nodes), which reduces:
- object count (GC pressure),
- pointer chasing (CPU cache locality),
- overhead per element.

If the goal is implementation alignment with Redis (not only command behavior), the packed representations need to
become truly contiguous byte storage.

## What Changes
- Introduce a minimal **listpack-like** byte container in `yierdis-server` for small collections:
  - binary-safe, length-prefixed entries,
  - optimized for “small N” (similar to Redis’ listpack use cases).
- Migrate the packed encodings of core composite types to use this listpack container:
  - `HashValue` packed form (fields/values stored contiguously)
  - `SetValue` listpack form (members stored contiguously)
  - `ListValue` packed form + quicklist nodes (nodes store listpack rather than `byte[][]`)
  - `ZSetValue` packed form (member/score pairs stored contiguously, preserving ordering rules)
- Keep the existing upgrade paths (packed → HT/skiplist/quicklist) and thresholds, but make the packed side closer to
  Redis’ memory model.

External RESP2 semantics and supported command set MUST remain unchanged.

## Scope
### In scope
- On-heap listpack-like storage for packed encodings (no new persistence, replication, or clustering features).
- Unit tests covering listpack behavior and the migrated packed encodings.
- Small refactors in `db/` to keep encoding metadata accurate.

### Out of scope (non-goals)
- “Full off-heap storage” migration (covered by a separate proposal).
- Exact byte-for-byte Redis listpack compatibility (this is a Java approximation).
- New Redis commands or new externally-visible features.

## Impact
- Affected code: primarily `yierdis-server/src/main/java/yier/bubu/redis/db/**` (composite values + encoding logic).
- Tests: new unit tests and updates to existing tests where internal behavior changes (external results should not).
- Performance: expected reduction in allocation rate and object count for hashes/lists/sets/zsets that stay in packed form.

## Risks & Mitigations
- **Correctness bugs in packed operations**: add focused tests per value type + “round-trip” invariants.
- **Performance regressions**: keep listpack use limited to small collections; upgrade early if operations become costly.
- **Complexity creep**: implement only minimal listpack operations needed by current command subset.

