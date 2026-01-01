# Change: Refactor encoding heuristics to be closer to Redis

## Why
Yierdis already models Redis-style logical types (`STRING/LIST/HASH/SET/ZSET`) and “small/packed → large” encodings,
but there are still important gaps vs Redis that affect both learning value and performance characteristics:

- **List encoding heuristics** are currently driven by entry/element thresholds that are only an approximation of Redis’
  `quicklist` + `list-max-listpack-size` behavior.
- **Set encoding** includes an intermediate `listpack`-like representation which is convenient in Java but differs from
  Redis’ `intset → hashtable` model.
- **Packed iteration** in some code paths can devolve into repeated index scans (O(n²) patterns) even when the data is
  small enough to remain packed.

This change aims to make the on-heap encodings and upgrade heuristics closer to Redis’ model while keeping the project
minimal and keeping external RESP2 semantics unchanged.

## What Changes
- Introduce a single place to define **Redis-aligned encoding thresholds** (names and default values modeled after
  Redis configuration knobs).
- Update `ListValue` heuristics to size/byte-based rules closer to Redis `quicklist` node sizing (rather than
  primarily entry-count-based rules).
- Update `SetValue` to follow Redis’ encoding set more closely (remove the intermediate set listpack encoding; keep
  binary-safe “canonical integer” checks for `intset`-style storage).
- Improve packed iteration for range-style operations so packed scans are sequential (cursor/offset-walk) instead of
  repeated “index → offset” rescans.

## Scope
### In scope
- Internal refactors in `yierdis-server/src/main/java/yier/bubu/redis/db/**`:
  - list/hash/set/zset value containers, upgrade heuristics, and packed iteration code paths.
- Unit tests that validate:
  - upgrade thresholds,
  - order semantics (where ordered),
  - and invariants under deletes/updates.

### Out of scope (non-goals)
- Exact byte-for-byte Redis listpack compatibility.
- New Redis commands, persistence, replication, clustering, or off-heap migration.
- Multi-threaded DB execution (keeps the single-owner-thread rule).

## Impact
- Affected code: `ListValue`, `SetValue`, `ZSetValue`, and shared threshold/config plumbing.
- Risk: subtle behavior changes in internal encoding selection (e.g., when a value upgrades), which may affect output
  ordering for unordered types (sets/hashes). Redis does not guarantee ordering there, so tests must be order-insensitive.

## Open Questions (need explicit approval)
1. **Null vs empty bulk strings inside collections**: current listpack-like storage preserves `null` vs empty element
   (see `ListValueTest`). Redis does not have “null elements” as a stored value. This proposal assumes we keep current
   external behavior and only align heuristics/structures.
2. **Expose thresholds via `ServerConfig` vs keep internal constants**: Redis makes these tunable. For Yierdis we can:
   - keep them as internal constants (minimal), or
   - add optional CLI flags (more Redis-like).

## Risks & Mitigations
- **Regression in packed/upgrade logic**: add targeted tests for boundary cases (threshold ±1, delete-to-empty, update-in-place).
- **Complexity creep**: limit changes to thresholds and iteration mechanics needed for the current command subset.

