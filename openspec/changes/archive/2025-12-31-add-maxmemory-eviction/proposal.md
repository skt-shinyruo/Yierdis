# Change: Add maxmemory + eviction (Redis-like memory management)

## Why
Yierdis is intended for learning and local demos, but today it does not have a Redis-style memory management story:

- There is no **maxmemory** guardrail, so memory growth is unbounded and failures are “JVM/OS decides”.
- There is no **eviction policy** (LRU/random/noeviction), so it is hard to demo how Redis behaves under pressure.
- Internal **encoding upgrades** (packed → large encodings) are implemented, but there is no easy way to observe them
  during teaching.

This change adds a small, explicit memory model (approximate for on-heap, exact for off-heap) and a minimal eviction
engine so the server can behave more like Redis under memory pressure while keeping the code small and readable.

## What Changes
- Add server configuration knobs:
  - `--maxmemoryBytes <bytes>` (default: `0` = unlimited)
  - `--maxmemoryPolicy noeviction|allkeys-random|allkeys-lru` (default: `noeviction` when maxmemory is enabled)
  - `--maxmemorySamples <n>` (default: small constant, e.g. 5)
- Add a DB-level **approximate memory accounting** model:
  - Maintain an approximate “used bytes” total for on-heap structures (byte arrays + container capacities).
  - Include off-heap allocator accounting when an off-heap backend is enabled (`usedBytes()` is already tracked).
- Add an **eviction loop** that runs after write commands when maxmemory is enabled:
  - Prefer deleting expired keys first (reuse existing expiration cleanup pattern).
  - Evict keys according to the selected policy until memory is under the limit or eviction is impossible.
- Add minimal **introspection commands** for teaching:
  - `MEMORY USAGE <key>`: returns the approximate bytes used by a key’s value (and optionally key overhead).
  - `OBJECT ENCODING <key>`: returns the internal encoding name (e.g. `embstr`, `raw`, `int`, `listpack`, `hashtable`, `intset`, `skiplist`, `quicklist`).

**Non-breaking by default**: when `--maxmemoryBytes` is `0`, behavior remains unchanged.

## Scope
### In scope
- `yierdis-server/src/main/java/yier/bubu/redis/ServerConfig.java`: parse new CLI flags.
- `yierdis-server/src/main/java/yier/bubu/redis/db/**`:
  - memory estimation helpers,
  - per-key access metadata (LRU clock / last access),
  - eviction loop integrated with existing TTL lazy deletion and cleanup sampling.
- `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`: wire `MEMORY` / `OBJECT` commands and call “enforce maxmemory” on write paths.
- Unit tests for memory accounting and eviction behavior (deterministic and small).
- README updates documenting flags and demo examples.

### Out of scope (non-goals)
- Exact Redis allocator accounting (jemalloc / RSS / fragmentation); on-heap accounting is approximate by design.
- New persistence (AOF/RDB), replication, clustering, ACL/auth, transactions, Lua scripting.
- Full Redis `CONFIG SET/GET` surface; only CLI flags are required for this change.

## Impact
- Adds new optional server flags and two new user-visible commands (`MEMORY USAGE`, `OBJECT ENCODING`).
- When maxmemory is enabled, some write commands may:
  - evict keys, or
  - fail with an OOM-style error under `noeviction`.
- Adds small per-key metadata for eviction (e.g. a clock or timestamp).

## Open Questions (need explicit approval)
1. **Accounting scope**: should maxmemory apply to:
   - on-heap estimates only, or
   - on-heap estimates + off-heap allocator used bytes (recommended)?
2. **Introspection detail**: should `MEMORY USAGE` include:
   - value bytes only (simpler), or
   - key + value + container overhead (more Redis-like but more “approximate”)?
3. **LRU clock source**: should LRU be updated on:
   - every key access (reads + writes; recommended), or
   - writes only (less overhead, less Redis-like)?

## Risks & Mitigations
- **Approximate accounting confusion**: document the model explicitly and keep `MEMORY USAGE` semantics stable.
- **Eviction non-determinism**: tests should control inputs and use `allkeys-random` only when randomness is seeded or avoided.
- **Complexity creep**: limit policies to `noeviction`, `allkeys-random`, and `allkeys-lru` (sampling-based).

