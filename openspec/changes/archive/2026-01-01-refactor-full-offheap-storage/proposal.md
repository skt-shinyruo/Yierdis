# Change: Refactor storage to full off-heap (Unsafe)

## Why
The current implementation stores all keys, values, and index structures on the JVM heap (primarily `byte[]` and Java objects).
For large datasets and write-heavy workloads this increases GC pressure and can introduce latency variability.

This change introduces a fully off-heap storage engine (Unsafe-based) to make memory usage and latency behavior closer to Redis'
native model (explicit allocation, deterministic free) while keeping the external RESP2 semantics unchanged.

## What Changes
- Replace in-heap key/value and index structures with off-heap equivalents.
- Introduce an Unsafe-based off-heap allocator with explicit lifecycle management.
- Change internal APIs so commands and RESP writers operate on byte slices (ptr/len) rather than copying into new `byte[]`.
- Add explicit memory limits, metrics, and safety rails (leak detection in tests, hard caps for user-controlled allocations).

**BREAKING (internal)**: storage internals, encodings, and many `db/` classes will be replaced. External protocol behavior SHOULD remain
compatible with the current supported command subset.

## Scope
### In scope
- Off-heap storage for:
  - Keyspace dictionary (keys + values)
  - TTL index (expires dict)
  - String encodings (INT/EMBSTR/RAW analogue)
  - Hash/List/Set/ZSet internal structures
  - ZSet skiplist nodes and member index
- Dedicated allocator API and a minimal slab/free-list implementation.
- Update protocol write path to avoid heap copies on replies where possible.

### Out of scope (non-goals)
- Persistence (RDB/AOF), replication, clustering, ACL/TLS.
- Perfect Redis memory layout fidelity; this is a Java approximation.
- Multi-threaded DB access (keeps current single-owner-thread rule).

## Impact
- Affected code: most of `src/main/java/yier/bubu/redis/db/**`, plus `protocol/` writer path and fast command processor write helpers.
- Tests: existing unit tests must be updated; new tests for allocator correctness/leaks are required.
- Ops: runtime needs explicit configuration for max off-heap memory and a clean shutdown path to free it.

## Risks & Mitigations
- **Native memory leaks / use-after-free**: require explicit ownership rules, central allocator, and test-time leak assertions.
- **Complexity explosion**: phase the migration and keep each step verifiable with unit tests.
- **Performance regression**: keep hot-path operations allocation-free where possible; measure with microbenchmarks (optional).

