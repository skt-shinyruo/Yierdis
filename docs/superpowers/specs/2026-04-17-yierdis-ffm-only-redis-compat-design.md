# Yierdis FFM-Only Redis Compatibility Design

**Date:** 2026-04-17

**Status:** Approved for planning

## Context

Yierdis already provides a working single-node in-memory KV server built on Java 25, Netty, a custom wire protocol (`Custom Protocol v1`), a single-thread command executor, and an off-heap/FFM-backed storage path.

The current codebase still reflects an earlier teaching/demo orientation:

- Some supported commands intentionally preserve simplified semantics.
- Several data structure implementations keep dual-path logic for heap and FFM storage.
- Documentation explicitly frames parts of the system as teaching-oriented rather than compatibility-oriented.

The next phase changes that direction. The project will remain a single-node in-memory server using `Custom Protocol v1`, but it will no longer accept semantic simplification for teaching purposes. The implementation direction is to converge on an FFM-only storage kernel while driving supported commands toward Redis-compatible behavior.

## Recorded Decisions

The following decisions are normative. They are not optional guidance.

1. Yierdis will continue to use `Custom Protocol v1`. This phase does not adopt RESP and does not target `redis-cli` or existing Redis client compatibility at the protocol layer.
2. Yierdis will remain a single-node in-memory server in this phase. Persistence, replication, clustering, ACL, TLS, Lua, and PubSub remain out of scope.
3. The supported data-structure scope for this phase is:
   `String`, `Hash`, `List`, `Set`, `ZSet`, `Bitmap`, `HLL`, and transactions via `MULTI/EXEC/DISCARD`.
4. The project must no longer simplify semantics for teaching reasons. This prohibition applies to existing functionality and to all future work.
5. FFM/off-heap storage becomes the only data-path implementation for user data. Heap-backed alternatives must be removed rather than maintained in parallel.
6. The server keeps its current single-thread DB access model. FFM-only storage does not justify relaxing owner-thread enforcement.
7. Any supported command that behaves differently from Redis must be tracked explicitly as an incompatibility to fix or as an intentional out-of-scope item. It must not be justified as a teaching simplification.

## Goals

- Preserve the current module boundaries and operational shape of the repository while upgrading semantics and storage internals.
- Converge the storage engine to a single FFM-only implementation path.
- Make supported commands behave as closely as practical to Redis semantics.
- Keep current strengths intact: bounded execution, backpressure, single-thread command semantics, and observability.
- Replace implicit compatibility drift with explicit compatibility records and regression tests.

## Non-Goals

- Switching to RESP.
- Making Yierdis a drop-in network-level replacement for Redis clients.
- Adding persistence, replication, clustering, ACL, TLS, Lua, or PubSub in this phase.
- Replacing the Netty-based server or rewriting the repository from scratch.
- Relaxing single-thread DB ownership in pursuit of parallel mutation.

## Current-State Baseline

The existing repository structure is already close to the desired long-term shape:

- `yierdis-server` owns Netty bootstrap, connection lifecycle, backpressure, and command execution scheduling.
- `yierdis-client` owns the CLI and the custom protocol client.
- `yierdis-protocol` owns protocol codecs and models for `Custom Protocol v1`.
- `yierdis-core-command` owns command dispatch and command-family registration.
- `yierdis-core-runtime` owns multi-DB instance assembly, runtime access, maintenance, and instance-level observability.
- `yierdis-core-db` owns the storage engine, object encodings, keyspace, expire index, TTL, maxmemory, and per-type operations.

The design does not replace these boundaries. It sharpens them and removes transitional heap-era implementation branches inside the DB layer.

## Target Architecture

### Protocol and Entry Points

The wire protocol remains `Custom Protocol v1`.

- Request framing stays `<len>:<json>\n`.
- Reply framing stays NDJSON with success/error envelopes.
- Protocol-level error recovery stays best-effort recoverable as it is today.

This phase does not redesign the protocol. The protocol is already decoupled from command execution and does not block semantic or storage upgrades.

### Command Layer as Compatibility SSOT

`yierdis-core-command` remains the semantic entry point for supported commands.

Its responsibilities are strengthened:

- Command parsing rules, arity checks, option parsing, and reply shaping should match Redis semantics for supported commands as closely as possible.
- Type errors, integer parsing failures, floating-point parsing failures, nil/null behaviors, and transaction queueing behavior must stop drifting per command family.
- The command layer must no longer tolerate a command being "supported but simplified for teaching". If it is supported, it is expected to target Redis semantics.

Server-only operational commands such as `INFO`, `STATS`, and protocol-facing metadata can continue to live in `yierdis-server`, but their output must not misrepresent compatibility posture.

### Runtime and Threading

`yierdis-core-runtime` remains responsible for instance assembly and multi-DB orchestration.

Key invariants remain:

- DB access is bound to a single owner thread.
- Netty I/O threads enqueue work; the command executor thread performs mutation and reads that depend on DB state.
- Maintenance work such as expiration cleanup still runs through the bound DB thread.

This model remains the correct fit for the current project because it aligns command semantics, simplifies FFM access discipline, and matches the current execution architecture.

### FFM-Only Storage Kernel

`yierdis-core-db` becomes an FFM-only kernel.

This means:

- Keyspace storage remains off-heap and backed by FFM-native structures.
- Expiration index remains off-heap and backed by FFM-native structures.
- User values are stored off-heap only. There is no heap fallback payload path.
- Transitional branches such as `if (memoryRuntime != null) ... else ...` must be removed from per-type implementations.
- Heap objects may still exist for control flow, metadata, and protocol execution records, but not as authoritative storage for user data.

## Storage and Memory Model

### Off-Heap Ownership Rules

The memory model is based on explicit off-heap ownership.

- The instance owns a `YierdisFfmMemoryRuntime`.
- Per-type blob stores own value payload regions and release them explicitly.
- Keyspace and expire index own key references explicitly.
- Replacements, deletions, expiry, transaction rollback paths, and type overwrites must all release old payloads deterministically.

Leak freedom is not a best-effort property. It is part of the contract.

### Allowed Heap Usage

Heap allocation is still allowed for:

- Protocol decode intermediates.
- Command argument containers.
- Connection/session state.
- Scheduling state and counters.
- Temporary comparison buffers and test helpers.

Heap allocation is not allowed for authoritative copies of user keys or values after ingestion into the DB.

### Memory Accounting

`maxmemory`, `MEMORY USAGE`, `MEMORY STATS`, `INFO`, and `STATS` must reflect the FFM-only world.

The accounting model should treat:

- Value payload bytes.
- Keyspace structural bytes.
- Expire-index structural bytes.
- Per-structure metadata bytes.
- Any FFM-managed encoding-specific backing storage.

as first-class memory usage, rather than partially reporting heap estimates while treating off-heap usage as secondary.

## Data Structure Convergence Plan

### Keyspace and Expire Index

`YierdisFfmKeyspace` and `YierdisFfmExpireIndex` remain the keyspace foundation.

Expected direction:

- They become the only key/index implementations.
- Any code paths still assuming heap keys must be removed.
- Canonical key ownership and key-handle access rules remain explicit and internal.

### Strings, Bitmaps, and HLL

Strings remain a single Redis-like logical family with multiple encodings.

Required encodings:

- Integer-encoded strings for canonical integer values.
- Embedded/raw string encodings, both backed by FFM-managed storage.
- Bitmap operations as string bit operations, not as a separate top-level storage family.
- HLL as a specialized string-backed encoding with FFM-managed payloads.

Required outcome:

- `SET/GET/APPEND/INCR/DECR/SETBIT/GETBIT/BITCOUNT/PFADD/PFCOUNT/PFMERGE` operate without heap-backed value fallback.
- Type overwrite and expiry paths release prior payloads correctly.

### Hashes

Hashes keep the Redis-like upgrade model:

- Compact packed representation for small hashes.
- Hashtable-like representation after thresholds are crossed.

But both representations must be FFM-backed. The packed path and upgraded path must share explicit ownership and release rules.

### Lists

Lists keep the Redis-like upgrade model:

- Packed/listpack-like representation for small lists.
- Quicklist-like node representation after thresholds are crossed.

Both representations must be FFM-backed. There must be no retained heap list payload path.

### Sets

Sets keep the Redis-like upgrade model:

- Integer-set representation for small integer-only sets.
- Hash-set representation after cardinality or member-type thresholds are crossed.

Both paths must be FFM-backed only.

### ZSets

ZSets keep the Redis-like upgrade model:

- Packed representation for small sorted sets.
- Dictionary plus skiplist-equivalent representation after thresholds are crossed.

Both paths must be FFM-backed only. Ordering, score parsing, lexicographic tie behavior, and range/removal semantics should align with Redis behavior for supported subcommands.

## Semantic Compatibility Policy

### Compatibility Standard

The compatibility target is:

- If a command is supported, its behavior should track Redis semantics as closely as practical.
- If a subcommand or edge case is not yet implemented, that gap must be explicit.
- A weaker behavior may not be retained because it is easier to explain or easier to demo.

### Compatibility Ledger

The project must maintain a compatibility ledger document at:

- `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`

Every supported command and significant subcommand should be placed in one of three states:

- `Compatible`
- `Known incompatibility to fix`
- `Explicitly out of scope`

Forbidden state:

- `Simplified for teaching`

Additional rule:

- No new supported command behavior may be merged unless the ledger entry for that command family is added or updated in the same change.

The ledger should cover:

- Reply value shape under `Custom Protocol v1`
- Nil/null behavior mapping
- Error messages where exact wording matters
- Numeric parsing and overflow behavior
- TTL/expiration edge cases
- Transaction queueing and `EXECABORT` behavior
- Multi-key command ordering and duplicate-key handling
- Encoding-observable commands such as `OBJECT ENCODING` and `MEMORY USAGE`

### Transaction Policy

This phase includes transactions only for:

- `MULTI`
- `EXEC`
- `DISCARD`

The goal is not to expand to full Redis transaction coverage in this phase, but the supported subset must behave like Redis for queueing, abort conditions, and replay semantics. Unsupported transaction features such as `WATCH` remain out of scope until explicitly planned.

## Error Handling

Error handling must be deterministic and compatibility-oriented.

- Protocol errors stay protocol-scoped and recoverable where safe.
- Command errors should not vary by storage encoding.
- Wrong-type errors should be consistent across all type families.
- FFM allocation failures must surface as command-visible OOM failures without leaking partially allocated native state.
- Background cleanup and eviction must not silently corrupt memory accounting or leave detached payloads live.

## Observability

The current observability surface is worth keeping, but it must be updated to match the new compatibility posture.

Required changes:

- `INFO`, `STATS`, and memory reporting should stop describing the system as teaching-oriented.
- Metrics and memory statistics must describe FFM-backed storage truthfully.
- Any compatibility-relevant counters or diagnostics added during migration should remain cheap enough for production-style use in a single-node deployment.

## Verification Strategy

This design requires three layers of verification.

### Differential Semantics

Add differential tests that compare Yierdis command behavior against a real Redis server at the semantic level.

Important note:

- The comparison is semantic, not protocol-byte-level, because Yierdis uses `Custom Protocol v1`.
- The harness should normalize protocol representation differences and compare logical outcomes.

### Native Memory Safety

For every data structure family, add tests covering:

- Insert/update/delete lifecycle
- Expiry cleanup
- Type overwrite
- Encoding upgrade transitions
- Shutdown with zero live-region leaks

### Compatibility Regression

Add command-family regression suites for:

- String and bitmap behavior
- TTL and expiration semantics
- Hash/list/set/zset behavior
- HLL behavior
- Transaction queueing and replay behavior
- Memory accounting and introspection behavior

## Migration Plan

The recommended implementation order is incremental and repository-local.

### Phase 1: Establish compatibility policy and tests

- Add the compatibility ledger.
- Add differential semantic tests for currently supported commands.
- Remove or rewrite documentation that still frames supported incompatibilities as teaching simplifications, including the current README positioning.

### Phase 2: Converge the DB kernel to FFM-only

- Refactor `YierdisDb` construction and internals so FFM is the only storage implementation path.
- Remove heap-backed branches from `YierdisObject`.
- Remove heap-backed branches from each type family one by one.

### Phase 3: Tighten command semantics

- Fix command-family behavior against the compatibility ledger in priority order:
  `String/Bitmap`, then `TTL`, then `Hash/List/Set/ZSet`, then `HLL`, then transactions.

### Phase 4: Reconcile observability and docs

- Update memory statistics and introspection to match the FFM-only kernel.
- Update README and related docs to reflect the new project posture and scope.

## Risks

- Removing heap fallbacks will expose places where command semantics currently depend on mixed representations.
- Memory accounting may drift during intermediate states unless each type family is migrated under test.
- Differential testing against Redis will surface more incompatibilities than the current docs admit; that is expected and desirable.
- Some exact Redis behaviors may need careful interpretation under `Custom Protocol v1`, especially around nil/error representation. Those cases must be normalized explicitly in the compatibility ledger rather than left implicit.

## Acceptance Criteria

This design is considered implemented only when all of the following are true:

- Supported functionality is no longer documented or justified as simplified for teaching.
- The DB storage engine has no authoritative heap-backed value or key paths.
- Heap/FFM dual branches are removed from supported data-structure implementations.
- A compatibility ledger exists and is kept current.
- Differential semantic tests exist for the supported command surface.
- Native memory leak tests cover all supported data-structure families and pass.
- `INFO`, `STATS`, and memory-reporting surfaces describe the system consistently with the FFM-only design.

## Implementation Guidance

The safest path is not a rewrite. It is controlled convergence on the existing architecture:

- Keep the current protocol and server structure.
- Keep the current single-thread execution model.
- Keep the current module boundaries.
- Replace transitional storage branches with one FFM-only implementation path.
- Use tests and the compatibility ledger to drive semantic tightening rather than ad hoc fixes.
