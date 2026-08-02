# DB And FFM Simplification Implementation Plan

> Implement each task in a compiling commit. Use built-in subagents for
> independent DB and FFM work; do not invoke nested `codex exec` workers.

**Goal:** Remove duplicated TTL state and performance-only allocator machinery
while preserving DB semantics, stable native handles, ownership, mutation
atomicity, maxmemory accounting, page-backed FFM storage, and lifecycle safety.

**Baseline:** Commit `495aab3d`; `yierdis-db-memory` has 29,420 production Java
lines and `yierdis-memory-ffm` has 5,222. The selected 29-project JDK 25 reactor
passes before changes.

**JDK:** Every Java and Maven command uses:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH
```

## Global Constraints

- Keep Java 25, Netty, RESP2/basic RESP3, serial DB ownership, FFM storage,
  stable handles, TTL, maxmemory, transactions, commit streaming,
  backpressure, explicit ownership, page trim, and defrag.
- Preserve public APIs, supported commands, wire results, errors, object
  encodings, scan cursors, mutation ordering, and shutdown behavior.
- Do not introduce a compatibility adapter, deprecated path, per-object Arena,
  raw address API, or heap production backend.
- Do not change Maven module topology in stage 3.
- Conventional collections, scans, and eager Java copies are allowed.
- Performance and allocation thresholds are not acceptance criteria.
- No ignored, disabled, or quarantined test counts as completion.
- Review comments in every touched region under the repository's
  `write-comments` rules; remove stale implementation claims.

## Task 1: Record The Stage 3 Baseline And Design

**Files:**

- Add `docs/superpowers/specs/2026-07-31-db-ffm-simplification-design.md`.
- Add `docs/superpowers/plans/2026-07-31-db-ffm-simplification.md`.

- [x] Confirm the Stage 2 base and clean worktree.
- [x] Run the affected baseline reactor:

```bash
mvn -pl yierdis-memory/yierdis-memory-ffm,\
yierdis-db/yierdis-db-memory,\
yierdis-tests/yierdis-integration-tests -am test
```

- [x] Record 29 successful reactor projects, 106 FFM tests, 234 integration
  tests, and the two production line baselines.
- [x] Review the design against the DB and FFM contract surveys.
- [x] Commit the design and plan.

## Task 2: Make Entry Records The Only TTL State

**Delete:**

- `internal/expire/YierdisExpireIndex.java`
- `internal/expire/YierdisNativeExpireIndex.java`
- `internal/expire/PreparedTtlMutation.java`
- expire-index implementation tests whose only subject disappears

**Modify:**

- `YierdisDbKeyLifecycle.java`
- `internal/ledger/PreparedEntryMutation.java`
- `internal/expire/YierdisTtlOps.java`
- all string/keyspace/collection operation classes that currently pass a
  `PreparedTtlMutation`
- DB construction, storage components, resources, memory accounting, and tests

- [x] Add or retain behavior tests for missing/persistent TTL, immediate
  deletion, overflow clamp, lazy expiry, value overwrite, stale prepared
  mutations, synthetic expiry commits, abort, and close.
- [x] Change TTL lookup to read the current `EntryRecord.expireAtMillis`, mapping
  `-1` to absence.
- [x] Remove expire-index construction, ownership, close, clear, memory growth,
  and hash-maintenance registration.
- [x] Remove `PreparedTtlMutation` fields and parameters from
  `PreparedEntryMutation`. Commit publishes only the entry change.
- [x] Remove TTL sidecar preparation and abort paths from all data-type writes.
- [x] Set TTL mutation upper-bound growth to the actual entry/value growth; an
  in-place deadline update adds no physical allocation.
- [x] Remove package-private direct TTL write bypasses and migrate tests to
  `TtlWriteOps`.
- [x] Compile the DB module to find every stale dependency:

```bash
mvn -pl yierdis-db/yierdis-db-memory -am -DskipTests compile
```

- [x] Run:

```bash
mvn -pl yierdis-db/yierdis-db-memory -am \
  -Dtest=TtlLifecycleDirectOpsTest,ExpireIndexTest,\
MutationFaultInjectionTest,CommitAwareMutationFaultInjectionTest,\
PreparedMutationStorageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

- [x] Commit the single-state TTL mutation model.

## Task 3: Replace Active Expiry With A Bounded Directory Scan

**Modify:**

- `internal/expire/YierdisDbExpirationSupport.java`
- `YierdisDbKeyLifecycle.java`
- `YierdisDbMemoryReporter.java`
- `internal/ledger/DbMemoryAccounting.java`
- active-expiry, maxmemory, and memory-stat tests

- [x] Add tests that a small work budget advances across the keyspace over
  repeated calls, never removes unexpired entries, honors an explicit `now`,
  and does not mutate the directory during its traversal callback.
- [x] Retain one scan cursor in the cleaner and inspect a fixed maximum number
  of keys per maintenance call under the configured time limit.
- [x] Collect `(KeyHandle, EntryRecord)` candidates during scan, then reclaim
  after leaving the traversal.
- [x] Revalidate complete handle identity, current record/version, and deadline
  before reclamation; retain the batch start cursor on a pre-publication
  failure.
- [x] Derive `expireCount` from current entry records.
- [x] Keep expire-stat fields but return false/zero for removed table state.
- [x] Rewrite physical-growth tests to assert that deadline-only mutation does
  not invent committed memory and stays inside maxmemory.
- [x] Run DB TTL, memory accounting, and `TtlMaxmemoryTest` through the
  integration reactor.
- [x] Commit bounded active expiry and converged statistics.

## Task 4: Consolidate DB Composition And Context Views

**Candidates:**

- `YierdisDbConfig.java`
- `YierdisDbComponents.java`
- `YierdisDbStorageComponents.java`
- `YierdisDbComponentFactory.java`
- `YierdisDbInternals.java`
- `YierdisDbRuntimeInternals.java`
- `YierdisDbWrites.java`
- `YierdisDbLifecycleOps.java`
- `YierdisDbOwnedResources.java`
- component structure and architecture tests

- [x] Add/retain a concentrated owner-thread matrix for bind, rebind,
  cross-thread access, shutdown, closing, and closed states.
- [x] Retain tests with two independently bound mutation-context views used in
  interleaved order.
- [x] Use validated `DbEngineConfig` directly; calculate derived nanosecond and
  defrag values at construction without retaining a duplicate config object.
- [x] Replace the broad component bag with the minimum immutable groups consumed
  by `YierdisDb`, runtime state, maintenance, and close.
- [x] Put context in immutable write/lifecycle views and delete the
  method-by-method contextual forwarding class.
- [x] Collapse the single-implementation internals interface where fault tests
  do not need substitution; keep direct named helpers for mutation execution,
  expiry reclamation, and eviction.
- [x] Delete unused callbacks, accessors, legacy snapshot paths, direct memory
  adjustment hooks, and structure-only guards.
- [x] Replace structure assertions with owner, cleanup, factory-only creation,
  and independent-context behavior assertions.
- [x] Run all `yierdis-db-memory` tests and architecture tests.
- [x] Confirm the DB production source has fallen by at least 1,700 lines from
  the baseline before closing stage 3.
- [x] Commit DB composition simplification.

## Task 5: Freeze FFM Lifecycle Gaps And Merge The Backend

**Modify:**

- `YierdisFfmStableMemoryBackend.java`
- allocator ownership and behavior tests

**Delete:**

- `YierdisStableNativeAllocator.java` after all implementation is absorbed

- [x] Add a regression test for closing with an active epoch and no live
  object: close reports the leak and still returns runtime regions to baseline.
- [x] Move allocator identity, table/page owners, epochs, scopes, counters,
  defrag, views, and close implementation into the public backend.
- [x] Keep owner-bound external regions and package-private validator injection.
- [x] Migrate tests to construct the public backend directly.
- [x] Preserve the owner check before local-handle decoding.
- [x] Run all 106+ FFM tests.
- [x] Commit backend convergence.

## Task 6: Use Explicit Epoch Scopes And Retired Blocks

**Modify:**

- `YierdisFfmStableMemoryBackend.java`
- quarantine and epoch tests

**Delete:**

- `YierdisNativeEpochManager.java`

- [x] Replace manager arrays with an active epoch-scope list and monotonic
  epoch counter.
- [x] Replace retired-block parallel arrays with
  `RetiredBlock(YierdisNativeBlock block, long epoch)` records.
- [x] Reclaim only after every observing epoch is closed and the pin count is
  zero.
- [x] On successful reclaim, close the block and decrement reserved/quarantine
  accounting exactly once.
- [x] Verify `COMMAND`, `SNAPSHOT`, defrag, realloc, multiple pins, and
  idempotent epoch close.
- [x] Commit explicit epoch and retired-block ownership.

## Task 7: Replace The Native Page Directory And Intrusive Indexes

**Modify:**

- `YierdisNativePageAllocator.java`
- `YierdisNativeBlock.java`
- `YierdisNativePageAllocatorStats.java` as needed
- page, trim, scope, and accounting tests

**Delete:**

- `YierdisNativePageDirectory.java`

- [x] Introduce one page-ID registry, reusable-ID set, next ID, and creation
  sequence as the only page descriptor state.
- [x] Allocate small blocks by scanning matching non-full pages.
- [x] Keep size classes, 64 KiB pages, medium/large span rounding, alignment,
  capacity, and page-class validation unchanged.
- [x] Keep at most one warm empty page per size class and trim empty pages in
  deterministic page-ID order under inspection/byte/time budgets.
- [x] Implement the scope checkpoint with creation sequence and an eager copy
  of reusable IDs; abort restores committed memory and ID validity.
- [x] Derive stats and growth estimates conservatively from the registry.
- [x] Rewrite reflection/no-allocation tests as behavior/accounting tests.
- [x] Run page allocator, allocation scope, maxmemory trim, and FFM DB trim
  integration tests.
- [x] Commit the page registry rewrite.

## Task 8: Simplify Object Table Segments And Scope Rollback

**Modify:**

- `YierdisNativeObjectTable.java`
- `YierdisNativeObjectTableStats.java`
- object table and allocation scope tests

- [x] Replace segment capacity arrays and availability queues with one
  exact-length segment array.
- [x] Scan segments for a reusable slot before appending a new segment.
- [x] Record only baseline segment count in a scope checkpoint.
- [x] On abort, free tracked handles, close new empty tail segments, and truncate
  the array without rolling generations backward.
- [x] Preserve 4,096-slot FFM segments, the 36-byte metadata layout, state
  transitions, cursor order, capacity checks, kind/domain validation, and
  generation retirement.
- [x] Compact stats into a package-private record if it reduces code without
  exposing mutable arrays.
- [x] Delete COW/no-copy/no-allocation representation tests and retain native
  metadata, lazy segment, rollback baseline, and bookkeeping upper-bound tests.
- [x] Run the full FFM module.
- [x] Commit object-table and scope simplification.

## Task 9: Remove Dead FFM Access Shapes And Update Documentation

**Delete where unused:**

- `YierdisFfmSpan.java`
- region/block span helpers
- test-only block properties and close variants
- allocation-free helper implementations with equivalent validated API defaults

**Modify:**

- `YierdisFfmRegion.java`
- `YierdisNativeBlock.java`
- stable object view implementation
- FFM tests
- `docs/project-docs/native-memory-runtime.md`
- `docs/project-docs/native-allocator-and-handles.md`
- `docs/project-docs/ttl-and-expiration-lifecycle.md`
- `docs/project-docs/db-internals.md`
- architecture policies and guards

- [x] Perform complete range and writability validation before delegating
  multi-byte writes to defaults.
- [x] Use one direct native block-copy helper for realloc and defrag.
- [x] Remove stale comments about the expire index, page directory, COW, and
  allocation-free paths.
- [x] Keep architecture guards for public API, module ownership, FFM imports,
  stable handles, and composition roots; delete guards for removed private
  field/class shapes.
- [x] Confirm `yierdis-memory-ffm` production Java has fallen by at least 1,000
  lines from the 5,222-line baseline.
- [x] Commit dead-code and documentation convergence.

## Task 10: Stage 3 Verification And Review

- [x] Run focused FFM tests:

```bash
mvn -pl yierdis-memory/yierdis-memory-ffm -am test
```

- [x] Run focused DB tests:

```bash
mvn -pl yierdis-db/yierdis-db-memory -am test
```

- [x] Run architecture and integration tests:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests,\
yierdis-tests/yierdis-integration-tests -am test
```

- [x] Run the full reactor:

```bash
mvn test
```

- [x] Search for deleted concepts and stale imports:

```bash
rg 'YierdisExpireIndex|YierdisNativeExpireIndex|PreparedTtlMutation|\
YierdisStableNativeAllocator|YierdisNativePageDirectory|YierdisNativeEpochManager|\
YierdisFfmSpan' --glob '!docs/superpowers/**'
```

- [x] Compare production LOC to `495aab3d` and require at least 2,700 net lines
  removed across DB-memory and memory-ffm.
- [x] Run two independent built-in subagent reviews: one for correctness and
  lifecycle risks, one for simplification completeness and stale architecture
  constraints.
- [x] Fix every confirmed finding and rerun the narrowest affected tests plus
  the final full reactor.
- [x] Commit the final Stage 3 review fixes and leave the worktree clean.
