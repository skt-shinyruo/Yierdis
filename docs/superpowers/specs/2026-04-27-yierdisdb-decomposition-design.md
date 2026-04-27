# YierdisDb Decomposition Design

## Summary

This design continues the ongoing `YierdisDb` decomposition by reducing
`YierdisDb` to a runtime-facing database facade and moving construction, glob
matching, memory estimation, and maxmemory policy parsing into focused package
classes.

After this change, `YierdisDb` still implements `RuntimeDbEngine` and remains the
stable concrete DB entry point for existing callers. It no longer owns unrelated
utility algorithms or the full object graph construction procedure directly.

## Problem Statement

`YierdisDb` has already delegated command families to dedicated classes such as
`YierdisStringOps`, `YierdisHashOps`, `YierdisListOps`, `YierdisSetOps`,
`YierdisZSetOps`, `YierdisHllOps`, `YierdisTtlOps`, and `YierdisKeyspaceOps`.
The architecture guard also prevents those command APIs from moving back into the
main DB class.

The remaining issue is that `YierdisDb` is still too broad. It currently combines:

- FFM runtime and foreign allocator resolution
- keyspace and expire-index construction
- owned-resource wiring
- maxmemory config validation and policy parsing
- memory ledger construction
- mutation executor construction
- expiration, maxmemory, lifecycle, memory, reads, and writes facade construction
- LRU clock ownership
- key lifecycle delegation methods
- memory estimation utilities
- Redis-style glob matching for `KEYS` and `SCAN`
- a private `DbInternals` adapter for ops classes

This makes the class difficult to review because unrelated reasons to change it
still converge on the same file. It also makes future architecture guards harder
to express: the class is already a facade, but it still contains implementation
details that are not part of the facade responsibility.

## Goals

- Keep the public construction surface of `YierdisDb` compatible for current
  tests and runtime wiring.
- Move DB component assembly into a dedicated component factory.
- Move Redis glob matching out of `YierdisDb` and into a focused matcher class.
- Move memory size estimation out of `YierdisDb` and consolidate repeated
  estimation helpers used by ops classes.
- Remove duplicate maxmemory policy parsing from `YierdisDb` and make parsing
  behavior explicit in one package-level helper.
- Reduce `YierdisDb` to runtime facade, thread guard, lifecycle entry points,
  LRU touch coordination, and delegation to components.
- Add architecture guards that prevent the extracted responsibilities from
  drifting back into `YierdisDb`.
- Preserve maxmemory, TTL, off-heap key, scan, and command behavior.

## Non-Goals

- No command semantics changes.
- No protocol, command registration, or client API redesign.
- No replacement of the FFM storage implementation.
- No new maxmemory policies.
- No attempt to remove all package-private access in one change.
- No broad cleanup of unrelated duplicated helpers outside the DB package unless
  they are directly part of the extracted memory estimation path.

## Current Facts

`YierdisDb` is 898 lines and starts at
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`.

Important existing collaborators:

- `YierdisDbMemoryLedger` owns write reservations and local maxmemory checks.
- `YierdisDbMutationExecutor` owns mutation reservation/commit/rollback flow.
- `YierdisDbExpirationSupport` owns active expiration cleanup.
- `YierdisDbMaxmemorySupport` owns local eviction and candidate sampling.
- `YierdisDbKeyLifecycle` owns key lookup, TTL removal, object removal, and touch
  callbacks.
- `YierdisDbMemoryReporter` owns memory stats and maxmemory usage reporting.
- `YierdisDbReads`, `YierdisDbWrites`, `YierdisDbMemoryOps`,
  `YierdisDbExpirationManager`, and `YierdisDbLifecycleOps` expose stable ops
  facades.
- `YierdisDbArchitectureGuardTest` already guards against command APIs,
  reporting APIs, raw container exposure, and resource lifetime logic moving back
  into `YierdisDb`.
- `yier.bubu.redis.ops.MaxmemoryPolicy.parse` already parses the public API
  policy enum, but `YierdisDb` has a separate local enum and parser with
  null/blank default-to-`noeviction` behavior.

## Considered Approaches

### Approach A: Only move glob matching and policy parsing

This is the lowest-risk change and quickly removes a large block of unrelated
utility code from `YierdisDb`.

Rejected as the full design because it leaves the constructor as the main source
of weight. It is still a good first implementation slice.

### Approach B: Introduce component factory and extract focused utilities

This approach keeps `YierdisDb` as the public facade, but moves object graph
assembly and pure helper algorithms into package-private classes:

- `YierdisDbConfig`
- `YierdisDbStorageComponents`
- `YierdisDbComponents`
- `YierdisDbComponentFactory`
- `YierdisGlobMatcher`
- `YierdisDbMemoryEstimator`
- `YierdisDbMaxmemoryPolicies`

Chosen because it addresses the real weight without changing external module
boundaries or command behavior.

### Approach C: Split maxmemory, expiration, and lifecycle behind new public APIs

This would expose narrower runtime interfaces and reduce direct `YierdisDb`
references from support classes.

Rejected for this change because it is a deeper boundary redesign. It should be a
later refactor after this design makes construction and utility ownership clear.

## Architectural Decision

Adopt Approach B.

`YierdisDb` remains the concrete `RuntimeDbEngine`. It should own:

- public constructors and static factories for compatibility
- `reads()`, `writes()`, `expiration()`, `memory()`, and `lifecycle()` facade
  accessors
- runtime hooks required by `RuntimeDbEngine`
- owner-thread guard entry points
- shutdown and flush entry points
- local LRU touch coordination
- package-private bridge methods needed by existing support classes

`YierdisDb` should not own:

- storage object construction
- foreign allocator/runtime resolution
- policy string parsing
- pure memory estimation helpers
- Redis glob matching
- repeated ops facade wiring code

The design intentionally keeps the first step package-private and same-module.
That avoids expanding the public API while still creating smaller units that can
be guarded by tests.

## Target File Structure

### Create: `YierdisDbConfig`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbConfig.java`

Responsibility:

- Validate DB construction inputs.
- Normalize null or blank `maxmemoryPolicy` to `noeviction`.
- Store `maxmemoryBytes`, parsed local policy, `maxmemorySamples`,
  `evictionTimeLimitNanos`, and `expireCleanupTimeLimitNanos`.
- Compute whether local LRU is enabled.

This class should be package-private and immutable.

### Create: `YierdisDbMaxmemoryPolicies`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemoryPolicies.java`

Responsibility:

- Own conversion from user-provided policy strings to
  `YierdisDb.MaxmemoryPolicy`.
- Preserve current `YierdisDb` behavior where null or blank means
  `YierdisDb.MaxmemoryPolicy.NOEVICTION`.
- Reuse the normalization rules already present in
  `yier.bubu.redis.ops.MaxmemoryPolicy.parse` where possible.

This keeps the local enum compatibility while removing parser code from
`YierdisDb`.

### Create: `YierdisDbStorageComponents`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbStorageComponents.java`

Responsibility:

- Hold resolved `YierdisFfmMemoryRuntime`.
- Hold resolved `OffHeapAllocator`.
- Hold `YierdisDbOwnedResources`.
- Hold `YierdisKeyspace<YierdisObject>`.
- Hold `YierdisExpireIndex`.
- Hold whether keys are stored off-heap.

This separates storage construction from runtime facade behavior.

### Create: `YierdisDbComponents`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponents.java`

Responsibility:

- Hold all constructed DB collaborators that `YierdisDb` needs as final fields.
- Include ledger, mutation executor, expiration support, maxmemory support,
  key lifecycle, internals, command ops, reporters, and public facades.
- Avoid behavior beyond simple ownership of constructed collaborators.

This class is a value object for the constructed object graph.

### Create: `YierdisDbComponentFactory`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponentFactory.java`

Responsibility:

- Resolve runtime and allocator from constructor inputs.
- Construct `YierdisFfmBlobStore`, `YierdisFfmKeyspace`, and
  `YierdisFfmExpireIndex`.
- Construct `YierdisDbConfig`.
- Construct ledger, mutation executor, lifecycle, support, ops, and facades.
- Accept a narrow owner callback object from `YierdisDb` for operations that must
  still call back into the facade, such as thread checking, LRU touch, maxmemory
  coordinator lookup, cleanup, eviction, and used-byte reporting.

The factory should be package-private. It should not make `YierdisDb` itself
larger by moving constructor code into more constructor overloads.

### Create: `YierdisGlobMatcher`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobMatcher.java`

Responsibility:

- Own Redis-style byte glob matching.
- Provide `matches(byte[] pattern, byte[] text)`.
- Provide `matches(byte[] pattern, BytesView text)`.
- Keep current semantics for `*`, `?`, escaped bytes, character classes,
  negated classes, ranges, trailing backslash, and unclosed classes.

`YierdisKeyspaceOps` should depend on this class directly instead of calling
`YierdisDb.globMatches`.

### Create: `YierdisDbMemoryEstimator`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryEstimator.java`

Responsibility:

- Estimate stored entry bytes from a key handle and `YierdisObject`.
- Estimate string write upper bounds.
- Sum byte-array lengths.
- Estimate collection write upper bounds.
- Estimate set and zset write upper bounds.
- Centralize constants currently kept in `YierdisDb` for set and zset member
  overhead.

The estimator must preserve the current off-heap rule: if a string payload is an
`OffHeapBuf`, the payload capacity is counted by the allocator/runtime path and
must not be double-counted in heap entry estimates.

### Modify: `YierdisDb`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

Responsibility after this change:

- Compatibility constructors and static factories.
- Store final references from `YierdisDbComponents`.
- Delegate `RuntimeDbEngine` methods to constructed collaborators.
- Keep `touch(YierdisObject)` because it owns the local LRU clock and coordinator
  interaction.
- Keep `bindToCurrentThread()`, `checkThread()`, `shutdown()`, `flushDb()`,
  `size()`, and package-private lifecycle bridge methods until support classes
  are narrowed in a later refactor.

Expected removals from `YierdisDb`:

- `parseMaxmemoryPolicy`
- `estimateEntryBytes`
- `estimateValueBytes`
- `estimateStringWriteUpperBound`
- `sumByteLengths`
- `estimateCollectionWriteUpperBound`
- `estimateSetWriteUpperBound`
- `estimateZSetWriteUpperBound`
- `globMatches`
- `findGlobClassEnd`
- `globClassMatches`
- direct FFM blob/keyspace/expire-index construction
- direct ops facade construction

### Modify: `YierdisKeyspaceOps`

Path:
`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`

Responsibility change:

- Replace `YierdisDb.globMatches(globPattern, k)` with
  `YierdisGlobMatcher.matches(globPattern, k)`.

No keyspace command behavior should change.

### Modify: `YierdisSetOps`, `YierdisZSetOps`, `YierdisHllOps`

Paths:

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`

Responsibility change:

- Replace calls to static estimation helpers on `YierdisDb` with
  `YierdisDbMemoryEstimator`.

### Modify: `YierdisHashOps`, `YierdisListOps`, `YierdisStringOps`

Paths:

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`

Responsibility change:

- Consolidate duplicated local `estimateStringWriteUpperBound`,
  `sumByteLengths`, and collection upper-bound helpers into
  `YierdisDbMemoryEstimator` where the signatures match current behavior.
- Keep type-specific overhead constants inside the owning ops class if moving
  them would obscure ownership. Move only the common arithmetic and byte length
  helpers in this change.

### Modify: `YierdisDbArchitectureGuardTest`

Path:
`yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`

Responsibility change:

- Guard that `YierdisDb` does not declare the extracted utility methods.
- Guard that `YierdisDb` does not contain the glob matcher helper names.
- Guard that `YierdisDb` no longer directly constructs
  `YierdisFfmBlobStore`, `YierdisFfmKeyspace`, and `YierdisFfmExpireIndex`.
- Guard that `YierdisKeyspaceOps` depends on `YierdisGlobMatcher` rather than
  `YierdisDb.globMatches`.

## Migration Plan

### Slice 1: Pure utility extraction

Extract `YierdisGlobMatcher` and update `YierdisKeyspaceOps`.

Why first:

- It is behaviorally isolated.
- It removes a large block from `YierdisDb`.
- It can be verified with focused `KEYS` and `SCAN` tests plus existing command
  tests.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ScanCursorContractTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=KeysBudgetTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test`

### Slice 2: Memory estimator extraction

Extract `YierdisDbMemoryEstimator` and update ops classes that currently call
`YierdisDb` static estimation helpers or duplicate common estimation arithmetic.

Why second:

- It removes another independent responsibility.
- It keeps memory accounting behavior testable before constructor changes begin.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MemoryStatsAccountingConsistencyTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MutationExecutorReservationTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MaxmemoryEvictionTest,TtlMaxmemoryTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test`

### Slice 3: Policy parsing and config extraction

Extract `YierdisDbMaxmemoryPolicies` and `YierdisDbConfig`.

Why third:

- It prepares constructor decomposition without moving all construction at once.
- It preserves the current null/blank default behavior explicitly.

Expected verification:

- Add focused tests for null, blank, case, underscore, and unknown policy strings
  through existing `YierdisDb` constructors.
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisInstanceTest,GlobalMaxmemoryLruAcrossDbsTest test`

### Slice 4: Storage and component factory extraction

Introduce `YierdisDbStorageComponents`, `YierdisDbComponents`, and
`YierdisDbComponentFactory`. Change `YierdisDb` constructors to delegate to the
factory and assign final fields from the resulting component object.

Why fourth:

- This is the broadest movement and should happen after pure helper behavior is
  already covered.
- It reduces the constructor without changing runtime contracts.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,OffHeapKeysToggleTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=OffHeapStringStorageTest,OffHeapLeakRegressionTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ExpireIndexTest,ExpireKeySharingTest,OffHeapBytesViewTtlRegressionTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test`

### Slice 5: Architecture guard hardening and documentation

Update architecture tests and DB internals documentation after the code shape is
stable.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest,ArchitectureBoundaryTest,DbEngineReadWriteBoundaryTest test`
- `jdk25 mvn test`

## Testing Strategy

The implementation should use focused tests per slice instead of relying only on
the full suite.

Required behavior areas:

- Glob semantics used by `KEYS` and `SCAN`.
- Memory accounting for string, hash, list, set, zset, HLL, TTL, and off-heap
  string payloads.
- Maxmemory noeviction, random eviction, LRU eviction, and global maxmemory
  coordinator behavior.
- Off-heap runtime ownership and shared-runtime construction.
- Expire index key sharing and cleanup.
- Architecture guards for the extracted responsibilities.

Recommended final commands:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MemoryStatsAccountingConsistencyTest,MutationExecutorReservationTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MaxmemoryEvictionTest,TtlMaxmemoryTest,GlobalMaxmemoryLruAcrossDbsTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ExpireIndexTest,ExpireKeySharingTest,OffHeapBytesViewTtlRegressionTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,OffHeapStringStorageTest,OffHeapLeakRegressionTest test`
- `jdk25 mvn test`

## Risks And Mitigations

### Risk: Policy parsing behavior drifts

Mitigation:

- Preserve `null` and blank as `NOEVICTION` for `YierdisDb` construction.
- Add constructor-level tests that cover null, blank, underscore, case, and
  unknown values.

### Risk: Memory accounting double-counts off-heap payloads

Mitigation:

- Keep the existing rule in `YierdisDbMemoryEstimator`: string payloads stored in
  `OffHeapBuf` contribute zero to entry heap estimate because allocator/runtime
  accounting handles them.
- Run memory stats and off-heap leak regression tests after estimator extraction.

### Risk: Component factory creates hidden circular dependencies

Mitigation:

- Keep `YierdisDbComponents` as a passive holder.
- Keep callbacks explicit in a package-private owner callback interface or small
  callback record.
- Do not let the factory call runtime methods during construction except through
  callback references that match current behavior.

### Risk: Support classes still depend on the whole `YierdisDb`

Mitigation:

- Accept this as a remaining issue for this design.
- Do not redesign `YierdisDbExpirationSupport` and `YierdisDbMaxmemorySupport`
  in the same change unless the implementation naturally exposes a small context
  object.
- Document the later follow-up: replace whole-DB dependencies with narrow
  package-private context interfaces.

### Risk: Architecture guard becomes brittle

Mitigation:

- Guard responsibilities, not exact line counts.
- Prefer forbidden declared methods and high-signal construction strings over
  fragile formatting checks.
- Keep guard failure messages explicit so future maintainers know which
  responsibility drifted.

## Acceptance Criteria

- `YierdisDb.java` no longer contains the glob matcher implementation.
- `YierdisDb.java` no longer contains memory estimation helper implementations.
- `YierdisDb.java` no longer contains maxmemory policy parser implementation.
- `YierdisDb.java` no longer directly constructs FFM blob store, keyspace, and
  expire index.
- `YierdisKeyspaceOps` uses `YierdisGlobMatcher`.
- Ops classes use `YierdisDbMemoryEstimator` for common write upper-bound and
  entry estimation behavior.
- Existing public constructors and static factory methods on `YierdisDb` remain
  source-compatible.
- Existing `RuntimeDbEngine`, `DbEngine`, `DbReads`, `DbWrites`,
  `ExpirationManager`, `MemoryOps`, and `DbLifecycleOps` contracts remain
  unchanged.
- Architecture guard tests prevent extracted responsibilities from returning to
  `YierdisDb`.
- Full Maven test suite passes after the refactor.

## Expected Outcome

The main result is not merely fewer lines in `YierdisDb`. The important outcome is
clearer ownership:

- construction belongs to component/config classes
- matching belongs to a matcher
- estimation belongs to an estimator
- command behavior remains in ops classes
- runtime facade behavior remains in `YierdisDb`

This creates a better base for a later pass that narrows
`YierdisDbExpirationSupport` and `YierdisDbMaxmemorySupport` so they no longer
need the whole `YierdisDb` instance.
