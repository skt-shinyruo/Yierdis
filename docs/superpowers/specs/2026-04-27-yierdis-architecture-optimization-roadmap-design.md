# Yierdis Architecture Optimization Roadmap Design

## Summary

This design defines the next optimization roadmap for Yierdis after the current
executor-core and `YierdisDb` decomposition work.

The project already has clear high-level boundaries: protocol decoding stays in
the protocol/server lane, command execution uses `ExecutionRequest` and
`ReplyWriter`, and DB access is routed through `DbEngine` capability interfaces.
The remaining design issues are now narrower and more concrete:

- `yierdis-executor-core` is Netty-free but still depends directly on
  `YierdisFastCommandProcessor` and `yierdis-core-command`.
- maxmemory and expiration paths still materialize off-heap keys into heap
  `byte[]` values in important sampling and eviction paths.
- FFM storage allocates many small native regions, often one confined arena per
  key or value blob.
- memory accounting works, but the distinction between heap estimates, native
  payload bytes, table overhead, and reserved bytes is implicit.
- command registration, metadata, and transaction restrictions are still
  manually maintained in multiple command modules.
- documentation still contains stale executor names from the pre-executor-core
  refactor.

This spec is a roadmap spec. It is intended to be implemented as independent
slices, each with its own focused plan and verification. It complements, rather
than replaces, the existing `YierdisDb Decomposition Design` spec.

## Problem Statement

Yierdis has been refactored toward good module boundaries, but several design
seams are still incomplete.

The most important issue is that some modules are transport-neutral in naming
but not yet fully contract-neutral in dependencies. `CommandExecutor` lives in
`yierdis-executor-core`, but it imports and stores `YierdisFastCommandProcessor`.
That means the executor runtime still depends on the concrete command module
instead of depending only on the execution contract.

The second issue is off-heap efficiency. Keyspace and expire index internals use
FFM storage, but hot maintenance paths still convert internal key references to
heap arrays. This is especially undesirable under memory pressure, where
eviction and cleanup should avoid avoidable allocation.

The third issue is allocation granularity. `YierdisFfmMemoryRuntime` creates a
new confined arena for every allocated region. `YierdisFfmBlobStore` and
`YierdisForeignOffHeapAllocator` then use that primitive for many small blobs.
This keeps ownership simple, but it creates high native allocation and tracking
overhead as key count grows.

The fourth issue is metadata drift. Command registration is modular, but command
arity, key positions, transaction restrictions, and COMMAND output metadata are
still encoded by hand near each module registration.

Finally, documentation and architecture guards need to match the current code
shape so future changes do not reintroduce old ownership models.

## Goals

- Make `yierdis-executor-core` depend only on execution contracts, not on
  concrete command processor classes.
- Avoid heap key materialization in maxmemory candidate sampling, eviction, and
  expiration cleanup paths where an internal key identity is sufficient.
- Introduce a slab/page allocation design for small FFM blobs while preserving
  the current resource ownership and leak detection guarantees.
- Make memory accounting categories explicit and testable.
- Introduce a command spec source of truth for registration and metadata without
  changing command semantics.
- Update docs and architecture guards to reflect the current executor-core and
  runtime boundaries.
- Keep changes incremental: each phase must be useful on its own and verifiable
  with focused tests.

## Non-Goals

- No Redis RESP compatibility.
- No persistence, replication, clustering, ACL, TLS, Lua, or Pub/Sub work.
- No public protocol redesign.
- No command behavior changes beyond metadata ownership cleanup.
- No one-shot rewrite of every DB value type.
- No requirement that the slab allocator immediately backs all off-heap data
  structures.
- No broad line-count cleanup unrelated to the optimization phases.

## Current Facts

Important current files and facts:

- `yierdis-executor-core/pom.xml` depends on `yierdis-core-command`.
- `CommandExecutor` imports `YierdisFastCommandProcessor` and accepts it in its
  constructor.
- `ProtocolCommandAdapter` already converts protocol DTOs into
  `ExecutionRequest`, so the request execution contract is a good seam.
- `YierdisFfmMemoryRuntime.allocateRegion(...)` creates `Arena.ofConfined()` per
  region.
- `YierdisFfmBlobStore.store(...)` allocates one FFM region for each stored blob.
- `YierdisFfmKeyspace.randomKey()` returns `byte[]`, and
  `YierdisFfmKeyspace.forEach(...)` exposes heap key copies.
- `YierdisFfmExpireIndex` already has `randomKeyHandle()`, proving the handle
  pattern works for one internal index.
- `MaxmemoryCandidate` currently stores `byte[] key`, which forces participant
  implementations to expose candidates as heap bytes.
- `HashValue`, `ListValue`, `SetValue`, and `ZSetValue` return zero from
  `estimatedBytes()` on FFM-backed paths because native usage is accounted
  elsewhere.
- `YierdisFastCommandProcessor` manually registers each default module, and
  server-facing commands are manually registered in `ServerCommandModule`.
- `docs/request-execution-flow.md` and related docs still reference old
  `NettyCommandExecutor` names even though the source now uses `CommandExecutor`.
- `docs/superpowers/specs/2026-04-27-yierdisdb-decomposition-design.md` already
  covers focused `YierdisDb` decomposition. This roadmap should not duplicate
  that work.

## Considered Approaches

### Approach A: Performance-first roadmap

Start with slab allocation and zero-copy eviction, then clean up executor and
command metadata boundaries later.

This has the fastest potential runtime payoff, but it risks doing large memory
changes while the surrounding boundaries are still ambiguous.

### Approach B: Boundary-first roadmap

Start with executor and command registration boundaries, then address off-heap
allocation and memory accounting later.

This makes later changes safer, but it delays the highest-impact runtime memory
work.

### Approach C: Risk-ordered phased roadmap

Make one roadmap spec and implement independent slices in dependency order:

1. executor execution boundary
2. zero-copy key identity for eviction and expiration
3. FFM slab/page allocator
4. explicit memory accounting categories
5. command spec source of truth
6. docs and architecture guardrails

Chosen because it keeps the work complete while still allowing each phase to be
planned and implemented independently.

## Architectural Decision

Adopt Approach C.

The roadmap should not be implemented as one large refactor. Each phase must
preserve existing behavior and should leave the codebase in a shippable state.
The phases are ordered to reduce risk:

- First remove unnecessary concrete dependencies from the executor.
- Then remove avoidable heap key materialization from internal pressure paths.
- Then introduce the larger FFM allocation redesign.
- Then make accounting categories explicit enough to verify the new allocator.
- Then reduce command metadata drift.
- Finally update docs and add guards that keep the new boundaries stable.

## Phase 1: Executor Execution Boundary

### Problem

`yierdis-executor-core` is transport-neutral but not command-implementation
neutral. It directly depends on `YierdisFastCommandProcessor` and therefore on
`yierdis-core-command`.

### Target Design

Create a narrow execution interface in `yierdis-executor-core`:

```java
@FunctionalInterface
public interface CommandExecutionEngine {
    void execute(ExecutionRequest request, CommandContext context);
}
```

`CommandExecutor` should accept `CommandExecutionEngine` instead of
`YierdisFastCommandProcessor`. `CommandExecutorExecutionSupport` should call that
interface.

`yierdis-server` should adapt the concrete processor at the composition root:

```java
CommandExecutionEngine engine = processor::execute;
```

After this phase, `yierdis-executor-core` should depend on
`yierdis-core-contract` only for execution types. It should not depend on
`yierdis-core-command`, protocol modules, or Netty.

### Files

Create:

- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java`

Modify:

- `yierdis-executor-core/pom.xml`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- executor-core tests that currently construct a concrete processor
- architecture boundary tests

### Acceptance Criteria

- `yierdis-executor-core/pom.xml` no longer depends on
  `yierdis-core-command`.
- No production source in `yierdis-executor-core` imports
  `yier.bubu.redis.command.*`.
- Existing executor behavior remains unchanged: submission, rejection, drain,
  close-after-reply, maintenance, and backpressure tests still pass.

## Phase 2: Zero-Copy Key Identity For Eviction And Expiration

### Problem

The keyspace supports `KeyHandle` for stable off-heap identity, but hot paths
still fall back to heap `byte[]` keys:

- `YierdisFfmKeyspace.randomKey()` copies a random key into a byte array.
- maxmemory candidate selection stores a `byte[]` in `MaxmemoryCandidate`.
- local eviction then looks up and removes by heap key bytes.
- expiration cleanup samples `expires.randomKey()` even though the expire index
  already has `randomKeyHandle()`.

### Target Design

Introduce a core-api marker for participant-owned candidate identity:

```java
public interface MaxmemoryKeyRef {
}
```

Change `MaxmemoryCandidate` from `byte[] key` to a participant-owned key
reference:

```java
public record MaxmemoryCandidate(
        MaxmemoryParticipant owner,
        MaxmemoryKeyRef keyRef,
        long lruClock
) { }
```

`MaxmemoryKeyRef` is intentionally opaque to the governor. The governor should
continue to compare only `owner` and `lruClock`, then call `owner.evict(...)`.
Only the owning participant may interpret the key reference.

Inside `core-db`, create a package-private key ref implementation that wraps the
existing `KeyHandle`:

```java
final class YierdisMaxmemoryKeyRef implements MaxmemoryKeyRef {
    private final KeyHandle handle;
}
```

Add `randomKeyHandle()` to `YierdisKeyspace`. `YierdisFfmKeyspace` should
implement it without copying key bytes, mirroring the existing expire index
handle path.

Update maxmemory support to sample and evict using `KeyHandle` where possible:

- `sampleCandidate(...)` returns a `YierdisMaxmemoryKeyRef`.
- `scanBestCandidate(...)` returns a `YierdisMaxmemoryKeyRef`.
- `evict(...)` validates `candidate.owner() == db`, unwraps the handle, checks
  expiration, removes TTL metadata, removes from keyspace by handle, and releases
  payload.

Update expiration cleanup to use `expires.randomKeyHandle()` and store/keyspace
lookup by handle instead of converting the expire key to a heap array.

### Files

Create:

- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryKeyRef.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisMaxmemoryKeyRef.java`

Modify:

- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCandidate.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspace.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmKeyspace.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java`
- related tests and architecture guards

### Acceptance Criteria

- maxmemory local eviction no longer requires `store.randomKey()`.
- global maxmemory candidate selection works with opaque key refs.
- expiration cleanup can sample expire keys by handle.
- No `KeyHandle` type leaks into `yierdis-core-api`.
- Existing maxmemory, TTL, expire key sharing, and off-heap key tests pass.

## Phase 3: FFM Slab/Page Allocator

### Problem

The current FFM runtime allocates one confined arena per region. This is simple
and safe, but inefficient for many small keys, fields, members, and string
payloads.

### Target Design

Add a small-object page allocator while preserving the existing region API for
large or isolated allocations.

The design should introduce these concepts in `yierdis-memory-foreign`:

- `YierdisFfmPage`: owns one arena and one memory segment.
- `YierdisFfmSlabAllocator`: manages pages for size classes.
- `YierdisFfmAllocation`: an offset-length allocation inside a page.
- `YierdisFfmAllocationRef`: closeable reference with owner page and length.

Small allocations should be rounded into size classes. Large allocations should
continue to use direct regions until there is a clear reason to pool them.

`YierdisFfmBlobStore` should be the first production consumer because it is used
by keys and many collection internals. Existing table storage can continue using
dedicated regions because table arrays are naturally larger and longer-lived.

The slab allocator must keep the existing safety properties:

- all allocations are accounted in `YierdisFfmMemoryRuntime.usedBytes()` or an
  explicit runtime-owned category
- all pages are closed when empty or when the owning runtime is closed
- leak detection still reports unreleased live native memory
- a released key/value ref cannot be read successfully

### Files

Create or modify in `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign`:

- `YierdisFfmPage.java`
- `YierdisFfmSlabAllocator.java`
- `YierdisFfmAllocation.java`
- `YierdisFfmAllocationRef.java`
- `YierdisFfmMemoryRuntime.java`

Modify in `core-db`:

- `YierdisFfmBlobStore`
- `YierdisFfmBytesRef`
- FFM access helpers as needed
- tests that assert region counts or exact runtime allocation shape

### Acceptance Criteria

- small blob storage can use slab/page allocation without changing key equality
  or refcount behavior.
- large blob storage can still fall back to existing direct region behavior.
- off-heap leak tests still fail on unreleased native memory.
- keyspace, expire index, hash/list/set/zset FFM smoke tests pass.
- memory stats can explain slab page bytes separately from table overhead.

## Phase 4: Explicit Memory Accounting Categories

### Problem

Memory accounting currently combines several domains:

- heap-side estimates kept by the DB ledger
- native bytes from FFM runtime and off-heap allocator paths
- keyspace and expire table overhead
- expire value object estimates
- reserved bytes during pending mutations

This works, but the ownership model is implicit. As FFM allocation changes, the
project needs stronger category-level accounting.

### Target Design

Introduce an internal memory breakdown model, preserving current user-facing
fields unless there is a deliberate API change.

Suggested categories:

- `heapDataBytesEstimate`
- `nativePayloadBytes`
- `nativeAllocatorOverheadBytes` if measurable, otherwise explicitly zero
- `keyspaceTableOverheadBytesEstimate`
- `expireTableOverheadBytesEstimate`
- `expireValueObjectsBytesEstimate`
- `reservedBytes`
- `effectiveUsedBytesForMaxmemory`

`YierdisDbMemoryReporter` should assemble the per-DB breakdown. Runtime
observability should aggregate categories across DBs, preserving the existing
global-scope rule that a shared FFM runtime is counted once.

The important rule remains: an off-heap string payload must not be counted both
as heap entry bytes and native payload bytes.

### Files

Create or modify:

- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/YierdisMemoryStats.java`
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java`
- memory stats command tests

### Acceptance Criteria

- `MEMORY STATS` remains understandable and backward-compatible where feasible.
- category sums are tested for single DB and global maxmemory scope.
- FFM-backed collection values do not undercount native payload bytes after slab
  allocation is introduced.
- `MemoryStatsAccountingConsistencyTest` or a successor test enforces the model.

## Phase 5: Command Spec Source Of Truth

### Problem

Command registration is modular, but metadata is still hand-maintained next to
registration calls. This creates drift risk as the command set grows.

### Target Design

Introduce a declarative command definition model in `yierdis-core-command`.

Suggested model:

```java
public final class CommandDefinition {
    private final String name;
    private final CommandDescriptor descriptor;
    private final String disallowedInMultiError;
    private final CommandModule.Handler handler;
}
```

Each command module should expose definitions and register them through a common
helper. Server-facing commands can use the same model from `ServerCommandModule`
without moving those commands into core defaults.

This phase should be deliberately simple. Do not introduce annotation processing
or code generation until the declarative model proves useful.

### Files

Create or modify:

- `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDefinition.java`
- `CommandModule`
- `CommandRegistry`
- `YierdisFastCommandProcessor`
- each `*Commands` module
- `ServerCommandModule`
- command metadata and registry tests

### Acceptance Criteria

- command name, handler, descriptor, and MULTI restriction are declared in one
  object per command.
- `COMMAND`, `COMMAND COUNT`, and `COMMAND INFO` keep current behavior.
- server-facing commands remain assembled by `yierdis-server`.
- architecture tests still prevent protocol model dependencies from entering
  core-command.

## Phase 6: Documentation And Guardrails

### Problem

Some docs describe old executor classes that no longer exist. Existing guards are
strong, but they should be updated for the next target boundaries.

### Target Design

Update docs after each code phase, then harden guardrails after the final code
shape is stable.

Docs to update:

- `docs/module-architecture.md`
- `docs/request-execution-flow.md`
- `docs/executor-and-backpressure.md`
- `docs/project-overview.md`
- `docs/main-path-walkthrough.md`
- `docs/configuration-and-operations.md`
- `docs/development-navigation.md`
- `docs/db-internals.md`
- `docs/bytes-and-fast-paths.md`

Guardrails to add or update:

- `yierdis-executor-core` must not import `yier.bubu.redis.command.*`.
- `yierdis-executor-core` must remain Netty-free and protocol-free.
- server must not reintroduce executor runtime ownership classes.
- maxmemory eviction support should not call `store.randomKey()` when a handle
  path exists.
- expiration cleanup should prefer `randomKeyHandle()`.
- docs should not reference deleted `NettyCommandExecutor` classes except in
  historical specs or plans.

### Acceptance Criteria

- docs describe `CommandExecutor`, `ExecutionConnection`, and
  `ExecutionIoAdapter` instead of old Netty-owned executor runtime names.
- architecture guard tests explain violations with actionable failure messages.
- stale docs are either updated or explicitly marked historical under
  `docs/superpowers/*`.

## Migration Plan

### Slice 1: Executor dependency inversion

Implement Phase 1 only.

Expected verification:

- `jdk25 mvn -pl yierdis-executor-core test`
- `jdk25 mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest,NettyExecutionAdapterIntegrationTest test`
- architecture boundary tests for executor dependencies

### Slice 2: Handle-based maxmemory and expiration paths

Implement Phase 2 only.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MaxmemoryEvictionTest,TtlMaxmemoryTest,GlobalMaxmemoryLruAcrossDbsTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ExpireIndexTest,ExpireKeySharingTest,OffHeapBytesViewTtlRegressionTest test`

### Slice 3: Slab allocator first consumer

Implement Phase 3 for `YierdisFfmBlobStore` first.

Expected verification:

- `jdk25 mvn -pl yierdis-memory/foreign test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=UnsafeOffHeapKeyspaceTest,OffHeapStringStorageTest,OffHeapLeakRegressionTest test`
- optional before/after bench using `REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh`

### Slice 4: Memory accounting categories

Implement Phase 4 after slab allocator accounting behavior is known.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MemoryStatsAccountingConsistencyTest test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisGlobalMaxmemoryGovernorTest,GlobalMaxmemoryLruAcrossDbsTest test`

### Slice 5: Command definition SSOT

Implement Phase 5.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-command test`
- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandRegistryTest,CommandDescriptorRegistryTest,CommandMetadataRegressionTest test`
- `jdk25 mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest test`

### Slice 6: Docs and guardrails

Implement Phase 6.

Expected verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest,ReplySsoTGuardTest,YierdisDbArchitectureGuardTest test`
- `jdk25 mvn test`

## Testing Strategy

Use focused tests per phase, then full suite verification at the end.

Required behavior coverage:

- executor submission, drain, close-after-reply, maintenance, and backpressure
- protocol-to-execution adapter integration in server
- maxmemory noeviction, random eviction, LRU eviction, and global governor
- TTL cleanup and expire-key sharing
- off-heap key lookup, scan, mutation, and resource release
- memory stats category consistency
- command registry and COMMAND metadata behavior
- architecture boundary scans

Benchmarking is recommended for slab and zero-copy phases, but it should not be
the only acceptance signal because the benchmark environment may vary.

## Risks And Mitigations

### Risk: `MaxmemoryCandidate` API change leaks DB internals into core-api

Mitigation:

- Use an opaque `MaxmemoryKeyRef` marker in core-api.
- Keep `KeyHandle` in core-db.
- Let only the owning participant interpret the key reference.

### Risk: Slab allocator complicates lifetime management

Mitigation:

- Keep the existing direct-region path for large allocations.
- Start with `YierdisFfmBlobStore` only.
- Preserve closeable refs and runtime leak detection.
- Add tests that release, double-release, leak, and read-after-release behavior
  stays deterministic.

### Risk: Memory stats drift during allocator migration

Mitigation:

- Introduce categories before or immediately after the first slab consumer.
- Keep the existing off-heap no-double-counting rule explicit in tests.
- Make category sums part of regression coverage.

### Risk: Command spec SSOT turns into over-engineering

Mitigation:

- Use plain Java records/classes first.
- Do not add annotation processors or generators in this roadmap.
- Preserve current command modules and handler methods.

### Risk: Roadmap scope becomes too large for one implementation plan

Mitigation:

- Treat this document as the roadmap spec.
- Create separate implementation plans for each slice.
- Do not start Phase 3 until Phase 2 is verified.

## Acceptance Criteria

- The roadmap is implemented as independent, reviewable slices.
- `yierdis-executor-core` no longer depends on `yierdis-core-command`.
- maxmemory candidate selection and eviction can use opaque key refs without
  materializing heap key copies.
- expiration cleanup can use handle-based sampling.
- the first FFM slab/page allocator consumer is implemented without breaking
  leak detection or existing off-heap behavior.
- memory accounting categories are explicit and tested.
- command registration metadata is declared through a command definition SSOT.
- docs and guards match the current source architecture.
- `jdk25 mvn test` passes after all slices are complete.

## Expected Outcome

After this roadmap, Yierdis should have clearer design ownership and better
runtime foundations:

- executor-core owns execution mechanics, not command implementation details
- core-db can evict and clean up by stable key identity instead of heap copies
- FFM storage can scale small allocations through pages instead of per-object
  arenas
- memory stats can explain what is counted and why
- command metadata has one source of truth
- docs and guardrails describe the architecture that actually exists
