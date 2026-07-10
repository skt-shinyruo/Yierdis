# Single-Node Production Hardening Design

## Status

Approved design for hardening Yierdis as a production-oriented single-node
Redis-style server. The external RESP behavior, existing command semantics,
and existing CLI options remain compatible. Internal Java APIs may change.

This design intentionally retains one command-owner thread. Multi-owner
sharding, AOF/RDB durability, replication, and cluster behavior remain outside
this program.

## Context

Yierdis already has a sound high-level shape:

- Netty event loops own socket I/O and RESP decoding.
- A bounded executor queue transfers complete requests to one command-owner
  thread.
- The command layer depends on DB capability interfaces rather than the
  concrete memory engine.
- The DB uses stable generation-bearing native handles, FFM-backed metadata and
  payloads, TTL indexes, memory ledgers, and maxmemory governors.
- Architecture tests enforce the most important module dependency rules.

The remaining risks are concentrated in resource and failure semantics rather
than command coverage:

- Every DB eagerly allocates a full native object metadata table and a boxed
  free-slot queue. With the default 16 DBs and 262,144 slots per DB, metadata
  alone consumes 301,989,888 bytes before user data exists.
- Collection mutations can modify visible state before a later allocation
  fails. Rolling back a memory reservation does not roll back those changes.
- Native slot exhaustion throws a different exception from the one translated
  into the Redis OOM reply path.
- Open-addressed key and collection maps retain tombstones, only grow, use a
  public deterministic hash, and do unbounded rehash work on the owner thread.
- Global and per-DB maxmemory use different accounting models. Empty small
  native pages are reusable but not physically released under pressure.
- RESP request memory is admitted only after a complete request is decoded, so
  partial commands across many connections are outside executor queue budgets.
- Change events depend on command handlers manually recording mutation
  outcomes and silently discard sink failures.
- Live native objects also have per-object heap mirrors and production allocator
  operations pay synchronization costs despite owner-thread confinement.

The goal is to repair these invariants in place while keeping the repository
buildable and testable after every delivery stage.

## Goals

- Make empty DB native object metadata lazy and independent of configured slot
  capacity.
- Preserve stable handle generation, pin, epoch, quarantine, stale-handle, and
  defrag safety.
- Guarantee per-command atomicity for expected capacity and memory failures.
- Translate all expected native capacity failures into the existing Redis OOM
  command error without closing a healthy connection.
- Make key and collection hash tables compactable, shrinkable, collision
  resistant, and incrementally rehashed.
- Give SCAN and maintenance operations budgets based on actual work.
- Use one physical-aware maxmemory accounting model for global and per-DB
  scopes.
- Reclaim empty native data pages under maxmemory pressure.
- Admit partial and decoded RESP request memory before large arrays are
  allocated.
- Publish committed changes through an ordered, bounded, observable in-process
  commit stream without silent event loss.
- Remove avoidable per-object heap mirrors and locks from the production DB
  path.
- Keep median GET, SET, HSET, and ZADD throughput within 10% of the pre-change
  baseline on the existing benchmark suite.

## Non-Goals

- Do not add multi-owner DB sharding or parallel mutation execution.
- Do not add AOF, RDB, restart recovery, replication, or cluster protocols.
- Do not make commit-stream events durable across process crashes.
- Do not make an entire MULTI/EXEC block rollback as one transaction. Each
  executed command receives the new failure-atomic guarantee independently.
- Do not maintain a second storage engine or a long-lived V2 compatibility
  path.
- Do not make arbitrary JVM `Error` values recoverable. The strong atomicity
  guarantee covers expected ledger, allocator, slot, and native capacity
  failures.
- Do not remove heap indexes that are authoritative query structures, such as
  collection hash-table arrays.

## Compatibility Boundary

The following behavior remains compatible:

- RESP command names, argument rules, success replies, and existing command
  error semantics.
- Existing CLI option names and meanings.
- `maxmemoryBytes=0` continues to disable enforcement and eviction.
- One owner thread continues to serialize all DB mutations and maintenance.
- Redis-compatible MULTI/EXEC behavior remains command-by-command rather than
  rollback transactional.

Internal allocator, DB mutation, memory reporting, change-event, and hash-table
APIs may be replaced. New optional configuration and observability fields may be
added without making existing options invalid.

Expected resource failures use the existing Redis maxmemory OOM reply. A full
or failed commit stream uses `BUSY commit stream unavailable`. A DB that detects
an internal invariant failure uses `MISCONF DB is in a degraded state; writes
are disabled`. A request that exceeds decode memory limits uses `ERR request
exceeds configured memory limit` and closes the connection after the reply.

## Approved Approach

Three approaches were considered:

1. Harden the current implementation in dependency order.
2. Build a second Storage Engine V2 and migrate after feature parity.
3. Patch each observed failure independently.

The approved approach is dependency-ordered in-place hardening. A parallel V2
would double storage paths, test matrices, and migration risk. Independent
patches would preserve the conflicting mutation, allocator, and maxmemory
models that caused the current failures.

The dependency order is:

```text
allocator and memory usage
  -> failure-atomic mutation
  -> bounded hash tables
  -> maxmemory convergence
  -> RESP ingress admission
  -> reliable commit stream
```

Each stage produces a working, independently testable repository. Temporary
adapters may bridge adjacent stages, but each adapter is removed in the stage
that replaces its final consumer.

## Architecture

The module dependency direction remains unchanged:

```text
networking
  inbound memory admission and RESP framing
        |
command
  parse and invoke DB capabilities
        |
DB mutation coordinator
  reserve, prepare, commit, abort
        |
storage structures
  prepared value, entry, and index changes
        |
native allocator
  segmented metadata, pages, usage, trim
        |
instance commit stream
  ordered asynchronous delivery
```

The command layer no longer decides whether a mutation occurred. The DB
mutation coordinator is the only logical commit authority. It returns a commit
receipt and publishes the corresponding event through an already-reserved
commit-stream slot.

All writes use this lifecycle:

```text
prepare
  reserve maxmemory
  reserve commit-event capacity when enabled
  allocate every capacity-sensitive native and heap resource
  construct an invisible prepared mutation

commit
  switch visible state without capacity-sensitive allocation
  commit memory accounting
  assign the next commit sequence
  publish the reserved event

abort
  release staged resources, memory reservation, and event reservation
  leave the previous visible state unchanged
```

The central invariant is that commit performs no operation that can fail due to
slot, queue, native, or configured memory capacity.

## Segmented Native Object Table

`YierdisNativeObjectTable` becomes a lazily allocated segmented table. Each
segment contains 4096 slots. The existing 40-bit handle slot id maps to a
segment index and an offset, so `NativeHandle` encoding does not change.

`nativeSlotCapacity` remains the maximum number of usable slots. It controls
admission but no longer controls startup allocation. An empty object table has
zero committed metadata segments.

Each segment owns primitive slot-management structures:

- native metadata records;
- a primitive free-slot stack or bitmap;
- a retired-slot bitmap;
- live, pinned, quarantined, and moving counts.

No `ArrayDeque<Integer>` or other boxed per-slot collection is used. A metadata
segment remains allocated after becoming empty so all per-slot generations and
retirement decisions remain available for stale-handle validation. This means
metadata follows historical maximum concurrent slot use, not configured
capacity or insertion/deletion count.

The table reports metadata committed bytes, active segments, live slots, free
slots, retired slots, and peak live slots without walking all configured slots.

## Native Page Reclamation

Small native pages remain size-class based. Freeing the last block marks a page
empty. Normal allocation may retain at most one empty warm page per size class.
A pressure trim closes every empty data page, removes it from page indexes, and
releases its FFM region.

Medium and large spans continue to close when their allocation is released.
All page-level accounting is updated before region close is reported complete.

The page allocator exposes bounded trim operations by inspected page count,
closed bytes, and elapsed time. Maxmemory enforcement runs pressure trim before
selecting additional eviction victims and again before declaring OOM.

## Native Failure Taxonomy

Expected allocation-capacity failures share one hierarchy:

```text
OffHeapOutOfMemoryException
  NativeCapacityExceededException
```

Slot capacity, allocator hard limits, page-region allocation failure, and FFM
native allocation exhaustion use this hierarchy. Allocation boundaries may
translate a native-memory `OutOfMemoryError` thrown by FFM into
`NativeCapacityExceededException`; unrelated JVM errors are rethrown.

`NativeMemoryException` remains an invariant and lifecycle exception for stale
handles, illegal state transitions, pin misuse, corrupted metadata, and invalid
object movement. It is not translated into an OOM reply.

## Authoritative Memory Usage

All memory consumers report a common immutable snapshot:

```text
MemoryUsageSnapshot
  heapEstimatedBytes
  nativeMetadataCommittedBytes
  nativeDataCommittedBytes
  nativeDataLiveBytes
  nativeReclaimableBytes
  effectiveBytesForMaxmemory
```

The effective maxmemory value is:

```text
heapEstimatedBytes
  + nativeMetadataCommittedBytes
  + nativeDataCommittedBytes
```

FFM runtime totals remain useful leak diagnostics but are no longer injected as
a separate global-only maxmemory term. Every DB component reports the regions
it owns. Global scope sums all participating DB snapshots once. Per-DB scope
uses the same snapshot and changes only budget ownership.

`maxmemoryBytes=0` skips enforcement but still produces complete statistics.
`offheap_included_in_maxmemory` describes the accounting model rather than
whether a governor object happens to be attached.

Saturating arithmetic is required for every estimate and aggregation. A
saturated estimate rejects growth rather than wrapping into an admissible
negative value.

## Prepared Mutations

Storage writes return a staged mutation:

```java
interface PreparedMutation<T> extends AutoCloseable {
    long actualDeltaBytes();
    T commit();
    void abort();
}
```

`abort()` is idempotent. `close()` aborts an uncommitted mutation. `commit()`
may update pointers, counters, primitive arrays, and already-allocated nodes,
but may not grow a table, create a native object, reserve memory, or reserve an
event.

`YierdisDbMutationExecutor` performs these steps:

1. Check owner-thread and DB health.
2. Reserve the operation's upper-bound memory estimate.
3. Reserve commit-stream count and byte capacity when a sink is enabled.
4. Prepare the value, entry, key-directory, TTL-index, and event changes.
5. Reconcile the reservation with the prepared mutation's measured delta.
6. Commit visible storage state.
7. Commit the memory ledger.
8. Publish the reserved commit event.
9. Release superseded objects whose ownership moved during commit.

If steps 2 through 5 fail with an expected resource exception, the executor
aborts all staging and returns the Redis OOM error. The connection remains open
and no event is emitted.

An unexpected invariant exception marks the DB degraded before the connection
failure path runs. Reads remain available. Subsequent writes receive the
documented MISCONF error. Health and INFO output expose the first degradation
cause.

### Type-Specific Staging

- String writes allocate the replacement value and entry before swapping the
  entry handle.
- List writes build every new node and calculate final head, tail, and length
  before linking them during commit.
- Hash and Set writes allocate new member handles and any replacement table
  arrays before changing visible buckets.
- ZSet writes choose skiplist levels and allocate the new member handle, node,
  and map capacity before unlinking an existing node.
- Listpack changes construct a replacement representation during prepare and
  swap it during commit.
- Key-directory, entry-table, and TTL-index growth is prepared before the value
  mutation becomes visible.

Releasing replaced resources after the visible swap is required to be
allocation-free. A release-time invariant failure degrades the DB rather than
attempting an unsafe logical rollback after visibility changed.

## Fault Injection

The allocator test kit gains a deterministic fail-on-allocation-N wrapper. For
each mutation family, tests run the operation repeatedly with failure at every
allocation point until one run succeeds without an injected failure.

Every failed run asserts:

- logical data equals the pre-command snapshot;
- TTL and mutation outcome are unchanged;
- used, reserved, committed, and reclaimable accounting return to their prior
  values;
- live native handle counts return to their prior values;
- no commit sequence is consumed and no event is queued;
- the connection can execute a subsequent read command.

This matrix covers new-key and existing-key paths for String, List, Hash, Set,
ZSet, HLL, key expiration updates, and key-directory/entry-table growth.

## Compactable Incremental Hash Tables

`NativeKeyDirectory` and `NativeByteMap` keep their separate typed APIs and use
one shared capacity policy:

- grow when filled slots exceed 75% of capacity;
- compact at the same capacity when tombstones exceed
  `max(size, capacity / 8)`;
- shrink one power-of-two level when size is below 12.5% of capacity;
- return to initial capacity on clear.

Resize work is incremental. A table undergoing rehash owns an active table, an
old table, and a rehash cursor. Lookups inspect both tables, while new writes go
to the active table. Each command and maintenance step migrates at most its
configured number of actual slots and obeys its elapsed-time limit.

Creating a replacement array happens during prepare. Failure leaves the old
table authoritative. Empty and tombstone slots consume scan and rehash budget,
so sparse tables cannot cause unaccounted owner-thread work.

SCAN cursors include table generation and rehash phase. A complete iteration
does not omit a key that exists for the entire iteration, but may return a key
more than once during rehash, matching Redis-style weak iteration semantics.

All byte-key hashes use a per-instance random SipHash key. Tests inject a fixed
key. The implementation is checked against official SipHash vectors and does
not convert request bytes into `String` values.

Hash-table metrics include capacity, size, filled slots, tombstones, rehash
state, rehash cursor, completed rehashes, and maximum observed probe length.

## RESP Ingress Memory Admission

A shared `InboundMemoryBudget` accounts for request memory from socket receipt
through executor completion. Its lease moves through these states:

```text
partial Netty input
  -> argv/reference reservation
  -> copied payload reservation
  -> ExecutionRequest reservation
  -> release after execution or rejection
```

The budget accounts for unread inbound bytes, argv array headers and reference
slots, copied bulk arrays, inline token arrays, and complete decoded requests.
The argv estimate is conservatively `16 + argc * 8` bytes.

The decoder reserves argv memory before creating `byte[argc][]`. It reserves a
bulk payload before creating its byte array. It checks declared bulk length
against per-command, per-connection, and global limits before allocation. Every
addition uses saturating arithmetic.

The existing protocol limit options keep their meanings. A new optional
`protocolGlobalInFlightBytes` value uses `0` for an automatically derived limit:

```text
max(128 MiB, saturatedMultiply(executorQueueMaxBytes, 2))
```

The per-connection hard limit is derived from protocol max command bytes plus
the maximum admitted argv overhead. The inbound high watermark disables that
connection's autoRead. A declaration that cannot fit its hard limit receives
the documented request-memory error and closes after the reply.

The global budget enters backpressure at 75% and clears at 50%. If an argv or
bulk reservation fits the connection hard limit but current global capacity is
temporarily unavailable, the decoder enters a waiting-for-budget state without
allocating the array. It disables autoRead and registers one waiter. Budget
release schedules that waiter on its channel event loop, where decoding resumes
from the existing cumulator. Waiters are served FIFO and a closed channel is
removed without consuming a wakeup. If an inbound ByteBuf itself cannot be
reserved because a high-watermark read was already in flight, that connection
is rejected and closed rather than retaining unaccounted memory.

The Netty cumulator strategy must release fully consumed components. New
request leases are reserved before old input leases are released so short
copying peaks remain accounted. Protocol error, channel close, decoder reset,
executor rejection, skipped closing commands, and normal completion all use an
idempotent release path.

Executor queued bytes remain a scheduling/backpressure metric. Inbound memory
is a lifetime admission metric. Both are exposed separately and are not added
together as if they represented distinct physical memory.

## Reliable In-Process Commit Stream

An instance-level `CommitStream` has the command-owner thread as its only
producer and a dedicated sink worker as its consumer. It provides strict global
ordering across all logical DBs without cross-producer coordination.

Each event contains:

```text
sequence
dbIndex
kind: USER | EXPIRED | EVICTED
immutable execution record
committedMemoryDelta
commitTimestamp
```

The user-command event retains the immutable execution request and its memory
lease rather than copying argv again. Expiration and eviction prepare a bounded
synthetic DEL record.

When a non-NOOP sink is configured, mutation prepare reserves queue count and
bytes. Commit assigns the next sequence and publishes into that reservation.
No-op commands and aborted mutations consume neither capacity nor sequence.

The default stream limits are 8192 queued events and 64 MiB of retained event
bytes. Embedded configuration may override either positive value. Graceful
shutdown uses a five-second default drain timeout. These limits are inactive
for a NOOP sink.

The sink acknowledges an event only after `onChange` returns successfully. On
success it advances `lastAckedSequence` and releases the event lease. On
exception it retains the head event, transitions to FAILED, records the first
failure, and stops accepting writes. Reads continue. No event is silently
dropped.

Expiration remains logically invisible after its deadline even when physical
deletion waits for stream capacity. Eviction waits rather than deleting without
an event. If stream pressure prevents enough physical reclamation, the incoming
write is rejected atomically.

A NOOP sink creates no worker or queue and adds no event reservation to writes.
Graceful shutdown stops accepting writes, drains acknowledged events within the
configured shutdown timeout, then stops the sink worker. An undrained or failed
stream makes shutdown report failure instead of claiming every change was
delivered.

Commit-stream observability includes state, queued event count, queued bytes,
last assigned sequence, last acknowledged sequence, rejected writes, and first
failure details.

`CommandSupport.recordMutation`, command-context `changedAny` emission, and
`DbChangeContext` ThreadLocal delivery are removed from the authoritative path.
User commands, expiration, and eviction all publish through the DB commit
coordinator.

## Heap Mirrors And Thread Confinement

The stable allocator removes its `HashMap<Long, Allocation>`. Live object
location, capacity, kind, and state come from the native object table. The page
allocator keeps page-level indexes only. An object view resolves an immutable
block location after pinning the handle.

`StringRoot` no longer tracks every live handle in a boxed `HashSet<Long>`.
Keyspace lifecycle releases owned values, and final allocator close performs a
leak validation before forced cleanup. Quarantine, free-slot, and defrag
candidate collections use primitive storage.

Production allocator methods replace method-wide synchronization with the same
owner-thread guard used by the DB. Standalone tools that require concurrent
allocator access use an explicit synchronized adapter. Pin, epoch, quarantine,
and stale-generation checks remain active even on the thread-confined path.

## Bounded Maintenance

Incremental rehash, expiration cleanup, eviction sampling, empty-page trim, and
defrag each accept limits for inspected objects or slots, moved or reclaimed
bytes, and elapsed time. An empty slot consumes work budget.

Maintenance metrics report pending rehash tables, expired entries awaiting
physical deletion, reclaimable pages, defrag backlog, and the reason the last
tick stopped. This makes persistent maintenance debt distinguishable from a
healthy idle system.

The existing one-owner architecture remains the intentional throughput ceiling.
Netty I/O thread count does not change DB mutation parallelism.

## Module Boundaries

Architecture rules are extended so `yierdis-db-memory` may not depend on
command, protocol, executor implementation, Netty, or server runtime packages.
The currently unused `yierdis-server-runtime-api` dependency is removed from
`yierdis-db-memory`.

Memory snapshots and commit-stream reservation contracts live in the lowest
API module that owns their semantics. Runtime composition depends on those
contracts; DB memory implementation does not depend upward on runtime.

## Delivery Stages

### Stage 1: Allocator And Usage Foundation

- Add empty-footprint and page-trim regression tests.
- Introduce segmented object metadata and primitive slot structures.
- Remove per-object allocator heap mirrors and production locks.
- Add authoritative component memory snapshots.
- Preserve existing allocator contracts and handle safety.

### Stage 2: Failure-Atomic Mutation And OOM

- Add fail-on-allocation-N tests for every mutation family.
- Introduce prepared mutations and non-allocating commit paths.
- Converge native capacity exceptions and command translation.
- Add DB degraded-state behavior for invariant failures.

### Stage 3: Bounded Hash Tables

- Add churn, collision, shrink, compact, and sparse-scan regression tests.
- Add keyed SipHash.
- Introduce incremental grow, compact, and shrink behavior.
- Integrate rehash work with owner-thread maintenance budgets.

### Stage 4: Maxmemory Convergence

- Replace global/per-DB accounting with component snapshots.
- Trim empty pages before and during eviction.
- Repair MEMORY STATS and INFO semantics when maxmemory is disabled.
- Make the existing global maxmemory integration tests pass without weakening
  their assertions.

### Stage 5: RESP Ingress Admission

- Add partial-request, huge-argc, huge-bulk, disconnect, and multi-connection
  budget tests.
- Transfer inbound memory leases through request execution.
- Integrate autoRead transitions and early allocation rejection.
- Add protocol fuzz and leak assertions.

### Stage 6: Commit Stream And Final Convergence

- Add ordered delivery, queue-full, sink-failure, expiry, eviction, and shutdown
  tests.
- Replace manual change recording with DB commit receipts.
- Add commit-stream health and metrics.
- Remove temporary adapters, strengthen architecture rules, update operational
  documentation, run soak checks, and enforce benchmark gates.

## Testing Strategy

Every behavior change follows red-green-refactor. A production change is made
only after its focused test has failed for the expected reason.

Focused verification uses JDK 25 and the smallest reactor containing the
changed module. Each delivery stage then runs the full JDK 25 Maven test suite.
Architecture rules run separately if an earlier reactor failure would otherwise
skip them.

Required test groups are:

- native handle and allocator contracts;
- page allocation, trim, and FFM runtime leak tests;
- DB mutation fault-injection matrix;
- collection direct operations and encoding transitions;
- key directory and NativeByteMap churn/rehash tests;
- global and per-DB maxmemory integration tests;
- RESP decoder, protocol error, queue rejection, and connection lifecycle tests;
- commit-stream ordering, failure, and shutdown tests;
- architecture dependency rules;
- end-to-end client and Redis compatibility tests.

## Performance And Soak Gates

The performance comparison uses the pre-change main commit as baseline and the
existing release benchmark environment. GET, SET, HSET, and ZADD run identical
warmup, client, pipeline, data-size, request-count, and repeat settings. The
median operations per second for each command must be at least 90% of baseline.
Raw artifacts are retained for review.

Hash-table and maintenance stress tests additionally verify that a single
operation never performs more than its configured slot/object budget, apart
from fixed setup overhead.

The bounded soak runs repeated fill, expire, delete, trim, and refill cycles for
at least ten minutes. After warmup:

- live handles return to the cycle baseline after cleanup;
- FFM live-region count returns to the cycle baseline;
- total committed native bytes return to the metadata high-water mark plus the
  configured warm-page bounds;
- commit-stream and inbound-memory leases return to zero;
- RSS does not grow monotonically across the final three completed cycles.

An RSS observation alone is not a correctness oracle; allocator, region, handle,
and lease counters are the required leak assertions.

## Acceptance Criteria

- The full JDK 25 Maven test suite passes.
- The four architecture dependency rules pass together with the new DB/runtime
  boundary rule.
- The two currently failing `MaxmemoryScopeTest` cases pass.
- Sixteen empty DBs commit zero object-table metadata bytes.
- Empty object-table memory is independent of `nativeSlotCapacity`.
- Every injected expected resource failure leaves data, TTL, accounting,
  handles, and commit sequence unchanged.
- Expected native capacity failures return Redis OOM and keep the connection
  usable.
- Million-operation key and member churn compacts or shrinks capacity and does
  not leak native handles.
- Sparse SCAN and rehash operations obey actual-slot work budgets.
- Multi-connection partial RESP pressure stays within configured inbound budget
  and releases all leases.
- Commit-stream delivery is ordered and no sink failure is silently ignored.
- Empty native pages are reclaimed under pressure and maxmemory can make forward
  progress after eviction.
- GET, SET, HSET, and ZADD median throughput each remain within the approved 10%
  regression limit.
- The bounded soak satisfies all counter and RSS conditions.

## Documentation

Implementation updates must keep these project documents consistent with the
new behavior:

- native allocator and handle lifecycle;
- native memory runtime and page reclamation;
- DB internals and prepared mutation flow;
- maxmemory and eviction accounting;
- executor and inbound backpressure;
- protocol limits and request rejection;
- change events and commit-stream failure handling;
- configuration, INFO/MEMORY STATS, testing, and debugging guidance.
