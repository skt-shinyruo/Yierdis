# DB And FFM Simplification Design

## Status

Approved in conversation on 2026-07-31 as stage 3 of the four-stage project
simplification program. Performance is explicitly not an acceptance criterion.

## Program Context

The approved implementation order is:

1. simplify the request and command path;
2. simplify networking and executor state machines;
3. simplify the DB and FFM implementation;
4. simplify Maven modules and test topology.

Stages 1 and 2 are complete at commit `495aab3d`. This design covers only
stage 3. Module moves and test-topology changes remain stage 4 work.

## Baseline

The JDK 25 baseline reactor for `yierdis-memory-ffm`, `yierdis-db-memory`, and
the integration tests passes all 29 selected reactor projects. The focused FFM
module runs 106 tests and the integration module runs 234 tests with no
failures.

Production Java baselines are:

| Module | Lines |
| --- | ---: |
| `yierdis-db-memory` | 29,420 |
| `yierdis-memory-ffm` | 5,222 |

The DB currently stores each deadline twice: in `EntryRecord.expireAtMillis`
and in a separate 1,418-line native expire hash table. Every write then stages
and commits both representations. The FFM backend similarly retains two
implementation layers and several custom indexes introduced to avoid scans,
copies, and temporary allocations.

Those shapes optimize throughput and allocation counts. They do not define the
project's core behavior and are removable under the approved constraints.

## Goals

- Make `EntryRecord.expireAtMillis` the only TTL state.
- Preserve lazy expiry, bounded active expiry, synthetic expiry commits, and
  all Redis-visible TTL results.
- Remove the expire hash table and its second prepared-commit protocol.
- Consolidate DB composition and context-bound operation facades that own no
  independent invariant.
- Keep the FFM stable-handle, native metadata, page/span, pin, epoch, scope,
  defrag, and physical-accounting model.
- Replace FFM performance-oriented indexes and COW checkpoints with direct
  owner-thread collections and scans.
- Produce a substantial net reduction in production Java.

## Non-Goals

- Do not change supported commands, wire replies, error messages, encodings,
  transaction behavior, commit-stream behavior, or owner-thread rules.
- Do not replace native DB storage with heap maps or expose raw addresses or
  `MemorySegment` outside the FFM implementation.
- Do not replace page-backed allocation with one Arena per object.
- Do not remove stable handles, generation checks, native metadata, size
  classes, page trim, epochs, quarantine, allocation scopes, or defrag.
- Do not remove packed/listpack, quicklist, hash-table, skiplist, or HLL value
  encodings; `OBJECT ENCODING` makes those choices observable.
- Do not remove prepared mutation or maxmemory admission boundaries.
- Do not merge or move Maven modules in this stage.
- Do not use throughput, latency, benchmark, or allocation-count thresholds as
  acceptance gates.

## Frozen Cross-Cutting Contracts

### Owner And Lifecycle

1. A DB and its backend bind only through `bindToCurrentThread()`.
2. Unbound, foreign-thread, closing, and closed access retain their current
   failure behavior.
3. An unbound instance may shut down; once bound, shutdown requires the owner.
4. Shutdown is idempotent, attempts every owned cleanup, preserves the primary
   failure, and attaches later failures as suppressed.
5. Views, pins, epochs, allocation scopes, regions, prepared mutations, and
   streamed results keep one explicit owner and one terminal release.

### Stable Identity

1. `NativeHandle` identity remains the complete `(allocatorId, localRaw)` pair.
2. Allocator ownership is checked before local-handle decoding.
3. Domain, kind, slot, state, and generation validation remain fail-fast.
4. Released handles stay stale; generation exhaustion retires a slot instead
   of wrapping.
5. Reallocation, defrag, rehash, and topology replacement preserve every live
   public handle.
6. The key directory owns key bytes. Entry records and secondary structures
   borrow the complete native key identity and never free it independently.

### Prepared Mutation

The visibility order remains:

```text
reserve/admit
  -> prepare invisible state
  -> reconcile reservation
  -> reserve commit record
  -> commit exactly once
  -> promote/settle native scope and ledger
  -> publish commit
  -> release superseded state
```

Failures before commit roll back staged state. Once commit begins, the system
does not claim rollback or retry; cleanup settles ownership and degrades the DB
when the existing contract requires it. Simplifying TTL removes one participant
from this protocol, not the protocol itself.

### Accounting

Logical mutation accounting and physical memory usage remain separate. Global
maxmemory admission continues to use the aggregated physical snapshot exactly
once. Native metadata, committed data, live data, reclaimable pages, retained
heap estimates, reservations, and shared runtime counters keep their current
meanings.

## DB Target Design

### One TTL State

`EntryRecord.expireAtMillis` is authoritative:

- `-1` means persistent;
- any non-negative value is the absolute expiry deadline in milliseconds;
- TTL reads load the current entry record;
- value rewrites carry the current deadline forward or deliberately replace it;
- delete and flush release the entry without a secondary TTL cleanup step.

Delete these types:

- `YierdisExpireIndex`;
- `YierdisNativeExpireIndex`;
- `PreparedTtlMutation`.

`PreparedEntryMutation` publishes one entry replacement or deletion. Setting or
clearing a TTL is already atomic with that entry publication, so there is no
second prepare, commit, release-superseded, or abort participant.

Direct test-only TTL mutators that bypass `YierdisTtlOps` are removed. Tests
create expiry through the same write API as production.

### TTL Semantics

The following behavior is unchanged:

- missing key returns `-2` from TTL/PTTL and false from expiry mutations;
- persistent key returns `-1`;
- non-positive relative and past absolute deadlines delete immediately;
- overflowing relative or seconds-to-milliseconds conversion clamps to
  `Long.MAX_VALUE`;
- reads lazily reclaim expired entries;
- active cleanup reclaims expired entries without a read;
- expiry publishes the existing synthetic `DEL` record with `EXPIRED` kind;
- commit-stream unavailability marks an expired entry as awaiting physical
  deletion and preserves it until publication can proceed;
- setting a new value without an expiry clears an old deadline where the
  existing command semantics require it.

### Bounded Active Expiry

The active cleaner scans the key directory with its existing cursor contract.
It performs bounded work per call and retains the next cursor between calls.
It collects the complete key identity and expected `EntryRecord` during a
scan, exits the directory traversal, and only then calls the
mutation/reclamation path. Each candidate is revalidated against the current
entry handle, record version, and deadline before deletion. This prevents
structural mutation during directory iteration and prevents a stale scan
candidate from deleting a replacement value.

The cleaner remains bounded by the configured time limit and a fixed maximum
number of inspected keys. Repeated maintenance calls eventually traverse the
whole current keyspace. Lazy expiry remains the immediate fallback, so an
expired key is never returned merely because the active scan has not reached
it.

The cleaner advances past a batch only after every candidate was deleted or
proved stale. A failure before entry publication keeps the batch start cursor
so the candidate remains retryable. A failure after publication retains the
committed deletion and follows the existing result-unknown health contract.

### TTL Statistics And Maxmemory

The public `YierdisMemoryStats` record keeps its field shape for protocol and
observability compatibility:

- `expireCount` is derived from current entry records;
- `expireRehashing` is false;
- expire table capacities and overhead are zero;
- expire value-object bytes remain zero.

Changing a deadline in an already allocated `EntryRecord` does not create
physical memory. TTL mutation admission therefore reserves only growth caused
by the actual entry/value operation. Tests that require a TTL update to create
a new native region are implementation-shape tests and are rewritten to assert
accurate physical accounting and unchanged TTL behavior.

### DB Composition And Context

After TTL convergence, consolidate the remaining pass-through ownership:

- use validated `DbEngineConfig` directly and remove duplicate
  `YierdisDbConfig` state;
- collapse the single-implementation `YierdisDbInternals` boundary into the
  runtime implementation unless a test double still protects a real fault
  boundary;
- replace the large mutable component bag with small immutable records grouped
  by lifecycle and operation ownership;
- bind `MutationContext` directly in immutable write/lifecycle views instead of
  retaining a method-by-method forwarding facade;
- remove callbacks, accessors, and legacy mutation helpers with no production
  caller.

Independent contextual views must remain independently bound and may be
interleaved. Owner checks occur at the same public operation boundary. A
prepared mutation's explicit context still takes precedence where the current
contract defines it.

Custom hash maintenance for the key directory and encoded collection values is
not removed in this stage. It participates in scan generations, staged
replacement, failure rollback, and public encoding behavior; mixing that risk
with TTL and allocator convergence would weaken reviewability.

## FFM Target Design

### Boundary

Keep:

```text
StableMemoryBackend API
allocator-scoped NativeHandle identity
native object-table segments and 36-byte metadata slots
64 KiB small pages and rounded medium/large spans
pin + epoch + quarantine reclamation
allocation-scope rollback
active defrag, trim, and physical accounting
```

Remove:

```text
public backend -> private allocator forwarding
segmented page-directory indexing
intrusive multi-list page indexes
copy-on-write checkpoint machinery
parallel arrays for retired moved blocks
allocation-free access specializations and test-only span wrappers
```

### Backend Convergence

Move the implementation of `YierdisStableNativeAllocator` into
`YierdisFfmStableMemoryBackend` and delete the internal allocator facade. The
public backend owns allocator identity, owner checks, object/page state,
external regions, counters, and the close sequence directly.

A package-private constructor may retain the defrag validator injection used by
fault tests. Tests may use narrow package-private inspection, but must not keep
a second production facade alive.

### Page Registry

Delete `YierdisNativePageDirectory`. `YierdisNativePageAllocator` owns a
conventional page-ID map, reusable-ID set, next ID, and creation sequence.
Allocation, warm-page selection, trim, statistics, and growth estimation may
scan those collections.

Small allocations retain size classes and never cross a page. Larger requests
retain rounded spans. Direct page lookup still validates page class, alignment,
offset, capacity, and ownership. Normal free retains at most one warm empty
page per size class; bounded trim closes only empty pages and preserves budget
stop reasons.

An allocation-scope page checkpoint records the creation sequence and an eager
copy of reusable IDs. Abort frees tracked objects, closes empty pages created
by the scope, and restores ID state. Java allocation during abort is allowed.

### Object Table

Keep FFM-backed 4,096-slot segments, packed metadata fields, generation
retirement, and all validation. Replace segment capacity arrays, availability
queues, queued flags, COW references, and abort allocation traps with an exact
segment array and scans for a reusable slot.

The scope checkpoint stores the baseline segment count. After tracked handles
are freed, abort closes new empty tail segments and truncates the array.
Generation advances in an existing segment are not rolled back.

### Epochs And Retired Blocks

Integrate epoch tracking into the backend as a list of owner-bound scopes and a
monotonic epoch counter. Reclamation scans active scopes to determine whether a
retired epoch is still visible.

Replace correlated arrays of page IDs, offsets, capacities, page classes, and
epochs with one `RetiredBlock(block, epoch)` list. Each record owns one old
block. Reclamation closes it once and removes the record only after successful
release.

Pinned free, epoch free, moved-block retirement, quarantine counters, and final
unpin/epoch-close reclamation retain their current semantics.

### Allocation Scope

- only one scope may be active;
- it tracks allocations in encounter order;
- explicit free removes a tracked handle;
- `promote()` transfers ownership;
- `abort()` and unpromoted `close()` release remaining handles in reverse order;
- terminal operations are idempotent;
- committed memory returns to the pre-scope baseline;
- `growth()` remains the peak since scope entry, including transient growth.

The old promises that opening a scope does not copy and abort does not allocate
are removed. Conservative bookkeeping and growth estimates remain required.

### Reallocation, Defrag, And Access

`NO_MOVE`, pin rejection, prefix preservation, failure-before-publication
rollback, stable handle identity, defrag budgets, validator rollback, and
report counters remain unchanged.

Delete `YierdisFfmSpan` and test-only block/region accessors. Object views may
reuse `NativeObjectView` default copy, comparison, and typed access after a
complete upfront range and writability check. Invalid multi-byte writes must
still fail before changing any byte.

### FFM Close And Accounting

`memoryUsage()` and `stats()` retain their public shape and count every retained
container conservatively. External regions are counted once. Empty backends
remain lazy. Clean shutdown returns runtime live-region count to baseline.

Closing with live objects, pins/views, scopes, epochs, or regions reports a
lifecycle leak but still attempts runtime cleanup. Add the missing regression
for an active epoch with no live object.

## Tests

Retain and strengthen behavioral coverage for:

- owner binding and shutdown;
- complete native handle identity and stale generation;
- mutation prepare/commit/rollback/publication failures;
- TTL command results, lazy expiry, active expiry, synthetic commits, and
  deferred physical deletion;
- physical maxmemory accounting without a dedicated expire allocation;
- streamed result ownership;
- pin, epoch, quarantine, scope, reallocation, defrag, page trim, and leak
  cleanup;
- FFM DB lifecycle, empty footprint, and off-heap leak integration;
- architecture boundaries and public API shape.

Delete or rewrite tests that freeze only these private performance shapes:

- dedicated expire-table capacity, rehash, packed layout, and replacement
  protocol;
- TTL updates allocating an FFM region;
- allocation-free byte access and decoder paths;
- exact internal collection fields or absence of ordinary collections;
- unsynchronized method reflection checks;
- no-copy scope opening, COW reference retention, abort-no-allocation, and
  snapshot-no-iteration rules.

No test may be ignored, disabled, or quarantined to complete the stage.

## Migration Order

1. Freeze missing TTL, owner, epoch-close, and accounting behaviors.
2. Remove the expire index and prepared TTL participant.
3. Replace active expiry with the bounded key-directory scan.
4. Consolidate DB composition and contextual facades.
5. Merge the FFM facade and allocator.
6. Integrate epoch scopes and explicit retired blocks.
7. Rewrite the page registry and delete the page directory.
8. Simplify object-table segments and scope checkpoints.
9. Remove dead access helpers and update project documentation.
10. Run focused, affected-reactor, architecture, integration, and full-reactor
    verification with JDK 25.

Every step must leave a compiling, tested tree. Page and object-table rewrites
remain separate commits.

## Acceptance Criteria

- Public DB, memory, command, and protocol behavior above is unchanged.
- `EntryRecord.expireAtMillis` is the only retained TTL state.
- No expire-index production type or dual TTL commit protocol remains.
- `YierdisFfmStableMemoryBackend` is the sole public FFM implementation and no
  second allocator facade remains.
- The FFM page/span and native metadata model remains present.
- New retained collections are included conservatively in memory estimates and
  cleared during shutdown.
- `yierdis-db-memory` production Java decreases by at least 1,700 lines from
  29,420.
- `yierdis-memory-ffm` production Java decreases by at least 1,000 lines from
  5,222.
- Stage 3 removes at least 2,700 production Java lines in total.
- Focused modules, affected reactor, architecture tests, integration tests, and
  the full JDK 25 reactor pass with no performance gate.
