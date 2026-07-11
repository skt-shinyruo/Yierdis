# Single-Node Production Hardening Design

## Status

Approved design for hardening Yierdis as a production-oriented single-node
Redis-style server. RESP behavior and command semantics remain compatible within
the configured resource limits. Existing CLI options keep their meanings;
bounded ingress, egress, and commit-stream failure behavior is added explicitly.
Internal Java APIs may change.

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
- Replies are built in unbounded allocator-chosen `ByteBuf` instances before
  Netty write admission. Queue rejection and protocol errors write directly on
  event-loop threads, so a later `BUSY` or protocol error can overtake an
  earlier accepted command reply on the same pipelined connection.
- Several reply paths first materialize heap copies or complete lists, including
  KEYS, SCAN, counted LPOP/RPOP, HGET, and SET GET. Their memory is outside both
  request admission and Netty's output-buffer watermarks.
- Change events depend on command handlers manually recording mutation
  outcomes and silently discard sink failures.
- Live native objects also have per-object heap mirrors. The allocator keeps
  append-only page/span lists, the FFM runtime keeps a concurrent set of every
  live region, and collection roots keep boxed handle-to-adapter maps.
  Production allocator operations also pay synchronization costs despite
  owner-thread confinement.

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
- Preserve per-connection request order across command, rejection, protocol,
  and internal-error replies.
- Bound global, per-connection, and per-reply outbound memory before allocating
  reply buffers, and remove full-result heap materialization from production
  reply paths.
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
- Do not make a reply durable after its client connection closes. Connection
  loss may discard unsent read results and makes the outcome of an already
  committed write unknown to that client.

## Compatibility Boundary

The following behavior remains compatible for commands admitted within the new
resource limits:

- RESP command names, argument rules, success replies, and existing command
  error semantics.
- Existing CLI option names and meanings.
- `maxmemoryBytes=0` continues to disable enforcement and eviction.
- One owner thread continues to serialize all DB mutations and maintenance.
- Redis-compatible MULTI/EXEC behavior remains command-by-command rather than
  rollback transactional.

Internal allocator, DB mutation, memory reporting, change-event, hash-table,
reply-source, and networking egress APIs may be replaced. New optional
configuration and observability fields may be added without making existing
options invalid.

Expected resource failures use the existing Redis maxmemory OOM reply. A full
or failed commit stream uses `BUSY commit stream unavailable`. A DB that detects
an internal invariant failure uses `MISCONF DB is in a degraded state; writes
are disabled`. A request that exceeds decode memory limits uses `ERR request
exceeds configured memory limit` and closes the connection after the reply.
Reply order follows request decode order, including executor rejection and
protocol errors. The new intentional compatibility exception is that a reply
which cannot fit the configured single-reply hard limit closes the connection
without a replacement error reply. Once a write command or EXEC may have
committed, an egress failure closes the connection and never substitutes
`BUSY`, OOM, or another reply that could imply the mutation did not occur.

The existing `clientOutputBufferLimitBytes` and
`clientOutputBufferOverLimitMillis` options retain their Netty writability and
slow-client soft-limit meanings. New outbound-admission options impose
allocation-time hard limits; they do not reinterpret or replace the existing
soft watermark.

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
  -> bounded ordered RESP egress
```

Each stage produces a working, independently testable repository. Temporary
adapters may bridge adjacent stages, but each adapter is removed in the stage
that replaces its final consumer.

## Architecture

The module dependency direction remains unchanged. Runtime composition injects
low-level contracts into networking and DB components; storage and memory never
depend back upward on runtime:

```text
instance runtime composition
  |-- owns inbound budget, outbound budget, child channels, and commit stream
  |-- injects DbCommitPublisher into each DB
  |
  +-> networking -> connection reply sequencer -> server execution
                           ^                         |
                           |                         v
                    bounded reply sink       command capabilities
                                                     |
                                                     v
                                             DB mutation coordinator
                                             reserve, prepare, commit, abort
                                                     |
                                                     v
                                             storage structures
                                                     |
                                                     v
                                             native allocator
```

The command layer no longer decides whether a mutation occurred. The DB
mutation coordinator is the only logical commit authority. It returns a commit
receipt and publishes the corresponding event through an already-reserved
commit-stream slot.

All writes use this lifecycle:

```text
prepare
  reserve maxmemory
  allocate every capacity-sensitive native and heap resource
  construct an invisible prepared mutation
  reconcile measured physical growth against the admitted upper bound
  if the prepared outcome changed, prefill and reserve a commit-stream ring slot

commit
  mark visibility commit as started
  switch visible state without capacity-sensitive allocation
  promote provisional allocator resources
  commit memory accounting
  publish the prefilled ring slot and its candidate sequence without allocation

abort
  release staged resources, memory reservation, and event reservation
  leave the previous visible state unchanged
```

Pure physical reclamation uses the same prepared lifecycle but a distinct
`RECLAMATION` admission mode. Expiry and eviction deletion plans must have zero
positive persistent growth and a non-positive committed delta. They obtain a
zero-byte ledger token without recursively invoking expiry cleanup, eviction,
or the global maxmemory governor. This prevents maxmemory admission from
re-entering itself while it is already trying to free memory.

The central invariant is that every successful-path operation after visibility
commit starts is prevalidated, allocation-free, and unable to fail because of
slot, queue, native, heap, or configured memory capacity. An unexpected
exception after commit starts is treated conservatively as post-visibility: the
allocator scope is promoted, no logical rollback is attempted, and the DB is
degraded.

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

The exception is an uncommitted mutation allocation scope. A segment created
only for invisible staged handles is provisional until storage commit. Aborting
the scope frees its staged handles and closes provisional empty segments, so an
expected preparation failure restores the pre-command physical snapshot. Once
promoted after visible commit, a segment follows the retained-generation rule
above.

The table reports metadata committed bytes, active segments, live slots, free
slots, retired slots, and peak live slots without walking all configured slots.
Its segment objects, primitive free stacks, retired bitmaps, and growable
segment directory are included in allocator heap estimates.

## Native Page Reclamation

Small native pages remain size-class based. Freeing the last block marks a page
empty. Normal allocation may retain at most one empty warm page per size class.
A pressure trim closes every empty data page, removes it from page indexes, and
releases its FFM region.

Page tracking has no append-only history. Small pages participate in intrusive
per-size-class non-full and empty lists, spans participate in an intrusive live
list, and all page classes are addressed through a segmented primitive page
directory. Closing a page removes it from every list and directory in O(1).
Page allocation ids are returned to a primitive free-id stack only after the
page is no longer referenced by live metadata, pins, defrag state, or
quarantine; a span consumes one allocation id regardless of its page count.
Directory segments with no live page references are released, so repeated
medium/large allocation churn cannot grow heap metadata according to lifetime
allocation count.

Medium and large spans continue to close when their allocation is released.
All page-level accounting is updated before region close is reported complete.

Small pages created inside an uncommitted mutation allocation scope are also
provisional. Abort closes a provisional page after freeing the scope's staged
blocks instead of retaining it as the normal warm page. Promotion after visible
commit makes it eligible for ordinary warm-page retention.

The page allocator exposes bounded trim operations by inspected page count,
closed bytes, and elapsed time. Maxmemory enforcement runs pressure trim before
selecting additional eviction victims and again before declaring OOM.

Page objects, primitive free-offset stacks, span descriptors, primitive page
directories, quarantine storage, and defrag cursors are allocator-owned heap
memory. They are reported and estimated rather than being treated as zero-cost
metadata. Page-id lookup uses a primitive or segmented directory; it does not
reintroduce boxed per-page keys.

The FFM runtime keeps atomic live-region and byte counters rather than a
`ConcurrentHashMap`-backed region set. Region wrappers, arenas, segments, and
allocator-owned references receive conservative per-region heap estimates.
Leak validation reports the remaining count and bytes; resource owners remain
responsible for closing their own regions.

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
  effectiveBytesForMaxmemory (derived, never independently supplied)
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

Snapshot construction is O(1) in key and collection count. Persistent Java
structures maintain owner-thread-confined aggregate counters when adapters,
quicklist/listpack storage, skiplist nodes, intsets, active/old hash arrays, or
maintenance registrations are created, replaced, retired, or closed. A memory
snapshot sums a fixed number of top-level component counters; it never walks
the keyspace or all collection adapters during write admission.

The allocator snapshot includes its retained Java control structures as
`heapEstimatedBytes`; it does not report heap zero merely because payload bytes
live off heap. Allocation planning and invisible staging use one smaller growth
contract:

```text
NativeAllocationGrowth
  heapEstimatedBytes
  nativeMetadataCommittedBytes
  nativeDataCommittedBytes
  effectiveBytes (saturating derived sum)
```

`NativeAllocator.estimateAdditionalGrowth(int...)` simulates a complete batch,
including directory/array growth for new metadata segments and pages.
`NativeAllocationScope.growth()` reports the corresponding measured growth for
the active invisible scope. This makes admission and reconciliation use the
same physical components.

`maxmemoryBytes=0` skips enforcement but still produces complete statistics.
`offheap_included_in_maxmemory` describes the accounting model rather than
whether a governor object happens to be attached.

Saturating arithmetic is required for every estimate and aggregation. A
saturated estimate rejects growth rather than wrapping into an admissible
negative value.

Before maxmemory admission, each mutation computes a conservative peak
physical-growth estimate. The native allocator simulates the allocation batch
against current metadata slots and size-class free blocks, including whole
metadata segments and pages that would need to be committed. Mutation code adds
exact FFM replacement-region and conservative heap-array growth. Maxmemory
reserves this peak; steady-state `actualDeltaBytes()` remains the ledger delta
reconciled after preparation.

## Prepared Mutations

Storage writes return a staged mutation:

```java
interface PreparedMutation<T> extends AutoCloseable {
    long actualDeltaBytes();
    long stagedNonNativeGrowthBytes();
    T commit();
    void releaseSuperseded();
    void abort();
}
```

`stagedNonNativeGrowthBytes()` reports the actual invisible FFM-region and heap
growth held outside the native allocator; the executor adds
`NativeAllocationScope.growth().effectiveBytes()` and reconciles that peak
against admission before visibility changes. `abort()` and
`releaseSuperseded()` are idempotent.
`close()` aborts an uncommitted mutation. `commit()` may update pointers,
counters, primitive arrays, and already-allocated nodes, but may not grow a
table, create a native object, reserve memory, reserve an event, or release
resources that were authoritative before the visible switch. The executor
invokes `releaseSuperseded()` only after memory accounting and event publication
are committed.

The mutation executor opens one owner-thread-confined native allocation scope
before storage preparation. Native allocations made during preparation belong
to that scope. Before visible commit, failure aborts prepared heap/FFM state and
the native scope, including provisional segments and pages. Immediately after
the visible switch, the executor promotes the scope through a non-throwing,
allocation-free transition so committed handles cannot later be reclaimed as
staging.

`PreparedMutation` has normal states `PREPARED`, `COMMITTING`, `COMMITTED`,
`RELEASED`, and `ABORTED`. `abort()` acts only on `PREPARED`; once `commit()`
enters `COMMITTING`, failure can no longer trigger staging rollback because
some visibility writes may already have occurred.

`YierdisDbMutationExecutor` performs these steps:

1. Check owner-thread and DB health.
2. Reserve the operation's upper-bound memory estimate.
3. Open a native allocation scope and prepare the value, entry, key-directory,
   and TTL-index changes invisibly.
4. Reconcile the memory reservation with measured allocator growth plus staged
   non-native peak growth.
5. If the prepared outcome changed, retain its immutable command record and
   reserve a prefilled commit-stream slot when a sink is enabled. The slot owns
   the candidate sequence and callback payload before visibility changes.
6. Mark commit started and switch visible storage state.
7. Promote the native allocation scope without allocation or failure.
8. Commit the memory ledger through its prevalidated counter assignment.
9. Publish the reserved slot through an allocation-free, non-retaining state
   transition.
10. Release superseded objects whose ownership moved during commit.

Normal user writes use `NORMAL` admission. Known deletion-only commands,
passive/active expiry deletion, and eviction use `RECLAMATION` admission. The
executor rejects a reclamation plan before visibility if its upper bound,
measured staged persistent growth, or native-scope growth is positive, and it
rejects it as an invariant if its steady-state delta is positive. Its ledger
token bypasses cleanup and governor callbacks but still settles the negative
committed delta exactly once.

If steps 2 through 5 fail with an expected resource exception, the executor
aborts all staging and returns the Redis OOM error. The connection remains open
and no event is emitted.

An unexpected invariant exception before commit starts aborts staging and marks
the DB degraded. An exception after commit starts never calls `abort()` or
rolls back the promoted scope; it settles accounting conservatively, marks the
DB degraded, and preserves any event that was already published. If a real
event reservation is still unpublished, the publisher converts it to a held
failed reservation and transitions the stream to FAILED through an
allocation-free, non-throwing operation; it is never silently canceled after
visibility may have changed. Reads remain available. Subsequent writes receive
the documented MISCONF error. Health and INFO output expose the first
degradation cause.

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

SCAN cursors include table generation and rehash phase. Migration leaves a
non-authoritative scan shadow in the old slot until that old table retires; DB
lookup and mutation paths ignore the shadow, while SCAN may still return it.
If retirement makes a cursor stale, scanning restarts at the current active
table. Therefore a complete iteration does not omit a key that exists for the
entire iteration, but may return a key more than once during rehash, matching
Redis-style weak iteration semantics.

The decimal cursor remains a non-negative 63-bit value and carries only a
29-bit generation token. The full table generation remains a monotonic `long`.
The no-omission statement is explicitly scoped to an iteration spanning fewer
than `2^29` structural generation changes; SCAN cursors are not durable
bookmarks and a cursor held across that horizon has unspecified weak-iteration
results. The implementation never describes the low-bit token as globally
unique, and boundary tests lock this documented horizon so a future change
cannot silently claim an impossible indefinite guarantee from a finite cursor.

All byte-key hashes use a per-instance random SipHash key. Tests inject a fixed
key. The implementation is checked against official SipHash vectors and does
not convert request bytes into `String` values.

Hash-table metrics include capacity, size, filled slots, tombstones, rehash
state, rehash cursor, completed rehashes, and maximum observed probe length.

## RESP Ingress Memory Admission

A shared `InboundMemoryBudget` accounts for request memory from before each
application receive-buffer allocation through the last retained request
consumer. A connection obtains one fixed-size read credit before Netty is
allowed to issue a read; the receive allocator performs at most one credited
buffer allocation per read cycle. With a NOOP change sink this normally ends at
executor completion, except that MULTI queues retain commands until EXEC,
DISCARD, disconnect, or shutdown. An enabled commit stream may retain the
immutable request and lease until event acknowledgment. Its lease moves through
these states:

```text
partial Netty input
  -> argv/reference reservation
  -> copied payload reservation
  -> ExecutionRequest reservation
  -> optional commit-event retention
  -> release after rejection, execution, or event acknowledgment
```

The budget accounts for retained inbound buffer capacity plus conservative
buffer/component metadata, argv array headers and reference slots, copied bulk
arrays including their headers/alignment, inline token arrays, parser frame
storage, transaction-queue views, and complete decoded requests. It never
releases input accounting merely because a reader index advanced; a component's
full charge remains until that component is actually released. A decoded heap
request uses a fixed
32-byte request estimate, `16 + argc * 8` for the outer argv, and
`16 + align8(payloadLength)` for every non-null argument array. Every term uses
saturating arithmetic.

The decoder reserves argv memory before creating `byte[argc][]`. It reserves a
bulk payload before creating its byte array. It checks declared bulk length
against per-command, per-connection, and global limits before allocation. Every
addition uses saturating arithmetic.

The existing protocol limit options keep their meanings. A new optional
`protocolGlobalInFlightBytes` value uses `0` for an automatically derived limit:

```text
max(128 MiB, saturatedMultiply(executorQueueMaxBytes, 2))
```

The per-connection hard limit is conservatively derived as protocol max command
bytes plus 48 fixed bytes plus `protocolMaxArgs * 32`, covering the request,
outer argv, references, per-argument array headers, and worst-case alignment.
The inbound high watermark disables that connection's autoRead. A declaration
that cannot fit its hard limit receives the documented request-memory error and
closes after the reply.

The global budget enters backpressure at 75% and clears at 50%. If an argv or
bulk reservation fits the connection hard limit but current global capacity is
temporarily unavailable, the decoder enters a waiting-for-budget state without
allocating the array. It disables autoRead and registers one waiter. Budget
release schedules that waiter on its channel event loop, where decoding resumes
from the existing cumulator. Waiters are served FIFO and a closed channel is
removed without consuming a wakeup. A single request larger than the global
budget is a hard-limit violation rather than a waiter, because no release can
make it fit. The credited fixed receive size is charged before `ctx.read()`.
An unexpected upstream `ByteBuf` whose retained capacity exceeds its credit
must reserve the difference before entering the decoder; failure releases the
buffer, sends the documented error once, and closes rather than retaining
unaccounted memory.

After usage falls to the 50% low watermark, the FIFO head may be admitted up to
the absolute capacity even if that single reservation crosses 75% and
immediately reenters backpressure. The watermark cannot permanently starve a
valid request whose size lies between 75% and 100% of the global budget.

The Netty cumulator is capacity-accounted. It releases fully consumed
components, keeps partial components charged at retained capacity, and cannot
perform an implicit unadmitted consolidation when the component limit is
reached. A custom consolidation path first reserves the allocator-selected
target capacity, copies, then releases the old component leases; the real
copying peak is therefore visible. New request leases are reserved before old
input leases are released. Transfer admission checks the per-connection hard
limit against the projected post-transfer total, while checking global
capacity against the real temporary peak before allocation. Protocol error,
channel close, decoder reset, transaction discard/connection loss, executor
rejection, skipped closing commands, and normal completion all use an
idempotent release path.

Decoded request leases are detached from Netty object graphs. They may retain a
small connection-account token for the per-connection limit, but never retain a
`Channel`, `ChannelHandlerContext`, decoder, cumulator, or `ByteBuf`. Closing a
budget stops admission and cancels waiters; already-issued leases remain valid
and release their counters later, including from the commit-stream worker.

Executor queued bytes remain a scheduling/backpressure metric. Inbound memory
is a lifetime admission metric. Both are exposed separately and are not added
together as if they represented distinct physical memory.

## Bounded Ordered RESP Egress

The decoder-to-executor ingress bridge registers a connection-local `ReplySlot`
in event-loop receive order before it emits each decoded request or terminal
protocol error downstream. If registration cannot proceed, the decoder remains
in its already-accounted waiting state, retains the decoded request and inbound
lease, disables auto-read, and emits no later message. The slot therefore exists
before executor submission, so an executor rejection fills the already-ordered
slot instead of writing `BUSY` directly. Command replies, queue rejection,
protocol errors, and internal errors all use the same `ConnectionReplySequencer`.
No handler may allocate a reply `ByteBuf` or call `Channel.write` outside this
path.

An error attributable to a decoded request fills that request's slot. An
unattributed pipeline failure may append one terminal internal-error slot after
all registered requests if its control reservation succeeds; otherwise it
closes the channel without a reply. It never overwrites or overtakes an existing
slot.

Each slot has a monotonically increasing connection-local sequence and these
states:

```text
REGISTERED
  -> WAITING_CAPACITY
  -> PRODUCING
  -> READY
  -> WRITING
  -> COMPLETED

REGISTERED | WAITING_CAPACITY | PRODUCING | READY
  -> CANCELLED

PRODUCING | READY | WRITING
  -> FAILED
```

Only the lowest non-terminal sequence may move from READY to WRITING. Once that
slot's chunks have been submitted to the channel in order, the sequencer may
submit the next READY slot; it need not wait for socket acknowledgment between
slots because the Netty channel preserves write order. A close-after-reply slot
prevents later slots from entering WRITING, closes the channel after its final
write future, and cancels every later slot.

The `ConnectionReplySequencer` and its ordered slot queue are channel-event-loop
confined. The command owner performs preflight and synchronous source encoding,
then publishes READY through a one-way handoff that schedules the event loop.
Only the event loop submits channel writes or advances ordered queue state.
Budget counters and the terminal cleanup claim are thread-safe because write
completion, connection close, and shutdown may race the producer handoff.

A slot contains one idempotent terminal cleanup action and an atomic
`cleanupOwner` selected exactly once from `SEQUENCER`, `FINAL_WRITE_FUTURE`,
`CONNECTION_CLOSE`, or `SHUTDOWN`. The selected owner releases the slot's
unconverted reservation, not-yet-submitted chunks, reply source, and any
retained request. Submitted chunk buffers belong to Netty; their write-future
listeners each release only their corresponding chunk lease, while the final
listener may also claim terminal slot cleanup. Channel close,
write failure, producer failure, executor failure, and shutdown all converge on
this transition rather than maintaining independent cleanup paths.

### Outbound Capacity Contract

An instance-level `OutboundMemoryBudget` has three positive hard limits:

```text
global reply capacity        256 MiB
per-connection capacity      128 MiB
single reply total charge     64 MiB
reply chunk payload           64 KiB
per-slot control reservation   4 KiB
```

Embedded and CLI configuration may override these values, subject to
`control reservation <= single reply <= per connection <= global`. Arithmetic
is saturating. The single-reply limit applies to the total admitted charge for
one top-level RESP reply, including an EXEC array: slot/source estimates,
encoded bytes, allocator-visible buffer capacity, and fixed chunk/component
overheads. It is independent of inbound command size and maxmemory.

The embedded fields and matching CLI options are
`replyGlobalCapacityBytes` (256 MiB),
`replyPerConnectionCapacityBytes` (128 MiB), `replyMaxTotalBytes` (64 MiB),
`replyChunkPayloadBytes` (64 KiB), `replyControlReservationBytes` (4 KiB), and
`replyDrainTimeoutMillis` (5000). All are strictly positive. Chunk payload and
control reservation must each fit the single-reply limit after fixed overhead;
startup rejects an invalid combination rather than silently clamping it.

Slot registration reserves the 4 KiB control reservation from both the
connection and global budgets before allocating slot state. That reservation
includes a conservative slot/source metadata estimate and the maximum admitted
BUSY, internal-error, or protocol-error encoding with one chunk and its fixed
components. Parser-generated protocol-error detail is bounded to this contract.
This guarantees control-reply capacity without a second unordered emergency
path. Slot metadata and control replies are charged; they are not free merely
because their payload is small. If capacity is temporarily unavailable, the
ingress bridge remains paused as described above. No replacement error is
allocated outside the budget.

Before visibility-changing execution, each task performs a side-effect-free
reply preflight and supplies a `ReplyPlan` with an exact or conservative total
charge and an optional closeable preflight view. Fixed replies use their exact
charge. Read-only single-value paths may obtain an owned view without copying
payload bytes. SET GET, counted LPOP/RPOP, and similar mutation paths inspect an
O(1) encoded-size description or synchronously visit the still-authoritative
values; ownership transfers only if their later mutation commits. Collection
visitors and replayable sequences perform a count/size pass without copying
elements. A path that cannot know its charge in O(1) first converts the control
reservation into a pessimistic single-reply reservation, then counts and returns
the unused difference before encoding. The plan reserves its full charge before
any reply buffer allocation or mutation. A bound above the single-reply limit
cancels the command before mutation and closes the connection.

If an exact or pessimistic reservation is temporarily unavailable, preflight
closes its view and leaves the task at the schedulable head. FAIR may execute
other connections, so the blocked task reruns preflight against current DB state
when retried; no size result or view survives that rotation. GLOBAL leaves the
task at the FIFO head and reruns preflight after its capacity wakeup. Once
reservation succeeds, the owner thread does not run another command or wait for
capacity until the command constructs its reply source and encoding finishes,
so counted state cannot change between passes.

If a command cannot preflight an exact safe bound before it may mutate, it
reserves the remaining single-reply capacity pessimistically. EXEC always does
this before executing any queued command, so its writes never wait for reply
capacity after commit. Production releases unused reservation as exact chunk
charges become known.

The reply writer emits fixed-size chunks. For each chunk, its allocator-visible
capacity plus a conservative `ByteBuf`, queue-node, promise/listener, and
component overhead charge is converted from the slot's unallocated reservation
to a chunk lease before allocation; conversion does not increase global,
connection, or single-reply usage. The buffer is created with a maximum capacity
no greater than the reserved chunk payload. After allocation, the lease records
the actual `ByteBuf.capacity()` and returns only that chunk's conservative
surplus to the slot reservation. Production completion releases any remaining
unconverted credit. An actual charge above the pre-allocation conversion is an
invariant failure, and that buffer is released rather than admitted
retroactively. Composite and implicit buffer growth paths are forbidden. Empty
final chunks are not allocated.

The slot keeps its admitted reservation while producing, so a synchronous
command never blocks the only command-owner thread halfway through a reply. A
temporary capacity miss occurs during side-effect-free preflight, before any
mutation. Under FAIR scheduling, the head task for that connection remains
queued, the connection is marked
reply-blocked, and the scheduler rotates to another active connection. Under
GLOBAL scheduling, bypassing the FIFO head would change documented ordering, so
the entire drain pauses until a budget-release callback reschedules it. Budget
waiters do not spin and closed connections are removed without consuming a
wakeup.

`inputPausedByReply` is an independent connection state from ingress-budget,
executor-backlog, Netty-writability, and closing pauses. Auto-read is restored
only when every active pause reason has cleared. Reply reservation release
schedules recovery on the command owner and the affected channel event loop;
neither thread waits on the other.

Every submitted chunk uses a real `ChannelPromise`. Its listener releases the
chunk lease on success or failure and initiates connection cleanup on failure.
The existing `clientOutputBufferLimitBytes` logic continues to disable reads
and close persistently non-writable clients as a soft slow-client policy. The
hard outbound budget remains charged until write completion even after a chunk
has left the application queue.

If a producer exceeds its declared bound or the single-reply hard limit, the
sequencer releases all not-yet-submitted chunks and closes the connection. A
read command that has not executed may be canceled without side effects. Once
a write command or any command inside EXEC may have reached visibility commit,
the sequencer never replaces its reply with BUSY, OOM, or an encoded limit
error; disconnect is the only safe signal and the client must treat the result
as unknown. This rule also applies to unexpected reply encoding and write
failures.

### Owned And Replayable Reply Sources

Production command APIs do not return unbounded `List<byte[]>` results. A
closeable reply source owns every value until encoding completes or the slot is
canceled:

- SET GET returns an owned `BulkStringValue` view of the superseded native
  value. Visibility commit transfers that value to the reply source, and slot
  cleanup releases it after encoding or disconnect.
- Counted LPOP/RPOP returns a closeable `PoppedValueSequence`. It synchronously
  visits popped values in reply order without copying them into a Java list.
- HGET returns `BulkStringValue` instead of a detached byte array.
- KEYS and SCAN return a constant-size scan-window descriptor plus a synchronous
  replayable sequence. The first pass counts matched elements and encoded bytes;
  the second pass emits them. Both passes run on the owner thread without an
  intervening command or maintenance step, use one captured expiry-evaluation
  timestamp, and do not advance rehash or physical expiry deletion. Every
  inspected slot in both passes consumes the command's work/time budget. The
  descriptor is discarded and preflight restarts if its table generation is not
  current before reservation succeeds.
- PING and ECHO encode directly from retained request argument slices.
- Existing visitor-based LRANGE, HGETALL, SMEMBERS, and ZRANGE paths gain a size
  pass and continue to stream values rather than build result lists.

The production `RedisReplyWriter.bulkStringArray(List<byte[]>)` API is removed.
Test adapters may build lists only after applying explicit small test bounds.
Every reply source is thread-confined to the command owner and has idempotent
close semantics. It cannot retain a `Channel`, decoder, or mutable command
context.

### Shutdown And Observability

Runtime composition owns an explicit child-channel collection. Shutdown stops
accepting connections, disables child input, drains or cancels executor work,
allows already-ordered replies to flush within the configured shutdown timeout,
then closes every child channel. It waits for all write futures and for global
and per-connection outbound lease counts to reach zero before reporting clean
shutdown. Timeout closes the remaining channels, runs the unique slot cleanup
owner, and reports failure if a lease remains; it never clears counters to hide
a leak.

Outbound observability reports global and per-connection reserved bytes,
allocated bytes, active slots, reply-blocked connections, waiters, peak usage,
hard-limit disconnects, estimate violations, write failures, canceled slots,
and shutdown leaks. Slot, source, buffer, and lease counts must all return to
zero after the final child channel closes.

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
commitAttemptTimestampMillis
```

The user-command event retains the immutable execution request and its memory
lease rather than copying argv again. Expiration and eviction prepare a bounded
synthetic DEL record.

When a non-NOOP sink is configured, mutation prepare first determines the
logical outcome without making it visible. A changed outcome then retains its
immutable command record and reserves a fixed ring slot, queue count, and bytes
before commit. Reservation fills the slot with DB, kind, record, measured delta,
`commitAttemptTimestampMillis`, and the candidate `lastAssignedSequence + 1` while
leaving that sequence unassigned. Commit publishes the slot with a
non-allocating state transition and only then advances `lastAssignedSequence`.
No-op commands reserve no capacity and consume no sequence; the FAILED-state
write-stop check still applies to every write command. Aborted mutations clear
their slot and may reuse the candidate sequence.

The default stream limits are 8192 queued events and 64 MiB of retained event
bytes. Embedded configuration may override either positive value. Graceful
shutdown uses a five-second default drain timeout. These limits are inactive
for a NOOP sink.

Sequence assignment never wraps. Once `Long.MAX_VALUE` has been assigned, the
stream rejects the next reservation before storage visibility with the normal
BUSY stream-unavailable error.

The ring storage is allocated when the stream is created. `publish` never
constructs an event, retains a record, grows a collection, or performs checked
capacity arithmetic. Projection objects used only by the sink worker may still
be allocated after commit; failure to create or deliver a projection transitions
the stream to FAILED while the ring slot and record lease remain intact.

Record ownership and callback access are separate contracts. Producers and
ring slots own an `ImmutableCommandRecord`, while sink APIs receive only a
`CommandRecordView` with byte-reading methods and no `retain()` or `close()`.
The view is valid only on the sink worker thread during `onChange`; every access
after callback return or from another thread fails. A sink that needs data
later must copy it before returning, so acknowledgment cannot hide an
unbounded retained request lease outside queue accounting.

The existing public `ExecutionRecord(int, ExecutionRequest)` constructor keeps
its detached defensive-copy behavior for callers that create standalone replay
records. Commit-stream delivery uses a separate `ExecutionRecord.borrowed(...)`
factory backed by the guarded `CommandRecordView`; that projection owns no
record or lease and becomes unreadable after the callback. This preserves the
public snapshot contract without reintroducing a sink-side ownership escape.

The sink acknowledges an event only after `onChange` returns successfully. On
success it advances `lastAcknowledgedSequence` and releases the event lease. On
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

The sink worker is a named daemon. Shutdown transitions RUNNING to DRAINING and
rejects new reservations. Successful drain acknowledges every queued event and
terminates the worker before CLOSED is reported. On timeout, the stream records
a drain-timeout failure, interrupts the worker, returns failure, and leaves an
in-flight event leased until that callback exits. Java cannot safely kill an
arbitrary callback that ignores interruption. Once it exits, terminal cleanup
releases the head and every other unacknowledged event without advancing the
acknowledged sequence. A failed callback that has already returned permits the
shutdown thread to perform the same cleanup immediately.

Internal slot states are exactly `EMPTY`, `RESERVED`, `QUEUED`, and
`IN_FLIGHT`. All slot transitions and terminal cleanup run under the stream
lock. A `callbackActive` marker plus a single `cleanupOwner` value (`NONE`,
`WORKER`, or `SHUTDOWN`) ensures that the shutdown thread and worker can never
close the same record twice. The shutdown thread may claim cleanup only when no
callback is active and no slot is `IN_FLIGHT`; an `IN_FLIGHT` slot remains
exclusively worker-owned until the callback's `finally` invalidates its borrowed
view. After an ordinary callback failure, the worker changes the retained head
back to `QUEUED` before stopping, which allows later shutdown cleanup. After a
drain timeout during the callback, the worker claims cleanup itself as the
callback exits. Normal instance shutdown drains command-owner work before
entering DRAINING, so an outstanding producer reservation is an invariant
failure rather than something shutdown waits on.

Commit-stream observability includes state, queued event count, queued bytes,
last assigned sequence, last acknowledged sequence, rejected writes, and first
failure details. It also exposes whether graceful shutdown timed out; count and
byte gauges include a RESERVED producer slot because it already consumes ring
capacity even though its candidate sequence is not assigned yet.

`CommandSupport.recordMutation`, command-context `changedAny` emission, and
`DbChangeContext` ThreadLocal delivery are removed from the authoritative path.
User commands, expiration, and eviction all publish through the DB commit
coordinator.

## Heap Mirrors And Thread Confinement

The stable allocator removes its `HashMap<Long, Allocation>`. Live object
location, capacity, kind, and state come from the native object table. The page
allocator keeps page-level indexes only. An object view resolves an immutable
block location after pinning the handle.

`StringRoot` and `EntryTable` no longer track every live handle in boxed
`HashSet<Long>` mirrors. `NativeCollectionRootTable` replaces its
`HashMap<Long, T>` with a lazy segmented reference directory keyed by validated
native slot id; empty adapter-directory segments may be released because native
object metadata remains the generation authority. The page allocator has no
append-only page/span lists, and the FFM runtime has no per-region set.
Keyspace lifecycle releases owned entries and values, and final allocator close
performs a leak validation before forced cleanup. Object-slot free lists,
small-page free offsets, free page ids, quarantine records, and defrag candidate
iteration use primitive storage.

Production allocator methods replace method-wide synchronization with the same
owner-thread guard used by the DB. Standalone tools that require concurrent
allocator access use an explicit synchronized adapter. Pin, epoch, quarantine,
and stale-generation checks remain active even on the thread-confined path.

## Bounded Maintenance

Incremental rehash, expiration cleanup, eviction sampling, empty-page trim, and
defrag each accept limits for inspected objects or slots, moved or reclaimed
bytes, and elapsed time. An empty slot consumes work budget.

Hash maintenance never discovers work by walking every collection in the DB.
Tables register on the owner thread when they acquire resize/rehash debt and
unregister when it clears or the table closes. An intrusive rotating debt
registry provides O(1) add/remove without per-tick snapshots or duplicate queue
nodes. Registry references and participant fields are included in heap memory
estimates.

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

Memory snapshots, inbound/outbound admission, reply sequencing, and
commit-stream reservation contracts live in the lowest API module that owns
their semantics. Runtime composition depends on those contracts. DB APIs expose
only thread-confined closeable or replayable value sources; DB memory never
depends on RESP, Netty, executor implementation, or server runtime.

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
- Make SET GET transfer its superseded native value to a closeable bulk reply
  view, and make counted LPOP/RPOP return a closeable synchronous popped-value
  sequence instead of copied arrays or lists.
- Converge native capacity exceptions and command translation.
- Add DB degraded-state behavior for invariant failures.

### Stage 3: Bounded Hash Tables

- Add churn, collision, shrink, compact, and sparse-scan regression tests.
- Add keyed SipHash.
- Introduce incremental grow, compact, and shrink behavior.
- Make KEYS and SCAN expose replayable, constant-size scan-window sequences with
  bounded discovery/count and emission passes.
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

### Stage 6: Reliable Commit Stream

- Add ordered delivery, queue-full, sink-failure, expiry, eviction, and shutdown
  tests.
- Replace manual change recording with DB commit receipts.
- Add commit-stream health and metrics.

### Stage 7: Bounded Ordered RESP Egress And Final Convergence

- Add reply-order tests covering pipelined command replies, BUSY rejection,
  protocol errors, internal errors, close-after-reply, and EXEC.
- Add global, per-connection, single-reply, chunk-allocation, disconnect, write
  failure, FAIR/GLOBAL scheduling, and shutdown lease tests.
- Route every reply through the connection sequencer and allocate only from
  pre-reserved outbound leases.
- Convert HGET, PING/ECHO, and existing visitor replies to owned or replayable
  sources, and remove production `bulkStringArray(List<byte[]>)`.
- Remove temporary adapters, strengthen architecture rules, update operational
  documentation, run the full soak, and enforce benchmark and acceptance gates.

## Testing Strategy

Every behavior change follows red-green-refactor. A production change is made
only after its focused test has failed for the expected reason.

Focused verification uses JDK 25 and the smallest reactor containing the
changed module. Each delivery stage then runs the full JDK 25 Maven test suite.
Architecture rules run separately if an earlier reactor failure would otherwise
skip them.

Required test groups are:

- native handle and allocator contracts;
- allocator heap/native growth estimate and scope reconciliation contracts;
- page allocation, trim, and FFM runtime leak tests;
- DB mutation fault-injection matrix;
- collection direct operations and encoding transitions;
- key directory and NativeByteMap churn/rehash tests;
- global and per-DB maxmemory integration tests;
- RESP decoder, protocol error, queue rejection, and connection lifecycle tests;
- commit-stream ordering, failure, and shutdown tests;
- reply sequencing, bounded chunk allocation, source ownership, scheduling
  recovery, write completion, disconnect, and shutdown tests;
- architecture dependency rules;
- end-to-end client and Redis compatibility tests.

## Performance And Soak Gates

The performance comparison runs only after Stage 7 against the pre-change main
commit in the existing release benchmark environment. GET, SET, HSET, and ZADD
run identical warmup, client, pipeline, data-size, request-count, and repeat
settings. The median operations per second for each command must be at least 90%
of baseline. Raw artifacts are retained for review. A pipelined large-reply
profile additionally records throughput, peak outbound reservations, allocated
reply capacity, and command-owner pauses; it is diagnostic rather than a new
compatibility threshold.

Hash-table and maintenance stress tests additionally verify that a single
operation never performs more than its configured slot/object budget, apart
from fixed setup overhead.

The Stage 7 bounded soak runs repeated fill, pipeline, scan, pop, expire, delete,
slow-reader disconnect, trim, and refill cycles across multiple connections for
at least ten minutes. After warmup:

- live handles return to the cycle baseline after cleanup;
- FFM live-region count returns to the cycle baseline;
- total committed native bytes return to the metadata high-water mark plus the
  configured warm-page bounds;
- commit-stream, inbound-memory, outbound-memory, reply-slot, reply-source, and
  reply-buffer leases return to zero;
- observed global, per-connection, and single-reply outbound usage never exceeds
  its configured hard limit;
- replies observed before disconnect remain in request order;
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
- Allocator-owned Java structures are included in memory snapshots and in the
  pre-write growth estimate; reconciliation rejects every underestimate before
  visibility changes.
- Expected native capacity failures return Redis OOM and keep the connection
  usable.
- Million-operation key and member churn compacts or shrinks capacity and does
  not leak native handles.
- Sparse SCAN and rehash operations obey actual-slot work budgets.
- Multi-connection partial RESP pressure stays within configured inbound budget
  and releases all leases.
- Commit-stream delivery is ordered and no sink failure is silently ignored.
- Pipelined command, BUSY, protocol-error, internal-error, and close-after-reply
  responses preserve connection request order.
- No reply buffer, reply source, slot, control reply, or write-future lease is
  allocated outside the configured global, per-connection, and single-reply
  hard limits.
- FAIR scheduling continues useful work on other connections while one waits for
  reply capacity; GLOBAL scheduling preserves FIFO by pausing at its blocked
  head without spinning.
- Reply overflow or post-commit encoding/write failure closes the connection and
  never returns a reply that falsely implies a write or EXEC did not commit.
- KEYS, SCAN, counted LPOP/RPOP, SET GET, HGET, PING/ECHO, LRANGE, HGETALL,
  SMEMBERS, and ZRANGE do not materialize unbounded production reply lists or
  detached value copies.
- Graceful and timed-out shutdown close every child channel and leave all
  outbound leases at zero, or explicitly report shutdown failure.
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
- ordered reply sequencing, outbound hard limits, reply-source ownership, and
  slow-client behavior;
- protocol limits and request rejection;
- change events and commit-stream failure handling;
- configuration, INFO/MEMORY STATS, testing, and debugging guidance.
