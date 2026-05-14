# Production Allocator And Stable Handle Design

## Status

Draft spec for a production-grade native allocator and handle ABI.

This spec refines the allocator part of `yierdis.md`. It assumes Yierdis is targeting a production Redis-compatible storage core where key bytes, entries, type roots, indexes, and data-structure nodes live in FFM-managed native memory.

## Goal

Define the allocator contract strongly enough that higher-level storage structures can depend on it as an ABI:

- stable 64-bit handles for all off-heap objects
- explicit memory scale and layout limits
- clear segment, page, span, block, object, and offset semantics
- production-safe allocation, free, and realloc behavior
- active defrag support without requiring every data structure to rewrite physical addresses
- stale handle detection for use-after-free and ABA-style bugs
- snapshot, scan, AOF rewrite, RDB, and replication-safe object movement
- allocator metrics and tests that can prove correctness under long-running workloads

The main design decision is:

```text
Public handles are stable indirect object handles.
Physical locations are allocator-private.
```

Business structures must not encode `segmentId + offset` as durable references. They store stable handles. The allocator resolves a handle through metadata to the current physical address.

## Non-Goals

- Do not design the whole Redis storage engine here.
- Do not implement cluster, replication, AOF, or RDB in this spec.
- Do not expose raw `MemorySegment` or allocator-private offsets to command implementations.
- Do not require active defrag in the first implementation milestone.
- Do not keep compatibility with the current raw `EntryHandle(long)` / `ValueHandle(long)` meaning. Existing handles are migration placeholders, not the production ABI.

## Current State

The repository already has useful foundations:

- `YierdisFfmMemoryRuntime` owns FFM regions and performs leak checks.
- `YierdisFfmSlabAllocator` allocates from slab-backed FFM regions.
- `OffHeapAllocator` exposes a simple buffer API for current string/HLL paths.
- `EntryTable`, `EntryHandle`, `ValueHandle`, `NativeKeyDirectory`, and type roots have introduced a handle-shaped storage model.
- Existing docs describe the intended native model:
  - `docs/superpowers/specs/2026-05-11-ffm-native-storage-core-design.md`
  - `docs/superpowers/specs/2026-05-11-yierdis-object-removal-design.md`
  - `docs/project-docs/ffm-usage.md`

The current allocator is not yet the production allocator:

- `EntryHandle` and `ValueHandle` are raw `long` wrappers without generation, kind, or ownership checks.
- `EntryTable` maps handles through a heap `HashMap<Long, Slot>`.
- `OffHeapAllocator.allocate()` returns `OffHeapBuf`, not stable object handles.
- `YierdisFfmSlabAllocator` uses a simple free-block list and does not expose size classes, page ownership, object metadata, or defrag.
- Some native bytes paths still allocate one FFM region per blob.

This spec replaces those conventions with a stable object-handle allocator. The existing code can migrate toward it incrementally.

## Terminology

```text
Runtime
  Owns all native memory for one DB, shard, or instance scope.

Segment
  A large FFM MemorySegment allocated from an Arena. Segment lifetime is owned by the runtime.

Page
  A fixed-size allocator unit inside a segment. Pages are assigned to size classes, medium spans, large spans, metadata, or quarantine.

Span
  One or more contiguous pages with the same ownership purpose.

Block
  A physical allocation unit inside a page/span.

Object
  A logical allocation with one stable handle and one metadata entry.

Object table
  Native metadata table mapping handle slot -> current physical location, size, generation, kind, flags, pin state, and ownership state.

Handle
  A stable 64-bit value stored by DB structures. It identifies an object-table slot and generation, not a physical address.

Pin
  A temporary constraint that prevents object movement and final reuse while a snapshot, cursor, or encoder is reading it.
```

## Architecture

The production allocator is split into five layers:

```text
YierdisNativeMemoryRuntime
  -> SegmentManager
  -> PageAllocator
  -> SizeClassAllocator / MediumSpanAllocator / LargeObjectAllocator
  -> ObjectTable
  -> HandleResolver
```

The storage engine uses only the public object API:

```text
handle = allocator.allocate(kind, size, alignment)
allocator.read(handle, offset, dst)
allocator.write(handle, offset, src)
handle = allocator.realloc(handle, newSize, policy)
allocator.free(handle)
```

For high-performance internal structures, typed roots may use a scoped resolved view:

```text
try (ResolvedObject obj = allocator.resolve(handle, READ_WRITE)) {
    // obj exposes bounds-checked primitive access to the current physical bytes.
}
```

Resolved views must not escape a shard operation or a snapshot batch.

## Handle ABI

### Handle Format

Production handles are 64-bit stable indirect handles:

```text
bits 63..60  domain      4 bits
bits 59..56  kind        4 bits
bits 55..16  slotId     40 bits
bits 15..4   generation 12 bits
bits 3..0    flags       4 bits
```

Field meanings:

- `domain`: handle namespace. Initial values:
  - `0`: null/reserved
  - `1`: storage object
  - `2`: entry object
  - `3`: key bytes
  - `4`: type root
  - `5`: index node
  - `6`: allocator metadata
- `kind`: object kind inside the domain. Example: string blob, list node, hash table, zskip node, stream segment, entry record.
- `slotId`: index into the object table.
- `generation`: stale-handle guard. It changes when a slot is freed and later reused.
- `flags`: small public flags. Initial values:
  - `0x0`: normal handle
  - `0x1`: immediate small integer payload handle
  - `0x2`: reserved for embedded empty object
  - other values reserved

`0L` is the only null handle. Any handle with `domain == 0` and non-zero bits is invalid.

### Scale

The 40-bit `slotId` supports up to 1,099,511,627,776 object slots per allocator namespace. Implementations may configure a lower hard cap.

Recommended initial production caps:

```text
max allocator addressable native bytes: 256 TiB
max segment size:                    4 GiB
default segment size:                1 GiB
default page size:                   64 KiB
max segments at 256 TiB:             262,144
max object table slots initially:    configurable, default 67,108,864
```

The handle does not need to encode segment count or offset. Those limits are object-table metadata limits, not public ABI limits.

### Generation

The 12-bit generation detects stale handles:

- Every object-table slot has a native `generation` field.
- A newly allocated slot uses its current generation in the returned handle.
- `free(handle)` marks the slot dead and increments generation before the slot can be reused.
- `resolve(handle)` checks `slot.generation == handle.generation`.
- A mismatch fails fast with a stale handle error in checks-enabled builds and increments a fatal allocator metric in production builds.

Generation wrap is possible after 4096 reuses of the same slot. To make wrap practically safe:

- Freed slots enter quarantine before reuse.
- A slot cannot be reused while pinned or while any active epoch may still hold the old handle.
- Debug/stress builds can use extended generation in side metadata and assert no wrap under active epochs.

## Object Table

Object-table entries are fixed-size native records:

```text
ObjectMeta {
  uint64 address              // packed physical address, allocator-private
  uint32 size                 // requested logical size
  uint32 capacity             // physical capacity
  uint16 segmentId            // optional fast-path cache
  uint16 pageClass            // size class or span class
  uint16 generation
  uint8  domain
  uint8  kind
  uint16 flags
  uint16 pinCount
  uint32 ownerShardId
  uint64 allocEpoch
  uint64 freeEpoch
}
```

The exact binary layout may be tuned, but it must preserve these semantics.

Object states:

```text
FREE
ALLOCATED
PINNED
MOVING
FREED_QUARANTINED
CORRUPT
```

State transitions:

```text
FREE -> ALLOCATED
ALLOCATED -> PINNED
PINNED -> ALLOCATED
ALLOCATED -> MOVING -> ALLOCATED
ALLOCATED -> FREED_QUARANTINED -> FREE
PINNED -> FREED_QUARANTINED
```

`CORRUPT` is terminal and should trigger allocator shutdown for that shard.

The object table is sharded per storage shard. Handles are meaningful only inside their owning allocator namespace unless an explicit cross-shard export API is added later.

## Physical Addressing

Physical addresses are allocator-private. A packed address may use:

```text
bits 63..40  segmentId  24 bits
bits 39..0   offset     40 bits
```

This supports:

- up to 16,777,216 segments
- up to 1 TiB offset per segment

Initial implementation should cap segment size to 4 GiB and use less of the offset range. The extra address space keeps the internal format stable.

No storage structure may store this physical address. Only allocator metadata, defrag code, and resolved views may use it.

## Page And Size-Class Semantics

Default page size is 64 KiB.

Small objects use size classes:

```text
16 B
24 B
32 B
48 B
64 B
96 B
128 B
192 B
256 B
384 B
512 B
768 B
1 KiB
1.5 KiB
2 KiB
3 KiB
4 KiB
6 KiB
8 KiB
12 KiB
16 KiB
24 KiB
32 KiB
```

Small-object rules:

- A small object never crosses a page boundary.
- A small page belongs to exactly one size class.
- Free slots inside small pages are tracked with a bitmap or freelist.
- Page metadata records live count, free count, and fragmentation score.

Medium objects:

- `> 32 KiB` and `<= 1 MiB`.
- Allocated from contiguous page spans.
- May move during defrag if not pinned.

Large objects:

- `> 1 MiB`.
- Allocated from dedicated contiguous spans.
- Large strings should prefer chunked layout before requesting huge contiguous objects.
- Large spans may be moved only by explicit large-object compaction.

Alignment:

- Minimum alignment is 8 bytes.
- Object kinds may request 16-byte or 64-byte alignment for vectorized or cache-line-sensitive layouts.

## Allocation

Public API shape:

```text
NativeHandle allocate(ObjectKind kind, int size, AllocationOptions options)
void free(NativeHandle handle)
NativeHandle realloc(NativeHandle handle, int newSize, ReallocOptions options)
ResolvedObject resolve(NativeHandle handle, AccessMode mode)
```

Allocation steps:

1. Validate size, kind, shard ownership, and maxmemory reservation.
2. Choose small, medium, or large path.
3. Reserve physical block.
4. Allocate or reuse object-table slot.
5. Write `ObjectMeta`.
6. Return stable handle containing slot id and generation.

Failure rules:

- Allocation failure must not publish a handle.
- OOM must preserve allocator metadata consistency.
- If allocation is part of a storage mutation, mutation rollback must release all unpublished or newly published handles.

## Free

`free(handle)` rules:

- Null handle is a no-op only if explicitly allowed by the API variant.
- Invalid domain, kind mismatch, unknown slot, generation mismatch, or already-freed object is an allocator error.
- Freeing a pinned object marks it `FREED_QUARANTINED`; physical memory is reclaimed after pin count reaches zero and all active epochs are past `freeEpoch`.
- Freeing an unpinned object releases its physical block immediately or places it in a per-size-class quarantine depending on debug level.
- The object-table slot generation is incremented before the slot becomes reusable.

Double free must be detected.

## Realloc

`realloc` keeps the public handle stable whenever possible.

Default semantics:

```text
sameHandle = allocator.realloc(handle, newSize, PRESERVE_PREFIX)
```

Rules:

- If `newSize <= capacity`, update logical size in metadata and return the same handle.
- If the owning page/span can grow in place, extend capacity and return the same handle.
- Otherwise allocate a new physical block, copy `min(oldSize, newSize)` bytes, update the same object-table entry to point to the new address, then free the old physical block.
- The handle value does not change unless the caller explicitly requests `REALLOC_MAY_CHANGE_HANDLE`.
- On failure, the old object remains valid and unchanged.
- Realloc of a pinned object may:
  - fail with `PINNED_OBJECT`, or
  - allocate-copy-update through copy-on-write only if the current access protocol permits it.

Recommended default: realloc of pinned objects fails. Higher layers should retry after the pin epoch or allocate a replacement object and update their parent entry through normal mutation logic.

For direct data-structure nodes with external invariants, roots may request:

```text
REALLOC_NO_MOVE
REALLOC_MAY_MOVE_SAME_HANDLE
REALLOC_MAY_CHANGE_HANDLE
```

`REALLOC_MAY_CHANGE_HANDLE` is reserved for migration code and must not be the default for production type roots.

## Active Defrag

Active defrag is built on stable indirect handles.

Defrag moves bytes, not handles:

```text
old physical address -> new physical address
ObjectMeta.address = new physical address
handle unchanged
old block released after safety checks
```

This avoids full graph reference rewriting for ordinary object movement.

Eligible objects:

- allocated
- not pinned
- not already moving
- not larger than the configured movement budget
- object kind has no external raw address exposure

Movement protocol:

1. Mark object metadata `MOVING`.
2. Allocate target block.
3. Copy object bytes.
4. Run object-kind validation hook if present.
5. Atomically publish new physical address in object metadata on the shard thread.
6. Mark object `ALLOCATED`.
7. Quarantine old block until active resolved views are known to be gone.

Because shard storage is single-threaded, publication does not require lock-free cross-thread reads for normal commands. Background components must use snapshot batches or pins, not raw addresses.

Defrag budget:

- Defrag runs as shard maintenance work.
- Each cycle has byte and time budgets.
- Defrag must stop before impacting p99 latency beyond configured threshold.
- Defrag reports moved bytes, skipped pinned objects, reclaimed pages, and failed moves.

## Reference Updating

With stable indirect handles, most defrag movement does not require reference updates. Higher-level references store handles, not addresses.

Reference updates are still needed in these cases:

- A root intentionally changes a handle, such as replacing a listpack with a hashtable.
- A structure stores packed inline references inside an object and the referenced object is semantically replaced.
- A migration from old direct handles to stable handles rewrites existing structures.
- Debug tooling wants to validate the handle graph.

Every type root must therefore expose a traversal contract:

```text
interface HandleGraphNode {
  void visitChildHandles(HandleVisitor visitor);
  void rewriteChildHandle(NativeHandle oldHandle, NativeHandle newHandle);
}
```

Production defrag should not rely on full graph rewriting. The graph contract is for:

- handle-changing migrations
- integrity checks
- leak detection
- optional compaction modes that collapse or split logical objects

## Pins, Epochs, And Snapshot Safety

Background tasks must not hold raw physical addresses across shard turns.

Supported safe-read mechanisms:

### Encoded Payload Cursor

The preferred model for RDB, AOF rewrite, and replication snapshot:

```text
shard thread resolves objects
shard thread encodes a bounded batch
background writer consumes encoded bytes
```

The background writer never sees allocator addresses.

### Pin-Based Cursor

When a cursor must stream directly from native memory:

1. It pins the object handle.
2. It resolves a bounded view.
3. It copies or writes the bytes before yielding.
4. It unpins before the next shard turn unless explicitly approved.

Pins prevent movement and final reuse, not logical mutation. If logical immutability is needed, use snapshot epoch or copy-on-write.

### Epoch-Based Reclamation

Every shard tracks active read epochs:

- command epoch
- scan cursor epoch
- snapshot epoch
- defrag epoch

Old physical blocks and freed object-table slots remain quarantined until no active epoch can reference them.

## Stale Handle Protection

The allocator must detect:

- use after free
- double free
- wrong domain
- wrong kind
- handle from another allocator namespace
- generation mismatch
- resolving a quarantined or corrupt slot

Debug mode behavior:

- throw a detailed allocator exception immediately
- include handle fields, slot state, generation, owner shard, allocation stack id if available

Production mode behavior:

- fail fast for metadata corruption
- return a controlled internal error only for recoverable caller misuse detected before mutation publication
- increment fatal allocator metrics
- optionally mark the shard unhealthy

Silent reuse of a stale physical address is not acceptable.

## Ownership And Threading

Each storage shard owns its allocator namespace.

Rules:

- Only the shard event loop mutates allocator metadata.
- Network threads, AOF writers, RDB writers, replication senders, metrics scrapers, and admin threads do not resolve handles directly.
- Cross-thread interactions request encoded batches, metrics snapshots, or pinned-copy operations through the shard.
- `Arena.ofConfined()` is preferred for shard-owned segments.
- `Arena.ofShared()` is allowed only for explicitly shared metadata or experimental tools, and must be justified in code.

## Integration With Storage Structures

Entry model:

- `EntryHandle` becomes a typed wrapper around the production native handle.
- `ValueHandle` becomes a typed wrapper around the production native handle.
- `EntryTable` no longer maps handles through heap `HashMap<Long, Slot>` in production mode.
- Entry records are allocated as native objects or from an entry-specific object table using the same generation and stale-handle rules.

Key directory:

- key bytes are native objects.
- key directory stores key handle and entry handle.
- hash table buckets store handles, not raw offsets.

Type roots:

- string, list, hash, set, zset, hll, stream, and index nodes store stable handles for child objects.
- type roots may cache resolved addresses only within one operation.
- replacement must update the parent entry or parent node before releasing the old handle.

Expire and eviction indexes:

- indexes reference entry handles.
- expired or evicted deletion goes through unified `unlinkEntry(entryHandle)`.
- index references are removed before entry handle reuse becomes visible.

## Metrics

Allocator metrics must include:

- logical used bytes
- reserved native bytes
- committed segment bytes
- free bytes by size class
- internal fragmentation
- external fragmentation
- object count by kind
- live page count by class
- free page count
- quarantine bytes
- pinned object count
- stale handle detection count
- double free detection count
- defrag moved bytes
- defrag reclaimed pages
- defrag skipped pinned objects
- realloc in-place count
- realloc moved count
- allocation latency histogram

`MEMORY STATS` should expose a stable subset. Detailed allocator metrics can live under a Yierdis-specific section.

## Migration Plan

### Phase 1: Define Public Contracts

- Add `NativeHandle` utilities for domain, kind, slot, generation, and flags.
- Add typed wrappers for entry, key, value, and index handles.
- Add allocator exceptions and checks-enabled mode.
- Document null handle and invalid handle behavior.

### Phase 2: Add Object Table

- Implement native object metadata table.
- Allocate stable handles from object-table slots.
- Add generation and quarantine.
- Keep physical allocator non-moving initially.

### Phase 3: Replace Entry And Value Handle Meaning

- Move `EntryTable` away from heap `HashMap<Long, Slot>`.
- Store entry records as native objects or entry-table slots governed by object metadata.
- Make `ValueHandle` refer to allocator objects or root-owned objects with generation checks.

### Phase 4: Size Classes And Large Objects

- Replace simple first-fit free-block allocation with page and size-class allocation.
- Add medium and large span paths.
- Add fragmentation metrics.

### Phase 5: Realloc

- Implement stable-handle realloc.
- Update string/listpack-heavy roots to use it.
- Add rollback tests for failed realloc.

### Phase 6: Pins And Epoch Reclamation

- Add resolved view lifecycle.
- Add pin/unpin and epoch quarantine.
- Convert snapshot and scan paths to encoded batch or bounded pin semantics.

### Phase 7: Active Defrag

- Add defrag planner and shard maintenance budget.
- Move unpinned objects by updating object metadata.
- Add validation hooks and metrics.
- Enable gradually behind config.

## Testing Strategy

### Unit Tests

- handle encode/decode
- null and invalid handle rejection
- generation mismatch rejection
- allocation/free/reuse generation changes
- double free detection
- size-class selection
- page ownership invariants
- realloc same-capacity, grow-in-place, move-same-handle, and failure rollback
- pin prevents movement and final reuse

### Property And Fuzz Tests

Randomly generate:

- allocate/free/realloc/resolve operations
- mixed object sizes
- forced OOM
- stale handle attempts
- pinned object movement attempts
- generation wrap pressure with quarantine enabled

Invariants:

- no overlapping live physical blocks
- object table state matches free lists
- live logical bytes match sum of object sizes
- every valid handle resolves to a block with enough capacity
- every freed handle eventually becomes invalid

### Integration Tests

- string overwrite and append under repeated realloc
- hash/listpack conversion under allocator pressure
- zset skiplist node allocation and release
- TTL deletion releases key, entry, value, expire index, and eviction references
- eviction unlink path does not leave reachable freed handles
- scan and snapshot while defrag is enabled
- AOF/RDB rewrite cursor uses encoded payload or bounded pin protocol

### Long-Running Stress

- many small keys
- large values and chunked values
- mixed TTL and eviction churn
- repeated active defrag cycles
- crash-style forced close with leak detection
- allocator metrics consistency under load

## Success Criteria

The design is implemented when:

- all off-heap production references use stable handles, not physical addresses
- stale handle, double free, and wrong-kind access are detected
- `realloc` has stable-handle semantics and failure rollback
- active defrag can move unpinned objects without rewriting normal graph references
- snapshot and scan paths are safe under object movement
- allocator metrics can explain reserved bytes, logical bytes, and fragmentation
- stress tests show no leaks, overlapping blocks, or stale physical reads

## Design Rationale

Direct physical handles are faster, but they push object movement complexity into every data structure. A production Redis-like system needs active defrag, safe snapshotting, and long-running correctness under allocation churn. Stable indirect handles make those requirements local to allocator metadata.

The cost is one metadata lookup per resolved object. That cost is acceptable because type roots can resolve once per command operation, use bounded local views, and keep hot loops inside the resolved object. The alternative is a system where defrag requires full graph rewriting and every missed reference can corrupt data.

## Summary

Production Yierdis should treat handles as a stable allocator ABI:

```text
64-bit stable handle
  -> object table slot + generation
  -> current physical segment/page/span/block
```

Storage structures store handles. The allocator owns physical addresses. Defrag moves physical bytes and updates metadata, not every reference in the database. Stale handles fail fast through generation checks and quarantine. This is the clean boundary needed before Yierdis can honestly claim a production-grade off-heap storage core.
