# DB Native Allocator List Quicklist Node Design

## Status

Track 2.1 child spec for nativeizing list/quicklist internal node records.

This document is scoped to list internals only. It builds on the DB native allocator roadmap but does not change the parent roadmap.

## Current State

`ListRoot` uses `NativeCollectionRootTable<ListValue>` with `NativeObjectKind.LIST_NODE`. That object is the list collection root record, not an internal quicklist node. `NativeCollectionRootTable` allocates `ROOT_RECORD_BYTES = Long.BYTES`, validates root handles through `NativeAllocator`, and removes/closes the Java adapter before freeing the root handle.

`ListValue` has two FFM list modes:

- packed mode: `YierdisFfmListpack listpackFfm`
- quicklist mode: `ArrayDeque<FfmListNode> quicklistFfm`

`FfmListNode` is currently a Java wrapper around `YierdisFfmListpack`; it has no `NativeHandle`. Its payload bytes are owned by `YierdisFfmListpack` and `YierdisFfmBlobStore`.

`NativeObjectKind` currently has `LIST_NODE`, `HASH_NODE`, `SET_NODE`, and `ZSET_NODE` root kinds. `NativeObjectKindCounts` has `listNodeObjects`, but no separate quicklist internal node count. `YierdisDbNativeHandleGraph` currently visits key bytes, entry records, string values, and collection roots; it does not traverse list internals.

## Goals

- Add a distinct native object kind for list quicklist internal node records: `LIST_QUICKLIST_NODE`.
- Keep existing `LIST_NODE` root behavior unchanged.
- Give each FFM quicklist node an allocator-backed liveness/linkage/metadata/stats record.
- Keep current listpack/blob payload storage unchanged in this split.
- Make quicklist internal nodes visible to allocator stats and future handle graph traversal.
- Preserve existing list command behavior, encoding transitions, node split/merge behavior, and cleanup semantics.

## Non-Goals

- Do not change hash, set, or zset internals.
- Do not migrate key bytes.
- Do not define DB defrag maintenance scheduling or policy.
- Do not define scan/snapshot epoch policy.
- Do not add or define benchmarks.
- Do not migrate listpack/blob payload bytes into the stable allocator in this split.
- Do not change the existing `LIST_NODE` root record kind or root adapter table behavior.
- Do not expose allocator views, memory segments, physical addresses, or blob offsets to command code.

## Out Of Scope

The following topics are explicitly out of scope for Track 2.1:

- hash/set/zset nativeization
- key byte nativeization
- DB defrag maintenance policy
- scan/snapshot epoch design
- benchmark plans or benchmark acceptance gates

## Object Kind

Add `NativeObjectKind.LIST_QUICKLIST_NODE` for internal quicklist node records.

`NativeObjectKind.LIST_NODE` remains the collection root record kind used by `ListRoot` and `NativeCollectionRootTable<ListValue>`. `LIST_QUICKLIST_NODE` must not replace, alias, or share accounting with the root kind.

`NativeObjectKindCounts` should gain a separate quicklist internal node count, for example `listQuicklistNodeObjects`. Existing `listNodeObjects` continues to mean root records allocated as `LIST_NODE`.

## Native Layout

The node record owns liveness, linkage, metadata, and accounting fields only. Payload bytes stay in `YierdisFfmListpack` / `YierdisFfmBlobStore`.

Recommended fixed-size little-endian layout:

```text
offset  size  field
0       8     ownerRootRawHandle
8       8     prevNodeRawHandle, or 0
16      8     nextNodeRawHandle, or 0
24      8     payloadRefRaw, transitional diagnostic value or 0
32      4     entryCount
36      4     encodedBytes
40      4     flags
44      4     reserved
```

`payloadRefRaw` is not a stable allocator handle in this split. It may be zero if `YierdisFfmListpack` does not expose a durable payload identifier that can be stored safely. It must not be used as liveness authority.

`ownerRootRawHandle` stores the raw `LIST_NODE` root handle for validation and graph traversal. `prevNodeRawHandle` and `nextNodeRawHandle` store raw `LIST_QUICKLIST_NODE` handles. A zero link means absent.

The Java `FfmListNode` adapter may cache:

- the `NativeHandle` for its `LIST_QUICKLIST_NODE` record
- the existing `YierdisFfmListpack` payload object

The adapter must not be the liveness authority and must not cache `NativeObjectView` or resolved spans across operations.

## Ownership

`ListRoot` continues to own the `LIST_NODE` root record through `NativeCollectionRootTable`.

`ListValue` owns the internal quicklist node records for the lifetime of the list value adapter. In FFM quicklist mode, every `FfmListNode` must correspond to one live `LIST_QUICKLIST_NODE` record. Packed FFM mode continues to use the single `listpackFfm` payload without internal quicklist node records.

Payload ownership remains unchanged:

- `YierdisFfmListpack` owns listpack payload behavior.
- `YierdisFfmBlobStore` owns blob payload bytes and release mechanics.
- The new native node record owns only node identity, links, metadata, and allocator stats.

The allocator object table is the liveness authority for `LIST_QUICKLIST_NODE`. Java collections such as `ArrayDeque<FfmListNode>` are adapter indexes only.

## Free Order

Free order must prevent Java adapters or payload wrappers from resurrecting freed native records:

1. Unlink the node from the Java quicklist adapter structure and, when links are present, update neighboring native node records.
2. Close/free the node payload object through `YierdisFfmListpack.close()`.
3. Clear adapter fields that reference the payload and native handle.
4. Free the `LIST_QUICKLIST_NODE` record through `NativeAllocator.free(...)`.

When releasing the whole list root, `NativeCollectionRootTable.release(handle)` already removes and closes the `ListValue` adapter before freeing the `LIST_NODE` root handle. `ListValue.close()` must therefore close/free all internal quicklist nodes before the root record is freed by the table.

If node allocation succeeds but payload creation fails, free the `LIST_QUICKLIST_NODE` record before rethrowing. If payload close fails during list release, still attempt to free the node record and attach suppressed failures consistently with the existing root cleanup style.

## Defrag Move Rules

Track 2.1 only defines object-level move safety for `LIST_QUICKLIST_NODE`; it does not define DB maintenance scheduling.

Rules:

- A moved `LIST_QUICKLIST_NODE` keeps the same stable `NativeHandle`.
- Java adapters may cache the `NativeHandle`, but must resolve the node record only for the current operation.
- Java adapters must not cache `NativeObjectView`, physical addresses, resolved spans, or payload memory addresses.
- `ownerRootRawHandle` must still resolve as `LIST_NODE` when validating a reachable node.
- `prevNodeRawHandle` and `nextNodeRawHandle` must either be zero or resolve as `LIST_QUICKLIST_NODE`.
- Defrag must not move or rewrite `YierdisFfmBlobStore` payload bytes in this split.
- Payload references remain owned by the current FFM payload path; any payload movement or compaction belongs to a later split.

## Stats Ownership

Allocator stats own:

- bytes and object counts for `LIST_QUICKLIST_NODE` records
- stale-handle detections for freed or wrong-generation quicklist node handles

Existing FFM payload stats continue to own:

- listpack/blob payload bytes
- `ListValue.estimatedBytes()` payload estimates
- `YierdisFfmBlobStore.liveBytes()` for list payload storage

Stats must avoid double counting. `listNodeObjects` counts only `LIST_NODE` root records. The new quicklist node count counts only internal quicklist node records. Payload bytes remain outside `LIST_QUICKLIST_NODE` allocator bytes until a later payload-native split.

## Stale Handle Behavior

Resolving a freed, wrong-generation, wrong-domain, or wrong-kind quicklist node handle must fail through allocator stale-handle validation.

List operations must treat the root handle as the first authority:

- validate the `LIST_NODE` root through `NativeCollectionRootTable`
- use the adapter table only after root validation succeeds
- validate each internal quicklist node handle before reading or mutating its native metadata

An old `FfmListNode` adapter reference must not make a freed node usable. After node removal or list release, operations that try to resolve the cached node handle must fail rather than observe a reused node.

## Handle Graph

`YierdisDbNativeHandleGraph` does not currently traverse list internals. Track 2.1 should define the traversal contract and the implementation plan must either add the minimal traversal hook for live list quicklist nodes or explicitly keep graph traversal root-only for this split with list-root focused tests covering internal handles.

Required traversal shape:

```text
ENTRY_RECORD
  -> LIST_NODE root
     -> LIST_QUICKLIST_NODE internal nodes
```

The graph visitor should distinguish collection roots from list internal nodes with a separate role, for example `LIST_QUICKLIST_NODE`. It must not traverse FFM blob payload bytes as allocator objects in this split.

## Tests And Acceptance Criteria

Acceptance criteria:

- `ListRoot` continues to allocate root records as `NativeObjectKind.LIST_NODE`.
- Creating or converting to FFM quicklist mode allocates one `LIST_QUICKLIST_NODE` record per `FfmListNode`.
- Adding a new quicklist node increments the new quicklist node object count, not `listNodeObjects`.
- Removing an empty quicklist node closes its payload and frees its `LIST_QUICKLIST_NODE` record exactly once.
- Merging two FFM quicklist nodes frees the discarded node record and preserves list element order.
- Releasing a list root frees all internal quicklist node records before freeing the `LIST_NODE` root record.
- Resolving a removed node handle fails with stale-handle semantics.
- Reusing an allocator slot for a later node does not allow an old node handle to observe the new node.
- Defrag-style movement of a node record preserves stable handles and does not require adapter updates beyond resolving the handle again.
- `YierdisDbNativeHandleGraph` can enumerate list internal node handles reachable from a live list root once traversal support is added.
- Existing list behavior tests for `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, range/streaming, packed-to-quicklist conversion, and FFM cleanup continue to pass.

Suggested focused tests:

- native kind/count test for root versus quicklist internal nodes
- packed-to-quicklist conversion allocation test
- pop-to-empty node free test
- merge free-order test for first and last node merges
- stale internal node handle test
- graph traversal test for list roots with multiple quicklist nodes
- close/release leak test that verifies allocator node counts return to zero

## Risks

- Root and internal nodes both use list-oriented names; keeping `LIST_NODE` as root-only and `LIST_QUICKLIST_NODE` as internal-only must be enforced in tests.
- Free order is easy to invert because `NativeCollectionRootTable` closes the adapter before freeing the root. Internal nodes must be closed from `ListValue.close()` while the root handle is still valid.
- Payload bytes remain outside the stable allocator, so allocator stats will not equal total list native bytes in this split.
- A cached Java adapter can accidentally become liveness authority if operations trust it without resolving the native node record.
- Graph traversal can overclaim completeness if it reports native node records but silently omits blob payload bytes; traversal docs and roles must make that boundary explicit.

## Blockers

No blockers.
