# Design: Canonical key sharing between store and expires

## Goal
Align Yierdis’ key/expire relationship with Redis’ internal model:
- the main keyspace (`store`) owns the canonical key bytes object
- secondary indexes (like `expires`) reference that canonical key, not a duplicate allocation

## Canonical key lookup
Add a method to `ByteArrayKeyspace`:

`byte[] canonicalKey(byte[] lookupKey)`
- returns the stored key reference if present (pointer identity)
- returns null if absent
- performs at most one hash-table probe (per table, during rehash)
- triggers the usual incremental `rehashStep()` to stay consistent with other operations

## Wiring in YierdisDb
When writing to the TTL index:
1) Resolve canonical key from `store` for the logical key
2) Write TTL into `expires` using that canonical key reference

### Repairing pre-existing non-canonical expires keys
If an expires entry exists but is keyed by a different `byte[]` instance, the fix should converge in-process by:
- reading the old `Long` value via `expires.get(lookupKey)`
- removing that entry via `expires.remove(lookupKey, expectedValue)`
- inserting under the canonical key reference

This keeps the runtime invariant close to Redis even if the process already had TTLs set before this change.

## External behavior
No command semantics change. The change is purely internal memory layout / identity sharing.

