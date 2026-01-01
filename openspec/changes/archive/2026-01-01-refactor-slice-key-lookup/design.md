# Design: Slice-based key lookup (request-scoped) for read path

## Problem statement
The request decoder now exposes command arguments as slice/len views over a retained Netty `ByteBuf` frame.

Even so, the DB layer (`YierdisDb`) is keyed by `byte[]`, so the fast command processor currently calls
`RespCommand.toByteArray()` for keys to perform lookups. For read-heavy workloads this keeps a “copy key per command”
hot path, which is the opposite of Redis’ internal `(ptr,len)` lookup style.

We want a minimal, safe way to:
- probe the DB keyspace using slice/len keys,
- resolve the canonical stored `byte[]` key reference if present,
- avoid materializing key bytes when the key does not need to be persisted (read path),
- keep the DB boundary free of Netty refCnt complexity (no buffer retention in DB).

## Constraints
- Stored keys remain canonical `byte[]` instances owned by the main keyspace (`store`).
- The expires index (`expires`) should continue to share canonical keys with `store` (see `refactor-expire-key-sharing`).
- The fast pipeline handler remains the ownership boundary for request buffer lifetime; the DB must not retain slices.
- Keep implementation minimal and aligned with existing patterns (no new “framework” abstractions).

## Proposed approach

### 1) Introduce a small key-view abstraction (request-scoped)
Add a tiny “bytes view” type for lookup keys:
- `int len()`
- `byte byteAt(int index)`

This is intentionally minimal: it supports hashing and equality checks without allocating `byte[]`.

For the fast path we can reuse a single mutable adapter object that points at:
- a `RespCommand` plus `(argIndex)` OR
- a `ByteBuf` plus `(offset,len)` if we prefer to keep it lower-level

The adapter MUST be request-scoped and must not escape the call stack.

Why an interface instead of `ByteBuf` directly?
- Keeps `db/` independent of Netty types.
- Keeps the boundary explicit: “this is a read-only view; do not store”.
- Allows future reuse (e.g. off-heap key views) without rewriting keyspace logic.

### 2) Extend `ByteArrayKeyspace` with slice-key probing
Today, `ByteArrayKeyspace` has:
- `V get(byte[] key)`
- `byte[] canonicalKey(byte[] key)`

We add slice-based variants:
- `V get(BytesView key)`
- `byte[] canonicalKey(BytesView key)`

Implementation details:
- Hashing MUST match the existing `byte[]` path:
  - compute the equivalent of `Arrays.hashCode(byte[])` on the slice (`result = 31*result + b`)
  - apply the same mixing and `seed` xor as the current `hash(byte[])`
- Equality checks compare a stored `byte[]` key against the slice byte-by-byte without copying.

This keeps the keyspace’s storage format unchanged while enabling “ptr+len” lookups.

### 3) Wire into `YierdisDb` using canonical keys
For read-path operations we add new overloads that accept a `BytesView` key and internally:
1) resolve the canonical stored key via `store.canonicalKey(view)`
2) use the canonical `byte[]` for:
   - reading the stored value (`store.get(canonical)`)
   - expiration checks (`expires.get(canonical)`), and
   - any mutation that only touches secondary indexes (e.g. `EXPIRE`)

If the key is absent, we return the same “not found” behavior as today without ever allocating a `byte[]`.

### 4) Update the fast command processor to call the new overloads
Update `YierdisFastCommandProcessor` so that read commands:
- do routing and option parsing using slice-based argument access (already done),
- perform DB key lookups using slice keys (new),
- only materialize `byte[]` for keys/values that must be stored (write path remains unchanged).

## Alternatives considered

### A) Materialize keys lazily in the processor (status quo)
Still allocates `byte[]` for every read command. This defeats the purpose of the request decode zero-copy work.

### B) Pass Netty `ByteBuf` to DB and do lookup with `(buf,off,len)`
This can be very efficient, but couples `db/` to Netty and blurs lifecycle boundaries.
If we do this, we must be extremely explicit that DB must not `retain()` or store references.

We prefer the small `BytesView` abstraction as the “least coupling” solution.

## Testing strategy
- Fast pipeline tests:
  - binary-safe key lookups through `GET`/`TYPE`/`EXISTS`
  - multi-key `EXISTS` correctness with mixed present/absent keys
- Unit tests on `ByteArrayKeyspace` slice lookup:
  - slice key matches stored canonical `byte[]` key
  - mismatch cases do not produce false positives

Allocation-free behavior is primarily validated by code inspection and by keeping the slice-key API “no `byte[]` return”
except when explicitly requested by call sites.

