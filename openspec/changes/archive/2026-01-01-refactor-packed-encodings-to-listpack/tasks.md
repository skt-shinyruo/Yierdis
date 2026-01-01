## 1. Proposal Acceptance
- [x] Confirm the goal: implementation alignment with Redis encodings (listpack/quicklist-like), while keeping external semantics unchanged.
- [x] Confirm constraints: keep code minimal; listpack implementation may be approximate (not byte-identical to Redis).
- [x] Confirm performance expectations: optimize for small collections, accept O(n) scans in packed encodings.

## 2. Core Listpack Primitive
- [x] Add a small binary-safe listpack-like container in `yierdis-server/src/main/java/yier/bubu/redis/db/` (single contiguous `byte[]` + minimal metadata).
- [x] Support operations required by current command set: append/prepend, remove first/last, index-based access for range, linear search by value, and delete by index.
- [x] Add targeted unit tests for the container (basic operations + bounds behavior).

## 3. Hash Packed Encoding
- [x] Replace `HashValue` packed arrays (`byte[][]`) with the listpack container (store field/value pairs contiguously).
- [x] Preserve current upgrade behavior to `ByteArrayHashMap` when thresholds are exceeded.
- [x] Add/adjust unit tests for `HSET/HGET/HDEL/HGETALL/HLEN` invariants with the new packed encoding.

## 4. Set Listpack Encoding
- [x] Replace `SetValue` listpack representation (`byte[][]`) with the listpack container.
- [x] Preserve current intset and hashset upgrade paths.
- [x] Add/adjust unit tests for `SADD/SREM/SISMEMBER/SMEMBERS/SCARD` invariants.

## 5. List Packed Encoding + Quicklist Nodes
- [x] Replace `ListValue.PackedList` (`byte[][]`) with listpack storage for the packed form.
- [x] Update quicklist nodes so each node stores a listpack (Redis-like), not a `byte[][]`.
- [x] Preserve upgrade thresholds (`LISTPACK_MAX_*`, quicklist node max entries/bytes) and merging behavior.
- [x] Add/adjust unit tests for `LPUSH/RPUSH/LPOP/RPOP/LRANGE` invariants and boundary cases.

## 6. ZSet Packed Encoding
- [x] Replace `ZSetValue.PackedZSet` (parallel arrays) with a listpack-based packed representation.
- [x] Preserve ordering rules (score ascending, then member lex order) and upgrade behavior to dict+skiplist.
- [x] Add/adjust unit tests for `ZADD/ZREM/ZRANGE/ZREVRANGE/ZRANGEBYSCORE` invariants.

## 7. Verification
- [x] Run `mvn test` and record the result in the PR/notes.
