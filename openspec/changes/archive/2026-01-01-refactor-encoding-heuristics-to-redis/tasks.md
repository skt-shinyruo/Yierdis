## 1. Proposal Acceptance
- [x] Confirm the Redis baseline (e.g., Redis 7.x defaults) for the threshold rules to mirror. (Approved: Redis 7.x common defaults)
- [x] Confirm whether set listpack encoding should be removed to match Redis (`intset` ↔ hashtable only). (Approved: remove set listpack)
- [x] Confirm whether thresholds should be exposed via `ServerConfig` flags or remain internal constants. (Approved: keep internal constants)

## 2. Encoding Threshold Plumbing
- [x] Introduce a single “encoding thresholds” holder in `db/` with Redis-like names and defaults.
- [x] Update `HashValue`, `ListValue`, `SetValue`, and `ZSetValue` to read thresholds from that holder (instead of hard-coded per-class constants) where applicable.

## 3. List Heuristics Closer to Redis Quicklist
- [x] Update list packed→quicklist upgrade heuristics to be primarily byte/size-based (closer to Redis `list-max-listpack-size` semantics).
- [x] Ensure quicklist node splitting/merging respects the configured node size bound.
- [x] Update/extend `ListValueTest` for the new rules (keep external list semantics identical).

## 4. Set Encoding Closer to Redis
- [x] Remove the intermediate set listpack encoding (keep `intset` for canonical integers and upgrade to hashtable on non-integer members or size overflow).
- [x] Update `ValueEncoding` accordingly and adjust any tests/assumptions.
- [x] Verify `SADD/SREM/SISMEMBER/SMEMBERS/SCARD` behavior remains correct and binary-safe.

## 5. Packed Iteration Improvements
- [x] Refactor packed ZSET range reply paths to avoid repeated index→offset rescans (cursor/offset-walk).
- [x] Ensure ordering rules remain: score asc, then member lex order.

## 6. Verification
- [x] Run `mvn test` and record the result in notes. (Done: BUILD SUCCESS, 125 tests on 2025-12-30)
