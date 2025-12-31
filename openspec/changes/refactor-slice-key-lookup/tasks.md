## 1. Proposal Acceptance
- [x] Confirm scope: read-path key lookup on slice/len keys (no stored-key format change).
- [x] Confirm which commands are in scope for this change (GET/TYPE/EXISTS, including multi-key EXISTS).

## 2. Key View API
- [x] Define a minimal request-scoped key view abstraction usable for hashing + equality (no `byte[]` allocation).
- [x] Ensure the key view does not escape (DB MUST NOT retain/store it).

## 3. Keyspace Slice Lookup
- [x] Extend `ByteArrayKeyspace` with slice-key lookup methods (`get` / `canonicalKey`) that:
  - hash slices using the same algorithm as `byte[]` keys
  - compare stored `byte[]` keys to slice keys without materialization
- [x] Add unit tests for slice lookup correctness (including binary keys).

## 4. DB Wiring
- [x] Add `YierdisDb` overloads for read-path operations that accept slice keys and resolve canonical stored keys.
- [x] Ensure expiration checks continue to use canonical keys (no semantic change).

## 5. Fast Command Processor Wiring
- [x] Update `YierdisFastCommandProcessor` so that in-scope commands use slice-key DB APIs and do not call `toByteArray()`
      for key lookup.
- [x] Keep write-path commands materializing keys/values where persistence requires it.

## 6. Tests
- [x] Add/extend fast pipeline tests for GET/TYPE/EXISTS (including multi-key EXISTS) with binary-safe keys.
- [x] Ensure behavior remains unchanged for missing keys and type errors.

## 7. Verification
- [x] Run `openspec validate refactor-slice-key-lookup --strict`.
- [x] Run `mvn test -pl :yierdis -am`.
