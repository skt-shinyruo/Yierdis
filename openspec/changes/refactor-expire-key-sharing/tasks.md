## 1. Proposal Acceptance
- [x] Confirm that the goal is Redis-style key sharing: `expires` MUST reference the same key `byte[]` as `store`.
- [x] Confirm that this is internal-only and should not change external semantics.

## 2. Canonical Key Lookup
- [x] Add a package-private method on `ByteArrayKeyspace` to return the stored key reference for a logical key (or null).
- [x] Ensure it works during incremental rehash and does not allocate.

## 3. Expires Index Wiring
- [x] Update `YierdisDb` so all TTL writes use the canonical key reference from `store`.
- [x] If an expires entry exists under a non-canonical key, move it to the canonical key when updating TTL.

## 4. Tests
- [x] Add a unit test proving that `store` and `expires` share the same `byte[]` instance for a TTL’d key.
- [x] Keep tests deterministic and avoid timing sleeps.

## 5. Verification
- [x] Run `openspec validate refactor-expire-key-sharing --strict`.
- [x] Run `mvn test` (at least `-pl :yierdis -am`) and ensure all tests pass.
