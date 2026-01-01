## 1. Proposal Acceptance
- [x] Confirm scope: extend slice-key lookup to STRLEN/TTL/EXPIRE only.

## 2. DB Wiring
- [x] Add `YierdisDb` overloads for `strlen`, `ttlSeconds`, `expire` that accept `YierdisBytesView`.
- [x] Ensure overloads resolve canonical keys and preserve existing expiration semantics.

## 3. Fast Command Processor Wiring
- [x] Update `YierdisFastCommandProcessor` implementations of STRLEN/TTL/EXPIRE to use slice-key DB APIs.
- [x] Ensure no key `byte[]` materialization for these lookup paths.

## 4. Tests
- [x] Add fast pipeline tests covering STRLEN/TTL/EXPIRE with binary-safe keys and acceptable TTL variance.

## 5. Verification
- [x] Run `openspec validate refactor-slice-key-lookup-extra-cmds --strict`.
- [x] Run `mvn test -pl :yierdis -am`.
