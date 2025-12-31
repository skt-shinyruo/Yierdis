## 1. Proposal Acceptance
- [x] Confirm scope: request decode zero-copy + lifecycle only (no DB slice lookup yet).
- [x] Confirm representation choice: single retained command frame + offset/len arrays (preferred).

## 2. Protocol Data Model
- [x] Refactor `RespCommand` to hold a retained `ByteBuf` frame and per-arg offset/len metadata.
- [x] Ensure `RespCommand.recycle()` releases retained buffers exactly once and resets state for reuse.
- [x] Provide `toByteArray()` / `copyToByteArray()` APIs for boundary conversions.

## 3. Decoder (Zero-Copy)
- [x] Update `RespCommandDecoder` to parse without allocating per-arg `byte[]`.
- [x] Retain exactly once per fully decoded command frame; do not retain on partial frames.
- [x] Ensure all exceptional paths release any retained buffers (no leaks).

## 4. Fast Pipeline Ownership
- [x] Update `YierdisFastCommandHandler` to handle the new `RespCommand` lifecycle safely (always recycle in `finally`).
- [x] Confirm QUIT handling still closes after sending `+OK`.

## 5. Fast Command Processor Wiring
- [x] Update `YierdisFastCommandProcessor` parsing to use slice-based access for:
  - command name routing
  - integer parsing for args like `LRANGE start/stop`, `EXPIRE`, etc.
- [x] Materialize `byte[]` only at DB boundaries (keys/values stored in DB).

## 6. Tests
- [x] Add protocol tests that validate:
  - binary-safe bulk args decode correctly
  - null bulk vs empty bulk semantics
  - partial frames do not produce commands and do not leak retained buffers
- [x] Add fast pipeline tests exercising common commands under the new decoder.

## 7. Verification
- [x] Run `openspec validate refactor-resp-decode-zero-copy --strict`.
- [x] Run `mvn test -pl :yierdis -am`.
