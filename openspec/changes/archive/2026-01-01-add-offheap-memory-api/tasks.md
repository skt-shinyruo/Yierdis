## 1. API Definition
- [x] Define minimal off-heap API surface (allocate/free, bounds-safe read/write, slice views, copy utilities, accounting).
- [x] Define ownership + lifecycle rules (who frees what, when).
- [x] Decide how to represent a “slice” without heap copies (ptr+len handle object vs primitive fields).

## 2. Netty Backend
- [x] Implement allocator + buffer wrappers using direct `ByteBuf` (pooled where possible).
- [x] Ensure deterministic release on overwrite/DEL/shutdown.
- [x] Add tests for leak-free behavior (reference counts reach zero).

## 3. Foreign Memory Backend
- [x] Choose packaging strategy:
  - [x] Maven profile compiling incubator backend on Java 17, or
  - [x] Reflection-based optional backend for newer JDKs.
- [x] Implement allocator + buffer wrappers using MemorySegment/Arena.
- [x] Add lifecycle tests (segments invalid after close, accounting returns to zero).

## 4. Integration Hooks
- [x] Add a configuration flag to choose backend (e.g., `--offheapBackend=netty|foreign`).
- [x] Wire backend selection into DB construction.
- [x] Keep default behavior unchanged unless flag is set.

## 5. Validation
- [x] Add backend equivalence tests (write/read/copy/slice returns identical bytes).
- [x] Run `mvn test` (default profile) and the Foreign Memory profile tests.
- [x] Document how to run with each backend (required JVM args).
