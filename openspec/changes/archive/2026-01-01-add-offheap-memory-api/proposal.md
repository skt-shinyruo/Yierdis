# Change: Add off-heap memory abstraction with Netty + Foreign Memory backends

## Why
The project currently represents all key/value bytes with heap `byte[]`, which is simple but makes it hard to:
- Experiment with off-heap storage while keeping command semantics stable.
- Compare different off-heap approaches (Netty direct buffers vs Java Foreign Memory API).
- Gradually migrate hot paths (e.g., reply writes) away from heap copies.

Adding a small off-heap memory abstraction layer allows the DB/storage code to depend on a stable API while swapping the underlying
implementation for experimentation and future migration.

## What Changes
- Introduce a new internal API for off-heap memory operations (allocate/free, read/write, slice views, copy helpers, accounting).
- Provide two implementations:
  - **Netty backend**: based on direct `ByteBuf` (preferably pooled) with deterministic `release()`.
  - **Foreign Memory API backend**: based on MemorySegment/Arena with deterministic close.
- Add configuration hooks to choose the backend at runtime (default remains current heap storage unless explicitly enabled).

**Non-breaking (external)**: supported RESP2 commands and wire protocol behavior remain unchanged.
**Breaking (internal)**: storage code may start depending on the new abstraction instead of raw `byte[]`.

## Key Decision: Java 17 vs Foreign Memory API
This repository targets Java 17. The Foreign Memory API is:
- **Incubator in Java 17** (`jdk.incubator.foreign.*`, requires `--add-modules jdk.incubator.foreign` at compile/test/run).
- **Stable in newer Java** (`java.lang.foreign.*`, Java 22+).

This change will implement the Foreign Memory backend in a way that keeps the default build on Java 17:
- The Foreign Memory backend is built/used behind a Maven profile (or reflection-based optional loading).
- The default runtime path uses the Netty backend when off-heap is enabled.

## Impact
- New package(s) for the off-heap API and implementations.
- Maven build updates if the incubator Foreign Memory backend is compiled as part of this repo.
- New unit tests validating backend equivalence and lifecycle correctness.

## Risks & Mitigations
- **Native memory leaks**: require `close()`/shutdown wiring and tests that assert accounting returns to zero.
- **API over-design**: keep the abstraction minimal (only what DB needs).
- **Build complexity**: isolate the Foreign Memory backend behind a profile; keep default `mvn test` working on Java 17.

