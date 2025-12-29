# Project Context

## Purpose
This repository implements **Yierdis**, a **simplified Redis-compatible server** intended for learning and local demos.
It speaks **RESP2 over TCP** and focuses on “good enough” compatibility for core commands with an **in-memory-only**
data model.

## Tech Stack
- **Java 17** (language/runtime)
- **Maven** (build)
- **Netty** (networking + TCP server)
- **JUnit 4** (tests)

## Project Conventions

### Code Style
- 4-space indentation, no tabs.
- Keep classes small and focused.
- Prefer clear, minimal code over over-engineering.
- **Package prefix**: `yier.bubu.redis`
- **Naming**: use `Yierdis*` for server-side classes (avoid introducing new `Redis*` class names).

### Architecture Patterns
- Netty transport belongs in `YierdisServer` and its channel handlers.
- RESP framing/codec belongs under `src/main/java/yier/bubu/redis/protocol/`.
- Command routing/semantics belong under `src/main/java/yier/bubu/redis/command/`.
- Storage and TTL behavior belong under `src/main/java/yier/bubu/redis/db/`.
- Favor Redis-like single-threaded execution semantics (one worker event loop owns the DB) to keep concurrency simple.

### Testing Strategy
- Prefer deterministic tests.
- Avoid `Thread.sleep` when possible; TTL-related assertions should allow small timing variance.
- Use `mvn test` as the default verification step.

### Git Workflow
- Conventional Commits are preferred (`feat:`, `fix:`, `refactor:`, `chore:`).
- When practical, include “How verified” in the commit body (e.g., `mvn test`).

## Domain Context
- Protocol: **RESP2**.
- This server intentionally supports a limited subset of commands for learning/demos.
- There is no persistence; everything is memory-only.

## Important Constraints
- No AUTH/TLS and memory-only storage: **do not expose this server to untrusted networks**.
- Keep changes scoped and consistent with existing patterns; avoid large architectural shifts unless explicitly requested.

## External Dependencies
- Netty (network transport and buffers)
- SLF4J + Logback (logging)
- JaCoCo (test coverage reports during `mvn test`)
