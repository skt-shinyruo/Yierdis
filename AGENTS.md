# Repository Guidelines

## Goal & Scope

This repository implements a **simplified Redis-compatible server** (RESP2 over TCP) in **Java 17** using **Maven**. Networking is handled by **Netty**. The code is intentionally minimal: in-memory only, core commands only, and “good enough” compatibility for learning and local demos.

Key requirements for contributions:
- Java toolchain: **JDK 17**
- Build system: **Maven**
- Package prefix: **`yier.bubu.redis`**
- Naming: use **`Yierdis*`** for server-side classes (e.g. `YierdisServer`, `YierdisDb`) even though the protocol is Redis-compatible. Avoid introducing new `Redis*` class names.

## Project Structure & Module Organization

- `pom.xml`: single-module build that produces a runnable shaded JAR.
- `src/main/java/yier/bubu/redis/**`: main code.
  - `protocol/`: RESP2 types + `RespDecoder`/`RespEncoder`
  - `command/`: command routing/semantics (`CommandProcessor`)
  - `db/`: in-memory data store + TTL handling
- `src/main/resources/`: runtime resources (e.g. `logback.xml`).
- `src/test/java/yier/bubu/redis/**`: JUnit 4 tests mirroring the main packages.

## Build, Test, and Run

- `mvn test`: compile + run unit tests.
- `mvn -DskipTests package`: build shaded JAR at `target/yierdis-0.1.0-SNAPSHOT.jar`.
- `java -jar target/yierdis-0.1.0-SNAPSHOT.jar --port 6378`: run locally.
  - Verify with `redis-cli --resp2 -p 6378 ping`.

## Coding Style & Testing

- Style: 4-space indentation, no tabs, keep classes small and focused.
- Architecture: keep Netty transport in `YierdisServer`/handler; keep RESP framing in `protocol/`; keep command behavior in `command/` + `db/`.
- Tests: deterministic when possible; avoid `Thread.sleep` (TTL assertions should allow small timing variance).

## Commit & Pull Request Guidelines

- If Git is used, prefer Conventional Commits (`feat:`, `fix:`, `refactor:`) and include “how verified” (e.g. `mvn test`) in the PR description.

## Security Notes

No AUTH/TLS and memory-only storage: do not expose this server to untrusted networks.
