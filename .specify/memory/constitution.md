# Yierdis Constitution

This repository is a **simplified Redis-compatible server** (RESP2 over TCP) written in **Java 17** with **Netty**. It is intentionally minimal and optimized for learning, demos, and correctness-focused iteration.

## Core Principles

### I. Compatibility Over Completeness
- Prefer **well-behaved RESP2** semantics and error messages over adding many commands.
- Preserve behavior that common clients rely on (e.g., `redis-cli --resp2`).
- Breaking changes require an explicit migration note.

### II. Keep The Server Minimal
- In-memory only; no production claims.
- Avoid adding large subsystems unless they clearly serve the learning goals (AOF/RDB/replication/cluster are out of scope unless explicitly planned).
- Prefer small, readable classes and direct control flow.

### III. Maintain The Project Identity
- Java package prefix is **`yier.bubu.redis`**.
- Server-side class naming uses **`Yierdis*`** (avoid introducing new `Redis*` server classes).
- Protocol framing stays in `protocol/`; command semantics stay in `command/` + `db/`.

### IV. Safety & Predictability First
- No AUTH/TLS: do not encourage exposure to untrusted networks.
- Add **bounds** for anything user-controlled that may cause CPU/memory blowups (e.g., RESP bulk length, array depth).
- Prefer deterministic tests; avoid `Thread.sleep` (allow small time variance for TTL assertions).

### V. Measure Before Optimizing
- Optimize only after identifying a clear bottleneck (profiling, benchmarks, or reproducible tests).
- Avoid changes that complicate the codebase without a measurable benefit.

## Quality Gates
- `mvn test` must pass for all changes.
- New commands or semantics changes require tests under `src/test/java/yier/bubu/redis/**`.
- Keep formatting consistent (4 spaces, no tabs).

## Non-Goals (Unless Explicitly Planned)
- Persistence (AOF/RDB), replication, cluster, Lua, ACL, TLS.
- Production-grade performance tuning and hardening.

## Governance
- This constitution is the top-level guidance for repo changes.
- Amendments should include: rationale, affected behaviors, and how to verify (tests/commands).

**Version**: 1.0.0 | **Ratified**: 2025-12-18 | **Last Amended**: 2025-12-18
