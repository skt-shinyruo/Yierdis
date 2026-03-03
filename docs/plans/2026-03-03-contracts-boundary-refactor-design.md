# Contracts Boundary Refactor — Design

**Date:** 2026-03-03  
**Branch:** `refactor/contracts-boundary`

## Goal

Refactor module boundaries so that:

1. “Execution contracts” (command IR + reply writer + session/transaction state) live in a dedicated, stable contract module, not in `protocol-model`.
2. `protocol-model` becomes “protocol model only” (limits + reply IR model + build info), and no longer contains CLI parsing utilities.
3. `yierdis-offheap-api` becomes a pure contract module (no hard-coded implementation class names / reflection fallbacks).
4. Instance-level background maintenance (expire cleanup + maxmemory enforcement) is an explicit runtime component, reusable outside the Netty server bootstrap.
5. `YierdisInstance` exposes only safe, implementation-agnostic views (no leaking `YierdisDb[]` / covariant array hazards).

Non-goal: change wire protocol, command semantics, performance profile, or existing public CLI flags.

## Current Issues (from code)

### 1) `protocol-model` mixes responsibilities

`yierdis-protocol-model` currently contains:

- execution contracts (e.g. `Command`, `ReplyWriter`, `CommandContext`, `ServerSession`, `TransactionState`)
- protocol limits/build info (`ProtocolLimits`, `YierdisBuildInfo`)
- reply IR model (`yier.bubu.redis.protocol.reply.*`)
- **CLI-only parsing** (`InlineCommandParser`)

This blurs boundaries and widens the dependency surface for components that should not “depend on protocol”.

### 2) off-heap “API” is not pure

`yierdis-offheap-api` currently contains `YierdisOffHeapAllocators` which:

- uses `ServiceLoader` (good)
- but also hard-codes backend implementation class names + reflection fallback (not a pure contract)

This makes the contract module aware of implementation details and forces edits in the API module when adding/changing backends.

### 3) server bootstrap owns instance maintenance logic

`YierdisServerBootstrap` schedules periodic maintenance and contains “global vs per-db” logic that relies on an implicit `firstDb` convention for global maxmemory enforcement. This is correct but:

- is not reusable for embedded use cases
- is easy to drift (maintenance logic living in server instead of runtime)

### 4) `YierdisInstance` leaks implementation and unsafe views

`YierdisInstance.engines()` returns the underlying `YierdisDb[]` as a covariant `DbEngine[]` view, and also exposes `dbs()` / `db(int)` returning `YierdisDb`. This increases coupling and introduces “array covariance / mutation” hazards.

## Proposed Architecture

### A) New module: `yierdis-core-contract` (recommended)

Add a new Maven module under `yierdis-core/`:

- Module path: `yierdis-core/yierdis-core-contract`
- Artifact: `yierdis-core-contract`
- Package: `yier.bubu.redis.contract.*` (new package, avoids split-package problems)

Move “execution contracts” from `yierdis-protocol-model` into this module:

- `Command`
- `ReplySink`
- `ReplyWriter`
- `ReplyWriterFactory`
- `CommandContext`
- `Session`
- `DbIndexProvider`
- `ServerSession`
- `TransactionState`

After this move:

- command layer depends on `core-contract` (instead of `protocol-model`)
- server/netty adapters depend on `core-contract`
- protocol codecs depend on `core-contract` for `Command` / `ReplyWriterFactory` contracts

`yierdis-protocol-model` keeps:

- `ProtocolLimits`
- `YierdisBuildInfo`
- `yier.bubu.redis.protocol.reply.*` (reply IR model)

### B) Move `InlineCommandParser` back to client/CLI

Move:

- from: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/InlineCommandParser.java`
- to: `yierdis-client/src/main/java/yier/bubu/redis/client/InlineCommandParser.java`

Update `YierdisCli` to import it from `yier.bubu.redis.client`.

### C) Make off-heap API pure: `ServiceLoader` only

Refactor `yierdis-offheap-api`:

- `YierdisOffHeapAllocators` becomes “provider resolver” only:
  - discover available providers via `ServiceLoader<YierdisOffHeapAllocatorProvider>`
  - select provider by `backend()`
  - delegate creation to `provider.create(maxBytes)`
  - on missing provider or provider failure, throw `YierdisOffHeapBackendUnavailableException` with a clear message
- remove:
  - hard-coded class names
  - reflection fallback
  - backend-specific module presence checks (these should live in providers or server wiring)

This preserves the existing extension mechanism (`META-INF/services/...`) and ensures the API module is a pure contract + discovery layer.

### D) Explicit instance maintenance component

Introduce a runtime component in `yierdis-core-runtime`:

- `yier.bubu.redis.runtime.YierdisInstanceMaintenance`

Responsibilities:

- perform a “maintenance tick” on a `YierdisInstance`:
  - expire cleanup for all DBs (`expiration().cleanupExpired()`)
  - enforce maxmemory according to configured scope:
    - `PER_DB`: enforce per DB
    - `GLOBAL`: enforce once in an instance-aware way (no `firstDb` convention in server code)

Server bootstrap becomes:

- schedule periodic job on Netty worker loop
- execute the maintenance tick inside executor owner thread via `executeMaintenance(...)`

Embedded usage can call the same maintenance component explicitly (or use a scheduled executor).

### E) Tighten `YierdisInstance` surface area

Change `YierdisInstance` public API to avoid leaking `YierdisDb` and unsafe arrays:

- replace `DbEngine[] engines()` with `List<DbEngine> engines()` (or `DbEngine[] enginesCopy()`)
- remove or restrict `YierdisDb[] dbs()` and `YierdisDb db(int)` from the public API
  - tests in the same module may use package-private helpers if needed

This keeps server/runtime consumers working with `DbEngine` only.

## Compatibility & Invariants

- Wire protocol: unchanged.
- Command semantics: unchanged (validated by existing unit/integration tests).
- Performance: no intentional regressions; contract moves are mostly compile-time structure changes.
- Architecture guards: update existing boundary tests to reflect new contract package (and keep the “no protocol imports in db/ops” invariant).

## Rollout Plan (high-level)

1. Add `yierdis-core-contract` module, move contract classes, and update Maven dependencies.
2. Update code imports across modules and ensure `mvn test` is green.
3. Move `InlineCommandParser` to `yierdis-client`, update CLI, keep behavior identical.
4. Refactor `YierdisOffHeapAllocators` to `ServiceLoader` only; update callers/tests.
5. Add `YierdisInstanceMaintenance`, wire server bootstrap periodic task to use it.
6. Tighten `YierdisInstance` API, adjust server and tests accordingly.

## Risks & Mitigations

- **Widespread compile breakage** (imports/poms): do the refactor in small commits and keep `mvn test` green frequently.
- **Boundary test drift**: update the guard tests explicitly to forbid the new contract package where appropriate.
- **off-heap backend availability errors**: keep error messages clear by surfacing available providers and backend requirements.

