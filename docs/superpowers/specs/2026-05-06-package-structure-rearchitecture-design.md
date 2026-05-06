# Package Structure Rearchitecture Design

## Summary

This design defines an aggressive but staged rearchitecture of Yierdis package
structure, Maven module layout, and test ownership.

The current codebase already has useful architectural boundaries: protocol,
execution, command, storage, runtime, memory, executor, server, client, bench,
architecture tests, and integration tests are separate Maven areas. The remaining
problem is consistency. Many Java packages still use broad legacy names such as
`yier.bubu.redis.db`, `yier.bubu.redis.command`, `yier.bubu.redis.contract`, and
the server root package `yier.bubu.redis`. Those names make the implemented
ownership model harder to see and harder to enforce.

The target is a structure where Maven modules, Java packages, tests, and
architecture policy all tell the same story:

- protocol owns wire formats and transport adapters;
- execution owns transport-neutral request, reply, session, and engine seams;
- command owns command contracts, parser/dispatch kernel, and default command
  families;
- storage owns DB capability APIs and the in-memory storage implementation;
- runtime owns embedded instance lifecycle and DB resource orchestration;
- executor owns queueing, scheduling, and backpressure only;
- apps own final composition and deployable entry points;
- tests are separated into module-local unit tests, cross-module integration
  tests, and whole-repo architecture guards.

This is intentionally a staged design. It should not be implemented as one
large mechanical move. Each phase must leave the repository buildable and must
update architecture guards before broad package rewrites proceed.

## Current State

The active Maven graph already includes these top-level areas:

```text
yierdis-memory
yierdis-bytes
yierdis-execution
yierdis-storage
yierdis-runtime
yierdis-protocol
yierdis-command
yierdis-executor-core
yierdis-client
yierdis-app/yierdis-server-app
yierdis-bench
yierdis-architecture-tests
yierdis-integration-tests
```

The strongest current boundaries are:

- `yierdis-custom-v1-wire` is wire-only protocol code.
- `yierdis-custom-v1-execution-adapter` adapts protocol DTOs to execution
  contracts and provides Custom Protocol v1 reply writing.
- `yierdis-custom-v1-netty` owns Netty protocol glue.
- `yierdis-execution-api` owns `ExecutionRequest`, `ReplyWriter`, `Session`,
  and related transport-neutral contracts.
- `yierdis-engine` owns the `YierdisEngine` execution facade and
  `EngineSession`.
- `yierdis-command-api`, `yierdis-command-kernel`, and
  `yierdis-command-defaults` separate command registration contracts,
  dispatch/transaction mechanics, and default command families.
- `yierdis-storage-api` owns command-facing DB capabilities.
- `yierdis-storage-memory` owns the concrete in-memory DB implementation.
- `yierdis-runtime-api` owns embedded runtime public contracts.
- `yierdis-runtime-embedded` owns multi-DB instance lifecycle and resource
  orchestration.
- `yierdis-executor-core` owns scheduling, queueing, and backpressure.
- `yierdis-server-app` is the process composition root.
- `yierdis-architecture-tests` and `yierdis-integration-tests` already exist as
  whole-repo verification modules.

The main mismatch is naming and package ownership:

- `storage-memory` implementation code is mostly under
  `yier.bubu.redis.db`, even though the business boundary is now storage.
- memory foreign code lives under `yier.bubu.redis.db.memory.foreign`, even
  though it is a memory backend, not a DB-owned package.
- command API, kernel, and defaults all share `yier.bubu.redis.command`, making
  artifact boundaries invisible from imports.
- execution contracts use `yier.bubu.redis.contract`, which hides their
  execution-layer ownership.
- server-app classes live directly under `yier.bubu.redis`, which makes app
  composition look like the project root.
- many runtime-embedded tests exercise full command behavior, which blurs
  runtime lifecycle tests and command integration tests.
- architecture policy still contains historical forbidden artifact names and
  old package prefixes.

## Goals

1. Make Maven module names, Java package names, and documentation align with the
   same architectural model.
2. Move broad legacy packages to ownership-revealing package prefixes.
3. Keep public seams thin and explicit while preserving staged compatibility
   where package migration risk is high.
4. Separate module-local unit tests from cross-module command/runtime/server
   integration tests.
5. Update architecture guards so they enforce the target model, not historical
   package names.
6. Preserve current runtime behavior throughout the migration.

## Non-Goals

- Do not redesign the Yierdis protocol or add RESP compatibility.
- Do not split every command family or storage data structure into its own Maven
  module immediately.
- Do not introduce a generic `common` module.
- Do not move server-only commands into transport-neutral command defaults.
- Do not make runtime own command processor assembly again.
- Do not remove compatibility shims until all production and test imports have
  migrated.

## Design Principles

Package names should expose ownership, not implementation history. A reader
should be able to tell whether a class belongs to protocol, execution, command,
storage, runtime, memory, executor, or app code from the package prefix alone.

Maven modules should enforce dependency direction. Java packages should make the
same direction visible in source code. Architecture tests should then verify both
artifact edges and import-prefix rules.

Internal packages must be real boundaries. A package containing `.internal`
belongs only to its owning artifact unless another artifact is explicitly
allowlisted for SPI use.

Compatibility is allowed only as a migration tool. Legacy package shims may
exist temporarily, but new source should use the target package names.

## Implementation Decisions

The migration plan fixes the remaining sequencing and compatibility choices so
the spec and the implementation checklist stay aligned:

1. Move physical Maven module directories before Java package renames.
2. Keep Maven artifact IDs stable during the first migration pass.
3. Use temporary legacy facades for `contract`, `ops`, and `offheap.api`.
4. Move the server-app package first, then move client and bench after the
   server-app package is stable.
5. Keep integration-test fixtures in `tests/yierdis-integration-tests` unless
   duplication still remains after the app-package move.
6. Remove compatibility facades only after target imports are verified and the
   architecture tests enforce the new package names.

## Considered Approaches

### Approach A: Package-Only Cleanup

Keep all current Maven modules and only rename Java packages.

This is the lowest-risk path, but it would leave root-level module layout and
test placement less clear. It also does not address runtime-embedded tests that
are really command integration tests.

### Approach B: Full Module Explosion

Promote command families, storage values, TTL, maxmemory, keyspace, protocol
codec, protocol model, app commands, and memory backends into many independent
Maven modules.

This makes ownership extremely explicit, but it would overfit the current code.
Many seams are internal implementation seams rather than stable public
contracts. The result would be more POM work and more dependency edges without a
clear payoff.

### Approach C: Hybrid Aggressive Reorganization

Reorganize top-level module directories into clearer `libs`, `apps`, and
`tests` areas; rename Java packages to explicit ownership prefixes; keep the
current number of production modules mostly stable; add compatibility shims only
where public imports need a staged migration; and move misplaced tests to the
right test module.

Chosen.

This approach is aggressive enough to fix the mental model while avoiding
unnecessary module explosion.

## Target Maven Layout

The target physical layout should be:

```text
libs/
  bytes/
    yierdis-bytes-lib/
    yierdis-bytes-netty/
  memory/
    yierdis-memory-api/
    yierdis-memory-foreign/
  execution/
    yierdis-execution-api/
    yierdis-engine/
  command/
    yierdis-command-api/
    yierdis-command-kernel/
    yierdis-command-defaults/
  storage/
    yierdis-storage-api/
    yierdis-storage-testkit/
    yierdis-storage-memory/
  runtime/
    yierdis-runtime-api/
    yierdis-runtime-embedded/
  protocol/
    yierdis-custom-v1-wire/
    yierdis-custom-v1-execution-adapter/
    yierdis-custom-v1-netty/
  executor/
    yierdis-executor-core/

apps/
  yierdis-server-app/
  yierdis-client/
  yierdis-bench/

tests/
  yierdis-architecture-tests/
  yierdis-integration-tests/
```

The artifact IDs do not need to change in the first migration pass. Keeping
artifact IDs stable reduces downstream Maven churn while the physical layout and
package names change.

Aggregator POMs can remain if they continue to help local builds, but they
should follow the same ownership names:

- `libs/command/pom.xml`
- `libs/storage/pom.xml`
- `libs/runtime/pom.xml`
- `libs/protocol/pom.xml`
- `libs/memory/pom.xml`
- `libs/bytes/pom.xml`
- `libs/execution/pom.xml`

The root POM should list modules by the new paths.

## Target Java Package Model

The root namespace remains `yier.bubu.redis` to avoid changing group identity.
The ownership prefixes under it should change.

### Bytes

```text
yier.bubu.redis.bytes
yier.bubu.redis.bytes.netty
```

These packages are already acceptable. No broad rename is needed.

### Memory

Target:

```text
yier.bubu.redis.memory.api
yier.bubu.redis.memory.foreign
```

Current:

```text
yier.bubu.redis.offheap.api
yier.bubu.redis.db.memory.foreign
```

`offheap.api` can remain as a temporary compatibility facade if needed, but new
code should depend on `memory.api`. The FFM implementation should move out of
the DB package because it is a memory backend used by storage, not DB business
logic itself.

### Execution

Target:

```text
yier.bubu.redis.execution.api
yier.bubu.redis.execution.engine
```

Current:

```text
yier.bubu.redis.contract
yier.bubu.redis.engine
```

The execution API package should make transport-neutral execution ownership
explicit. `CommandContext`, `Session`, `ReplyWriter`, `ExecutionRequest`, and
`TransactionState` belong here unless later split into more precise subpackages.

### Command

Target:

```text
yier.bubu.redis.command.api
yier.bubu.redis.command.kernel
yier.bubu.redis.command.defaults
yier.bubu.redis.command.defaults.string
yier.bubu.redis.command.defaults.hash
yier.bubu.redis.command.defaults.list
yier.bubu.redis.command.defaults.set
yier.bubu.redis.command.defaults.zset
yier.bubu.redis.command.defaults.hll
yier.bubu.redis.command.defaults.keyspace
yier.bubu.redis.command.defaults.ttl
yier.bubu.redis.command.defaults.connection
yier.bubu.redis.command.defaults.transaction
```

Current:

```text
yier.bubu.redis.command
```

All three command artifacts currently share one package prefix, which makes
artifact boundaries hard to see in imports. The package model should mirror the
module split:

- API contracts in `command.api`;
- processor, registry, transaction dispatch mechanics in `command.kernel`;
- built-in command families in `command.defaults.*`.

Server-facing commands such as `HELLO`, `INFO`, and `STATS` should not move into
`command.defaults` if they require build info, executor observability, protocol
specifics, or app-level state.

### Storage

Target API:

```text
yier.bubu.redis.storage.api
yier.bubu.redis.storage.api.result
yier.bubu.redis.storage.testkit
```

Current API:

```text
yier.bubu.redis.ops
yier.bubu.redis.ops.result
yier.bubu.redis.storage.testkit
```

Target memory implementation:

```text
yier.bubu.redis.storage.memory
yier.bubu.redis.storage.memory.internal
yier.bubu.redis.storage.memory.internal.key
yier.bubu.redis.storage.memory.internal.keyspace
yier.bubu.redis.storage.memory.internal.expire
yier.bubu.redis.storage.memory.internal.ledger
yier.bubu.redis.storage.memory.internal.value
yier.bubu.redis.storage.memory.internal.value.string
yier.bubu.redis.storage.memory.internal.value.hash
yier.bubu.redis.storage.memory.internal.value.list
yier.bubu.redis.storage.memory.internal.value.set
yier.bubu.redis.storage.memory.internal.value.zset
yier.bubu.redis.storage.memory.internal.value.hll
yier.bubu.redis.storage.memory.internal.ffm
```

Current memory implementation:

```text
yier.bubu.redis.db
yier.bubu.redis.db.key
yier.bubu.redis.db.memory
yier.bubu.redis.db.memory.ffm
```

`YierdisDb` should become a storage-memory facade. Internal data structures,
indexes, ledgers, value encodings, mutation support, TTL support, and FFM-backed
storage structures should be under storage-memory internal packages.

The API package should eventually stop using `ops`. `ops` describes usage style,
not ownership. The new `storage.api` prefix tells command code where the
boundary lives.

### Runtime

Target:

```text
yier.bubu.redis.runtime.api
yier.bubu.redis.runtime.embedded
yier.bubu.redis.runtime.embedded.internal
```

Current:

```text
yier.bubu.redis.runtime
yier.bubu.redis.runtime.api
```

`YierdisInstanceConfig`, `YierdisChangeEvent`, and `YierdisChangeSink` should
remain runtime API types. `YierdisInstance`, `YierdisInstanceResources`,
`YierdisInstanceMaintenance`, `YierdisInstanceRuntimeAccess`, and
`YierdisGlobalMaxmemoryGovernor` should move to `runtime.embedded`.

### Protocol

Target:

```text
yier.bubu.redis.protocol.custom.v1.wire
yier.bubu.redis.protocol.custom.v1.reply
yier.bubu.redis.protocol.custom.v1.json
yier.bubu.redis.protocol.custom.v1.execution
yier.bubu.redis.protocol.custom.v1.netty
```

Current:

```text
yier.bubu.redis.protocol
yier.bubu.redis.protocol.v1
yier.bubu.redis.protocol.reply
yier.bubu.redis.protocol.json
yier.bubu.redis.protocol.netty
```

The package should make the protocol name and adapter layer explicit. If a
future protocol exists, it should not share ambiguous `protocol.v1` packages.

### Executor

Target:

```text
yier.bubu.redis.execution.executor
```

Current:

```text
yier.bubu.redis.executor
```

The executor is part of the execution lane but does not own command semantics.
The package name should make that relationship visible without merging it into
engine or command packages.

### Apps

Target server app:

```text
yier.bubu.redis.app.server
yier.bubu.redis.app.server.args
yier.bubu.redis.app.server.netty
yier.bubu.redis.app.server.command
yier.bubu.redis.app.server.info
```

Current server app:

```text
yier.bubu.redis
yier.bubu.redis.args
```

The server app should no longer occupy the root project package. `YierdisServer`
and `YierdisServerBootstrap` are app composition classes, not root-domain
classes.

Target client and bench:

```text
yier.bubu.redis.app.client
yier.bubu.redis.app.bench
```

Current:

```text
yier.bubu.redis.client
yier.bubu.redis.bench
```

These can migrate later because they are already less ambiguous than server-app.

## Compatibility Strategy

Use a two-stage package migration for public or widely used seams:

1. Introduce new target packages and move implementation.
2. Keep deprecated facade or forwarding types in old packages where needed.
3. Migrate production imports to target packages.
4. Migrate tests and docs.
5. Tighten architecture tests to forbid new production imports from legacy
   packages.
6. Remove facades only after a full-tree search shows no production dependency
   remains and the user accepts the breaking cleanup.

Compatibility facades are most useful for:

- `yier.bubu.redis.contract` -> `yier.bubu.redis.execution.api`
- `yier.bubu.redis.ops` -> `yier.bubu.redis.storage.api`
- `yier.bubu.redis.offheap.api` -> `yier.bubu.redis.memory.api`

They are less useful for internal implementation packages:

- `yier.bubu.redis.db`
- `yier.bubu.redis.db.memory.ffm`
- `yier.bubu.redis.command` internals
- root server app package `yier.bubu.redis`

Internal packages should be migrated directly because external compatibility is
not the purpose of those names.

## Test Architecture

Tests should be divided by ownership:

### Module-Local Unit Tests

Each production module keeps focused tests for its own internals:

- protocol wire parser/writer tests stay with protocol wire;
- Netty decoder tests stay with protocol Netty;
- command parser and registry tests stay with command modules;
- storage data structure and accounting tests stay with storage-memory;
- runtime instance lifecycle tests stay with runtime-embedded;
- executor queueing/backpressure tests stay with executor-core;
- server bootstrap lifecycle tests stay with server-app.

### Integration Tests

Cross-module behavior should move to `tests/yierdis-integration-tests`:

- command behavior using real default command modules and storage;
- command/runtime interactions such as TTL and maxmemory behavior;
- request execution path smoke tests;
- server/client protocol smoke tests;
- regression tests that intentionally combine command, runtime, storage, and
  execution layers.

Several tests currently under `yierdis-runtime-embedded/src/test/java/.../command`
belong here because they validate command semantics with runtime/storage wiring,
not runtime lifecycle ownership.

### Architecture Tests

`tests/yierdis-architecture-tests` should own:

- Maven dependency policy;
- production import allowlists and forbidden prefixes;
- `.internal` package ownership rules;
- package migration guards that reject new production imports from legacy
  packages;
- source-ownership checks for high-risk behavior:
  - command parser and registry ownership;
  - command context construction;
  - protocol reply encoding;
  - storage internals;
  - server-app composition;
  - runtime lifecycle and maintenance only.

The current `architecture-policy.yml` should be updated from old package names
to target package names and should remove obsolete historical artifact names
once they no longer provide useful protection.

## Migration Plan

### Phase 0: Freeze Target Policy

Create the target package and module policy in documentation and architecture
test resources before moving source. Add guard tests that permit current legacy
packages but define the target allowlist.

Verification:

```text
mvn -pl yierdis-architecture-tests test
```

### Phase 1: Move Physical Module Layout

Move Maven modules into `libs`, `apps`, and `tests` directories while keeping
artifact IDs stable. Update root and aggregator POM module paths. Do not rename
Java packages in this phase.

Verification:

```text
mvn test
```

### Phase 2: Rename App and Executor Packages

Move server-app out of the root package and move executor to
`execution.executor`. These are high-signal and lower compatibility-risk package
renames.

Verification:

```text
mvn -pl apps/yierdis-server-app,libs/executor/yierdis-executor-core test
mvn -pl tests/yierdis-architecture-tests test
```

### Phase 3: Rename Command Packages

Move command API, kernel, and defaults into package prefixes that match their
artifacts. Split default command families into subpackages. Keep command module
artifact IDs stable.

Verification:

```text
mvn -pl libs/command/yierdis-command-api,libs/command/yierdis-command-kernel,libs/command/yierdis-command-defaults test
mvn -pl tests/yierdis-architecture-tests test
```

### Phase 4: Rename Execution and Storage API Packages With Facades

Introduce `execution.api` and `storage.api` target packages. Keep compatibility
facades for legacy `contract` and `ops` packages during migration.

Verification:

```text
mvn -pl libs/execution/yierdis-execution-api,libs/storage/yierdis-storage-api test
mvn -pl tests/yierdis-architecture-tests test
```

### Phase 5: Rename Storage-Memory Internals

Move `db` implementation packages under `storage.memory` and its internal
subpackages. This is the largest phase and should be sliced by internal
ownership:

1. key handles and keyspace;
2. expire index and TTL support;
3. memory ledger and mutation executor;
4. value objects and encoding helpers;
5. FFM-backed storage structures;
6. DB facade, factory, components, introspection, and lifecycle adapters.

Verification should run storage-memory tests after each slice.

```text
mvn -pl libs/storage/yierdis-storage-memory test
mvn -pl tests/yierdis-architecture-tests test
```

### Phase 6: Rename Runtime and Memory Packages

Move runtime embedded implementation to `runtime.embedded` and memory API/backend
packages to `memory.api` and `memory.foreign`. Keep public facades only where
needed.

Verification:

```text
mvn -pl libs/runtime/yierdis-runtime-api,libs/runtime/yierdis-runtime-embedded,libs/memory/yierdis-memory-api,libs/memory/yierdis-memory-foreign test
mvn -pl tests/yierdis-architecture-tests test
```

### Phase 7: Move Misplaced Tests

Move command/runtime/storage integration tests out of runtime-embedded and into
the integration-test module. Deduplicate local `testutil` packages or extract a
small integration-test fixture package if the duplication persists.

Verification:

```text
mvn -pl tests/yierdis-integration-tests test
mvn test
```

### Phase 8: Remove Legacy Facades

After production imports use the target packages and tests pass, remove
compatibility facades in a separate breaking-cleanup phase.

Verification:

```text
mvn test
rg 'yier\\.bubu\\.redis\\.(contract|ops|offheap\\.api|db)(\\.|;)' -g '*.java'
```

## Architecture Guard Updates

Architecture policy should move from forbidden historical names to explicit
target ownership. Example target rules:

- `command.api` may depend on execution API and storage API only.
- `command.kernel` may depend on command API and runtime API only.
- `command.defaults` may depend on command API, execution API, storage API, and
  bytes-lib only.
- `storage.memory` must not depend on command, protocol, executor, Netty, or
  app modules.
- `runtime.embedded` may depend on runtime API, storage API, storage-memory, and
  memory backend, but not command-defaults or server-app.
- `execution.executor` may depend on execution API only.
- protocol wire must not depend on execution, command, storage, runtime, Netty,
  or app modules.
- protocol execution adapter may depend on protocol wire, execution API, and
  bytes-lib.
- protocol Netty may depend on protocol wire, protocol execution adapter,
  bytes-netty, and Netty.
- server-app may depend on the composition set: execution API, engine, command
  modules, runtime, storage, protocol adapters, executor, bytes-netty, memory
  backend, Netty, logging, and CLI parsing.

Import guards should forbid:

- direct imports of any `.internal` package from outside its owning artifact;
- command packages importing protocol or storage-memory internals;
- storage-memory packages importing command, protocol, executor, Netty, or app
  packages;
- runtime packages importing server-app, protocol, executor, or command-defaults
  production packages unless explicitly part of a test;
- execution API importing command, storage implementation, runtime, protocol,
  Netty, memory backend, or app packages;
- server-app classes living in the root `yier.bubu.redis` package.

## Risks

Package migration has high merge-conflict risk. The work should happen in small
phases and avoid concurrent feature work touching the same files.

Compatibility facades can linger and weaken the design. Each facade needs an
explicit removal condition and architecture guard that prevents new production
use.

Maven path changes can break CI scripts, docs, and developer commands even when
Java code still compiles. Documentation and scripts need to be updated in the
same phase as module moves.

Large package moves can hide behavior changes. Each phase should prefer pure
move/rename commits and keep semantic edits separate.

## Acceptance Criteria

- Root Maven module paths use `libs`, `apps`, and `tests`.
- Java package prefixes reveal ownership for protocol, execution, command,
  storage, runtime, memory, executor, and apps.
- Server-app no longer uses the root `yier.bubu.redis` package.
- Storage-memory implementation no longer uses `yier.bubu.redis.db`.
- Command API, kernel, and defaults no longer share one package prefix.
- Runtime-embedded tests cover runtime ownership; command/runtime behavior tests
  live in integration tests.
- Architecture policy is expressed in target package names and artifact names.
- Architecture tests reject `.internal` imports across owner boundaries.
- Full Maven test suite passes after each completed migration phase.

## Implementation Notes

No open migration decisions remain for the initial rearchitecture. If any of
the choices above need to change later, update this spec and the matching plan
before implementation begins.
