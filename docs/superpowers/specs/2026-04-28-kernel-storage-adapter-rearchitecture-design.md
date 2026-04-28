# Kernel, Storage, And Adapter Rearchitecture Design

## Summary

This design defines a deeper Maven and code-structure rearchitecture for
Yierdis.

The current architecture has improved through several focused refactors:
execution requests are separated from protocol DTOs, command execution enters
through `YierdisEngine`, DB access goes through capability interfaces, and
executor-core is no longer supposed to own command semantics. Those changes make
the request path clearer, but they do not fully fix the larger structural issue:
several modules still bundle multiple architectural concepts under broad names
such as `core-api`, `core-command`, `core-db`, and `server`.

The target architecture is:

```text
foundation -> execution-api -> command kernel/features
           -> storage-api -> storage implementations/policies
           -> runtime
           -> protocol/transport adapters
           -> applications
```

In practical terms, this means moving from the current "core plus server" shape
to a system organized around four explicit ownership areas:

- execution kernel contracts
- storage contracts and implementations
- command feature modules
- protocol, transport, and application adapters

This is a roadmap spec. It is too large and too risky to implement as one
mechanical commit. Each phase must preserve behavior, keep the project
buildable, and add or update architecture guards before the next phase starts.

## Problem Statement

The current Maven graph expresses useful boundaries, but several modules are too
wide:

- `yierdis-core-api` contains DB capability contracts, off-heap API contracts,
  maxmemory coordination contracts, and runtime change-tracking contracts.
- `yierdis-core-command` contains command registry mechanics, command execution
  pipeline, parsers, command-family implementations, command metadata, and
  transaction policy.
- `yierdis-core-db` contains DB facade code, keyspace implementations, value
  encodings, FFM-backed structures, memory accounting, expiration, maxmemory,
  and concrete factory logic.
- `yierdis-core-runtime` owns runtime lifecycle but also carries many
  cross-module architecture and integration tests.
- `yierdis-server` is the process application, transport composition root,
  protocol-to-execution adapter, reply-writer home, server-facing command
  provider, INFO/STATS provider, and native-memory availability gate.
- `yierdis-args` is shared by server and tooling, but its configuration model
  reaches across protocol limits and storage policies.
- `yierdis-protocol-codec` is kept independent from execution contracts, which
  forces execution reply encoding (`JsonLineReplyWriter`) to live in server even
  though it is conceptually part of Custom Protocol v1.

These issues are deeper than package naming. They make it hard to answer:

- Which module owns the executable command contract?
- Which module owns storage-facing APIs?
- Which module owns memory APIs?
- Which module owns protocol reply encoding?
- Which module is the application, and which modules are reusable library
  pieces?

The current layout still works, but it encourages future changes to continue
expanding wide modules instead of creating narrow, explicit ownership seams.

## Goals

- Replace broad `core-*` module ownership with explicit kernel, command,
  storage, runtime, adapter, and application ownership.
- Split `core-api` into narrower API modules so command code does not see
  memory/runtime contracts it does not need.
- Split `core-command` into command API, command kernel, and command-family
  modules.
- Split `core-db` into storage API, storage memory implementation, keyspace,
  value, memory, expiration, and maxmemory policy modules where the split gives
  real compile-time ownership.
- Move cross-module architecture tests out of `core-runtime`.
- Make Custom Protocol v1 a first-class adapter family, including request
  parsing and execution reply encoding.
- Reduce `server` to an application composition module.
- Preserve the current behavioral scope: Custom Protocol v1, single-node
  in-memory storage, current command semantics, Netty server/client, and JDK 25
  FFM memory.
- Make the migration phaseable, with each phase independently testable and
  reversible.

## Non-Goals

- No Redis RESP compatibility.
- No persistence, replication, clustering, Lua, ACL, TLS, Pub/Sub, or module
  system redesign.
- No command semantic changes unless separately specified by focused command
  specs.
- No replacement of Netty.
- No attempt to make all command families load dynamically at runtime.
- No split that makes currently package-private storage internals public without
  a concrete ownership reason.
- No one-shot source-tree rename that breaks all history without improving
  module boundaries.
- No JPMS module-info migration in the first implementation cycle.

## Current Constraints To Preserve Or Replace

The current architecture guards and documents several rules that remain valid
after the rearchitecture, even if their module names change:

- Protocol request DTOs must not become command execution authority.
- `ExecutionRequest`, `Session`, and `ReplyWriter` remain execution contracts.
- Command execution must enter through `YierdisEngine` or its successor kernel
  facade.
- Executor-core owns scheduling, pending state, backpressure, and close state
  only.
- Command parsing and command metadata must stay in command modules.
- Command modules must not depend on concrete storage implementations.
- Storage pressure paths should use key identity handles where possible instead
  of forcing heap key materialization.
- Runtime owns instance lifecycle and owner-thread seams, not command parsing.
- Application/server code may compose modules, but it must not own command
  semantics or storage internals.

Existing guard tests should not simply be deleted. They should move to a new
architecture-test module and be rewritten against the new module names.

## Considered Approaches

### Approach A: Package-Level Cleanup Only

Keep the current Maven modules and reorganize packages inside them.

This reduces local reading cost but does not fix the broad ownership problem.
`core-api`, `core-command`, `core-db`, and `server` would remain large conceptual
buckets.

Rejected as the primary design because it does not match the requested depth of
change.

### Approach B: Collapse Into Fewer Large Modules

Merge current modules into a smaller set, for example `yierdis-kernel`,
`yierdis-storage`, `yierdis-protocol`, and `yierdis-server`.

This shortens the Maven graph, but it risks hiding coupling rather than removing
it. Large modules would still allow command code, storage internals, runtime
contracts, and application adapters to drift together.

Rejected because the project already has too many broad buckets. The next
architecture should introduce sharper boundaries, not fewer vague modules.

### Approach C: Kernel, Storage, Feature Commands, And Adapters

Split the system by ownership:

- foundational bytes and memory APIs;
- execution API and engine kernel;
- storage API and storage implementations;
- command API, command kernel, and command-family modules;
- runtime assembly over storage and command modules;
- protocol and transport adapters;
- applications and cross-module tests.

Chosen because it makes compile-time dependencies match the way the system is
explained: execution contracts, command behavior, storage behavior, runtime
lifecycle, adapters, and applications each have separate owners.

## Architectural Decision

Adopt Approach C.

The target architecture should make these rules obvious from module names and
dependencies:

```text
Commands depend on execution-api and storage-api.
Storage implementations depend on storage-api and memory APIs.
Runtime depends on storage implementations and runtime-api.
Executor depends on execution-api only.
Protocol adapters depend on protocol APIs and execution-api where they encode
or decode execution-facing messages.
Applications depend on adapters, runtime, command features, and executor.
```

The old `core-*` names should be retired gradually. The destination names should
describe architectural ownership, not historical location.

## Target Maven Structure

### Foundation

```text
yierdis-foundation
├─ yierdis-bytes
├─ yierdis-memory-api
└─ yierdis-memory-ffm
```

Responsibilities:

- `yierdis-bytes`: neutral byte abstractions such as `BytesView`, `BytesSlice`,
  `BytesSink`, and Netty-independent byte utilities.
- `yierdis-memory-api`: memory contracts such as `OffHeapAllocator`,
  `OffHeapSlice`, `OffHeapBuf`, and memory exceptions.
- `yierdis-memory-ffm`: JDK 25 FFM implementation and runtime resources.

Current source mapping:

- `yierdis-bytes-lib` maps to `yierdis-bytes`.
- `yierdis-bytes-netty` maps to `yierdis-transport-netty`; it must not move
  into neutral `yierdis-bytes`.
- `yierdis-core-api` off-heap contracts map to `yierdis-memory-api`.
- `yierdis-memory/foreign` maps to `yierdis-memory-ffm`.

### Execution

```text
yierdis-execution
├─ yierdis-execution-api
└─ yierdis-engine
```

Responsibilities:

- `yierdis-execution-api`: `ExecutionRequest`, `ExecutionRecord`, `ReplyWriter`,
  `ReplyWriterFactory`, `Session`, `ServerSession`, `CommandContext`, and
  connection stat views.
- `yierdis-engine`: `YierdisEngine`, `EngineSession`, default engine
  implementation, command context construction, transaction replay entry, and
  maintenance delegation.

Current source mapping:

- `yierdis-core-contract` maps mostly to `yierdis-execution-api`.
- `yierdis-core-engine` maps to `yierdis-engine`.

`yierdis-execution-api` must not depend on command modules, storage
implementations, protocol DTOs, Netty, or runtime implementations.

### Storage

```text
yierdis-storage
├─ yierdis-storage-api
├─ yierdis-storage-memory
├─ yierdis-storage-keyspace
├─ yierdis-storage-values
├─ yierdis-storage-expire
├─ yierdis-storage-maxmemory
└─ yierdis-storage-testkit
```

Responsibilities:

- `yierdis-storage-api`: command-facing DB capabilities:
  `DbEngine`, `DbReads`, `DbWrites`, `StringReadOps`, `StringWriteOps`,
  hash/list/set/zset/HLL ops, keyspace ops, TTL ops, memory ops,
  `DbEngineFactory`, and storage-facing result types.
- `yierdis-storage-memory`: concrete in-memory DB facade and orchestration:
  `YierdisDb`, `YierdisDbEngineFactory`, component factory, lifecycle, and
  read/write facade wiring.
- `yierdis-storage-keyspace`: heap and FFM keyspace structures, key handles,
  byte-array maps/sets, canonical key handling, and scan/key iteration
  primitives.
- `yierdis-storage-values`: string, hash, list, set, zset, HLL value
  implementations and compact encodings.
- `yierdis-storage-expire`: expire indexes, expiration manager implementation,
  cleanup policy, and key-sharing rules.
- `yierdis-storage-maxmemory`: maxmemory participant/coordinator contracts,
  eviction policy implementation, candidate sampling, memory accounting, and
  noeviction errors.
- `yierdis-storage-testkit`: reusable storage fixtures and conformance tests
  consumed by integration and command tests.

Current source mapping:

- Most of `yierdis-core-api/ops` maps to `yierdis-storage-api`.
- `yierdis-core-db` maps across the storage implementation modules.
- `yierdis-core-runtime` tests under storage packages should move to
  `yierdis-storage-testkit`, `yierdis-integration-tests`, or
  `yierdis-architecture-tests` depending on scope.

The storage split should be implemented carefully. Classes should move only when
the new module owns a coherent concept. If moving a class forces many internal
types to become public, delay that class until a narrower interface is defined.

### Commands

```text
yierdis-command
├─ yierdis-command-api
├─ yierdis-command-kernel
├─ yierdis-command-string
├─ yierdis-command-hash
├─ yierdis-command-list
├─ yierdis-command-set
├─ yierdis-command-zset
├─ yierdis-command-keyspace
├─ yierdis-command-transaction
├─ yierdis-command-connection
├─ yierdis-command-admin
└─ yierdis-command-defaults
```

Responsibilities:

- `yierdis-command-api`: `CommandDescriptor`, `CommandSpec`,
  `CommandParser`, `CommandHandler`, `CommandModule`, parser result contracts,
  and shared command error contracts.
- `yierdis-command-kernel`: `CommandRegistry`,
  `YierdisCommandProcessor` or successor, processor lifecycle, parse/dispatch
  pipeline, runtime error mapping, transaction queuing rules, and common
  `ArgReader` mechanics.
- `yierdis-command-*`: command-family implementations. Each family registers
  its own commands and depends only on command API/kernel support,
  execution-api, and storage-api.
- `yierdis-command-admin`: transport-neutral admin commands such as
  `COMMAND`, `FLUSHDB`, memory-related command behavior, and other commands
  that are not tied to a single value type.
- `yierdis-command-defaults`: the default command module bundle used by
  embedded runtime and server-app composition.

Current source mapping:

- `yierdis-core-command` splits across these modules.
- Server-facing commands that require build info, protocol-specific fields, or
  Netty/executor observability should remain application or adapter commands,
  not move into transport-neutral command modules.

Command-family modules must not depend on concrete storage implementations.
They may depend on `yierdis-storage-api`, `yierdis-execution-api`, and
`yierdis-command-api`.

### Runtime

```text
yierdis-runtime
├─ yierdis-runtime-api
└─ yierdis-runtime-memory
```

Responsibilities:

- `yierdis-runtime-api`: embedded instance configuration, runtime access,
  observability, change tracking, and lifecycle contracts.
- `yierdis-runtime-memory`: default in-memory runtime assembly:
  multi-DB instance creation, owner-thread access, global maxmemory
  coordination, maintenance, and resource cleanup.

Current source mapping:

- `yierdis-core-runtime` production code maps to these modules.
- `YierdisChangeEvent`, `YierdisChangeSink`, and `YierdisChangeTracking` should
  move from broad storage API ownership to runtime API ownership.

Runtime should not own command parser registration. It may accept a command
bundle or engine factory during composition.

### Executor

```text
yierdis-executor
└─ yierdis-executor-core
```

Responsibilities:

- queueing, scheduling, pending command accounting, pending byte accounting,
  backpressure decisions, close-after-reply state, fair/global drain policy, and
  owner-thread dispatch.

Dependencies:

- `yierdis-execution-api`
- foundational byte utilities only if needed for accounting

Executor must not depend on command modules, storage APIs, storage
implementations, runtime implementations, protocol DTOs, or Netty.

### Protocol And Transport Adapters

```text
yierdis-adapter
├─ yierdis-protocol-custom-v1-api
├─ yierdis-protocol-custom-v1-codec
├─ yierdis-protocol-custom-v1-netty
├─ yierdis-transport-netty
└─ yierdis-cli-common
```

Responsibilities:

- `yierdis-protocol-custom-v1-api`: protocol DTOs, protocol limits, protocol
  version constants, and Custom Protocol v1 request/reply model needed by
  tools. Application build information belongs to `yierdis-server-app`, not the
  protocol API.
- `yierdis-protocol-custom-v1-codec`: JSON parser/writer, request encoder,
  request payload parser, reply parser, reply inspector, NDJSON reply writer,
  and reply writer factory for Custom Protocol v1.
- `yierdis-protocol-custom-v1-netty`: Netty decoder/encoder glue for Custom
  Protocol v1.
- `yierdis-transport-netty`: transport-level connection integration that is not
  specific to Custom Protocol v1.
- `yierdis-cli-common`: shared CLI argument parsing primitives that are not
  server runtime config.

Important decision:

`yierdis-protocol-custom-v1-codec` may depend on `yierdis-execution-api` for
`ReplyWriter` and `ReplyWriterFactory`.

This replaces the current awkward rule that protocol-codec must not depend on
core-contract while server owns `JsonLineReplyWriter`. The new boundary is:

- protocol adapters may know execution-facing reply contracts;
- protocol adapters must not know command modules, storage APIs, storage
  implementations, runtime implementations, or application classes.

### Applications And Tests

```text
yierdis-app
├─ yierdis-server-app
├─ yierdis-client-cli
├─ yierdis-bench
├─ yierdis-integration-tests
└─ yierdis-architecture-tests
```

Responsibilities:

- `yierdis-server-app`: process entry point, server runtime config, module
  composition, Netty server bootstrap, server-facing commands, INFO/STATS
  provider, native memory availability checks, and process lifecycle.
- `yierdis-client-cli`: CLI entry point and client commands using protocol
  adapters.
- `yierdis-bench`: external benchmark tool using protocol adapters and CLI
  config.
- `yierdis-integration-tests`: cross-module behavior tests that require real
  runtime/server/client wiring.
- `yierdis-architecture-tests`: source and dependency guard tests for the new
  architecture.

Server-app is allowed to depend broadly because it is the composition root. It
should still contain little business logic.

## Target Request Flow

The final request flow should remain linear:

```text
Netty bytes
  -> custom-v1 netty decoder
  -> custom-v1 request DTO
  -> protocol adapter converts to ExecutionRequest
  -> executor-core schedules Session + ExecutionRequest + ReplyWriter
  -> yierdis-engine executes
  -> command-kernel lookup/parse/dispatch
  -> command-family handler calls storage-api
  -> storage implementation mutates or reads data
  -> ReplyWriter encodes custom-v1 NDJSON reply
  -> Netty flush
```

The important ownership difference is that each step belongs to a module family
with a narrow dependency rule.

## Target Dependency Rules

The architecture-test module should enforce at least these rules:

- `yierdis-execution-api` imports no protocol, command, storage implementation,
  runtime implementation, application, or Netty packages.
- `yierdis-command-api` imports no storage implementation, protocol, runtime
  implementation, application, or Netty packages.
- `yierdis-command-kernel` imports no concrete storage implementation packages.
- `yierdis-command-*` imports no concrete storage implementation packages.
- `yierdis-storage-api` imports no command, protocol, application, or Netty
  packages.
- `yierdis-storage-memory` imports no command implementation packages and no
  protocol packages.
- `yierdis-executor-core` imports only execution contracts and executor-owned
  support types.
- `yierdis-runtime-api` imports no storage implementation, protocol, command
  implementation, application, or Netty packages.
- `yierdis-runtime-memory` may depend on storage implementations and runtime
  API, but must not own command parser registration.
- `yierdis-protocol-custom-v1-codec` may import execution reply contracts, but
  must not import command or storage packages.
- `yierdis-protocol-custom-v1-netty` may import Netty and custom-v1 codec, but
  must not import command or storage packages.
- `yierdis-server-app` may compose all production modules, but source guards
  should prevent it from constructing command contexts, parsing command syntax,
  or directly using storage internals.

## Migration Phases

### Phase 1: Extract Execution API

Move command execution contracts out of `yierdis-core-contract` into
`yierdis-execution-api`.

Scope:

- `ExecutionRequest`
- `ExecutionRecord`
- `ByteArrayExecutionRequest`
- `ReplyWriter`
- `ReplyWriterFactory`
- `ReplySink`
- `Session`
- server/session capability interfaces
- `CommandContext`
- connection stat views

Acceptance criteria:

- Existing engine, executor, command, and server modules compile against
  `yierdis-execution-api`.
- No protocol DTO moves into execution API.
- Architecture guards assert execution API has no dependency on protocol,
  command implementation, storage implementation, runtime implementation,
  application, or Netty.

### Phase 2: Split Storage, Memory, And Runtime APIs

Split `yierdis-core-api` into:

- `yierdis-storage-api`
- `yierdis-memory-api`
- `yierdis-runtime-api`

Scope:

- DB operation contracts move to storage API.
- Off-heap contracts move to memory API.
- change tracking and runtime observability contracts move to runtime API.
- maxmemory contracts are classified explicitly as storage API or storage
  maxmemory implementation contracts.

Acceptance criteria:

- Command modules depend on storage API but not memory API unless a concrete
  command genuinely requires a memory contract.
- Storage implementation modules depend on memory API and storage API.
- Runtime modules depend on runtime API and storage API.
- Former broad `ops` imports are replaced or intentionally kept only in storage
  API packages.

### Phase 3: Move Architecture And Integration Tests Out Of Runtime

Create:

- `yierdis-architecture-tests`
- `yierdis-integration-tests`

Scope:

- Move source-scanning architecture tests out of `core-runtime`.
- Move tests that require multiple modules into integration tests.
- Keep focused unit tests with their owning modules.

Acceptance criteria:

- Runtime module tests cover runtime behavior, not whole-repo architecture.
- Architecture tests enforce the new dependency rules.
- Integration tests cover server/client/protocol/runtime command paths.

### Phase 4: Split Command API, Kernel, And Feature Modules

Split `yierdis-core-command` into command API, command kernel, and command
feature modules.

Scope:

- Command contracts and metadata move to `yierdis-command-api`.
- Registry, parse/dispatch lifecycle, transaction queuing, and processor logic
  move to `yierdis-command-kernel`.
- Command families move to focused modules.
- A `yierdis-command-defaults` module composes the default transport-neutral
  command set.

Acceptance criteria:

- Each command-family module depends on storage API, execution API, command API,
  and only the narrow command-kernel support it needs.
- Adding a new command family no longer requires editing one broad command
  module except for default bundle registration.
- Transaction syntax validation and replay still pass existing tests.
- Server-facing commands that require application/runtime/protocol observability
  remain outside transport-neutral command modules.

### Phase 5: Split Storage Implementations

Split concrete storage code by ownership.

Initial safe order:

1. `yierdis-storage-keyspace`
2. `yierdis-storage-values`
3. `yierdis-storage-expire`
4. `yierdis-storage-maxmemory`
5. `yierdis-storage-memory`

The split should proceed only when each module can expose narrow package or
public contracts without making storage internals broadly public.

Acceptance criteria:

- Command modules still depend only on storage API.
- Runtime-memory depends on storage-memory as the default implementation.
- Keyspace and value tests move with their owning modules.
- Maxmemory and expiration tests move to their owning modules or integration
  tests based on whether they require a full DB instance.
- Existing off-heap leak and memory accounting tests still pass.

### Phase 6: Reframe Custom Protocol V1 As An Adapter Family

Rename and reorganize protocol modules around Custom Protocol v1.

Scope:

- Protocol DTOs and limits move to `yierdis-protocol-custom-v1-api`.
- JSON and Custom Protocol v1 codecs move to
  `yierdis-protocol-custom-v1-codec`.
- `JsonLineReplyWriter` and `JsonLineReplyWriterFactory` move from server into
  codec.
- Netty-specific decoder glue moves to `yierdis-protocol-custom-v1-netty`.

Acceptance criteria:

- Custom-v1 codec may depend on execution API for reply writing.
- Custom-v1 codec does not depend on command modules or storage modules.
- Server-app no longer owns protocol-specific reply writer implementation.
- Client and bench consume protocol adapter modules directly.

### Phase 7: Reduce Server To Application Composition

Create `yierdis-server-app` as the composition root.

Scope:

- Move process entry point, startup config, server bootstrap, app-specific INFO
  provider, server-only commands, native memory availability checks, and module
  wiring into server-app.
- Move reusable Netty transport pieces to adapter modules where they are not
  server-app-specific.
- Keep server-app tests focused on process wiring, pipeline order, server-facing
  commands, close behavior, and integration.

Acceptance criteria:

- Server-app may depend on many modules, but does not own command parsing,
  storage internals, protocol codec internals, or executor algorithms.
- Existing smoke scripts still start server and run CLI commands successfully.
- Packaging still produces runnable server and client jars.

### Phase 8: Retire Old Core Module Names

Once behavior is stable under the new module families, remove or convert the old
aggregators:

- `yierdis-core-contract`
- `yierdis-core-api`
- `yierdis-core-command`
- `yierdis-core-db`
- `yierdis-core-engine`
- `yierdis-core-runtime`

Acceptance criteria:

- No production module depends on retired artifacts.
- Documentation and test names use new module families.
- Root `pom.xml` and dependency management no longer expose retired artifact
  names.

## Package Naming Guidelines

The package root may remain `yier.bubu.redis` for the first rearchitecture
cycle. A product-wide package rename is high churn and should be a separate
decision.

Recommended package families:

```text
yier.bubu.redis.bytes
yier.bubu.redis.memory.api
yier.bubu.redis.memory.ffm
yier.bubu.redis.execution
yier.bubu.redis.engine
yier.bubu.redis.storage.api
yier.bubu.redis.storage.memory
yier.bubu.redis.storage.keyspace
yier.bubu.redis.storage.values
yier.bubu.redis.storage.expire
yier.bubu.redis.storage.maxmemory
yier.bubu.redis.command.api
yier.bubu.redis.command.kernel
yier.bubu.redis.command.string
yier.bubu.redis.command.hash
yier.bubu.redis.command.list
yier.bubu.redis.command.set
yier.bubu.redis.command.zset
yier.bubu.redis.command.keyspace
yier.bubu.redis.command.transaction
yier.bubu.redis.command.connection
yier.bubu.redis.command.admin
yier.bubu.redis.runtime.api
yier.bubu.redis.runtime.memory
yier.bubu.redis.protocol.custom.v1
yier.bubu.redis.protocol.custom.v1.codec
yier.bubu.redis.protocol.custom.v1.netty
yier.bubu.redis.transport.netty
yier.bubu.redis.app.server
yier.bubu.redis.app.client
yier.bubu.redis.app.bench
```

Avoid naming packages after implementation accidents such as `coreapi`,
`corecommand`, or a transport detail unless the package truly owns that
transport adapter.

## Configuration Model

The current `yierdis-args` module should be split instead of kept as one shared
configuration bucket.

Target:

- Server runtime config belongs to `yierdis-server-app`.
- Protocol limits belong to custom-v1 protocol API.
- Storage policies such as `MaxmemoryPolicy` belong to storage API or storage
  maxmemory.
- Shared CLI parsing helpers belong to `yierdis-cli-common`.
- Bench-specific options belong to `yierdis-bench`.

This prevents shared tooling config from becoming a hidden dependency bridge
between protocol, storage, runtime, and application code.

## Documentation Updates

The rearchitecture requires documentation changes in the same phases as code
changes.

Required docs:

- Replace `docs/module-architecture.md` with a new module-family architecture.
- Update `docs/request-execution-flow.md` with the target request flow.
- Update `docs/development-navigation.md` so new contributors know which module
  owns a change.
- Update `docs/testing-and-debugging.md` with the new architecture and
  integration test modules.
- Update README module boundary bullets after each phase that changes public
  structure.
- Keep this spec as the high-level roadmap and create focused implementation
  plans per phase.

## Testing Strategy

Each migration phase needs three test layers:

- focused unit tests in the owning module;
- integration tests for request execution and server/client paths;
- architecture tests for dependency and source-ownership rules.

Recommended final verification after each major phase:

```bash
jdk25 mvn test
```

Recommended focused verification examples:

```bash
jdk25 mvn -pl yierdis-architecture-tests test
jdk25 mvn -pl yierdis-integration-tests test
jdk25 mvn -pl yierdis-command/yierdis-command-kernel test
jdk25 mvn -pl yierdis-storage/yierdis-storage-memory test
jdk25 mvn -pl yierdis-app/yierdis-server-app test
```

Exact module paths may change during implementation, but each phase plan must
name concrete verification commands before code changes start.

## Risks And Mitigations

### Risk: Too many Maven modules slow down development

Mitigation:

- Use parent aggregators by module family.
- Split only where compile-time ownership improves.
- Delay storage internals that would require poor public APIs.

### Risk: Public API surface grows during storage split

Mitigation:

- Prefer moving cohesive clusters together.
- Introduce narrow interfaces before splitting modules.
- Keep implementation packages package-private until a clean seam exists.

### Risk: Protocol codec depending on execution API feels like a layering
change

Mitigation:

- Make the rule explicit: protocol adapters may depend on execution contracts
  for request/reply adaptation, but never on command or storage modules.
- Add architecture tests enforcing this distinction.

### Risk: Existing docs and tests become stale during migration

Mitigation:

- Move architecture tests early.
- Update docs in each phase, not only at the end.
- Keep compatibility aggregators temporarily if needed, but do not leave them as
  permanent aliases.

### Risk: The rearchitecture delays feature work

Mitigation:

- Implement in behavior-preserving phases.
- Stop after any phase if the next split does not offer enough value.
- Keep command and storage semantics stable so feature branches can continue to
  merge.

## Acceptance Criteria For The Whole Rearchitecture

- The root Maven graph exposes explicit foundation, execution, storage,
  command, runtime, executor, adapter, app, integration-test, and
  architecture-test module families.
- `core-api`, `core-command`, `core-db`, and `server` no longer act as broad
  conceptual buckets.
- Command-family modules compile without concrete storage implementation
  dependencies.
- Executor-core compiles without command, storage, runtime, protocol, Netty, or
  app dependencies.
- Custom Protocol v1 owns both request codec and execution reply codec.
- Server-app is a composition root with minimal business logic.
- Architecture tests enforce the new dependency rules.
- Existing command behavior, Custom Protocol v1 behavior, server/client smoke
  behavior, maxmemory behavior, TTL behavior, and FFM memory tests pass.
- Documentation reflects the new architecture and no longer describes the old
  core-centric module graph as the target model.

## Expected Outcome

The end state should make the codebase easier to reason about at a structural
level:

- execution contracts are small and reusable;
- command behavior is split by command family and no longer hides behind one
  large command module;
- storage APIs are separate from storage implementation and memory runtime
  details;
- protocol adapters own protocol concerns instead of pushing reply encoding into
  server;
- server is clearly an application composition module;
- tests are located according to their scope rather than collected under
  runtime.

The main optimization is not shorter files or prettier directories. The main
optimization is architectural friction reduction: future command, storage,
protocol, and runtime changes should touch fewer modules, compile against
narrower contracts, and be protected by sharper architecture tests.
