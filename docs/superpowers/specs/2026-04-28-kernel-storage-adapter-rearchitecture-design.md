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

The revised target architecture is:

```text
stable APIs -> engine and command kernel
            -> storage runtime
            -> protocol/transport adapters
            -> applications
```

In practical terms, this means moving from the current "core plus server" shape
to a system organized around five explicit ownership areas:

- stable execution, storage, memory, and runtime APIs
- execution and command kernel behavior
- one in-memory storage runtime with strong internal package boundaries
- protocol, transport, and application adapters
- architecture and integration tests outside production modules

This is a roadmap spec. It is too large and too risky to implement as one
mechanical commit. Each phase must preserve behavior, keep the project
buildable, and add or update architecture guards before the next phase starts.

The important correction from an overly aggressive split is that Maven modules
should express stable compile-time boundaries. Domain organization inside a
still-coupled area should first be expressed with packages and tests. A package
or collaborator should become a Maven module only after it has a narrow public
contract and does not require making internal storage or command types public
just to satisfy the build.

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
- Split `core-command` into command API, command kernel, and a default command
  bundle; keep command families as internal packages until they need independent
  compilation.
- Split `core-db` into storage API, one storage-memory implementation module,
  and a storage testkit; organize keyspace, values, expiration, maxmemory, and
  FFM internals as packages first.
- Move cross-module architecture tests out of `core-runtime`.
- Make Custom Protocol v1 a first-class adapter family, including request
  parsing and execution reply encoding.
- Reduce `server` to an application composition module.
- Promote command-family or storage-internal packages to Maven modules only
  after they become mature seams with narrow public contracts.
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

### Approach C: Maximum Kernel, Storage, Feature Commands, And Adapters Split

Split the system by ownership:

- foundational bytes and memory APIs;
- execution API and engine kernel;
- storage API plus separate storage keyspace, values, expire, maxmemory, and
  memory implementation modules;
- command API, command kernel, and one Maven module per command family;
- runtime assembly over storage and command modules;
- protocol and transport adapters;
- applications and cross-module tests.

This is architecturally explicit, but it creates too many modules before the
code has enough stable seams. In the current storage code, keyspace, values,
expiration, maxmemory, memory accounting, and DB facade behavior still cooperate
through package-private implementation details. Splitting them immediately would
force premature public APIs. Command families have a similar risk: they share
registry, parser, support, transaction, and reply mechanics.

Rejected as the immediate target. It remains a possible long-term direction for
specific seams after those seams are proven inside fewer modules.

### Approach D: Stable APIs, Kernel, Storage Runtime, Adapters, And Apps

Split only the boundaries that need compile-time isolation now:

- stable execution, storage, memory, and runtime APIs;
- engine and command kernel;
- one command-defaults module with command-family packages;
- one storage-memory module with storage-internal packages;
- Custom Protocol v1 wire/execution/Netty adapter modules;
- server, client, bench, architecture-test, and integration-test modules.

This avoids module explosion while still fixing the broad ownership problem.
It gives the project sharper API boundaries first, then allows later promotion
of command-family or storage-internal packages into Maven modules only when
there is a narrow public seam.

Chosen.

## Architectural Decision

Adopt Approach D.

The target architecture should make these rules obvious from module names and
dependencies:

```text
Command kernel/defaults depend on execution-api and storage-api.
Storage-memory depends on storage-api and memory APIs.
Runtime depends on storage-memory and runtime-api.
Executor depends on execution-api only.
Custom-v1 execution adapters depend on wire APIs and execution-api.
Applications depend on adapters, runtime, command defaults, and executor.
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
└─ yierdis-storage-testkit
```

Responsibilities:

- `yierdis-storage-api`: command-facing DB capabilities:
  `DbEngine`, `DbReads`, `DbWrites`, `StringReadOps`, `StringWriteOps`,
  hash/list/set/zset/HLL ops, keyspace ops, TTL ops, memory ops,
  `DbEngineFactory`, and storage-facing result types.
- `yierdis-storage-memory`: concrete in-memory DB facade and orchestration:
  `YierdisDb`, `YierdisDbEngineFactory`, component factory, lifecycle, and
  read/write facade wiring. Internally it should use packages for keyspace,
  values, expiration, maxmemory, memory accounting, and FFM-backed structures.
- `yierdis-storage-testkit`: reusable storage fixtures and conformance tests
  consumed by integration and command tests.

Initial internal package structure for `yierdis-storage-memory`:

```text
yier.bubu.redis.storage.memory
├─ keyspace
├─ values
├─ expire
├─ maxmemory
├─ accounting
└─ ffm
```

These packages are not separate Maven modules at first. They are candidate
seams. A package can be promoted later only if it exposes a narrow contract and
does not require broadening storage internals just for compile visibility.

Current source mapping:

- Most of `yierdis-core-api/ops` maps to `yierdis-storage-api`.
- `yierdis-core-db` maps primarily to `yierdis-storage-memory` first, with
  internal package reorganization before any further module split.
- `yierdis-core-runtime` tests under storage packages should move to
  `yierdis-storage-testkit`, `yierdis-integration-tests`, or
  `yierdis-architecture-tests` depending on scope.

The storage split should deliberately stop at `storage-memory` until the
internal package boundaries prove stable. If moving a class would force many
package-private collaborators to become public, keep that cluster in
`storage-memory`.

### Commands

```text
yierdis-command
├─ yierdis-command-api
├─ yierdis-command-kernel
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
- `yierdis-command-defaults`: the default transport-neutral command bundle used
  by embedded runtime and server-app composition. It contains command-family
  packages such as string, hash, list, set, zset, keyspace, transaction,
  connection, and admin.

Initial internal package structure for `yierdis-command-defaults`:

```text
yier.bubu.redis.command.defaults
├─ string
├─ hash
├─ list
├─ set
├─ zset
├─ keyspace
├─ transaction
├─ connection
└─ admin
```

Command-family packages are not separate Maven modules at first. They can be
promoted later only if a family needs independent enablement, independent
dependencies, or independent conformance testing.

Current source mapping:

- `yierdis-core-command` splits across these modules.
- Server-facing commands that require build info, protocol-specific fields, or
  Netty/executor observability should remain application or adapter commands,
  not move into transport-neutral command modules.

Command-family packages must not depend on concrete storage implementations.
The `yierdis-command-defaults` module may depend on `yierdis-storage-api`,
`yierdis-execution-api`, `yierdis-command-api`, and `yierdis-command-kernel`.

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
├─ yierdis-custom-v1-wire
├─ yierdis-custom-v1-execution-adapter
├─ yierdis-custom-v1-netty
├─ yierdis-transport-netty
└─ yierdis-cli-common
```

Responsibilities:

- `yierdis-custom-v1-wire`: Custom Protocol v1 wire DTOs, limits, version
  constants, JSON parser/writer, request frame encoder, request payload parser,
  reply parser, reply inspector, and protocol-side reply value tooling.
  Application build information belongs to `yierdis-server-app`, not the wire
  API.
- `yierdis-custom-v1-execution-adapter`: adapters between Custom Protocol v1
  and execution contracts. This includes protocol DTO to `ExecutionRequest`
  conversion, `JsonLineReplyWriter`, and `JsonLineReplyWriterFactory`.
- `yierdis-custom-v1-netty`: Netty decoder/encoder glue for Custom Protocol v1.
- `yierdis-transport-netty`: transport-level connection integration that is not
  specific to Custom Protocol v1.
- `yierdis-cli-common`: shared CLI argument parsing primitives that are not
  server runtime config.

Important decision:

`yierdis-custom-v1-wire` must not depend on execution contracts.
`yierdis-custom-v1-execution-adapter` may depend on `yierdis-execution-api` for
`ExecutionRequest`, `ReplyWriter`, and `ReplyWriterFactory`.

This replaces the current awkward rule that protocol-codec must not depend on
core-contract while server owns `JsonLineReplyWriter`. The new boundary is:

- wire code knows only the wire protocol;
- execution adapters know wire DTOs and execution contracts;
- Netty adapters know wire codecs and Netty;
- no protocol adapter knows command modules, storage APIs, storage
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
  -> custom-v1 execution adapter converts to ExecutionRequest
  -> executor-core schedules Session + ExecutionRequest + ReplyWriter
  -> yierdis-engine executes
  -> command-kernel lookup/parse/dispatch
  -> command-defaults handler calls storage-api
  -> storage-memory mutates or reads data
  -> custom-v1 execution adapter ReplyWriter encodes NDJSON reply
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
- `yierdis-command-defaults` imports no concrete storage implementation
  packages.
- `yierdis-storage-api` imports no command, protocol, application, or Netty
  packages.
- `yierdis-storage-memory` imports no command implementation packages and no
  protocol packages. Internal package boundaries should prevent unrelated
  storage concerns from drifting back into the DB facade.
- `yierdis-executor-core` imports only execution contracts and executor-owned
  support types.
- `yierdis-runtime-api` imports no storage implementation, protocol, command
  implementation, application, or Netty packages.
- `yierdis-runtime-memory` may depend on storage implementations and runtime
  API, but must not own command parser registration.
- `yierdis-custom-v1-wire` imports no execution, command, storage, runtime,
  application, or Netty packages.
- `yierdis-custom-v1-execution-adapter` may import execution contracts and
  custom-v1 wire packages, but must not import command or storage packages.
- `yierdis-custom-v1-netty` may import Netty and custom-v1 wire/adapter
  packages, but must not import command or storage packages.
- `yierdis-server-app` may compose all production modules, but source guards
  should prevent it from constructing command contexts, parsing command syntax,
  or directly using storage internals.

## Migration Phases

### Phase 0: Move Architecture And Integration Tests First

Create:

- `yierdis-architecture-tests`
- `yierdis-integration-tests`

Scope:

- Move source-scanning architecture tests out of `core-runtime`.
- Move tests that require multiple modules into integration tests.
- Keep focused unit tests with their owning modules.
- Make the architecture-test module understand both the current module names
  and the target names during migration.

Acceptance criteria:

- Runtime module tests cover runtime behavior, not whole-repo architecture.
- Architecture tests enforce the current dependency rules before other modules
  move.
- Integration tests cover server/client/protocol/runtime command paths.
- Later phases can update architecture guards without leaving `core-runtime` as
  the global guard-test owner.

### Phase 1: Extract Stable API Modules

Create the stable API modules before moving implementation-heavy code:

- `yierdis-execution-api`
- `yierdis-storage-api`
- `yierdis-memory-api`
- `yierdis-runtime-api`

Scope:

- Move execution contracts out of `yierdis-core-contract`.
- Move command-facing DB operation contracts out of `yierdis-core-api`.
- Move off-heap contracts out of `yierdis-core-api`.
- Move runtime change tracking, runtime access, and observability contracts out
  of broad API ownership.
- Classify maxmemory contracts explicitly as storage API, storage-memory
  implementation, or runtime API.

Acceptance criteria:

- Existing engine, executor, command, and server modules compile against
  `yierdis-execution-api`.
- Existing command and storage modules compile against `yierdis-storage-api`.
- No protocol DTO moves into execution API.
- Command modules depend on storage API but not memory API unless a concrete
  command genuinely requires a memory contract.
- Storage implementation modules depend on memory API and storage API.
- Runtime modules depend on runtime API and storage API.
- Architecture guards assert that each API module stays narrow.

### Phase 2: Reframe Custom Protocol V1 As Wire And Execution Adapters

Rename and reorganize protocol modules around Custom Protocol v1:

- `yierdis-custom-v1-wire`
- `yierdis-custom-v1-execution-adapter`
- `yierdis-custom-v1-netty`

Scope:

- Protocol DTOs, limits, JSON parser/writer, request encoder, reply parser, and
  protocol-side reply model move to wire.
- Protocol DTO to `ExecutionRequest` conversion moves from server into the
  execution adapter.
- `JsonLineReplyWriter` and `JsonLineReplyWriterFactory` move from server into
  the execution adapter.
- Netty-specific decoder glue moves to custom-v1-netty.

Acceptance criteria:

- Wire imports no execution, command, storage, runtime, application, or Netty
  packages.
- Execution adapter may depend on wire and execution API.
- Execution adapter does not depend on command or storage modules.
- Server-app no longer owns protocol-specific reply writer implementation.
- Client and bench consume wire/netty modules directly.

### Phase 3: Reduce Server To Application Composition

Create `yierdis-server-app` as the composition root.

Scope:

- Move process entry point, startup config, server bootstrap, app-specific INFO
  provider, server-only commands, native memory availability checks, and module
  wiring into server-app.
- Move reusable Netty transport pieces to adapter modules where they are not
  server-app-specific.
- Keep server-app tests focused on process wiring, pipeline order,
  server-facing commands, close behavior, packaging, and integration.

Acceptance criteria:

- Server-app may depend on many modules, but does not own command parsing,
  storage internals, protocol wire internals, or executor algorithms.
- Existing smoke scripts still start server and run CLI commands successfully.
- Packaging still produces runnable server and client jars.

### Phase 4: Split Command API, Kernel, And Defaults

Split `yierdis-core-command` into command API, command kernel, and one default
command bundle.

Scope:

- Command contracts and metadata move to `yierdis-command-api`.
- Registry, parse/dispatch lifecycle, transaction queuing, and processor logic
  move to `yierdis-command-kernel`.
- Command families move to packages inside `yierdis-command-defaults`.
- `yierdis-command-defaults` composes the default transport-neutral command set.

Acceptance criteria:

- `yierdis-command-defaults` depends on storage API, execution API, command API,
  and command kernel, but not concrete storage implementations.
- Adding a new command family usually means adding a package and updating the
  default bundle registration, not editing registry or processor internals.
- Transaction syntax validation and replay still pass existing tests.
- Server-facing commands that require application/runtime/protocol observability
  remain outside transport-neutral command modules.

### Phase 5: Reorganize Storage-Memory Internals

Move the concrete storage implementation into `yierdis-storage-memory`, then
organize internals by package instead of splitting more Maven modules.

Initial package targets:

1. `storage.memory.keyspace`
2. `storage.memory.values`
3. `storage.memory.expire`
4. `storage.memory.maxmemory`
5. `storage.memory.accounting`
6. `storage.memory.ffm`

This phase should reduce `YierdisDb` and make the internal ownership model
clear without forcing internal collaborators into public Maven-module APIs.

Acceptance criteria:

- Command modules still depend only on storage API.
- Runtime-memory depends on storage-memory as the default implementation.
- Keyspace, value, maxmemory, expiration, and memory accounting tests move with
  their owning packages or to storage testkit/integration tests based on scope.
- Storage-memory architecture guards prevent unrelated responsibilities from
  drifting back into `YierdisDb`.
- Existing off-heap leak and memory accounting tests still pass.

### Phase 6: Promote Mature Seams Only

After the package-level reorganization proves stable, evaluate whether any
internal seam deserves its own Maven module.

Candidate promotions:

- command-family modules for independently enabled command sets;
- storage keyspace module if it exposes a narrow reusable keyspace contract;
- storage values module if value encodings can be tested and reused separately;
- storage maxmemory module if it becomes a policy engine independent from
  `YierdisDb`;
- transport-netty module if it is shared by server and client beyond
  Custom Protocol v1.

Acceptance criteria:

- Promotion reduces coupling or enables independent tests/dependencies.
- Promotion does not require making broad internal implementation types public.
- Architecture tests capture the new dependency rule before or with the
  promotion.
- If a candidate fails these checks, it remains an internal package.

### Phase 7: Retire Old Core Module Names

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
yier.bubu.redis.storage.memory.keyspace
yier.bubu.redis.storage.memory.values
yier.bubu.redis.storage.memory.expire
yier.bubu.redis.storage.memory.maxmemory
yier.bubu.redis.storage.memory.accounting
yier.bubu.redis.storage.memory.ffm
yier.bubu.redis.command.api
yier.bubu.redis.command.kernel
yier.bubu.redis.command.defaults
yier.bubu.redis.command.defaults.string
yier.bubu.redis.command.defaults.hash
yier.bubu.redis.command.defaults.list
yier.bubu.redis.command.defaults.set
yier.bubu.redis.command.defaults.zset
yier.bubu.redis.command.defaults.keyspace
yier.bubu.redis.command.defaults.transaction
yier.bubu.redis.command.defaults.connection
yier.bubu.redis.command.defaults.admin
yier.bubu.redis.runtime.api
yier.bubu.redis.runtime.memory
yier.bubu.redis.custom.v1.wire
yier.bubu.redis.custom.v1.execution
yier.bubu.redis.custom.v1.netty
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
- Protocol limits belong to `yierdis-custom-v1-wire`.
- Storage policies such as `MaxmemoryPolicy` belong either to storage API or to
  storage-memory internals, depending on whether command/runtime code must see
  them.
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
- Keep command families and storage internals as packages until they pass a
  module-promotion gate.
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
  architecture-test module families without creating one Maven module for every
  command family or storage-internal concern up front.
- `core-api`, `core-command`, `core-db`, and `server` no longer act as broad
  conceptual buckets.
- Command-defaults and its command-family packages compile without concrete
  storage implementation dependencies.
- Executor-core compiles without command, storage, runtime, protocol, Netty, or
  app dependencies.
- Custom Protocol v1 wire and execution adapter modules own request codec and
  execution reply codec.
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
- command behavior is organized by command family inside a default command
  bundle and no longer hides behind one large command module;
- storage APIs are separate from storage implementation and memory runtime
  details;
- storage implementation internals are organized by package before any
  premature Maven split;
- protocol adapters own protocol concerns instead of pushing reply encoding into
  server;
- server is clearly an application composition module;
- tests are located according to their scope rather than collected under
  runtime.

The main optimization is not shorter files or prettier directories. The main
optimization is architectural friction reduction: future command, storage,
protocol, and runtime changes should touch fewer modules, compile against
narrower contracts, and be protected by sharper architecture tests.
