# Engine-Centric Architecture Design

> Module ownership note: source paths and Maven module names in this older
> spec are superseded by
> `2026-04-28-kernel-storage-adapter-rearchitecture-design.md`. The engine
> boundary guidance still applies; current code uses `yierdis-engine` and
> `yierdis-runtime-embedded`.

## Summary

This design makes `YierdisEngine` the single command execution kernel for
Yierdis.

The current project already has meaningful high-level boundaries, but the
request and data flow still feels hard to follow because execution semantics are
spread across several places:

- `yierdis-server` wires protocol, runtime, command processor, executor, and
  maintenance.
- `yierdis-executor-core` owns queueing, owner-thread dispatch, backpressure, and
  per-connection execution context.
- `yierdis-core-command` owns registry lookup, command parsing, transaction
  behavior, error mapping, DB routing, and change tracking.
- `yierdis-core-runtime` owns DB instance lifecycle and maintenance seams.
- `yierdis-core-db` owns storage, TTL, maxmemory, key identity, and memory
  accounting.

The target architecture does not start by moving many Maven modules. It starts
by creating one unavoidable execution kernel:

```text
YierdisEngine.execute(session, request, replyWriter)
```

After this design is implemented, all command requests must pass through
`YierdisEngine`. Transport, executor, runtime, and storage become supporting
roles around that kernel instead of sharing command execution ownership.

## Problem Statement

Yierdis currently has a macro architecture, but the micro data flow is still a
transition-state graph rather than one official path.

The original symptoms were:

- Request objects had multiple forms: protocol DTOs, `ExecutionRequest`, and a
  deprecated `Command` compatibility surface.
- Commands were partly migrated to typed `CommandSpec<T>` parsing, but legacy
  handler registration still allowed handlers to parse and emit syntax errors
  directly.
- Session and transaction ownership sat in executor-facing context, which made
  the executor look like part of command semantics rather than pure scheduling
  infrastructure.
- DB-facing data uses several representations: `byte[]`, `BytesView`,
  `BytesSlice`, and internal `KeyHandle`.
- TTL and maxmemory pressure paths materialized off-heap key identity into heap
  `byte[]` in important paths.
- Server bootstrap is a composition root, but it still reads as the place where
  command execution, runtime lifecycle, maintenance, and observability are all
  assembled by hand.

These issues do not mean the existing boundaries are wrong. They mean too many
intermediate ownership seams remain visible to readers and future changes.

## Goals

- Introduce `YierdisEngine` as the single owner of command execution semantics.
- Make the official request flow linear and easy to document.
- Keep transport, executor, runtime, and storage responsibilities narrow.
- Move business session state out of executor connection context and into
  engine-owned session state.
- Make `CommandSpec<T>` the only command registration and parsing model.
- Remove the deprecated `Command` compatibility path after all producers use
  `ExecutionRequest`.
- Define storage-facing data representation rules:
  - command input uses `BytesView` and `BytesSlice` where possible;
  - DB internal key identity uses `KeyHandle`;
  - heap `byte[]` is allowed only at protocol boundaries, small compatibility
    points, and explicit materialization APIs.
- Preserve existing command behavior unless a later implementation plan
  explicitly changes tests and documentation.

## Non-Goals

- No Redis RESP compatibility.
- No persistence, replication, clustering, Lua, ACL, TLS, or Pub/Sub.
- No one-shot rewrite of all storage data structures.
- No immediate Maven module collapse.
- No replacement of Netty.
- No removal of existing architecture guard tests unless replaced by stricter
  guard tests.
- No public protocol redesign.

## Current Implementation Status

- `ProtocolCommandAdapter` already adapts protocol DTOs into
  `ExecutionRequest`.
- `CommandExecutor` accepts a transport-neutral `CommandExecutionEngine` shaped
  as `Session + ExecutionRequest + ReplyWriter`.
- `YierdisEngine` is the server/executor command execution entry point and owns
  command-context construction.
- `EngineSession` carries selected DB, transaction state, client metadata, and
  authentication state.
- `EngineSession` can expose read-only `ConnectionStatsView` for INFO/STATS,
  while the actual pending/backpressure counters remain owned by
  `ExecutionConnectionContext`.
- `ExecutionConnectionContext` owns executor-local state only: pending counts,
  pending bytes, closing flags, input-disabled state, queue state, and stats.
- `CommandSpec<T>` is the production command registration and parsing contract.
- The production `Command` compatibility execution path is removed; production
  execution uses `ExecutionRequest`.
- `YierdisExpireIndex` and keyspace implementations expose handle-based methods
  such as `randomKeyHandle()`.
- `MaxmemoryCandidate` stores `KeyHandle keyHandle`, avoiding mandatory heap key
  materialization in maxmemory victim selection.
- Existing architecture specs already cover command contract unification,
  executor-core extraction, maxmemory policy unification, and `YierdisDb`
  decomposition. This design sits above those specs and defines the target
  ownership model.

## Considered Approaches

### Approach A: Continue Local Boundary Cleanup

Keep the current architecture and finish the local cleanups:

- migrate all commands to `CommandSpec<T>`;
- delete legacy `Command`;
- introduce `KeyHandle` for maxmemory and TTL paths;
- simplify server bootstrap.

This is low risk and should still happen, but by itself it does not give the
project one obvious execution center.

### Approach B: Add A General Application Kernel

Introduce an application-kernel layer that owns request execution, command
parsing, DB routing, transactions, error mapping, and change tracking.

This clarifies responsibilities, but the term "application kernel" is broad. It
could become a new vague layer unless its API is narrow and enforced.

### Approach C: Collapse Maven Modules

Merge several core modules into fewer modules such as `yierdis-kernel`,
`yierdis-storage`, `yierdis-protocol`, and `yierdis-server`.

This shortens the dependency graph, but it does not automatically simplify the
data flow. If done first, it risks moving the current confusion into larger
modules.

### Approach D: Engine-Centric Architecture

Introduce `YierdisEngine` as the one command execution kernel. Transport,
executor, runtime, and storage adapt to it.

This gives the project a simple rule:

```text
If a command can affect DB state or produce a command reply, it must enter
through YierdisEngine.
```

Chosen.

### Approach E: Single-Thread Reactor Kernel

Combine runtime, executor, and engine into one Redis-like single-thread reactor.

This would make the request path shorter, but it discards useful executor-core
separation and requires larger backpressure and lifecycle rewrites. It is too
large for the next architectural step.

## Architectural Decision

Adopt Approach D.

`YierdisEngine` becomes the system's command execution authority. Its public
surface should be small:

```java
public interface YierdisEngine extends AutoCloseable {
    void execute(Session session, ExecutionRequest request, ReplyWriter out);

    void maintenanceTick();

    @Override
    void close();
}
```

The exact Java type shape can change during implementation, but the ownership
rule must not change:

- `YierdisEngine` owns command semantics.
- `CommandExecutor` owns scheduling and backpressure only.
- `YierdisServer` owns transport adaptation only.
- `YierdisInstance` owns DB resources and lifecycle only.
- `YierdisDb` and storage collaborators own data structures only.

## Target Layers

### Transport Adapter

Module area: `yierdis-server`, `yierdis-protocol-*`, `yierdis-bytes-netty`.

Responsibilities:

- Decode bytes into protocol DTOs.
- Adapt protocol DTOs into `ExecutionRequest`.
- Own Netty channel lifecycle and wire flush.
- Create or look up the connection's `EngineSession`.
- Submit work to the executor.
- Encode replies through the configured `ReplyWriterFactory`.

Must not:

- parse commands;
- inspect command metadata;
- own transactions;
- route DB indexes;
- enforce command errors except transport/protocol errors.

### Executor

Module area: `yierdis-executor-core`.

Responsibilities:

- Queue accepted requests.
- Enforce backlog byte and task budgets.
- Dispatch accepted work on the owner thread.
- Manage pending counts, pending bytes, closing state, and input recovery.
- Batch reply flushing through the transport adapter.

Must not:

- own selected DB index;
- own transaction queues;
- know command names;
- know command modules;
- know DB routing.

### Engine

Module area: `yierdis-core-engine`.

Responsibilities:

- Own `EngineSession`.
- Own command registry construction.
- Own `CommandSpec<T>` lookup, parsing, and typed handler dispatch.
- Own transaction lifecycle and replay semantics.
- Own DB routing for selected logical DB.
- Own command error mapping.
- Own change tracking and change event emission.
- Own maintenance entry point delegation to runtime/storage.

Must not:

- own Netty details;
- own queueing or backpressure;
- own protocol DTOs;
- own storage internals beyond DB capability interfaces.

### Runtime

Module area: `yierdis-core-runtime`.

Responsibilities:

- Create and close DB engines.
- Bind DBs to the owner thread.
- Provide runtime access for maintenance.
- Provide instance-level observability.
- Coordinate global maxmemory.

Must not:

- assemble command modules;
- own command registry;
- own transaction/session state;
- inspect protocol DTOs.

### Storage

Module area: `yierdis-core-api`, `yierdis-core-db`, `yierdis-memory-*`.

Responsibilities:

- Own keyspace, value encodings, TTL, maxmemory, memory accounting, and native
  memory usage.
- Expose command-facing DB capability interfaces.
- Use `KeyHandle` for internal key identity on hot paths.

Must not:

- know command names except where API method names intentionally reflect command
  families;
- emit protocol reply models;
- own command parser errors;
- force heap key materialization in TTL/maxmemory pressure paths.

## Official Data Flow

Normal request:

```text
Netty ByteBuf
  -> CustomRequestDecoder
  -> protocol request DTO
  -> ProtocolCommandAdapter
  -> ExecutionRequest
  -> CommandExecutor.trySubmit(connection, request)
  -> owner thread
  -> YierdisEngine.execute(session, request, replyWriter)
  -> YierdisFastCommandProcessor
  -> CommandSpec<T>.parse(...)
  -> typed command handler
  -> DbEngine capability interface
  -> storage
  -> ReplyWriter
  -> transport flush
```

Transaction queueing:

```text
YierdisEngine.execute(...)
  -> lookup CommandSpec
  -> parse request before queueing
  -> EngineSession.transaction.enqueue(snapshot)
  -> QUEUED reply
```

Transaction replay:

```text
EXEC
  -> EngineSession.transaction.drain()
  -> replay through YierdisEngine internal execution path
  -> same parse, error mapping, DB routing, and change tracking rules
```

Maintenance:

```text
timer
  -> CommandExecutor.executeMaintenance(...)
  -> owner thread
  -> YierdisEngine.maintenanceTick()
  -> runtime/storage maintenance
```

## Core Abstractions

### `YierdisEngine`

The single execution kernel. It should be the only production object that wires
together:

- command registry;
- command support;
- DB router;
- server/runtime info provider;
- change sink;
- maintenance access.

### `EngineSession`

Business session state for one logical connection.

Owns:

- selected DB index;
- transaction state;
- optional client name or future client flags;
- future command execution metadata that is not transport-level backpressure.

Does not own:

- pending counts;
- pending bytes;
- channel closing state;
- input-disabled state.

### `ExecutionConnectionContext`

Executor/transport scheduling state only.

Owns:

- pending command count;
- pending retained bytes;
- closing flag;
- input-disabled-by-executor flag;
- executor-local stats;
- fair scheduling queue state.

It must not reference or own `EngineSession`; the concrete connection object
owns both `EngineSession` and `ExecutionConnectionContext` as separate state
slices.

### `CommandSpec<T>`

The only command registration contract.

Contains:

- descriptor;
- parser;
- typed handler;
- MULTI policy.

Legacy `CommandModule.Handler` and legacy handler-only registration are removed
from production command registration.

### `Storage Key Identity`

Rules:

- Public command input may arrive as `ExecutionRequest`.
- Command-to-storage input should prefer `BytesView` or `BytesSlice` for
  non-owning reads and writes.
- Storage internal identity should prefer `KeyHandle`.
- `byte[]` remains valid for explicit materialization, tests, client-facing
  results, and small command argument snapshots.

## Migration Phases

### Phase 1: Introduce Engine Facade

Create `YierdisEngine` as a wrapper around the existing
`YierdisFastCommandProcessor`, runtime access, observability, and maintenance
objects.

Acceptance criteria:

- Server bootstrap constructs one engine.
- Executor receives `engine::execute`.
- Maintenance calls `engine.maintenanceTick()`.
- Existing behavior remains unchanged.

Status in this branch: implemented.

### Phase 2: Move Engine Session Ownership

Move selected DB index and transaction state from executor-owned session types
to `EngineSession`.

Acceptance criteria:

- Executor tests can run without command/DB session semantics.
- Command tests can construct `EngineSession` without Netty or executor context.
- `ExecutionConnectionContext` contains only scheduling and transport state.

Status in this branch: implemented.

### Phase 3: Complete CommandSpec Migration

Convert all command modules and server-local commands to typed `CommandSpec<T>`.

Acceptance criteria:

- No production command registration uses legacy `CommandModule.Handler`.
- Command parse errors are produced by parsers, not handler-side ad hoc checks,
  except for explicitly documented runtime validation.
- Transaction queueing validates through the same parser used by direct
  execution.

Status in this branch: implemented.

### Phase 4: Remove Legacy Request Compatibility

Delete the deprecated `Command` execution path and move any remaining zero-copy
capability into `ExecutionRequest`.

Acceptance criteria:

- `YierdisFastCommandProcessor.execute(Command, ...)` no longer exists.
- `CommandSupport` no longer checks `request instanceof Command`.
- Protocol-to-execution adaptation produces only `ExecutionRequest`.

Status in this branch: implemented.

### Phase 5: Normalize Storage Data Flow

Move hot TTL and maxmemory paths to `KeyHandle` or opaque key references.
Gradually convert DB capability interfaces from `byte[]` to `BytesView` and
`BytesSlice` where useful.

Acceptance criteria:

- `MaxmemoryCandidate` no longer stores `byte[] key`.
- TTL cleanup uses `randomKeyHandle()` for off-heap indexes.
- `YierdisFfmKeyspace.randomKey()` and `forEach(...)` are not used on hot
  pressure paths that can use handles.

Status in this branch: implemented.

### Phase 6: Strengthen Architecture Guards And Docs

Update docs and add guard tests for the new engine-centered ownership model.

Acceptance criteria:

- Request flow docs show `YierdisEngine` as the command execution center.
- Architecture guard tests prevent command parsing from moving into server,
  executor, runtime, protocol, or storage.
- Guard tests prevent executor session state from owning transaction semantics.

Status in this branch: implemented by updating docs and architecture boundary
guards for the engine-centered flow.

## Testing Strategy

- Keep all existing command behavior tests.
- Add engine-level unit tests that execute requests without Netty.
- Keep executor-core tests transport-neutral and command-semantic-free.
- Keep server tests focused on protocol, Netty integration, connection close
  behavior, and backpressure adaptation.
- Add architecture guard tests for:
  - all command execution goes through `YierdisEngine`;
  - executor does not import command modules or DB implementations;
  - server does not instantiate `YierdisFastCommandProcessor` directly after the
    engine facade is introduced;
  - transaction state is not owned by executor-local scheduling context;
  - storage pressure paths avoid heap key materialization where handle APIs
    exist.

## Risks And Mitigations

- Risk: `YierdisEngine` becomes a new god object.
  Mitigation: keep it as an orchestrator. Command modules, DB ops, executor, and
  runtime collaborators remain separate.

- Risk: Moving session state breaks transaction behavior.
  Mitigation: migrate behind an adapter first, then move fields after tests prove
  direct and replay execution still share the same path.

- Risk: Refactoring key identity changes storage semantics.
  Mitigation: start with TTL and maxmemory hot paths only; keep command-visible
  key materialization APIs unchanged.

- Risk: Module movement causes large churn.
  Mitigation: start with packages and facades. Create or move Maven modules only
  after the execution path is stable.

## Final Target

The final architecture should be explainable as:

```text
protocol decodes requests;
executor schedules requests;
engine executes requests;
runtime owns DB lifecycle;
storage owns data;
transport writes replies.
```

The most important invariant is:

```text
No command request bypasses YierdisEngine.
```
