# Backend Architecture Contract Rewrite Design

## Status

Approved in conversation on 2026-07-21.

## Context

Yierdis already has a deliberate Maven module graph, a transport-neutral command
executor, explicit inbound and outbound memory budgets, ordered reply slots, and
Redis-style single-threaded DB ownership. The remaining architecture problems do
not come from an absence of modules. They come from contracts that are weaker
than the invariants their implementations require, and from state machines whose
ownership is split across modules.

The concrete failures and risks motivating this rewrite are:

- reply control admission and reply-lease expansion share one waiter per
  connection, so a valid expansion wait can be rejected and its admitted reply
  slot silently cancelled;
- transport code maintains a second pending-command queue outside the executor,
  while transport and executor overwrite the same binary read-pause flag;
- command arity is duplicated in descriptor metadata and parser rules, with the
  current `AUTH` definitions already disagreeing inside `MULTI`;
- command handlers manually order reply reservation, mutation, output, and
  source ownership, while the executor may rerun a handler that has not emitted
  bytes;
- `Session`, `CommandExecutor`, `RuntimeDbEngine`, `TransactionState`, and
  `NativeAllocator` publish contracts weaker than the production behavior that
  depends on them;
- native handles identify only a local slot and generation, not the allocator
  that owns them;
- the DB API and implementation calculate RESP wire byte counts;
- `server-main` implements server commands, reply rendering, observability
  aggregation, configuration conversion, and final composition;
- architecture policy files duplicate the real Maven dependency graph without
  checking that the two agree.

The user explicitly approved breaking public APIs and selected a coordinated
contract rewrite instead of compatibility-first migration. The server keeps one
owner thread for the entire instance. Multi-core command sharding is outside
this design.

## Goals

- Give every critical state transition and invariant one authoritative owner.
- Make invalid session, executor, storage, memory, and transaction compositions
  unrepresentable where practical and fail at startup otherwise.
- Guarantee that every admitted request either produces exactly one ordered
  reply or explicitly closes its connection.
- Guarantee that a mutating command is never automatically executed more than
  once because reply capacity was unavailable.
- Make command syntax the sole source for parsing, transaction preflight, and
  Redis command metadata.
- Make native handles allocator-scoped and reject cross-allocator access.
- Remove RESP wire-size knowledge from storage modules.
- Return `server-main` to composition and adapter wiring.
- Make Maven POMs the source of truth for module dependencies.
- Preserve Redis-style whole-instance single-threaded command and maintenance
  semantics.

## Non-Goals

- Do not retain deprecated adapters for the deleted APIs.
- Do not introduce multi-owner, per-DB, or per-key execution.
- Do not change Redis command behavior except where current behavior is wrong,
  such as bare `AUTH` transaction preflight.
- Do not add a new wire protocol or remove RESP2/RESP3 support.
- Do not replace the FFM implementation with another production backend in this
  change; make the boundary real and testable so that another backend can be
  supplied later.
- Do not add distributed transactions, persistence, replication, or clustering.

## Design Principles

1. A critical invariant has one owner and one representation.
2. Required behavior is abstract, not a default no-op.
3. Optional behavior is an explicit capability that startup validates against
   enabled configuration.
4. Admission, waiting, execution, cancellation, and cleanup transfer ownership
   explicitly.
5. A mutation starts only after its reply envelope is reserved.
6. Protocol adapters calculate wire representation; storage reports content.
7. The owner-thread decision is visible in types instead of being an unwritten
   property of a supplied `Executor`.
8. No compatibility layer keeps both the old and new architecture alive.

## Target Module Ownership

### `yierdis-server-api`

Owns semantic connection and reply contracts. It defines a complete
`CommandSession`, connection-scoped capabilities, semantic reply shapes, and
reply plans. It does not define transport capacity waiters, Netty types, storage
types, or an empty marker session.

### `yierdis-command-api` and `yierdis-command-core`

Own command identity, syntax, Redis metadata, transaction policy, parsing,
preparation, and execution orchestration. A command definition has one syntax
object from which parser validation and `COMMAND INFO` arity are derived.

### `yierdis-db-api`

Owns semantic DB operations, result sources, runtime lifecycle, factory
configuration, and explicit runtime capability interfaces. It contains no RESP
terminology or wire-size formula.

### `yierdis-memory-api`

Owns the complete stable-memory backend contract required by `db-memory`:
stable objects, regions, allocation scopes, growth estimation, accounting, and
allocator-scoped handles. It exposes no FFM implementation class.

### `yierdis-memory-ffm`

Implements the stable-memory backend using JDK 25 FFM. Local raw references and
FFM regions remain implementation details.

### `yierdis-networking-resp` and `yierdis-networking-netty`

Own RESP wire sizing and encoding, decoding, ingress state, Netty adaptation,
and transport callbacks. They do not own command semantics or DB result
measurement.

### `yierdis-server-executor`

Owns the single serial owner queue, executor admission, capacity accounting,
task lifecycle, and execution retries before mutation. It depends on a
`SerialOwnerExecutor`, not an arbitrary `Executor`.

### `yierdis-server-runtime` and `yierdis-server-main`

Runtime owns instance lifecycle and capability validation. `server-main` owns
only CLI conversion, concrete adapter creation, and final wiring. Server command
implementations and reply formatting do not live in `server-main`.

## Session And Owner Contracts

Delete the empty `Session` interface and `CommandSessionCapabilities.from(...)`.
Introduce a complete session type:

```java
public interface CommandSession extends
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
}
```

`ExecutionConnection.session()`, `CommandExecutionEngine.execute(...)`, and
`YierdisEngine.execute(...)` accept `CommandSession`. A transport adapter cannot
compile unless it supplies the full contract.

`TransactionState` makes `aborted()`, `markAborted()`, bounded enqueue,
read-only planning, draining, and cleanup required methods. There are no
defaults that silently ignore transaction abort or ownership.

Introduce `SerialOwnerExecutor` in executor-core. Its contract guarantees that:

- every submitted action runs on the same physical thread;
- actions never overlap;
- `start(...)`, command drain, maintenance, writable recovery, and shutdown use
  the same serial context;
- the implementation can assert whether the caller is on the owner thread.

The production implementation adapts one Netty `EventExecutor`. Tests use a
deterministic serial implementation. `CommandExecutor` no longer accepts a
general `java.util.concurrent.Executor`.

DB and allocator ownership use one storage-owned atomic guard created during DB
composition. Binding that guard on the serial owner thread binds the entire DB
resource graph at once. The current separate DB check-then-set and allocator CAS
sequence is removed.

## Unified Command Definition

Replace the separate descriptor arity and parser arity with one
`CommandSyntax`:

```java
public record CommandSyntax(
        String nameUpper,
        CommandArity arity,
        CommandKeySpec keys,
        TransactionPolicy transactionPolicy
) {
}
```

`CommandArity` both validates an argument reader and emits the Redis-compatible
metadata arity. Irregular rules such as ranges and one-of sets still declare a
single minimum/metadata representation inside `CommandArity`; no caller enters
a second integer manually.

`TransactionPolicy` distinguishes normal queueable commands, transaction
control commands, and commands disallowed during `MULTI`. The transaction queue
policy no longer hard-codes command names.

`CommandDefinition<T>` combines syntax, parser, and preparer. Registration uses
only that object. `COMMAND INFO`, normal dispatch, and transaction preflight all
look up the same definition.

Bare `AUTH` has a minimum argument count of two including the command name. In
`MULTI`, a bare `AUTH` therefore returns wrong-arity immediately, marks the
transaction aborted, and causes `EXECABORT` without applying queued writes.

## Prepared Command Execution

The new execution flow is:

```text
decode
  -> acquire reply-control slot and executor admission
  -> parse
  -> prepare using a read-only context
  -> reserve the prepared reply plan
  -> execute once using the mutation-capable context
  -> render into the reserved reply
  -> complete the ordered slot
```

Preparation returns a framework-owned `PreparedCommand`:

```java
public interface PreparedCommand extends AutoCloseable {
    ReplyShape replyShape();
    ValidationResult validateBeforeExecute();
    void execute(CommandExecutionContext context);
}
```

Preparation may retain a read result, a semantic result source, or a storage
version token. Its retained memory is included in `ReplyShape`. The framework
closes it after success, parse failure, connection close, cancellation, stale
re-preparation, or shutdown.

If capacity is unavailable, the task waits with the same prepared object. The
handler is not rerun. Before execution, `validateBeforeExecute()` checks any DB
version token. A stale preparation is closed and prepared again before any
mutation. This permits other connections to make progress while the task waits
without committing against an obsolete preview.

Mutation commands with result-dependent replies use a storage-provided
`PreparedMutation`: preview and version token are read-only; validation and
commit run consecutively on the owner thread after reservation. Commands with a
request-bounded reply may reserve that conservative bound without a DB preview.

Once mutation execution begins, reply-capacity failure is a framework defect,
not a retry signal. The executor never reruns the command. If the mutation
result cannot be rendered or its visibility is unknown, the connection closes
and later slots are cancelled.

`EXEC` prepares every queued command, combines their reply plans into one
envelope, reserves that envelope, validates the prepared set, and only then
executes the queued commands in order. Any pre-execution parse or preparation
failure preserves Redis dirty-transaction behavior. No queued mutation begins
before the `EXEC` envelope exists.

## Ingress, Backpressure, And Reply Capacity

There is one executor admission authority. Delete
`YierdisFastCommandHandler.pendingSubmissions`. The decoder retains at most one
fully decoded message that has not completed handoff.

Handoff acquires two resources as one logical admission:

1. an ordered reply-control slot;
2. an executor backlog reservation for the request's retained bytes.

If either acquisition fails, the coordinator releases the partial acquisition
and waits with the decoder's single pending message. A request becomes admitted
only after both resources are owned and its executor task is published.

Outbound capacity maintains typed waiters rather than one untyped waiter per
connection:

- `CONTROL_ADMISSION` waits for a new reply-control lease;
- `LEASE_EXPANSION` waits for one existing reply slot to expand;
- each existing reply slot may own at most one expansion waiter;
- one connection may simultaneously have expansion waiters for admitted slots
  and one control-admission waiter for the decoder;
- expansion of an already admitted head reply has priority over admitting
  another request on the same connection;
- global fairness rotates across connections after satisfying the progress
  requirement for admitted work.

Failure to register or retain a capacity wait is not task cancellation. An
admitted slot is removed without output only while its connection is already
closing or during explicit shutdown cleanup. In every other case the framework
produces a terminal error reply or closes the connection.

Read control uses an enum set or equivalent token set with these independent
reasons:

- `INGRESS_BUDGET`;
- `EXECUTOR_QUEUE`;
- `REPLY_CAPACITY`;
- `TRANSPORT_UNWRITABLE`;
- `CLOSING`.

Adding or removing one reason cannot overwrite another owner. A physical read
is scheduled only when no pause reason remains and the connection is open.

## Runtime DB Capabilities

`DbEngineFactory.create(...)` accepts one `DbEngineConfig` and must return a
non-null `RuntimeDbEngine`. Positional factory parameters are removed.

`RuntimeDbEngine` requires owner binding, baseline maintenance, shutdown, and
the semantic DB API. Optional production behavior uses explicit interfaces:

- `CommitPublishingDbEngine` for commit publication attachment;
- `GlobalMaxmemoryDbEngine` for cleanup, sampling, and eviction;
- `DefragmentableDbEngine` for native defrag maintenance.

`YierdisInstance` validates all engines before starting the commit stream or
accepting commands:

- a configured change sink requires every DB to implement commit publication;
- global maxmemory requires every participating DB to implement global
  maxmemory operations;
- enabled native defrag requires every configured DB to implement defrag;
- a null engine or inconsistent capability set fails startup.

Test doubles implement only the interfaces needed by their test configuration.
Production and test requirements no longer share silent defaults.

## Stable Memory Backend And Handle Identity

Replace the partial `NativeAllocator` extension point with the complete
`StableMemoryBackend` contract used by `db-memory`. It includes:

- stable object allocate, reallocate, resolve, pin, unpin, and free;
- region allocation needed by indexes;
- allocation scopes with commit and rollback;
- additional-growth estimation;
- memory usage and retained-resource accounting;
- owner binding and lifecycle.

`db-memory` depends only on this API. `YierdisFfmMemoryRuntime`,
`YierdisFfmRegion`, `YierdisStableNativeAllocator`, and FFM segment details stay
inside `memory-ffm`. Expiration-index regions are created through the backend
region API instead of directly constructing FFM regions.

The public stable handle is opaque and allocator-scoped:

```java
public record NativeHandle(long allocatorId, long localRaw) {
}
```

Each live backend receives a process-unique, monotonically allocated 64-bit
identity that is not reused. `resolve`, `free`, `pin`, `unpin`, and reallocation
validate allocator identity before inspecting the local slot. Cross-allocator
operations throw a dedicated ownership exception.

Compact local raw references may remain inside one backend's private native
data structures because the allocator identity is implicit there. They cannot
be constructed or resolved through the public API. `AllocatorKeyHandle` uses a
raw-handle equality fast path only when allocator identities also match;
otherwise semantic equality compares content.

## Protocol-Neutral DB Results

Delete storage contracts named or documented in terms of encoded RESP bytes,
including the current RESP-oriented metrics implementations.

`yierdis-server-api` defines protocol-neutral `ReplyShape` values for Redis
scalars, aggregates, semantic byte sequences, and retained-source memory. A
prepared command returns a shape, not a wire-byte count. The execution context
also exposes a `ReplySizer` port implemented by the active reply adapter:

```java
public interface ReplySizer {
    ReplyPlan plan(CommandSession session, ReplyShape shape);
}
```

`ReplyPlan` is the opaque capacity result consumed by the executor and reply
sink. Command and storage code may pass semantic shapes to the port but may not
implement protocol header or delimiter formulas.

Storage result sources expose semantic content and a repeatable, zero-copy
length view. A sequence reports its element count, retained-memory cost, and a
way to visit element payload lengths without consuming or copying the payload.
Map results expose pair count and field/value payload lengths in encounter
order. Storage does not calculate decimal header digits, CRLF bytes, RESP null
encodings, or aggregate headers.

Before execution, the framework passes the prepared shape to `ReplySizer`. The
active RESP adapter calculates exact RESP2 or RESP3 wire bytes from the payload
lengths and negotiated session version. Wire formulas therefore have one
implementation in the protocol/reply adapter.

## Observability And Server Composition

Move `HELLO`, `INFO`, and `STATS` command definitions out of `server-main` and
into the server command module in the command lane. They consume pure data
ports:

- `ServerIdentity` supplies product name, version, mode, and role;
- `ServerSnapshotProvider` returns immutable runtime, executor, connection,
  inbound, outbound, commit-stream, memory, and health snapshots.

These data ports live in `yierdis-command-api` because they are inputs to server
commands and contain no runtime or transport implementation type.

The provider does not receive `ExecutionRequest`, `CommandContext`, or
`RedisReplyWriter`. Command code parses sections and renders RESP2, RESP3, and
Redis-compatible INFO text. Runtime and transport adapters only capture their
own snapshots; `server-main` assembles the provider.

Replace the flat runtime configuration with:

```text
YierdisServerConfig
  NetworkConfig
  ExecutorConfig
  ReplyConfig
  StorageConfig
  MaintenanceConfig
```

CLI normalization builds these records with named builders or named factory
arguments. Bootstrap, runtime, executor, and channel initialization receive only
the configuration group they consume. Native defrag configuration has one path
from `StorageConfig` through `DbEngineConfig`; there is no composition-root
side channel.

## Architecture Governance

Maven POMs are the only source of allowed module dependencies. Remove manually
duplicated `allowed_dependencies` lists from `architecture-policy.yml`.

The policy keeps forbidden edges, package ownership, and composition rules. A
new reactor graph test parses every active module POM, resolves internal
artifact dependencies, and evaluates the forbidden-edge policy against the
actual graph. It fails for unknown policy module names and duplicate package
ownership.

ArchUnit and source guards enforce semantic boundaries that dependency graphs
cannot express:

- DB API and memory implementation contain no RESP or wire-encoding contract;
- command modules contain no Netty, transport waiter, or outbound-budget type;
- executor-core contains no concrete transport or storage implementation;
- `server-main` defines no command handler or reply renderer;
- execution entry points accept `CommandSession`, not a marker interface;
- production runtime capabilities do not use default no-op implementations.

Current architecture documentation is updated from the same actual module graph
and no longer shows stale edges.

## Error And Cleanup Semantics

- Invalid composition, missing capabilities, null engines, or non-serial owner
  executors fail before the server binds a listening socket.
- Parse and preparation errors occur before mutation and may return a normal
  Redis error reply.
- Capacity waits retain one explicit owner for the request, prepared command,
  reply slot, lease, and result sources.
- Connection close, shutdown, stale re-preparation, and task rejection each
  close retained resources exactly once.
- A failure after mutation begins is never retried automatically.
- Unknown mutation visibility closes the connection and cancels later ordered
  slots.
- Cancelling an admitted slot on an otherwise live connection without a reply
  is an invariant violation and closes the connection.

## Migration Strategy

This is a coordinated breaking rewrite with no compatibility adapters.

1. Define the replacement session, serial owner, command definition, prepared
   execution, runtime capability, stable backend, handle, snapshot, and grouped
   configuration contracts.
2. Delete the superseded public types and methods so compile errors enumerate
   every migration site.
3. Migrate memory and DB implementation to the new backend and runtime
   contracts.
4. Migrate command registration, transaction execution, and reply preparation.
5. Replace decoder handoff, transport pending submissions, capacity waiting,
   and read-pause state.
6. Migrate runtime and `server-main` to capability validation, snapshots, and
   grouped configuration.
7. Replace policy duplication and update current architecture documentation.

The branch may be temporarily uncompilable while downstream modules are being
migrated. The final implementation contains only the new architecture. Work is
split into independently reviewed implementation tasks even though the API
transition itself is coordinated.

## Test Strategy

All Java and Maven commands use JDK 25.

### Command and transaction tests

- parser validation and `COMMAND INFO` metadata derive from the same syntax;
- bare `AUTH` outside and inside `MULTI` has correct arity behavior;
- bare `AUTH` in `MULTI` marks the transaction dirty, `EXEC` returns
  `EXECABORT`, and queued writes do not apply;
- transaction control comes from definition policy, not command-name checks;
- `EXEC` reserves the combined envelope before executing any queued mutation.

### Prepared execution tests

- capacity blocking retains one prepared command and does not rerun prepare or
  execute unnecessarily;
- a stale version token causes pre-execution re-preparation;
- one admitted mutating command executes at most once;
- a post-mutation output failure closes the connection instead of replaying.

### Network state-machine tests

- one connection can hold a control-admission waiter and one or more lease
  expansion waiters without losing a reply;
- a pipelined sequence receives exactly one reply per admitted request in order;
- an admitted head expansion progresses before new same-connection admission;
- executor saturation leaves at most one decoder-pending message and creates no
  transport command queue;
- removing one pause reason does not resume input while another remains;
- close and shutdown release waiter, slot, lease, request, and prepared-command
  ownership exactly once.

### Storage and memory tests

- a factory returning null fails instance creation;
- configured commit, global maxmemory, and defrag features reject engines that
  lack the corresponding capability;
- two allocators may create the same local raw value, but cross-allocator
  resolve, free, pin, and equality fast paths cannot alias;
- concurrent first owner binding has one winner and cannot split DB and backend
  ownership;
- a non-FFM fake stable backend can exercise the DB construction and basic
  mutation contract without importing FFM implementation types;
- semantic sequence sizing can be consumed by RESP2 and RESP3 sizers without
  storage knowing either encoding.

### Architecture and configuration tests

- parsed Maven dependencies satisfy the forbidden graph;
- policy module and package names all resolve to active sources;
- storage sources have no RESP byte formulas;
- `server-main` has no command implementation;
- each grouped configuration validates independently and bootstrap passes the
  exact group to each component;
- CLI round trips through grouped configuration without positional coupling.

### Verification

Focused tests run after each migration task. Final verification runs the full
Maven test reactor on JDK 25, followed by existing server smoke checks that do
not require external services. No ignored or disabled regression test is an
acceptable completion condition.

## Risks And Mitigations

The primary risk is the size of the coordinated API break. The implementation
plan must define exact interface signatures first and assign every compile
failure to one migration task. Old adapters are not used to reduce the apparent
scope.

Prepared mutation validation can add read work. Request-bounded conservative
plans remain available, and version validation avoids committing against a
stale preview. Performance is measured after correctness; the rewrite does not
weaken capacity guarantees to recover throughput.

Typed waiters can starve admission if expansion is permanently dominant.
Scheduling therefore gives admitted head work progress priority while rotating
across connections. Deterministic fairness tests cover both progress and
admission recovery.

The paired native handle is wider than the current Java record. Compact local
references remain private to the backend so native data structures do not need
to repeat allocator identity for every same-backend edge.

Removing RESP metrics from storage may introduce an extra length traversal.
Result sources must provide a repeatable zero-copy size view, and protocol
sizing benchmarks will identify any hot path that needs a protocol-owned cached
shape.

## Success Criteria

- Every admitted request produces one ordered reply or explicitly closes its
  connection; no capacity path silently drops a reply slot.
- Transport code has no second command backlog and read pauses compose by
  reason.
- Command arity has one source of truth and the `AUTH` transaction regression
  passes.
- Capacity waiting cannot execute a mutation twice.
- Execution entry points require `CommandSession` and a serial owner executor.
- Runtime engine capability mismatches fail startup; production capabilities
  have no default no-op.
- Cross-allocator native-handle operations cannot alias another live object.
- `db-api` and `db-memory` contain no RESP wire-size calculation.
- `db-memory` imports only the stable memory API, not FFM implementation types.
- INFO/STATS rendering and server command definitions no longer live in
  `server-main`.
- Runtime configuration is grouped and no 40-plus-argument constructor remains.
- Architecture policy is evaluated against the actual Maven graph.
- The complete JDK 25 Maven test reactor and relevant smoke checks pass.
- No old compatibility adapter, ignored failure, or unrelated worktree change
  remains in the final implementation.
