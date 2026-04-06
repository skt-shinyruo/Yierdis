# Protocol/Execution Model Redesign

**Date:** 2026-04-06

## Goal

Unify Yierdis request execution around one execution-layer request model without changing the Custom Protocol v1 wire format, command semantics, CLI behavior, transaction semantics, or server reply semantics.

This design is a follow-up to the completed architecture remediation wave. It intentionally reopens protocol/execution internals only after the earlier server/runtime/db boundary work has landed.

## Why This Is In Scope Now

The earlier architecture remediation design explicitly said not to reopen protocol-layer rewrites while command-spec SSOT and `YierdisDb` shrink work were still in flight.

That constraint no longer applies in the same form:

- the command registry and server command assembly cleanup have already landed
- runtime observability and ownership seams have already been extracted
- connection ownership has already been centralized around `ServerConnectionContext`
- the remaining architectural duplication is now concentrated in the protocol-to-execution path

At this point, keeping the current request-model duplication creates more long-term coupling than a targeted redesign would.

## Confirmed Constraints

- Custom Protocol v1 wire format must remain compatible.
- Existing command behavior must remain compatible.
- CLI behavior must remain compatible.
- Server command execution write-back must still use `ReplyWriter` as the single semantic authority.
- Netty backpressure, queueing, and connection lifecycle behavior must remain compatible.
- The redesign must be incremental and shippable in intermediate states.

## Problem Summary

The current request path still uses multiple overlapping representations for the same command:

1. `CustomRequestDecoder` produces `CustomProtocolV1Request`, a protocol DTO with `String cmd` and `List<String> args`.
2. `ProtocolCommandAdapter` converts that DTO into an execution-layer `Command` implementation backed by `byte[][]`.
3. `YierdisFastCommandProcessor` copies argv again when queueing transactional commands.
4. `TransactionCommands` rewraps queued `byte[][]` into another `Command` implementation for replay.
5. `YierdisChangeEvent` records writes as raw `byte[][]`, introducing yet another replay-oriented request shape.

This has four architectural costs:

- request validation rules are split across protocol adaptation, command execution, and transaction replay
- transaction replay is not using the same first-class request type as normal execution
- change events, future AOF, and future replication have no stable execution-layer request contract to share
- every new request source risks inventing a new representation instead of plugging into one stable execution model

## Recommended Approach

Introduce a new execution-layer contract, `ExecutionRequest`, in `yierdis-core-contract`, and make it the only request model accepted by the command processor.

Wrap replayable command facts in `ExecutionRecord` when the request must outlive a single connection execution context, for example:

- transaction queues
- change events
- future AOF
- future replication

This keeps the architecture split clean:

- protocol modules own wire decoding and protocol DTOs only
- server owns connection context, scheduling, and protocol-to-execution adaptation
- core command owns execution of `ExecutionRequest`
- replay-oriented subsystems share `ExecutionRecord` instead of inventing their own argv container

## Alternatives Considered

### Option A: Keep `Command` and remove only one copy layer

Retain `Command` as the primary execution contract and only optimize `ProtocolCommandAdapter` or transaction queue copying.

- Pros: small surface-area change
- Cons: preserves the core architectural problem because the system still lacks a stable execution-layer request model shared by replay and observability paths

### Option B: Redesign protocol DTOs to be byte-oriented and reuse them directly

Replace `CustomProtocolV1Request(String, List<String>)` with a binary-friendly protocol DTO and pass it through execution.

- Pros: reduces string-to-bytes conversions on the protocol path
- Cons: pollutes protocol modules with execution concerns and collapses an intentionally useful protocol boundary

### Option C: Introduce `ExecutionRequest` and `ExecutionRecord`

Create an execution-layer request contract and migrate protocol adaptation, transaction replay, and change events onto it.

- Pros: gives the repository one execution-language for normal execution and replay, keeps protocol DTOs wire-focused, and reduces long-term coupling
- Cons: requires a staged migration because `Command` currently sits on the hot path

**Recommendation:** Option C.

## Target Architecture

### Core Principles

- One first-class execution request model.
- Protocol DTOs remain protocol-shaped, not execution-shaped.
- Replayable command facts use one stable record type.
- `ReplyWriter` remains the only server reply semantic authority.
- Connection state stays outside the request object.

### Public Execution Contracts

#### `ExecutionRequest`

`ExecutionRequest` lives in `yierdis-core-contract` and becomes the primary input to command execution.

It should preserve the performance-relevant properties currently needed from `Command`:

- argc access
- null-aware argv access
- byte-oriented argument access
- retained-bytes accounting
- closeable lifecycle for request-owned buffers when needed

It must not carry transport or connection state.

#### `ExecutionRecord`

`ExecutionRecord` also lives in `yierdis-core-contract` and wraps replayable execution facts:

- `dbIndex`
- immutable `ExecutionRequest` snapshot

This type is used only when the request must be persisted, queued, forwarded, or replayed outside the original request handling call.

## Module Boundaries

### `yierdis-protocol-*`

Protocol modules continue to own:

- frame parsing
- JSON/schema validation
- protocol DTO definitions
- protocol codec tests

Protocol modules must not depend on `yierdis-core-command`.

Protocol modules may know about protocol request DTOs only. They must not emit executable requests directly.

### `yierdis-server`

Server owns:

- `ProtocolRequest -> ExecutionRequest` adaptation
- connection context
- session state
- scheduling/backpressure
- handoff into the command executor

Server remains the composition root between protocol and execution.

### `yierdis-core-contract`

Core contract owns:

- `ExecutionRequest`
- `ExecutionRecord`
- `ReplyWriter`
- `CommandContext`
- session-facing execution contracts

This is the only place where the execution request model should be defined.

### `yierdis-core-command`

Core command owns:

- command registry
- command processor
- command handlers
- transaction command behavior

It should consume only `ExecutionRequest` as input.

### `yierdis-core-runtime` and `yierdis-core-db`

Runtime and DB layers remain protocol-agnostic.

They should not need to know whether a request came from:

- a network protocol
- a transaction replay
- a change sink replay
- a future persistence or replication source

## Target Data Flow

The target request flow becomes:

1. `ByteBuf` frame decode
2. `CustomProtocolV1Request`
3. server-side adaptation to `ExecutionRequest`
4. command execution
5. optional wrapping as `ExecutionRecord` when the request must be queued or emitted

The repository should no longer rely on this longer chain:

1. `CustomProtocolV1Request`
2. server-local `Command`
3. copied `byte[][]`
4. transaction-local `QueuedCommand`
5. raw `byte[][]` change event payload

## Transaction And Replay Design

### Transaction Queue

The transaction queue should store `ExecutionRequest` snapshots directly instead of `byte[][]`.

`MULTI/EXEC` behavior remains the same:

- queue commands during `MULTI`
- preserve current queue limits
- abort on queueing errors
- replay queued commands during `EXEC`

The difference is architectural, not behavioral: transactional replay now uses the same first-class execution request model as the normal execution path.

### Change Events

`YierdisChangeEvent` should stop carrying raw `byte[][] argv`.

Instead, it should carry an `ExecutionRecord`, which gives change consumers a replayable execution payload plus DB routing information without exposing an ad hoc argv format.

### Future AOF/Replication

This redesign does not implement AOF or replication, but it establishes the intended contract:

- capture an `ExecutionRecord`
- persist or forward it
- replay it through the same execution path

That is a better long-term seam than capturing raw argv arrays in one subsystem and custom command wrappers in another.

## Request Ownership And Error Handling

The redesign does not move error ownership between layers.

### Protocol-layer errors stay in protocol

These remain protocol concerns:

- frame length parsing
- JSON parse failures
- schema validation failures
- protocol resynchronization

### Execution-layer errors stay in command execution

These remain execution concerns:

- unknown command
- wrong arity
- wrong type
- OOM / command-level semantic failures

The redesign changes request representation and flow, not where errors are defined.

## Migration Strategy

Use a staged migration with compatibility shims:

### Phase 1: Introduce new contracts

- add `ExecutionRequest`
- add `ExecutionRecord`
- keep `Command` temporarily

### Phase 2: Add processor support for the new model

- add an `ExecutionRequest` entry path to `YierdisFastCommandProcessor`
- keep the old `Command` entry path as a compatibility adapter

### Phase 3: Move request producers and replayers

- migrate `ProtocolCommandAdapter` to emit `ExecutionRequest`
- migrate transaction queues to store `ExecutionRequest`
- migrate `YierdisChangeEvent` to store `ExecutionRecord`

### Phase 4: Remove the old primary path

- stop using `Command` as the main processor input
- keep only the compatibility surface that is still justified by tests or external embedding use
- delete dead wrappers once all production call sites are migrated

## Testing Strategy

### Required Test Categories

- protocol-to-execution adaptation tests
- transaction replay equivalence tests
- change event replay payload tests
- request lifecycle and retained-bytes tests
- architecture guard tests for dependency direction
- focused server integration tests

### New Verification Gates

#### Adaptation semantics

Tests must verify that `ProtocolRequest -> ExecutionRequest` preserves:

- UTF-8 encoding semantics
- null argv semantics
- retained-bytes accounting
- command-name normalization behavior

#### Transaction equivalence

A command executed directly and the same command executed through `MULTI/EXEC` must produce:

- the same reply
- the same DB mutation result
- the same change-event semantics

#### Change-event contract

Tests must verify that `YierdisChangeEvent` emits replayable execution facts through `ExecutionRecord`, not raw argv arrays.

#### Boundary guards

Guards must fail if:

- `protocol-*` depends on `core-command`
- server reintroduces a production-only `Command` wrapper as the main protocol handoff
- transaction replay falls back to raw `byte[][]`
- change events fall back to raw argv payloads

## Risks And Mitigations

### Risk: Hot-path regression

Replacing `Command` in the hot path can introduce extra allocations or slower argument access.

Mitigation:

- make `ExecutionRequest` byte-oriented from the start
- keep the old processor path as a benchmarkable compatibility layer during migration
- run focused executor and command regression tests after each phase

### Risk: Partial migration leaves two first-class models

If both `Command` and `ExecutionRequest` remain primary for too long, the redesign will increase complexity instead of reducing it.

Mitigation:

- define `ExecutionRequest` as the target SSOT immediately
- treat `Command` as transitional only
- add guard tests preventing new production call sites from depending on the old path

### Risk: Drift between request and replay semantics

If transactions or change events keep bespoke snapshot logic, replay will still diverge from direct execution.

Mitigation:

- require both transactions and change events to use the new shared contracts
- add equivalence tests between direct execution and replayed execution

## Out Of Scope

- Changing the Custom Protocol v1 wire format
- Redesigning `ReplyWriter`
- Replacing Netty executor scheduling/backpressure architecture
- Implementing AOF, replication, or persistence features
- Continuing the deeper `YierdisDb` decomposition in the same change set

## Expected Outcomes

- one first-class execution request model
- one replayable execution record model
- protocol DTOs remain wire-focused
- transactions and change events stop inventing separate argv containers
- future persistence and replication work gains a stable replay contract
- external behavior remains unchanged
