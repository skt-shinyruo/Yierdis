# Transport-Neutral Executor Core Design

> Module ownership note: source paths and Maven module names in this older
> spec are superseded by
> `2026-04-28-kernel-storage-adapter-rearchitecture-design.md`. The executor
> ownership guidance still applies; current code uses explicit execution,
> command, runtime, and app module families.

## Summary

This design moves Yierdis command execution runtime out of `yierdis-server` and turns
`yierdis-executor-core` into the true home of submission, drain, backpressure,
connection/session state, and execution lifecycle.

After this change, `yierdis-server` stops owning the command execution model. It only
owns:

- Netty pipeline wiring
- protocol decode / encode glue
- transport adapters for connection state, I/O control, and reply flushing

The command executor core becomes transport-neutral and no longer directly depends on
`Channel`, `ChannelHandlerContext`, `ByteBuf`, `autoRead`, or other Netty types.

## Problem Statement

The current architecture says `yierdis-server` is a wiring shell and
`yierdis-executor-core` is a Netty-free executor helper module. In practice, that
boundary is false.

Today, `yierdis-server` owns:

- `NettyCommandExecutor`
- `NettyCommandSubmitter`
- `NettyCommandDrainLoop`
- `NettyCommandExecutionSupport`
- `ServerConnectionContext`
- `ServerSessionState`

That means the server module still owns:

- command submission and rejection semantics
- backlog accounting
- fair scheduling state
- connection-local backpressure state
- close-after-reply and closing semantics
- executor-level statistics
- session access during command execution

The result is that `server` is not just wiring. It is the real runtime owner for command
execution.

This creates four concrete problems:

1. The documented module boundary is misleading.
2. Execution runtime cannot be reused by a non-Netty transport without rewriting core
   behavior.
3. Many tests under `yierdis-server` are testing executor semantics rather than Netty
   adaptation.
4. Transport concerns and execution concerns are entangled, so any change to one side
   tends to spill into the other.

## Goals

- Make `yierdis-executor-core` the single runtime owner for command execution semantics.
- Reduce `yierdis-server` to protocol wiring plus Netty adaptation.
- Remove direct Netty types from the executor core public API and internal hot path.
- Preserve the Redis-style single-thread command execution model.
- Keep `core-command`, `core-runtime`, and `core-db` responsibilities intact.
- Reorganize tests so executor behavior is tested in `yierdis-executor-core`, while
  `yierdis-server` tests focus on Netty integration.

## Non-Goals

- No DB-internal refactor beyond what is needed to keep the new executor integrated.
- No protocol redesign.
- No command registration redesign.
- No attempt to preserve source compatibility with the existing `NettyCommandExecutor`
  API.
- No extra transport implementation in this change beyond Netty; the goal is to make it
  possible, not to ship a second transport immediately.

## Considered Approaches

### Approach A: Keep everything in `yierdis-server`, only split classes

This reduces file size but keeps `server` as the owner of execution semantics.

Rejected because it addresses readability without fixing the wrong ownership boundary.

### Approach B: Move the execution runtime into `yierdis-executor-core`

This extends the already-existing Netty-free executor module from "algorithms only" into
the real command execution runtime.

Chosen because it fixes the ownership problem directly, aligns with existing module
intent, and does not require inventing another module.

### Approach C: Create a new runtime module just for command execution

This would create a conceptually clean new home for the executor runtime.

Rejected because it overlaps too much with `yierdis-executor-core` and would create
unnecessary module duplication.

## Architectural Decision

Adopt Approach B.

`yierdis-executor-core` becomes the runtime owner for:

- command submission
- drain loop execution
- backlog budget
- backpressure state transitions
- connection-local execution state
- session ownership during execution
- maintenance dispatch onto the owner thread
- executor statistics and snapshots

`yierdis-server` becomes an adapter layer that:

- decodes protocol messages into `ExecutionRequest`
- resolves a transport connection handle
- adapts transport events to executor-core abstractions
- encodes and flushes replies via Netty

## Target Module Responsibilities

### `yierdis-executor-core`

Owns the transport-neutral execution runtime.

New or expanded responsibilities:

- `CommandExecutor`
- submission controller
- drain loop
- execution support
- connection context
- default session state
- executor statistics
- maintenance submission
- transport-neutral backpressure integration

This module may depend on:

- `yierdis-core-contract`
- `yierdis-core-command`

It must not depend on:

- Netty
- protocol modules

### `yierdis-server`

Owns Netty-specific integration only.

Responsibilities after the refactor:

- `YierdisServer`
- `YierdisServerBootstrap`
- `YierdisServerChannelInitializer`
- protocol decode / protocol error path
- Netty adapter implementations for executor-core interfaces
- NDJSON reply writing / `ByteBuf` glue
- server-facing command module

### `yierdis-core-runtime`

Still owns:

- `YierdisInstance`
- DB lifecycle
- owner-thread runtime seam
- global maxmemory coordination

It must continue to avoid assembling the command processor or executor runtime.

## New Core Abstractions

The executor runtime should no longer use transport objects as its scheduling key. It
should operate through explicit interfaces.

### `ExecutionConnection`

Represents one logical command connection from the executor core point of view.

Responsibilities:

- stable connection identity
- access to per-connection execution context
- access to the associated session object
- ability to query whether the connection is already closing
- optional transport-owned attachment handle for adapters

This is the executor-side handle used as the queue key and backpressure key.

### `ExecutionIoAdapter<C>`

Represents the transport boundary for a connection type `C`.

Responsibilities:

- `isActive(C connection)`
- `isWritable(C connection)`
- `disableInput(C connection)`
- `enableInput(C connection)`
- `registerCloseCallback(C connection, Runnable callback)`
- `newReplySink(C connection)` returning a `BytesSink` suitable for `ReplyWriterFactory`
- `writeBufferedReply(C connection, boolean closeAfterReply)`
- `flushPending(C connection)` or equivalent batching hook

This keeps reply semantic authority with `ReplyWriter` while letting the transport decide
how bytes are buffered and flushed. The exact batching shape can be decided during
implementation, but the executor core must own the lifecycle and only delegate the
transport effect.

### `ExecutionConnectionContext`

Transport-neutral replacement for the generic parts of `ServerConnectionContext`.

Responsibilities:

- session reference
- pending count / pending bytes
- closing flag
- executor-owned input-disabled marker
- fair-scheduling queue state
- connection-local statistics

This object lives in executor core. A transport adapter may store it wherever it wants
for lookup, but executor core owns the type and behavior.

### `DefaultExecutionSession`

Transport-neutral replacement for the generic parts of `ServerSessionState`.

Responsibilities:

- selected DB index
- client metadata if still needed by commands
- transaction queue state

Transport-specific stats access must not live here.

If server-specific session fields remain necessary, they must wrap or compose this type
rather than reintroduce a server-owned session authority.

## Reply Model In The New Design

This refactor does not change reply semantic authority.

`ReplyWriter` remains the single command-layer reply authority. The executor core still
constructs a `CommandContext(session, out)` and invokes `YierdisFastCommandProcessor`.

The change is only about where output goes afterward:

- core produces replies through `ReplyWriter`
- transport adapter turns them into wire output

The executor core must not start depending on protocol reply DTOs.

## New Request Execution Flow

The target request path is:

1. transport receives bytes
2. protocol decoder produces a protocol request
3. server adapter converts the request into `ExecutionRequest`
4. server resolves or creates the transport connection adapter
5. server submits `(ExecutionConnection, ExecutionRequest)` to executor core
6. executor core performs submission checks and queue bookkeeping
7. executor core drain loop runs on the owner thread
8. executor core builds `CommandContext(session, out)`
9. `YierdisFastCommandProcessor` executes against `DbEngine`
10. transport adapter writes and flushes the reply

The critical invariant remains:

- I/O threads do not touch DB state
- DB state is only accessed on the owner thread

## Class Migration Plan

### Move or replace in `yierdis-executor-core`

- replace `NettyCommandExecutor` with `CommandExecutor`
- move and rename `NettyCommandSubmitter`
- move and rename `NettyCommandDrainLoop`
- move and rename `NettyCommandExecutionSupport`
- move generic state out of `ServerConnectionContext`
- move generic session state out of `ServerSessionState`
- keep `ExecutorTaskQueue`, `ExecutorBackpressureController`, `ExecutorBacklogBudget`,
  and related helper classes where they are

### Keep in `yierdis-server`

- `YierdisServer`
- `YierdisServerBootstrap`
- `YierdisServerChannelInitializer`
- `YierdisFastCommandHandler`
- `ProtocolErrorReplyHandler`
- `ProtocolCommandAdapter`
- reply writer factory and Netty byte sink

### Remove or shrink in `yierdis-server`

- remove the old `NettyCommandExecutor` as the runtime owner
- replace `ServerConnectionContext` with a thin Netty attachment wrapper
- shrink `ServerSessionState` or remove it entirely if `DefaultExecutionSession` fully
  covers current needs

## Breaking Changes Accepted By This Design

- The existing `NettyCommandExecutor` construction and testing API may be removed.
- `ServerConnectionContext` is no longer a public or test-referenced state authority for
  execution semantics.
- `ServerSessionState` may be replaced by a transport-neutral session implementation.
- Existing unit tests that directly assert current server-owned executor internals may be
  rewritten.
- Internal observability snapshot types may move modules or change shape.

Where possible, the external `INFO` and `STATS` user-facing surface should stay stable,
but it is not a hard compatibility promise for this refactor.

## Migration Strategy

The refactor should happen in six phases.

### Phase 1: Introduce transport-neutral executor runtime types

Add the new executor-core abstractions and keep the current Netty executor intact.

This phase creates:

- connection abstraction
- I/O adapter abstraction
- connection context
- default session type
- new executor stats snapshot

### Phase 2: Port submit/drain/backpressure logic into executor core

Rebuild current runtime behavior behind the new abstractions while keeping the old server
integration available as a temporary compatibility layer.

### Phase 3: Add Netty adapters in `yierdis-server`

Implement:

- Netty connection wrapper
- Netty I/O adapter
- Netty reply flushing adapter

At this point, `yierdis-server` should only adapt to executor core rather than own the
behavior.

### Phase 4: Rewire bootstrap and pipeline

Update `YierdisServerBootstrap`, `YierdisFastCommandHandler`, and related server classes
to construct and use the new executor core.

### Phase 5: Delete obsolete server-owned runtime types

Remove:

- old executor implementation
- obsolete connection context authority
- duplicated stats and scheduling state

### Phase 6: Rewrite tests and docs around the new boundary

Move semantic executor tests to `yierdis-executor-core` and keep only Netty integration
tests in `yierdis-server`.

## Testing Strategy

### New executor-core test scope

`yierdis-executor-core` must gain direct tests for:

- submit success and rejection reasons
- queue capacity and queued-bytes budget
- fair scheduling
- backpressure enter / exit behavior
- closing semantics
- close-after-reply
- maintenance dispatch on the same owner thread
- command execution against a fake transport adapter

These tests should not require Netty.

### Remaining server test scope

`yierdis-server` tests should cover:

- Netty pipeline wiring
- protocol decode to `ExecutionRequest`
- Netty writability events mapped into executor-core input control
- reply encoding and flush behavior through Netty
- connection close callbacks reaching the executor runtime

### Guardrail updates

Architecture guard tests should be updated so they assert the new model:

- `yierdis-executor-core` must not depend on Netty or protocol modules
- `yierdis-server` must not reintroduce executor-runtime state ownership
- reply authority stays with `ReplyWriter`

## Documentation Impact

At minimum, these docs will need updates after implementation:

- `docs/module-architecture.md`
- `docs/request-execution-flow.md`
- `docs/executor-and-backpressure.md`
- `docs/project-overview.md`
- `docs/main-path-walkthrough.md`
- `docs/configuration-and-operations.md`
- `docs/development-navigation.md`

The docs currently describe `NettyCommandExecutor` and `ServerConnectionContext` as core
runtime objects. After this change, that description must move to executor-core concepts
with Netty called out as one adapter only.

## Why This Direction Is Worth The Cost

This is a breaking refactor, so it must pay for itself structurally.

It does, because it fixes the actual ownership problem rather than hiding it.

After the refactor:

- module names match reality
- server stops being the real runtime owner
- executor logic becomes reusable and easier to test
- Netty integration becomes thinner and easier to replace
- future transport work no longer requires copying the command execution model

Without this refactor, the codebase can only keep pretending that execution runtime lives
outside `server` while continuing to centralize it there in practice.

## Open Questions Resolved By This Design

- Should the fix be conservative? No. The design intentionally permits breaking changes
  so the ownership boundary can be corrected cleanly.
- Should a new module be introduced? No. `yierdis-executor-core` is already the natural
  home for this runtime.
- Should the target still be Netty-biased? No. The design explicitly requires a
  transport-neutral executor core with Netty as an adapter only.
