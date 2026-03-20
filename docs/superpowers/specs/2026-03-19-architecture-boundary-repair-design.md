# Architecture Boundary Repair Design

**Date:** 2026-03-19

## Goal

Repair the current boundary leaks between `core-command`, `core-runtime`, `protocol-model`, and `yierdis-server` without changing the external wire protocol or command semantics.

## Problem Summary

The current codebase has three architectural leaks that amplify coupling:

1. `core-command` still owns server-facing command behavior such as `HELLO/INFO/STATS`, and directly imports protocol metadata.
2. `core-runtime` still assembles command processors through `YierdisInstance`, which means runtime depends on command concerns instead of only DB lifecycle and routing.
3. Dependency guardrails only cover `core-api` and `core-db`, so the boundary intent described in the README is not enforced for `core-command` and `core-runtime`.

The deeper connection-state model issues in `Session`/`ServerSession`/`ServerRuntimeState` remain real, and ownership of `SELECT/QUIT/COMMAND` should be revisited together with that redesign. Changing them before the module boundaries are cleaned up would create a larger, riskier refactor. This design therefore splits the work into two waves.

## Scope

### In Scope for Wave 1

- Make `YierdisFastCommandProcessor` extensible so server-only commands are not hard-coded in the core default registry.
- Move protocol/build-info/observability command registration out of `core-command` and into `yierdis-server`.
- Remove command processor factory helpers from `YierdisInstance`.
- Remove unnecessary protocol/runtime dependencies caused by the old assembly path.
- Add architecture tests / build guardrails that fail when these boundaries regress.

### Out of Scope for Wave 1

- Redesign `Session` / `ServerSession` / `TransactionState`.
- Replace `NettyCommandExecutor` or collapse server channel state classes.
- Change command semantics, protocol format, or Netty backpressure behavior.

## Recommended Approach

Introduce a small command-registration extension point in `core-command`. The default core processor should register only data/DB-oriented commands plus other transport-agnostic behavior that already belongs in core, such as `PING/ECHO/COMMAND/SELECT/QUIT/FLUSHDB`. The server module will then contribute its own command registrar for server-facing commands such as `HELLO/INFO/STATS` and wire that registrar at composition time.

At the same time, `YierdisInstance` should stop manufacturing processors. Its job should end at DB engine lifecycle, routing, and resource ownership. Processor assembly belongs to the composition root, which for the Netty server is `yierdis-server`.

## Resulting Boundaries

- `yierdis-core-contract`: protocol-agnostic command/reply/session contracts.
- `yierdis-core-api`: DB capability and lifecycle contracts.
- `yierdis-core-db`: concrete DB implementation.
- `yierdis-core-command`: transport-agnostic command processor + default core command modules + current shared connection commands.
- `yierdis-core-runtime`: instance assembly for DB engines and routing only.
- `yierdis-server`: Netty pipeline, connection state, protocol/build-info/observability commands, runtime observability, composition root.
- `yierdis-protocol-*`: wire format and protocol metadata only.

## Wave 2 Follow-up

Once Wave 1 lands, follow up with a separate refactor that introduces an explicit server connection context, reduces the current `instanceof` / `Channel.attr(...)` coordination pattern, and re-evaluates whether `SELECT/QUIT/COMMAND` should move fully into `yierdis-server`. That work should happen after module boundaries are stable so it does not get mixed with package ownership changes.
