# Architecture Remediation Design

**Date:** 2026-03-31

## Goal

Execute the full architecture remediation of Yierdis without changing the external protocol, command semantics, CLI behavior, or user-visible replies.

The active execution plan is now `docs/superpowers/plans/2026-04-06-architecture-remediation-replan.md`, and server command execution write-back still uses ReplyWriter.

## Confirmed Constraints

- Custom Protocol v1 wire format must remain compatible.
- Existing command behavior must remain compatible.
- CLI behavior must remain compatible.
- The work is architectural and internal. It must not become a feature rewrite.
- The refactor must be incremental. Each wave must compile, pass focused verification, and remain shippable.

## Problem Summary

The current codebase has three linked architecture problems:

1. Connection ownership is fragmented across multiple `Channel.attr(...)` state holders in `yierdis-server`, so transaction state, closing state, backpressure state, and scheduling state do not have a single owner.
2. `YierdisDb` still concentrates too many responsibilities. The public `DbReads` and `DbWrites` split exists, but the implementation still routes most behavior back into one very large state owner.
3. Protocol and reply abstractions had drifted in two directions at once. Protocol decoding already produced executable `Command` objects, while reply semantics had been blurred between `ReplyWriter` and `ReplyValue`.

These issues amplify each other. If the DB is split before connection ownership is stabilized, the project will keep refining code on top of unclear boundaries. If protocol abstractions are changed before the core ownership boundaries are fixed, the refactor will add more adapters without actually reducing coupling.

## Recommended Approach

Use a staged architecture refactor with three sequential waves:

1. Fix ownership boundaries in `server` and remove residual command metadata leakage from `core-command`.
2. Reduce responsibility density in `core-db` by extracting real internal collaborators and making `DbReads` / `DbWrites` compose them directly.
3. Decouple protocol requests from command execution contracts and define a single reply semantic source of truth.

This is preferred over a big-bang rewrite because the existing guardrails are mostly source-level and behavioral. The repository needs small, verifiable intermediate states rather than one large migration.

## Alternatives Considered

### Option A: Big-bang rewrite

Refactor `server`, `core-db`, and `protocol` in one pass.

- Pros: shortest time spent in compatibility states
- Cons: highest regression risk, worst debuggability, hardest review shape

### Option B: Staged remediation

Refactor in explicit waves with compatibility shims and focused tests at each boundary.

- Pros: best verification story, easiest rollback, keeps ownership changes understandable
- Cons: temporary duplication during transitions

### Option C: Parallel subsystem refactors

Refactor `server`, `db`, and `protocol` independently and merge at the end.

- Pros: can look faster on paper
- Cons: unsafe here because the current boundaries are exactly what is unstable

**Recommendation:** Option B.

## Target Architecture

### Core principles

- Each runtime concern has one owner.
- Public module APIs should be capability-oriented, not implementation-shaped.
- Protocol modules should model wire concerns, not execution concerns.
- Internal collaborator extraction is only useful if it actually removes responsibility from the current owner.

### Resulting boundaries

- `yierdis-core-contract`: command execution contracts only
- `yierdis-core-api`: DB capability contracts and runtime-neutral off-heap contracts
- `yierdis-core-db`: DB state owner plus package-local DB collaborators
- `yierdis-core-command`: transport-neutral command processor, registry, descriptors, and core command modules
- `yierdis-core-runtime`: DB instance assembly, owner-thread runtime seam, and global maxmemory coordination
- `yierdis-memory-*`: off-heap backend providers and backend-specific implementations
- `yierdis-protocol-*`: request/reply wire models, codecs, parsers, and protocol adapters
- `yierdis-server`: Netty composition root, connection context, runtime observability, and protocol-to-command adaptation

## Wave 1: Server Ownership and Boundary Cleanup

### Objectives

- Replace multiple server-side `Channel.attr(...)` state owners with a single server connection context.
- Remove the remaining server-only command metadata knowledge from `core-command`.
- Consolidate off-heap contract ownership so there is one SSOT API.

### Design

Introduce a single server connection object, `ServerConnectionContext`, as the only `Channel.attr(...)` entry point in `yierdis-server`.

It owns three state slices:

- command/session state: selected DB, transaction queue, client identity, auth flags
- runtime state: pending counters, pending bytes, closing flag, observability counters
- scheduling state: fair-queue state and executor scheduling flags

The existing `ServerSessionState`, `ServerRuntimeState`, and `NettyExecutorChannelState` must stop acting as independent externally accessible state roots. They can either be removed or turned into implementation details behind the new context.

For command metadata, `CommandRegistry` should evolve from "name -> handler" into "name -> handler + descriptor". `COMMAND` should query registry descriptors instead of hardcoding metadata in `CoreConnectionCommands`. `ServerCommandModule` must register `HELLO`, `INFO`, and `STATS` descriptors together with their handlers so core no longer needs explicit knowledge of server-only commands.

For off-heap APIs, the SSOT should become `yierdis-core-api` `offheap.api.*`. The `yierdis-memory/api` module should keep provider discovery and backend enumeration, but provider creation should return the core `OffHeapAllocator` contract directly instead of maintaining a parallel `YierdisOffHeapAllocator` hierarchy.

### Expected outcomes

- one server connection owner
- no duplicated channel state roots
- no server-only command metadata in `core-command`
- one off-heap contract family

## Wave 2: Core DB Responsibility Reduction

### Objectives

- Turn `YierdisDb` back into a state owner and coordinator instead of an all-in-one implementation surface.
- Make `DbReads` and `DbWrites` compose real collaborators, not forwarding shells.

### Design

`YierdisDb` should retain:

- key state containers: keyspace, expire index, memory ledger, thread guard
- cross-cutting collaborators: mutation execution, expiration support, maxmemory support
- public API façades: `reads()`, `writes()`, `memory()`, `lifecycle()`

Data-structure-specific logic should move into package-local collaborators, for example:

- `YierdisStringOps`
- `YierdisHashOps`
- `YierdisListOps`
- `YierdisSetOps`
- `YierdisZSetOps`
- `YierdisHllOps`
- `YierdisKeyspaceOps`
- `YierdisTtlOps`

To prevent those collaborators from reaching directly into arbitrary DB internals, add a package-local internal access seam, `YierdisDbInternals`, that exposes only the minimal primitives they need: live object lookup, `computeWithHandle`, expire mutation helpers, touch/LRU helpers, and mutation reservation hooks.

`YierdisDbReads` and `YierdisDbWrites` should then be rebuilt as composition layers over those collaborators rather than thin wrappers around `YierdisDb`.

### Expected outcomes

- smaller `YierdisDb`
- real collaborator boundaries per data structure family
- `DbReads` / `DbWrites` backed by independent focused implementations

## Wave 3: Protocol and Reply Semantics Cleanup

### Objectives

- Stop protocol decoding from producing execution-layer commands directly.
- Define a single reply semantic source of truth for the server write path.

### Design

Protocol modules should output protocol request models only. `CustomCommand` should stop being the direct protocol decode result and instead become a server-side adapter if command execution still needs a `Command` implementation.

The protocol decode flow should become:

1. protocol frame decode
2. protocol request model
3. server adapter to execution contract
4. command execution

For replies, the server write path should use `ReplyWriter` as the single semantic source of truth. `ReplyValue` should remain for client/tooling/parser use and optional encoding helpers, but it should not be treated as a parallel command-layer IR or alternate server write-path authority.

`CustomProtocolV1NdjsonEncoder` remains useful for client, bench, parser, and `ReplyValue` support, but it should not coexist with the main server write path as an alternate semantic authority.

`CustomProtocolV1Request` plus `ProtocolCommandAdapter` already provide most of the protocol/request decoupling needed right now. Do not reopen protocol-layer rewrites while command-spec SSOT and `YierdisDb` shrink work are still in flight.

### Expected outcomes

- protocol modules no longer depend on command execution contracts
- request models are wire-focused
- server write semantics have one source of truth

## Testing Strategy

### Required test categories

- architecture guard tests
- server request-execute-reply integration tests
- DB behavior regression tests across data structures, TTL, and maxmemory
- protocol compatibility tests for framing and reply shape

### Wave verification gates

#### Wave 1

- server integration tests stay green
- `COMMAND INFO`, `HELLO`, `INFO`, and `STATS` output remains compatible
- backpressure, close-after-reply, and fair scheduling behavior remains compatible

#### Wave 2

- DB and runtime command regression suites stay green
- `YierdisDb` shrinks materially
- `DbReads` and `DbWrites` stop being forwarding shells

#### Wave 3

- protocol modules no longer depend on `core-contract`
- client, bench, and server protocol tests stay green
- server write path keeps only one semantic source of truth

## Delivery Strategy

- Commits should be boundary-scoped, not file-scoped.
- Each wave should follow:
  1. add or tighten guard tests
  2. introduce new seam or collaborator
  3. migrate callers
  4. remove compatibility layer
- Do not mix behavior changes with boundary movement in the same commit.
- Before deleting a compatibility layer, add a guard test that would fail if it reappears.

## Out of Scope

- changing protocol format
- changing command semantics
- redesigning user-visible CLI behavior
- introducing new product features
- rewriting the executor scheduling model beyond what is needed to stabilize ownership boundaries

## Success Criteria

The remediation is complete when:

- server connection ownership has a single state root
- server-only command metadata is self-owned by server registrations
- off-heap contracts have one SSOT API family
- `YierdisDb` is no longer a monolithic command-shaped implementation surface
- protocol decode no longer produces execution-layer commands directly
- the server reply path has one semantic source of truth
- all focused regression and compatibility tests pass at the end of each wave
