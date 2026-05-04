# Architecture Boundary Hardening Design

> This spec supersedes the older architecture guidance where it overlaps. It keeps the current module split, but hardens the boundaries so they are enforced by code and build rules instead of mostly by convention.

## Summary

Yierdis already has a sensible macro split:

- protocol decodes and adapts wire input;
- executor-core schedules and backpressures;
- engine executes commands;
- runtime owns instance lifecycle;
- storage owns data structures.

The problem is that several critical seams are still enforced informally. The codebase relies on optional casts, ThreadLocal change tracking, handwritten dependency scans, and a very large storage facade to hold the architecture together. That makes the design readable in docs but fragile in code.

This spec hardens the architecture in one pass. The goal is not to invent a new layered system. The goal is to make the existing one real:

- dependencies are enforced with actual build-time rules;
- session state is explicit instead of marker-based;
- change tracking is explicit instead of hidden in ThreadLocal side effects;
- server bootstrap stays the only composition root;
- `YierdisDb` becomes a facade rather than a permanent object graph owner.

## Problem Statement

The current architecture has five related problems:

1. Module boundaries are guarded mainly by text-scanning tests and YAML policy files.
2. `Session` is a marker interface, so command code must downcast to optional capabilities.
3. Command-level change emission depends on `YierdisChangeTracking`, a hidden ThreadLocal channel shared between command and DB layers.
4. `YierdisDb` still owns too many internal subsystems directly, even if the implementation has been partially decomposed.
5. Server/runtime composition is still easy to reintroduce in the wrong layer because the current interfaces do not make illegal ownership impossible.

That combination creates a false sense of separation. The package layout looks clean, but the actual enforcement is too weak for a long-lived codebase.

## Goals

- Make architecture boundaries enforceable, not advisory.
- Replace marker-session + optional casts with an explicit engine-owned session contract.
- Replace implicit change emission with explicit mutation results or event collection.
- Keep `YierdisServerBootstrap` as the only application composition root.
- Keep `YierdisDb` focused on storage behavior, not object graph orchestration.
- Preserve existing user-visible command behavior.

## Non-Goals

- No protocol redesign.
- No persistence, replication, clustering, ACL, or Pub/Sub work.
- No full module collapse.
- No rewrite of all storage data structures.
- No attempt to remove Netty.
- No semantic change to command responses unless a later implementation task explicitly does so.

## Current State

The current code already shows the intended direction:

- `CommandExecutor` only needs `Session + ExecutionRequest + ReplyWriter`.
- `DefaultYierdisEngine` already wraps command execution.
- `NettyExecutionConnection` already holds transport connection state plus an `EngineSession`.
- `YierdisDbComponentFactory` already assembles the storage graph.

But these seams are still too loose:

- `CommandContext` exposes `serverSessionOrNull()`, `dbIndexProviderOrNull()`, and `connectionStatsOrNull()` through runtime type checks.
- `YierdisFastCommandProcessor` performs transaction handling, parsing dispatch, error mapping, and change-event emission in one method.
- `YierdisChangeTracking` uses a ThreadLocal scope that DB code and command code must both remember to respect.
- `YierdisDb` still exposes many internal fields and retains a lot of orchestration responsibility.
- Architecture tests mostly search for forbidden strings rather than enforcing relationships structurally.

## Considered Approaches

### Approach A: Tighten Tests Only

Keep the code structure as-is and add stricter scanning tests.

Pros:
- low risk;
- easy to land.

Cons:
- still relies on convention;
- does not remove optional runtime casts or ThreadLocal coupling;
- makes the architecture look stronger than it is.

### Approach B: Boundary Hardening With Minimal Surface Change

Keep the module split, but make the critical contracts explicit and enforce them:

- hard dependency rules at build time;
- engine-owned session contract;
- explicit mutation/change result;
- storage facade contraction;
- server composition root only.

Pros:
- fixes the root causes;
- keeps scope manageable;
- preserves current behavior and module names.

Cons:
- touches several layers at once;
- requires careful migration of tests and wiring.

### Approach C: Deep Module Rebuild

Rename and merge modules around a new kernel/runtime/storage split.

Pros:
- can produce a cleaner package graph.

Cons:
- too large for the current codebase state;
- high risk of churn without better guarantees than Approach B.

## Decision

Choose Approach B.

The architecture will stay recognizable, but each critical boundary becomes explicit and enforceable.

## Target Architecture

### 1. Build-Time Boundary Enforcement

The current `architecture-policy.yml` and string-scanning tests stay as a backstop, but they are no longer the primary guard.

The build must enforce module ownership with a real dependency rule system. An `yierdis-architecture-tests` rule set built on ArchUnit or an equivalent dependency-graph checker is preferred because the repo already has a dedicated architecture test module. The policy should cover at least:

- command-api / command-kernel / command-defaults;
- executor-core;
- runtime-api / runtime-embedded;
- storage-api / storage-memory / storage-testkit;
- protocol modules;
- server-app as the composition root.

The rule set must prevent:

- command modules from importing protocol, runtime, server, or storage internals;
- storage-memory from importing command, protocol, executor, or Netty;
- runtime-embedded from importing command or protocol internals;
- executor-core from importing command implementation types or DB implementation types;
- server-app from depending on protocol reply model internals or storage internals outside the public API.

String-scanning tests remain only as regression guards for a few edge cases.

### 2. Explicit Engine Session Contract

`Session` should stop being a blank marker that requires optional casts everywhere.

The target shape is:

- `Session` remains transport-neutral and is used only at the executor/engine boundary;
- `ServerSession` becomes the explicit server-side session contract;
- `CommandContext` carries a required `ServerSession`, not a generic `Session` plus optional accessors;
- the engine owns the concrete implementation and performs any needed adaptation once at the boundary;
- command code depends on declared methods, not on `instanceof` checks.

If a caller cannot supply a server-capable session, the boundary must fail fast before command modules run. Silent fallback to a weaker context is not acceptable for the production command path.

Command code that needs DB selection, transaction state, client metadata, or connection stats should get them from an explicit server-session contract, not by probing a generic marker.

The minimal contract should look like this:

```java
public interface ServerSession extends Session {
    int dbIndex();
    void setDbIndex(int dbIndex);
    long clientId();
    String clientName();
    void setClientName(String clientName);
    boolean authenticated();
    void setAuthenticated(boolean authenticated);
    TransactionState transaction();
    ConnectionStatsView connectionStats();
}
```

`CommandContext` should expose that server session directly, with no `serverSessionOrNull()`, `dbIndexProviderOrNull()`, or `connectionStatsOrNull()` fallback path in production command code.

### 3. Explicit Change Reporting

`YierdisChangeTracking` is replaced by a direct mutation reporting path.

The target is not just “different plumbing”. The target is a different ownership model:

- DB write paths return whether they changed value or TTL metadata;
- command execution receives that result explicitly;
- change events are emitted from the command layer without hidden thread-local state;
- maintenance and cleanup code do not need to fake or suppress command-scope state.

This removes the hidden cross-layer contract between command code and storage code.

The write-path contract should be represented by a small explicit result object or equivalent flag set, for example:

```java
public record MutationOutcome(boolean valueChanged, boolean ttlChanged) {
}
```

Existing storage-specific result types may remain, but they must surface the same facts so the command layer can emit `ExecutionRecord` / `YierdisChangeEvent` without consulting `YierdisChangeTracking`.

### 4. Storage Facade Contraction

`YierdisDb` must keep the public engine-facing behavior, but it should not remain the owner of every internal detail.

The internal graph should stay behind:

- `YierdisDbComponentFactory`;
- `YierdisDbStorageComponents`;
- `YierdisDbComponents`;
- focused helper classes for memory, lifecycle, expiration, maxmemory, and key identity.

`YierdisDb` should expose:

- public `DbReads`, `DbWrites`, `ExpirationManager`, `MemoryOps`, and `DbLifecycleOps`;
- thread binding and shutdown hooks required by `RuntimeDbEngine`;
- a small number of helper methods that are clearly storage facade methods.

It should not be the place where construction policy, mutation accounting, or storage subgraph assembly keeps growing.

### 5. Single Composition Root

`YierdisServerBootstrap` stays the only place where the full application is assembled.

It may:

- choose runtime config;
- create `YierdisInstance`;
- wire executor, engine, protocol adapters, and server handlers;
- start maintenance.

It must not:

- create ad hoc command parsers outside command modules;
- recreate runtime or storage ownership decisions;
- leak server-only concerns back into runtime or storage modules.

## Data Flow

The happy path should read like this:

1. Netty decodes protocol bytes.
2. Protocol adapters create `ExecutionRequest`.
3. `CommandExecutor` schedules the request on the owner thread.
4. `YierdisEngine.execute(session, request, out)` handles command semantics.
5. Command modules parse and execute against the DB API.
6. DB write methods report explicit mutation results.
7. The engine emits change events only when the result says the request changed durable state.
8. The executor writes the reply and manages backpressure only.

The unhappy path should also stay linear:

- protocol errors stay in the protocol layer;
- command syntax errors stay in command parsing;
- wrong-type / command / OOM errors stay as command-layer replies;
- transport close and backpressure remain executor concerns.

## Error Handling

The target error model is:

- protocol layer: malformed wire input;
- command layer: invalid command, arity, wrong type, transaction policy, or DB-level command exceptions;
- executor layer: queue saturation, transport backpressure, connection close handling;
- runtime layer: lifecycle and owner-thread violations.

No layer should translate every other layer’s errors by poking into implementation details.

## Testing Strategy

The spec requires three kinds of tests:

1. Dependency boundary tests that enforce module ownership with real tooling.
2. Behavioral tests that prove command execution still works through `ExecutionRequest`, `CommandContext`, and `YierdisEngine`.
3. Regression tests that prove storage change reporting and runtime lifecycle still behave correctly after removing implicit coupling.

The current architecture tests can remain, but they should be demoted from primary enforcement to secondary regression checks.

## Acceptance Criteria

This spec is done when all of the following are true:

- architecture rules are enforced by build/test tooling, not only text scans;
- command code no longer depends on optional casts from a marker session for core ownership decisions;
- change emission does not require hidden ThreadLocal coordination between command and storage layers;
- `YierdisServerBootstrap` remains the only place that wires protocol, runtime, command, executor, and transport together;
- `YierdisDb` no longer grows additional orchestration responsibilities;
- the existing command behavior and integration tests still pass.

## Open Questions

None required for the spec itself.

## Notes

This is intentionally a single cross-cutting architecture hardening spec. It is broad, but it is still one coherent change set because every issue is about the same thing: too many boundaries are enforced by convention instead of by explicit contracts.
