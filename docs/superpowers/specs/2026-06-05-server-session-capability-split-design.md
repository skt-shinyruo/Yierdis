# Server Session Capability Split Design

## Status

Design approved in conversation on 2026-06-05 as the second cleanup round.

## Goal

Remove `ServerSession` as a compatibility aggregate and make command execution depend on explicit session capability interfaces.

This spec assumes the first cleanup round has removed the other internal compatibility aliases, especially `ReplyWriter` in favor of `RedisReplyWriter`. If this spec is implemented before that cleanup, keep the same session design but use the current reply writer type names until the first cleanup lands.

## Problem

`ServerSession` currently has no behavior of its own. It extends five narrower interfaces:

- `DbIndexSession`
- `ClientMetadataSession`
- `TransactionSession`
- `ConnectionStatsSession`
- `ProtocolNegotiationSession`

That aggregate made earlier migrations easier because code could pass one broad session type around. The narrower capability interfaces now exist, and architecture tests already require protocol writer code to depend on `ProtocolNegotiationSession` instead of the full `ServerSession`.

Keeping `ServerSession` has two costs:

- New code can depend on the whole connection state surface when it only needs one capability.
- Tests and helper APIs keep implementing a broad aggregate, which hides the intended dependency boundaries.

## Scope

Remove `ServerSession` and replace it with explicit capability dependencies:

- Delete `yierdis-server-api` `ServerSession`.
- Make `EngineSession` implement the five narrow capability interfaces directly.
- Remove `CommandSessionCapabilities.from(ServerSession)`.
- Remove `CommandContext` constructors and reset methods that accept `ServerSession`.
- Update test sessions and helpers to implement only the capabilities they actually need, or to build a `CommandSessionCapabilities` bundle explicitly.
- Update package docs and architecture guards to enforce absence of the aggregate interface.

## Non-Goals

- Do not change what connection state exists.
- Do not change `SELECT`, client metadata, auth, transactions, connection stats, or RESP negotiation behavior.
- Do not change Redis protocol compatibility.
- Do not introduce separate runtime objects for each capability unless a test helper benefits from it.
- Do not move session state ownership out of `EngineSession`.
- Do not change executor scheduling, transaction queue limits, or reply ordering.

## Design

### Delete the aggregate interface

Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java`.

Update `package-info.java` so it lists the five narrow capability interfaces as the session contract surface. Remove language that describes `ServerSession` as a compatibility aggregate.

`Session` remains as the transport-neutral marker/base interface. Concrete connection sessions can still implement multiple capability interfaces; they just do not advertise that through a broad aggregate type.

### EngineSession implementation

Change `EngineSession` from:

```java
public final class EngineSession implements ServerSession
```

to:

```java
public final class EngineSession implements
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession
```

The fields and methods stay behaviorally unchanged:

- `dbIndex`
- `clientId`
- `clientName`
- `authenticated`
- `respVersion`
- `transaction`
- `connectionStatsSupplier`

This preserves the current per-connection state owner while removing only the aggregate type.

### CommandSessionCapabilities as the command boundary

`CommandSessionCapabilities` should become the only full command-session bundle:

- Keep `from(Session session)` and its runtime check that the object implements all required capabilities.
- Keep `of(DbIndexSession, ClientMetadataSession, TransactionSession, ConnectionStatsSession, ProtocolNegotiationSession)`.
- Delete `from(ServerSession session)`.

When code has a concrete `Session`, it should call `CommandSessionCapabilities.from(session)`. When tests want a deliberately assembled bundle, they should call `CommandSessionCapabilities.of(...)`.

### CommandContext construction

Remove `CommandContext` overloads that accept `ServerSession`.

The command layer should construct contexts from `CommandSessionCapabilities`:

```java
new CommandContext(CommandSessionCapabilities.from(session), out)
```

or, in tests:

```java
new CommandContext(CommandSessionCapabilities.of(
        dbIndexSession,
        clientMetadataSession,
        transactionSession,
        connectionStatsSession,
        protocolNegotiationSession
), out)
```

This makes the required command session surface explicit at the context boundary.

### Targeted capability dependencies

Code that needs only one capability should depend only on that capability:

- RESP writer factory should continue to inspect/use `ProtocolNegotiationSession`.
- DB routing and `SELECT` should use `DbIndexSession`.
- `CLIENT`, `AUTH`, and `HELLO` handlers should use `ClientMetadataSession` and/or `ProtocolNegotiationSession`.
- Transaction queueing should use `TransactionSession`.
- `INFO` / `STATS` should use `ConnectionStatsSession`.

No production code should import a deleted aggregate replacement. The only broad object should be `CommandSessionCapabilities`, and it should be used at the command execution boundary where all capabilities are genuinely required.

### Test helper cleanup

Test helpers currently implement `ServerSession` because it is convenient. Replace them with one of two patterns:

1. A small `TestCommandSession` that implements the five narrow interfaces directly.
2. Separate minimal capability objects combined through `CommandSessionCapabilities.of(...)`.

Use the first pattern for integration-style helpers such as fast command clients where a single mutable connection state is useful. Use the second pattern for focused unit tests that only need one or two capabilities.

Do not keep a test-only `ServerSession` equivalent. That would recreate the aggregate under another name.

### Architecture guards

Update architecture tests to enforce the new steady state:

- `ServerSession.java` does not exist.
- No production or test source imports `yier.bubu.redis.execution.api.ServerSession`.
- `EngineSession` implements the five narrow capability interfaces directly.
- `CommandContext` does not expose `ServerSession` constructors or reset methods.
- `CommandSessionCapabilities` does not have `from(ServerSession)`.
- RESP writer factory does not depend on a broad session aggregate.

The existing guard that says "`ServerSession` should remain only as a compatibility aggregate" should be replaced with guards that prove the aggregate has been removed.

## Compatibility Impact

This is a Java API breaking change for internal/test/embedded callers that implement or accept `ServerSession`.

Runtime behavior must remain unchanged:

- Existing connections still have one session state object.
- DB index selection still lives on the connection session.
- Client metadata and auth state still live on the connection session.
- Transactions still live on the connection session.
- RESP version negotiation still lives on the connection session.
- Connection stats remain readable by INFO/STATS paths.

Redis wire/client compatibility is out of scope and must not change.

## Testing Strategy

Run at least:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-server/yierdis-server-core test
mvn -pl yierdis-command/yierdis-command-core test
mvn -pl yierdis-server/yierdis-server-runtime test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Because session state backs connection behavior, also run:

```bash
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-integration-tests test
```

No manual Redis smoke is required unless implementation touches protocol negotiation or Netty connection wiring beyond type changes.

## Risks

- Test churn can be larger than production churn because many tests currently implement `ServerSession` for convenience.
- Replacing `ServerSession` with a new broad test helper interface would preserve the same smell. Helpers should either implement the five real capabilities or assemble `CommandSessionCapabilities`.
- `CommandSessionCapabilities.from(Session)` fails at runtime if a concrete session misses one capability. Architecture and unit tests should cover `EngineSession` so missing capability implementation fails early.
- This cleanup should follow the first internal compatibility cleanup where possible, so reply writer renames and session-boundary changes do not interleave in the same implementation step.

