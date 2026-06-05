# Internal Legacy Compatibility Removal Design

## Status

Design approved in conversation on 2026-06-05.

## Goal

Remove Yierdis' internal historical compatibility surfaces while preserving Redis wire/client compatibility and current command behavior.

This is the first cleanup round. It removes old Java API aliases, deprecated overloads, and a CLI package wrapper that exists only to preserve project-internal names. It deliberately keeps `ServerSession` for a later focused session-capability cleanup.

## Problem

The codebase still exposes several names whose only purpose is to keep older internal or embedded callers compiling:

- `Command` is a deprecated alias over `ExecutionRequest`.
- `ReplyWriter` is a compatibility name over the explicit `RedisReplyWriter` reply model.
- `BytesView.len()` / `BytesView.byteAt()` and storage `KeyHandle.len()` / `KeyHandle.byteAt()` duplicate newer `length()` / `getByte()` APIs.
- `YierdisDb` and `YierdisInstanceConfig.Builder` accept deprecated `String maxmemoryPolicy` overloads even though internal runtime wiring already has `MaxmemoryPolicy`.
- `yierdis-cli` has an `InlineCommandParser` wrapper that only forwards to the shared RESP inline parser.

These surfaces make the architecture harder to read because there are two names for the same boundary. They also force architecture tests to distinguish "allowed compatibility alias" from the canonical model.

## Scope

Remove these internal compatibility surfaces:

- `yierdis-server-api` `Command` alias.
- `yierdis-server-api` `ReplyWriter` alias.
- `yierdis-server-api` `ReplyWriterFactory` as a factory name tied to the deleted alias.
- `BytesView.len()` and `BytesView.byteAt()`.
- `yierdis-db-api` `KeyHandle.len()` and `KeyHandle.byteAt()`, plus the matching memory implementation overrides.
- Deprecated `String maxmemoryPolicy` overloads in `YierdisDb` and `YierdisInstanceConfig.Builder`.
- `yierdis-cli` package-local `InlineCommandParser` wrapper.
- Docs and architecture guards that describe these surfaces as retained compatibility.

Replace them with canonical names:

- `ExecutionRequest` for command request models.
- `RedisReplyWriter` for command reply models.
- `RedisReplyWriterFactory` for factories that produce command reply writers.
- `length()` and `getByte()` for byte views.
- `MaxmemoryPolicy` for internal maxmemory policy wiring.
- `yier.bubu.redis.protocol.resp.InlineCommandParser` as the single inline parser implementation.

## Non-Goals

- Do not remove `ServerSession` in this round.
- Do not remove RESP2, RESP3 negotiation, `HELLO`, `CLIENT SETINFO`, `CLIENT SETNAME`, `CLIENT GETNAME`, `AUTH`, inline command support, Redis-style error strings, `SCAN` cursor wire shape, or Redis-style command replies.
- Do not remove external CLI/operator input normalization such as `maxmemoryScope` accepting `perdb` or `per_db`.
- Do not remove `MaxmemoryPolicy.parse(String)`; CLI and benchmark arguments still need string parsing at the external boundary.
- Do not change command semantics, storage semantics, transaction behavior, or maxmemory behavior.

## Design

### Execution request alias removal

Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java`.

All production and test references must use `ExecutionRequest` directly. Existing architecture guards that only forbid production use of `Command` should become stricter: the repository should not contain the deleted compatibility type, and no source should import `yier.bubu.redis.execution.api.Command`.

Update package documentation so `ExecutionRequest` is the only request model entry in the execution API audience list.

### Redis reply writer naming

Delete `ReplyWriter` and make `RedisReplyWriter` the only semantic reply writer interface.

Rename the server API factory from `ReplyWriterFactory` to `RedisReplyWriterFactory` so the factory name matches the canonical reply model. The factory methods should return `RedisReplyWriter`:

```java
public interface RedisReplyWriterFactory {
    RedisReplyWriter newWriter(BytesSink out);

    default RedisReplyWriter newWriter(Session session, BytesSink out) {
        return newWriter(out);
    }
}
```

`RespReplyWriter` should implement `RedisReplyWriter` directly. `RespReplyWriterFactory` should implement `RedisReplyWriterFactory`. Executor, engine, protocol adapter, command handler, and test writer imports should all move from `ReplyWriter` to `RedisReplyWriter`.

This change is intentionally name-only for behavior. RESP2/RESP3 encoding, close-after-reply signaling, protocol errors, and Redis error prefix normalization must remain unchanged.

### Byte view API cleanup

Remove the default alias methods from `BytesView`:

- `len()`
- `byteAt(int index)`

Callers should use:

- `length()`
- `getByte(int index)`

Remove `len()` and `byteAt(int)` from `yierdis-db-api` `KeyHandle` and the matching `yierdis-db-memory` internal `KeyHandle`. `KeyHandle` should rely on the inherited storage/public byte view methods and keep only key-specific behavior such as `dictHash()` and native-handle factory construction.

The cleanup must not touch `ExecutionRequest.len(int argIndex)` or `ExecutionRequest.byteAt(int argIndex, int byteIndex)`. Those methods are part of the request argv API and are not the historical byte-view aliases being removed.

### Maxmemory policy overload cleanup

Delete deprecated internal overloads that accept `String maxmemoryPolicy`:

- `YierdisDb.createWithSharedFfmRuntime(..., String maxmemoryPolicy, ...)`
- `YierdisDb.createWithOwnedFfmRuntime(..., String maxmemoryPolicy, ...)`
- `YierdisInstanceConfig.Builder.maxmemoryPolicy(String rawPolicy)`

Delete helper code that exists only to support those overloads, such as `compatibilityMaxmemoryPolicy(String)`.

Internal callers must pass `MaxmemoryPolicy` directly. External argument parsing remains at CLI/benchmark/server args boundaries and should continue to use `MaxmemoryPolicy.parse(String)` before building runtime config.

### CLI parser wrapper cleanup

Delete `yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java`.

`YierdisCli` should import the shared parser directly:

```java
import yier.bubu.redis.protocol.resp.InlineCommandParser;
```

The REPL and one-line command parsing behavior must stay the same. This removes only the forwarding wrapper, not inline command support.

### Architecture and documentation updates

Update architecture tests to assert the new steady state:

- `Command.java` does not exist.
- `ReplyWriter.java` does not exist.
- `RedisReplyWriterFactory` is the server API factory boundary.
- Command, executor, server, and protocol code do not import deleted compatibility types.
- `BytesView` does not contain `len()` or `byteAt()`.
- `yierdis-db-api` and `yierdis-db-memory` `KeyHandle` types do not contain `len()` or `byteAt()`.
- `YierdisDb` and `YierdisInstanceConfig.Builder` do not expose deprecated string maxmemory policy overloads.
- The CLI wrapper class does not exist.

Update project docs that currently call `ReplyWriter` or `Command` compatibility aliases. Documentation should describe `ExecutionRequest`, `RedisReplyWriter`, and `RedisReplyWriterFactory` as the canonical execution API.

Do not rewrite Redis protocol documentation to reduce Redis compatibility claims. That behavior remains supported.

## Compatibility Impact

This is a breaking Java API cleanup for old embedded/internal callers. It should not be a Redis protocol breaking change.

Expected behavior that must remain unchanged:

- RESP2 remains default.
- `HELLO 3` still negotiates RESP3 replies.
- Inline command input still works.
- Redis-style command errors remain stable.
- `CLIENT SETINFO`, `CLIENT SETNAME`, `CLIENT GETNAME`, and `AUTH` still provide minimal client handshake compatibility.
- CLI command parsing still follows the shared Redis `sdssplitargs`-style parser.

## Testing Strategy

Run at least:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-command test
mvn -pl yierdis-networking test
mvn -pl yierdis-cli test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

If API changes ripple into runtime/server integration tests, also run:

```bash
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-integration-tests test
```

Manual smoke is not required for this spec because no Redis wire behavior should change. If implementation touches protocol writer code beyond type renaming, run the existing RESP/redis-cli compatibility tests.

## Risks

- The `ReplyWriter` rename touches many files. Keep the implementation mechanical and behavior-neutral.
- Search-and-replace for `len()` / `byteAt()` can accidentally hit `ExecutionRequest`; review every replacement.
- Removing `String maxmemoryPolicy` overloads can break tests that intentionally exercise old constructors. Replace those tests with enum-based construction and architecture guards for the deleted overloads.
- Leaving docs with `ReplyWriter` compatibility language after deletion would make architecture docs misleading.
