## What you implemented

- Made `RespProtocolErrorReplyHandler` session-aware through injected `Session` lookup and closing-aware through injected closing-state callbacks, without importing `NettyExecutionConnection` into `yierdis-networking-netty`.
- Preserved protocol-error reply encoding through `writer.protocolError(...)`, so protocol errors still emit `-ERR ...` rather than RESP3 blob errors.
- Replaced handler-local drop behavior that only watched a private boolean with logic that also checks an injected closing-state signal.
- Wired `YierdisServerChannelInitializer` to provide the connection session, connection closing signal, and existing close-after-reply callback from server-main.
- Added unit and integration coverage for session-aware writer selection, closing-aware inbound dropping, and `HELLO 3` followed by malformed RESP still returning `-ERR Protocol error...` then closing.

## What you tested and test results

- Focused suite:
  - `RespProtocolErrorReplyHandlerTest`
  - `RespProtocolErrorIntegrationTest`
  - `ClosingSkipSideEffectsIntegrationTest`
- Result: PASS
- Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am -Dtest=RespProtocolErrorReplyHandlerTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- Final output summary:

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## TDD evidence with RED and GREEN commands/output

### RED

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am -Dtest=RespProtocolErrorReplyHandlerTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Observed failure:

```text
no suitable constructor found for RespProtocolErrorReplyHandler(...)
```

This confirmed the new tests were asking for injected session/closing hooks that the handler did not yet provide.

### GREEN

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am -Dtest=RespProtocolErrorReplyHandlerTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Observed success:

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Files changed

- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`

## Self-review findings

- The handler remains transport-module-safe: no `server-main` dependency was introduced into `yierdis-networking-netty`.
- The public constructor surface is minimal: server wiring gets injected callbacks, while the extra dropped-message hook stays package-private for testability.
- RESP3 protocol errors intentionally remain `-ERR ...`; tests explicitly lock that in.
- The new `HELLO 3` integration test only asserts the required invariant: protocol error format stays `-ERR ...` and the connection closes.

## Any issues or concerns

- No functional concerns after the focused suite passed.
