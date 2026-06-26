# RESP Protocol Error Layering Design

## Status

Approved design for tightening RESP protocol error handling so protocol errors
stay entirely inside the protocol layer and never enter the command layer.

## Context

The current RESP server path already has the right building blocks:

- `RespRequestDecoder` parses RESP bytes and emits either `RespCommandRequest`
  or `RespProtocolError`.
- `RespProtocolErrorReplyHandler` can convert `RespProtocolError` into a RESP
  error reply and close the connection.
- `RespCommandAdapter` converts `RespCommandRequest` into
  transport-neutral `ExecutionRequest`.
- `YierdisFastCommandHandler` submits command requests to the executor and owns
  command-layer error replies.

The remaining problem is boundary drift. The server pipeline currently places
`RespCommandAdapter` before `RespProtocolErrorReplyHandler`, and
`YierdisFastCommandHandler.exceptionCaught(...)` still contains a defensive
protocol-error fallback. That makes the architecture less explicit than the
intended design:

- protocol errors should be closed over at the RESP/Netty boundary;
- command handlers should only see execution requests and execution-time
  failures;
- connection closing semantics should be shared, but protocol error reply
  generation should not be duplicated across layers.

There is also a smaller cleanup issue in `RespRequestDecoder`: the 4-argument
constructor and `safeDiscardBytes(...)` suggest configurable discard behavior,
but the extra parameter is not used. That creates a false configuration point
and makes the decoder API noisier than necessary.

## Goals

- Make RESP protocol errors a protocol-layer-only concern.
- Ensure malformed RESP is replied to exactly once by
  `RespProtocolErrorReplyHandler`.
- Ensure protocol errors do not reach `RespCommandAdapter` or
  `YierdisFastCommandHandler`.
- Keep connection-level `closing` semantics intact so already-queued commands
  are skipped after protocol error close begins.
- Remove the unused 4-argument `RespRequestDecoder` constructor and related dead
  code.
- Update tests and docs so the layering rule is explicit and enforced.

## Non-Goals

- Do not redesign command execution, backpressure, or reply writer semantics.
- Do not introduce a new protocol exception hierarchy.
- Do not change the public malformed-RESP behavior of "reply with protocol error
  then close".
- Do not broaden RESP feature support or command parsing behavior.
- Do not refactor unrelated pipeline handlers or executor internals.

## Design Summary

The server should expose one clear main path for RESP input:

1. `RespRequestDecoder`
2. `RespProtocolErrorReplyHandler`
3. `RespCommandAdapter`
4. `YierdisFastCommandHandler`

Only `RespCommandRequest` should proceed into the adapter and command layer.
Only `RespProtocolError` should be consumed by the protocol error reply
handler. Any exception that reaches `YierdisFastCommandHandler` is treated as an
internal/server failure, never as a protocol error.

## Pipeline Responsibilities

### `RespRequestDecoder`

`RespRequestDecoder` remains responsible for:

- parsing RESP arrays and inline commands;
- enforcing protocol limits such as bulk size, argument count, and line size;
- emitting `RespProtocolError` for malformed or over-limit RESP input;
- entering decoder-local `CLOSING` state after a close-after-reply protocol
  error so remaining inbound bytes are discarded.

`RespRequestDecoder` should not:

- write replies;
- mark executor state directly;
- translate requests into execution-layer types;
- expose unused discard configuration knobs.

The decoder API should be simplified to a single public constructor:

```java
new RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes)
```

The unused 4-argument constructor and `safeDiscardBytes(...)` helper should be
removed.

### `RespProtocolErrorReplyHandler`

`RespProtocolErrorReplyHandler` becomes the only protocol error reply point in
the normal malformed-RESP path.

It remains responsible for:

- intercepting `RespProtocolError`;
- encoding `writer.protocolError(...)` through the shared reply writer;
- disabling `autoRead` once close-after-reply begins;
- notifying the connection-level close observer so connection stats and
  transaction cleanup enter `closing`;
- closing the channel after the reply is flushed when requested;
- dropping any later inbound messages after the handler has entered its own
  `closing` state.

It should not:

- convert normal command requests;
- infer command semantics;
- depend on command-layer classes.

### `RespCommandAdapter`

`RespCommandAdapter` should now sit strictly after the protocol error reply
handler. Its job stays narrow:

- accept `RespCommandRequest`;
- convert it to `ExecutionRequest`;
- forward the execution request downstream.

It should never see `RespProtocolError` in the normal pipeline.

### `YierdisFastCommandHandler`

`YierdisFastCommandHandler` remains responsible for:

- submitting `ExecutionRequest` objects to the executor;
- replying with `ERR busy ...` on submit rejection except
  `CONNECTION_CLOSING`;
- replying with `ERR internal error` and closing on handler/runtime failures.

It should stop trying to recognize protocol errors from exception messages or
decoder wrapper types. `exceptionCaught(...)` must treat every exception that
reaches it as an internal/server error path. If a protocol failure ever reaches
this handler, that is now considered a layering bug rather than an alternate
supported flow.

## Closing Semantics

Protocol-layer close behavior and executor-layer close behavior should remain
compatible:

- `RespProtocolErrorReplyHandler` invokes the close observer.
- The observer calls `NettyExecutionConnection.markClosing()`.
- `markClosing()` flips connection `closing` state and discards transaction
  state.
- `CommandExecutorSubmitter` rejects any future submissions with
  `CONNECTION_CLOSING`.
- `CommandExecutorExecutionSupport.execute(...)` skips already-queued commands
  when it observes `context.isClosing()`.

This preserves the existing guarantee that malformed RESP cannot be followed by
later command side effects or out-of-order replies.

## Error Handling Rules

### Protocol Errors

Examples:

- invalid multibulk length
- invalid bulk length
- invalid bulk terminator
- invalid inline command
- request line or header over configured limits

Required behavior:

- decoder emits `RespProtocolError(..., true)`;
- decoder enters `CLOSING` and discards remaining bytes;
- protocol handler writes one `-ERR Protocol error ...` reply;
- connection enters `closing`;
- channel closes after the reply flushes.

### Internal Errors

Examples:

- command handler runtime exceptions
- executor-time failures while processing an `ExecutionRequest`
- unexpected downstream handler failures after decode

Required behavior:

- command layer writes `-ERR internal error`;
- connection enters `closing`;
- channel closes after the reply flushes;
- queued commands are skipped through the existing executor closing checks.

No command-layer branch should attempt to reinterpret an internal exception as a
protocol error.

## Testing Strategy

### Decoder Tests

Update `RespRequestDecoderTest` to keep coverage for:

- array decode success;
- inline decode success;
- argument and bulk-size limit failures emitting `RespProtocolError`;
- malformed RESP dropping pipelined commands in the same read;
- adapter conversion of valid requests.

Remove any assumptions tied to the deleted 4-argument constructor.

### Protocol Error Handler Tests

Keep and extend `RespProtocolErrorReplyHandlerTest` to prove:

- `RespProtocolError` is intercepted before downstream handlers;
- close-after-reply triggers the close observer;
- the outbound reply is a protocol error reply;
- the channel closes after the reply;
- later inbound messages are dropped once the handler is closing.

### Server / Integration Tests

Keep and adjust integration coverage so it proves:

- malformed RESP still returns one protocol error reply then closes;
- malformed RESP in the same packet drops later pipelined commands;
- connection `closing` is marked for protocol error close paths;
- internal command/runtime errors still return internal error replies, not
  protocol errors;
- already-queued commands are skipped after closing begins.

The existing tests in `RespProtocolErrorIntegrationTest` and
`ClosingSkipSideEffectsIntegrationTest` should continue to cover these
guarantees after pipeline reordering and fallback removal.

## Documentation Changes

Update the server flow docs to make the layering rule explicit:

- protocol errors are handled in the protocol layer;
- `RespProtocolErrorReplyHandler` is the unified protocol error reply point;
- `YierdisFastCommandHandler` no longer owns a protocol error fallback path;
- command parsing/execution docs should describe protocol errors as staying
  outside the command layer.

The key document updates are expected in:

- `docs/project-docs/request-execution-flow.md`
- `docs/project-docs/configuration-and-operations.md`
- `docs/project-docs/command-parsing-and-dispatch.md`

## File-Level Change Scope

Primary implementation files:

- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`

Primary test files:

- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`

## Risks And Mitigations

- Risk: pipeline reordering changes which handler observes a message first.
  Mitigation: add focused pipeline tests that prove `RespProtocolError` does not
  reach downstream command handlers.
- Risk: removing the command-layer protocol fallback could hide a previously
  tolerated malformed path.
  Mitigation: keep integration coverage for malformed RESP and treat any
  protocol exception reaching the command layer as a test failure or internal
  error.
- Risk: removing the unused constructor could break hidden callers.
  Mitigation: repository-wide search currently shows no production callers of
  the 4-argument constructor.

## Acceptance Criteria

- The main server pipeline orders protocol error handling before command
  adaptation.
- `RespRequestDecoder` exposes only the supported 3-argument constructor.
- `YierdisFastCommandHandler` no longer contains protocol-error-specific reply
  behavior.
- Malformed RESP still produces one protocol error reply and closes the
  connection.
- Protocol errors do not enter the command layer in the normal server path.
- Closing behavior still prevents queued commands from producing later side
  effects or extra replies after protocol error close begins.
