# Redis Protocol Compatibility Design

## Goal

Replace Yierdis Custom Protocol v1 with Redis wire protocol compatibility as the only public server protocol.

The initial compatibility target is RESP2 for `redis-cli`, Jedis, Lettuce, and go-redis. RESP3 is supported through `HELLO 3` negotiation and basic reply types, without attempting full Redis server feature parity.

References:

- Redis protocol specification: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Redis `HELLO` command: https://redis.io/docs/latest/commands/hello/

## Non-Goals

- Do not keep Custom Protocol v1 as a fallback protocol.
- Do not implement Redis Cluster, Pub/Sub, replication, ACL, TLS, Lua, or persistence as part of this protocol change.
- Do not change command storage semantics unless Redis client compatibility requires a small connection or metadata command.
- Do not make RESP3 the default client protocol. Default remains RESP2 until the client sends `HELLO 3`.

## Current State

The server currently exposes Custom Protocol v1:

- Request framing is `<len>:<json-payload>\n`.
- Replies are NDJSON.
- Server pipeline is assembled in `YierdisServerChannelInitializer`.
- Request decoding lives in `CustomRequestDecoder`.
- Request-to-execution adaptation lives in `ProtocolCommandAdapter`.
- Reply encoding lives in `JsonLineReplyWriter`.

The command executor and command engine already have useful protocol-agnostic boundaries:

- `ExecutionRequest` models argv-style commands.
- `ReplyWriter` models reply shapes independent of wire encoding.
- `CommandExecutor` already supports queue capacity, queued bytes, per-connection pending counts, and input backpressure.
- `NettyExecutionIoAdapter` already writes buffered replies and closes the connection when a reply requests it.

The design should preserve those boundaries and replace only the protocol lane and server wiring that depend on Custom Protocol v1.

## Protocol Scope

### RESP2 Requests

The decoder must support the Redis command request shape most clients use:

```text
*<argc>\r\n
$<len>\r\n
<bytes>\r\n
...
```

Each bulk string becomes one binary-safe argv element. Empty bulk strings are valid. Null bulk strings are rejected for command argv because Redis commands require concrete arguments.

The decoder should also support inline commands such as:

```text
PING\r\n
SET a 1\r\n
```

Inline command support is mainly for `redis-cli` and manual debugging. It can use the existing CLI-style argument splitting behavior as guidance, but the network protocol implementation should not depend on the Custom Protocol v1 client.

### RESP3 Requests

RESP3 clients still send commands as arrays of bulk strings in normal operation. The decoder can parse the same request representation for both negotiated protocol versions.

The first connection protocol version is RESP2. `HELLO 3` changes only reply encoding for subsequent replies on that connection. `HELLO 2` switches the connection back to RESP2.

### Pipeline

Pipeline support is a natural consequence of incremental decoding: a single inbound `ByteBuf` can emit multiple `ExecutionRequest` objects. Each request must produce exactly one reply, in input order, unless the connection is closed due to protocol or resource protection.

The existing executor scheduling and per-connection queue state must continue to preserve reply ordering per connection.

## Connection State

Add Redis protocol state to `ServerSession` and implement it in `EngineSession`:

- `respVersion`: default `2`, allowed values `2` and `3`.
- `clientName`: already exists on `ServerSession`; use it for `CLIENT SETNAME` and `CLIENT GETNAME`.
- `authenticated`: already exists but there is no configured password in this change.

The reply writer must be created with the current connection protocol state, not as a process-wide constant. Extend `ReplyWriterFactory` with a default method:

```java
default ReplyWriter newWriter(ServerSession session, BytesSink out) {
    return newWriter(out);
}
```

Then `CommandExecutorExecutionSupport` creates writers with `replyWriterFactory.newWriter(connection.session(), sink)`. Existing test writers and temporary adapters keep working through the default method, while `RespReplyWriterFactory` uses `session.respVersion()` to choose RESP2 or RESP3 encoding.

## Command Compatibility

Existing commands that already support client compatibility:

- `PING`
- `ECHO`
- `COMMAND`
- `SELECT`
- `QUIT`
- `INFO`
- `SET` / `GET` and the existing data-structure command subset
- `MULTI` / `EXEC` / `DISCARD`

Commands to adjust or add:

- `HELLO [2|3] [AUTH ...] [SETNAME name]`
  - `HELLO 2` and `HELLO 3` must set the connection protocol state before writing the reply.
  - Reply should include `server`, `version`, `proto`, `mode`, and `role`.
  - `AUTH` options return an authentication error until password support exists.
  - `SETNAME` stores `clientName`.
- `CLIENT SETINFO ...`
  - Return `OK`. Modern clients may send this automatically.
- `CLIENT SETNAME <name>`
  - Store `clientName` and return `OK`.
- `CLIENT GETNAME`
  - Return null when no name is set.
- `AUTH`
  - Return Redis-style error because no password is configured.

Unknown commands and arity errors should remain command-layer behavior, but the RESP writer must encode them as Redis error replies.

## Reply Encoding

Introduce a RESP reply writer that implements `ReplyWriter`.

### RESP2 Encoding

- Simple string: `+value\r\n`
- Error: `-ERR ...\r\n` unless the message already starts with a Redis error prefix such as `ERR`, `WRONGTYPE`, `EXECABORT`, `OOM`, or `NOAUTH`.
- Integer: `:<value>\r\n`
- Bulk string: `$<len>\r\n<bytes>\r\n`
- Null value: `$-1\r\n`
- Null array: `*-1\r\n`
- Array: `*<count>\r\n`
- Map: encode as an array with `pairs * 2` elements.
- Set: encode as an array.
- Boolean: encode as integer `1` or `0`.
- Double and big number: encode as bulk strings unless command compatibility requires a different Redis2 shape.
- Verbatim string: encode as bulk string.
- Blob error: encode as an error.
- Push and attributes: encode as arrays or ignore attributes in RESP2-compatible paths.

### RESP3 Encoding

- Simple string: `+value\r\n`
- Blob string: `$<len>\r\n<bytes>\r\n`
- Error: `-ERR ...\r\n`
- Integer: `:<value>\r\n`
- Null: `_\r\n`
- Array: `*<count>\r\n`
- Map: `%<pairs>\r\n`
- Set: `~<count>\r\n`
- Boolean: `#t\r\n` or `#f\r\n`
- Double: `,<value>\r\n`
- Big number: `(<value>\r\n`
- Verbatim string: `=<len>\r\n<format>:<bytes>\r\n`
- Blob error: `!<len>\r\n<message>\r\n`
- Push: `><count>\r\n`
- Attribute: `|<pairs>\r\n`

Nested aggregate handling must preserve the existing `ReplyWriter` streaming style so command handlers do not need to construct full reply object graphs.

## Error Behavior

Protocol decode errors should be close to Redis behavior:

- Unknown leading byte: write `-ERR Protocol error: invalid multibulk length\r\n` or a more specific protocol error, then close.
- Array length too large: write `-ERR Protocol error: invalid multibulk length\r\n`, then close.
- Bulk length too large: write `-ERR Protocol error: invalid bulk length\r\n`, then close.
- Missing CRLF or malformed integer: write protocol error and close.
- Too many arguments: write `-ERR Protocol error: too many arguments\r\n`, then close.

For safety, malformed RESP frames should close the connection after the error reply. RESP does not have the same reliable resync point as the current length-prefixed Custom Protocol v1.

Command execution errors should keep the connection open unless the command explicitly requests close.

## Backpressure And Slow Clients

Keep the existing executor backpressure model:

- global queue capacity
- global queued byte budget
- per-connection pending command high/low watermarks
- per-connection pending byte high/low watermarks
- `autoRead(false)` when the server is under pressure
- channel writability callbacks to pause/resume input

Add network-level protection:

- configure Netty `WRITE_BUFFER_WATER_MARK` from existing or new config values
- add idle read timeout configuration for inactive clients
- enforce decoder cumulative buffer limits
- close slow clients whose outbound buffer remains above the high watermark too long

Rejected submissions should use Redis-style errors. The existing `ERR busy <reason>` can remain initially, but the writer must encode it as `-ERR busy <reason>\r\n`.

## Module And File Ownership

Create a RESP protocol lane under networking:

- `yierdis-networking/yierdis-networking-resp`
  - RESP request model
  - RESP parser helpers
  - RESP encoder helpers
  - RESP reply writer
  - unit tests

Create or update Netty integration under networking:

- `yierdis-networking/yierdis-networking-netty`
  - `RespRequestDecoder`
  - `RespCommandAdapter`
  - `RespProtocolErrorReplyHandler`
  - decoder integration tests

Update server wiring:

- `YierdisServerChannelInitializer`
- `YierdisServerBootstrap`
- `YierdisFastCommandHandler` if connection-aware reply factory plumbing requires it
- `NettyExecutionConnection` for RESP version state if state is stored there
- `ServerCommandModule` for `HELLO`
- connection command module for `CLIENT` and `AUTH`

Remove Custom Protocol v1 modules and references:

- `yierdis-networking-custom-v1`
- `yierdis-networking-custom-v1-execution`
- Custom Protocol v1 Netty handlers
- custom protocol architecture tests, replacing them with RESP boundary tests
- custom protocol CLI and benchmark assumptions; the project CLI and benchmark should be rewritten to speak RESP2 rather than removed
- README and docs references that say Yierdis is not RESP-compatible

## Testing Strategy

Unit tests:

- RESP2 request decoder parses arrays, bulk strings, empty strings, partial frames, inline commands, pipelined commands, malformed frames, and limits.
- RESP writer emits exact bytes for RESP2 and RESP3 scalar and aggregate replies.
- `HELLO 2` and `HELLO 3` change the connection protocol state.
- `CLIENT SETINFO`, `CLIENT SETNAME`, and `CLIENT GETNAME` return Redis-compatible replies.

Embedded Netty tests:

- Pipeline multiple commands in one buffer and assert ordered replies.
- Backpressure disables and re-enables input.
- Protocol error writes an error and closes the connection.
- `QUIT` writes `OK` before closing.

Client compatibility smoke tests:

- `redis-cli -p <port> PING`
- `redis-cli -p <port> SET k v`
- `redis-cli -p <port> GET k`
- Jedis: connect, ping, set, get, pipeline.
- Lettuce: connect, ping, set, get, pipeline.
- go-redis: connect with RESP2, ping, set, get, pipeline.
- RESP3 smoke path: `redis-cli -3 HELLO 3`, then `PING`, `SET`, `GET`.

Regression tests:

- existing command integration tests continue to pass with RESP reply writer test utilities adjusted.
- architecture tests enforce no remaining production dependency on `protocol.custom.v1`.

## Rollout Order

1. Add RESP2 decoder and writer with unit tests.
2. Add connection-aware reply writer plumbing.
3. Replace server pipeline with RESP handlers.
4. Implement `HELLO` protocol switching and client metadata commands.
5. Add Redis client smoke tests.
6. Remove Custom Protocol v1 modules, docs, CLI assumptions, and architecture guards.
7. Add RESP3 reply types and RESP3 smoke tests.
8. Update user-facing documentation to describe Redis protocol compatibility as the public contract.

## Decisions

- Keep the project-local CLI, but rewrite it as a RESP2 client that uses the existing inline input parser.
- Add explicit server flags for idle timeout and output-buffer slow-client protection:
  - `--client-idle-timeout-millis`, default `300000`.
  - `--client-output-buffer-limit-bytes`, default `67108864`.
  - `--client-output-buffer-over-limit-millis`, default `10000`.
- Keep `AUTH` as a Redis-style fixed error in this change. Password and ACL support belong in a separate feature.
