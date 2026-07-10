# RESP Protocol Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the RESP protocol ingress path so malformed or adversarial input cannot cause unbounded command allocation, protocol/error handling stays fully inside the protocol layer, and regression tests lock the behavior across RESP2 and RESP3.

**Architecture:** Keep the existing pipeline split of `RespRequestDecoder -> RespProtocolErrorReplyHandler -> RespCommandAdapter -> YierdisFastCommandHandler`, but strengthen each boundary. Add an explicit per-command byte limit to the decoder/config surface, convert array decoding into a stateful incremental parser that does not repeatedly re-copy partial requests, and make protocol-error replies and closing decisions derive from connection/session state instead of handler-local conventions.

**Tech Stack:** Java 25, Maven, Netty, JUnit 4, Picocli, existing `RespReplyWriterFactory`, existing Netty/EmbeddedChannel integration tests.

## Global Constraints

- Use `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH` for every Maven/test command.
- Preserve the pipeline order `RespRequestDecoder -> RespProtocolErrorReplyHandler -> RespCommandAdapter -> YierdisFastCommandHandler`.
- Keep malformed RESP behavior as “reply once, then close the connection”.
- Do not reintroduce command-layer RESP protocol error formatting; protocol errors must stay on the protocol layer path.
- Follow existing CLI/config naming style: protocol ingress flags use `--protocolMax...`.

---

## Scope Check

This work is one subsystem: RESP ingress hardening. It spans decoder limits, incremental parsing, protocol error reply semantics, and the tests/docs that define those contracts. The tasks are sequenced so each one yields a shippable tightening step without requiring a larger executor or command-kernel redesign.

## File Structure

Modify protocol implementation files:

- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolLimits.java`: add SSOT defaults/maxima for per-command cumulative bytes.
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`: add the new limit and replace the current restart-from-scratch array parsing with explicit incremental state.
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`: source reply encoding and closing behavior from the connection/session rather than a handler-local default-only writer path.

Modify server config/wiring files:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`: add the new CLI flag constant.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`: parse/copy/validate/export the new protocol limit.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`: carry the new runtime-config field.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`: pass the new limit into `RespRequestDecoder`.

Modify tests that lock protocol behavior:

- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`: add cumulative-byte-limit and fragmented-input regression tests.
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java`: add connection-aware RESP3 protocol error and connection-closing state tests.
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`: assert the decoder receives the new runtime limit.
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`: add end-to-end tests for total-command-byte enforcement and RESP3 protocol-error encoding.
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`: cover parsing, validation, copying, argv round-trip, and runtime-config export for the new limit.
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerConfigArgsTest.java`: cover CLI-to-runtime-config flow for the new limit.

Modify docs:

- `docs/project-docs/configuration-and-operations.md`
- `docs/project-docs/protocol-reference.md`
- `docs/project-docs/request-execution-flow.md`

### Task 1: Add A Per-Command Total Byte Limit

**Files:**
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolLimits.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerConfigArgsTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`

**Interfaces:**
- Consumes: `new RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxCommandBytes)` as the new server-side constructor shape for pipeline wiring.
- Produces: `YierdisServerRuntimeConfig.protocolMaxCommandBytes(): int` and `YierdisServerArgs.protocolMaxCommandBytes` as the config surface used by later tasks and docs.

- [ ] **Step 1: Write the failing decoder/unit/config tests**

Add these tests to `RespRequestDecoderTest.java`:

```java
    @Test
    public void emitsProtocolErrorWhenTotalCommandBytesExceedLimit() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 4));
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$3\r\nGET\r\n$2\r\nab\r\n",
                    StandardCharsets.US_ASCII
            )));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertEquals("ERR Protocol error: command is too large", ((RespProtocolError) msg).message());
            Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
```

Add this test to `YierdisServerArgsTest.java`:

```java
    @Test
    public void protocolCommandBytesParsesAndExportsToRuntimeConfig() {
        YierdisServerArgs args = parse("--protocolMaxCommandBytes", "1234");

        args.normalizeAndValidate();

        Assert.assertEquals(1234, args.protocolMaxCommandBytes);
        Assert.assertEquals(1234, args.copy().protocolMaxCommandBytes);
        Assert.assertTrue(args.toArgv().contains("--protocolMaxCommandBytes"));
        Assert.assertEquals(1234, args.toRuntimeConfig().protocolMaxCommandBytes());
    }
```

Add this test to `ServerConfigArgsTest.java`:

```java
    @Test
    public void protocolCommandBytesFlowsThroughServerConfig() {
        ServerConfig config = ServerConfig.fromArgs(new String[]{
                "--protocolMaxCommandBytes", "1234"
        });

        Assert.assertEquals(1234, config.runtimeConfig().protocolMaxCommandBytes());
    }
```

Add this assertion block to `YierdisServerBootstrapCommandWiringTest.channelInitializerUsesRuntimeConfigForSessionAndProtocolLimits()` after the existing decoder field assertions:

```java
                Assert.assertEquals(5, intField(decoder, "maxCommandBytes"));
```

Change the helper signature and call sites in that test from:

```java
    private static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxLineBytes
    )
```

to:

```java
    private static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxLineBytes,
            int protocolMaxCommandBytes
    )
```

and pass `protocolMaxCommandBytes` into the record constructor.

Add this integration test to `RespProtocolErrorIntegrationTest.java`:

```java
    @Test
    public void oversizedTotalCommandBytesReturnsProtocolErrorAndClosesConnection() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--protocolMaxCommandBytes", "4"
        );
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$3\r\nGET\r\n$2\r\nab\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String error = readLine(in);
            Assert.assertEquals("-ERR Protocol error: command is too large\r", error);
            Assert.assertEquals(-1, in.read());
        }
    }
```

- [ ] **Step 2: Run the targeted tests to verify the new limit is currently missing**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespRequestDecoderTest,YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,RespProtocolErrorIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `RespRequestDecoder` has no max-command-bytes field/constructor and the CLI/runtime config surface does not expose `protocolMaxCommandBytes`.

- [ ] **Step 3: Implement the config surface and decoder enforcement**

Update `RespProtocolLimits.java` to add:

```java
    public static final int DEFAULT_MAX_COMMAND_BYTES = 64 * 1024 * 1024;
    public static final int MAX_COMMAND_BYTES = DEFAULT_MAX_BULK_BYTES;
```

Update `YierdisServerArgNames.java` to add:

```java
    public static final String PROTOCOL_MAX_COMMAND_BYTES = "--protocolMaxCommandBytes";
```

Update `YierdisServerArgs.java`:

```java
    private static final int DEFAULT_PROTOCOL_MAX_COMMAND_BYTES = RespProtocolLimits.DEFAULT_MAX_COMMAND_BYTES;
```

```java
    @Option(
            names = YierdisServerArgNames.PROTOCOL_MAX_COMMAND_BYTES,
            defaultValue = "" + DEFAULT_PROTOCOL_MAX_COMMAND_BYTES,
            description = "Protocol max cumulative bytes per command."
    )
    public int protocolMaxCommandBytes = DEFAULT_PROTOCOL_MAX_COMMAND_BYTES;
```

Add validation beside the existing protocol limits:

```java
        if (protocolMaxCommandBytes <= 0) {
            throw new IllegalArgumentException("protocolMaxCommandBytes must be > 0");
        }
        if (protocolMaxCommandBytes > RespProtocolLimits.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException("protocolMaxCommandBytes must be <= " + RespProtocolLimits.MAX_COMMAND_BYTES);
        }
        if (protocolMaxCommandBytes < protocolMaxBulkBytes) {
            throw new IllegalArgumentException("protocolMaxCommandBytes must be >= protocolMaxBulkBytes");
        }
```

Copy/export the field in `copy()`, `toRuntimeConfig()`, and `toArgv()`:

```java
        out.protocolMaxCommandBytes = protocolMaxCommandBytes;
```

```java
                protocolMaxCommandBytes,
```

```java
        out.add(YierdisServerArgNames.PROTOCOL_MAX_COMMAND_BYTES);
        out.add(Integer.toString(protocolMaxCommandBytes));
```

Update `YierdisServerRuntimeConfig.java` record header so the protocol fields become:

```java
        int protocolMaxBulkBytes,
        int protocolMaxArgs,
        int protocolMaxLineBytes,
        int protocolMaxCommandBytes,
```

Update `YierdisServerChannelInitializer.java` to instantiate the decoder with the extra field:

```java
                .addLast("respRequestDecoder", new RespRequestDecoder(
                        config.protocolMaxBulkBytes(),
                        config.protocolMaxArgs(),
                        config.protocolMaxLineBytes(),
                        config.protocolMaxCommandBytes()
                ))
```

Update `RespRequestDecoder.java` field/constructor block:

```java
    private final int maxCommandBytes;
```

```java
    public RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxCommandBytes) {
        this.maxBulkBytes = Math.max(0, maxBulkBytes);
        this.maxArgs = Math.max(0, maxArgs);
        this.maxInlineBytes = Math.max(0, maxInlineBytes);
        this.maxCommandBytes = Math.max(0, maxCommandBytes);
    }
```

Then, inside array decoding, reject before allocating the next body when cumulative retained bytes would exceed the limit:

```java
            if (maxCommandBytes > 0 && retainedBytes > maxCommandBytes - len) {
                emitProtocolError(out, "ERR Protocol error: command is too large", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
```

For inline commands, reject after parsing but before emitting:

```java
            if (maxCommandBytes > 0 && decoded.retainedBytes() > maxCommandBytes) {
                emitProtocolError(out, "ERR Protocol error: command is too large", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
```

- [ ] **Step 4: Run the targeted tests to verify the new limit passes**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespRequestDecoderTest,YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,RespProtocolErrorIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS for all listed test classes.

- [ ] **Step 5: Commit the per-command limit**

```bash
git add \
  yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolLimits.java \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerConfigArgsTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java
git commit -m "feat: cap cumulative RESP command bytes"
```

### Task 2: Make RESP Array Decoding Truly Incremental

**Files:**
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`

**Interfaces:**
- Consumes: `RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxCommandBytes)` from Task 1.
- Produces: incremental decoder behavior that can accept fragmented array payloads without restarting from `commandStart` after every partial body.

- [ ] **Step 1: Write the failing fragmented-input regression tests**

Add these tests to `RespRequestDecoderTest.java`:

```java
    @Test
    public void decodesFragmentedArrayCommandAcrossMultipleReads() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 1024));
        try {
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$4\r\nPI", StandardCharsets.US_ASCII)));
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("NG\r\n$3\r\nhe", StandardCharsets.US_ASCII)));
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("y\r\n", StandardCharsets.US_ASCII)));

            RespCommandRequest req = ch.readInbound();
            Assert.assertEquals(2, req.argc());
            Assert.assertArrayEquals(bytes("PING"), req.readOnlyArg(0));
            Assert.assertArrayEquals(bytes("hey"), req.readOnlyArg(1));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void fragmentedOversizedCommandFailsOnceAndDropsRemainingInput() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024, 4));
        try {
            Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$3\r\nGET\r\n$2\r\n", StandardCharsets.US_ASCII)));
            Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("ab\r\n*1\r\n$4\r\nPING\r\n", StandardCharsets.US_ASCII)));

            Object msg = ch.readInbound();
            Assert.assertTrue(msg instanceof RespProtocolError);
            Assert.assertEquals("ERR Protocol error: command is too large", ((RespProtocolError) msg).message());
            Assert.assertNull(ch.readInbound());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
```

- [ ] **Step 2: Run the decoder test class to verify the current restart-from-scratch path is inadequate**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty -am \
  -Dtest=RespRequestDecoderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: at least one new test FAILS because the current decoder does not preserve partial array state across reads.

- [ ] **Step 3: Replace the array parser with explicit incremental state**

In `RespRequestDecoder.java`, replace the simple enum with:

```java
    private enum State {
        READ_COMMAND,
        READ_ARRAY_BODY,
        CLOSING
    }
```

Add parser state fields:

```java
    private byte[][] pendingArgv;
    private int pendingArgc;
    private int pendingArgIndex;
    private int pendingRetainedBytes;
    private int pendingBulkLength = -1;
```

Refactor `decode(...)` so `READ_COMMAND` parses the array header once, allocates `pendingArgv`, sets `pendingArgc`, flips to `READ_ARRAY_BODY`, and then lets a new helper continue body parsing:

```java
            if (state == State.READ_COMMAND) {
                byte first = in.getByte(in.readerIndex());
                ParseResult result = first == ARRAY ? tryStartArray(in, out) : tryReadInline(in, out);
                if (result == ParseResult.NEED_MORE) {
                    return;
                }
                if (result == ParseResult.ERROR) {
                    if (shouldCloseAfterReply(out)) {
                        state = State.CLOSING;
                        in.readerIndex(in.writerIndex());
                        return;
                    }
                    continue;
                }
                if (result == ParseResult.EMITTED) {
                    continue;
                }
            }
            if (state == State.READ_ARRAY_BODY) {
                ParseResult result = tryContinueArray(in, out);
                if (result == ParseResult.NEED_MORE) {
                    return;
                }
                if (result == ParseResult.ERROR) {
                    if (shouldCloseAfterReply(out)) {
                        state = State.CLOSING;
                        in.readerIndex(in.writerIndex());
                        return;
                    }
                    continue;
                }
            }
```

Implement `tryStartArray(...)`, `tryContinueArray(...)`, and `resetPendingArray()` so partially read array elements remain in decoder fields until the whole command is complete or errors out. The body helper should:

- parse each bulk header only once,
- remember `pendingBulkLength`,
- copy each body exactly once,
- enforce `maxCommandBytes` before body allocation,
- emit `RespCommandRequest.wrapReadOnly(pendingArgv, pendingRetainedBytes)` only when `pendingArgIndex == pendingArgc`,
- call `resetPendingArray()` on success or error.

Keep inline-command logic stateless.

- [ ] **Step 4: Run the decoder tests again**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty -am \
  -Dtest=RespRequestDecoderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS for `RespRequestDecoderTest`.

- [ ] **Step 5: Commit the incremental decoder**

```bash
git add \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java
git commit -m "refactor: make RESP array decoding incremental"
```

### Task 3: Make Protocol Error Replies Session-Aware And Closing-State-Aware

**Files:**
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`

**Interfaces:**
- Consumes: `NettyExecutionConnection.get(Channel)` to discover session and connection closing state.
- Produces: protocol error replies encoded with `replyWriterFactory.newWriter(connection.session(), ...)` when a connection exists, and handler dropping behavior keyed to connection closing rather than a private boolean.

- [ ] **Step 1: Write the failing RESP3/session-aware protocol-error tests**

Add this test to `RespProtocolErrorReplyHandlerTest.java`:

```java
    @Test
    public void protocolErrorUsesConnectionSessionReplyVersionWhenAvailable() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespProtocolErrorReplyHandler(new RespReplyWriterFactory()));
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 16, 1024);
            connection.session().setRespVersion(3);

            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error", true)));
            Assert.assertArrayEquals(ascii("!18\r\nERR Protocol error\r\n"), readOutbound(ch));
            Assert.assertFalse(ch.isOpen());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
```

Add this integration test to `RespProtocolErrorIntegrationTest.java`:

```java
    @Test
    public void malformedRespAfterHello3ReturnsResp3BlobErrorThenCloses() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0");
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals('%', in.read());
            readRespFrame(in);

            out.write("*1\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();

            Assert.assertEquals('!', in.read());
            Assert.assertEquals("18\r", readLine(in));
            Assert.assertEquals("ERR Protocol error\r", readLine(in));
            Assert.assertEquals(-1, in.read());
        }
    }
```

Add this helper to `RespProtocolErrorIntegrationTest.java`:

```java
    private static void readRespFrame(InputStream in) throws IOException {
        int type = in.read();
        if (type == '%') {
            int pairs = Integer.parseInt(readLine(in));
            for (int i = 0; i < pairs * 2; i++) {
                if (in.read() != '$') {
                    throw new IOException("expected bulk string in HELLO map");
                }
                int len = Integer.parseInt(readLine(in));
                in.readNBytes(len);
                if (in.read() != '\r' || in.read() != '\n') {
                    throw new IOException("expected CRLF after HELLO bulk string");
                }
            }
            return;
        }
        throw new IOException("unexpected RESP frame type: " + type);
    }
```

- [ ] **Step 2: Run the targeted handler/integration tests to verify protocol errors currently ignore session version**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespProtocolErrorReplyHandlerTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because protocol errors are still written through `replyWriterFactory.newWriter(new NettyByteBufSink(out))`, which forces RESP2.

- [ ] **Step 3: Make the handler derive writer/closing state from the connection**

Update `RespProtocolErrorReplyHandler.java`:

```java
import yier.bubu.redis.app.server.NettyExecutionConnection;
import yier.bubu.redis.execution.api.RedisReplyWriter;
```

Replace the field:

```java
    private boolean closing;
```

with:

```java
    private volatile boolean closingStarted;
```

Update `channelRead(...)` to derive writer/session from the attached connection:

```java
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (closingStarted || (connection != null && connection.context().isClosing())) {
            closeIfPossible(msg);
            return;
        }
```

and:

```java
        if (error.closeAfterReply()) {
            closingStarted = true;
            safeDisableAutoRead(ctx);
            closeAfterReplyObserver.accept(ctx);
        }
```

Replace the writer creation with:

```java
            RedisReplyWriter writer = connection == null
                    ? replyWriterFactory.newWriter(new NettyByteBufSink(out))
                    : replyWriterFactory.newWriter(connection.session(), new NettyByteBufSink(out));
```

Keep the rest of the reply path unchanged.

- [ ] **Step 4: Run the targeted protocol-error tests again**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespProtocolErrorReplyHandlerTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS for all listed test classes.

- [ ] **Step 5: Commit the session-aware protocol-error handler**

```bash
git add \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandlerTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolErrorIntegrationTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java
git commit -m "refactor: encode RESP protocol errors with session state"
```

### Task 4: Lock The Protocol/Error Boundary With Regression Coverage And Docs

**Files:**
- Modify: `docs/project-docs/configuration-and-operations.md`
- Modify: `docs/project-docs/protocol-reference.md`
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`

**Interfaces:**
- Consumes: behavior from Tasks 1-3.
- Produces: docs and regression tests that state protocol errors are protocol-layer replies, cumulative command bytes are capped, and queued command skip semantics remain unchanged after closing starts.

- [ ] **Step 1: Add the boundary regression wording/tests**

In `ClosingSkipSideEffectsIntegrationTest.java`, rename the decoder-fallback regression test to make the contract explicit:

```java
    @Test
    public void commandHandlerFallbackStillTreatsThrownDecoderFailuresAsInternalErrors() throws Exception {
```

Leave the assertions unchanged. This test should continue proving that the command handler is only an internal-error fallback, not the normal protocol-error path.

Update `configuration-and-operations.md` so the protocol-limits section reads:

```markdown
`--protocolMaxBulkBytes`、`--protocolMaxArgs`、`--protocolMaxLineBytes` 和 `--protocolMaxCommandBytes` 会直接传给 `RespRequestDecoder`。它们分别约束 bulk body、参数个数、header/inline 行长度，以及单条命令累计字节数。暴露在不可信网络里时，优先收紧这四个入口上限，再考虑更深层的内存调参。
```

Update `protocol-reference.md` so the malformed RESP section reads:

```markdown
malformed RESP 没有可靠的重同步点。Yierdis 的策略是：尽量返回 RESP error reply，然后关闭当前连接。实现上，`RespRequestDecoder` 产出 `RespProtocolError`，`RespProtocolErrorReplyHandler` 用当前连接 session 的 RESP 版本统一编码错误并标记 close-after-reply，flush 后断开连接。
```

Update the limits table in `protocol-reference.md` to add:

```markdown
| 单条请求累计字节数 | 64 MiB | `--protocolMaxCommandBytes` |
```

Update `request-execution-flow.md` to include:

```markdown
- `RespRequestDecoder`：从 `ByteBuf` 解析 RESP array 或 inline command，执行 bulk/argc/line/command-bytes 四类入口限制，只产出 `RespCommandRequest` 或 `RespProtocolError`
```

- [ ] **Step 2: Run the final focused verification set**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main -am \
  -Dtest=RespRequestDecoderTest,RespProtocolErrorReplyHandlerTest,YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,RespProtocolErrorIntegrationTest,ClosingSkipSideEffectsIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS for all listed test classes.

- [ ] **Step 3: Sanity-check the docs wording**

Run:

```bash
rg -n "protocolMaxCommandBytes|当前连接 session 的 RESP 版本|bulk/argc/line/command-bytes" \
  docs/project-docs/configuration-and-operations.md \
  docs/project-docs/protocol-reference.md \
  docs/project-docs/request-execution-flow.md
```

Expected: one or more matches in each file.

- [ ] **Step 4: Commit the docs and boundary lock-in**

```bash
git add \
  docs/project-docs/configuration-and-operations.md \
  docs/project-docs/protocol-reference.md \
  docs/project-docs/request-execution-flow.md \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java
git commit -m "docs: lock RESP protocol hardening boundaries"
```
