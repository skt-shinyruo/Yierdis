# RESP Null Bulk And Retained Bytes Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement RESP `$-1` null-bulk propagation and saturated `retainedBytes()` accounting without changing the protocol-to-command boundary or widening command null acceptance beyond the existing `PING`/`ECHO` allowance.

**Architecture:** Keep `RespCommandRequest -> RespExecutionAdapter -> ExecutionRequest` as the only request adaptation boundary. Make `RespRequestDecoder` and `RespCommandRequest` faithfully carry null bulk strings, centralize retained-byte saturation inside `ByteArrayExecutionRequest`, and keep command semantics centralized in `YierdisFastCommandProcessor` with regression coverage at decoder, adapter, executor, transaction, and end-to-end layers.

**Tech Stack:** Java 25, Maven, JUnit 4, Netty, Yierdis command/runtime test utilities

**Environment:** Before running any `mvn` command in this repository, activate the `use-jdk25` skill or otherwise ensure `java -version` and `mvn -version` resolve to JDK 25.

---

## File Map

**Protocol / request adaptation**

- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
  - Accept RESP bulk length `-1` as null bulk string in array decoding.
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
  - Preserve null argv elements in `copyOf(...)` and `wrapReadOnly(...)`.
- Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
  - Add RESP `$-1` decode, `< -1` reject, and adapter propagation tests.
- Modify: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
  - Add null argv preservation coverage.
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
  - Add adapter-to-handler end-to-end null-bulk tests.

**Execution request accounting**

- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java`
  - Add shared saturated retained-byte helper and route all factories through it.
- Modify: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ExecutionRequestContractTest.java`
  - Add retained-byte saturation helper tests and constructor-path regression coverage.
- Modify: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
  - Add queue-budget regression showing snapshot bytes are checked even when the incoming estimate is low.

**Command / transaction policy regression**

- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
  - Tighten comments so null handling is documented as policy, not accidental NPE shielding.
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandVariantCoverageTest.java`
  - Add `PING null` / `ECHO null` coverage through the default command stack.
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandDispatchErrorTest.java`
  - Add representative null-bulk rejection coverage for normal command execution.
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
  - Add representative null-bulk rejection coverage under `MULTI`.

**Docs**

- Modify: `docs/project-docs/command-parsing-and-dispatch.md`
  - Explain that RESP arrays may carry null bulk strings into the processor and that the processor is the semantic gate.
- Modify: `docs/project-docs/request-execution-flow.md`
  - Correct the adaptation description from “copy” to the current read-only wrapping path and mention null argv propagation.
- Modify: `docs/project-docs/bytes-and-fast-paths.md`
  - Correct the `RespExecutionAdapter` fast-path description and note saturated retained-byte accounting.

## Task 1: Implement RESP Null-Bulk Decode And Adaptation

**Files:**
- Modify: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- Modify: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`

- [ ] **Step 1: Write the failing decoder, adapter, and Netty integration tests**

Add these methods to `RespRequestDecoderTest`:

```java
@Test
public void decodesNullBulkStringArgumentInArrayCommand() {
    EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
    try {
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                "*2\r\n$4\r\nECHO\r\n$-1\r\n",
                StandardCharsets.US_ASCII
        )));

        RespCommandRequest req = ch.readInbound();
        Assert.assertEquals(2, req.argc());
        Assert.assertArrayEquals(bytes("ECHO"), req.readOnlyArg(0));
        Assert.assertNull(req.readOnlyArg(1));
        Assert.assertEquals(4, req.retainedBytes());
        Assert.assertNull(ch.readInbound());
    } finally {
        ch.finishAndReleaseAll();
    }
}

@Test
public void rejectsBulkLengthBelowNegativeOne() {
    EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
    try {
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                "*2\r\n$4\r\nECHO\r\n$-2\r\n",
                StandardCharsets.US_ASCII
        )));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespProtocolError);
        Assert.assertTrue(((RespProtocolError) msg).closeAfterReply());
        Assert.assertNull(ch.readInbound());
    } finally {
        ch.finishAndReleaseAll();
    }
}

@Test
public void adapterPreservesNullBulkArgument() {
    EmbeddedChannel ch = new EmbeddedChannel(
            new RespRequestDecoder(1024, 16, 1024),
            new RespCommandAdapter()
    );
    ExecutionRequest request = null;
    try {
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(
                "*2\r\n$4\r\nECHO\r\n$-1\r\n",
                StandardCharsets.US_ASCII
        )));

        request = ch.readInbound();
        Assert.assertEquals(2, request.argc());
        Assert.assertArrayEquals(bytes("ECHO"), request.readOnlyByteArray(0));
        Assert.assertTrue(request.isNull(1));
        Assert.assertNull(request.readOnlyByteArray(1));
        Assert.assertEquals(4, request.retainedBytes());
    } finally {
        if (request != null) {
            request.close();
        }
        ch.finishAndReleaseAll();
    }
}
```

Replace the null-rejection tests in `RespExecutionAdapterTest` with null-preservation coverage:

```java
@Test
public void copyOfPreservesNullArgvElement() {
    RespCommandRequest request = RespCommandRequest.copyOf(java.util.Arrays.asList(bytes("ECHO"), null));

    Assert.assertArrayEquals(bytes("ECHO"), request.readOnlyArg(0));
    Assert.assertNull(request.readOnlyArg(1));
    Assert.assertEquals(4, request.retainedBytes());
}

@Test
public void wrapReadOnlyPreservesNullArgvElement() {
    RespCommandRequest request = RespCommandRequest.wrapReadOnly(
            new byte[][]{bytes("ECHO"), null},
            4
    );

    ExecutionRequest out = RespExecutionAdapter.DEFAULT.toExecutionRequest(request);
    Assert.assertArrayEquals(bytes("ECHO"), out.readOnlyByteArray(0));
    Assert.assertTrue(out.isNull(1));
    Assert.assertNull(out.readOnlyByteArray(1));
    Assert.assertEquals(4, out.retainedBytes());
}
```

Add these methods to `NettyExecutionAdapterIntegrationTest`:

```java
@Test
public void echoNullBulkStringSurvivesRespAdapterAndWritesNullReply() {
    try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                Runnable::run,
                new RespReplyWriterFactory(),
                ioAdapter,
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        EmbeddedChannel channel = new EmbeddedChannel(
                new RespCommandAdapter(),
                new YierdisFastCommandHandler(executor, new RespReplyWriterFactory())
        );
        try {
            NettyExecutionConnection.getOrCreate(channel, 16, 1024);
            channel.writeInbound(RespCommandRequest.wrapReadOnly(
                    new byte[][]{utf8("ECHO"), null},
                    4
            ));

            Assert.assertArrayEquals("$-1\r\n".getBytes(StandardCharsets.UTF_8), readOutbound(channel));
        } finally {
            channel.finishAndReleaseAll();
            executor.close();
        }
    }
}

@Test
public void setNullBulkStringSurvivesRespAdapterAndHitsCommandError() {
    try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                Runnable::run,
                new RespReplyWriterFactory(),
                ioAdapter,
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        EmbeddedChannel channel = new EmbeddedChannel(
                new RespCommandAdapter(),
                new YierdisFastCommandHandler(executor, new RespReplyWriterFactory())
        );
        try {
            NettyExecutionConnection.getOrCreate(channel, 16, 1024);
            channel.writeInbound(RespCommandRequest.wrapReadOnly(
                    new byte[][]{utf8("SET"), utf8("k"), null},
                    4
            ));

            Assert.assertArrayEquals(
                    "-ERR Protocol error: null bulk string\r\n".getBytes(StandardCharsets.UTF_8),
                    readOutbound(channel)
            );
        } finally {
            channel.finishAndReleaseAll();
            executor.close();
        }
    }
}
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-main \
  -Dtest=RespRequestDecoderTest,RespExecutionAdapterTest,NettyExecutionAdapterIntegrationTest test
```

Expected:

- `RespRequestDecoderTest` fails because `$-1` is still rejected as invalid bulk length.
- `RespExecutionAdapterTest` fails because `RespCommandRequest.copyOf(...)` / `wrapReadOnly(...)` still reject null argv elements.
- `NettyExecutionAdapterIntegrationTest` fails with `IllegalArgumentException: RESP command argv must not contain null bulk strings`.

- [ ] **Step 3: Implement null-bulk decode and DTO preservation**

Update `RespRequestDecoder.tryReadArray(...)` to treat `-1` as null bulk:

```java
Long lenValue = parseInteger(in, bulkLineStart + 1, bulkLf - 1);
if (lenValue == null || lenValue < -1 || lenValue > RespProtocolLimits.MAX_BULK_BYTES) {
    emitProtocolError(out, "ERR Protocol error: invalid bulk length", true);
    state = State.CLOSING;
    return ParseResult.ERROR;
}
if (lenValue.longValue() == -1L) {
    argv[i] = null;
    continue;
}
int len = lenValue.intValue();
if (maxBulkBytes > 0 && len > maxBulkBytes) {
    emitProtocolError(out, "ERR Protocol error: invalid bulk length", true);
    state = State.CLOSING;
    return ParseResult.ERROR;
}
```

Update `RespCommandRequest.copyOf(...)` and `wrapReadOnly(...)` to preserve nulls:

```java
public static RespCommandRequest copyOf(List<byte[]> args) {
    Objects.requireNonNull(args, "args");
    byte[][] argv = new byte[args.size()][];
    int retainedBytes = 0;
    for (int i = 0; i < args.size(); i++) {
        byte[] arg = args.get(i);
        if (arg == null) {
            argv[i] = null;
            continue;
        }
        argv[i] = arg.clone();
        retainedBytes = saturatedRetainedBytes(retainedBytes, arg.length);
    }
    return new RespCommandRequest(argv, retainedBytes);
}

public static RespCommandRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
    Objects.requireNonNull(argv, "argv");
    byte[][] owned = new byte[argv.length][];
    for (int i = 0; i < argv.length; i++) {
        owned[i] = argv[i];
    }
    return new RespCommandRequest(owned, Math.max(0, retainedBytes));
}
```

Keep `readOnlyArg(int)` returning `null` for null argv entries and leave `RespExecutionAdapter` unchanged unless the compiler points out an unnecessary assumption.

- [ ] **Step 4: Re-run the focused tests to verify they pass**

Run:

```bash
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-main \
  -Dtest=RespRequestDecoderTest,RespExecutionAdapterTest,NettyExecutionAdapterIntegrationTest test
```

Expected: `BUILD SUCCESS`, with the new null-bulk decode and end-to-end reply tests passing.

- [ ] **Step 5: Commit the protocol/adaptation changes**

```bash
git add \
  yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java \
  yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java \
  yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java \
  yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java
git commit -m "feat: support resp null bulk request args"
```

## Task 2: Saturate `ByteArrayExecutionRequest.retainedBytes()`

**Files:**
- Modify: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ExecutionRequestContractTest.java`
- Modify: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java`

- [ ] **Step 1: Write the failing retained-byte accounting tests**

Add this method to `ExecutionRequestContractTest`:

```java
@Test
public void byteArrayExecutionRequestRetainedBytesHelperSaturatesOnOverflow() {
    Assert.assertEquals(Integer.MAX_VALUE, ByteArrayExecutionRequest.saturatedRetainedBytes(Integer.MAX_VALUE - 1, 2));
    Assert.assertEquals(Integer.MAX_VALUE, ByteArrayExecutionRequest.saturatedRetainedBytes(Integer.MAX_VALUE, 1));
    Assert.assertEquals(5, ByteArrayExecutionRequest.saturatedRetainedBytes(3, 2));
    Assert.assertEquals(3, ByteArrayExecutionRequest.saturatedRetainedBytes(-4, 3));
}
```

Add this regression to `EngineSessionTest`:

```java
@Test
public void transactionRejectsWhenSnapshotBytesExceedBudgetEvenIfEstimatedBytesDoNot() {
    EngineSession session = new EngineSession(4, 6);
    session.transaction().begin();

    ExecutionRequest underestimated = new ExecutionRequest() {
        private final byte[][] argv = new byte[][]{
                "SET".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                "k".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                "value".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
        };

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return argv[index].length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            System.arraycopy(argv[index], 0, dst, dstOff, argv[index].length);
        }

        @Override
        public byte[] toByteArray(int index) {
            return argv[index].clone();
        }

        @Override
        public int retainedBytes() {
            return 0;
        }

        @Override
        public void close() {
        }
    };

    Assert.assertEquals("ERR Transaction queue is full", session.transaction().tryEnqueue(underestimated));
    Assert.assertTrue(session.transaction().aborted());
    Assert.assertEquals(0, session.transaction().size());
}
```

- [ ] **Step 2: Run the focused accounting tests to verify the new helper test fails**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core \
  -Dtest=ExecutionRequestContractTest,EngineSessionTest test
```

Expected:

- `ExecutionRequestContractTest` fails to compile because `ByteArrayExecutionRequest.saturatedRetainedBytes(...)` does not exist yet.
- `EngineSessionTest` may already pass once compiled; keep it because it locks the second-stage snapshot-bytes check.

- [ ] **Step 3: Implement a shared saturated retained-byte helper and route all factories through it**

Update `ByteArrayExecutionRequest` like this:

```java
static int saturatedRetainedBytes(int retainedBytes, int argLength) {
    long next = (long) Math.max(0, retainedBytes) + Math.max(0, argLength);
    return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
}
```

Use it in every retained-byte accumulation site:

```java
retainedBytes = saturatedRetainedBytes(retainedBytes, copy.length);
```

Apply that replacement in:

- `copyOf(List<byte[]>)`
- `copyOf(ExecutionRequest)`
- `fromUtf8(String, List<String>)`

Do not change `wrapReadOnly(...)`; it accepts a caller-supplied retained-byte value by design.

- [ ] **Step 4: Re-run the focused accounting tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core \
  -Dtest=ExecutionRequestContractTest,EngineSessionTest test
```

Expected: `BUILD SUCCESS`, with the helper test proving saturation and the queue-budget regression still passing.

- [ ] **Step 5: Commit the retained-byte accounting changes**

```bash
git add \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java \
  yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ExecutionRequestContractTest.java \
  yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java
git commit -m "fix: saturate execution request retained bytes"
```

## Task 3: Lock Command And Transaction Null-Policy Coverage

**Files:**
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandVariantCoverageTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandDispatchErrorTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`

- [ ] **Step 1: Add regression coverage for allowed and disallowed null argv**

Add this method to `CommandVariantCoverageTest`:

```java
@Test
public void connectionCommandsAcceptNullBulkMessages() {
    forEachDb(db -> {
        YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            Assert.assertTrue(client.execute(java.util.Arrays.asList(b("PING"), null)) instanceof ReplyNull);
            Assert.assertTrue(client.execute(java.util.Arrays.asList(b("ECHO"), null)) instanceof ReplyNull);
        }
    });
}
```

Extend `DefaultCommandDispatchErrorTest.commandProcessorRejectsEmptyAndUnknownCommandsBeforeDispatch()` with:

```java
assertError(
        "ERR Protocol error: null bulk string",
        client.execute(Arrays.asList(b("SET"), b("k"), null))
);
```

Add this method to `TransactionCommandTest`:

```java
@Test
public void nullBulkStringInsideMultiAbortsBeforeExec() {
    forEachDb(db -> {
        YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
        TestSession session = new TestSession();
        try (FastTestClient client = new FastTestClient(processor, session)) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

            ReplyObject badNull = client.execute(Arrays.asList(b("SET"), b("k"), null));
            Assert.assertTrue(badNull instanceof ReplyError);
            Assert.assertEquals("ERR Protocol error: null bulk string", ((ReplyError) badNull).message());
            Assert.assertEquals(0, session.transactionState().size());

            ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
            Assert.assertTrue(exec instanceof ReplyError);
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
        }
    });
}
```

- [ ] **Step 2: Run the integration command regressions**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests \
  -Dtest=CommandVariantCoverageTest,DefaultCommandDispatchErrorTest,TransactionCommandTest test
```

Expected: `BUILD SUCCESS`. If any of these fail after Tasks 1-2, the null-policy contract is still drifting between the protocol and command layers.

- [ ] **Step 3: Make the processor comment describe policy explicitly**

Replace the current null-gate comment in `YierdisFastCommandProcessor` with:

```java
// RESP arrays may legally carry null bulk strings into ExecutionRequest.
// Command semantics still reject them by default so DB and command implementations
// never see unexpected nulls. The only allowed form remains PING/ECHO with a
// single message argument at argv[1].
```

Do not change the actual null-gate behavior in this task unless the tests from Step 2 reveal a real mismatch.

- [ ] **Step 4: Re-run the focused integration command regressions**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests \
  -Dtest=CommandVariantCoverageTest,DefaultCommandDispatchErrorTest,TransactionCommandTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Commit the regression-coverage and policy-comment changes**

```bash
git add \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandVariantCoverageTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandDispatchErrorTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java \
  yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java
git commit -m "test: lock null bulk command policy coverage"
```

## Task 4: Update Docs And Run Full Verification

**Files:**
- Modify: `docs/project-docs/command-parsing-and-dispatch.md`
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `docs/project-docs/bytes-and-fast-paths.md`

- [ ] **Step 1: Update the project docs to match the new steady state**

Replace the null-policy paragraph in `docs/project-docs/command-parsing-and-dispatch.md` with:

```md
### 空命令和 null bulk string

这两类错误在 `YierdisFastCommandProcessor` 最前面直接处理：

- `argc <= 0` 或 `argv[0]` 为空：`ERR empty command`
- RESP array 带进来的非法 null bulk string：`ERR Protocol error: null bulk string`

RESP 协议层现在会把 array 里的 `$-1` 忠实解成 `ExecutionRequest` 里的 null argv 元素；真正决定这些 null 是否合法的是 command-kernel。当前只有 `PING` / `ECHO` 的单 message 参数允许为 null，其余命令都会在 processor 入口被拒绝。
```

Replace the adaptation description in `docs/project-docs/request-execution-flow.md` with:

```md
`RespExecutionAdapter` 是协议和执行层之间的转换器。它读取 `RespCommandRequest` 的 argv 视图，包装成 read-only `ByteArrayExecutionRequest`，再以 `ExecutionRequest` 形式交给后续层。RESP array 里的 null bulk string 会在这里原样保留为 null argv 元素；命令是否合法由后面的 command-kernel 决定。
```

Replace the `RespExecutionAdapter` paragraph in `docs/project-docs/bytes-and-fast-paths.md` with:

```md
`RespExecutionAdapter` 的生产路径默认选择是 `ByteArrayExecutionRequest.wrapReadOnly(...)`：它复制外层 argv 引用数组，但继续共享已经 materialize 完成的 heap `byte[]` 参数，并要求后续层按只读约定消费。这样请求跨过 Netty decoder 生命周期后就拥有稳定 argv 与 retained-bytes 元数据，同时避免在协议适配点做第二次逐参数复制。

`ByteArrayExecutionRequest` 自己负责 retained bytes 的饱和计数。无论请求来自 RESP 适配、`copyOf(...)` 快照还是 `fromUtf8(...)` 测试构造，累计值都会封顶到 `Integer.MAX_VALUE`，不会因为 `int` 回绕变成负数。
```

- [ ] **Step 2: Review the doc diff for stale wording**

Run:

```bash
git diff -- docs/project-docs/command-parsing-and-dispatch.md \
  docs/project-docs/request-execution-flow.md \
  docs/project-docs/bytes-and-fast-paths.md
```

Expected: the diff only replaces stale wording about null-bulk handling, adaptation copying, and retained-byte accounting.

- [ ] **Step 3: Run the full verification set**

Run:

```bash
mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-networking/yierdis-networking-resp \
  -Dtest=RespRequestDecoderTest,RespExecutionAdapterTest test

mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core \
  -Dtest=ExecutionRequestContractTest,EngineSessionTest test

mvn -pl yierdis-server/yierdis-server-main \
  -Dtest=NettyExecutionAdapterIntegrationTest test

mvn -pl yierdis-tests/yierdis-integration-tests \
  -Dtest=CommandVariantCoverageTest,DefaultCommandDispatchErrorTest,TransactionCommandTest test
```

Expected: all four commands end with `BUILD SUCCESS`.

- [ ] **Step 4: Commit the docs and final verification results**

```bash
git add \
  docs/project-docs/command-parsing-and-dispatch.md \
  docs/project-docs/request-execution-flow.md \
  docs/project-docs/bytes-and-fast-paths.md
git commit -m "docs: align resp null bulk request flow"
```
