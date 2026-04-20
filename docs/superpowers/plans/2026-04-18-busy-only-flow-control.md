# Busy-Only Flow Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove all Netty `autoRead` backpressure and backpressure CLI flags, keeping only bounded executor budgets and `ERR busy <reason>`, while reserving a minimal server-internal flow-control SPI for future development.

**Architecture:** Keep `ExecutorBacklogBudget` as the SSOT for “can we accept this request” (task count + optional queued bytes). All “flow control” becomes a server-internal SPI (`NettyFlowControl`) called on submit rejection and command completion; default implementation is no-op.

**Tech Stack:** Java 25, Netty 4.1, Maven, JUnit 4, picocli.

---

## File Map (Create / Modify / Delete)

**Create:**
- `yierdis-server/src/main/java/yier/bubu/redis/NettyFlowControl.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NoopNettyFlowControl.java`

**Modify:**
- `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java`
- `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
- `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- `README.md`

**Delete:**
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureController.java`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureIo.java`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureRuntime.java`
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureObserver.java`

---

### Task 1: Remove Backpressure CLI Flags (Unknown Option)

**Files:**
- Modify: `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`

- [ ] **Step 1: Add failing args test that backpressure flags are rejected at parse time**

Add to `YierdisServerArgsTest`:

```java
@Test
public void deletedBackpressureFlagsAreRejectedAtParseTime() {
    YierdisServerArgs args = new YierdisServerArgs();
    assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--backpressureHigh", "1"));
    assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--backpressureLow", "1"));
    assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--backpressureBytesHigh", "1"));
    assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(args).parseArgs("--backpressureBytesLow", "1"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
mvn -pl yierdis-args test -Dtest=YierdisServerArgsTest#deletedBackpressureFlagsAreRejectedAtParseTime
```

Expected: FAIL (flags still parse in current code).

- [ ] **Step 3: Remove the backpressure option names + `@Option` annotations + argv emission**

Update `YierdisServerArgNames`:

```diff
-    public static final String BACKPRESSURE_HIGH = "--backpressureHigh";
-    public static final String BACKPRESSURE_LOW = "--backpressureLow";
-    public static final String BACKPRESSURE_BYTES_HIGH = "--backpressureBytesHigh";
-    public static final String BACKPRESSURE_BYTES_LOW = "--backpressureBytesLow";
```

In `YierdisServerArgs`, remove the `@Option(...)` annotations for:

- `backpressureHighWatermark`
- `backpressureLowWatermark`
- `backpressureBytesHighWatermark`
- `backpressureBytesLowWatermark`

Also remove these from `toArgv()`:

```diff
-        out.add(YierdisServerArgNames.BACKPRESSURE_HIGH);
-        out.add(Integer.toString(backpressureHighWatermark));
-        out.add(YierdisServerArgNames.BACKPRESSURE_LOW);
-        out.add(Integer.toString(backpressureLowWatermark));
-        out.add(YierdisServerArgNames.BACKPRESSURE_BYTES_HIGH);
-        out.add(Long.toString(backpressureBytesHighWatermark));
-        out.add(YierdisServerArgNames.BACKPRESSURE_BYTES_LOW);
-        out.add(Long.toString(backpressureBytesLowWatermark));
```

Update `YierdisServerArgsTest.normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields`:

- Remove these argv pairs from `parse(...)`:
  - `--backpressureHigh/--backpressureLow/--backpressureBytesHigh/--backpressureBytesLow`
- Remove the runtime config assertions for `backpressure*Watermark` fields.

Delete these now-meaningless tests from `YierdisServerArgsTest`:

```diff
-    @Test
-    public void invalidWatermarkOrderIsRejected() { ... }
-
-    @Test
-    public void invalidBytesWatermarkOrderIsRejected() { ... }
-
-    @Test
-    public void bytesLowWithoutBytesHighIsRejected() { ... }
```

Update `ServerConfigArgsTest`:

- Replace `invalidWatermarkOrderFailsFast` with a parse-time rejection test:

```java
@Test
public void deletedBackpressureFlagsFailFast() {
    YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
            "--backpressureHigh", "10"
    }));
    Assert.assertEquals(2, error.exitCode());
    Assert.assertTrue(error.shouldPrintUsage());
}
```

- In `normalizedArgsExposeSharedRuntimeConfig`, remove the backpressure argv pairs and delete the backpressure assertions.

- [ ] **Step 4: Run args + server tests**

Run:
```bash
mvn -pl yierdis-args,yierdis-server test
```

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java \
  yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java \
  yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java
git commit -m "refactor(cli): remove backpressure flags (busy-only)"
```

---

### Task 2: Busy-Only Server (Remove autoRead Backpressure + Drop Runtime Backpressure Fields)

**Files:**
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- Modify: `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Delete: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`

- [ ] **Step 1: Add failing tests for “no backpressure fields” and “autoRead never toggles”**

In `YierdisServerArgsTest.normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields`, add:

```java
Assert.assertFalse(runtimeConfig.containsKey("backpressureHighWatermark"));
Assert.assertFalse(runtimeConfig.containsKey("backpressureLowWatermark"));
Assert.assertFalse(runtimeConfig.containsKey("backpressureBytesHighWatermark"));
Assert.assertFalse(runtimeConfig.containsKey("backpressureBytesLowWatermark"));
```

In `ServerConfigArgsTest.normalizedArgsExposeSharedRuntimeConfig`, add the same `containsKey(...) == false` assertions on the `recordValues(...)` map.

Add to `NettyCommandExecutorTest`:

```java
@Test
public void queueFullDoesNotDisableAutoRead() {
    try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                ImmediateEventExecutor.INSTANCE,
                new JsonLineReplyWriterFactory(),
                1,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
        );

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());
            ch.writeInbound(request("PING"));
            ch.writeInbound(request("PING"));
            ch.runPendingTasks();
            Assert.assertTrue("autoRead should remain enabled in busy-only mode", ch.config().isAutoRead());
        } finally {
            executor.close();
            ch.finishAndReleaseAll();
        }
    }
}
```

- [ ] **Step 2: Run targeted tests to verify they fail**

Run:
```bash
mvn -pl yierdis-args,yierdis-server test -Dtest=YierdisServerArgsTest#normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields,ServerConfigArgsTest#normalizedArgsExposeSharedRuntimeConfig,NettyCommandExecutorTest#queueFullDoesNotDisableAutoRead
```

Expected: FAIL (runtime config still has backpressure fields, executor still toggles autoRead).

- [ ] **Step 3: Drop backpressure fields from runtime config + args**

Update `YierdisServerRuntimeConfig` record signature:

```diff
 public record YierdisServerRuntimeConfig(
         int port,
         int databases,
         long cleanupIntervalMillis,
         int ioThreads,
         int executorQueueCapacity,
         long executorQueueMaxBytes,
         ExecutorSchedulingPolicy executorSchedulingPolicy,
-        int backpressureHighWatermark,
-        int backpressureLowWatermark,
-        long backpressureBytesHighWatermark,
-        long backpressureBytesLowWatermark,
         int executorMaxDrainCommands,
         long executorDrainTimeLimitMillis,
         int transactionQueueMaxCommands,
         long transactionQueueMaxBytes,
         int protocolMaxBulkBytes,
         int protocolMaxArgs,
         int protocolMaxLineBytes,
         long maxmemoryBytes,
         MaxmemoryScope maxmemoryScope,
         MaxmemoryPolicy maxmemoryPolicy,
         int maxmemorySamples,
         long evictionTimeLimitMillis,
         long expireCleanupTimeLimitMillis,
         long keysTimeBudgetMillis,
         int keysMaxResults
 ) {
```

Update `YierdisServerArgs.toRuntimeConfig()`:

```diff
         return new YierdisServerRuntimeConfig(
                 port,
                 databases,
                 cleanupIntervalMillis,
                 ioThreads,
                 executorQueueCapacity,
                 executorQueueMaxBytes,
                 YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.fromArgvValue(executorSchedulingPolicy),
-                backpressureHighWatermark,
-                backpressureLowWatermark,
-                backpressureBytesHighWatermark,
-                backpressureBytesLowWatermark,
                 executorMaxDrainCommands,
                 executorDrainTimeLimitMillis,
                 transactionQueueMaxCommands,
                 transactionQueueMaxBytes,
                 protocolMaxBulkBytes,
                 protocolMaxArgs,
                 protocolMaxLineBytes,
                 maxmemoryBytes,
                 YierdisServerRuntimeConfig.MaxmemoryScope.fromArgvValue(maxmemoryScope),
                 YierdisServerRuntimeConfig.MaxmemoryPolicy.fromArgvValue(maxmemoryPolicy),
                 maxmemorySamples,
                 evictionTimeLimitMillis,
                 expireCleanupTimeLimitMillis,
                 keysTimeBudgetMillis,
                 keysMaxResults
         );
```

Then delete the now-unused backpressure fields from `YierdisServerArgs` (`backpressureHighWatermark`, `backpressureLowWatermark`, `backpressureBytesHighWatermark`, `backpressureBytesLowWatermark`) entirely.

- [ ] **Step 4: Remove autoRead backpressure + remove write-buffer handler**

Update `NettyCommandExecutorConfig` to remove backpressure fields and stop reading them from runtime config:

```diff
 record NettyCommandExecutorConfig(
         int queueCapacity,
         long queueMaxBytes,
-        int backpressureHighWatermark,
-        int backpressureLowWatermark,
-        long backpressureBytesHighWatermark,
-        long backpressureBytesLowWatermark,
         int maxDrainCommands,
         long drainTimeLimitMillis,
         yier.bubu.redis.executor.SchedulingPolicy schedulingPolicy
 ) {
```

In `NettyCommandExecutor` / `NettyCommandSubmitter` / `NettyCommandExecutionSupport`:

- Remove all `Channel.config().setAutoRead(false/true)` usage
- Remove `ExecutorBackpressureController` wiring and any `disableAutoRead/enableAutoRead/scheduleGlobalRecovery`
- Remove submit-time “pending watermark” checks and any “disable autoRead on reject”

In `YierdisFastCommandHandler` and `NettyCommandExecutionSupport`, remove any call that disables autoRead on closing/internal error.

In `YierdisServerChannelInitializer`, remove `.addLast("writeBufferBackpressure", ...)` and delete the handler class.

Update `YierdisServerBootstrapCommandWiringTest.channelInitializerUsesRuntimeConfigForSessionAndProtocolLimits` ordering assertions to not require `writeBufferBackpressure` (same diff as in the earlier plan version: assert `decoderIndex >= 0` and order relative to decoder).

- [ ] **Step 5: Update executor tests for new constructor signature and remove hysteresis tests**

Delete:
- `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`

In `NettyCommandExecutorTest`, delete:
- `autoReadIsDisabledAndReenabledWithHysteresis`
- `autoReadIsDisabledAndReenabledWithBytesHysteresis`

Update every `new NettyCommandExecutor(...)` callsite in tests to the new signature (no backpressure params):

```java
new NettyCommandExecutor(
        instance::bindToCurrentThread,
        processor,
        eventExecutor,
        new JsonLineReplyWriterFactory(),
        queueCapacity,
        queueMaxBytes,
        maxDrainCommands,
        drainTimeLimitMillis,
        SchedulingPolicy.FAIR
);
```

- [ ] **Step 6: Run full tests**

Run:
```bash
mvn test
```

Expected: `BUILD SUCCESS`

- [ ] **Step 7: Commit**

```bash
git add \
  yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java \
  yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java \
  yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutorConfig.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java
git rm yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java
git commit -m "refactor(server): busy-only mode (drop backpressure + autoRead toggling)"
```

---

### Task 3: Add `NettyFlowControl` SPI (No-Op) + Hook Test

**Files:**
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyFlowControl.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NoopNettyFlowControl.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`

- [ ] **Step 1: Add failing test for SPI hooks**

Add to `NettyCommandExecutorTest`:

```java
@Test
public void flowControlIsNotifiedOnRejectAndFinish() {
    try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
        class CountingFlowControl implements NettyFlowControl {
            final AtomicInteger rejected = new AtomicInteger(0);
            final AtomicInteger finished = new AtomicInteger(0);

            @Override
            public void onSubmitRejected(io.netty.channel.Channel ch, NettyCommandExecutor.SubmitRejectReason reason) {
                rejected.incrementAndGet();
            }

            @Override
            public void onCommandFinished(io.netty.channel.Channel ch) {
                finished.incrementAndGet();
            }
        }

        CountingFlowControl fc = new CountingFlowControl();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                ImmediateEventExecutor.INSTANCE,
                new JsonLineReplyWriterFactory(),
                1,
                0,
                128,
                10,
                SchedulingPolicy.FAIR,
                fc
        );

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            ch.writeInbound(request("PING"));
            Assert.assertNull(ch.readOutbound());

            ch.writeInbound(request("PING"));
            Assert.assertArrayEquals(ascii("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy queue_full\"}}\n"), readOutbound(ch));
            Assert.assertEquals(1, fc.rejected.get());

            executor.start();
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch, 1000));
            Assert.assertEquals(1, fc.finished.get());
        } finally {
            executor.close();
            ch.finishAndReleaseAll();
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
mvn -pl yierdis-server test -Dtest=NettyCommandExecutorTest#flowControlIsNotifiedOnRejectAndFinish
```

Expected: FAIL (SPI/constructor overload not present).

- [ ] **Step 3: Add SPI types + wire hooks**

Create `NettyFlowControl`:

```java
package yier.bubu.redis;

import io.netty.channel.Channel;

interface NettyFlowControl {
    void onSubmitRejected(Channel ch, NettyCommandExecutor.SubmitRejectReason reason);
    void onCommandFinished(Channel ch);
}
```

Create `NoopNettyFlowControl`:

```java
package yier.bubu.redis;

import io.netty.channel.Channel;

final class NoopNettyFlowControl implements NettyFlowControl {
    static final NoopNettyFlowControl INSTANCE = new NoopNettyFlowControl();

    private NoopNettyFlowControl() {
    }

    @Override
    public void onSubmitRejected(Channel ch, NettyCommandExecutor.SubmitRejectReason reason) {
        // no-op
    }

    @Override
    public void onCommandFinished(Channel ch) {
        // no-op
    }
}
```

In `NettyCommandExecutor`, add a new constructor overload with an extra last parameter:

```java
NettyCommandExecutor(
        Runnable bindToCurrentThread,
        YierdisFastCommandProcessor commandProcessor,
        EventExecutor executor,
        ReplyWriterFactory replyWriterFactory,
        int queueCapacity,
        long queueMaxBytes,
        int maxDrainCommands,
        long drainTimeLimitMillis,
        SchedulingPolicy schedulingPolicy,
        NettyFlowControl flowControl
)
```

Public constructors should default to `NoopNettyFlowControl.INSTANCE`.

In `NettyCommandSubmitter`, call `flowControl.onSubmitRejected(ch, reason)` right before returning a non-null `SubmitRejectReason`.

In `NettyCommandExecutionSupport.onCommandFinished(...)`, call `flowControl.onCommandFinished(ch)` after releasing budgets.

- [ ] **Step 4: Run the single test**

Run:
```bash
mvn -pl yierdis-server test -Dtest=NettyCommandExecutorTest#flowControlIsNotifiedOnRejectAndFinish
```

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-server/src/main/java/yier/bubu/redis/NettyFlowControl.java \
  yierdis-server/src/main/java/yier/bubu/redis/NoopNettyFlowControl.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java
git commit -m "feat(server): add NettyFlowControl SPI (noop)"
```

---

### Task 4: Remove Backpressure Runtime State + Observability Fields

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`

- [ ] **Step 1: Update connection stats snapshot shape**

In `ServerRuntimeState`, remove:

```diff
-    private final AtomicBoolean autoReadDisabledByExecutor = new AtomicBoolean(false);
...
-    private final AtomicLong backpressureEnter = new AtomicLong(0);
-    private final AtomicLong backpressureExit = new AtomicLong(0);
```

And remove the related accessors:

```diff
-    boolean markAutoReadDisabledByExecutor() { ... }
-    boolean clearAutoReadDisabledByExecutor() { ... }
-    boolean autoReadDisabledByExecutor() { ... }
...
-    AtomicLong backpressureEnterCounter() { ... }
-    AtomicLong backpressureExitCounter() { ... }
```

In `ServerConnectionContext`, delete the wrappers:

```diff
-    boolean markAutoReadDisabledByExecutor() { ... }
-    boolean autoReadDisabledByExecutor() { ... }
-    boolean clearAutoReadDisabledByExecutor() { ... }
-    void recordBackpressureEnter() { ... }
-    void recordBackpressureExit() { ... }
```

Update `ConnectionStatsSnapshot` to remove the deleted fields:

```diff
    record ConnectionStatsSnapshot(
            int pending,
            long pendingBytes,
-            boolean autoReadDisabledByExecutor,
            boolean closing,
            long commandsEnqueued,
            long commandsExecuted,
            long commandsRejected,
            long commandsSkippedClosing,
            long closeAfterReply,
-            long backpressureEnter,
-            long backpressureExit
    ) {
    }
```

Update `ServerSessionState.connectionStatsSnapshot()` and `ServerConnectionContext.statsSnapshot()` constructors to match.

- [ ] **Step 2: Remove backpressure fields from executor StatsSnapshot**

In `NettyCommandExecutor.StatsSnapshot`, remove:

- backpressure watermarks
- global backpressure watermarks
- `channelsAutoReadDisabled`
- `backpressureEnter`
- `backpressureExit`

Also remove their wiring in `statsSnapshot()` and the corresponding constants in `NettyServerInfoProvider`.

- [ ] **Step 3: Update STATS/INFO outputs to remove backpressure keys**

In `NettyServerInfoProvider`, delete these keys and any `writePair` usage:

- `backpressure_*` (structured INFO)
- `channels_autoread_disabled`
- `backpressure_enter_total` / `backpressure_exit_total`
- `conn_autoread_disabled_by_executor`
- `conn_backpressure_enter` / `conn_backpressure_exit`

Update `pairs` counts accordingly:

- `stats()` pairs: remove 3 global pairs (channels_autoread_disabled, backpressure_enter_total, backpressure_exit_total)
- connection pairs: remove 3 conn pairs (conn_autoread_disabled_by_executor, conn_backpressure_enter, conn_backpressure_exit)
- `INFO yierdis` pairs: remove 4 backpressure watermark pairs

- [ ] **Step 4: Run server tests**

Run:
```bash
mvn -pl yierdis-server test
```

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java \
  yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java \
  yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java
git commit -m "refactor(server): drop backpressure runtime state and stats fields"
```

---

### Task 5: Delete Executor-Core Backpressure Helpers (Now Unused)

**Files:**
- Delete: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureController.java`
- Delete: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureIo.java`
- Delete: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureRuntime.java`
- Delete: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureObserver.java`

- [ ] **Step 1: Delete the four files**

```bash
git rm \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureController.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureIo.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureRuntime.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutorBackpressureObserver.java
```

- [ ] **Step 2: Run full tests**

Run:
```bash
mvn test
```

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git commit -m "refactor(executor): remove unused backpressure helper classes"
```

---

### Task 6: Update README (Remove Backpressure Flags, Document Busy-Only)

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the “协议上限与反压” section to remove backpressure flags**

Replace the backpressure bullets with budget-only bullets:

```diff
-## 协议上限与反压（推荐）
+## 协议上限与过载保护（推荐）
 ...
 - `--protocolMaxBulkBytes <bytes>` / `--protocolMaxArgs <n>` / `--protocolMaxLineBytes <bytes>`：输入上限（DoS 防护）
 - `--executorQueueCapacity <n>`：全局执行队列条数上限（有界队列）
 - `--executorQueueMaxBytes <bytes>`：全局执行队列 bytes 上限（`0` 表示禁用）
- - `--backpressureHigh/--backpressureLow`：连接级条数背压水位线（滞回）
- - `--backpressureBytesHigh/--backpressureBytesLow`：连接级 bytes 背压水位线（滞回；`0` 表示禁用）
```

Add a short note under “开放网络环境建议” explaining当前为 busy-only：

```markdown
- 当前实现为 busy-only（只返回 `ERR busy <reason>`，不做 `autoRead` 背压）；慢客户端可能导致出站 buffer 增长，公网环境不建议使用默认配置。
```

- [ ] **Step 2: Run a quick doc sanity check**

Run:
```bash
rg -n \"backpressureHigh|backpressureLow|backpressureBytes\" README.md
```

Expected: no matches.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: remove backpressure flags (busy-only overload protection)"
```

---

### Task 7: Final Verification

**Files:**
- None

- [ ] **Step 1: Full test run**

Run:
```bash
mvn test
```

Expected: `BUILD SUCCESS`

- [ ] **Step 2: Smoke run server (optional manual)**

Run:
```bash
mvn -DskipTests package
java -jar yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 6378
```

Expected: server starts; `yierdis-client` can `PING`; when queue is full, replies contain `ERR busy <reason>`.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-18-busy-only-flow-control.md`. Two execution options:

1. Subagent-Driven (recommended) - I dispatch a fresh subagent per task, review between tasks, fast iteration
2. Inline Execution - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
