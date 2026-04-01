# Architecture Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the full Yierdis architecture remediation across `server`, `core-db`, and `protocol` while preserving external protocol and command behavior.

**Architecture:** Execute the work in three waves. Wave 1 unifies server connection ownership, removes server-only command metadata leakage from `core-command`, and consolidates off-heap API ownership. Wave 2 reduces `YierdisDb` responsibility density by extracting real package-local collaborators behind a narrow internal seam. Wave 3 decouples protocol request decoding from execution contracts and formalizes `ReplyWriter` as the single server reply semantic source of truth.

**Tech Stack:** Java 17, Maven multi-module reactor, Netty 4, JUnit 4

---

### Task 1: Unify Server Connection Ownership

**Files:**
- Create: `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorChannelState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Create: `yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Write failing context-ownership tests**

```java
public class ServerConnectionContextTest {
    @Test
    public void getOrCreateReturnsSingleStateRoot() {
        NioSocketChannel channel = new NioSocketChannel();
        try {
            ServerConnectionContext a = ServerConnectionContext.getOrCreate(channel);
            ServerConnectionContext b = ServerConnectionContext.getOrCreate(channel);

            Assert.assertSame(a, b);
            Assert.assertSame(a.session(), b.session());
            Assert.assertSame(a.runtime(), b.runtime());
            Assert.assertSame(a.scheduling(), b.scheduling());
        } finally {
            channel.unsafe().closeForcibly();
        }
    }
}
```

- [ ] **Step 2: Write failing guard test that only the new context owns `Channel.attr(...)`**

```java
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis"),
        offenders,
        "channel.attr(",
        "Channel.attr("
);
allowOnly(
        offenders,
        "yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java"
);
```

- [ ] **Step 3: Run focused server tests to verify RED**

Run: `mvn -pl yierdis-server -am -Dtest=ServerConnectionContextTest,YierdisServerBootstrapCommandWiringTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `ServerConnectionContext` does not exist and guardrails still observe direct `Channel.attr(...)` ownership in multiple files.

- [ ] **Step 4: Implement the single server context and migrate wrappers/callers**

```java
final class ServerConnectionContext {
    private static final AttributeKey<ServerConnectionContext> KEY =
            AttributeKey.valueOf("yierdis.serverConnectionContext");

    static ServerConnectionContext getOrCreate(Channel channel) {
        Attribute<ServerConnectionContext> attr = channel.attr(KEY);
        ServerConnectionContext existing = attr.get();
        if (existing != null) {
            return existing;
        }
        ServerConnectionContext created = new ServerConnectionContext();
        ServerConnectionContext raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    private final ServerSessionState session = new ServerSessionState();
    private final ServerRuntimeState runtime = new ServerRuntimeState();
    private final NettyExecutorChannelState scheduling = new NettyExecutorChannelState();

    ServerSessionState session() { return session; }
    ServerRuntimeState runtime() { return runtime; }
    NettyExecutorChannelState scheduling() { return scheduling; }
}
```

```java
@Override
protected void initChannel(SocketChannel ch) {
    ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
    context.configureTransactionLimits(
            config.transactionQueueMaxCommands(),
            config.transactionQueueMaxBytes()
    );
    ch.pipeline()
            .addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(executor))
            .addLast("customRequestDecoder", new CustomRequestDecoder(
                    config.protocolMaxBulkBytes(),
                    config.protocolMaxArgs(),
                    config.protocolMaxLineBytes()
            ))
            .addLast("protocolErrorReply", new ProtocolErrorReplyHandler(executor))
            .addLast("commandHandler", new YierdisFastCommandHandler(executor));
}
```

- [ ] **Step 5: Run the focused server tests to verify GREEN**

Run: `mvn -pl yierdis-server -am -Dtest=ServerConnectionContextTest,YierdisServerBootstrapCommandWiringTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorChannelState.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: unify server connection context"
```

### Task 2: Move Command Metadata Into Registry-Owned Descriptors

**Files:**
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDescriptor.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Write failing descriptor tests**

```java
public class CommandDescriptorRegistryTest {
    @Test
    public void serverCommandsExposeDescriptorsThroughRegistry() {
        CommandRegistry registry = new CommandRegistry();
        new ServerCommandModule(new StubInfoProvider()).register(registry);

        CommandDescriptor hello = registry.descriptor("HELLO");

        Assert.assertNotNull(hello);
        Assert.assertEquals(-1, hello.arity());
        Assert.assertEquals(0, hello.firstKey());
        Assert.assertEquals(0, hello.lastKey());
        Assert.assertEquals(0, hello.step());
    }
}
```

- [ ] **Step 2: Write failing guard that `CoreConnectionCommands` no longer hardcodes `HELLO/INFO/STATS` metadata**

```java
scanFileForForbiddenText(
        repoRoot,
        coreConnectionCommandsFile,
        offenders,
        "case \"HELLO\"",
        "case \"INFO\"",
        "case \"STATS\""
);
```

- [ ] **Step 3: Run focused registry and wiring tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandRegistryGuardTest,YierdisServerBootstrapCommandWiringTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because descriptors are not modeled in the registry and `CoreConnectionCommands` still owns server-only metadata.

- [ ] **Step 4: Add descriptor support and migrate `COMMAND` metadata reads**

```java
public record CommandDescriptor(
        String upperName,
        int arity,
        int firstKey,
        int lastKey,
        int step
) {}
```

```java
public interface Registration {
    void register(String name, Handler handler, CommandDescriptor descriptor);

    void registerDisallowedInMulti(
            String name,
            Handler handler,
            CommandDescriptor descriptor,
            String errorMessage
    );
}
```

```java
private static void command(Command cmd, ReplyWriter out, CommandRegistry registry) {
    CommandDescriptor descriptor = registry.descriptor(upper);
    writeCommandInfo(out, descriptor);
}
```

- [ ] **Step 5: Register server descriptors from `ServerCommandModule` and delete the old switches**

```java
registration.registerDisallowedInMulti(
        "HELLO",
        this::hello,
        new CommandDescriptor("HELLO", -1, 0, 0, 0),
        "ERR HELLO is not allowed in MULTI"
);
registration.register(
        "INFO",
        this::info,
        new CommandDescriptor("INFO", -1, 0, 0, 0)
);
registration.register(
        "STATS",
        this::stats,
        new CommandDescriptor("STATS", 1, 0, 0, 0)
);
```

- [ ] **Step 6: Run focused registry and wiring tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandRegistryGuardTest,YierdisServerBootstrapCommandWiringTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDescriptor.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: move command descriptors into registry"
```

### Task 3: Consolidate Off-Heap API Ownership

**Files:**
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorProvider.java`
- Modify: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocators.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocator.java`
- Modify: `yierdis-memory/netty/src/main/java/yier/bubu/redis/db/memory/netty/NettyOffHeapAllocatorProvider.java`
- Modify: `yierdis-memory/unsafe/src/main/java/yier/bubu/redis/db/memory/unsafe/UnsafeOffHeapAllocatorProvider.java`
- Modify: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/ForeignOffHeapAllocatorProvider.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`
- Modify: `yierdis-memory/netty/src/test/java/yier/bubu/redis/db/memory/netty/YierdisNettyOffHeapAllocatorTest.java`
- Modify: `yierdis-memory/unsafe/src/test/java/yier/bubu/redis/db/memory/unsafe/YierdisUnsafeOffHeapAllocatorTest.java`
- Modify: `yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocatorTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Write failing guard test that server no longer imports memory-only allocator contracts**

```java
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java"),
        offenders,
        "import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;"
);
```

- [ ] **Step 2: Run focused memory/server tests to verify RED**

Run: `mvn -pl yierdis-memory/api,yierdis-memory/netty,yierdis-memory/unsafe,yierdis-memory/foreign,yierdis-server -am -Dtest=YierdisServerBootstrapCloseTest,YierdisNettyOffHeapAllocatorTest,YierdisUnsafeOffHeapAllocatorTest,YierdisForeignOffHeapAllocatorTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL after the guard is added because provider discovery still returns the memory-only allocator hierarchy.

- [ ] **Step 3: Change provider discovery to return core `OffHeapAllocator`**

```java
public interface YierdisOffHeapAllocatorProvider {
    YierdisOffHeapBackend backend();

    OffHeapAllocator create(long maxBytes);
}
```

```java
public static OffHeapAllocator create(YierdisOffHeapBackend backend, long maxBytes) {
    if (backend == null || backend == YierdisOffHeapBackend.NONE) {
        return null;
    }
    YierdisOffHeapAllocatorProvider provider = findProvider(backend);
    if (provider == null) {
        throw new YierdisOffHeapBackendUnavailableException("missing provider for backend: " + backend);
    }
    return provider.create(maxBytes);
}
```

- [ ] **Step 4: Remove the parallel allocator type and update all callers**

```java
private OffHeapAllocator offHeapAllocator;

offHeapAllocator = YierdisOffHeapAllocators.create(backend, runtimeConfig.offheapMaxBytes());
instance = YierdisInstance.create(
        YierdisInstanceConfig.builder()
                .offHeapAllocator(offHeapAllocator)
                .build()
);
```

- [ ] **Step 5: Run focused memory/server tests to verify GREEN**

Run: `mvn -pl yierdis-memory/api,yierdis-memory/netty,yierdis-memory/unsafe,yierdis-memory/foreign,yierdis-server -am -Dtest=YierdisServerBootstrapCloseTest,YierdisNettyOffHeapAllocatorTest,YierdisUnsafeOffHeapAllocatorTest,YierdisForeignOffHeapAllocatorTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorProvider.java \
        yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocators.java \
        yierdis-memory/netty/src/main/java/yier/bubu/redis/db/memory/netty/NettyOffHeapAllocatorProvider.java \
        yierdis-memory/unsafe/src/main/java/yier/bubu/redis/db/memory/unsafe/UnsafeOffHeapAllocatorProvider.java \
        yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/ForeignOffHeapAllocatorProvider.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java \
        yierdis-memory/netty/src/test/java/yier/bubu/redis/db/memory/netty/YierdisNettyOffHeapAllocatorTest.java \
        yierdis-memory/unsafe/src/test/java/yier/bubu/redis/db/memory/unsafe/YierdisUnsafeOffHeapAllocatorTest.java \
        yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocatorTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocator.java
git commit -m "refactor: consolidate offheap allocator contract"
```

### Task 4: Introduce `YierdisDbInternals` and Extract String/TTL/Keyspace Ops

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java`

- [ ] **Step 1: Write failing architecture guard that string and TTL methods are no longer owned directly by `YierdisDb`**

```java
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "setString", byte[].class, byte[].class, SetMode.class, ExpireOption.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "getStringBytes", byte[].class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "expire", BytesView.class, long.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "keys", byte[].class, int.class, long.class));
```

- [ ] **Step 2: Run focused DB/runtime tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,CommandProcessorTest,ExpireSemanticsTest,TtlMaxmemoryTest,MemoryStatsCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the methods still live on `YierdisDb`.

- [ ] **Step 3: Add the internal seam and move string/TTL/keyspace logic behind it**

```java
interface YierdisDbInternals {
    void checkThread();
    MemoryReservation reserveMutation(long estimatedUpperBoundBytes);
    void commitMutation(MemoryReservation reservation, long actualDeltaBytes);
    void rollbackMutation(MemoryReservation reservation);
    void touch(YierdisObject object);
    boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis);
    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis);
}
```

```java
final class YierdisStringOps {
    private final YierdisDbInternals internals;

    YierdisStringOps(YierdisDbInternals internals) {
        this.internals = internals;
    }

    boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        return setStringWithResult(keyBytes, value, mode, expireOption, false).applied();
    }

    BulkStringValue getStringValue(BytesView keyView) {
        internals.checkThread();
        return liveStringValue(keyView);
    }

    int append(byte[] keyBytes, BytesSlice value) {
        internals.checkThread();
        return appendInternal(keyBytes, value);
    }
}
```

- [ ] **Step 4: Rebuild `YierdisDbReads` and `YierdisDbWrites` over the extracted collaborators**

```java
final class YierdisDbWrites implements DbWrites {
    YierdisDbWrites(
            YierdisStringOps strings,
            YierdisHashOps hashes,
            YierdisListOps lists,
            YierdisSetOps sets,
            YierdisZSetOps zsets,
            YierdisHllOps hll,
            YierdisKeyspaceOps keyspace,
            YierdisTtlOps ttl
    ) {
        this.strings = strings;
        this.hashes = hashes;
        this.lists = lists;
        this.sets = sets;
        this.zsets = zsets;
        this.hll = hll;
        this.ttl = ttl;
        this.keyspace = keyspace;
    }
}
```

- [ ] **Step 5: Run focused DB/runtime tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,CommandProcessorTest,ExpireSemanticsTest,TtlMaxmemoryTest,MemoryStatsCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java
git commit -m "refactor: extract string ttl and keyspace db ops"
```

### Task 5: Extract Collection-Family DB Collaborators and Shrink `YierdisDb`

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HashCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ListCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ZSetCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java`

- [ ] **Step 1: Extend the failing guard to cover collection-family methods**

```java
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "hset", byte[].class, List.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "lpush", byte[].class, List.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "sadd", byte[].class, List.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "zadd", byte[].class, List.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "pfadd", byte[].class, List.class));
```

- [ ] **Step 2: Run focused collection command tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,HashCommandTest,ListCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the collection-family methods still live on `YierdisDb`.

- [ ] **Step 3: Extract the collection-family collaborators**

```java
final class YierdisHashOps {
    private final YierdisDbInternals internals;

    YierdisHashOps(YierdisDbInternals internals) {
        this.internals = internals;
    }

    int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        internals.checkThread();
        return hsetInternal(keyBytes, fieldValuePairs);
    }

    BulkStringMapPairs hgetall(byte[] keyBytes) {
        internals.checkThread();
        return readAllPairs(keyBytes);
    }

    int hdel(byte[] keyBytes, List<byte[]> fields) {
        internals.checkThread();
        return hdelInternal(keyBytes, fields);
    }
}
```

```java
final class YierdisZSetOps {
    private final YierdisDbInternals internals;

    YierdisZSetOps(YierdisDbInternals internals) {
        this.internals = internals;
    }

    int zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        internals.checkThread();
        return zaddInternal(keyBytes, scoreMemberPairs);
    }

    BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        return zrangeSequence(keyBytes, start, stop, withScores);
    }

    int zrem(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        return zremInternal(keyBytes, members);
    }
}
```

- [ ] **Step 4: Rewire reads/writes and remove the migrated collection methods from `YierdisDb`**

```java
this.reads = new YierdisDbReads(strings, hashes, lists, sets, zsets, hll, keyspace, ttl);
this.writes = new YierdisDbWrites(strings, hashes, lists, sets, zsets, hll, keyspace, ttl);
```

- [ ] **Step 5: Run focused collection command tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,HashCommandTest,ListCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HashCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ListCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ZSetCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java
git commit -m "refactor: extract collection db ops"
```

### Task 6: Decouple Protocol Request Models From Execution Contracts

**Files:**
- Create: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java`
- Modify: `yierdis-protocol/yierdis-protocol-codec/pom.xml`
- Modify: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestEncoder.java`
- Delete: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomCommand.java`
- Modify: `yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java`
- Create: `yierdis-protocol/yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Write failing protocol-model tests**

```java
public class CustomRequestDecoderTest {
    @Test
    public void decoderEmitsProtocolRequestModelInsteadOfCommand() {
        EmbeddedChannel channel = new EmbeddedChannel(new CustomRequestDecoder(1024, 16, 128));
        assertTrue(channel.writeInbound(frame("{\"cmd\":\"PING\",\"args\":[]}")));

        Object decoded = channel.readInbound();

        Assert.assertTrue(decoded instanceof CustomProtocolV1Request);
        Assert.assertFalse(decoded instanceof Command);
    }
}
```

- [ ] **Step 2: Run focused protocol/server/client tests to verify RED**

Run: `mvn -pl yierdis-protocol/yierdis-protocol-codec,yierdis-protocol/yierdis-protocol-netty,yierdis-client,yierdis-server -am -Dtest=CustomRequestDecoderTest,YierdisClientTest,YierdisServerBootstrapCommandWiringTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because protocol decode still emits `CustomCommand` implementing `Command`.

- [ ] **Step 3: Add the wire-focused request model and retarget the decoder**

```java
public record CustomProtocolV1Request(String cmd, List<String> args) {
    public CustomProtocolV1Request {
        Objects.requireNonNull(cmd, "cmd");
        args = args == null ? List.of() : List.copyOf(args);
    }
}
```

```java
private CustomProtocolV1Request parseCommandPayload(ByteBuf payload) {
    JsonValue v = parsePayloadJson(payload);
    if (!(v instanceof JsonObject obj)) {
        throw new IllegalArgumentException("request must be a JSON object");
    }
    Map<String, JsonValue> map = obj.values();
    JsonValue cmdVal = map.get("cmd");
    if (!(cmdVal instanceof JsonString cmdString)) {
        throw new IllegalArgumentException("cmd must be a string");
    }
    List<String> args = decodeArgs(map.get("args"));
    String cmd = cmdString.value();
    return new CustomProtocolV1Request(cmd, args);
}
```

- [ ] **Step 4: Add the server-side adapter and keep pipeline behavior compatible**

```java
final class ProtocolCommandAdapter implements Command {
    static ProtocolCommandAdapter from(CustomProtocolV1Request request) {
        List<String> args = request.args();
        byte[][] argv = new byte[1 + args.size()][];
        argv[0] = request.cmd().getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            argv[i + 1] = arg == null ? null : arg.getBytes(StandardCharsets.UTF_8);
        }
        return new ProtocolCommandAdapter(argv);
    }
}
```

```java
ch.pipeline()
        .addLast("customRequestDecoder", new CustomRequestDecoder(
                config.protocolMaxBulkBytes(),
                config.protocolMaxArgs(),
                config.protocolMaxLineBytes()
        ))
        .addLast("protocolCommandAdapter", new ProtocolRequestToCommandHandler())
        .addLast("protocolErrorReply", new ProtocolErrorReplyHandler(executor))
        .addLast("commandHandler", new YierdisFastCommandHandler(executor));
```

- [ ] **Step 5: Remove the `core-contract` dependency from protocol-codec and update callers**

Run: `mvn -pl yierdis-protocol/yierdis-protocol-codec,yierdis-protocol/yierdis-protocol-netty,yierdis-client,yierdis-server -am -Dtest=CustomRequestDecoderTest,YierdisClientTest,YierdisServerBootstrapCommandWiringTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java \
        yierdis-protocol/yierdis-protocol-codec/pom.xml \
        yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestEncoder.java \
        yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java \
        yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerChannelInitializer.java \
        yierdis-protocol/yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java \
        yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git rm yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomCommand.java
git commit -m "refactor: decouple protocol requests from command contracts"
```

### Task 7: Formalize `ReplyWriter` As the Server Reply SSOT and Finish Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-31-architecture-remediation-design.md`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`
- Modify: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/reply/ReplyValue.java`
- Modify: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java`

- [ ] **Step 1: Write failing guard tests that server/core production code do not use `ReplyValue` as an alternate write-path authority**

```java
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java"),
        offenders,
        "import yier.bubu.redis.protocol.reply.ReplyValue",
        "writeOkEnvelope(",
        "writeValue(out, value)"
);
```

```java
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/src/main/java"),
        offenders,
        "import yier.bubu.redis.protocol.reply.ReplyValue"
);
```

- [ ] **Step 2: Run focused guard tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ReplySsoTGuardTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL until the new guardrail is encoded and docs/comments are aligned with the intended single-source model.

- [ ] **Step 3: Tighten documentation and code comments so `ReplyValue` is clearly tooling/client-side**

```java
/**
 * Reply model for protocol-side tooling and parsing.
 * Server command execution writes replies through ReplyWriter.
 */
public sealed interface ReplyValue permits ReplyNull, ReplyBoolean, ReplyLong, ReplyDouble, ReplyString, ReplyBytes, ReplyArray, ReplyMap, ReplyError
```

```java
/**
 * Encoder used for protocol-side tools and ReplyValue support.
 * The server write path remains ReplyWriter-based.
 */
public final class CustomProtocolV1NdjsonEncoder {
    private CustomProtocolV1NdjsonEncoder() {
    }
}
```

- [ ] **Step 4: Run the full targeted verification set**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server,yierdis-client,yierdis-bench,yierdis-protocol/yierdis-protocol-codec,yierdis-protocol/yierdis-protocol-netty -am test`
Expected: PASS

- [ ] **Step 5: Run repository verification**

Run: `mvn test`
Expected: PASS

- [ ] **Step 6: Review diff shape**

Run: `git diff --stat`
Expected: changes are concentrated in `yierdis-server`, `yierdis-core`, `yierdis-memory`, and `yierdis-protocol`, with docs and guardrails updated in step with implementation.

- [ ] **Step 7: Commit**

```bash
git add README.md \
        docs/superpowers/specs/2026-03-31-architecture-remediation-design.md \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java \
        yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/reply/ReplyValue.java \
        yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1NdjsonEncoder.java
git commit -m "docs: finalize reply ssot architecture guardrails"
```
