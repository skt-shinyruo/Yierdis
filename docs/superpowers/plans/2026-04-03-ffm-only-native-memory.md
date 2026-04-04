# FFM-Only Native Memory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every non-FFM memory path and make JDK 25 `java.lang.foreign` the default and only native-memory implementation for Yierdis, including keyspace, expires, and all value types.

**Architecture:** First collapse the public/config/build surface so the server always boots in FFM mode. Then add FFM-native runtime primitives, thread them through `YierdisInstance` and `YierdisDb`, migrate key/index/value storage from address-based helpers to FFM-backed structures, and finally delete the legacy `unsafe` / `netty` codepaths, modules, tests, and docs.

**Tech Stack:** Java 25, Maven multi-module reactor, JUnit 4, Picocli, Netty server runtime, JDK 25 Foreign Function and Memory API

---

## File Map

This spec is one coordinated subsystem refactor, not multiple independent features. The tasks stay in one plan because they are sequentially dependent and share the same runtime/storage boundary.

**Config / startup / tooling**
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBenchArgs.java`

**FFM runtime primitives**
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntime.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmRegion.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmSpan.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmAccess.java`
- Modify: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocator.java`

**DB / runtime wiring**
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java`

**FFM key / index / value storage**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBytesRef.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBlobStore.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmKeyspace.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmExpireIndex.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmString.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmListpack.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmQuickList.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmDictLong.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmIntSet.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmZSet.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`

**Build / cleanup / docs**
- Modify: `yierdis-memory/pom.xml`
- Modify: `yierdis-core/yierdis-core-db/pom.xml`
- Modify: `yierdis-core/yierdis-core-runtime/pom.xml`
- Modify: `yierdis-server/pom.xml`
- Modify: `pom.xml`
- Modify: `README.md`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/MaxmemoryScopeTest.java`
- Delete: `yierdis-memory/netty/**`
- Delete: `yierdis-memory/unsafe/**`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/**`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocators.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorProvider.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBackend.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAddressAllocator.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBlock.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAddressAllocator.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapBlock.java`

### Task 1: Remove Backend/Toggles From CLI and Startup Surface

**Files:**
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ForeignMemoryAutoModulesTest.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBenchArgs.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/MaxmemoryScopeTest.java`

- [ ] **Step 1: Write failing tests for the deleted CLI flags and FFM-only runtime config**

```java
@Test
public void normalizedArgsConvertToRuntimeConfigWithoutOffheapFields() {
    YierdisServerArgs args = parse(
            "--port", "6380",
            "--databases", "32",
            "--noCleanup",
            "--maxmemoryBytes", "1048576",
            "--maxmemoryScope", "Per_Db",
            "--maxmemoryPolicy", "ALLKEYS-RANDOM",
            "--maxmemorySamples", "9",
            "--evictionTimeLimitMillis", "11",
            "--expireCleanupTimeLimitMillis", "13",
            "--keysTimeBudgetMillis", "17",
            "--keysMaxResults", "23"
    );

    args.normalizeAndValidate();

    Assert.assertEquals(
            new YierdisServerRuntimeConfig(
                    6380,
                    32,
                    0,
                    1,
                    1024,
                    67108864L,
                    YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.FAIR,
                    256,
                    128,
                    16777216L,
                    8388608L,
                    512,
                    2,
                    1024,
                    67108864L,
                    DEFAULT_PROTOCOL_MAX_BULK_BYTES,
                    DEFAULT_PROTOCOL_MAX_ARGS,
                    DEFAULT_PROTOCOL_MAX_LINE_BYTES,
                    1048576L,
                    YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB,
                    YierdisServerRuntimeConfig.MaxmemoryPolicy.ALLKEYS_RANDOM,
                    9,
                    11,
                    13,
                    17,
                    23
            ),
            args.toRuntimeConfig()
    );
}

@Test
public void deletedOffheapFlagsFailFast() {
    String err = captureStderr(() -> {
        YierdisCliException error = assertThrows(YierdisCliException.class, () -> ServerConfig.fromArgs(new String[]{
                "--offheapBackend", "foreign"
        }));
        Assert.assertEquals(2, error.exitCode());
        Assert.assertTrue(error.shouldPrintUsage());
    });

    Assert.assertTrue(err.contains("--offheapBackend"));
}

@Test
public void ffmAvailabilityCheckPassesOnJdk25() {
    ForeignMemoryAutoModules.ensureFfmAvailable();
}
```

- [ ] **Step 2: Run the focused config/startup tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-args,yierdis-server,yierdis-client -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,ForeignMemoryAutoModulesTest,MaxmemoryScopeTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the runtime config still carries `offheap*` fields, Picocli still accepts deleted flags, and server/bootstrap still reference backend selection.

- [ ] **Step 3: Implement the FFM-only CLI/startup surface**

```java
public record YierdisServerRuntimeConfig(
        int port,
        int databases,
        long cleanupIntervalMillis,
        int ioThreads,
        int executorQueueCapacity,
        long executorQueueMaxBytes,
        ExecutorSchedulingPolicy executorSchedulingPolicy,
        int backpressureHighWatermark,
        int backpressureLowWatermark,
        long backpressureBytesHighWatermark,
        long backpressureBytesLowWatermark,
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
}
```

```java
public final class YierdisServerArgs {
    @Option(names = YierdisServerArgNames.MAXMEMORY_BYTES, defaultValue = "0", description = "Maxmemory in bytes (0 disables eviction).")
    public long maxmemoryBytes = 0;

    public void normalizeAndValidate() {
        if (noCleanup) {
            cleanupIntervalMillis = 0;
        }
        maxmemoryScope = normalizeMaxmemoryScope(maxmemoryScope);
        maxmemoryPolicy = normalizeMaxmemoryPolicy(maxmemoryPolicy);
    }
}
```

```java
final class ForeignMemoryAutoModules {
    static void ensureFfmAvailable() {
        try {
            Class.forName("java.lang.foreign.Arena");
        } catch (ClassNotFoundException e) {
            throw YierdisCliException.userError(
                    "当前 JVM 不支持 java.lang.foreign。Yierdis 现在要求使用 JDK 25 运行。",
                    e
            );
        }
    }
}
```

```java
public static void main(String[] args) throws Exception {
    final ServerConfig config = ServerConfig.fromArgs(args);
    if (config == null) {
        return;
    }

    try {
        ForeignMemoryAutoModules.ensureFfmAvailable();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
            log.info("yierdis started on 0.0.0.0:{} (Custom Protocol v1)", server.port());
            server.awaitClose();
        }
    } catch (YierdisCliException e) {
        System.err.println(e.getMessage());
        System.exit(e.exitCode());
    }
}
```

```java
@Command(
        name = "yierdis-bench",
        description = "Pure Java benchmark tool for Yierdis (Custom Protocol v1 over TCP).",
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class YierdisBenchArgs {
    @Option(
            names = "--portBase",
            defaultValue = "16378",
            description = "Port for the auto-started server. In connect-only mode, this is the target port."
    )
    public int portBase = 16378;
}
```

- [ ] **Step 4: Run the focused config/startup tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-args,yierdis-server,yierdis-client -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,ForeignMemoryAutoModulesTest,MaxmemoryScopeTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the CLI/startup surface change**

```bash
git add yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgNames.java \
        yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java \
        yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java \
        yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServer.java \
        yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/ForeignMemoryAutoModulesTest.java \
        yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBenchArgs.java \
        yierdis-client/src/test/java/yier/bubu/redis/client/MaxmemoryScopeTest.java
git commit -m "refactor: remove offheap CLI toggles"
```

### Task 2: Add FFM Runtime Primitives and Make Them the Low-Level SSOT

**Files:**
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntime.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmRegion.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmSpan.java`
- Create: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmAccess.java`
- Create: `yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntimeTest.java`
- Modify: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocator.java`
- Modify: `yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocatorTest.java`

- [ ] **Step 1: Write failing tests for region lifecycle, spans, and allocator delegation**

```java
@Test
public void regionLifecycleUpdatesRuntimeAccounting() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-runtime")) {
        YierdisFfmRegion region = runtime.allocateRegion("test-region", 32);
        Assert.assertEquals(32L, runtime.usedBytes());
        region.close();
        Assert.assertEquals(0L, runtime.usedBytes());
    }
}

@Test
public void spanReadsAndWritesUseValueLayouts() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-runtime")) {
        YierdisFfmRegion region = runtime.allocateRegion("test-region", 8);
        YierdisFfmSpan span = region.span(0, 8);
        YierdisFfmAccess.setByte(span, 0, (byte) 'a');
        YierdisFfmAccess.setLong(span, 0, 42L);
        Assert.assertEquals(42L, YierdisFfmAccess.getLong(span, 0));
        region.close();
    }
}

@Test
public void foreignAllocatorUsesFfmRuntimeForBufOwnership() {
    try (YierdisForeignOffHeapAllocator allocator = new YierdisForeignOffHeapAllocator(0)) {
        YierdisOffHeapBuf buf = allocator.allocate(16);
        buf.setByte(0, (byte) 'x');
        Assert.assertEquals('x', buf.getByte(0));
        buf.close();
        Assert.assertEquals(0L, allocator.usedBytes());
    }
}
```

- [ ] **Step 2: Run the foreign-module tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/foreign -am -Dtest=YierdisFfmMemoryRuntimeTest,YierdisForeignOffHeapAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the new FFM runtime classes do not exist and the allocator still owns memory directly.

- [ ] **Step 3: Implement the FFM runtime primitives and delegate the allocator to them**

```java
public final class YierdisFfmMemoryRuntime implements AutoCloseable {
    private final String name;
    private final java.util.concurrent.atomic.AtomicLong usedBytes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.Set<YierdisFfmRegion> liveRegions = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    public YierdisFfmMemoryRuntime(String name) {
        this.name = name;
    }

    public YierdisFfmRegion allocateRegion(String owner, int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        Arena arena = Arena.ofConfined();
        MemorySegment segment = arena.allocate(bytes);
        YierdisFfmRegion region = new YierdisFfmRegion(this, owner, arena, segment, bytes);
        liveRegions.add(region);
        usedBytes.addAndGet(bytes);
        return region;
    }

    void onRegionClosed(YierdisFfmRegion region) {
        if (liveRegions.remove(region)) {
            usedBytes.addAndGet(-region.size());
        }
    }

    public long usedBytes() {
        return usedBytes.get();
    }

    @Override
    public void close() {
        if (!liveRegions.isEmpty()) {
            throw new IllegalStateException("native memory leak in " + name + ": " + liveRegions.size() + " live regions");
        }
    }
}
```

```java
public record YierdisFfmSpan(MemorySegment segment) {
    public int size() {
        return Math.toIntExact(segment.byteSize());
    }

    public YierdisFfmSpan slice(int offset, int length) {
        return new YierdisFfmSpan(segment.asSlice(offset, length));
    }
}
```

```java
public final class YierdisFfmAccess {
    private YierdisFfmAccess() {
    }

    public static byte getByte(YierdisFfmSpan span, int offset) {
        return span.segment().get(ValueLayout.JAVA_BYTE, offset);
    }

    public static void setByte(YierdisFfmSpan span, int offset, byte value) {
        span.segment().set(ValueLayout.JAVA_BYTE, offset, value);
    }

    public static long getLong(YierdisFfmSpan span, int offset) {
        return span.segment().get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    public static void setLong(YierdisFfmSpan span, int offset, long value) {
        span.segment().set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value);
    }
}
```

```java
public final class YierdisForeignOffHeapAllocator implements OffHeapAllocator {
    private final long maxBytes;
    private final YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("foreign-allocator");

    @Override
    public YierdisOffHeapBuf allocate(int capacity) {
        YierdisFfmRegion region = runtime.allocateRegion("buf", capacity);
        return new YierdisForeignOffHeapBuf(this, region, capacity);
    }

    @Override
    public long usedBytes() {
        return runtime.usedBytes();
    }
}
```

- [ ] **Step 4: Run the foreign-module tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/foreign -am -Dtest=YierdisFfmMemoryRuntimeTest,YierdisForeignOffHeapAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the low-level FFM runtime**

```bash
git add yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntime.java \
        yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmRegion.java \
        yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmSpan.java \
        yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisFfmAccess.java \
        yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocator.java \
        yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisFfmMemoryRuntimeTest.java \
        yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocatorTest.java
git commit -m "feat: add FFM runtime primitives"
```

### Task 3: Make `YierdisInstance` and `YierdisDb` Own a Shared FFM Runtime

**Files:**
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java`

- [ ] **Step 1: Write failing tests for shared runtime ownership and the simplified factory signature**

```java
@Test
public void globalMaxmemoryCountsSharedFfmRuntimeOnceAcrossDbs() {
    YierdisInstanceConfig config = YierdisInstanceConfig.builder()
            .databases(2)
            .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
            .maxmemoryBytes(9000)
            .maxmemoryPolicy("noeviction")
            .build();

    try (YierdisInstance instance = YierdisInstance.create(config)) {
        instance.bindToCurrentThread();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        TestSession session = new TestSession();
        byte[] value = new byte[4000];
        Arrays.fill(value, (byte) 'a');

        try (FastTestClient client = new FastTestClient(processor, session)) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("k0"), value))).value());
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());
            Object reply = client.execute(Arrays.asList(b("SET"), b("k1"), value));
            Assert.assertFalse(reply instanceof ReplyError);
        }
    }
}

@Test
public void smokeAcrossFfmRuntimeAndMaxmemoryScopes() {
    runCase(YierdisInstanceConfig.MaxmemoryScope.GLOBAL);
    runCase(YierdisInstanceConfig.MaxmemoryScope.PER_DB);
}
```

- [ ] **Step 2: Run the runtime tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest,ContractsIntegrationSmokeTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the runtime still depends on injected allocators, ownership flags, and `offHeapKeysEnabled`.

- [ ] **Step 3: Replace allocator plumbing with one shared FFM runtime**

```java
public interface DbEngineFactory {
    RuntimeDbEngine create(
            int dbIndex,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    );
}
```

```java
public final class YierdisDbEngineFactory implements DbEngineFactory {
    private final YierdisFfmMemoryRuntime memoryRuntime;

    public YierdisDbEngineFactory(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
    }

    @Override
    public RuntimeDbEngine create(
            int dbIndex,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(memoryRuntime, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }
}
```

```java
public final class YierdisInstanceConfig {
    private final int databases;
    private final DbEngineFactory engineFactory;
    private final long maxmemoryBytes;
    private final MaxmemoryScope maxmemoryScope;
    private final String maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitMillis;
    private final long expireCleanupTimeLimitMillis;
}
```

```java
public static YierdisInstance create(YierdisInstanceConfig config) {
    YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
    DbEngineFactory engineFactory = config.engineFactory();
    if (engineFactory == null) {
        engineFactory = new YierdisDbEngineFactory(memoryRuntime);
    }

    RuntimeDbEngine[] dbs = new RuntimeDbEngine[config.databases()];
    for (int i = 0; i < dbs.length; i++) {
        dbs[i] = engineFactory.create(
                i,
                dbMax,
                config.maxmemoryPolicy(),
                config.maxmemorySamples(),
                config.evictionTimeLimitMillis(),
                config.expireCleanupTimeLimitMillis()
        );
    }

    return new YierdisInstance(config, dbs, memoryRuntime);
}
```

```java
interface YierdisDbInternals {
    YierdisFfmMemoryRuntime memoryRuntime();
}
```

- [ ] **Step 4: Run the runtime tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest,ContractsIntegrationSmokeTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the runtime/db ownership change**

```bash
git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java
git commit -m "refactor: share one FFM runtime across DBs"
```

### Task 4: Move Key Identity, Keyspace, and Expires to FFM References

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBytesRef.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBlobStore.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmKeyspace.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmExpireIndex.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisExpireIndex.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/KeyHandleContractTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapBytesViewTtlRegressionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/OffHeapKeysZeroCopyReadPathTest.java`

- [ ] **Step 1: Write failing tests for FFM-backed key handles, TTL lookups, and zero-copy key reads**

```java
@Test
public void keyHandleEqualityIsContentBasedAcrossHeapAndFfm() {
    byte[] key = "hello".getBytes(StandardCharsets.US_ASCII);
    KeyHandle heap = KeyHandle.forHeap(key, 123);

    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test")) {
        YierdisFfmBytesRef ref = YierdisFfmBlobStore.fromBytes(runtime, key);
        KeyHandle ffm = KeyHandle.forFfm(ref, 456);

        Assert.assertEquals(heap, ffm);
        Assert.assertEquals(ffm, heap);
        Assert.assertEquals(heap.hashCode(), ffm.hashCode());
    }
}

@Test
public void pexpireBytesViewDoesNotTriggerLinearExpireIndexScan() {
    YierdisDb db = new YierdisDb(new YierdisFfmMemoryRuntime("db"), 0, "noeviction", 5, 5, 5);
    db.bindToCurrentThread();
    try {
        db.writes().strings().setString(b("k00000"), new byte[0], SetMode.NORMAL, null);
        Assert.assertTrue(db.writes().ttl().pexpire(new TestBytesView(b("k00000")), 60_000));
    } finally {
        db.shutdown();
    }
}
```

- [ ] **Step 2: Run the key/index tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=KeyHandleContractTest,OffHeapBytesViewTtlRegressionTest,OffHeapKeysZeroCopyReadPathTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because key handles still depend on allocators/addresses and DB still instantiates heap keyspace or unsafe-only keyspace logic.

- [ ] **Step 3: Implement FFM key refs, blob storage, and FFM key/index structures**

```java
public record YierdisFfmBytesRef(YierdisFfmRegion region, int offset, int length) {
    public YierdisFfmSpan span() {
        return region.span(offset, length);
    }
}
```

```java
public final class FfmKeyHandle implements KeyHandle {
    private final YierdisFfmBytesRef ref;
    private final int dictHash;
    private final int contentHash;

    FfmKeyHandle(YierdisFfmBytesRef ref, int dictHash) {
        this.ref = java.util.Objects.requireNonNull(ref, "ref");
        this.dictHash = dictHash;
        this.contentHash = hashBytesView(this, ref.length());
    }

    @Override
    public byte byteAt(int index) {
        return YierdisFfmAccess.getByte(ref.span(), index);
    }
}
```

```java
public interface KeyHandle extends BytesView {
    static KeyHandle forFfm(YierdisFfmBytesRef ref, int dictHash) {
        return new FfmKeyHandle(ref, dictHash);
    }
}
```

```java
public final class YierdisFfmKeyspace<V> implements YierdisKeyspace<V> {
    private final YierdisFfmBlobStore blobStore;
    private Table table0;

    private static final class Table {
        final YierdisFfmRegion states;
        final YierdisFfmRegion hashes;
        final YierdisFfmRegion keyRegionIds;
        final YierdisFfmRegion keyOffsets;
        final YierdisFfmRegion keyLengths;
        final Object[] values;
    }
}
```

```java
public final class YierdisFfmExpireIndex implements YierdisExpireIndex {
    private final YierdisFfmBlobStore blobStore;
    private Table table0;

    @Override
    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        YierdisFfmBytesRef ref = KeyHandleAccess.ffmBytesRef(keyHandle);
        insertOrUpdate(ref, keyHandle.dictHash(), expireAtMillis);
    }
}
```

```java
public YierdisDb(YierdisFfmMemoryRuntime memoryRuntime, long maxmemoryBytes, String maxmemoryPolicy, int maxmemorySamples, long evictionTimeLimitMillis, long expireCleanupTimeLimitMillis) {
    this.memoryRuntime = memoryRuntime;
    this.blobStore = new YierdisFfmBlobStore(memoryRuntime);
    this.store = new YierdisFfmKeyspace<>(blobStore);
    this.expires = new YierdisFfmExpireIndex(blobStore);
    this.keysStoredOffHeap = true;
}
```

- [ ] **Step 4: Run the key/index tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=KeyHandleContractTest,OffHeapBytesViewTtlRegressionTest,OffHeapKeysZeroCopyReadPathTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the FFM key/index migration**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBytesRef.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmBlobStore.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmKeyspace.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmExpireIndex.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisExpireIndex.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/KeyHandleContractTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapBytesViewTtlRegressionTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/OffHeapKeysZeroCopyReadPathTest.java
git commit -m "feat: move keyspace and expires to FFM"
```

### Task 5: Store Strings and HLL Payloads in FFM by Default

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmString.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java`

- [ ] **Step 1: Write failing tests for default FFM string storage and HLL rewrites**

```java
@Test
public void setGetUsesFfmSliceAndDelFrees() {
    YierdisDb db = new YierdisDb(new YierdisFfmMemoryRuntime("db"), 0, "noeviction", 5, 5, 5);
    try {
        db.bindToCurrentThread();
        byte[] key = b("k");
        byte[] value = b("hello");

        Assert.assertTrue(db.writes().strings().setString(key, value, SetMode.NORMAL, null));

        RecordingBulkOutput out = new RecordingBulkOutput();
        db.reads().strings().getStringValue(new TestBytesView(key)).writeTo(out);
        Assert.assertTrue(out.usedOffHeapSlice);
        Assert.assertArrayEquals(value, out.bytes);
    } finally {
        db.shutdown();
    }
}

@Test
public void denseHllSupportsInPlacePfaddAfterPfmergeUnderFfmStorage() {
    try (YierdisDb db = new YierdisDb(new YierdisFfmMemoryRuntime("db"), 13000, "noeviction", 5, 5, 5)) {
        db.bindToCurrentThread();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        try (FastTestClient client = new FastTestClient(processor)) {
            client.execute(cmd("PFADD", "src", "a", "b"));
            client.execute(cmd("PFMERGE", "dense", "src"));
            ReplyInteger add = (ReplyInteger) client.execute(cmd("PFADD", "dense", "c"));
            Assert.assertEquals(1, add.value());
        }
    }
}
```

- [ ] **Step 2: Run the string/HLL tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=OffHeapStringStorageTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because strings still branch across `byte[]`, `OffHeapBuf`, and `YierdisUnsafeOffHeapString`, and HLL rewrites still assume that split payload model.

- [ ] **Step 3: Implement FFM-native string payloads and HLL rewrites**

```java
public final class YierdisFfmString implements AutoCloseable, OffHeapSlice {
    private final YierdisFfmRegion region;
    private int length;

    public static YierdisFfmString fromBytes(YierdisFfmMemoryRuntime memoryRuntime, BytesSlice value) {
        YierdisFfmRegion region = memoryRuntime.allocateRegion("string", value.length());
        YierdisFfmString out = new YierdisFfmString(region, value.length());
        out.setBytes(0, value, 0, value.length());
        return out;
    }
}
```

```java
static YierdisObject newString(YierdisFfmMemoryRuntime memoryRuntime, BytesSlice value) {
    Long parsed = tryParseLongForIntEncoding(value);
    if (parsed != null) {
        return newStringInt(parsed);
    }

    YierdisFfmString next = YierdisFfmString.fromBytes(memoryRuntime, value);
    ValueEncoding enc = next.length() <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
    YierdisObject out = new YierdisObject(ValueType.STRING, enc, next);
    out.rawLen = next.length();
    return out;
}
```

```java
static boolean pfAdd(YierdisObject o, YierdisFfmMemoryRuntime memoryRuntime, List<byte[]> elements) {
    if (isDense(o)) {
        return pfAddDenseInPlace(o, elements);
    }
    byte[] rewritten = rewriteSparseToDense(o.stringBytesView(), elements);
    o.overwriteWithString(memoryRuntime, rewritten);
    return true;
}
```

- [ ] **Step 4: Run the string/HLL tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=OffHeapStringStorageTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the string/HLL migration**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmString.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java
git commit -m "refactor: move string and hll payloads to FFM"
```

### Task 6: Migrate Hash/List/Set/ZSet Encodings to Shared FFM Containers

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmListpack.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmQuickList.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmDictLong.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmIntSet.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmZSet.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapCollectionReadStreamingTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java`

- [ ] **Step 1: Write failing tests for composite values stored entirely in FFM**

```java
@Test
public void offHeapCompositeTypesWorkAndShutdownDoesNotLeak() {
    YierdisDb db = new YierdisDb(new YierdisFfmMemoryRuntime("db"), 0, "noeviction", 5, 5, 5);
    try {
        db.bindToCurrentThread();
        Assert.assertTrue(db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null));
        db.writes().hashes().hset(b("h"), List.of(b("f"), b("v")));
        db.writes().lists().rpush(b("l"), List.of(b("a"), b("b")));
        db.writes().sets().sadd(b("set"), List.of(b("alpha"), b("beta")));
        db.writes().zsets().zadd(b("z"), List.of(b("1"), b("m1"), b("2"), b("m2")));
    } finally {
        db.shutdown();
    }
}

@Test
public void hgetallAndLrangeStreamOffHeapSlicesFromFfmContainers() {
    YierdisDb db = new YierdisDb(new YierdisFfmMemoryRuntime("db"), 0, "noeviction", 5, 5, 5);
    try {
        db.bindToCurrentThread();
        db.writes().hashes().hset(b("hash"), List.of(b("field"), b("value")));
        db.writes().lists().rpush(b("list"), List.of(b("a"), b("b"), b("c")));

        RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
        db.reads().hashes().hgetall(b("hash")).emitPairsTo(out);
        db.reads().lists().lrange(b("list"), 0, -1).emitTo(out);
        Assert.assertTrue(out.sawOffHeapSlice());
    } finally {
        db.shutdown();
    }
}
```

- [ ] **Step 2: Run the composite-value tests and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=OffHeapCollectionReadStreamingTest,UnsafeOffHeapDbSmokeTest,OffHeapLeakRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the collection implementations still depend on `YierdisUnsafeOffHeap*` classes and heap fallback branches.

- [ ] **Step 3: Replace collection encodings with shared FFM containers**

```java
final class HashValue implements YierdisValue {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private YierdisFfmListpack packed;
    private YierdisFfmDictLong dict;

    HashValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.packed = new YierdisFfmListpack(memoryRuntime);
    }
}
```

```java
final class ListValue implements YierdisValue {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private YierdisFfmListpack listpack;
    private YierdisFfmQuickList quicklist;

    ListValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.listpack = new YierdisFfmListpack(memoryRuntime);
    }
}
```

```java
final class SetValue implements YierdisValue {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private YierdisFfmIntSet intset;
    private YierdisFfmDictLong hashset;

    SetValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.intset = new YierdisFfmIntSet(memoryRuntime);
    }
}
```

```java
final class ZSetValue implements YierdisValue {
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private YierdisFfmListpack packed;
    private YierdisFfmZSet skiplist;

    ZSetValue(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = memoryRuntime;
        this.packed = new YierdisFfmListpack(memoryRuntime);
    }
}
```

- [ ] **Step 4: Run the composite-value tests and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=OffHeapCollectionReadStreamingTest,UnsafeOffHeapDbSmokeTest,OffHeapLeakRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit the collection migration**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmListpack.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmQuickList.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmDictLong.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmIntSet.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/ffm/YierdisFfmZSet.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapCollectionReadStreamingTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java
git commit -m "refactor: move collection encodings to FFM"
```

### Task 7: Delete Legacy Memory Backends, Old Address APIs, and Update Docs

**Files:**
- Modify: `yierdis-memory/pom.xml`
- Modify: `yierdis-core/yierdis-core-db/pom.xml`
- Modify: `yierdis-core/yierdis-core-runtime/pom.xml`
- Modify: `yierdis-server/pom.xml`
- Modify: `README.md`
- Modify: `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBenchArgs.java`
- Modify: `yierdis-memory/api/src/test/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorsTest.java`
- Delete: `yierdis-memory/netty/**`
- Delete: `yierdis-memory/unsafe/**`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap/**`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocators.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorProvider.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBackend.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAddressAllocator.java`
- Delete: `yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBlock.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAddressAllocator.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapBlock.java`
- Delete: `yierdis-memory/netty/src/test/java/yier/bubu/redis/db/memory/netty/YierdisNettyOffHeapAllocatorTest.java`
- Delete: `yierdis-memory/unsafe/src/test/java/yier/bubu/redis/db/memory/unsafe/YierdisUnsafeOffHeapAllocatorTest.java`
- Delete: `yierdis-memory/unsafe/src/test/java/yier/bubu/redis/db/memory/unsafe/NettyPlatformDependentMemoryAccessTest.java`

- [ ] **Step 1: Write the final FFM-only verification expectations**

```java
@Test
public void foreignProviderIsNoLongerDiscoveredViaServiceLoader() {
    try {
        Class.forName("yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators");
        Assert.fail("legacy allocator discovery should be deleted");
    } catch (ClassNotFoundException expected) {
        Assert.assertTrue(true);
    }
}
```

```markdown
## Native Memory

Yierdis now requires JDK 25 and always uses `java.lang.foreign` for native memory.

- no `--offheapBackend`
- no `--offheapKeysEnabled`
- no `--offheapMaxBytes`
- `maxmemory` is the only memory budget knob
```

- [ ] **Step 2: Run the full targeted suite and verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/api,yierdis-memory/foreign,yierdis-core/yierdis-core-runtime,yierdis-server -am test`
Expected: FAIL because the old modules, provider discovery classes, and legacy tests/docs still exist in the reactor.

- [ ] **Step 3: Delete the old world and update docs/build files**

```xml
<modules>
    <module>api</module>
    <module>foreign</module>
</modules>
```

```xml
<dependencies>
    <dependency>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-memory-foreign</artifactId>
    </dependency>
</dependencies>
```

```bash
git rm -r yierdis-memory/netty yierdis-memory/unsafe
git rm -r yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/memory/offheap
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocators.java
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorProvider.java
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBackend.java
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAddressAllocator.java
git rm yierdis-memory/api/src/main/java/yier/bubu/redis/db/memory/api/YierdisOffHeapBlock.java
git rm yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAddressAllocator.java
git rm yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapBlock.java
```

```markdown
- `foreign`: 基于 JDK 25 正式 `java.lang.foreign` FFM API
- native memory 默认启用，并统一纳入 `maxmemory`
```

- [ ] **Step 4: Run full verification and verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test`
Expected: PASS

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -DskipTests package`
Expected: PASS

- [ ] **Step 5: Commit the FFM-only cleanup**

```bash
git add pom.xml yierdis-memory/pom.xml yierdis-core/yierdis-core-db/pom.xml yierdis-core/yierdis-core-runtime/pom.xml yierdis-server/pom.xml README.md yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBenchArgs.java
git add -A yierdis-memory yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/offheap/api
git commit -m "refactor: remove legacy offheap backends"
```

## Self-Review

### Spec Coverage

- System-level simplification: Task 1 and Task 7
- FFM runtime SSOT: Task 2 and Task 3
- Key identity/keyspace/expires: Task 4
- String and HLL storage: Task 5
- Hash/list/set/zset storage: Task 6
- Build/docs/test-matrix cleanup: Task 7

No spec section is left without a concrete task.

### Placeholder Scan

- No `TODO`, `TBD`, or “similar to previous task” placeholders remain.
- Every task has concrete file paths, test commands, code skeletons, and a commit checkpoint.

### Type Consistency

The plan uses one consistent naming scheme end to end:

- `YierdisFfmMemoryRuntime`
- `YierdisFfmRegion`
- `YierdisFfmSpan`
- `YierdisFfmAccess`
- `YierdisFfmBytesRef`
- `YierdisFfmBlobStore`
- `YierdisFfmKeyspace`
- `YierdisFfmExpireIndex`
- `YierdisFfmString`
- `YierdisFfmListpack`
- `YierdisFfmQuickList`
- `YierdisFfmDictLong`
- `YierdisFfmIntSet`
- `YierdisFfmZSet`
- `FfmKeyHandle`
