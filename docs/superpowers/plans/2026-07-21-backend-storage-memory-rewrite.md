# Backend Storage And Memory Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace permissive runtime/storage hooks, raw native handles, the partial allocator seam, and RESP-aware storage results with explicit runtime capabilities, allocator-scoped stable memory, an API-only DB backend boundary, and repeatable protocol-neutral semantic result sources.

**Architecture:** `server-runtime` constructs every DB from one `DbEngineConfig`, validates the complete capability matrix before attaching resources, and invokes only explicit capability interfaces. `db-memory` owns one atomic owner guard and depends on the complete `StableMemoryBackend` API; the FFM module supplies the public backend facade while keeping arenas, segments, local-handle codecs, and allocators internal. `db-api` exposes byte values and repeatable length visitors only. Command code adapts those sources into the `ReplyShape` contract owned by `server-api`, and the active protocol adapter alone converts semantic lengths into wire bytes.

**Tech Stack:** Java 25, Maven reactor, JUnit 4, JDK FFM API

## Global Constraints

- Treat `docs/superpowers/specs/2026-07-21-backend-architecture-contract-rewrite-design.md` as the approved behavioral source of truth.
- Run every Java and Maven command with `/usr/lib/jvm/java-25-openjdk-amd64` as shown in this plan. Focused reactor tests that select test classes must include `-Dsurefire.failIfNoSpecifiedTests=false`.
- Complete each task red-green-refactor: add the specified regression tests, observe the stated compile or assertion failure, implement the complete contract, rerun the focused tests, then run the broader module set.
- Do not retain adapters for `NativeAllocator`, public raw-handle construction/decoding, `allocateRaw`/`resolveRaw`/`freeRaw`/`pinRaw`/`unpinRaw`, RESP byte metrics, or default no-op runtime capabilities.
- `ReplyShape`, `ReplyPlan`, and `ReplySizer` have exactly one owner: `yierdis-server-api`. Neither `yierdis-db-api` nor `yierdis-db-memory` may depend on `yierdis-server-api`; command code constructs shapes through zero-copy wrappers or method references over the DB visitors.
- The grouped configuration plan owns the final public `StorageConfig` and `MaintenanceConfig` records. This plan owns the lower runtime `DbDefragConfig` and `DbEngineConfig` contracts and the deletion of the old `YierdisInstanceConfig` defrag accessors. Because Task 1 precedes grouped configuration, its temporary bootstrap mapping may read the current flat runtime config only to construct one `DbDefragConfig`; it must not import or create `StorageConfig`. Command/runtime Task 5 replaces that temporary composition mapping with `StorageConfig.defrag()` without changing the runtime API again.
- Storage `PreparedMutation<R>` is distinct from `yierdis-db-memory`'s internal allocation/ledger preparations. Public preview and version checks are read-only; a stale public preparation is closed and recreated before `commit(MutationContext)`.
- Added or rewritten Java comments follow the repository comment policy: Chinese comments only where a non-obvious ownership, layout, concurrency, or protocol boundary needs explanation.
- Preserve unrelated worktree changes. The commit commands below are implementation checkpoints for the worker executing this plan; this planning session does not run them.

## Final Contract Map

| Owner | Contract | Consumers |
| --- | --- | --- |
| `yierdis-db-api` | `DbEngineConfig`, runtime capability interfaces, `PreparedMutation`, `ByteValue`, sequence/map visitors | runtime and command |
| `yierdis-memory-api` | allocator-scoped `NativeHandle`, `StableMemoryBackend`, `StableMemoryRegion`, owner/factory contracts | DB implementations and memory backends |
| `yierdis-memory-ffm` | public `YierdisFfmStableMemoryBackend`; internal allocator/runtime/regions/local codec | server, benchmark, and integration-test composition only through `StableMemoryBackendFactory` |
| `yierdis-db-memory` | one `DbThreadGuard`, backend-neutral expire index, full-handle storage layouts, semantic sources | `DbEngineFactory` and `db-api` |
| `yierdis-server-api` | `ReplyShape`, `ReplyPlan`, `ReplySizer` | command, executor, protocol adapters |
| `yierdis-networking-resp` | RESP2/RESP3 implementation of `ReplySizer` | networking composition |

---

### Task 1: Replace Runtime Defaults With Configured DB Capabilities

**Files:**
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbDefragConfig.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngineConfig.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/CommitPublishingDbEngine.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/GlobalMaxmemoryDbEngine.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DefragmentableDbEngine.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngineFactory.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/RuntimeDbEngine.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/DbEngine.java`
- Rename: `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/DbEngineFactoryPolicyContractTest.java` to `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/DbEngineFactoryConfigContractTest.java`
- Modify: `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceResources.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernor.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Create: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/YierdisInstanceCapabilityValidationTest.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineReadWriteBoundaryTest.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernorTest.java`
- Modify: `yierdis-server/yierdis-server-runtime-api/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisInstances.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/CommitStreamIntegrationTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestYierdisInstances.java`

**Interfaces:**
- Produces: one immutable factory input per DB and explicit commit, global-maxmemory, and defrag capability types.
- Consumes: the current flat `YierdisInstanceConfig` and server runtime config only during this pre-grouped checkpoint; it exports `DbDefragConfig` for the later grouped-config task. No Task 1 production source imports `StorageConfig` or `MaintenanceConfig`.
- Invariant: all engines are non-null and have the same optional-capability vector; configured features require their corresponding capability on every engine.

- [ ] **Step 1: Write the failing factory-config contract test**

Replace the old positional-policy test with this reflection-and-value contract. Keep the helper engine in the same test file so the test has no production implementation dependency:

```java
package yier.bubu.redis.storage.api;

import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.Test;

public class DbEngineFactoryConfigContractTest {
    @Test
    public void factoryHasOneConfiguredCreateParameter() throws Exception {
        Method create = DbEngineFactory.class.getMethod("create", DbEngineConfig.class);

        Assert.assertEquals(RuntimeDbEngine.class, create.getReturnType());
        Assert.assertArrayEquals(new Class<?>[]{DbEngineConfig.class}, create.getParameterTypes());
    }

    @Test
    public void engineConfigCarriesDefragAndAdmissionValues() {
        DbDefragConfig defrag = new DbDefragConfig(true, 4096L, 7L, 3L);
        DbEngineConfig config = new DbEngineConfig(
                2,
                1_048_576L,
                MaxmemoryPolicy.ALLKEYS_LRU,
                9,
                5L,
                11L,
                defrag
        );

        Assert.assertEquals(2, config.dbIndex());
        Assert.assertEquals(1_048_576L, config.maxmemoryBytes());
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, config.maxmemoryPolicy());
        Assert.assertEquals(9, config.maxmemorySamples());
        Assert.assertEquals(5L, config.evictionTimeLimitMillis());
        Assert.assertEquals(11L, config.expireCleanupTimeLimitMillis());
        Assert.assertSame(defrag, config.defrag());
    }
}
```

- [ ] **Step 2: Write the failing startup-capability tests**

Create `YierdisInstanceCapabilityValidationTest.java` from the complete class below. Its nested `BaselineEngine`, `CommitEngine`, and `AllCapabilitiesEngine` cover the required capability vectors. `BaselineEngine` returns `null` for semantic DB views because startup validation must not touch command operations, records `shutdownCalls`, and implements only `RuntimeDbEngine`.

```java
package yier.bubu.redis.runtime.embedded;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.storage.api.*;

public class YierdisInstanceCapabilityValidationTest {
    @Test
    public void nullEngineFailsAndClosesPreviouslyCreatedEngine() {
        BaselineEngine first = new BaselineEngine();
        Factory factory = new Factory(first, null);

        IllegalStateException failure = Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(2, factory).build())
        );

        Assert.assertTrue(failure.getMessage().contains("dbIndex=1"));
        Assert.assertEquals(1, first.shutdownCalls.get());
    }

    @Test
    public void mixedCapabilityVectorsFailBeforeAnyAttachment() {
        CommitEngine first = new CommitEngine();
        BaselineEngine second = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(2, new Factory(first, second)).build())
        );

        Assert.assertEquals(0, first.attachCalls.get());
        Assert.assertEquals(1, first.shutdownCalls.get());
        Assert.assertEquals(1, second.shutdownCalls.get());
    }

    @Test
    public void configuredChangeSinkRequiresCommitCapabilityBeforeStreamStart() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .changeSink(event -> { })
                        .build())
        );

        Assert.assertEquals(1, engine.shutdownCalls.get());
    }

    @Test
    public void globalMaxmemoryRequiresGlobalCapability() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .maxmemoryBytes(1024L)
                        .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                        .build())
        );
    }

    @Test
    public void enabledDefragRequiresDefragCapability() {
        BaselineEngine engine = new BaselineEngine();

        Assert.assertThrows(
                IllegalStateException.class,
                () -> YierdisInstance.create(baseConfig(1, new Factory(engine))
                        .defrag(new DbDefragConfig(true, 64L * 1024L, 64L, 1L))
                        .build())
        );
    }

    @Test
    public void validCapabilitiesReceiveConfigurationBeforeUse() {
        AllCapabilitiesEngine engine = new AllCapabilitiesEngine();
        Factory factory = new Factory(engine);

        try (YierdisInstance ignored = YierdisInstance.create(baseConfig(1, factory)
                .changeSink(event -> { })
                .maxmemoryBytes(1024L)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .defrag(new DbDefragConfig(true, 2048L, 4L, 6L))
                .build())) {
            Assert.assertEquals(1, engine.attachCalls.get());
            Assert.assertEquals(1, factory.configs.size());
            Assert.assertEquals(new DbDefragConfig(true, 2048L, 4L, 6L), factory.configs.get(0).defrag());
        }
    }

    private static YierdisInstanceConfig.Builder baseConfig(int databases, DbEngineFactory factory) {
        return YierdisInstanceConfig.builder().databases(databases).engineFactory(factory);
    }

    private static class Factory implements DbEngineFactory {
        private final RuntimeDbEngine[] engines;
        private final List<DbEngineConfig> configs = new ArrayList<>();
        private int next;

        private Factory(RuntimeDbEngine... engines) {
            this.engines = engines;
        }

        @Override
        public RuntimeDbEngine create(DbEngineConfig config) {
            configs.add(config);
            return engines[next++];
        }
    }

    private static class BaselineEngine implements RuntimeDbEngine {
        private final AtomicInteger shutdownCalls = new AtomicInteger();
        @Override public DbReads reads() { return null; }
        @Override public DbWrites writes() { return null; }
        @Override public ExpirationManager expiration() { return null; }
        @Override public MemoryOps memory() { return null; }
        @Override public DbLifecycleOps lifecycle() { return null; }
        @Override public void bindToCurrentThread() { }
        @Override public void runMaintenance() { }
        @Override public void shutdown() { shutdownCalls.incrementAndGet(); }
    }

    private static class CommitEngine extends BaselineEngine implements CommitPublishingDbEngine {
        private final AtomicInteger attachCalls = new AtomicInteger();
        @Override public void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex) {
            attachCalls.incrementAndGet();
        }
    }

    private static final class AllCapabilitiesEngine extends CommitEngine
            implements GlobalMaxmemoryDbEngine, DefragmentableDbEngine {
        @Override public MemoryUsageSnapshot memoryUsage() {
            return new MemoryUsageSnapshot(0L, 0L, 0L, 0L, 0L);
        }
        @Override public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
            return MemoryReclaimResult.empty();
        }
        @Override public int keyCountEstimate() { return 0; }
        @Override public void cleanupExpired(long nowMillis) { }
        @Override public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) { return null; }
        @Override public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) { return null; }
        @Override public boolean evict(MaxmemoryCandidate candidate, long nowMillis) { return false; }
        @Override public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) { }
        @Override public void defragMaintenance() { }
    }
}
```

- [ ] **Step 3: Run the focused tests and verify RED**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime -am \
  -Dtest=DbEngineFactoryConfigContractTest,YierdisInstanceCapabilityValidationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: test compilation fails because `DbEngineConfig`, `DbDefragConfig`, the three capability interfaces, and `RuntimeDbEngine.runMaintenance()` do not exist and `DbEngineFactory` still exposes positional arguments.

- [ ] **Step 4: Add the immutable config and explicit capability interfaces**

Create the records with complete validation:

```java
package yier.bubu.redis.storage.api;

public record DbDefragConfig(
        boolean enabled,
        long maxMoveBytes,
        long maxObjects,
        long timeLimitMillis
) {
    public DbDefragConfig {
        if (maxMoveBytes < 0L || maxObjects < 0L || timeLimitMillis < 0L) {
            throw new IllegalArgumentException("defrag limits must be non-negative");
        }
    }
}
```

```java
package yier.bubu.redis.storage.api;

import java.util.Objects;

public record DbEngineConfig(
        int dbIndex,
        long maxmemoryBytes,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis,
        DbDefragConfig defrag
) {
    public DbEngineConfig {
        if (dbIndex < 0) throw new IllegalArgumentException("dbIndex must be non-negative");
        if (maxmemoryBytes < 0L) throw new IllegalArgumentException("maxmemoryBytes must be non-negative");
        if (maxmemorySamples < 1) throw new IllegalArgumentException("maxmemorySamples must be positive");
        if (evictionTimeLimitMillis < 0L || expireCleanupTimeLimitMillis < 0L) {
            throw new IllegalArgumentException("maintenance limits must be non-negative");
        }
        maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        defrag = Objects.requireNonNull(defrag, "defrag");
    }
}
```

Replace the factory and runtime interfaces exactly:

```java
package yier.bubu.redis.storage.api;

@FunctionalInterface
public interface DbEngineFactory {
    RuntimeDbEngine create(DbEngineConfig config);
}
```

```java
package yier.bubu.redis.storage.api;

public interface RuntimeDbEngine extends DbEngine {
    void bindToCurrentThread();
    void runMaintenance();
    void shutdown();
}
```

```java
package yier.bubu.redis.storage.api;

public interface CommitPublishingDbEngine extends RuntimeDbEngine {
    void attachCommitPublisher(DbCommitPublisher publisher, int dbIndex);
}
```

```java
package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;

public interface GlobalMaxmemoryDbEngine
        extends RuntimeDbEngine, MaxmemoryParticipant, MaxmemoryCoordinatorAware {
    @Override
    MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis);

    @Override
    MemoryReclaimResult trimMemory(MemoryPressureBudget budget);
}
```

```java
package yier.bubu.redis.storage.api;

public interface DefragmentableDbEngine extends RuntimeDbEngine {
    void defragMaintenance();
}
```

Keep `DbEngine` limited to the semantic `reads()`, `writes()`, `expiration()`, `memory()`, `lifecycle()`, and health view. Remove runtime-capability wording and imports from it; do not move lifecycle hooks onto `DbEngine`.

- [ ] **Step 5: Build and validate the complete engine array before side effects**

Before the later grouped-config task, replace the four flat defrag fields/builders in `YierdisInstanceConfig` with one non-null value:

```java
private final DbDefragConfig defrag;

public DbDefragConfig defrag() {
    return defrag;
}

public Builder defrag(DbDefragConfig defrag) {
    this.defrag = Objects.requireNonNull(defrag, "defrag");
    return this;
}
```

The builder default is `new DbDefragConfig(false, 64L * 1024L, 64L, 1L)`, preserving the current defaults. Task 1 must migrate every use of the removed instance-config accessors in the files listed above. Until command/runtime Task 5 creates `StorageConfig`, `YierdisServerBootstrap` and the two test instance factories construct the record once from the current `YierdisServerRuntimeConfig`/existing instance config:

```java
.defrag(new DbDefragConfig(
        runtimeConfig.nativeDefragEnabled(),
        runtimeConfig.nativeDefragMaxMoveBytes(),
        runtimeConfig.nativeDefragMaxObjects(),
        runtimeConfig.nativeDefragTimeLimitMillis()
));
```

This is the only temporary flat-to-record composition mapping. It is replaced by `storage.defrag()` in command/runtime Task 5; do not retain old `YierdisInstanceConfig` getters or builder setters. In `YierdisInstance`, replace the positional factory call with one record construction per index:

```java
RuntimeDbEngine engine = engineFactory.create(new DbEngineConfig(
        i,
        dbMax,
        config.maxmemoryPolicy(),
        config.maxmemorySamples(),
        config.evictionTimeLimitMillis(),
        config.expireCleanupTimeLimitMillis(),
        config.defrag()
));
if (engine == null) {
    throw new IllegalStateException("DbEngineFactory returned null for dbIndex=" + i);
}
dbs[i] = engine;
```

After all engines exist, compute and validate one capability vector before preparing a commit stream, attaching a publisher/coordinator, or returning an instance:

```java
private record CapabilityVector(boolean commit, boolean globalMaxmemory, boolean defrag) {
    static CapabilityVector of(RuntimeDbEngine engine) {
        return new CapabilityVector(
                engine instanceof CommitPublishingDbEngine,
                engine instanceof GlobalMaxmemoryDbEngine,
                engine instanceof DefragmentableDbEngine
        );
    }
}

private static CapabilityVector validateCapabilities(
        RuntimeDbEngine[] engines,
        YierdisInstanceConfig config
) {
    CapabilityVector expected = CapabilityVector.of(engines[0]);
    for (int index = 1; index < engines.length; index++) {
        CapabilityVector actual = CapabilityVector.of(engines[index]);
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                    "inconsistent DB capabilities: dbIndex=0 " + expected
                            + ", dbIndex=" + index + " " + actual
            );
        }
    }
    boolean commitRequired = config.changeSink() != YierdisChangeSink.NOOP;
    boolean globalRequired = config.maxmemoryBytes() > 0L
            && config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;
    if (commitRequired && !expected.commit()) {
        throw new IllegalStateException("configured change sink requires CommitPublishingDbEngine");
    }
    if (globalRequired && !expected.globalMaxmemory()) {
        throw new IllegalStateException("global maxmemory requires GlobalMaxmemoryDbEngine");
    }
    if (config.defrag().enabled() && !expected.defrag()) {
        throw new IllegalStateException("native defrag requires DefragmentableDbEngine");
    }
    return expected;
}
```

Only after this method succeeds:

1. Prepare the commit stream when enabled.
2. Build `GlobalMaxmemoryDbEngine[]`, construct `YierdisGlobalMaxmemoryGovernor` from that type, and attach its coordinator.
3. Attach the commit publisher through `CommitPublishingDbEngine`.
4. Start the commit stream.
5. Publish `YierdisInstanceResources`.

On any failure, `YierdisInstanceResources.startupFailure(...)` shuts down every non-null engine already created exactly once, closes the prepared-but-not-published commit stream if present, and closes owned factory resources in reverse registration order while suppressing cleanup failures onto the startup failure.

- [ ] **Step 6: Route maintenance through baseline and explicit capabilities**

Replace runtime calls to `expiration().cleanupExpired()`, `enforceMaxmemoryMaintenance()`, and optional default defrag with:

```java
for (RuntimeDbEngine engine : engines) {
    engine.runMaintenance();
    if (defragEnabled) {
        ((DefragmentableDbEngine) engine).defragMaintenance();
    }
}
if (globalMaxmemoryGovernor != null) {
    globalMaxmemoryGovernor.enforceMaintenance();
}
```

Production `YierdisDb.runMaintenance()` performs its DB-local expiration, hash-table, and per-DB admission maintenance. Global eviction remains solely in `YierdisGlobalMaxmemoryGovernor`. Update governor fields, constructor arguments, and loops from `MaxmemoryParticipant[]`/`RuntimeDbEngine[]` to `GlobalMaxmemoryDbEngine[]`; its explicit overrides guarantee deterministic full scan and trim support.

Update existing test doubles so each implements only the capability required by its configuration. Tests using global mode must implement `GlobalMaxmemoryDbEngine`; commit-stream tests must implement `CommitPublishingDbEngine`; defrag tests must implement `DefragmentableDbEngine`.

- [ ] **Step 7: Run focused tests and verify GREEN**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime -am \
  -Dtest=DbEngineFactoryConfigContractTest,YierdisInstanceCapabilityValidationTest,DbEngineFactoryInjectionTest,DbEngineReadWriteBoundaryTest,YierdisGlobalMaxmemoryGovernorTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

Expected: PASS. The clean phase removes the renamed `DbEngineFactoryPolicyContractTest` bytecode before the subsequent broad suite. Factory calls carry one immutable config, invalid compositions fail before attachment, and cleanup counters are exactly one.

- [ ] **Step 8: Run the broader capability-owner suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-api,yierdis-server/yierdis-server-runtime -am test
```

Expected: PASS with all DB API and runtime capability, commit-stream, maxmemory, and embedded-instance tests green. At this deliberate API-break checkpoint, `yierdis-db-memory`, `yierdis-server-main`, `yierdis-architecture-tests`, `yierdis-integration-tests`, and `yierdis-benchmark` are the five direct downstream modules with positional-factory/default-capability source or deliberately broken dependencies. `yierdis-cli` is the one transitively affected leaf module because its tests depend on `yierdis-server-main`; its own sources consume no retired API. Task 4 migrates the five direct modules and verifies all six in its GREEN commands.

- [ ] **Step 9: Commit the runtime capability boundary**

```bash
git add \
  yierdis-db/yierdis-db-api \
  yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java \
  yierdis-server/yierdis-server-runtime-api/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java \
  yierdis-server/yierdis-server-runtime \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisInstances.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/CommitStreamIntegrationTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestYierdisInstances.java
git commit -m "refactor: make runtime database capabilities explicit"
```

---

### Task 2: Define Allocator-Scoped Handles And The Complete Stable Memory API

**Files:**
- Delete: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
- Rewrite: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandle.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StableMemoryBackendIds.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StableMemoryBackendIdSequence.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeHandleOwnershipException.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/MemoryOwner.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StableMemoryBackendFactory.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StableMemoryRegion.java`
- Create: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/StableMemoryBackend.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectView.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocationScope.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeEpochScope.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeHandleTest.java`
- Rename: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java` to `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/StableMemoryBackendContractTest.java`
- Rename: `yierdis-memory/yierdis-memory-testkit/src/main/java/yier/bubu/redis/memory/testkit/FailOnAllocationNativeAllocator.java` to `yierdis-memory/yierdis-memory-testkit/src/main/java/yier/bubu/redis/memory/testkit/FailOnAllocationStableMemoryBackend.java`
- Rename: `yierdis-memory/yierdis-memory-testkit/src/test/java/yier/bubu/redis/memory/testkit/FailOnAllocationNativeAllocatorTest.java` to `yierdis-memory/yierdis-memory-testkit/src/test/java/yier/bubu/redis/memory/testkit/FailOnAllocationStableMemoryBackendTest.java`

**Interfaces:**
- Produces: a backend-complete, implementation-neutral API, including owner binding, stable objects, regions, scopes, epochs, accounting, trim, and defrag.
- Invariant: `allocatorId == 0 && localRaw == 0` is the sole null handle; a live backend ID is positive, process-unique, monotonic, and never reused.
- Invariant: public code can carry `localRaw` as opaque data but cannot construct/decode the old packed local format or invoke a local-only operation.

- [ ] **Step 1: Write the failing handle and ID tests without losing metadata coverage**

Modify `NativeHandleTest.java`; do not replace the suite wholesale. Retain
`domainLookupDoesNotAllocateForEveryDecodedHandle()`,
`collectionNativeObjectKindsHaveDistinctCodesInsideTheirDomains()`, its exact
domain-mapping assertions, `allocatedBytesBean()`, and the
`java.lang.management.ManagementFactory` import verbatim. These tests cover
allocation-free metadata lookup and per-domain object-kind code uniqueness,
which remain valid after the public packed handle disappears.

Remove the raw packed-handle tests from this public API suite:
`nullHandleIsOnlyZeroRawValue()`, `encodesAndDecodesHandleFields()`,
`primitiveDecodersMatchHandleAccessorsWithoutPerCallAllocation()`,
`rejectsOutOfRangeFields()`, `rejectsMismatchedDomainAndKind()`, and
`rejectsNonZeroReservedDomain()`, together with the now-unused `assertIllegal`
and `consumeRawFields` helpers. Task 3 ports the still-valid codec behavior and
allocation test to package-private `YierdisLocalHandleCodecTest`; only the raw
public API location is deleted. Add imports for `ArrayList`, `HashSet`, `List`,
`Set`, `CountDownLatch`, `ExecutorService`, `Executors`, `Future`, and
`TimeUnit`, then add these two-part handle and backend-ID tests to the retained
class:

```java
@Test
public void nullRequiresBothIdentityPartsToBeZero() {
    Assert.assertTrue(NativeHandle.NULL.isNull());
    Assert.assertEquals(0L, NativeHandle.NULL.allocatorId());
    Assert.assertEquals(0L, NativeHandle.NULL.localRaw());
    Assert.assertFalse(new NativeHandle(1L, 0L).isNull());
    Assert.assertFalse(new NativeHandle(0L, 1L).isNull());
}

@Test
public void equalityIncludesAllocatorIdentity() {
    NativeHandle first = new NativeHandle(11L, 77L);
    NativeHandle same = new NativeHandle(11L, 77L);
    NativeHandle otherAllocator = new NativeHandle(12L, 77L);

    Assert.assertEquals(first, same);
    Assert.assertNotEquals(first, otherAllocator);
}

@Test
public void backendIdsArePositiveAndStrictlyMonotonic() {
    long first = StableMemoryBackendIds.nextId();
    long second = StableMemoryBackendIds.nextId();
    long third = StableMemoryBackendIds.nextId();

    Assert.assertTrue(first > 0L);
    Assert.assertTrue(second > first);
    Assert.assertTrue(third > second);
}

@Test
public void backendIdsArePositiveAndUniqueAcrossConcurrentCallers() throws Exception {
    int workerCount = 8;
    int idsPerWorker = 1_000;
    ExecutorService executor = Executors.newFixedThreadPool(workerCount);
    CountDownLatch start = new CountDownLatch(1);
    try {
        List<Future<List<Long>>> futures = new ArrayList<>();
        for (int worker = 0; worker < workerCount; worker++) {
            futures.add(executor.submit(() -> {
                start.await();
                List<Long> generated = new ArrayList<>(idsPerWorker);
                for (int index = 0; index < idsPerWorker; index++) {
                    generated.add(StableMemoryBackendIds.nextId());
                }
                return generated;
            }));
        }
        start.countDown();

        Set<Long> unique = new HashSet<>();
        for (Future<List<Long>> future : futures) {
            for (long id : future.get(5L, TimeUnit.SECONDS)) {
                Assert.assertTrue(id > 0L);
                Assert.assertTrue("duplicate backend ID " + id, unique.add(id));
            }
        }
        Assert.assertEquals(workerCount * idsPerWorker, unique.size());
    } finally {
        executor.shutdownNow();
        Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
    }
}

@Test
public void backendIdExhaustionIsPermanentInAnIsolatedSequence() {
    StableMemoryBackendIdSequence sequence =
            new StableMemoryBackendIdSequence(Long.MAX_VALUE);

    Assert.assertEquals(Long.MAX_VALUE, sequence.nextId());
    Assert.assertThrows(IllegalStateException.class, sequence::nextId);
    Assert.assertThrows(IllegalStateException.class, sequence::nextId);
}
```

- [ ] **Step 2: Extend the renamed contract suite without dropping record coverage**

Rename `NativeAllocatorContractTest` to `StableMemoryBackendContractTest` and
rename the class, but retain these existing test methods and their bodies
verbatim:

- `statsRecordExposesAllocatorCounters`
- `statsRecordExposesProductionAllocatorCounters`
- `defragResultFactoriesExposeMovementOutcomes`
- `defragCycleRecordsExposeBudgetsAndCounters`
- `epochKindsCoverAllocatorReadSafetyScopes`
- `exceptionTypesCarryMessages`
- `nativeCapacityExceptionIsAnOffHeapOom`

The final method preserves the API category invariant:
`NativeCapacityExceededException` is an `OffHeapOutOfMemoryException`, not a
`NativeMemoryException`. Add these imports and reflection/legacy-absence tests
to that retained suite:

```java
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

@Test
public void requiredBackendBehaviorIsAbstract() {
    Set<String> required = Set.of(
            "allocatorId", "bindToCurrentThread", "allocate", "reallocate",
            "free", "pin", "unpin", "beginEpoch", "beginAllocationScope",
            "estimateAllocationScopeBookkeepingBytes", "resolve", "resolvePinned",
            "allocateRegion", "defragOne", "defragCycle", "logicalUsedBytes",
            "stats", "metadataStats", "memoryUsage", "trimEmptyPages",
            "estimateAdditionalGrowth", "estimateConservativeAdditionalGrowth",
            "liveRegionCount", "close"
    );
    Method[] declaredMethods = StableMemoryBackend.class.getDeclaredMethods();
    Assert.assertEquals(required.size(), declaredMethods.length);
    Set<String> declared = Arrays.stream(declaredMethods)
            .peek(method -> Assert.assertTrue(
                    method.getName() + " must be abstract",
                    Modifier.isAbstract(method.getModifiers())
            ))
            .map(Method::getName)
            .collect(Collectors.toSet());

    Assert.assertEquals(required, declared);
}

@Test
public void backendMethodSignaturesAreExact() throws Exception {
    assertAbstractMethod(StableMemoryBackend.class, long.class, "allocatorId");
    assertAbstractMethod(StableMemoryBackend.class, void.class, "bindToCurrentThread");
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeHandle.class,
            "allocate",
            NativeObjectKind.class,
            int.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeHandle.class,
            "reallocate",
            NativeHandle.class,
            int.class,
            NativeReallocPolicy.class
    );
    assertAbstractMethod(StableMemoryBackend.class, void.class, "free", NativeHandle.class);
    assertAbstractMethod(StableMemoryBackend.class, void.class, "pin", NativeHandle.class);
    assertAbstractMethod(StableMemoryBackend.class, void.class, "unpin", NativeHandle.class);
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeEpochScope.class,
            "beginEpoch",
            NativeEpochKind.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeAllocationScope.class,
            "beginAllocationScope"
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            long.class,
            "estimateAllocationScopeBookkeepingBytes",
            int.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeObjectView.class,
            "resolve",
            NativeHandle.class,
            NativeAccessMode.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeObjectView.class,
            "resolvePinned",
            NativeHandle.class,
            NativeAccessMode.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            StableMemoryRegion.class,
            "allocateRegion",
            String.class,
            int.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeDefragResult.class,
            "defragOne",
            NativeHandle.class,
            long.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeDefragReport.class,
            "defragCycle",
            NativeDefragOptions.class
    );
    assertAbstractMethod(StableMemoryBackend.class, long.class, "logicalUsedBytes");
    assertAbstractMethod(StableMemoryBackend.class, NativeAllocatorStats.class, "stats");
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeAllocatorMetadataStats.class,
            "metadataStats"
    );
    assertAbstractMethod(StableMemoryBackend.class, MemoryUsageSnapshot.class, "memoryUsage");
    assertAbstractMethod(
            StableMemoryBackend.class,
            MemoryReclaimResult.class,
            "trimEmptyPages",
            MemoryPressureBudget.class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeAllocationGrowth.class,
            "estimateAdditionalGrowth",
            int[].class
    );
    assertAbstractMethod(
            StableMemoryBackend.class,
            NativeAllocationGrowth.class,
            "estimateConservativeAdditionalGrowth",
            int[].class
    );
    assertAbstractMethod(StableMemoryBackend.class, long.class, "liveRegionCount");
    assertAbstractMethod(StableMemoryBackend.class, void.class, "close");
}

@Test
public void regionMethodSignaturesAreExact() throws Exception {
    Assert.assertEquals(11, StableMemoryRegion.class.getDeclaredMethods().length);
    assertAbstractMethod(StableMemoryRegion.class, int.class, "size");
    assertAbstractMethod(StableMemoryRegion.class, byte.class, "getByte", int.class);
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "setByte",
            int.class,
            byte.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            int.class,
            "getIntLittleEndian",
            int.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "setIntLittleEndian",
            int.class,
            int.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            long.class,
            "getLongLittleEndian",
            int.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "setLongLittleEndian",
            int.class,
            long.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "getBytes",
            int.class,
            byte[].class,
            int.class,
            int.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "setBytes",
            int.class,
            byte[].class,
            int.class,
            int.class
    );
    assertAbstractMethod(
            StableMemoryRegion.class,
            void.class,
            "copyTo",
            int.class,
            StableMemoryRegion.class,
            int.class,
            int.class
    );
    assertAbstractMethod(StableMemoryRegion.class, void.class, "close");
}

@Test
public void ownerFactoryAndOwnershipExceptionContractsAreExact() throws Exception {
    Assert.assertEquals(3, MemoryOwner.class.getDeclaredMethods().length);
    assertAbstractMethod(MemoryOwner.class, void.class, "bindToCurrentThread");
    assertAbstractMethod(MemoryOwner.class, void.class, "checkCurrentThread");
    assertAbstractMethod(MemoryOwner.class, void.class, "checkCurrentThreadForShutdown");
    Assert.assertEquals(1, StableMemoryBackendFactory.class.getDeclaredMethods().length);
    assertAbstractMethod(
            StableMemoryBackendFactory.class,
            StableMemoryBackend.class,
            "create",
            String.class,
            int.class,
            MemoryOwner.class
    );
    Assert.assertTrue(
            StableMemoryBackendFactory.class.isAnnotationPresent(FunctionalInterface.class)
    );

    NativeHandleOwnershipException failure =
            new NativeHandleOwnershipException(11L, 12L);
    Assert.assertEquals(
            1,
            NativeHandleOwnershipException.class.getDeclaredConstructors().length
    );
    Assert.assertNotNull(
            NativeHandleOwnershipException.class.getConstructor(long.class, long.class)
    );
    Assert.assertEquals(
            Set.of("expectedAllocatorId", "actualAllocatorId"),
            Arrays.stream(NativeHandleOwnershipException.class.getDeclaredMethods())
                    .map(Method::getName)
                    .collect(Collectors.toSet())
    );
    Assert.assertEquals(
            long.class,
            NativeHandleOwnershipException.class
                    .getDeclaredMethod("expectedAllocatorId")
                    .getReturnType()
    );
    Assert.assertEquals(
            long.class,
            NativeHandleOwnershipException.class
                    .getDeclaredMethod("actualAllocatorId")
                    .getReturnType()
    );
    Assert.assertEquals(11L, failure.expectedAllocatorId());
    Assert.assertEquals(12L, failure.actualAllocatorId());
    Assert.assertEquals(
            long.class,
            NativeHandleOwnershipException.class
                    .getDeclaredField("expectedAllocatorId")
                    .getType()
    );
    Assert.assertEquals(
            long.class,
            NativeHandleOwnershipException.class
                    .getDeclaredField("actualAllocatorId")
                    .getType()
    );
    Assert.assertTrue(failure instanceof NativeMemoryException);
}

@Test
public void publicMemoryApiHasNoRawOperationOrLegacyAllocator() throws Exception {
    Set<String> methodNames = Arrays.stream(StableMemoryBackend.class.getMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

    Assert.assertFalse(methodNames.stream().anyMatch(name -> name.endsWith("Raw")));
    Assert.assertThrows(
            ClassNotFoundException.class,
            () -> Class.forName("yier.bubu.redis.memory.api.NativeAllocator")
    );
    Assert.assertArrayEquals(
            new String[]{"allocatorId", "localRaw"},
            Arrays.stream(NativeHandle.class.getRecordComponents())
                    .map(component -> component.getName())
                    .toArray(String[]::new)
    );
    Assert.assertEquals(1, NativeHandle.class.getDeclaredConstructors().length);
    Assert.assertNotNull(NativeHandle.class.getConstructor(long.class, long.class));
    Assert.assertEquals(
            Set.of(
                    "allocatorId():long",
                    "localRaw():long",
                    "isNull():boolean",
                    "equals(java.lang.Object):boolean",
                    "hashCode():int",
                    "toString():java.lang.String"
            ),
            Arrays.stream(NativeHandle.class.getDeclaredMethods())
                    .map(StableMemoryBackendContractTest::methodDescriptor)
                    .collect(Collectors.toSet())
    );
    Set<String> retiredHandleMethods = Set.of(
            "raw", "fromRaw", "rawOf", "of", "requireNonNull", "requireValidRaw", "domain",
            "domainCode", "kindCode", "slotId", "generation", "flags"
    );
    Set<String> handleMethods = Arrays.stream(NativeHandle.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());
    for (String retired : retiredHandleMethods) {
        Assert.assertFalse("retired NativeHandle method remains: " + retired,
                handleMethods.contains(retired));
    }
    Assert.assertThrows(
            NoSuchMethodException.class,
            () -> NativeHandle.class.getDeclaredMethod("isNull", long.class)
    );
}

private static void assertAbstractMethod(
        Class<?> owner,
        Class<?> returnType,
        String name,
        Class<?>... parameterTypes
) throws Exception {
    Method method = owner.getDeclaredMethod(name, parameterTypes);
    Assert.assertEquals(name, returnType, method.getReturnType());
    Assert.assertTrue(name + " must be abstract", Modifier.isAbstract(method.getModifiers()));
}

private static String methodDescriptor(Method method) {
    String parameters = Arrays.stream(method.getParameterTypes())
            .map(Class::getTypeName)
            .collect(Collectors.joining(","));
    return method.getName() + "(" + parameters + "):" + method.getReturnType().getTypeName();
}
```

Rename `FailOnAllocationNativeAllocatorTest` to
`FailOnAllocationStableMemoryBackendTest` and replace its allocator fixture
with a handwritten recording backend, never a dynamic proxy. The renamed test
proves allocation and capacity-growing reallocation consume attempts, whereas
shrink and within-capacity reallocation neither consume the quota nor throw.
The two failure paths must assert the allocation category directly:

```java
Assert.assertThrows(
        NativeCapacityExceededException.class,
        () -> backend.allocate(NativeObjectKind.STRING_BYTES, 8)
);
Assert.assertThrows(
        NativeCapacityExceededException.class,
        () -> backend.reallocate(handle, 32, NativeReallocPolicy.PRESERVE_PREFIX)
);
```

The test method `delegatesEveryNonAllocationOperationExactlyOnce()` invokes
every pass-through exactly once against an ordered recording backend and
verifies both exact argument identity and returned sentinel identity for
`allocatorId`, owner binding, `free`, `pin`, `unpin`, both scope factories,
bookkeeping estimation, `resolve`, `resolvePinned`, region allocation, both
defrag methods, `logicalUsedBytes`, `stats`, `metadataStats`, `memoryUsage`,
trim, both growth estimators, `liveRegionCount`, and `close`. This prevents a
complete-looking wrapper from forwarding a method to the wrong delegate
operation.

- [ ] **Step 3: Run the memory API tests and verify RED**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-testkit -am \
  -Dtest=NativeHandleTest,StableMemoryBackendContractTest,FailOnAllocationStableMemoryBackendTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: test compilation fails because the stable backend types, two-part
handle, and renamed `FailOnAllocationStableMemoryBackend` wrapper do not exist.
If the new types are added before deleting the old interface,
`publicMemoryApiHasNoRawOperationOrLegacyAllocator` remains red because
`NativeAllocator` is still loadable.

- [ ] **Step 4: Replace the public handle and allocate non-reusable backend IDs**

Replace `NativeHandle` completely; domain, kind, slot, generation, flags, `fromRaw`, `rawOf`, and single-raw helper methods leave the public API:

```java
package yier.bubu.redis.memory.api;

/**
 * 标识由某个 {@link StableMemoryBackend} 拥有的稳定内存对象。
 * {@code allocatorId} 是后端身份，{@code localRaw} 是只能由所属后端解释的不透明局部值；
 * 调用方不得解码该值，也不得把句柄交给其他后端。重分配和整理搬迁不改变句柄身份，
 * 释放对象后句柄失效；仅当两个分量都为 {@code 0} 时表示空句柄。
 */
public record NativeHandle(long allocatorId, long localRaw) {
    public static final NativeHandle NULL = new NativeHandle(0L, 0L);

    public boolean isNull() {
        return allocatorId == 0L && localRaw == 0L;
    }
}
```

Create a package-private sequence so exhaustion can be tested without mutating
the process-global source. Its zero value is a permanent exhaustion sentinel:
the final positive value may be returned once, and no later call can wrap to a
reused ID.

```java
package yier.bubu.redis.memory.api;

import java.util.concurrent.atomic.AtomicLong;

final class StableMemoryBackendIdSequence {
    private final AtomicLong next;

    StableMemoryBackendIdSequence(long initialId) {
        if (initialId <= 0L) {
            throw new IllegalArgumentException("initialId must be positive");
        }
        this.next = new AtomicLong(initialId);
    }

    long nextId() {
        for (;;) {
            long current = next.get();
            if (current <= 0L) {
                throw new IllegalStateException("stable memory backend IDs are exhausted");
            }
            long successor = current == Long.MAX_VALUE ? 0L : current + 1L;
            if (next.compareAndSet(current, successor)) {
                return current;
            }
        }
    }
}
```

The public process-wide source owns one non-resettable sequence:

```java
package yier.bubu.redis.memory.api;

/**
 * 分配进程范围内的稳定内存后端身份。
 * 返回值始终为正且从不复用；可用正值耗尽后，当前进程中的后续分配永久失败。
 */
public final class StableMemoryBackendIds {
    private static final StableMemoryBackendIdSequence PROCESS_IDS =
            new StableMemoryBackendIdSequence(1L);

    private StableMemoryBackendIds() {
    }

    /** 返回新的进程唯一身份；身份空间耗尽时抛出 {@link IllegalStateException}。 */
    public static long nextId() {
        return PROCESS_IDS.nextId();
    }
}
```

During review, verify there is no reset method or second production sequence;
only the isolated package-private sequence test may choose a starting ID.

Create the dedicated ownership error. Its fields make tests and callers independent of message wording:

```java
package yier.bubu.redis.memory.api;

/**
 * 表示句柄所属后端与接收操作的后端不一致。
 * expected/actual 身份作为结构化契约公开，调用方无需解析异常消息。
 */
public final class NativeHandleOwnershipException extends NativeMemoryException {
    private final long expectedAllocatorId;
    private final long actualAllocatorId;

    public NativeHandleOwnershipException(long expected, long actual) {
        super("native handle belongs to allocator " + actual + ", expected " + expected);
        this.expectedAllocatorId = expected;
        this.actualAllocatorId = actual;
    }

    /** 返回接收本次操作的后端身份。 */
    public long expectedAllocatorId() {
        return expectedAllocatorId;
    }

    /** 返回句柄携带的所属后端身份。 */
    public long actualAllocatorId() {
        return actualAllocatorId;
    }
}
```

- [ ] **Step 5: Add the owner, region, factory, and complete backend contracts**

Create these interfaces exactly:

```java
package yier.bubu.redis.memory.api;

/**
 * 为稳定内存后端提供显式 owner thread 绑定与校验。
 * 普通访问不会隐式绑定；未绑定实例可以执行关闭校验，绑定后只能由 owner thread 关闭。
 */
public interface MemoryOwner {
    /** 将未绑定 owner 绑定到当前线程；同一线程重复绑定有效，跨线程绑定失败。 */
    void bindToCurrentThread();

    /** 校验普通访问来自已绑定的 owner thread；绑定前或跨线程访问失败。 */
    void checkCurrentThread();

    /** 校验关闭来自 owner thread；尚未绑定时允许关闭，绑定后拒绝跨线程关闭。 */
    void checkCurrentThreadForShutdown();
}
```

```java
package yier.bubu.redis.memory.api;

/**
 * 为指定 {@link MemoryOwner} 创建独立且尚未绑定的稳定内存后端。
 * 每次调用都返回非 {@code null} 的新后端；调用方接管后端及其关闭责任，工厂不得代为绑定 owner。
 */
@FunctionalInterface
public interface StableMemoryBackendFactory {
    StableMemoryBackend create(String name, int maxSlots, MemoryOwner owner);
}
```

```java
package yier.bubu.redis.memory.api;

/**
 * 由后端分配并计入其内存用量的固定大小区域。
 * 多字节整数固定使用 little-endian，{@link #copyTo} 在源和目标重叠时也保持复制语义。
 * 复制只传递字节，不转移任一区域的所有权。区域沿用后端 owner 约束；
 * 调用方须在关闭后端前关闭区域，关闭后不得继续访问。
 */
public interface StableMemoryRegion extends AutoCloseable {
    int size();
    byte getByte(int offset);
    void setByte(int offset, byte value);
    int getIntLittleEndian(int offset);
    void setIntLittleEndian(int offset, int value);
    long getLongLittleEndian(int offset);
    void setLongLittleEndian(int offset, long value);
    void getBytes(int offset, byte[] dst, int dstOffset, int length);
    void setBytes(int offset, byte[] src, int srcOffset, int length);
    void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length);

    /** 释放区域；重复关闭不重复释放资源或扣减后端统计。 */
    @Override void close();
}
```

`StableMemoryBackend` must declare every operation without a default implementation:

```java
package yier.bubu.redis.memory.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

/**
 * 提供稳定对象、独立区域、回收和统计能力的 owner-bound 后端。
 * 每个实例具有进程内不复用的正 {@code allocatorId}；句柄必须先校验该身份，
 * 再由所属后端解释 {@code localRaw}。内存访问须显式绑定，所有视图、scope、epoch、
 * 显式 pin 和 region 都须在后端关闭前结束。
 */
public interface StableMemoryBackend extends AutoCloseable {
    long allocatorId();

    /** 显式绑定 owner；任何普通内存访问都不得代替调用方完成绑定。 */
    void bindToCurrentThread();
    NativeHandle allocate(NativeObjectKind kind, int size);

    /** 调整容量并保持完整句柄身份不变。 */
    NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy);
    void free(NativeHandle handle);

    /** 保留对象；每次成功调用都必须由同一 owner 配对调用 {@link #unpin(NativeHandle)}。 */
    void pin(NativeHandle handle);
    void unpin(NativeHandle handle);
    NativeEpochScope beginEpoch(NativeEpochKind kind);
    NativeAllocationScope beginAllocationScope();
    long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount);

    /** 返回拥有自身保留的视图；关闭视图会释放该保留。 */
    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    /**
     * 借用调用方已经持有的 pin，只接受 {@link NativeAccessMode#READ_ONLY}；
     * 关闭视图不会替调用方执行 {@link #unpin(NativeHandle)}。
     */
    NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode);
    StableMemoryRegion allocateRegion(String owner, int bytes);
    NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes);
    NativeDefragReport defragCycle(NativeDefragOptions options);
    long logicalUsedBytes();
    NativeAllocatorStats stats();
    NativeAllocatorMetadataStats metadataStats();
    MemoryUsageSnapshot memoryUsage();
    MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget);
    NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes);
    NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes);
    long liveRegionCount();

    /** 按 owner 的 shutdown 规则释放后端；调用前必须先结束所有派生资源和显式 pin。 */
    @Override void close();
}
```

Add these caller-contract Javadocs immediately before the existing public type
declarations; keep their method bodies and signatures unchanged:

```java
/**
 * 在受限生命周期内访问稳定对象的当前内容。
 * 视图沿用后端 owner 和访问模式；只读视图拒绝写入，关闭后任何内容访问都无效。
 * 普通 resolve 的视图释放自身保留，resolvePinned 的视图不会替调用方 unpin。
 */
public interface NativeObjectView extends AutoCloseable {
```

```java
/**
 * 跟踪同一后端在单一活动分配范围内新建的对象。
 * {@link #promote()} 提交对象并把后续释放责任交给调用方；{@link #abort()} 释放仍被跟踪的对象；
 * {@link #close()} 等同 abort，因此成功路径必须显式 promote。终止操作可重复调用且不影响后续 scope。
 */
public interface NativeAllocationScope extends AutoCloseable {
```

```java
/**
 * 表示后端回收屏障中的活动 epoch。
 * 关闭前，该 epoch 保护的退役存储不得回收；关闭只撤销屏障，不保证立即回收。
 * scope 沿用后端 owner，必须在后端前关闭，重复关闭无效果。
 */
public interface NativeEpochScope extends AutoCloseable {
```

Keep the existing stats type names in this migration; they are semantic memory records, not extension points. `NativeHandleDomain` and `NativeObjectKind` remain allocation metadata used by the FFM-local codec, but no public method decodes them from `NativeHandle.localRaw()`.

- [ ] **Step 6: Port the allocation-failure testkit without weakening the interface**

Replace the renamed testkit class with this complete delegation surface; only `allocate` and `reallocate` alter behavior:

```java
package yier.bubu.redis.memory.testkit;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.*;

public final class FailOnAllocationStableMemoryBackend implements StableMemoryBackend {
    private final StableMemoryBackend delegate;
    private final AtomicLong attempts = new AtomicLong();
    private final Map<NativeHandle, Integer> knownCapacities = new ConcurrentHashMap<>();
    private volatile long failAt = -1L;

    public FailOnAllocationStableMemoryBackend(StableMemoryBackend delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    public void failOnAllocation(long oneBasedIndex) {
        if (oneBasedIndex <= 0L) {
            throw new IllegalArgumentException("oneBasedIndex must be > 0");
        }
        failAt = oneBasedIndex;
    }

    public void disableFailures() {
        failAt = -1L;
    }

    public void resetAttempts() {
        attempts.set(0L);
    }

    public long allocationAttempts() {
        return attempts.get();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        checkAllocationAttempt();
        NativeHandle handle = delegate.allocate(kind, size);
        rememberCapacity(handle, size);
        return handle;
    }

    @Override
    public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        Objects.requireNonNull(handle, "handle");
        int currentCapacity = capacityOf(handle);
        if (newSize > currentCapacity) {
            checkAllocationAttempt();
        }
        NativeHandle resized = delegate.reallocate(handle, newSize, policy);
        knownCapacities.remove(handle);
        rememberCapacity(resized, Math.max(newSize, currentCapacity));
        return resized;
    }

    @Override public long allocatorId() { return delegate.allocatorId(); }
    @Override public void bindToCurrentThread() { delegate.bindToCurrentThread(); }
    @Override public void free(NativeHandle handle) {
        knownCapacities.remove(handle);
        delegate.free(handle);
    }
    @Override public void pin(NativeHandle handle) { delegate.pin(handle); }
    @Override public void unpin(NativeHandle handle) { delegate.unpin(handle); }
    @Override public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        return delegate.beginEpoch(kind);
    }
    @Override public NativeAllocationScope beginAllocationScope() {
        return delegate.beginAllocationScope();
    }
    @Override public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        return delegate.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
    }
    @Override public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        return delegate.resolve(handle, mode);
    }
    @Override public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return delegate.resolvePinned(handle, mode);
    }
    @Override public StableMemoryRegion allocateRegion(String owner, int bytes) {
        return delegate.allocateRegion(owner, bytes);
    }
    @Override public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        return delegate.defragOne(handle, maxMoveBytes);
    }
    @Override public NativeDefragReport defragCycle(NativeDefragOptions options) {
        return delegate.defragCycle(options);
    }
    @Override public long logicalUsedBytes() { return delegate.logicalUsedBytes(); }
    @Override public NativeAllocatorStats stats() { return delegate.stats(); }
    @Override public NativeAllocatorMetadataStats metadataStats() { return delegate.metadataStats(); }
    @Override public MemoryUsageSnapshot memoryUsage() { return delegate.memoryUsage(); }
    @Override public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        return delegate.trimEmptyPages(budget);
    }
    @Override public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return delegate.estimateAdditionalGrowth(requestedBytes);
    }
    @Override public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        return delegate.estimateConservativeAdditionalGrowth(requestedBytes);
    }
    @Override public long liveRegionCount() { return delegate.liveRegionCount(); }
    @Override public void close() {
        knownCapacities.clear();
        delegate.close();
    }

    private void checkAllocationAttempt() {
        long attempt = attempts.incrementAndGet();
        if (attempt == failAt) {
            throw new NativeCapacityExceededException(
                    "injected stable memory allocation failure at attempt " + attempt
            );
        }
    }

    private int capacityOf(NativeHandle handle) {
        Integer known = knownCapacities.get(handle);
        if (known != null) {
            return known;
        }
        NativeObjectView view = delegate.resolve(handle, NativeAccessMode.READ_ONLY);
        if (view == null) {
            return 0;
        }
        try {
            int capacity = view.capacity();
            knownCapacities.put(handle, capacity);
            return capacity;
        } finally {
            view.close();
        }
    }

    private void rememberCapacity(NativeHandle handle, int fallback) {
        if (handle == null) {
            return;
        }
        int capacity = fallback;
        try {
            NativeObjectView view = delegate.resolve(handle, NativeAccessMode.READ_ONLY);
            if (view != null) {
                try {
                    capacity = view.capacity();
                } finally {
                    view.close();
                }
            }
        } catch (RuntimeException ignored) {
            // 测试替身可能不提供对象视图，此时用请求大小作为容量下界。
        }
        knownCapacities.put(handle, capacity);
    }
}
```

Keep `failOnAllocation`, `disableFailures`, `resetAttempts`, and
`allocationAttempts` as the single canonical controls so Task 4 changes
`MutationFaultInjectionTest` and `ZSetValueTest` only to the renamed type and
backend construction. The map is keyed by the complete `NativeHandle`.

- [ ] **Step 7: Run focused API and testkit tests and verify GREEN**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-testkit -am \
  -Dtest=NativeHandleTest,StableMemoryBackendContractTest,FailOnAllocationStableMemoryBackendTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

Expected: PASS. The clean phase removes deleted `NativeAllocator` bytecode
before the `Class.forName` absence assertion. Reflection proves exact abstract
signatures, the public handle exposes no packed codec, backend IDs stay unique
under concurrent allocation and permanently exhaust in isolated state, and the
capacity-aware failure wrapper preserves allocator identity and every delegate
operation.

- [ ] **Step 8: Run the broader API-side memory suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-testkit -am test
```

Expected: PASS for the API owner and testkit checkpoint. At this deliberate API-break checkpoint, `yierdis-memory-ffm` is the direct owner-side consumer of the deleted allocator/raw-handle surface and Task 3 restores it. The exact Task 4 direct restoration set is `yierdis-db-memory`, `yierdis-server-main`, `yierdis-architecture-tests`, `yierdis-integration-tests`, and `yierdis-benchmark`; `yierdis-cli` is additionally broken only through its test-scoped dependency on `yierdis-server-main`. Task 4 migrates the five direct modules and verifies the transitive CLI restoration. No legacy adapter is introduced at either boundary.

- [ ] **Step 9: Commit the stable memory API break**

```bash
git add \
  yierdis-memory/yierdis-memory-api \
  yierdis-memory/yierdis-memory-testkit
git commit -m "refactor: define allocator-scoped stable memory API"
```

---

### Task 3: Implement The FFM Facade And Enforce Handle Ownership First

**Files:**
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmStableMemoryBackend.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisLocalHandleCodec.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectMeta.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeDefragPlanner.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeDefragValidator.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmRegion.java`
- Rename: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/SynchronizedNativeAllocator.java` to `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/SynchronizedStableMemoryBackend.java`
- Delete: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisAllocatorThreadGuard.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorOwnershipTest.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisLocalHandleCodecTest.java`
- Rename: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/SynchronizedNativeAllocatorTest.java` to `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/SynchronizedStableMemoryBackendTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/NativeAllocationScopeTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntimeTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTableTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorTest.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

**Interfaces:**
- Produces: the sole public FFM composition class, `YierdisFfmStableMemoryBackend`, whose constructor matches `StableMemoryBackendFactory`.
- Internal only: FFM runtime, arenas, regions, spans, object table, local raw codec, page allocator, and stable allocator.
- Invariant: reallocate, resolve, resolve-pinned, free, pin, unpin, and defrag validate `allocatorId` before interpreting `localRaw` or looking up a slot.
- Invariant: backend access never binds on first use. `bindToCurrentThread()` delegates to the supplied `MemoryOwner`; every other operation checks it.

- [ ] **Step 1: Write failing ownership, binding, and local-codec tests**

Create `YierdisStableNativeAllocatorOwnershipTest.java`:

```java
package yier.bubu.redis.memory.foreign;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.*;

public class YierdisStableNativeAllocatorOwnershipTest {
    @Test
    public void firstAccessDoesNotBindOwnerImplicitly() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("unbound", owner)) {
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> backend.allocate(NativeObjectKind.GENERIC, 8)
            );

            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 8);
            backend.free(handle);
        }
    }

    @Test
    public void sameLocalRawFromTwoBackendsCannotAlias() {
        TestOwner firstOwner = new TestOwner();
        TestOwner secondOwner = new TestOwner();
        try (StableMemoryBackend first = backend("first", firstOwner);
             StableMemoryBackend second = backend("second", secondOwner)) {
            first.bindToCurrentThread();
            second.bindToCurrentThread();
            NativeHandle firstHandle = first.allocate(NativeObjectKind.GENERIC, 8);
            NativeHandle secondHandle = second.allocate(NativeObjectKind.GENERIC, 8);

            Assert.assertEquals(firstHandle.localRaw(), secondHandle.localRaw());
            Assert.assertNotEquals(firstHandle.allocatorId(), secondHandle.allocatorId());
            assertOwnershipFailure(first, secondHandle);

            first.free(firstHandle);
            second.free(secondHandle);
        }
    }

    @Test
    public void ownershipIsCheckedBeforeMalformedLocalRaw() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("order", owner)) {
            backend.bindToCurrentThread();
            NativeHandle foreignMalformed = new NativeHandle(backend.allocatorId() + 1L, Long.MIN_VALUE);

            assertOwnershipFailure(backend, foreignMalformed);
            NativeHandleOwnershipException failure = Assert.assertThrows(
                    NativeHandleOwnershipException.class,
                    () -> backend.resolve(foreignMalformed, NativeAccessMode.READ_ONLY)
            );

            Assert.assertEquals(backend.allocatorId(), failure.expectedAllocatorId());
            Assert.assertEquals(foreignMalformed.allocatorId(), failure.actualAllocatorId());
        }
    }

    @Test
    public void everyDerivedResourceOperationChecksTheBoundOwner() throws Exception {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("derived-owner", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 16);
            NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE);

            assertWrongThreadRejected(view::handle);
            assertWrongThreadRejected(view::size);
            assertWrongThreadRejected(view::capacity);
            assertWrongThreadRejected(() -> view.getByte(0));
            assertWrongThreadRejected(() -> view.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> view.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.copyBytes(0, 1, 1));
            assertWrongThreadRejected(() -> view.contentEquals(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> view.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> view.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> view.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(view::close);
            Assert.assertEquals(16, view.size());
            Assert.assertEquals(0, view.getByte(0));
            view.close();
            backend.free(handle);

            NativeAllocationScope allocationScope = backend.beginAllocationScope();
            assertWrongThreadRejected(allocationScope::growth);
            assertWrongThreadRejected(allocationScope::promote);
            assertWrongThreadRejected(allocationScope::abort);
            assertWrongThreadRejected(allocationScope::close);
            NativeHandle scoped = backend.allocate(NativeObjectKind.GENERIC, 8);
            allocationScope.abort();
            Assert.assertThrows(
                    StaleNativeHandleException.class,
                    () -> backend.resolve(scoped, NativeAccessMode.READ_ONLY)
            );

            NativeEpochScope epochScope = backend.beginEpoch(NativeEpochKind.COMMAND);
            assertWrongThreadRejected(epochScope::kind);
            assertWrongThreadRejected(epochScope::epoch);
            assertWrongThreadRejected(epochScope::close);
            epochScope.close();

            StableMemoryRegion region = backend.allocateRegion("derived-region", 16);
            assertWrongThreadRejected(region::size);
            assertWrongThreadRejected(() -> region.getByte(0));
            assertWrongThreadRejected(() -> region.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> region.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> region.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> region.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> region.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(() -> region.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.copyTo(0, region, 1, 1));
            assertWrongThreadRejected(region::close);
            Assert.assertEquals(16, region.size());
            Assert.assertEquals(0, region.getByte(0));
            region.close();
        }
    }

    @Test
    public void pinnedViewsAreReadOnlyAndDoNotOwnTheCallersPin() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("pinned-view", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 8);
            backend.pin(handle);

            Assert.assertThrows(
                    NativeMemoryException.class,
                    () -> backend.resolvePinned(handle, NativeAccessMode.READ_WRITE)
            );
            try (NativeObjectView borrowed =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, borrowed.handle());
                Assert.assertThrows(
                        NativeMemoryException.class,
                        () -> borrowed.setByte(0, (byte) 1)
                );
            }
            try (NativeObjectView ignored =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, ignored.handle());
            }

            backend.unpin(handle);
            backend.free(handle);
        }
    }

    private static void assertOwnershipFailure(StableMemoryBackend backend, NativeHandle foreign) {
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.reallocate(foreign, 16, NativeReallocPolicy.PRESERVE_PREFIX)
        );
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.resolve(foreign, NativeAccessMode.READ_ONLY)
        );
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.resolvePinned(foreign, NativeAccessMode.READ_ONLY)
        );
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.free(foreign));
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.pin(foreign));
        Assert.assertThrows(NativeHandleOwnershipException.class, () -> backend.unpin(foreign));
        Assert.assertThrows(
                NativeHandleOwnershipException.class,
                () -> backend.defragOne(foreign, 16L)
        );
    }

    private static StableMemoryBackend backend(String name, TestOwner owner) {
        return new YierdisFfmStableMemoryBackend(name, 128, owner);
    }

    private static void assertWrongThreadRejected(Runnable operation) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                operation.run();
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "wrong-memory-owner");
        worker.setDaemon(true);
        worker.start();
        worker.join(5_000L);

        if (worker.isAlive()) worker.interrupt();
        Assert.assertFalse("wrong-owner operation did not finish", worker.isAlive());
        Assert.assertTrue(
                "expected owner rejection, got " + failure.get(),
                failure.get() instanceof IllegalStateException
        );
    }

    private static final class TestOwner implements MemoryOwner {
        private final AtomicReference<Thread> owner = new AtomicReference<>();

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            Thread existing = owner.get();
            if (existing == current) return;
            if (existing != null || !owner.compareAndSet(null, current)) {
                throw new IllegalStateException("memory owner already belongs to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner.get() != Thread.currentThread()) {
                throw new IllegalStateException("memory access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            Thread existing = owner.get();
            if (existing != null && existing != Thread.currentThread()) {
                throw new IllegalStateException("memory shutdown is outside the owner thread");
            }
        }
    }
}
```

Create `YierdisLocalHandleCodecTest.java` by moving the still-valid packed
format coverage out of the public `NativeHandleTest`. The codec remains
package-private, so the test lives in the same FFM package:

```java
package yier.bubu.redis.memory.foreign;

import java.lang.management.ManagementFactory;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class YierdisLocalHandleCodecTest {
    @Test
    public void encodesAndDecodesHandleFields() {
        long localRaw = YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        YierdisLocalHandleCodec.requireValid(localRaw);
        Assert.assertEquals(0x1100_075b_cd15_04d3L, localRaw);
        Assert.assertEquals(
                NativeHandleDomain.STORAGE_OBJECT,
                YierdisLocalHandleCodec.domain(localRaw)
        );
        Assert.assertEquals(
                NativeObjectKind.STRING_BYTES.code(),
                YierdisLocalHandleCodec.kindCode(localRaw)
        );
        Assert.assertEquals(123456789L, YierdisLocalHandleCodec.slotId(localRaw));
        Assert.assertEquals(77, YierdisLocalHandleCodec.generation(localRaw));
        Assert.assertEquals(3, YierdisLocalHandleCodec.flags(localRaw));
    }

    @Test
    public void primitiveLocalDecodersDoNotAllocatePerCall() {
        long localRaw = YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        for (int index = 0; index < 10_000; index++) {
            consumeLocalFields(localRaw);
        }
        long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int index = 0; index < 100_000; index++) {
            consumeLocalFields(localRaw);
        }
        long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        Assert.assertEquals(
                NativeHandleDomain.STORAGE_OBJECT,
                YierdisLocalHandleCodec.domain(localRaw)
        );
        Assert.assertEquals(
                NativeObjectKind.STRING_BYTES.code(),
                YierdisLocalHandleCodec.kindCode(localRaw)
        );
        Assert.assertEquals(123456789L, YierdisLocalHandleCodec.slotId(localRaw));
        Assert.assertEquals(77, YierdisLocalHandleCodec.generation(localRaw));
        Assert.assertEquals(3, YierdisLocalHandleCodec.flags(localRaw));
        Assert.assertTrue(
                "primitive local-handle decoding allocated " + allocatedBytes + " bytes",
                allocatedBytes < 4_096L
        );
    }

    @Test
    public void rejectsOutOfRangeFields() {
        assertIllegal(() -> encode(-1L, 1, 0));
        assertIllegal(() -> encode(1L << 40, 1, 0));
        assertIllegal(() -> encode(1L, -1, 0));
        assertIllegal(() -> encode(1L, 4096, 0));
        assertIllegal(() -> encode(1L, 1, -1));
        assertIllegal(() -> encode(1L, 1, 16));
    }

    @Test
    public void rejectsMismatchedDomainAndKind() {
        assertIllegal(() -> YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.ENTRY_RECORD,
                1L,
                1,
                0
        ));
    }

    @Test
    public void rejectsNonZeroReservedDomain() {
        assertIllegal(() -> YierdisLocalHandleCodec.requireValid(0x10L));
    }

    private static long encode(long slotId, int generation, int flags) {
        return YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                slotId,
                generation,
                flags
        );
    }

    private static void assertIllegal(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    private static long consumeLocalFields(long localRaw) {
        return YierdisLocalHandleCodec.domain(localRaw).code()
                + YierdisLocalHandleCodec.kindCode(localRaw)
                + YierdisLocalHandleCodec.slotId(localRaw)
                + YierdisLocalHandleCodec.generation(localRaw)
                + YierdisLocalHandleCodec.flags(localRaw);
    }

    private static com.sun.management.ThreadMXBean allocatedBytesBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assert.assertTrue(
                "thread allocation accounting is unavailable",
                bean instanceof com.sun.management.ThreadMXBean
        );
        com.sun.management.ThreadMXBean allocatedBytesBean =
                (com.sun.management.ThreadMXBean) bean;
        Assert.assertTrue(
                "thread allocation accounting is unsupported",
                allocatedBytesBean.isThreadAllocatedMemorySupported()
        );
        if (!allocatedBytesBean.isThreadAllocatedMemoryEnabled()) {
            allocatedBytesBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocatedBytesBean;
    }
}
```

- [ ] **Step 2: Add failing region/accounting coverage**

Add these methods to the same test class:

```java
@Test
public void regionProvidesBackendNeutralTypedAccessAndCopy() {
    TestOwner owner = new TestOwner();
    try (StableMemoryBackend backend = backend("regions", owner)) {
        backend.bindToCurrentThread();
        try (StableMemoryRegion source = backend.allocateRegion("source", 32);
             StableMemoryRegion target = backend.allocateRegion("target", 32)) {
            source.setByte(0, (byte) 7);
            source.setIntLittleEndian(4, 0x78563412);
            source.setLongLittleEndian(8, 0x0102030405060708L);
            byte[] sourceBytes = new byte[12];
            source.getBytes(4, sourceBytes, 0, sourceBytes.length);
            Assert.assertArrayEquals(
                    new byte[]{
                            0x12, 0x34, 0x56, 0x78,
                            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
                    },
                    sourceBytes
            );
            source.copyTo(0, target, 0, 16);

            Assert.assertEquals(7, target.getByte(0));
            Assert.assertEquals(0x78563412, target.getIntLittleEndian(4));
            Assert.assertEquals(0x0102030405060708L, target.getLongLittleEndian(8));
            byte[] targetBytes = new byte[12];
            target.getBytes(4, targetBytes, 0, targetBytes.length);
            Assert.assertArrayEquals(sourceBytes, targetBytes);
            Assert.assertTrue(backend.liveRegionCount() >= 2L);
        }
    }
}

@Test
public void sameRegionCopyIsOverlapSafeAcrossPortableChunkBoundary() {
    TestOwner owner = new TestOwner();
    try (StableMemoryBackend backend = backend("overlap", owner)) {
        backend.bindToCurrentThread();
        try (StableMemoryRegion region = backend.allocateRegion("overlap", 16_384)) {
            byte[] original = new byte[16_384];
            for (int index = 0; index < original.length; index++) {
                original[index] = (byte) (index % 251);
            }
            region.setBytes(0, original, 0, original.length);

            region.copyTo(0, region, 4_096, 12_000);

            byte[] expected = original.clone();
            System.arraycopy(original, 0, expected, 4_096, 12_000);
            byte[] actual = new byte[expected.length];
            region.getBytes(0, actual, 0, actual.length);
            Assert.assertArrayEquals(expected, actual);
        }
    }
}

@Test
public void regionRejectsUseAfterClose() {
    TestOwner owner = new TestOwner();
    try (StableMemoryBackend backend = backend("closed-region", owner)) {
        backend.bindToCurrentThread();
        StableMemoryRegion region = backend.allocateRegion("closed", 16);
        region.close();

        Assert.assertThrows(IllegalStateException.class, region::size);
        Assert.assertThrows(IllegalStateException.class, () -> region.getByte(0));
        Assert.assertThrows(
                IllegalStateException.class,
                () -> region.setIntLittleEndian(0, 1)
        );
    }
}

@Test
public void externallyAllocatedRegionIsCountedOnce() {
    TestOwner owner = new TestOwner();
    try (StableMemoryBackend backend = backend("accounting", owner)) {
        backend.bindToCurrentThread();
        long before = backend.memoryUsage().nativeDataCommittedBytes();
        try (StableMemoryRegion ignored = backend.allocateRegion("index", 257)) {
            Assert.assertEquals(
                    257L,
                    backend.memoryUsage().nativeDataCommittedBytes() - before
            );
        }
        Assert.assertEquals(before, backend.memoryUsage().nativeDataCommittedBytes());
    }
}
```

- [ ] **Step 3: Run the focused FFM tests and verify RED**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-ffm -am \
  -Dtest=YierdisStableNativeAllocatorOwnershipTest,YierdisLocalHandleCodecTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: main compilation fails because the FFM implementation still implements deleted `NativeAllocator`, uses one-part handles/raw operations, and lacks the public facade constructor.

- [ ] **Step 4: Move packed local-handle logic behind the FFM package boundary**

Move the old domain/kind/slot/generation/flags constants and validation from public `NativeHandle` into package-private `YierdisLocalHandleCodec`. Its API is local-only:

```java
package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeObjectKind;

final class YierdisLocalHandleCodec {
    private static final int DOMAIN_SHIFT = 60;
    private static final int KIND_SHIFT = 56;
    private static final int SLOT_SHIFT = 16;
    private static final int GENERATION_SHIFT = 4;
    private static final long FOUR_BIT_MASK = 0x0fL;
    private static final long SLOT_MASK = (1L << 40) - 1L;
    private static final long GENERATION_MASK = (1L << 12) - 1L;

    private YierdisLocalHandleCodec() {
    }

    static long encode(
            NativeHandleDomain domain,
            NativeObjectKind kind,
            long slotId,
            int generation,
            int flags
    ) {
        Objects.requireNonNull(domain, "domain");
        Objects.requireNonNull(kind, "kind");
        if (domain == NativeHandleDomain.RESERVED || kind.domain() != domain) {
            throw new IllegalArgumentException("invalid local handle domain/kind");
        }
        if (slotId < 0L || slotId > SLOT_MASK) {
            throw new IllegalArgumentException("slotId out of range: " + slotId);
        }
        if (generation < 0 || generation > GENERATION_MASK) {
            throw new IllegalArgumentException("generation out of range: " + generation);
        }
        if (flags < 0 || flags > FOUR_BIT_MASK) {
            throw new IllegalArgumentException("flags out of range: " + flags);
        }
        return ((long) domain.code() << DOMAIN_SHIFT)
                | ((long) kind.code() << KIND_SHIFT)
                | (slotId << SLOT_SHIFT)
                | ((long) generation << GENERATION_SHIFT)
                | flags;
    }

    static void requireValid(long localRaw) {
        if (localRaw != 0L && domainCode(localRaw) == NativeHandleDomain.RESERVED.code()) {
            throw new IllegalArgumentException("non-zero local handle cannot use reserved domain");
        }
    }

    static NativeHandleDomain domain(long localRaw) {
        return NativeHandleDomain.fromCode(domainCode(localRaw));
    }

    static int kindCode(long localRaw) {
        return (int) ((localRaw >>> KIND_SHIFT) & FOUR_BIT_MASK);
    }

    static long slotId(long localRaw) {
        return (localRaw >>> SLOT_SHIFT) & SLOT_MASK;
    }

    static int generation(long localRaw) {
        return (int) ((localRaw >>> GENERATION_SHIFT) & GENERATION_MASK);
    }

    static int flags(long localRaw) {
        return (int) (localRaw & FOUR_BIT_MASK);
    }

    private static int domainCode(long localRaw) {
        return (int) ((localRaw >>> DOMAIN_SHIFT) & FOUR_BIT_MASK);
    }
}
```

All object-table/page/defrag code uses this package-private codec. No method accepting only `long localRaw` is public, and no class outside `yier.bubu.redis.memory.foreign` imports the codec.

- [ ] **Step 5: Scope every public allocator operation before local lookup**

Keep `YierdisStableNativeAllocator` as the internal stable-object engine; `YierdisFfmStableMemoryBackend` is the sole complete `StableMemoryBackend` facade. Pass `allocatorId` and `MemoryOwner` to the allocator, and defer final package-private visibility for internal FFM classes until Task 4 has migrated downstream imports.

```java
private NativeHandle publicHandle(long localRaw) {
    return localRaw == 0L ? NativeHandle.NULL : new NativeHandle(allocatorId, localRaw);
}

private long requireOwned(NativeHandle handle) {
    Objects.requireNonNull(handle, "handle");
    if (handle.allocatorId() != allocatorId) {
        throw new NativeHandleOwnershipException(allocatorId, handle.allocatorId());
    }
    long localRaw = handle.localRaw();
    YierdisLocalHandleCodec.requireValid(localRaw);
    return localRaw;
}
```

The required ordering is:

```java
@Override
public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
    owner.checkCurrentThread();
    long localRaw = requireOwned(handle);
    return resolveLocal(localRaw, Objects.requireNonNull(mode, "mode"));
}

@Override
public void free(NativeHandle handle) {
    owner.checkCurrentThread();
    long localRaw = requireOwned(handle);
    freeLocal(localRaw);
}
```

Apply the identical owner-check then `requireOwned` ordering to `reallocate`, `resolvePinned`, `pin`, `unpin`, and `defragOne`. Allocation wraps the object table's private local result with `publicHandle(...)`. Every `NativeObjectView.handle()` returns the scoped public handle. Delete all raw overloads rather than forwarding them.

`bindToCurrentThread()` is exactly `owner.bindToCurrentThread()`. Normal operations call `owner.checkCurrentThread()` and never bind. `close()` calls `owner.checkCurrentThreadForShutdown()` before freeing allocator state.

Apply the same no-implicit-bind rule to every derived resource returned by the
allocator. Every public `NativeObjectView` operation, including `handle`, size
and capacity access, byte/typed reads and writes, copies, comparisons, and
`close`, calls `owner.checkCurrentThread()` before touching view state. Every
`NativeAllocationScope` operation (`growth`, `promote`, `abort`, and `close`)
and every `NativeEpochScope` operation (`kind`, `epoch`, and `close`) does the
same, including repeated terminal calls. Replace the old
`checkOrBindCurrentThread()` paths; no derived-resource operation may call
`bindToCurrentThread()`.

`resolvePinned` first checks the owner and complete handle identity, then
rejects any mode other than `NativeAccessMode.READ_ONLY`. Its returned view
borrows the caller's existing pin and never unpins it on close.

- [ ] **Step 6: Build the public facade and backend-neutral region implementation**

`YierdisFfmStableMemoryBackend` is public and final. Its factory-compatible constructor allocates one ID and composes one internal runtime plus allocator:

```java
package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.*;

public final class YierdisFfmStableMemoryBackend implements StableMemoryBackend {
    private final long allocatorId;
    private final MemoryOwner owner;
    private final YierdisFfmMemoryRuntime runtime;
    private final YierdisStableNativeAllocator allocator;
    private final AtomicLong externalRegionBytes = new AtomicLong();

    public YierdisFfmStableMemoryBackend(String name, int maxSlots, MemoryOwner owner) {
        this.allocatorId = StableMemoryBackendIds.nextId();
        this.owner = Objects.requireNonNull(owner, "owner");
        this.runtime = new YierdisFfmMemoryRuntime(Objects.requireNonNull(name, "name"));
        this.allocator = new YierdisStableNativeAllocator(runtime, maxSlots, allocatorId, owner);
    }

    @Override
    public long allocatorId() {
        return allocatorId;
    }

    @Override public void bindToCurrentThread() { allocator.bindToCurrentThread(); }
    @Override public NativeHandle allocate(NativeObjectKind kind, int size) {
        return allocator.allocate(kind, size);
    }
    @Override public NativeHandle reallocate(
            NativeHandle handle,
            int newSize,
            NativeReallocPolicy policy
    ) {
        return allocator.reallocate(handle, newSize, policy);
    }
    @Override public void free(NativeHandle handle) { allocator.free(handle); }
    @Override public void pin(NativeHandle handle) { allocator.pin(handle); }
    @Override public void unpin(NativeHandle handle) { allocator.unpin(handle); }
    @Override public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        return allocator.beginEpoch(kind);
    }
    @Override public NativeAllocationScope beginAllocationScope() {
        return allocator.beginAllocationScope();
    }
    @Override public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        return allocator.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
    }
    @Override public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        return allocator.resolve(handle, mode);
    }
    @Override public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return allocator.resolvePinned(handle, mode);
    }

    @Override
    public StableMemoryRegion allocateRegion(String regionOwner, int bytes) {
        owner.checkCurrentThread();
        Objects.requireNonNull(regionOwner, "regionOwner");
        if (bytes <= 0) throw new IllegalArgumentException("bytes must be > 0");
        YierdisFfmRegion region = runtime.allocateRegion(regionOwner, bytes);
        externalRegionBytes.addAndGet(bytes);
        return new TrackingRegion(region, bytes);
    }

    @Override public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        return allocator.defragOne(handle, maxMoveBytes);
    }
    @Override public NativeDefragReport defragCycle(NativeDefragOptions options) {
        return allocator.defragCycle(options);
    }
    @Override public long logicalUsedBytes() { return allocator.logicalUsedBytes(); }
    @Override public NativeAllocatorStats stats() { return allocator.stats(); }
    @Override public NativeAllocatorMetadataStats metadataStats() {
        return allocator.metadataStats();
    }

    private MemoryUsageSnapshot externalRegionUsage() {
        long bytes = externalRegionBytes.get();
        return new MemoryUsageSnapshot(0L, 0L, bytes, bytes, 0L);
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return allocator.memoryUsage().plus(externalRegionUsage());
    }

    @Override public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        return allocator.trimEmptyPages(budget);
    }
    @Override public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return allocator.estimateAdditionalGrowth(requestedBytes);
    }
    @Override public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        return allocator.estimateConservativeAdditionalGrowth(requestedBytes);
    }

    @Override
    public long liveRegionCount() {
        owner.checkCurrentThread();
        return runtime.liveRegionCount();
    }

    @Override
    public void close() {
        owner.checkCurrentThreadForShutdown();
        RuntimeException failure = null;
        try {
            allocator.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            runtime.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure != null) throw failure;
    }

    private final class TrackingRegion implements StableMemoryRegion {
        private final YierdisFfmRegion delegate;
        private final int bytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TrackingRegion(YierdisFfmRegion delegate, int bytes) {
            this.delegate = delegate;
            this.bytes = bytes;
        }

        @Override public int size() { checkOpen(); return delegate.size(); }
        @Override public byte getByte(int offset) { checkOpen(); return delegate.getByte(offset); }
        @Override public void setByte(int offset, byte value) {
            checkOpen();
            delegate.setByte(offset, value);
        }
        @Override public int getIntLittleEndian(int offset) {
            checkOpen();
            return delegate.getIntLittleEndian(offset);
        }
        @Override public void setIntLittleEndian(int offset, int value) {
            checkOpen();
            delegate.setIntLittleEndian(offset, value);
        }
        @Override public long getLongLittleEndian(int offset) {
            checkOpen();
            return delegate.getLongLittleEndian(offset);
        }
        @Override public void setLongLittleEndian(int offset, long value) {
            checkOpen();
            delegate.setLongLittleEndian(offset, value);
        }
        @Override public void getBytes(int offset, byte[] dst, int dstOffset, int length) {
            checkOpen();
            delegate.getBytes(offset, dst, dstOffset, length);
        }
        @Override public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            checkOpen();
            delegate.setBytes(offset, src, srcOffset, length);
        }
        @Override public void copyTo(
                int sourceOffset,
                StableMemoryRegion target,
                int targetOffset,
                int length
        ) {
            checkOpen();
            Objects.requireNonNull(target, "target");
            if (target instanceof TrackingRegion trackingTarget) {
                trackingTarget.checkOpen();
                delegate.copyTo(
                        sourceOffset,
                        trackingTarget.delegate,
                        targetOffset,
                        length
                );
                return;
            }
            delegate.copyTo(sourceOffset, target, targetOffset, length);
        }

        @Override
        public void close() {
            checkOwner();
            if (!closed.compareAndSet(false, true)) return;
            delegate.close();
            long remaining = externalRegionBytes.addAndGet(-bytes);
            if (remaining < 0L) {
                throw new IllegalStateException("external region accounting underflow");
            }
        }

        private void checkOwner() {
            owner.checkCurrentThread();
        }

        private void checkOpen() {
            checkOwner();
            if (closed.get()) {
                throw new IllegalStateException("stable memory region is closed");
            }
        }
    }
}
```

Make `YierdisFfmRegion` implement `StableMemoryRegion` and make its interface methods public. Implement portable cross-region copying without exposing a segment:

```java
@Override
public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) {
    Objects.requireNonNull(target, "target");
    ensureOpen();
    checkRange(sourceOffset, length);
    if (target instanceof YierdisFfmRegion ffmTarget) {
        ffmTarget.ensureOpen();
        ffmTarget.checkRange(targetOffset, length);
        MemorySegment.copy(segment, sourceOffset, ffmTarget.segment, targetOffset, length);
        return;
    }
    byte[] snapshot = new byte[length];
    getBytes(sourceOffset, snapshot, 0, length);
    target.setBytes(targetOffset, snapshot, 0, length);
}
```

Every `YierdisFfmRegion` interface method, including `size()`, calls
`ensureOpen()` before returning or mutating data; closed regions reject all
subsequent access.

`TrackingRegion.copyTo` unwraps another tracking target before this method so
FFM-to-FFM copies, including same-region overlap, always use
`MemorySegment.copy`. The full snapshot fallback preserves overlap semantics
even for an opaque `StableMemoryRegion` implementation. This also prevents
allocator page regions, already represented by `allocator.memoryUsage()`, from
being counted again through `runtime.usedBytes()`. Close order is allocator,
any facade state, then runtime; combine failures with suppression. A live
externally allocated region makes runtime close fail visibly rather than
silently leaking.

- [ ] **Step 7: Port synchronized and existing FFM tests to the complete interface**

Rename the synchronized wrapper and test. The wrapper retains `delegate.allocatorId()`, delegates all stable backend methods under its existing lock, uses `reallocate`, and removes every raw method. Allocation-scope ownership remains explicit. Update existing FFM tests to construct `YierdisFfmStableMemoryBackend` with a test `MemoryOwner`, call `bindToCurrentThread()` before access, and use `NativeHandle` end to end.

Do not make internal classes package-private until Task 4 has migrated all downstream imports. In this task, remove their public construction paths from new code; Task 4 performs the final visibility reduction after `db-memory` and `server-main` use only the facade/factory.

- [ ] **Step 8: Run focused FFM tests and verify GREEN**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-ffm -am \
  -Dtest=YierdisStableNativeAllocatorOwnershipTest,YierdisLocalHandleCodecTest,YierdisStableNativeAllocatorTest,SynchronizedStableMemoryBackendTest,YierdisFfmMemoryRuntimeTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

Expected: PASS. The clean phase removes deleted guard/wrapper classes and the
renamed synchronized test bytecode. Cross-backend operations fail with
`NativeHandleOwnershipException`, malformed foreign local values are never
decoded, public access and every derived-resource operation check without
auto-binding, pinned views remain read-only and retain the caller's pin, local
codec coverage is preserved behind the package boundary, explicit
little-endian bytes survive copying, overlapping region copies are safe, and
external regions change accounting exactly once.

- [ ] **Step 9: Run the complete FFM module suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-memory/yierdis-memory-ffm -am test
```

Expected: PASS for memory API, object table, page allocator, scopes, epochs, defrag, synchronized backend, facade, ownership, regions, and close-order tests. At this deliberate checkpoint, the exact direct Task 4 restoration set remains `yierdis-db-memory`, `yierdis-server-main`, `yierdis-architecture-tests`, `yierdis-integration-tests`, and `yierdis-benchmark`: the first four still contain old FFM implementation/raw-handle source or depend directly on the unmigrated DB implementation, while the benchmark still uses retired Task 1 factory/capability forms and depends on `yierdis-db-memory`. `yierdis-cli` remains transitively broken only through its test-scoped `yierdis-server-main` dependency. Task 4 migrates the five direct modules, makes internal FFM classes package-private, and verifies CLI restoration without a CLI source change.

- [ ] **Step 10: Commit the FFM stable backend**

```bash
git add \
  yierdis-memory/yierdis-memory-ffm
git commit -m "refactor: scope FFM memory handles by backend"
```

---

### Task 4: Inject Stable Memory Into DB-Memory And Migrate Every External Handle Layout

**Files:**
- Create: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbBackendConfig.java`
- Modify: `yierdis-db/yierdis-db-memory/pom.xml`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/DbThreadGuard.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeState.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbOwnedResources.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- Modify: every existing source under `yierdis-db/yierdis-db-memory/src/main/java`
  that consumes `NativeAllocator`, an FFM implementation type, a raw-handle
  operation, or an accessor whose return type changes to `StableMemoryBackend`.
  This main-source migration closure includes `DbComponentMemoryUsage`,
  `YierdisDbNativeHandleGraph`, `YierdisDbDataMaintenance`,
  `YierdisDbIntrospection`, `YierdisKeyspaceOps`, every `Yierdis*Ops`
  implementation, every collection/string root, the mutation-ledger sources,
  `HashValue`, `SetValue`, and the native scan/popped-value sources; Step 14's
  source scan must return no retired consumer.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/NativeCollectionRootTable.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandleAccess.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/AllocatorKeyHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeByteStore.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeByteMap.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeListpack.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeBytesSlice.java`
- Rename: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeRawHandleSet.java` to `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeHandleSet.java`
- Rename: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java` to `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/YierdisNativeExpireIndex.java`
- Delete: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmIntSet.java`
- Create: `yierdis-memory/yierdis-memory-testkit/src/main/java/yier/bubu/redis/memory/testkit/HeapStableMemoryBackend.java`
- Create: `yierdis-memory/yierdis-memory-testkit/src/test/java/yier/bubu/redis/memory/testkit/HeapStableMemoryBackendTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/StableMemoryBackendCompositionTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/DbOwnerBindingTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/expire/ExpireIndexContractTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandleContractTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandleContractTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java`
- Delete: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/RawPathRecordingAllocator.java`
- Modify: all existing `yierdis-db/yierdis-db-memory/src/test/java` tests that construct the old allocator, FFM runtime/region, or raw handles.
- Modify: `yierdis-server/yierdis-server-main/pom.xml`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisInstances.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCloseTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/pom.xml`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/MaxmemoryPhysicalProgressTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/EmptyDatabaseFootprintTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/HllCommandTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MaxmemoryDoubleReplyRegressionTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MaxmemoryEvictionTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OffHeapKeysCommandSmokeTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OffHeapKeysZeroCopyReadPathTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TtlMaxmemoryTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/storage/memory/OffHeapLeakRegressionTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisSnapshotTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestDbs.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestYierdisInstances.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-benchmark/pom.xml`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/storage/StorageBenchmarkRunner.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/storage/StorageBenchmarkRunnerTest.java`
- Finalize visibility: every FFM implementation class listed as internal in Task 3 becomes package-private; only `YierdisFfmStableMemoryBackend` remains public for composition.

**Interfaces:**
- Consumes: `StableMemoryBackendFactory`; `db-memory` main code imports only `yier.bubu.redis.memory.api`.
- Produces: `YierdisDb`, which explicitly implements `CommitPublishingDbEngine`, `GlobalMaxmemoryDbEngine`, and `DefragmentableDbEngine` in addition to baseline runtime.
- Invariant: one `DbThreadGuard` is constructed before the backend, passed to the backend as `MemoryOwner`, and reused by all DB owner checks.
- Invariant: a handle stored outside `memory-ffm` retains both longs. Only FFM-private native structures may rely on implicit allocator identity and store local raw values alone.

- [ ] **Step 1: Write the failing non-FFM composition test**

Create `StableMemoryBackendCompositionTest.java`:

```java
package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.*;

public class StableMemoryBackendCompositionTest {
    @Test
    public void factoryConstructsAndMutatesDatabaseWithHeapBackend() {
        AtomicReference<HeapStableMemoryBackend> created = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory((name, maxSlots, owner) -> {
            HeapStableMemoryBackend backend = new HeapStableMemoryBackend(name, maxSlots, owner);
            created.set(backend);
            return backend;
        }, new YierdisDbBackendConfig(4096));
        RuntimeDbEngine engine = factory.create(new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));

        try {
            engine.bindToCurrentThread();
            byte[] key = "key".getBytes(StandardCharsets.US_ASCII);
            byte[] value = "value".getBytes(StandardCharsets.US_ASCII);
            WriteResult<Boolean> result = engine.writes().strings()
                    .setString(key, value, SetMode.NORMAL, null);
            Assert.assertTrue(result.value());
            Assert.assertArrayEquals(value, engine.reads().strings().getStringBytes(key));
        } finally {
            engine.shutdown();
        }
        Assert.assertTrue(created.get().isClosedForTesting());
    }
}
```

Create `HeapStableMemoryBackendTest.java` so the backend-neutral fake is held
to the same derived-resource, pinned-view, byte-order, and overlap contracts:

```java
package yier.bubu.redis.memory.testkit;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.*;

public class HeapStableMemoryBackendTest {
    @Test
    public void everyDerivedResourceOperationChecksTheBoundOwner() throws Exception {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("derived-owner", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 16);
            NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE);

            assertWrongThreadRejected(view::handle);
            assertWrongThreadRejected(view::size);
            assertWrongThreadRejected(view::capacity);
            assertWrongThreadRejected(() -> view.getByte(0));
            assertWrongThreadRejected(() -> view.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> view.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.copyBytes(0, 1, 1));
            assertWrongThreadRejected(() -> view.contentEquals(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> view.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> view.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> view.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> view.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(view::close);
            Assert.assertEquals(16, view.size());
            Assert.assertEquals(0, view.getByte(0));
            view.close();
            backend.free(handle);

            NativeAllocationScope allocationScope = backend.beginAllocationScope();
            assertWrongThreadRejected(allocationScope::growth);
            assertWrongThreadRejected(allocationScope::promote);
            assertWrongThreadRejected(allocationScope::abort);
            assertWrongThreadRejected(allocationScope::close);
            NativeHandle scoped = backend.allocate(NativeObjectKind.GENERIC, 8);
            allocationScope.abort();
            Assert.assertThrows(
                    StaleNativeHandleException.class,
                    () -> backend.resolve(scoped, NativeAccessMode.READ_ONLY)
            );

            NativeEpochScope epochScope = backend.beginEpoch(NativeEpochKind.COMMAND);
            assertWrongThreadRejected(epochScope::kind);
            assertWrongThreadRejected(epochScope::epoch);
            assertWrongThreadRejected(epochScope::close);
            epochScope.close();

            StableMemoryRegion region = backend.allocateRegion("derived-region", 16);
            assertWrongThreadRejected(region::size);
            assertWrongThreadRejected(() -> region.getByte(0));
            assertWrongThreadRejected(() -> region.setByte(0, (byte) 1));
            assertWrongThreadRejected(() -> region.getIntLittleEndian(0));
            assertWrongThreadRejected(() -> region.setIntLittleEndian(0, 1));
            assertWrongThreadRejected(() -> region.getLongLittleEndian(0));
            assertWrongThreadRejected(() -> region.setLongLittleEndian(0, 1L));
            assertWrongThreadRejected(() -> region.getBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.setBytes(0, new byte[1], 0, 1));
            assertWrongThreadRejected(() -> region.copyTo(0, region, 1, 1));
            assertWrongThreadRejected(region::close);
            Assert.assertEquals(16, region.size());
            Assert.assertEquals(0, region.getByte(0));
            region.close();
        }
    }

    @Test
    public void pinnedViewsAreReadOnlyAndDoNotOwnTheCallersPin() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("pinned-view", owner)) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 8);
            backend.pin(handle);

            Assert.assertThrows(
                    NativeMemoryException.class,
                    () -> backend.resolvePinned(handle, NativeAccessMode.READ_WRITE)
            );
            try (NativeObjectView borrowed =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, borrowed.handle());
                Assert.assertThrows(
                        NativeMemoryException.class,
                        () -> borrowed.setByte(0, (byte) 1)
                );
            }
            try (NativeObjectView ignored =
                         backend.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(handle, ignored.handle());
            }

            backend.unpin(handle);
            backend.free(handle);
        }
    }

    @Test
    public void regionsUseLittleEndianBytesAndOverlapSafeCopy() {
        TestOwner owner = new TestOwner();
        try (StableMemoryBackend backend = backend("regions", owner)) {
            backend.bindToCurrentThread();
            try (StableMemoryRegion layout = backend.allocateRegion("layout", 16)) {
                layout.setIntLittleEndian(0, 0x78563412);
                layout.setLongLittleEndian(4, 0x0102030405060708L);
                byte[] actual = new byte[12];
                layout.getBytes(0, actual, 0, actual.length);
                byte[] littleEndian = new byte[]{
                        0x12, 0x34, 0x56, 0x78,
                        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
                };
                Assert.assertArrayEquals(littleEndian, actual);
                layout.setBytes(0, littleEndian, 0, littleEndian.length);
                Assert.assertEquals(0x78563412, layout.getIntLittleEndian(0));
                Assert.assertEquals(0x0102030405060708L, layout.getLongLittleEndian(4));
            }
            try (StableMemoryRegion region = backend.allocateRegion("overlap", 16_384)) {
                byte[] original = new byte[16_384];
                for (int index = 0; index < original.length; index++) {
                    original[index] = (byte) (index % 251);
                }
                region.setBytes(0, original, 0, original.length);

                region.copyTo(0, region, 4_096, 12_000);

                byte[] expected = original.clone();
                System.arraycopy(original, 0, expected, 4_096, 12_000);
                byte[] actual = new byte[expected.length];
                region.getBytes(0, actual, 0, actual.length);
                Assert.assertArrayEquals(expected, actual);
            }
            StableMemoryRegion closed = backend.allocateRegion("closed", 16);
            closed.close();
            Assert.assertThrows(IllegalStateException.class, closed::size);
            Assert.assertThrows(IllegalStateException.class, () -> closed.getByte(0));
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> closed.setIntLittleEndian(0, 1)
            );
        }
    }

    private static StableMemoryBackend backend(String name, TestOwner owner) {
        return new HeapStableMemoryBackend(name, 128, owner);
    }

    private static void assertWrongThreadRejected(Runnable operation) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                operation.run();
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }, "wrong-memory-owner");
        worker.setDaemon(true);
        worker.start();
        worker.join(5_000L);

        if (worker.isAlive()) worker.interrupt();
        Assert.assertFalse("wrong-owner operation did not finish", worker.isAlive());
        Assert.assertTrue(
                "expected owner rejection, got " + failure.get(),
                failure.get() instanceof IllegalStateException
        );
    }

    private static final class TestOwner implements MemoryOwner {
        private final AtomicReference<Thread> owner = new AtomicReference<>();

        @Override
        public void bindToCurrentThread() {
            Thread current = Thread.currentThread();
            Thread existing = owner.get();
            if (existing == current) return;
            if (existing != null || !owner.compareAndSet(null, current)) {
                throw new IllegalStateException("memory owner already belongs to another thread");
            }
        }

        @Override
        public void checkCurrentThread() {
            if (owner.get() != Thread.currentThread()) {
                throw new IllegalStateException("memory access is outside the owner thread");
            }
        }

        @Override
        public void checkCurrentThreadForShutdown() {
            Thread existing = owner.get();
            if (existing != null && existing != Thread.currentThread()) {
                throw new IllegalStateException("memory shutdown is outside the owner thread");
            }
        }
    }
}
```

- [ ] **Step 2: Write the failing atomic owner-binding test**

Create `DbOwnerBindingTest.java`. It races two distinct threads, keeps the winner alive long enough to exercise DB access, and shuts down on that same thread:

```java
package yier.bubu.redis.storage.memory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.*;

public class DbOwnerBindingTest {
    @Test
    public void concurrentFirstBindHasOneDbAndBackendWinner() throws Exception {
        AtomicInteger successfulBinds = new AtomicInteger();
        AtomicInteger rejectedBinds = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch bindAttemptsFinished = new CountDownLatch(2);
        CountDownLatch winnerMayClose = new CountDownLatch(1);
        RuntimeDbEngine engine = new YierdisDbEngineFactory(
                HeapStableMemoryBackend::new,
                new YierdisDbBackendConfig(4096)
        ).create(config());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Void> contender = () -> {
                ready.countDown();
                start.await();
                try {
                    engine.bindToCurrentThread();
                    successfulBinds.incrementAndGet();
                } catch (IllegalStateException expected) {
                    rejectedBinds.incrementAndGet();
                    return null;
                } finally {
                    bindAttemptsFinished.countDown();
                }
                Assert.assertFalse(
                        engine.reads().keyspace().existsKey(new ByteArrayView("absent"))
                );
                Assert.assertTrue(winnerMayClose.await(5L, TimeUnit.SECONDS));
                engine.shutdown();
                return null;
            };

            Future<Void> first = pool.submit(contender);
            Future<Void> second = pool.submit(contender);
            Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            Assert.assertTrue(bindAttemptsFinished.await(5L, TimeUnit.SECONDS));
            winnerMayClose.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            winnerMayClose.countDown();
            pool.shutdownNow();
        }

        Assert.assertEquals(1, successfulBinds.get());
        Assert.assertEquals(1, rejectedBinds.get());
    }

    private static DbEngineConfig config() {
        return new DbEngineConfig(
                0, 0L, MaxmemoryPolicy.NOEVICTION, 5, 5L, 5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        );
    }

    private record ByteArrayView(byte[] bytes) implements yier.bubu.redis.bytes.BytesView {
        private ByteArrayView(String ascii) {
            this(ascii.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }

        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }
    }
}
```

- [ ] **Step 3: Add failing expire-index and architecture guards**

Extend `ExpireIndexContractTest` so the functional contract runs with `HeapStableMemoryBackend`, and add this boundary assertion:

```java
@Test
public void expireIndexUsesOnlyStableMemoryApi() throws Exception {
    Class<?> type = Class.forName(
            "yier.bubu.redis.storage.memory.internal.expire.YierdisNativeExpireIndex"
    );
    Assert.assertThrows(
            ClassNotFoundException.class,
            () -> Class.forName(
                    "yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex"
            )
    );
    for (Field field : type.getDeclaredFields()) {
        String fieldType = field.getType().getName();
        Assert.assertFalse(fieldType, fieldType.contains("memory.foreign"));
        Assert.assertFalse(fieldType, fieldType.contains("java.lang.foreign"));
    }
}
```

Add two source/POM guards to `YierdisDbArchitectureGuardTest`. Parse the POM as XML instead of matching dependency text:

```java
@Test
public void dbMemoryMainHasNoFfmImplementationImport() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("unable to resolve repository root", repoRoot);
    List<String> offenders = new ArrayList<>();
    int scanned = scanForForbiddenText(
            repoRoot,
            storageMemoryMain(repoRoot),
            offenders,
            "yier.bubu.redis.memory.foreign",
            "java.lang.foreign"
    );
    Assert.assertTrue("expected to scan DB-memory main sources", scanned > 0);
    if (!offenders.isEmpty()) {
        Assert.fail("DB-memory main must depend only on memory-api:\n" + String.join("\n", offenders));
    }
}

@Test
public void dbMemoryPomDependsOnMemoryApiAndNotFfm() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("unable to resolve repository root", repoRoot);
    Path pomPath = repoRoot.resolve("yierdis-db/yierdis-db-memory/pom.xml");
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(false);
    Document pom = factory.newDocumentBuilder().parse(pomPath.toFile());

    Assert.assertEquals("compile", dependencyScope(pom, "yierdis-memory-api"));
    Assert.assertNull(dependencyScope(pom, "yierdis-memory-ffm"));
}

private static String dependencyScope(Document pom, String artifactId) {
    NodeList dependencies = pom.getElementsByTagName("dependency");
    for (int index = 0; index < dependencies.getLength(); index++) {
        Element dependency = (Element) dependencies.item(index);
        if (!artifactId.equals(childText(dependency, "artifactId"))) {
            continue;
        }
        String scope = childText(dependency, "scope");
        return scope == null || scope.isBlank() ? "compile" : scope;
    }
    return null;
}

private static String childText(Element parent, String tagName) {
    NodeList children = parent.getElementsByTagName(tagName);
    return children.getLength() == 0 ? null : children.item(0).getTextContent().trim();
}
```

Add imports for `javax.xml.parsers.DocumentBuilderFactory`, `org.w3c.dom.Document`, `org.w3c.dom.Element`, and `org.w3c.dom.NodeList`.

Add this source-level visibility guard to `ArchitectureBoundaryTest`; the
line-anchored declaration pattern intentionally checks top-level types and
rejects any second public FFM implementation entry point:

```java
@Test
public void ffmModuleExposesOnlyStableBackendFacade() throws IOException {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("unable to resolve repository root", repoRoot);
    Path ffmSources = repoRoot.resolve(
            "yierdis-memory/yierdis-memory-ffm/src/main/java/"
                    + "yier/bubu/redis/memory/foreign"
    );
    Pattern declaration = Pattern.compile(
            "(?m)^public\\s+(?:(?:abstract|final|sealed|non-sealed|strictfp)\\s+)*"
                    + "(?:class|interface|@interface|enum|record)\\s+"
                    + "([A-Za-z][A-Za-z0-9_]*)\\b"
    );
    List<String> publicTypes = new ArrayList<>();
    try (Stream<Path> files = Files.walk(ffmSources)) {
        for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
            var matcher = declaration.matcher(Files.readString(file, StandardCharsets.UTF_8));
            while (matcher.find()) {
                publicTypes.add(matcher.group(1));
            }
        }
    }
    publicTypes.sort(String::compareTo);

    Assert.assertEquals(List.of("YierdisFfmStableMemoryBackend"), publicTypes);
}
```

- [ ] **Step 4: Run the focused tests and verify RED**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=HeapStableMemoryBackendTest,StableMemoryBackendCompositionTest,DbOwnerBindingTest,ExpireIndexContractTest,YierdisDbArchitectureGuardTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: compilation fails because the factory still constructs FFM
runtime/allocator directly, the heap backend and backend-neutral expire index
do not exist, and `db-memory` main has an FFM compile dependency. Once those
types compile but before visibility is finalized,
`ffmModuleExposesOnlyStableBackendFacade` remains red because the internal FFM
types are still public.

- [ ] **Step 5: Implement one atomic DB/backend owner and named backend config**

Create the DB-implementation configuration record. `nativeSlotCapacity` stays out of per-DB runtime policy and out of positional factory arguments:

```java
package yier.bubu.redis.storage.memory;

public record YierdisDbBackendConfig(int nativeSlotCapacity) {
    public YierdisDbBackendConfig {
        if (nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("nativeSlotCapacity must be non-negative");
        }
    }
}
```

Make `DbThreadGuard` implement `MemoryOwner` with a single atomic owner reference:

```java
public final class DbThreadGuard implements MemoryOwner {
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    @Override
    public void bindToCurrentThread() {
        Thread current = Thread.currentThread();
        Thread existing = owner.get();
        if (existing == current) return;
        if (existing != null || !owner.compareAndSet(null, current)) {
            throw new IllegalStateException("YierdisDb is already bound to another thread");
        }
    }

    @Override
    public void checkCurrentThread() {
        Thread existing = owner.get();
        if (existing == null) {
            throw new IllegalStateException("YierdisDb accessed before bindToCurrentThread()");
        }
        if (existing != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb accessed from a non-owner thread");
        }
    }

    @Override
    public void checkCurrentThreadForShutdown() {
        Thread existing = owner.get();
        if (existing != null && existing != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb shutdown from a non-owner thread");
        }
    }
}
```

`YierdisDbEngineFactory` has one exact public constructor:

```java
public YierdisDbEngineFactory(
        StableMemoryBackendFactory backendFactory,
        YierdisDbBackendConfig backendConfig
) {
    this.backendFactory = Objects.requireNonNull(backendFactory, "backendFactory");
    this.backendConfig = Objects.requireNonNull(backendConfig, "backendConfig");
    this.hashSeed = HashSeed.random();
}

@Override
public RuntimeDbEngine create(DbEngineConfig config) {
    Objects.requireNonNull(config, "config");
    DbThreadGuard owner = new DbThreadGuard();
    StableMemoryBackend backend = backendFactory.create(
            "db-" + config.dbIndex(),
            backendConfig.nativeSlotCapacity(),
            owner
    );
    if (backend == null) {
        throw new IllegalStateException("StableMemoryBackendFactory returned null");
    }
    try {
        return YierdisDb.create(config, backend, owner, hashSeed);
    } catch (Throwable failure) {
        try {
            backend.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        throw failure;
    }
}
```

Remove every no-argument, slot-count-only, and FFM-runtime constructor from this factory. `YierdisDbRuntimeState` receives the same guard and backend during composition. Its binding method performs exactly one call:

```java
void bindToCurrentThread() {
    stableMemoryBackend.bindToCurrentThread();
}
```

All DB checks continue through the shared `DbThreadGuard`. There is no second guard bind and no backend-local first-use binding.

- [ ] **Step 6: Replace runtime/allocator ownership with one backend resource**

Change storage composition fields from `YierdisFfmMemoryRuntime` plus `NativeAllocator` to one `StableMemoryBackend`. Allocate expiration regions through `backend.allocateRegion(...)`; report `backend.liveRegionCount()` and `backend.memoryUsage()`.

`YierdisDbOwnedResources.releaseAll(...)` closes in this exact order, aggregating failures and guarding every close once:

1. Clear expiry, key, entry, and value graphs while handles remain resolvable.
2. Close `YierdisNativeExpireIndex` regions.
3. Close entry/key/value tables and roots.
4. Close the `StableMemoryBackend` last.

Remove `ownsMemoryRuntime`, `ownsNativeAllocator`, separate runtime close, and concrete FFM `instanceof` branches. `YierdisDb` implements all three optional runtime capability interfaces and converts `DbDefragConfig` to memory options only inside DB composition:

```java
NativeDefragOptions options = config.defrag().enabled()
        ? new NativeDefragOptions(
                config.defrag().maxMoveBytes(),
                config.defrag().maxObjects(),
                TimeUnit.MILLISECONDS.toNanos(config.defrag().timeLimitMillis())
        )
        : null;
```

- [ ] **Step 7: Store full handles in every DB-owned layout**

Replace raw wrapper records with opaque full handles:

```java
public record EntryHandle(NativeHandle nativeHandle) {
    public EntryHandle {
        Objects.requireNonNull(nativeHandle, "nativeHandle");
        if (nativeHandle.isNull()) {
            throw new IllegalArgumentException("entry handle must not be null");
        }
    }
}
```

```java
public record ValueHandle(NativeHandle nativeHandle) {
    public static final ValueHandle NULL = new ValueHandle(NativeHandle.NULL);

    public ValueHandle {
        Objects.requireNonNull(nativeHandle, "nativeHandle");
    }

    public boolean isNull() {
        return nativeHandle.isNull();
    }
}
```

The public handle no longer exposes kind/domain decoding, so these DB wrappers validate only nullability. Allocation kind is enforced by the backend's local object table.

Change `EntryRecord.keyHandle` from `long` to `NativeHandle`. Widen `EntryTable` from 56 to 72 bytes with this fixed layout:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 8 | key allocator ID |
| 8 | 8 | key local raw |
| 16 | 8 | value allocator ID |
| 24 | 8 | value local raw |
| 32 | 4 | key hash |
| 36 | 4 | value type ordinal |
| 40 | 4 | value encoding ordinal |
| 44 | 4 | flags |
| 48 | 8 | expiry millis |
| 56 | 8 | version |
| 64 | 8 | LRU/LFU value |

Write/read handles without reconstructing an identity from one local value:

```java
private static void writeHandle(NativeObjectView view, int offset, NativeHandle handle) {
    view.setLongLittleEndian(offset, handle.allocatorId());
    view.setLongLittleEndian(offset + Long.BYTES, handle.localRaw());
}

private static NativeHandle readHandle(NativeObjectView view, int offset) {
    return new NativeHandle(
            view.getLongLittleEndian(offset),
            view.getLongLittleEndian(offset + Long.BYTES)
    );
}
```

Apply the same two-long rule at every hotspot:

| Hotspot | Required representation |
| --- | --- |
| `NativeCollectionRootTable` | `Map<NativeHandle, AdapterSlot<T>>`; never derive a public slot index by decoding `localRaw` |
| `NativeKeyDirectory` | two adjacent longs for each stored key/entry handle and updated slot/capacity estimates |
| `AllocatorKeyHandle` | retain `StableMemoryBackend` plus full `NativeHandle`; identity fast path compares both longs |
| `NativeByteStore` / `NativeByteMap` | `NativeHandle` fields/values; native records allocate 16 bytes per embedded handle |
| `NativeListpack` / `ListValue` / `ZSetValue` | paired node/member references and recalculated offsets, growth estimates, copies, and defrag graph walks |
| `NativeHandleSet` | full `NativeHandle[]` or paired-long arrays; equality/hash includes allocator ID |
| popped and scan sources | retain/pin/unpin the exact full handle received from storage |
| expire-index key arrays | `NativeHandle[]`; never a `long[]` of local raw values |

Delete `fromRaw`, `.raw()`, and every raw allocator call from DB main and tests. `AllocatorKeyHandle.equals` takes the identity fast path only when the complete handles match:

```java
if (other instanceof AllocatorKeyHandle allocatorOther
        && handle.allocatorId() == allocatorOther.handle.allocatorId()
        && handle.localRaw() == allocatorOther.handle.localRaw()) {
    return true;
}
```

If identities differ, compare cached content hash, length, and bytes semantically. This remains required when local raw values happen to match.

- [ ] **Step 8: Make expiration storage backend-neutral**

Move the expire index to `internal.expire.YierdisNativeExpireIndex`. Its constructor is:

```java
public YierdisNativeExpireIndex(
        StableMemoryBackend backend,
        HashSeed hashSeed,
        HashTableMaintenanceRegistry maintenanceRegistry
) {
    this.backend = Objects.requireNonNull(backend, "backend");
    this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
    this.maintenanceRegistry = Objects.requireNonNull(maintenanceRegistry, "maintenanceRegistry");
}
```

Replace FFM region/span fields with `StableMemoryRegion` and explicit
little-endian typed offset access. Keep state, hash, and expiry data in regions
and full key handles in `NativeHandle[]`. Region cleanup remains reverse
allocation order and close-once. Delete the unused `YierdisFfmIntSet` source
and tests instead of porting it.

- [ ] **Step 9: Supply a complete heap backend test double**

Implement `HeapStableMemoryBackend` in `memory-testkit` with this complete backend-neutral test implementation:

```java
package yier.bubu.redis.memory.testkit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.*;

public final class HeapStableMemoryBackend implements StableMemoryBackend {
    private final long allocatorId = StableMemoryBackendIds.nextId();
    private final MemoryOwner owner;
    private final int maxSlots;
    private final AtomicLong nextLocalRaw = new AtomicLong(1L);
    private final AtomicLong nextEpoch = new AtomicLong(1L);
    private final Map<Long, byte[]> objects = new HashMap<>();
    private final Map<Long, NativeObjectKind> objectKinds = new HashMap<>();
    private final Set<Long> pinned = new HashSet<>();
    private final Set<HeapView> views = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<HeapRegion> regions = Collections.newSetFromMap(new IdentityHashMap<>());

    private HeapAllocationScope activeAllocationScope;
    private int activeEpochs;
    private boolean closed;

    public HeapStableMemoryBackend(String name, int maxSlots, MemoryOwner owner) {
        Objects.requireNonNull(name, "name");
        if (maxSlots < 0) throw new IllegalArgumentException("maxSlots must be non-negative");
        this.maxSlots = maxSlots;
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public long allocatorId() {
        return allocatorId;
    }

    @Override
    public void bindToCurrentThread() {
        owner.bindToCurrentThread();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        checkOpen();
        Objects.requireNonNull(kind, "kind");
        if (size < 0) throw new IllegalArgumentException("size must be non-negative");
        if (maxSlots != 0 && objects.size() >= maxSlots) {
            throw new NativeCapacityExceededException("heap test backend slot capacity exceeded");
        }
        long localRaw = nextLocalRaw.getAndIncrement();
        if (localRaw <= 0L) {
            throw new NativeCapacityExceededException("heap test backend handle space exhausted");
        }
        objects.put(localRaw, new byte[size]);
        objectKinds.put(localRaw, kind);
        NativeHandle handle = new NativeHandle(allocatorId, localRaw);
        if (activeAllocationScope != null) activeAllocationScope.track(handle);
        return handle;
    }

    @Override
    public NativeHandle reallocate(
            NativeHandle handle,
            int newSize,
            NativeReallocPolicy policy
    ) {
        checkOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize < 0) throw new IllegalArgumentException("newSize must be non-negative");
        long localRaw = requireLive(handle);
        if (pinned.contains(localRaw)) throw new NativeMemoryException("native object is pinned");
        if (hasLiveView(localRaw)) throw new NativeMemoryException("native object has a live view");
        byte[] previous = objects.get(localRaw);
        if (newSize == previous.length) return handle;
        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("heap test object cannot resize in place");
        }
        objects.put(localRaw, Arrays.copyOf(previous, newSize));
        return handle;
    }

    @Override
    public void free(NativeHandle handle) {
        checkOpen();
        long localRaw = requireLive(handle);
        if (pinned.contains(localRaw)) throw new NativeMemoryException("native object is pinned");
        if (hasLiveView(localRaw)) throw new NativeMemoryException("native object has a live view");
        objects.remove(localRaw);
        objectKinds.remove(localRaw);
        if (activeAllocationScope != null) activeAllocationScope.untrack(handle);
    }

    @Override
    public void pin(NativeHandle handle) {
        checkOpen();
        long localRaw = requireLive(handle);
        if (!pinned.add(localRaw)) throw new IllegalStateException("native object is already pinned");
    }

    @Override
    public void unpin(NativeHandle handle) {
        checkOpen();
        long localRaw = requireLive(handle);
        if (!pinned.remove(localRaw)) throw new IllegalStateException("native object is not pinned");
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        checkOpen();
        Objects.requireNonNull(kind, "kind");
        long epoch = nextEpoch.getAndIncrement();
        activeEpochs++;
        return new HeapEpochScope(kind, epoch);
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        checkOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native allocation scope is already active");
        }
        activeAllocationScope = new HeapAllocationScope(objectBytes());
        return activeAllocationScope;
    }

    @Override
    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        checkOpen();
        if (expectedAllocationCount < 0) {
            throw new IllegalArgumentException("expectedAllocationCount must be non-negative");
        }
        return 32L + 16L * expectedAllocationCount;
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        checkOpen();
        Objects.requireNonNull(mode, "mode");
        long localRaw = requireLive(handle);
        HeapView view = new HeapView(handle, localRaw, objects.get(localRaw), mode);
        views.add(view);
        return view;
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        checkOpen();
        Objects.requireNonNull(mode, "mode");
        if (mode != NativeAccessMode.READ_ONLY) {
            throw new NativeMemoryException("retained native views must be read-only");
        }
        long localRaw = requireLive(handle);
        if (!pinned.contains(localRaw)) throw new NativeMemoryException("native object is not pinned");
        HeapView view = new HeapView(handle, localRaw, objects.get(localRaw), mode);
        views.add(view);
        return view;
    }

    @Override
    public StableMemoryRegion allocateRegion(String regionOwner, int bytes) {
        checkOpen();
        Objects.requireNonNull(regionOwner, "regionOwner");
        if (bytes <= 0) throw new IllegalArgumentException("bytes must be > 0");
        HeapRegion region = new HeapRegion(bytes);
        regions.add(region);
        return region;
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        checkOpen();
        if (maxMoveBytes < 0L) throw new IllegalArgumentException("maxMoveBytes must be non-negative");
        long localRaw = requireLive(handle);
        if (pinned.contains(localRaw)) return NativeDefragResult.skippedPinnedObject();
        return new NativeDefragResult(false, false, false, 0L);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        checkOpen();
        Objects.requireNonNull(options, "options");
        return new NativeDefragReport(0L, 0L, 0L, 0L, 0L, 0L, false, false, false);
    }

    @Override
    public long logicalUsedBytes() {
        checkOpen();
        return objectBytes();
    }

    @Override
    public NativeAllocatorStats stats() {
        checkOpen();
        long bytes = objectBytes();
        return new NativeAllocatorStats(
                bytes, bytes, bytes, 0L, 0L,
                0L, 0L, 0L,
                objects.size(), pinned.size(), 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                objectKindCounts(), NativeAllocationLatencyHistogram.empty()
        );
    }

    @Override
    public NativeAllocatorMetadataStats metadataStats() {
        checkOpen();
        long freeSlots = maxSlots == 0 ? 0L : maxSlots - objects.size();
        return new NativeAllocatorMetadataStats(objects.isEmpty() ? 0L : 1L, freeSlots);
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        checkOpen();
        long bytes = Math.addExact(objectBytes(), regionBytes());
        return new MemoryUsageSnapshot(0L, 0L, bytes, bytes, 0L);
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        checkOpen();
        Objects.requireNonNull(budget, "budget");
        return MemoryReclaimResult.empty();
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        checkOpen();
        return new NativeAllocationGrowth(0L, 0L, sumRequestedBytes(requestedBytes));
    }

    @Override
    public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        checkOpen();
        return new NativeAllocationGrowth(0L, 0L, sumRequestedBytes(requestedBytes));
    }

    @Override
    public long liveRegionCount() {
        checkOpen();
        return regions.size();
    }

    public boolean isClosedForTesting() {
        return closed;
    }

    @Override
    public void close() {
        owner.checkCurrentThreadForShutdown();
        if (closed) return;
        if (!views.isEmpty()) throw new IllegalStateException("heap backend has live object views");
        if (!regions.isEmpty()) throw new IllegalStateException("heap backend has live regions");
        if (!pinned.isEmpty()) throw new IllegalStateException("heap backend has pinned objects");
        if (activeAllocationScope != null) {
            throw new IllegalStateException("heap backend has an active allocation scope");
        }
        if (activeEpochs != 0) throw new IllegalStateException("heap backend has active epochs");
        objects.clear();
        objectKinds.clear();
        closed = true;
    }

    private void checkOpen() {
        owner.checkCurrentThread();
        if (closed) throw new IllegalStateException("heap stable memory backend is closed");
    }

    private long requireLive(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        if (handle.allocatorId() != allocatorId) {
            throw new NativeHandleOwnershipException(allocatorId, handle.allocatorId());
        }
        long localRaw = handle.localRaw();
        if (localRaw <= 0L || !objects.containsKey(localRaw)) {
            throw new StaleNativeHandleException("stale heap native handle: " + localRaw);
        }
        return localRaw;
    }

    private boolean hasLiveView(long localRaw) {
        return views.stream().anyMatch(view -> view.localRaw == localRaw);
    }

    private long objectBytes() {
        long bytes = 0L;
        for (byte[] object : objects.values()) bytes = Math.addExact(bytes, object.length);
        return bytes;
    }

    private long regionBytes() {
        long bytes = 0L;
        for (HeapRegion region : regions) bytes = Math.addExact(bytes, region.data.length);
        return bytes;
    }

    private NativeObjectKindCounts objectKindCounts() {
        return new NativeObjectKindCounts(
                kindCount(NativeObjectKind.GENERIC),
                kindCount(NativeObjectKind.STRING_BYTES),
                kindCount(NativeObjectKind.LISTPACK_BYTES),
                kindCount(NativeObjectKind.HASH_FIELD_BYTES),
                kindCount(NativeObjectKind.HASH_VALUE_BYTES),
                kindCount(NativeObjectKind.SET_MEMBER_BYTES),
                kindCount(NativeObjectKind.ZSET_MEMBER_BYTES),
                kindCount(NativeObjectKind.SCORE_BYTES),
                kindCount(NativeObjectKind.ENTRY_RECORD),
                kindCount(NativeObjectKind.KEY_BYTES),
                kindCount(NativeObjectKind.LIST_ROOT),
                kindCount(NativeObjectKind.HASH_ROOT),
                kindCount(NativeObjectKind.SET_ROOT),
                kindCount(NativeObjectKind.ZSET_ROOT),
                kindCount(NativeObjectKind.LIST_NODE),
                kindCount(NativeObjectKind.HASH_TABLE),
                kindCount(NativeObjectKind.SET_TABLE),
                kindCount(NativeObjectKind.ZSET_TABLE),
                kindCount(NativeObjectKind.ZSET_NODE),
                kindCount(NativeObjectKind.INDEX_NODE),
                kindCount(NativeObjectKind.METADATA_RECORD)
        );
    }

    private long kindCount(NativeObjectKind kind) {
        return objectKinds.values().stream().filter(kind::equals).count();
    }

    private static long sumRequestedBytes(int[] requestedBytes) {
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        long total = 0L;
        for (int bytes : requestedBytes) {
            if (bytes < 0) throw new IllegalArgumentException("requested bytes must be non-negative");
            total = MemoryUsageSnapshot.addSaturating(total, bytes);
        }
        return total;
    }

    private final class HeapEpochScope implements NativeEpochScope {
        private final NativeEpochKind kind;
        private final long epoch;
        private boolean scopeClosed;

        private HeapEpochScope(NativeEpochKind kind, long epoch) {
            this.kind = kind;
            this.epoch = epoch;
        }

        @Override public NativeEpochKind kind() {
            owner.checkCurrentThread();
            return kind;
        }
        @Override public long epoch() {
            owner.checkCurrentThread();
            return epoch;
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (scopeClosed) return;
            scopeClosed = true;
            activeEpochs--;
        }
    }

    private final class HeapAllocationScope implements NativeAllocationScope {
        private final long baselineBytes;
        private final List<NativeHandle> allocations = new ArrayList<>();
        private boolean terminal;

        private HeapAllocationScope(long baselineBytes) {
            this.baselineBytes = baselineBytes;
        }

        @Override
        public NativeAllocationGrowth growth() {
            checkOpen();
            if (terminal) return NativeAllocationGrowth.zero();
            return new NativeAllocationGrowth(
                    0L,
                    0L,
                    Math.max(0L, objectBytes() - baselineBytes)
            );
        }

        @Override
        public void promote() {
            checkOpen();
            if (terminal) return;
            terminal = true;
            allocations.clear();
            activeAllocationScope = null;
        }

        @Override
        public void abort() {
            checkOpen();
            if (terminal) return;
            terminal = true;
            activeAllocationScope = null;
            RuntimeException failure = null;
            for (int index = allocations.size() - 1; index >= 0; index--) {
                NativeHandle handle = allocations.get(index);
                long localRaw = handle.localRaw();
                if (!objects.containsKey(localRaw)) continue;
                try {
                    if (pinned.contains(localRaw) || hasLiveView(localRaw)) {
                        throw new NativeMemoryException("scoped native object is still retained");
                    }
                    objects.remove(localRaw);
                    objectKinds.remove(localRaw);
                } catch (RuntimeException releaseFailure) {
                    if (failure == null) failure = releaseFailure;
                    else failure.addSuppressed(releaseFailure);
                }
            }
            allocations.clear();
            if (failure != null) throw failure;
        }

        private void track(NativeHandle handle) {
            if (terminal) throw new IllegalStateException("native allocation scope is closed");
            allocations.add(handle);
        }

        private void untrack(NativeHandle handle) {
            allocations.remove(handle);
        }
    }

    private final class HeapView implements NativeObjectView {
        private final NativeHandle handle;
        private final long localRaw;
        private final byte[] data;
        private final NativeAccessMode mode;
        private boolean viewClosed;

        private HeapView(NativeHandle handle, long localRaw, byte[] data, NativeAccessMode mode) {
            this.handle = handle;
            this.localRaw = localRaw;
            this.data = data;
            this.mode = mode;
        }

        @Override public NativeHandle handle() { checkReadable(); return handle; }
        @Override public int size() { checkReadable(); return data.length; }
        @Override public int capacity() { checkReadable(); return data.length; }
        @Override public byte getByte(int index) {
            checkReadable();
            return data[index];
        }
        @Override public void setByte(int index, byte value) {
            checkWritable();
            data[index] = value;
        }
        @Override public void getBytes(int index, byte[] dst, int dstOff, int len) {
            checkReadable();
            Objects.requireNonNull(dst, "dst");
            Objects.checkFromIndexSize(index, len, data.length);
            Objects.checkFromIndexSize(dstOff, len, dst.length);
            System.arraycopy(data, index, dst, dstOff, len);
        }
        @Override public void setBytes(int index, byte[] src, int srcOff, int len) {
            checkWritable();
            Objects.requireNonNull(src, "src");
            Objects.checkFromIndexSize(index, len, data.length);
            Objects.checkFromIndexSize(srcOff, len, src.length);
            System.arraycopy(src, srcOff, data, index, len);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (viewClosed) return;
            viewClosed = true;
            views.remove(this);
        }

        private void checkReadable() {
            checkOpen();
            if (viewClosed) throw new IllegalStateException("heap object view is closed");
            if (objects.get(localRaw) != data) {
                throw new StaleNativeHandleException("heap object view is stale");
            }
        }

        private void checkWritable() {
            checkReadable();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new NativeMemoryException("heap object view is read-only");
            }
        }
    }

    private final class HeapRegion implements StableMemoryRegion {
        private final byte[] data;
        private boolean regionClosed;

        private HeapRegion(int bytes) {
            this.data = new byte[bytes];
        }

        @Override public int size() { checkRegionOpen(); return data.length; }
        @Override public byte getByte(int offset) { checkRange(offset, 1); return data[offset]; }
        @Override public void setByte(int offset, byte value) {
            checkRange(offset, 1);
            data[offset] = value;
        }
        @Override public int getIntLittleEndian(int offset) {
            checkRange(offset, Integer.BYTES);
            return (data[offset] & 0xff)
                    | ((data[offset + 1] & 0xff) << 8)
                    | ((data[offset + 2] & 0xff) << 16)
                    | ((data[offset + 3] & 0xff) << 24);
        }
        @Override public void setIntLittleEndian(int offset, int value) {
            checkRange(offset, Integer.BYTES);
            for (int index = 0; index < Integer.BYTES; index++) {
                data[offset + index] = (byte) (value >>> (index * 8));
            }
        }
        @Override public long getLongLittleEndian(int offset) {
            checkRange(offset, Long.BYTES);
            long value = 0L;
            for (int index = 0; index < Long.BYTES; index++) {
                value |= ((long) data[offset + index] & 0xffL) << (index * 8);
            }
            return value;
        }
        @Override public void setLongLittleEndian(int offset, long value) {
            checkRange(offset, Long.BYTES);
            for (int index = 0; index < Long.BYTES; index++) {
                data[offset + index] = (byte) (value >>> (index * 8));
            }
        }
        @Override public void getBytes(int offset, byte[] dst, int dstOffset, int length) {
            checkRange(offset, length);
            Objects.requireNonNull(dst, "dst");
            Objects.checkFromIndexSize(dstOffset, length, dst.length);
            System.arraycopy(data, offset, dst, dstOffset, length);
        }
        @Override public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            checkRange(offset, length);
            Objects.requireNonNull(src, "src");
            Objects.checkFromIndexSize(srcOffset, length, src.length);
            System.arraycopy(src, srcOffset, data, offset, length);
        }
        @Override public void copyTo(
                int sourceOffset,
                StableMemoryRegion target,
                int targetOffset,
                int length
        ) {
            checkRange(sourceOffset, length);
            Objects.requireNonNull(target, "target");
            byte[] copy = Arrays.copyOfRange(data, sourceOffset, sourceOffset + length);
            target.setBytes(targetOffset, copy, 0, copy.length);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (regionClosed) return;
            regionClosed = true;
            regions.remove(this);
        }

        private void checkRegionOpen() {
            checkOpen();
            if (regionClosed) throw new IllegalStateException("heap region is closed");
        }

        private void checkRange(int offset, int length) {
            checkRegionOpen();
            Objects.checkFromIndexSize(offset, length, data.length);
        }
    }
}
```

Every interface method is declared directly on the class; the fake imports
neither the FFM package nor reflection/proxy APIs. `NO_MOVE` rejects any
heap-array size change because the fake has no spare capacity. Every derived
view/scope/epoch operation checks `owner.checkCurrentThread()` and never binds;
`resolvePinned` is read-only and does not own the caller's pin. The fake uses
explicit little-endian typed region access to match the DB layouts, and the
temporary full-array copy in `copyTo` preserves overlap semantics.

- [ ] **Step 10: Remove every DB-to-FFM dependency and inject FFM at the composition root**

In `yierdis-db-memory/pom.xml`, retain `yierdis-memory-api` at compile scope and delete the `yierdis-memory-ffm` dependency completely. Convert backend-neutral DB unit tests to `HeapStableMemoryBackend`. Keep allocator-page, FFM defrag, and native leak behavior in `yierdis-memory-ffm` tests or `yierdis-integration-tests`, where the composition root may depend on both modules; no DB-memory main or test source imports `yier.bubu.redis.memory.foreign`.

In `MutationFaultInjectionTest` and `ZSetValueTest`, retain the existing
`failOnAllocation`, `disableFailures`, `resetAttempts`, and
`allocationAttempts` calls unchanged. Migrate only the fixture type and its
construction to `FailOnAllocationStableMemoryBackend`; do not add aliases or
reinterpret the one-based allocation-attempt contract.

In `yierdis-server-main/pom.xml`, add a direct compile dependency on `yierdis-memory-api` because bootstrap declares `StableMemoryBackendFactory`; retain its direct `yierdis-db-api`, `yierdis-db-memory`, and `yierdis-memory-ffm` composition dependencies. In `yierdis-integration-tests/pom.xml`, add direct test-scope dependencies on `yierdis-memory-api` and `yierdis-memory-ffm`, retaining the direct test dependencies on `yierdis-db-api` and `yierdis-db-memory`. Neither module may rely on the removed `db-memory -> memory-ffm` transitive edge.

At this checkpoint `YierdisServerBootstrap` still owns the old
`YierdisServerRuntimeConfig`. Adapt that existing input through the named DB
implementation record so Task 4 remains compilable before grouped configuration
exists:

```java
StableMemoryBackendFactory stableMemoryBackendFactory = YierdisFfmStableMemoryBackend::new;
DbEngineFactory dbEngineFactory = new YierdisDbEngineFactory(
        stableMemoryBackendFactory,
        new YierdisDbBackendConfig(runtimeConfig.nativeSlotCapacity())
);
```

Command/runtime Task 5 replaces this one `runtimeConfig.nativeSlotCapacity()`
expression with `storage.nativeSlotCapacity()` while it introduces
`StorageConfig`; Task 4 must not refer to `StorageConfig` directly.

Migrate all downstream sources named in this Task 4 file list at this checkpoint. Apply these exact Task 1 contract rules to `YierdisServerBootstrapCloseTest` and `YierdisInstanceTest`:

```java
DbEngineFactory factory = config ->
        new FailingRuntimeDbEngine("db-" + config.dbIndex(), closeOrder);
```

An anonymous factory overrides only the configured signature:

```java
@Override
public RuntimeDbEngine create(DbEngineConfig config) {
    if (calls++ == 0) {
        return new CloseTrackingRuntimeDbEngine(
                "db-" + config.dbIndex(),
                closeOrder
        );
    }
    throw new IllegalStateException("boom-create-" + config.dbIndex());
}
```

Delete every six-argument factory lambda/override and do not unpack `DbEngineConfig` into a compatibility call. Every baseline test double implements `reads()`, `writes()`, `expiration()`, `memory()`, `lifecycle()`, `bindToCurrentThread()`, `runMaintenance()`, and `shutdown()`. It does not implement `memoryUsage()`, `enforceMaxmemoryMaintenance()`, commit attachment, maxmemory participation, or defrag. A test double implements `CommitPublishingDbEngine`, `GlobalMaxmemoryDbEngine`, or `DefragmentableDbEngine` only when that test's configured change sink, global maxmemory, or enabled defrag requires the capability; a combined test double implements only the required combination. Keep the corresponding capability methods and counters on those explicit types, with no default methods, adapter, or fallback.

`MaxmemoryPhysicalProgressTest` declares its fake as `GlobalMaxmemoryDbEngine` and constructs the governor with the exact array type:

```java
GlobalMaxmemoryDbEngine participant = new PhysicalProgressEngine(evictions);
YierdisGlobalMaxmemoryGovernor governor = new YierdisGlobalMaxmemoryGovernor(
        new GlobalMaxmemoryDbEngine[]{participant},
        100,
        MaxmemoryPolicy.ALLKEYS_RANDOM,
        5,
        0
);
```

`PhysicalProgressEngine` implements the complete baseline runtime methods plus `GlobalMaxmemoryDbEngine` methods, including `memoryUsage()`, `trimMemory(...)`, `scanBestCandidate(...)`, coordinator attachment, key/expiry/candidate/eviction behavior, and `runMaintenance()`. Do not pass `MaxmemoryParticipant[]` to the governor.

All server-main and integration FFM/DB construction goes through `YierdisFfmStableMemoryBackend`, `StableMemoryBackendFactory`, `YierdisDbEngineFactory`, `YierdisDbBackendConfig`, and `DbEngineConfig`. `TestDbs` may centralize concrete-only integration fixtures with this exact construction shape:

```java
public static YierdisDb createFfmDb(DbEngineConfig config, int nativeSlotCapacity) {
    StableMemoryBackendFactory backendFactory =
            YierdisFfmStableMemoryBackend::new;
    RuntimeDbEngine engine = new YierdisDbEngineFactory(
            backendFactory,
            new YierdisDbBackendConfig(nativeSlotCapacity)
    ).create(Objects.requireNonNull(config, "config"));
    if (engine instanceof YierdisDb db) {
        return db;
    }
    engine.shutdown();
    throw new IllegalStateException("YierdisDbEngineFactory did not create YierdisDb");
}
```

The helper is `public` because command, runtime, and storage integration packages consume it. It returns an unbound, caller-owned `YierdisDb`; each direct caller binds it, shuts it down exactly once in `finally`, and does not close the injected backend separately. Instance helpers pass the same configured `YierdisDbEngineFactory` to `YierdisInstance`; the runtime supplies one `DbEngineConfig` per database and the instance owns shutdown. Do not reconstruct the positional arguments, call a deleted `YierdisDb` constructor/static FFM factory, construct/share `YierdisFfmMemoryRuntime`, or expose the stable allocator. Each database receives one backend and the same `DbThreadGuard` as its owner; callers bind only the engine/instance, never a second owner.

For `EmptyDatabaseFootprintTest` and `OffHeapLeakRegressionTest`, capture each public facade produced by the injected factory when physical assertions need it:

```java
List<YierdisFfmStableMemoryBackend> createdBackends = new ArrayList<>();
StableMemoryBackendFactory backendFactory = (name, maxSlots, owner) -> {
    YierdisFfmStableMemoryBackend backend =
            new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
    createdBackends.add(backend);
    return backend;
};
```

Preserve the existing empty-footprint, eviction-progress, reclaim, and no-leak assertions with `backend.memoryUsage()`, `backend.liveRegionCount()`, and the engine's checked `GlobalMaxmemoryDbEngine.memoryUsage()` while the owner is bound. Record a physical construction baseline and the workload high-water snapshot. After deleting/expiring the workload's remaining keys, call `backend.trimEmptyPages(MemoryPressureBudget.unlimited())`; assert `nativeDataLiveBytes()` and `liveRegionCount()` return to their fixture baselines, and that metadata/data committed bytes do not exceed their workload high-water values. Do not require total committed bytes to equal the construction baseline, because empty allocator pages may remain committed/reclaimable until close. Do not replace physical assertions with semantic `MemoryStats` alone or delete the after-eviction/expiry inequalities. The database/instance remains the sole close owner; successful shutdown must prove the facade can release allocator state and all external regions without a leak failure, and no test reads a backend snapshot after close.

No FFM runtime or allocator escapes into server or DB composition. After downstream imports are removed, make the FFM runtime, region, stable allocator, synchronized wrapper, spans, object table, page allocator, and local codec package-private. Keep only `YierdisFfmStableMemoryBackend` public.

- [ ] **Step 11: Migrate benchmark composition and require physical-memory capability**

Make `yierdis-benchmark` an explicit FFM composition root. In `yierdis-benchmark/pom.xml`, retain the direct `yierdis-db-memory` dependency and add direct compile dependencies on `yierdis-db-api`, `yierdis-memory-api`, and `yierdis-memory-ffm`. Do not add a server or runtime module dependency.

In `StorageBenchmarkRunner.defaultEngineFactory()`, preserve the existing `Supplier<RuntimeDbEngine>` seam and replace the no-argument factory plus positional `create(...)` call with this exact composition:

```java
StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new;
YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
        backendFactory,
        new YierdisDbBackendConfig(0)
);
DbEngineConfig engineConfig = new DbEngineConfig(
        0,
        0L,
        MaxmemoryPolicy.NOEVICTION,
        5,
        5L,
        5L,
        new DbDefragConfig(false, 0L, 0L, 0L)
);
return () -> factory.create(engineConfig);
```

The benchmark reports physical storage accounting, so capability-check the engine before taking a snapshot. Add this package-visible helper:

```java
static GlobalMaxmemoryDbEngine requirePhysicalMemoryCapability(RuntimeDbEngine engine) {
    Objects.requireNonNull(engine, "engine");
    if (engine instanceof GlobalMaxmemoryDbEngine globalEngine) {
        return globalEngine;
    }
    throw new IllegalStateException(
            "storage benchmark requires GlobalMaxmemoryDbEngine"
    );
}
```

At the start of `snapshot(...)`, before reading RSS or any memory views, require the capability:

```java
GlobalMaxmemoryDbEngine globalEngine =
        requirePhysicalMemoryCapability(engine);
```

Pass `globalEngine.memoryUsage()` to `StorageMemorySnapshot.from(...)`; retain `engine.memory().memoryStats()` for the semantic DB view. There is no `engine.memoryUsage()` call through `RuntimeDbEngine`, compatibility overload, fallback snapshot, or alternate accounting path.

Keep `smallRealDatabaseRunReportsConsistentStructureAndAccounting()` in `StorageBenchmarkRunnerTest` unchanged in purpose. Add a focused regression proving that a baseline runtime engine is rejected:

```java
@Test
public void physicalSnapshotCapabilityIsRequired() {
    RuntimeDbEngine engine = new BaselineEngine();

    IllegalStateException failure = Assert.assertThrows(
            IllegalStateException.class,
            () -> StorageBenchmarkRunner.requirePhysicalMemoryCapability(engine)
    );

    Assert.assertEquals(
            "storage benchmark requires GlobalMaxmemoryDbEngine",
            failure.getMessage()
    );
}

private static final class BaselineEngine implements RuntimeDbEngine {
    @Override public DbReads reads() { return null; }
    @Override public DbWrites writes() { return null; }
    @Override public ExpirationManager expiration() { return null; }
    @Override public MemoryOps memory() { return null; }
    @Override public DbLifecycleOps lifecycle() { return null; }
    @Override public void bindToCurrentThread() { }
    @Override public void runMaintenance() { }
    @Override public void shutdown() { }
}
```

The test double implements only the baseline `RuntimeDbEngine` contract; do not make it implement `GlobalMaxmemoryDbEngine` or add a compatibility capability.

- [ ] **Step 12: Run focused DB, downstream restoration, architecture, and benchmark tests and verify GREEN**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-memory,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests,yierdis-benchmark -am \
  -Dtest=HeapStableMemoryBackendTest,StableMemoryBackendCompositionTest,DbOwnerBindingTest,ExpireIndexContractTest,EntryHandleContractTest,ValueHandleContractTest,EntryTableContractTest,YierdisDbArchitectureGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCloseTest,YierdisServerBootstrapCommandWiringTest,NettyExecutionAdapterIntegrationTest,YierdisInstanceTest,CommitStreamIntegrationTest,GlobalMaxmemoryLruAcrossDbsTest,MaxmemoryPhysicalProgressTest,EmptyDatabaseFootprintTest,YierdisSnapshotTest,OffHeapLeakRegressionTest,MaxmemoryEvictionTest,TtlMaxmemoryTest,MaxmemoryDoubleReplyRegressionTest,HllCommandTest,OffHeapKeysZeroCopyReadPathTest,OffHeapKeysCommandSmokeTest,StringCommandTest,CollectionScanCommandTest,StorageBenchmarkRunnerTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

Expected: PASS. The clean phase removes renamed/deleted DB classes before the
expire-index `Class.forName` absence assertion. The heap backend matches owner,
pinned-view, little-endian, and overlap contracts; it constructs and mutates a
DB, concurrent binding has one winner shared by DB/backend, full handles
round-trip through entry/collection/expire layouts, main DB source/POM have no
FFM implementation edge, server close/configuration tests use
`DbEngineConfig` and baseline/capability-typed doubles, governor tests use
`GlobalMaxmemoryDbEngine[]`, every named integration construction/accounting
path uses the public backend facade, and the benchmark requires the
physical-memory capability. `ArchitectureBoundaryTest` also proves that
`YierdisFfmStableMemoryBackend` is the sole public top-level FFM type.

- [ ] **Step 13: Run broader storage, server composition, architecture, integration, benchmark, and CLI tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-memory,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests,yierdis-benchmark,yierdis-cli -am test
```

Expected: PASS, including fault injection, native regression, benchmark physical-memory accounting, maxmemory, defrag, expiry, server bootstrap, architecture, off-heap leak tests, and the CLI tests that transitively exercise the restored `yierdis-server-main` dependency. The CLI requires no source migration because its source tree has no retired storage API consumer.

- [ ] **Step 14: Verify removal mechanically**

```bash
rg -n \
  'NativeAllocator|allocateRaw|reallocRaw|resolveRaw|resolvePinnedRaw|freeRaw|pinRaw|unpinRaw|NativeHandle\.fromRaw|\.raw\(\)|memory\.foreign|java\.lang\.foreign' \
  yierdis-db/yierdis-db-memory/src/main
```

Expected: no matches.

```bash
rg -n '<artifactId>yierdis-memory-ffm</artifactId>' \
  yierdis-db/yierdis-db-memory/pom.xml
```

Expected: no matches. `YierdisDbArchitectureGuardTest.dbMemoryPomDependsOnMemoryApiAndNotFfm` enforces the same rule structurally.

```bash
rg -n -U --pcre2 \
  -e 'new\s+YierdisDbEngineFactory\(\)' \
  -e 'new\s+YierdisDbEngineFactory\(\s*(?:nativeDefragOptions|memoryRuntime|runtime)\b' \
  -e 'engineFactory\(\s*\(\s*dbIndex\s*,' \
  -e 'DbEngineFactory\s+\w+\s*=\s*\(\s*dbIndex\s*,' \
  -e 'RuntimeDbEngine\s+create\(\s*int\s+dbIndex\b' \
  -e '\.create\(\s*\d+\s*,\s*\d+L\s*,\s*MaxmemoryPolicy\.' \
  -e '\b(?:NativeDefragOptions|enforceMaxmemoryMaintenance|YierdisStableNativeAllocator|NativeAllocator)\b' \
  -e '\bYierdisFfm(?!StableMemoryBackend\b)\w*\b' \
  -e 'YierdisDb\.createWith(?:Owned|Shared)FfmRuntime\s*\(' \
  -e 'new\s+YierdisDb\s*\(' \
  -e 'MaxmemoryParticipant\s*\[\s*\]' \
  -e '\bengine\.memoryUsage\s*\(' \
  yierdis-server/yierdis-server-main/src \
  yierdis-tests/yierdis-integration-tests/src/test/java \
  yierdis-benchmark/src
```

Expected: no matches. These roots contain no positional/no-argument/old-constructor factory form, retired baseline runtime method, old FFM runtime/allocator type, deleted `YierdisDb` construction path, old governor participant array, or unchecked `RuntimeDbEngine` physical-memory call. `YierdisFfmStableMemoryBackend` is the only allowed FFM implementation type outside `memory-ffm`.

- [ ] **Step 15: Commit DB-memory decoupling and handle migration**

```bash
git add \
  yierdis-db/yierdis-db-memory \
  yierdis-memory/yierdis-memory-testkit \
  yierdis-memory/yierdis-memory-ffm/src/main/java \
  yierdis-server/yierdis-server-main \
  yierdis-tests/yierdis-architecture-tests \
  yierdis-tests/yierdis-integration-tests/pom.xml \
  yierdis-tests/yierdis-integration-tests/src/test/java \
  yierdis-benchmark/pom.xml \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/storage/StorageBenchmarkRunner.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/storage/StorageBenchmarkRunnerTest.java
git commit -m "refactor: decouple database memory from FFM"
```

---

### Task 5: Replace RESP-Aware Storage Results With Semantic Sources And Prepared Mutations

**Files:**
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/PreparedMutation.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/PayloadLengthSink.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/ByteValueSink.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/ByteSequenceSource.java`
- Create: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/ByteMapSource.java`
- Rename: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringValue.java` to `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/ByteValue.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMetrics.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/MeasuredBulkStringSequence.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/MeasuredBulkStringSequences.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapMetrics.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapMetricsSources.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapPairs.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringMapPairsSupport.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSequence.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSequences.java`
- Delete: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/BulkStringSink.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/CollectionScanWindow.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/KeyScanWindow.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/result/PoppedValueSequence.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/StringReadOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/StringWriteOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/HashReadOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ListReadOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ListWriteOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/SetReadOps.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/ZSetReadOps.java`
- Rename: `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/MeasuredReplySourceTest.java` to `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/SemanticResultSourceTest.java`
- Rename: `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/OwnedReplyValueTest.java` to `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/result/ByteValueTest.java`
- Create: `yierdis-db/yierdis-db-api/src/test/java/yier/bubu/redis/storage/api/PreparedMutationContractTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisStringOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisHashOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisListOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisSetOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisZSetOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisKeyspaceOps.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/MaterializedCollectionScanWindow.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeCollectionScanWindow.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/PinnedPoppedValueSequence.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/PreparedPoppedValueSequence.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeListpack.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisListpack.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- Rename: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MeasuredReplySourceTest.java` to `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/SemanticResultSourceTest.java`
- Create: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/PreparedMutationStorageTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeCollectionReadStreamingTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/NativeCollectionScanWindowTest.java`
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/PinnedPoppedValueSequenceTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java`

**Downstream compile consumers assigned to command/runtime Task 3, not edited in this task:**
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Rename there: `BulkStringReplyAdapter.java` to `ByteValueReplyAdapter.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CollectionScanCommandSupport.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplySizer.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplySizerTest.java`

**Interfaces:**
- Produces: scalar, sequence, map, scan, pop, and prepared-mutation contracts with semantic payload lengths and retained-memory cost only.
- Invariant: a length visit is repeatable, zero-copy, encounter-ordered, and does not emit, consume, unpin, transfer ownership, or close the source.
- Invariant: sequence visits emit exactly `elementCount()` lengths; map visits emit exactly `pairCount() * 2` lengths in field/value order; `-1` is the only semantic null length.
- Invariant: no DB API/storage class imports server reply types or implements `$`, `*`, `%`, CRLF, header-digit, RESP2, or RESP3 formulas.

- [ ] **Step 1: Write the failing semantic source tests**

Replace the DB API measured-source test with `SemanticResultSourceTest.java`:

```java
package yier.bubu.redis.storage.api.result;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class SemanticResultSourceTest {
    @Test
    public void sequenceLengthsAreRepeatableOrderedAndNonConsuming() {
        AtomicInteger emissions = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        ByteSequenceSource source = new ByteSequenceSource() {
            @Override public int elementCount() { return 3; }
            @Override public long retainedMemoryBytes() { return 17L; }
            @Override public void visitElementLengths(PayloadLengthSink out) {
                out.payloadLength(3);
                out.payloadLength(-1);
                out.payloadLength(5);
            }
            @Override public void emitTo(ByteValueSink out) {
                emissions.incrementAndGet();
                out.value(new byte[]{1, 2, 3});
                out.nullValue();
                out.value(new byte[]{4, 5, 6, 7, 8});
            }
            @Override public void close() { closes.compareAndSet(0, 1); }
        };

        Assert.assertEquals(List.of(3, -1, 5), lengths(source::visitElementLengths));
        Assert.assertEquals(List.of(3, -1, 5), lengths(source::visitElementLengths));
        Assert.assertEquals(0, emissions.get());
        source.emitTo(new CountingSink());
        Assert.assertEquals(1, emissions.get());
        source.close();
        source.close();
        Assert.assertEquals(1, closes.get());
    }

    @Test
    public void mapLengthsAreFieldValueOrdered() {
        ByteMapSource source = new ByteMapSource() {
            @Override public int pairCount() { return 2; }
            @Override public long retainedMemoryBytes() { return 23L; }
            @Override public void visitPairLengths(PayloadLengthSink out) {
                out.payloadLength(2);
                out.payloadLength(4);
                out.payloadLength(3);
                out.payloadLength(-1);
            }
            @Override public void emitPairsTo(ByteValueSink out) { }
            @Override public void close() { }
        };

        Assert.assertEquals(List.of(2, 4, 3, -1), lengths(source::visitPairLengths));
        Assert.assertEquals(2, source.pairCount());
        Assert.assertEquals(23L, source.retainedMemoryBytes());
    }

    private static List<Integer> lengths(LengthVisit visit) {
        List<Integer> values = new ArrayList<>();
        visit.accept(values::add);
        return values;
    }

    @FunctionalInterface
    private interface LengthVisit {
        void accept(PayloadLengthSink out);
    }

    private static final class CountingSink implements ByteValueSink {
        @Override public void value(byte[] data) { }
        @Override public void value(byte[] data, int offset, int length) { }
        @Override public void value(yier.bubu.redis.bytes.BytesSlice slice) { }
        @Override public void longAscii(long value) { }
        @Override public void nullValue() { }
    }
}
```

- [ ] **Step 2: Write the failing public prepared-mutation contract test**

Create `PreparedMutationContractTest.java`:

```java
package yier.bubu.redis.storage.api;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.MutationContext;

public class PreparedMutationContractTest {
    @Test
    public void previewAndValidationAreReadOnlyAndCommitIsExplicit() {
        AtomicInteger mutations = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        PreparedMutation<String> prepared = new PreparedMutation<>() {
            @Override public String preview() { return "result"; }
            @Override public boolean isCurrent() { return true; }
            @Override public MutationOutcome commit(MutationContext context) {
                mutations.incrementAndGet();
                return MutationOutcome.NONE;
            }
            @Override public void close() { closed.compareAndSet(false, true); }
        };

        Assert.assertEquals("result", prepared.preview());
        Assert.assertTrue(prepared.isCurrent());
        Assert.assertEquals(0, mutations.get());
        Assert.assertEquals(MutationOutcome.NONE, prepared.commit(MutationContext.none()));
        Assert.assertEquals(1, mutations.get());
        prepared.close();
        prepared.close();
        Assert.assertTrue(closed.get());
    }
}
```

- [ ] **Step 3: Run focused DB API tests and verify RED**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-api -am \
  -Dtest=SemanticResultSourceTest,ByteValueTest,PreparedMutationContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: test compilation fails because semantic source/sink contracts, `ByteValue`, and public `PreparedMutation` do not exist.

- [ ] **Step 4: Add the exact protocol-neutral result contracts**

Create these interfaces exactly:

```java
package yier.bubu.redis.storage.api.result;

@FunctionalInterface
public interface PayloadLengthSink {
    void payloadLength(int length);
}
```

`-1` means semantic null; implementations reject values below `-1` before publishing a source.

```java
package yier.bubu.redis.storage.api.result;

import yier.bubu.redis.bytes.BytesSlice;

public interface ByteValueSink {
    void value(byte[] data);
    void value(byte[] data, int offset, int length);
    void value(BytesSlice slice);
    void longAscii(long value);
    void nullValue();
}
```

```java
package yier.bubu.redis.storage.api.result;

public interface ByteSequenceSource extends AutoCloseable {
    int elementCount();
    long retainedMemoryBytes();
    void visitElementLengths(PayloadLengthSink out);
    void emitTo(ByteValueSink out);
    @Override void close();
}
```

```java
package yier.bubu.redis.storage.api.result;

public interface ByteMapSource extends AutoCloseable {
    int pairCount();
    long retainedMemoryBytes();
    void visitPairLengths(PayloadLengthSink out);
    void emitPairsTo(ByteValueSink out);
    @Override void close();
}
```

Specialized sources become:

```java
public interface PoppedValueSequence extends ByteSequenceSource {
    boolean isNull();
}

public interface CollectionScanWindow extends ByteSequenceSource {
    ScanCursorV2 nextCursor();
}

public interface KeyScanWindow extends ByteSequenceSource {
    ScanCursorV2 nextCursor();
    long inspectedSlots();
    long tableGeneration();
    long expiryEvaluationMillis();
    boolean current();
}
```

Create the public storage preparation in the root API package:

```java
package yier.bubu.redis.storage.api;

import yier.bubu.redis.common.command.MutationContext;

public interface PreparedMutation<R> extends AutoCloseable {
    R preview();
    boolean isCurrent();
    MutationOutcome commit(MutationContext context);
    @Override void close();
}
```

- [ ] **Step 5: Rename scalar ownership to `ByteValue`**

Rename `BulkStringValue` without retaining the old class. Keep these factories and methods:

```java
package yier.bubu.redis.storage.api.result;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.bytes.BytesSlice;

public final class ByteValue implements AutoCloseable {
    private enum Kind {
        NULL,
        BYTES,
        SLICE,
        LONG_ASCII,
        OWNED
    }

    private static final ByteValue NULL_VALUE = new ByteValue(
            Kind.NULL, null, 0, 0, null, 0L, 0L, null
    );

    private final Kind kind;
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private final BytesSlice slice;
    private final long longValue;
    private final long retainedMemoryBytes;
    private final AutoCloseable owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ByteValue(
            Kind kind,
            byte[] bytes,
            int offset,
            int length,
            BytesSlice slice,
            long longValue,
            long retainedMemoryBytes,
            AutoCloseable owner
    ) {
        this.kind = kind;
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.slice = slice;
        this.longValue = longValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
        this.owner = owner;
    }

    public static ByteValue nullValue() {
        return NULL_VALUE;
    }

    public static ByteValue bytes(byte[] data) {
        return data == null
                ? NULL_VALUE
                : new ByteValue(Kind.BYTES, data, 0, data.length, null, 0L, 0L, null);
    }

    public static ByteValue bytes(byte[] data, int offset, int length) {
        if (data == null) return NULL_VALUE;
        Objects.checkFromIndexSize(offset, length, data.length);
        return new ByteValue(Kind.BYTES, data, offset, length, null, 0L, 0L, null);
    }

    public static ByteValue slice(BytesSlice slice) {
        return slice == null
                ? NULL_VALUE
                : new ByteValue(Kind.SLICE, null, 0, 0, slice, 0L, 0L, null);
    }

    public static ByteValue longAscii(long value) {
        return new ByteValue(Kind.LONG_ASCII, null, 0, 0, null, value, 0L, null);
    }

    public static ByteValue owned(
            BytesSlice slice,
            int payloadLength,
            long retainedMemoryBytes,
            AutoCloseable owner
    ) {
        Objects.requireNonNull(slice, "slice");
        Objects.requireNonNull(owner, "owner");
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payloadLength must be non-negative");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
        }
        return new ByteValue(
                Kind.OWNED,
                null,
                0,
                payloadLength,
                slice,
                0L,
                retainedMemoryBytes,
                owner
        );
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public int payloadLength() {
        return switch (kind) {
            case NULL -> -1;
            case BYTES, OWNED -> length;
            case SLICE -> slice.length();
            case LONG_ASCII -> Long.toString(longValue).length();
        };
    }

    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    public void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        switch (kind) {
            case NULL -> out.nullValue();
            case BYTES -> out.value(bytes, offset, length);
            case SLICE, OWNED -> out.value(slice);
            case LONG_ASCII -> out.longAscii(longValue);
        }
    }

    @Override
    public void close() {
        if (owner == null || !closed.compareAndSet(false, true)) return;
        try {
            owner.close();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("byte value owner close failed", failure);
        }
    }
}
```

Replace `OwnedReplyValueTest` with `ByteValueTest`:

```java
package yier.bubu.redis.storage.api.result;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;

public class ByteValueTest {
    @Test
    public void nullArraySliceAndLongExposeSemanticPayloads() {
        RecordingSink sink = new RecordingSink();

        ByteValue nullValue = ByteValue.nullValue();
        Assert.assertTrue(nullValue.isNull());
        Assert.assertEquals(-1, nullValue.payloadLength());
        nullValue.emitTo(sink);
        Assert.assertEquals("null", sink.kind);

        ByteValue array = ByteValue.bytes(bytes("abcd"), 1, 2);
        Assert.assertFalse(array.isNull());
        Assert.assertEquals(2, array.payloadLength());
        array.emitTo(sink);
        Assert.assertEquals("array", sink.kind);
        Assert.assertEquals("bc", sink.value);

        ByteValue slice = ByteValue.slice(new ArraySlice(bytes("slice")));
        Assert.assertEquals(5, slice.payloadLength());
        slice.emitTo(sink);
        Assert.assertEquals("slice", sink.kind);
        Assert.assertEquals("slice", sink.value);

        ByteValue number = ByteValue.longAscii(Long.MIN_VALUE);
        Assert.assertEquals(20, number.payloadLength());
        number.emitTo(sink);
        Assert.assertEquals("long", sink.kind);
        Assert.assertEquals(Long.toString(Long.MIN_VALUE), sink.value);
    }

    @Test
    public void arrayWindowChecksBounds() {
        byte[] data = bytes("abc");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> ByteValue.bytes(data, -1, 1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> ByteValue.bytes(data, 2, 2));
    }

    @Test
    public void ownedValueReportsRetainedBytesAndClosesOwnerOnce() {
        AtomicInteger closes = new AtomicInteger();
        ByteValue value = ByteValue.owned(
                new ArraySlice(bytes("owned")),
                5,
                37L,
                closes::incrementAndGet
        );

        Assert.assertEquals(5, value.payloadLength());
        Assert.assertEquals(37L, value.retainedMemoryBytes());
        value.close();
        value.close();
        Assert.assertEquals(1, closes.get());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private record ArraySlice(byte[] data) implements BytesSlice {
        @Override public int length() { return data.length; }
        @Override public byte getByte(int index) { return data[index]; }
        @Override public void writeTo(BytesSink out) {
            out.writeBytes(data, 0, data.length);
        }
    }

    private static final class RecordingSink implements ByteValueSink {
        private String kind;
        private String value;

        @Override public void value(byte[] data) {
            kind = "array";
            value = new String(data, StandardCharsets.US_ASCII);
        }
        @Override public void value(byte[] data, int offset, int length) {
            kind = "array";
            value = new String(data, offset, length, StandardCharsets.US_ASCII);
        }
        @Override public void value(BytesSlice slice) {
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            kind = "slice";
            value = new String(data, StandardCharsets.US_ASCII);
        }
        @Override public void longAscii(long number) {
            kind = "long";
            value = Long.toString(number);
        }
        @Override public void nullValue() {
            kind = "null";
            value = null;
        }
    }
}
```

Use the existing five-kind representation (`NULL`, byte array, slice, long ASCII, owned slice). `emitTo` maps them exactly to `nullValue`, `value(byte[], offset, length)`, `value(BytesSlice)`, and `longAscii`. The owned close path retains its existing `AtomicBoolean`, so the owner closes once after success, stale reprepare, cancellation, or shutdown. `ByteValueTest` covers null length `-1`, array bounds, retained bytes, every representation's sink path, and double close.

- [ ] **Step 6: Replace operation return types and expose prepared result mutations**

Apply this exact API mapping:

| API method | New result |
| --- | --- |
| `StringReadOps.getStringValue` | `ByteValue` |
| `StringReadOps.previewStringValue` | delete |
| `HashReadOps.hget` | `ByteValue` |
| `HashReadOps.hgetall` | `ByteMapSource` |
| `ListReadOps.lrange` | `ByteSequenceSource` |
| `ListReadOps.previewPop` | delete |
| `SetReadOps.smembers` | `ByteSequenceSource` |
| all four zset range methods | `ByteSequenceSource` |
| key/collection scan methods | existing specialized source extending `ByteSequenceSource` |
| `StringWriteOps.SetStringValue.oldValue` | `ByteValue` |

Add the two result-dependent preparation entries:

```java
PreparedMutation<StringWriteOps.SetStringValue> prepareSet(
        byte[] keyBytes,
        BytesSlice value,
        SetMode mode,
        ExpireOption expireOption
);
```

Keep direct `set(...)`/`setString(...)` only for parsed SET forms that do not request the old value. Replace `ListWriteOps.lpop(...)` and `rpop(...)` with:

```java
PreparedMutation<PoppedValueSequence> preparePop(
        byte[] keyBytes,
        int count,
        boolean left
);
```

The boolean selects left/right and one API covers counted and uncounted command forms. There is no separate read preview followed by a direct pop.

- [ ] **Step 7: Implement repeatable zero-copy length traversal in storage**

For every materialized source, retain payload arrays/slices once and implement `visitElementLengths` by reading their existing lengths. For every native source, retain the exact pins/handles already required for emission and traverse listpack/object metadata without allocating payload arrays. A visit may open and close a read-only `NativeObjectView`; it must not unpin or close the source.

Replace storage wire metrics as follows:

| Existing code | Replacement |
| --- | --- |
| `BulkStringMetrics` pre-emission pass | direct `PayloadLengthSink.payloadLength(payloadLength)` calls |
| `encodedElementBytes` fields | remove |
| `encodedBulkStringBytes(...)` | remove |
| RESP header/delimiter `decimalDigits(...)` | remove |
| long/integer payloads | report the semantic ASCII payload length used by `ByteValueSink.longAscii(...)` |
| source emission | `ByteValueSink`, with no close or ownership transfer inside `emitTo` |

`MaterializedCollectionScanWindow`, `NativeCollectionScanWindow`, `PinnedPoppedValueSequence`, and `PreparedPoppedValueSequence` each use an `AtomicBoolean` or equivalent one-way state for close. `visit*Lengths` and `emit*` reject use after close. Neither method closes the source; the prepared command owner does so exactly once.

For maps, `YierdisHashOps`/`HashValue` call the visitor field then value for every encounter. For zset ranges with scores, report member then score lengths in the same order as emission. `elementCount()` includes score elements when `WITHSCORES` is true.

- [ ] **Step 8: Implement storage-backed `PreparedMutation` for SET GET and POP**

In `YierdisStringOps.prepareSet(...)` and `YierdisListOps.preparePop(...)`:

1. Capture the entry handle plus version token with read-only lookup.
2. Build/stage the existing internal ledger/value mutation without publishing it.
3. Build one owned `ByteValue` or `PoppedValueSequence` preview from the pre-commit state.
4. `preview()` returns the same preview object without changing keyspace, expiry, ledger, commit publication, or access clocks.
5. `isCurrent()` performs read-only handle/version comparison and returns false after replacement, expiry-state change, deletion, or another mutation.
6. `commit(context)` requires the DB owner, rejects a stale/closed/already-committed preparation, applies the staged mutation once through the existing ledger executor, and returns its `MutationOutcome`.
7. `close()` aborts uncommitted staging and closes the preview/pins once; after commit it closes only retained preview ownership.

Use this state check in both concrete preparations:

```java
private void requireCommittable() {
    if (closed) throw new IllegalStateException("prepared mutation is closed");
    if (committed) throw new IllegalStateException("prepared mutation is already committed");
    if (!isCurrent()) throw new IllegalStateException("prepared mutation is stale");
}
```

`MutationContext` is accepted only by `commit`; it is not retained during preview and is passed to the existing commit-publication path at the moment of commit.

- [ ] **Step 9: Add storage-level stale and no-mutation-before-commit tests**

Create `PreparedMutationStorageTest` with the complete heap-backed fixture and assertions below:

```java
package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.*;
import yier.bubu.redis.storage.api.result.*;

public class PreparedMutationStorageTest {
@Test
public void popPreviewAndValidationDoNotMutateAndStalePreparationCannotCommit() {
    try (TestDb db = heapDb()) {
        db.push("list", List.of("a", "b", "c"));
        try (PreparedMutation<PoppedValueSequence> prepared =
                     db.writes().lists().preparePop(bytes("list"), 2, true)) {
            Assert.assertEquals(List.of("a", "b"), strings(prepared.preview()));
            Assert.assertTrue(prepared.isCurrent());
            Assert.assertEquals(List.of("a", "b", "c"), db.list("list"));

            db.writes().lists().rpush(bytes("list"), List.of(bytes("d")));
            Assert.assertFalse(prepared.isCurrent());
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> prepared.commit(MutationContext.none())
            );
        }
        Assert.assertEquals(List.of("a", "b", "c", "d"), db.list("list"));
    }
}

@Test
public void currentPopCommitsOnceAndKeepsPreviewReadableUntilClose() {
    try (TestDb db = heapDb()) {
        db.push("list", List.of("a", "b", "c"));
        try (PreparedMutation<PoppedValueSequence> prepared =
                     db.writes().lists().preparePop(bytes("list"), 2, true)) {
            PoppedValueSequence preview = prepared.preview();
            MutationOutcome outcome = prepared.commit(MutationContext.none());

            Assert.assertTrue(outcome.changedAny());
            Assert.assertEquals(List.of("a", "b"), strings(preview));
            Assert.assertEquals(List.of("c"), db.list("list"));
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> prepared.commit(MutationContext.none())
            );
        }
    }
}

@Test
public void setPreviewAndValidationDoNotMutateAndFreshPreparationCommitsOnce() {
    try (TestDb db = heapDb()) {
        db.set("key", "old");
        try (PreparedMutation<StringWriteOps.SetStringValue> stale =
                     db.writes().strings().prepareSet(
                             bytes("key"), slice("new"), SetMode.NORMAL, null
                     )) {
            Assert.assertTrue(stale.preview().applied());
            Assert.assertEquals("old", string(stale.preview().oldValue()));
            Assert.assertTrue(stale.isCurrent());
            Assert.assertEquals("old", db.string("key"));

            db.set("key", "intervening");
            Assert.assertFalse(stale.isCurrent());
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> stale.commit(MutationContext.none())
            );
        }
        Assert.assertEquals("intervening", db.string("key"));

        try (PreparedMutation<StringWriteOps.SetStringValue> fresh =
                     db.writes().strings().prepareSet(
                             bytes("key"), slice("fresh"), SetMode.NORMAL, null
                     )) {
            StringWriteOps.SetStringValue preview = fresh.preview();
            Assert.assertTrue(preview.applied());
            Assert.assertEquals("intervening", string(preview.oldValue()));
            Assert.assertEquals("intervening", db.string("key"));

            MutationOutcome outcome = fresh.commit(MutationContext.none());

            Assert.assertTrue(outcome.changedAny());
            Assert.assertEquals("intervening", string(preview.oldValue()));
            Assert.assertEquals("fresh", db.string("key"));
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> fresh.commit(MutationContext.none())
            );
        }
    }
}

private static TestDb heapDb() {
    RuntimeDbEngine engine = new YierdisDbEngineFactory(
            HeapStableMemoryBackend::new,
            new YierdisDbBackendConfig(4096)
    ).create(new DbEngineConfig(
            0,
            0L,
            MaxmemoryPolicy.NOEVICTION,
            5,
            5L,
            5L,
            new DbDefragConfig(false, 0L, 0L, 0L)
    ));
    engine.bindToCurrentThread();
    return new TestDb(engine);
}

private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
}

private static BytesSlice slice(String value) {
    return new ArraySlice(bytes(value));
}

private static List<String> strings(ByteSequenceSource source) {
    CollectingSink sink = new CollectingSink();
    source.emitTo(sink);
    return sink.values();
}

private static String string(ByteValue value) {
    CollectingSink sink = new CollectingSink();
    value.emitTo(sink);
    Assert.assertEquals(1, sink.values().size());
    return sink.values().get(0);
}

private static final class TestDb implements AutoCloseable {
    private final RuntimeDbEngine engine;

    private TestDb(RuntimeDbEngine engine) {
        this.engine = engine;
    }

    private DbWrites writes() {
        return engine.writes();
    }

    private void push(String key, List<String> values) {
        engine.writes().lists().rpush(
                bytes(key),
                values.stream().map(PreparedMutationStorageTest::bytes).toList()
        );
    }

    private List<String> list(String key) {
        try (ByteSequenceSource source = engine.reads().lists().lrange(bytes(key), 0, -1)) {
            return strings(source);
        }
    }

    private void set(String key, String value) {
        engine.writes().strings().setString(bytes(key), bytes(value), SetMode.NORMAL, null);
    }

    private String string(String key) {
        byte[] value = engine.reads().strings().getStringBytes(bytes(key));
        return value == null ? null : new String(value, StandardCharsets.US_ASCII);
    }

    @Override
    public void close() {
        engine.shutdown();
    }
}

private record ArraySlice(byte[] data) implements BytesSlice {
    @Override
    public int length() {
        return data.length;
    }

    @Override
    public byte getByte(int index) {
        return data[index];
    }

    @Override
    public void writeTo(BytesSink out) {
        out.writeBytes(data, 0, data.length);
    }
}

private static final class CollectingSink implements ByteValueSink {
    private final List<String> values = new ArrayList<>();

    @Override
    public void value(byte[] data) {
        values.add(new String(data, StandardCharsets.US_ASCII));
    }

    @Override
    public void value(byte[] data, int offset, int length) {
        values.add(new String(data, offset, length, StandardCharsets.US_ASCII));
    }

    @Override
    public void value(BytesSlice slice) {
        byte[] data = new byte[slice.length()];
        slice.getBytes(0, data, 0, data.length);
        value(data);
    }

    @Override
    public void longAscii(long value) {
        values.add(Long.toString(value));
    }

    @Override
    public void nullValue() {
        values.add(null);
    }

    private List<String> values() {
        return new ArrayList<>(values);
    }
}
}
```

- [ ] **Step 10: Run focused result and preparation tests and verify GREEN**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-api,yierdis-db/yierdis-db-memory -am \
  -Dtest=SemanticResultSourceTest,ByteValueTest,PreparedMutationContractTest,PreparedMutationStorageTest,NativeCollectionReadStreamingTest,NativeCollectionScanWindowTest,PinnedPoppedValueSequenceTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

Expected: PASS. The clean phase removes deleted result APIs and renamed test
classes before compilation and focused discovery. Storage modules compile
without any old result interface, repeated visitors preserve sources, and
prepared SET/POP paths mutate only during one current commit.

- [ ] **Step 11: Run the broader DB suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-db/yierdis-db-api,yierdis-db/yierdis-db-memory -am test
```

Expected: PASS for every DB API and DB-memory test. This is the Task 5 GREEN boundary. At this deliberate API-break checkpoint, `yierdis-command-builtin`, `yierdis-server-executor`, `yierdis-server-main`, `yierdis-networking-resp`, `yierdis-architecture-tests`, and `yierdis-integration-tests` still consume the retired measured/preview result contracts; command/runtime Task 3 migrates those exact modules to `ReplyShape`, `ReplySizer`, `ByteValueSink`, and `PreparedMutation` in the master execution order.

- [ ] **Step 12: Enforce protocol-neutral storage mechanically**

```bash
rg -n \
  'BulkString|MeasuredBulkString|encodedElementBytes|encodedBulkStringBytes|RESP2|RESP3|RespReply|ReplyShape|ReplyPlan|ReplySizer|\\r\\n' \
  yierdis-db/yierdis-db-api/src/main \
  yierdis-db/yierdis-db-memory/src/main
```

Expected: no matches.

```bash
rg -n \
  'decimalDigits|headerBytes|delimiterBytes|1L *\\+.*2L|2L *\\+.*2L' \
  yierdis-db/yierdis-db-memory/src/main
```

Expected: no RESP framing formulas. Semantic numeric-to-ASCII payload length helpers may use a name such as `signedLongAsciiLength`; they must not add protocol header or delimiter bytes.

- [ ] **Step 13: Record the exact zero-copy downstream handoff**

Command/runtime Task 3 updates every downstream file listed above. It constructs shapes without importing DB types into `server-api`:

```java
ReplyShape sequenceShape = ReplyShapes.sequence(
        source.elementCount(),
        source.retainedMemoryBytes(),
        consumer -> source.visitElementLengths(consumer::accept)
);

ReplyShape mapShape = ReplyShapes.byteMap(
        source.pairCount(),
        source.retainedMemoryBytes(),
        consumer -> source.visitPairLengths(consumer::accept)
);
```

For `ByteValue`, command preparation chooses `ReplyShapes.nullValue()` when `isNull()` and otherwise `ReplyShapes.bulkString(value.payloadLength(), value.retainedMemoryBytes())`. Rendering uses `ByteValueReplyAdapter implements ByteValueSink`. The RESP `ReplySizer` visits these shapes, applies RESP2 array versus RESP3 map rules, validates emitted length counts/values, and owns every wire formula.

For `PreparedMutation`, command preparation owns the returned object, derives the reply shape from `preview()`, maps `isCurrent()` to `ValidationResult.VALID`/`STALE`, calls `commit(context.mutationContext())` once during execution, and closes stale/success/cancellation/shutdown preparations once.

- [ ] **Step 14: Commit semantic storage results**

```bash
git add \
  yierdis-db/yierdis-db-api \
  yierdis-db/yierdis-db-memory \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java
git commit -m "refactor: expose protocol-neutral storage results"
```

---

## Final Verification After Command/Runtime Task 3

Run these only after command/runtime Task 3 has migrated the assigned consumers and added the server-owned reply algebra/RESP sizer.

- [ ] **Run the complete JDK 25 reactor**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn clean test
```

Expected: PASS across a clean complete reactor with no stale deleted class and
no skipped migration regression.

- [ ] **Run architecture and integration acceptance explicitly**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests -am test
```

Expected: PASS for dependency graph, source ownership, FFM boundary, protocol-neutral storage, runtime startup, command behavior, RESP2/RESP3 sizing, and leak coverage.

- [ ] **Verify no legacy API or cross-layer formula remains**

```bash
rg -n \
  'NativeAllocator|allocateRaw|reallocRaw|resolveRaw|resolvePinnedRaw|freeRaw|pinRaw|unpinRaw|NativeHandle\.fromRaw|BulkStringMetrics|MeasuredBulkString|encodedElementBytes' \
  --glob '*.java' \
  yierdis-memory yierdis-db yierdis-command yierdis-server yierdis-networking
```

Expected: no matches.

```bash
rg -n \
  'ReplyShape|ReplyPlan|ReplySizer|RESP2|RESP3|RespReply|\\r\\n' \
  yierdis-db/yierdis-db-api/src/main \
  yierdis-db/yierdis-db-memory/src/main
```

Expected: no matches.

## Risk Checklist

- A DB-owned native record cannot store one local raw handle and later reconstruct `new NativeHandle(currentBackendId, localRaw)`; store both longs or a heap-side `NativeHandle`.
- Backend ID exhaustion sets a permanent sentinel. Zero is never a live identity and IDs never wrap or reuse.
- Ownership failure precedes local-format validation and slot lookup for every handle-taking operation.
- FFM facade accounting adds externally allocated index regions once and does not add allocator page regions a second time.
- Backend close is last; live views, pins, or regions remain visible failures rather than suppressed leaks.
- The single atomic owner guard is injected before backend construction; backend access cannot bind implicitly or win a different race from DB access.
- Runtime capability validation completes for all engines before commit-stream preparation/start, coordinator attachment, or listener publication.
- Length visits neither allocate payload copies nor consume pinned sources; map order remains field/value and RESP2/RESP3 interpretation stays in `RespReplySizer`.
- Prepared previews and validation are read-only. A stale object closes before reprepare and no mutation executes twice.
- Source/preparation ownership closes once on success, stale reprepare, cancellation, connection close, and shutdown.
- Final source/POM guards reject old adapters, DB-to-server reply dependencies, storage wire formulas, and DB-memory-to-FFM compile imports.
