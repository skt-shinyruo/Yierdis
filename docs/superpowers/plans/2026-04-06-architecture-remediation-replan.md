# Architecture Remediation Replacement Plan

> Execution status: completed on 2026-04-06. All seven tasks and the final cross-wave regression gate were verified in the main workspace, and the work is being finalized in one aggregate commit rather than per-task commits.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current architecture roadmap with a lower-risk sequence that fixes metadata authority, runtime/server ownership seams, connection ownership, global maxmemory ownership, and `YierdisDb` centralization without changing wire protocol or command semantics.

**Architecture:** Execute the work in seven shippable waves. Waves 1 and 2 make metadata and runtime observability authoritative before any hot-path state-model changes. Waves 3 and 4 then stabilize connection ownership and global runtime ownership. Waves 5 and 6 shrink `YierdisDb` by extracting resource/accounting ownership first and only then narrowing the ops seam. Wave 7 locks protocol/reply boundaries and documents the final architecture.

**Tech Stack:** Java 25, Maven multi-module reactor, Netty 4.1, JUnit 4

**Supersedes:** `docs/superpowers/plans/2026-04-04-architecture-refactor-roadmap.md`

---

### Task 1: Remove Command Metadata Fallbacks And Make Registry Specs Authoritative

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Tighten guard tests so fallback metadata in `CommandRegistry` is illegal**

```java
scanFileForForbiddenText(
        repoRoot,
        commandRegistryFile,
        offenders,
        "defaultDescriptorForNameUpper(",
        "defaultArity(",
        "defaultFirstKeyIndex(",
        "defaultLastKeyIndex(",
        "defaultKeyStep("
);
```

```java
Assert.assertEquals(-1, registry.specByUpperName("INFO").descriptor().arity());
Assert.assertEquals("ERR HELLO is not allowed in MULTI", registry.specByUpperName("HELLO").disallowedInMultiError());
Assert.assertNotNull(registry.specByUpperName("SET").descriptor());
```

- [ ] **Step 2: Run focused command and boundary tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandMetadataRegressionTest,TransactionCommandTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `CommandRegistry` still manufactures descriptors from fallback tables and registrations are not yet the only metadata source.

- [ ] **Step 3: Remove descriptor fallback generation and require explicit registration metadata**

```java
public interface Registration {
    void register(String name, CommandSpec spec);

    default void register(String name, Handler handler, CommandDescriptor descriptor) {
        register(name, new CommandSpec(handler, Objects.requireNonNull(descriptor, "descriptor"), null));
    }
}
```

```java
private void registerInternal(String name, CommandSpec spec) {
    Objects.requireNonNull(spec.descriptor(), "descriptor");
    insert(new Entry(ascii, hash, spec));
}
```

- [ ] **Step 4: Update all core and server command registrations to provide explicit descriptors**

```java
registration.register("PING", this::ping, CommandDescriptor.of(-1, 0, 0, 0));
registration.register("ECHO", this::echo, CommandDescriptor.of(2, 0, 0, 0));
registration.register(
        "HELLO",
        new CommandSpec(this::hello, CommandDescriptor.of(-1, 0, 0, 0), "ERR HELLO is not allowed in MULTI")
);
```

- [ ] **Step 5: Run focused command and boundary tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandMetadataRegressionTest,TransactionCommandTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: remove command metadata fallbacks"
```

### Task 2: Move Runtime Observability And Maintenance Behind `core-runtime`

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Add failing guards for server-side DB/runtime reconstruction**

```java
scanFileForForbiddenText(
        repoRoot,
        bootstrapFile,
        offenders,
        "engines = instance.engines()",
        "bindEngines("
);
scanFileForForbiddenText(
        repoRoot,
        infoProviderFile,
        offenders,
        "db.memory().memoryStats()",
        "appendKeyspace("
);
```

- [ ] **Step 2: Run focused runtime and server tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisInstanceTest,MemoryStatsCommandTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because server code still keeps raw engine arrays and still reconstructs instance summaries from DB views.

- [ ] **Step 3: Expand `YierdisInstanceObservability` so it owns instance-wide memory and keyspace summaries**

```java
public final class YierdisInstanceObservability {
    public YierdisMemoryStats memoryStats() {
        return collectMemoryStats(instance.engines(), instance.config());
    }

    public List<YierdisDbSummary> dbSummaries() {
        return collectDbSummaries(instance.engines());
    }
}
```

```java
public record YierdisDbSummary(int dbIndex, int keyCount, int expireCount) {}
```

- [ ] **Step 4: Make server maintenance and INFO/STATS consume only runtime-owned seams**

```java
YierdisInstanceMaintenance maintenance = new YierdisInstanceMaintenance(instance);
cleanupFuture = workerGroup.next().scheduleWithFixedDelay(
        () -> exForTask.executeMaintenance(maintenance::maintenanceTick),
        period,
        period,
        TimeUnit.MILLISECONDS
);
```

```java
private void appendKeyspace(StringBuilder sb) {
    YierdisInstanceObservability runtimeObservability = observability;
    if (runtimeObservability == null) {
        return;
    }
    for (YierdisDbSummary summary : runtimeObservability.dbSummaries()) {
        if (summary.keyCount() <= 0 && summary.expireCount() <= 0) {
            continue;
        }
        sb.append("db").append(summary.dbIndex())
                .append(":keys=").append(summary.keyCount())
                .append(",expires=").append(summary.expireCount())
                .append("\r\n");
    }
}
```

- [ ] **Step 5: Run focused runtime and server tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisInstanceTest,MemoryStatsCommandTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceBoundaryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: move runtime observability behind core-runtime"
```

### Task 3: Turn `ServerConnectionContext` Into A Behavior Owner Instead Of A Slice Wrapper

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorChannelState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add failing guardrails that forbid slice reach-through outside `ServerConnectionContext`**

```java
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis"),
        offenders,
        ".runtime()",
        ".session()",
        ".scheduling()"
);
allowOnly(
        offenders,
        "yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java"
);
```

```java
Assert.assertTrue(context.markClosing());
Assert.assertFalse(context.markClosing());
Assert.assertFalse(context.commandSession().transaction().active());
```

- [ ] **Step 2: Run focused server and transaction tests to verify RED**

Run: `mvn -pl yierdis-server,yierdis-core/yierdis-core-runtime -am -Dtest=ServerConnectionContextTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,TransactionCommandTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because server code still reads and writes the session/runtime/scheduling slices directly.

- [ ] **Step 3: Introduce behavior methods on `ServerConnectionContext` and migrate executor callers**

```java
final class ServerConnectionContext {
    ServerSessionState commandSession() {
        return session();
    }

    ExecutorKeyState<NettyExecutorTask> queueState() {
        return scheduling;
    }

    boolean isClosing() {
        return runtime.isClosing();
    }

    boolean markClosing() {
        boolean changed = !runtime.isClosing();
        runtime.markClosing(session());
        return changed;
    }

    void recordCommandEnqueued(int retainedBytes) {
        runtime.pendingCounter().incrementAndGet();
        runtime.pendingBytesCounter().addAndGet(retainedBytes);
        runtime.commandsEnqueuedCounter().incrementAndGet();
    }

    void recordCommandRejected() {
        runtime.commandsRejectedCounter().incrementAndGet();
    }

    void recordCommandFinished(int retainedBytes, boolean executed) {
        if (executed) {
            runtime.commandsExecutedCounter().incrementAndGet();
        }
        runtime.pendingCounter().decrementAndGet();
        runtime.pendingBytesCounter().addAndGet(-retainedBytes);
    }

    ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                runtime.pendingCounter().get(),
                runtime.pendingBytesCounter().get(),
                runtime.autoReadDisabledByExecutor(),
                runtime.isClosing(),
                runtime.commandsEnqueuedCounter().get(),
                runtime.commandsExecutedCounter().get(),
                runtime.commandsRejectedCounter().get(),
                runtime.commandsSkippedClosingCounter().get(),
                runtime.closeAfterReplyCounter().get(),
                runtime.backpressureEnterCounter().get(),
                runtime.backpressureExitCounter().get()
        );
    }
}
```

```java
ServerConnectionContext connection = ServerConnectionContext.getOrCreate(ch);
if (connection.isClosing()) {
    executionSupport.recordSkippedClosing(ch);
    executionSupport.recycleAndRelease(task);
    return;
}
connection.recordCommandEnqueued(retainedBytes);
```

- [ ] **Step 4: Make close-after-reply, executor failure handling, and STATS read only the context API**

```java
if (connection.markClosing()) {
    backpressureController.disableAutoRead(ch);
}
```

```java
ServerConnectionContext.ConnectionStatsSnapshot stats = connection.statsSnapshot();
writePair(out, KEY_CONN_PENDING, stats.pending());
writePair(out, KEY_CONN_COMMANDS_EXECUTED, stats.commandsExecuted());
```

- [ ] **Step 5: Run focused server and transaction tests to verify GREEN**

Run: `mvn -pl yierdis-server,yierdis-core/yierdis-core-runtime -am -Dtest=ServerConnectionContextTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,TransactionCommandTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/ServerConnectionContext.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerRuntimeState.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorChannelState.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java \
        yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-server/src/test/java/yier/bubu/redis/ServerConnectionContextTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: centralize server connection behavior"
```

### Task 4: Make Runtime And Global Maxmemory Ownership Explicit

**Files:**
- Create: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceResources.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/RuntimeDbEngine.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add failing guards against RTTI-based runtime assembly and “first engine wins” maintenance**

```java
scanFileForForbiddenText(
        repoRoot,
        instanceFile,
        offenders,
        "instanceof MaxmemoryParticipant",
        "instanceof MaxmemoryCoordinatorAware"
);
scanFileForForbiddenText(
        repoRoot,
        runtimeAccessFile,
        offenders,
        "RuntimeDbEngine firstEngine",
        "firstEngine.enforceMaxmemoryMaintenance()"
);
```

- [ ] **Step 2: Run focused runtime and maxmemory tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest,GlobalMaxmemoryLruAcrossDbsTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,TtlMaxmemoryTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `YierdisInstance` still wires global maxmemory via RTTI and maintenance still relies on a first-engine convention.

- [ ] **Step 3: Make runtime engine capabilities explicit and centralize instance resource ownership**

```java
public interface RuntimeDbEngine extends DbEngine, MaxmemoryParticipant, MaxmemoryCoordinatorAware {
    void bindToCurrentThread();
    void enforceMaxmemoryMaintenance();
    void shutdown();
}
```

```java
final class YierdisInstanceResources implements AutoCloseable {
    private final RuntimeDbEngine[] dbs;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final YierdisGlobalMaxmemoryGovernor governor;

    void enforceGlobalMaintenance() {
        if (governor != null) {
            governor.prepareWrite(0);
        }
    }

    void shutdownAll() {
        Throwable failure = null;
        for (RuntimeDbEngine engine : dbs) {
            if (engine == null) {
                continue;
            }
            try {
                engine.shutdown();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                } else {
                    failure.addSuppressed(t);
                }
            }
        }
        try {
            memoryRuntime.close();
        } catch (Throwable t) {
            if (failure == null) {
                failure = t;
            } else {
                failure.addSuppressed(t);
            }
        }
        if (failure != null) {
            throw new IllegalStateException("instance resource shutdown failed", failure);
        }
    }
}
```

- [ ] **Step 4: Update `YierdisInstance` and `YierdisInstanceRuntimeAccess` to use explicit runtime capabilities**

```java
for (int i = 0; i < dbs.length; i++) {
    RuntimeDbEngine engine = dbs[i];
    participants[i] = engine;
    engine.attachMaxmemoryCoordinator(governor);
}
```

```java
if (global) {
    instance.resources().enforceGlobalMaxmemoryMaintenance();
}
```

- [ ] **Step 5: Run focused runtime and maxmemory tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest,GlobalMaxmemoryLruAcrossDbsTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,TtlMaxmemoryTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceResources.java \
        yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/RuntimeDbEngine.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: make runtime maxmemory ownership explicit"
```

### Task 5: Extract DB Resource Lifetime And Memory Ledger Ownership Out Of `YierdisDb`

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbOwnedResources.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MemoryStatsAccountingConsistencyTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`

- [ ] **Step 1: Add failing guards that `YierdisDb` no longer owns resource flags or ledger implementation details directly**

```java
scanFileForForbiddenText(
        repoRoot,
        dbFile,
        offenders,
        "private final boolean ownsOffHeapAllocator;",
        "private final boolean ownsMemoryRuntime;",
        "private final class DbMemoryLedger",
        "offHeapAllocator.close();",
        "memoryRuntime.close();"
);
```

- [ ] **Step 2: Run focused DB ownership tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,TtlMaxmemoryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `YierdisDb` still owns allocator/runtime lifetime flags, direct shutdown logic, and the memory ledger implementation.

- [ ] **Step 3: Extract owned resources and memory ledger into dedicated collaborators**

```java
final class YierdisDbOwnedResources implements AutoCloseable {
    void releaseAll(YierdisKeyspace<YierdisObject> store, YierdisExpireIndex expires) {
        store.forEach((k, e) -> e.releasePayloadIfAny());
        store.clear();
        expires.clear();
        close();
    }

    @Override
    public void close() {
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            offHeapAllocator.close();
        }
        if (ownsMemoryRuntime && memoryRuntime != null) {
            memoryRuntime.close();
        }
    }
}
```

```java
final class YierdisDbMemoryLedger implements MemoryLedger {
    long usedBytes() {
        return usedBytes;
    }

    long reservedBytes() {
        return reservedBytes;
    }

    MemoryReservation reserve(long estimatedExtraBytes) {
        if (estimatedExtraBytes < 0) {
            throw new IllegalArgumentException("estimatedExtraBytes must be >= 0");
        }
        cleanupExpired.run();
        if (limitBytes > 0 && effectiveUsedBytes() + estimatedExtraBytes > limitBytes) {
            throw new MemoryLedgerOutOfMemoryException();
        }
        reservedBytes += estimatedExtraBytes;
        return new SimpleMemoryReservation(estimatedExtraBytes);
    }
}
```

- [ ] **Step 4: Make `YierdisDb` delegate shutdown, flush, reservation, and reporting to the extracted collaborators**

```java
this.resources = new YierdisDbOwnedResources(memoryRuntime, offHeapAllocator, ownsMemoryRuntime, ownsOffHeapAllocator);
this.ledger = new YierdisDbMemoryLedger(maxmemoryBytes, this::cleanupExpired, this::evictUntilUnder, () -> maxmemoryCoordinator);
```

```java
public void shutdown() {
    threadGuard.checkThreadForShutdown();
    if (!threadGuard.tryMarkClosed()) {
        return;
    }
    resources.releaseAll(store, expires);
}
```

- [ ] **Step 5: Run focused DB ownership tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,TtlMaxmemoryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbOwnedResources.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MemoryStatsAccountingConsistencyTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java
git commit -m "refactor: extract db resource and ledger ownership"
```

### Task 6: Narrow `YierdisDb` To A Minimal Internal Seam For Data-Structure Ops

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MutationExecutorReservationTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java`

- [ ] **Step 1: Add failing guards that `YierdisDbInternals` exposes only lifecycle, mutation, and key-lifecycle primitives**

```java
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "adjustUsedBytes"));
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "refreshEstimatedBytes", KeyHandle.class, YierdisObject.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "estimateListWriteUpperBound", int.class, java.util.List.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "estimateHashWriteUpperBound", int.class, java.util.List.class));
```

- [ ] **Step 2: Run focused DB collaborator tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MutationExecutorReservationTest,DbEngineReadWriteBoundaryTest,ExpireSemanticsTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because ops classes still depend on `YierdisDb` helper leakage instead of a minimal seam.

- [ ] **Step 3: Narrow the seam and move write-estimate logic next to the ops that use it**

```java
interface YierdisDbInternals {
    void checkThread();
    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);
    YierdisDbKeyLifecycle keyLifecycle();
    MemoryLedger ledger();
}
```

```java
final class YierdisStringOps implements StringReadOps, StringWriteOps {
    private static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength)
                + Math.max(0, valueLength)
                + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }
}
```

- [ ] **Step 4: Make `YierdisDbReads` and `YierdisDbWrites` compose only the narrowed collaborators**

```java
this.reads = new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
this.writes = new YierdisDbWrites(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
```

- [ ] **Step 5: Run focused DB collaborator tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MutationExecutorReservationTest,DbEngineReadWriteBoundaryTest,ExpireSemanticsTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MutationExecutorReservationTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java
git commit -m "refactor: narrow db ops seam"
```

### Task 7: Lock Protocol And Reply Boundaries, Then Update The Docs

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-31-architecture-remediation-design.md`
- Modify: `docs/superpowers/plans/2026-04-04-architecture-refactor-roadmap.md`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add failing guardrails that keep `ReplyWriter` as the only server write semantic authority**

```java
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java"),
        offenders,
        "ReplyValue.",
        "ReplyArray(",
        "ReplyMap("
);
```

```java
Assert.assertFalse(serverSource.contains("ReplyValue."));
Assert.assertTrue(requestSource.contains("This is a protocol-layer DTO only"));
```

- [ ] **Step 2: Run focused reply and protocol boundary tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=ReplySsoTGuardTest,JsonLineReplyWriterTest,CustomProtocolResyncIntegrationTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the current guardrails do not yet prevent server-side fallback to protocol reply models or document the replacement roadmap.

- [ ] **Step 3: Tighten reply/protocol docs and tests without reopening protocol execution churn**

```java
Assert.assertTrue(source.contains("This is a protocol-layer DTO only"));
Assert.assertTrue(source.contains("server command execution write-back still uses ReplyWriter"));
```

```markdown
This roadmap is superseded by `docs/superpowers/plans/2026-04-06-architecture-remediation-replan.md`.
```

- [ ] **Step 4: Run focused reply and protocol boundary tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=ReplySsoTGuardTest,JsonLineReplyWriterTest,CustomProtocolResyncIntegrationTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add README.md \
        docs/superpowers/specs/2026-03-31-architecture-remediation-design.md \
        docs/superpowers/plans/2026-04-04-architecture-refactor-roadmap.md \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriterTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "docs: finalize architecture remediation boundaries"
```

### Cross-Wave Verification Gate

**Files:**
- Modify: `docs/superpowers/plans/2026-04-06-architecture-remediation-replan.md`

- [ ] **Step 1: Run the final focused regression gate after all seven tasks**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandMetadataRegressionTest,TransactionCommandTest,YierdisInstanceBoundaryTest,YierdisInstanceTest,ServerConnectionContextTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,GlobalMaxmemoryLruAcrossDbsTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,YierdisSnapshotTest,MemoryStatsCommandTest,TtlMaxmemoryTest,ReplySsoTGuardTest,JsonLineReplyWriterTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 2: Commit the completion marker only after the gate stays green**

```bash
git add docs/superpowers/plans/2026-04-06-architecture-remediation-replan.md
git commit -m "docs: mark architecture remediation roadmap verified"
```
