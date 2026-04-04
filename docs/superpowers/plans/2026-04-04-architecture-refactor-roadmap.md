# Architecture Refactor Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the remaining Yierdis design debt in command registration, runtime/server seams, and `core-db` ownership without changing protocol behavior or command semantics.

**Architecture:** Execute the work in four shippable waves. Wave 1 makes command registration the single source of truth for metadata and MULTI policy. Wave 2 moves runtime observability and maintenance policy behind `core-runtime` seams so `yierdis-server` stops reconstructing DB/runtime knowledge. Waves 3 and 4 shrink `YierdisDb` by narrowing the internal seam and extracting introspection/accounting responsibilities from the current state owner.

**Tech Stack:** Java 25, Maven multi-module reactor, Netty 4.1, JUnit 4

---

### Task 1: Make Command Registration The Single Source Of Truth

**Files:**
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDescriptor.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Tighten tests so metadata and MULTI policy must come from registry specs**

```java
public record CommandSpec(
        CommandModule.Handler handler,
        CommandDescriptor descriptor,
        String disallowedInMultiError
) {
    public boolean allowedInMulti() {
        return disallowedInMultiError == null;
    }
}
```

```java
Assert.assertEquals("ERR HELLO is not allowed in MULTI", registry.spec("HELLO").disallowedInMultiError());
Assert.assertEquals(-1, registry.spec("INFO").descriptor().arity());
Assert.assertNull(registry.spec("SET").disallowedInMultiError());
```

- [ ] **Step 2: Add guardrails that forbid fallback metadata tables in `core-command`**

```java
scanFileForForbiddenText(
        repoRoot,
        descriptorFile,
        offenders,
        "switch (nameUpper)",
        "case \"PING\":",
        "case \"INFO\":",
        "case \"STATS\":"
);
```

- [ ] **Step 3: Run focused command and boundary tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandMetadataRegressionTest,TransactionCommandTest,CommandRegistryGuardTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because registry entries still split handler/descriptor/MULTI policy across different structures and fallback metadata code still exists.

- [ ] **Step 4: Introduce `CommandSpec` and migrate all registrations to it**

```java
public interface Registration {
    void register(String name, CommandSpec spec);

    default void register(String name, CommandModule.Handler handler, CommandDescriptor descriptor) {
        register(name, new CommandSpec(handler, descriptor, null));
    }
}
```

```java
registry.register(
        "HELLO",
        new CommandSpec(this::hello, CommandDescriptor.of(-1, 0, 0, 0), "ERR HELLO is not allowed in MULTI")
);
```

- [ ] **Step 5: Delete descriptor fallback switches and make `COMMAND` read only registry-owned specs**

```java
CommandSpec spec = registry.spec(upper);
if (spec == null) {
    out.nullArray();
    return;
}
writeCommandInfo(out, upper, spec.descriptor());
```

- [ ] **Step 6: Run focused command and boundary tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=CommandDescriptorRegistryTest,CommandMetadataRegressionTest,TransactionCommandTest,CommandRegistryGuardTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDescriptor.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: unify command spec authority"
```

### Task 2: Move Runtime Observability And Maintenance Behind `core-runtime`

**Files:**
- Create: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Add failing guards for the two current leaks**

```java
scanFileForForbiddenText(
        repoRoot,
        bootstrapFile,
        offenders,
        "runtimeAccess.maintenanceTick()"
);
scanFileForForbiddenText(
        repoRoot,
        infoProviderFile,
        offenders,
        "for (int i = 0; i < local.length; i++)",
        "db.memory().memoryStats()"
);
```

- [ ] **Step 2: Run focused runtime and server tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisInstanceTest,MemoryStatsCommandTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because server still drives maintenance through `runtimeAccess` directly and still reconstructs global memory stats by iterating engine views.

- [ ] **Step 3: Add explicit runtime observability and use the existing maintenance seam**

```java
public final class YierdisInstanceObservability {
    private final YierdisInstance instance;

    public YierdisMemoryStats globalMemoryStats() {
        return DbMemoryAccounting.snapshotInstance(instance);
    }
}
```

```java
YierdisInstanceMaintenance maintenance = new YierdisInstanceMaintenance(instance);
cleanupFuture = workerGroup.next().scheduleWithFixedDelay(
        () -> exForTask.executeMaintenance(maintenance::maintenanceTick),
        period,
        period,
        TimeUnit.MILLISECONDS
);
```

- [ ] **Step 4: Make `NettyServerInfoProvider` depend on runtime observability instead of raw `DbEngine[]` aggregation**

```java
final class NettyServerInfoProvider implements ServerInfoProvider {
    private volatile YierdisInstanceObservability observability;

    @Override
    public YierdisMemoryStats memoryStats(CommandContext ctx) {
        return observability == null ? null : observability.globalMemoryStats();
    }
}
```

- [ ] **Step 5: Run focused runtime and server tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisInstanceTest,MemoryStatsCommandTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceObservability.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceBoundaryTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: expose runtime observability seam"
```

### Task 3: Narrow `YierdisDbInternals` Around Key Lifecycle And Mutation

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
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
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java`

- [ ] **Step 1: Write failing guards that `YierdisDbInternals` no longer exposes raw containers**

```java
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "store"));
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "expires"));
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "offHeapAllocator"));
Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "memoryRuntime"));
```

- [ ] **Step 2: Run focused DB and command tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MutationExecutorReservationTest,ExpireSemanticsTest,DbEngineReadWriteBoundaryTest,HashCommandTest,ListCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because data-structure ops still depend on a wide internal seam that exposes raw DB containers and low-level memory objects.

- [ ] **Step 3: Introduce a focused key-lifecycle collaborator and reduce the internals contract**

```java
final class YierdisDbKeyLifecycle {
    YierdisObject getLiveObject(KeyHandle keyHandle, boolean touch) { ... }
    boolean expireIfNeeded(KeyHandle keyHandle, YierdisObject object, long nowMillis) { ... }
    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) { ... }
    void removeExpire(KeyHandle keyHandle) { ... }
}
```

```java
interface YierdisDbInternals {
    <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan);
    YierdisDbKeyLifecycle keyLifecycle();
    void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object);
}
```

- [ ] **Step 4: Migrate all ops classes to the narrowed seam**

```java
YierdisObject existing = internals.keyLifecycle().getLiveObject(handle, true);
return internals.executeMutation(plan);
```

- [ ] **Step 5: Run focused DB and command tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MutationExecutorReservationTest,ExpireSemanticsTest,DbEngineReadWriteBoundaryTest,HashCommandTest,ListCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbInternals.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
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
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java
git commit -m "refactor: narrow db internals seam"
```

### Task 4: Extract Introspection And Accounting Out Of `YierdisDb`

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbIntrospection.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MemoryStatsAccountingConsistencyTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisSnapshotTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`

- [ ] **Step 1: Add failing guards for methods that should leave `YierdisDb`**

```java
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "memoryStats"));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "memoryUsage", yier.bubu.redis.bytes.BytesView.class));
Assert.assertNull(findDeclaredMethod(YierdisDb.class, "snapshot", yier.bubu.redis.ops.ScanCursorV2.class, int.class, java.util.List.class));
```

- [ ] **Step 2: Run focused DB introspection tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,YierdisSnapshotTest,MemoryStatsCommandTest,TtlMaxmemoryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `YierdisDb` still owns memory reporting, snapshot scanning, encoding naming, and maxmemory introspection helpers directly.

- [ ] **Step 3: Extract dedicated collaborators and delegate from façades**

```java
final class YierdisDbMemoryReporter {
    YierdisMemoryStats snapshot() { ... }
    long usedBytesForMaxmemory() { ... }
    long memoryUsage(BytesView keyView) { ... }
}
```

```java
final class YierdisDbIntrospection {
    ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out) { ... }
    String encodingName(byte[] keyBytes) { ... }
}
```

- [ ] **Step 4: Run focused DB introspection tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,YierdisSnapshotTest,MemoryStatsCommandTest,TtlMaxmemoryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbIntrospection.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MemoryStatsAccountingConsistencyTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisSnapshotTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java
git commit -m "refactor: extract db introspection and accounting"
```

### Task 5: Final Hardening And Explicitly Defer Protocol Churn

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-31-architecture-remediation-design.md`
- Modify: `docs/superpowers/plans/2026-04-04-architecture-refactor-roadmap.md`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`

- [ ] **Step 1: Document the new priority order and explicitly defer protocol rewrites**

```markdown
- Protocol request adaptation is already mostly decoupled through `CustomProtocolV1Request` + `ProtocolCommandAdapter`.
- Do not reopen protocol-layer rewrites while command-spec SSOT and `YierdisDb` shrink work are still in flight.
```

- [ ] **Step 2: Tighten reply SSOT guard text so future changes do not reintroduce `ReplyValue` authority drift**

```java
Assert.assertFalse(source.contains("server reply authority"));
Assert.assertFalse(source.contains("command-layer reply model"));
```

- [ ] **Step 3: Run the full focused regression bundle**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=ArchitectureBoundaryTest,CoreCommandBoundaryGuardTest,ReplySsoTGuardTest,YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,CommandDescriptorRegistryTest,CommandMetadataRegressionTest,YierdisDbArchitectureGuardTest,MemoryStatsAccountingConsistencyTest,YierdisSnapshotTest,YierdisServerBootstrapCommandWiringTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add README.md \
        docs/superpowers/specs/2026-03-31-architecture-remediation-design.md \
        docs/superpowers/plans/2026-04-04-architecture-refactor-roadmap.md \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java
git commit -m "docs: finalize architecture refactor roadmap"
```

## Notes

- `ProtocolCommandAdapter` and `ReplySsoTGuardTest` show that a large part of the old protocol/reply drift has already been corrected. Do not restart a protocol-layer redesign before Tasks 1-4 are complete.
- `YierdisDb` is still the highest-density object in the repo. If time is limited, complete Tasks 1 and 3 before anything else.
- Keep commits boundary-scoped. Do not mix command-spec work with DB extraction in the same commit.
