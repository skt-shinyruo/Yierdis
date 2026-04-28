# Engine Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the first Engine-Centric slice by adding `YierdisEngine` as the command execution and maintenance facade, then wiring server bootstrap and scheduled maintenance through it.

**Architecture:** This phase creates a new `yierdis-core-engine` module that depends on `yierdis-core-command` and owns construction of the existing `YierdisFastCommandProcessor`. `CommandExecutor` already accepts a neutral `CommandExecutionEngine`, so server bootstrap will pass `engine::execute`; scheduled maintenance will call `engine.maintenanceTick()`. Business session ownership, full `EngineSession`, complete `CommandSpec` migration, legacy `Command` deletion, and storage key identity normalization remain separate follow-up plans.

**Tech Stack:** Java 25, Maven, JUnit 4, existing `ExecutionRequest`, `CommandContext`, `CommandExecutor`, `YierdisFastCommandProcessor`, `YierdisInstanceMaintenance`, and architecture guard tests.

---

## Scope

This plan implements Phase 1 from `docs/superpowers/specs/2026-04-28-engine-centric-architecture-design.md`: **Introduce Engine Facade**.

This plan intentionally does not:

- move selected DB or transaction state out of `DefaultExecutionSession`;
- remove `YierdisFastCommandProcessor`;
- delete deprecated `Command`;
- convert all commands to typed `CommandSpec<T>`;
- change DB/storage key representations;
- move server-facing `HELLO`, `INFO`, or `STATS` out of `yierdis-server`.

Those are separate phases because they touch different ownership boundaries and need separate tests.

## Current Starting Point

The current code already has the executor boundary needed by this plan:

- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java`
  defines `void execute(ExecutionRequest request, CommandContext context)`.
- `CommandExecutor` accepts `CommandExecutionEngine`.
- `YierdisServerBootstrap` still constructs `YierdisFastCommandProcessor` directly and passes `processor::execute` to the executor.
- `YierdisServerBootstrap` still schedules maintenance by calling `maintenance.maintenanceTick()` inside the scheduled task.

The target after this plan:

```text
YierdisServerBootstrap
  -> creates one YierdisEngine
  -> passes engine::execute to CommandExecutor
  -> schedules engine.maintenanceTick()
```

## File Structure

### Create

- `yierdis-core/yierdis-core-engine/pom.xml`
  New core submodule for engine facade code.
- `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java`
  Public engine facade contract for this phase.
- `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java`
  Adapter that owns construction of `YierdisFastCommandProcessor` and delegates maintenance.
- `yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java`
  Unit tests proving command execution and maintenance delegation go through the engine facade.

### Modify

- `pom.xml`
  Add dependency management entry for `yierdis-core-engine`.
- `yierdis-core/pom.xml`
  Add `yierdis-core-engine` to core modules.
- `yierdis-server/pom.xml`
  Add dependency on `yierdis-core-engine`.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  Construct one `YierdisEngine`, pass `engine::execute` to executor, and call `engine.maintenanceTick()` from scheduled maintenance.
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  Update local executor construction helper to use `YierdisEngine`.
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`
  Update close-path executor test wiring to use `YierdisEngine`.
- `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`
  Update executor test wiring to use `YierdisEngine`.
- `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
  Update executor test wiring to use `YierdisEngine`.
- `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`
  Update executor test wiring to use `YierdisEngine`.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  Add guard that server bootstrap does not wire direct `YierdisFastCommandProcessor` execution or direct maintenance ticks.
- `docs/module-architecture.md`
  Document `yierdis-core-engine` as the engine facade layer.
- `docs/request-execution-flow.md`
  Update the main request flow to show `YierdisEngine` between executor and command processor.

---

## Task 1: Add Failing Engine-Centric Boundary Guard

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add the failing guard**

Insert this test method before `executorCoreMustNotDependOnCoreCommand()`:

```java
    @Test
    public void serverBootstrapMustWireCommandExecutionThroughYierdisEngine() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path engineFile = repoRoot.resolve(
                "yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java"
        );
        Assert.assertTrue(
                "缺少 YierdisEngine facade，server bootstrap 不应继续直接接线 command processor",
                Files.isRegularFile(engineFile)
        );

        Path bootstrapFile = repoRoot.getParent().resolve(
                "yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisServerBootstrap.java", Files.isRegularFile(bootstrapFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                bootstrapFile,
                offenders,
                "import yier.bubu.redis.command.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "processor::execute",
                "maintenance.maintenanceTick()"
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server bootstrap 仍绕过 YierdisEngine 直接接线 command processor 或 maintenance：\n"
                            + String.join("\n", offenders)
            );
        }
    }
```

- [ ] **Step 2: Run the guard and verify it fails**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest#serverBootstrapMustWireCommandExecutionThroughYierdisEngine test
```

Expected: FAIL. The first failure should mention missing `YierdisEngine.java`; if the file already exists locally, the failure should mention direct bootstrap snippets such as `processor::execute` or `new YierdisFastCommandProcessor(`.

- [ ] **Step 3: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "test: guard engine-centric bootstrap wiring"
```

---

## Task 2: Add `yierdis-core-engine` Module

**Files:**
- Create: `yierdis-core/yierdis-core-engine/pom.xml`
- Create: `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java`
- Create: `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java`
- Modify: `pom.xml`
- Modify: `yierdis-core/pom.xml`

- [ ] **Step 1: Add dependency management for the engine module**

In root `pom.xml`, inside `<dependencyManagement><dependencies>`, add this entry immediately after the `yierdis-core-command` dependency:

```xml
            <dependency>
                <groupId>yier.bubu.redis</groupId>
                <artifactId>yierdis-core-engine</artifactId>
                <version>${project.version}</version>
            </dependency>
```

- [ ] **Step 2: Add the core submodule**

In `yierdis-core/pom.xml`, replace:

```xml
        <module>yierdis-core-command</module>
        <module>yierdis-core-runtime</module>
```

with:

```xml
        <module>yierdis-core-command</module>
        <module>yierdis-core-engine</module>
        <module>yierdis-core-runtime</module>
```

- [ ] **Step 3: Create the module POM**

Create `yierdis-core/yierdis-core-engine/pom.xml` with this content:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-core</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>yierdis-core-engine</artifactId>
    <packaging>jar</packaging>

    <name>yierdis-core-engine</name>
    <description>Engine facade and execution kernel wiring for Yierdis core.</description>

    <dependencies>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-core-contract</artifactId>
        </dependency>

        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-core-command</artifactId>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Create `YierdisEngine`**

Create `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java` with this content:

```java
package yier.bubu.redis.engine;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

/**
 * Engine facade for command execution and owner-thread maintenance.
 *
 * This phase keeps the existing CommandContext shape so executor-core can keep
 * using its transport-neutral CommandExecutionEngine seam. Later phases will
 * move business session state behind an engine-owned session type.
 */
public interface YierdisEngine extends AutoCloseable {
    void execute(ExecutionRequest request, CommandContext context);

    void maintenanceTick();

    @Override
    default void close() {
    }
}
```

- [ ] **Step 5: Create `DefaultYierdisEngine`**

Create `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java` with this content:

```java
package yier.bubu.redis.engine;

import yier.bubu.redis.command.CommandModule;
import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

/**
 * Default engine facade backed by the current command processor.
 */
public final class DefaultYierdisEngine implements YierdisEngine {
    private final YierdisFastCommandProcessor commandProcessor;
    private final Runnable maintenanceTick;

    public DefaultYierdisEngine(
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor,
            Runnable maintenanceTick,
            CommandModule... extraModules
    ) {
        this(
                new YierdisFastCommandProcessor(
                        Objects.requireNonNull(dbRouter, "dbRouter"),
                        infoProvider,
                        slowGovernor,
                        extraModules
                ),
                maintenanceTick
        );
    }

    DefaultYierdisEngine(YierdisFastCommandProcessor commandProcessor, Runnable maintenanceTick) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.maintenanceTick = Objects.requireNonNull(maintenanceTick, "maintenanceTick");
    }

    @Override
    public void execute(ExecutionRequest request, CommandContext context) {
        commandProcessor.execute(request, context);
    }

    @Override
    public void maintenanceTick() {
        maintenanceTick.run();
    }
}
```

- [ ] **Step 6: Compile the new module**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-engine -am -Dmaven.test.skip=true test
```

Expected: PASS. This verifies the new module participates in the Maven reactor and compiles with its upstream dependencies.

- [ ] **Step 7: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add pom.xml \
  yierdis-core/pom.xml \
  yierdis-core/yierdis-core-engine/pom.xml \
  yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java \
  yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java
git commit -m "feat: add core engine facade module"
```

---

## Task 3: Add Engine Facade Unit Tests

**Files:**
- Create: `yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java`

- [ ] **Step 1: Create tests for command execution and maintenance delegation**

Create `yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java` with this content:

```java
package yier.bubu.redis.engine;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.CommandDescriptor;
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.DbIndexProvider;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MemoryOps;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultYierdisEngineTest {
    @Test
    public void executeDelegatesThroughOwnedCommandProcessor() {
        YierdisEngine engine = new DefaultYierdisEngine(
                singleDbRouter(new NoopDbEngine()),
                null,
                SlowCommandGovernor.DEFAULT,
                () -> {
                },
                registration -> registration.register(
                        "LOCAL",
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK"),
                        CommandDescriptor.of(1, 0, 0, 0)
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        engine.execute(
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                new CommandContext(null, out)
        );

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }

    @Test
    public void maintenanceTickDelegatesToOwnerThreadRuntimeHook() {
        AtomicInteger ticks = new AtomicInteger();
        YierdisEngine engine = new DefaultYierdisEngine(
                singleDbRouter(new NoopDbEngine()),
                null,
                SlowCommandGovernor.DEFAULT,
                ticks::incrementAndGet
        );

        engine.maintenanceTick();
        engine.maintenanceTick();

        Assert.assertEquals(2, ticks.get());
    }

    private static YierdisDbRouter singleDbRouter(DbEngine engine) {
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexProvider dbIndexProvider) {
                return engine;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }

    private static final class NoopDbEngine implements DbEngine {
        @Override
        public DbReads reads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbWrites writes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpirationManager expiration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryOps memory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbLifecycleOps lifecycle() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CapturingReplyWriter implements ReplyWriter {
        private String simpleStringValue;
        private String errorValue;

        @Override
        public void requestCloseAfterReply() {
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return false;
        }

        @Override
        public void simpleString(String value) {
            this.simpleStringValue = value;
        }

        @Override
        public void error(String message) {
            this.errorValue = message;
        }

        @Override
        public void integer(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void booleanValue(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doubleValue(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bigNumberAscii(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void blobError(String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nullArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void arrayHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkStringArray(List<byte[]> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emptyArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mapHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pushHeader(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void attributeHeader(int pairs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkString(BytesSlice slice) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkStringLongAscii(long value) {
            throw new UnsupportedOperationException();
        }
    }
}
```

- [ ] **Step 2: Run the engine tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-engine -Dtest=DefaultYierdisEngineTest test
```

Expected: PASS. The first test proves execution enters through `YierdisEngine`; the second proves maintenance can be called through the engine facade.

- [ ] **Step 3: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java
git commit -m "test: cover default yierdis engine facade"
```

---

## Task 4: Wire Server Bootstrap Through `YierdisEngine`

**Files:**
- Modify: `yierdis-server/pom.xml`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

- [ ] **Step 1: Add the server dependency**

In `yierdis-server/pom.xml`, add this dependency immediately after the `yierdis-core-command` dependency:

```xml
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-core-engine</artifactId>
        </dependency>
```

- [ ] **Step 2: Update imports in `YierdisServerBootstrap`**

In `YierdisServerBootstrap.java`, remove:

```java
import yier.bubu.redis.command.YierdisFastCommandProcessor;
```

Add:

```java
import yier.bubu.redis.engine.DefaultYierdisEngine;
import yier.bubu.redis.engine.YierdisEngine;
```

- [ ] **Step 3: Add an engine field**

In the core resources section of `YierdisServerBootstrap`, replace:

```java
    private YierdisInstance instance;
    private CommandExecutor<NettyExecutionConnection> executor;
    private NettyServerInfoProvider infoProvider;
```

with:

```java
    private YierdisInstance instance;
    private YierdisEngine engine;
    private CommandExecutor<NettyExecutionConnection> executor;
    private NettyServerInfoProvider infoProvider;
```

- [ ] **Step 4: Replace direct processor construction with engine construction**

In `startInternal()`, replace:

```java
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                dbRouter(instance),
                infoProvider,
                slowGovernor,
                new ServerCommandModule(infoProvider)
        );
```

with:

```java
        YierdisEngine commandEngine = new DefaultYierdisEngine(
                dbRouter(instance),
                infoProvider,
                slowGovernor,
                new YierdisInstanceMaintenance(instance)::maintenanceTick,
                new ServerCommandModule(infoProvider)
        );
        engine = commandEngine;
```

- [ ] **Step 5: Pass `engine::execute` to the executor**

In the `CommandExecutor` construction, replace:

```java
                processor::execute,
```

with:

```java
                commandEngine::execute,
```

The surrounding block should read:

```java
        executor = new CommandExecutor<>(
                runtimeAccess::bindToCurrentThread,
                commandEngine::execute,
                commandGroup.next(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                executorConfig
        );
```

- [ ] **Step 6: Route scheduled maintenance through the engine**

In the cleanup scheduling block, replace:

```java
                        maintenance.maintenanceTick();
```

with:

```java
                        commandEngine.maintenanceTick();
```

Do not change the existing `cleanupPending` coalescing behavior or the `executeMaintenance(...)` call. Maintenance must still run on the command executor owner thread.

- [ ] **Step 7: Close the engine facade before runtime access closes**

In `close()`, after executor shutdown and before `YierdisInstance inst = instance;`, insert:

```java
        YierdisEngine eng = engine;
        if (eng != null) {
            try {
                eng.close();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        engine = null;
```

Then keep the existing runtime close flow unchanged:

```java
        YierdisInstance inst = instance;
        if (inst != null) {
            try {
                closeRuntimeAccess(ex, inst.runtimeAccess());
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
```

- [ ] **Step 8: Compile server production code**

Run:

```bash
mvn -pl yierdis-server -am -Dmaven.test.skip=true test
```

Expected: PASS. This verifies server production code sees the new engine module and no longer needs direct `YierdisFastCommandProcessor` wiring in bootstrap.

- [ ] **Step 9: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add yierdis-server/pom.xml \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java
git commit -m "refactor: wire server bootstrap through yierdis engine"
```

---

## Task 5: Update Server Test Wiring To Use Engine Facade

**Files:**
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`

- [ ] **Step 1: Replace imports in each listed test file**

In each listed file, remove:

```java
import yier.bubu.redis.command.YierdisFastCommandProcessor;
```

Add these imports if they are not already present:

```java
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.engine.DefaultYierdisEngine;
import yier.bubu.redis.engine.YierdisEngine;
```

- [ ] **Step 2: Replace direct processor wiring with engine wiring**

For each local executor construction currently shaped like:

```java
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    processor::execute,
                    commandGroup.next(),
                    new yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory(),
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
```

replace the processor line and executor argument with:

```java
            YierdisEngine engine = new DefaultYierdisEngine(
                    TestDbRouters.forInstance(instance),
                    null,
                    SlowCommandGovernor.DEFAULT,
                    () -> {
                    }
            );
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    commandGroup.next(),
                    new yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory(),
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
```

Preserve each test's existing `CommandExecutorConfig` values. Only replace the processor construction and the executor execution argument.

- [ ] **Step 3: Update `InitializerTestEnv` in `YierdisServerBootstrapCommandWiringTest`**

In `InitializerTestEnv`, replace:

```java
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            this.replyWriterFactory = new JsonLineReplyWriterFactory();
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    processor::execute,
                    ImmediateEventExecutor.INSTANCE,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 1024, 10, SchedulingPolicy.FAIR)
            );
```

with:

```java
            YierdisEngine engine = new DefaultYierdisEngine(
                    TestDbRouters.forInstance(instance),
                    null,
                    SlowCommandGovernor.DEFAULT,
                    () -> {
                    }
            );
            this.replyWriterFactory = new JsonLineReplyWriterFactory();
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    ImmediateEventExecutor.INSTANCE,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 1024, 10, SchedulingPolicy.FAIR)
            );
```

- [ ] **Step 4: Run the affected server tests**

Run:

```bash
mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,NettyExecutionAdapterIntegrationTest test
```

Expected: PASS. Existing server behavior should remain unchanged because `DefaultYierdisEngine.execute(...)` delegates to the same command processor path.

- [ ] **Step 5: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java
git commit -m "test: wire server executor tests through yierdis engine"
```

---

## Task 6: Update Docs For The New Request Flow

**Files:**
- Modify: `docs/module-architecture.md`
- Modify: `docs/request-execution-flow.md`

- [ ] **Step 1: Update module architecture summary**

In `docs/module-architecture.md`, replace the one-sentence core summary near the end:

```markdown
- core-contract / core-api 负责执行契约和 DB 能力边界
- core-db 负责真实存储
- core-runtime 负责实例生命周期
- server 负责最后的组装
```

with:

```markdown
- core-contract / core-api 负责执行契约和 DB 能力边界
- core-db 负责真实存储
- core-command 负责命令注册、解析、事务语义和分发细节
- core-engine 负责把命令执行和 owner-thread maintenance 收敛成 `YierdisEngine`
- core-runtime 负责实例生命周期
- server 负责协议、Netty 和最后的进程级组装
```

- [ ] **Step 2: Update request execution flow main chain**

In `docs/request-execution-flow.md`, replace the first 8-step chain:

```markdown
1. `YierdisServer` 启动 server
2. `YierdisServerBootstrap` 组装 `YierdisInstance`、`YierdisFastCommandProcessor`、`NettyCommandExecutor`
3. `YierdisServerChannelInitializer` 为每个连接建立 Netty pipeline 和连接态
4. `CustomRequestDecoder` 把网络帧解成协议请求对象
5. `ProtocolCommandAdapter` 把协议请求适配为 `ExecutionRequest`
6. `YierdisFastCommandHandler` 把 `ExecutionRequest` 提交给 command executor
7. `NettyCommandDrainLoop` 在单线程 executor 上串行执行命令
8. `YierdisFastCommandProcessor` 分发到具体命令处理器，并通过 `ReplyWriter` 写回
```

with:

```markdown
1. `YierdisServer` 启动 server
2. `YierdisServerBootstrap` 组装 `YierdisInstance`、`YierdisEngine`、`CommandExecutor`
3. `YierdisServerChannelInitializer` 为每个连接建立 Netty pipeline 和连接态
4. `CustomRequestDecoder` 把网络帧解成协议请求对象
5. `ProtocolCommandAdapter` 把协议请求适配为 `ExecutionRequest`
6. `YierdisFastCommandHandler` 把 `ExecutionRequest` 提交给 command executor
7. `CommandExecutorDrainLoop` 在单线程 executor 上串行调度命令
8. `YierdisEngine` 接收执行请求，内部委托当前 command processor 分发到具体命令处理器，并通过 `ReplyWriter` 写回
```

- [ ] **Step 3: Update the drain-loop execution list**

In `docs/request-execution-flow.md`, replace this item:

```markdown
5. 调用 `YierdisFastCommandProcessor.execute(...)`
```

with:

```markdown
5. 调用 `YierdisEngine.execute(...)`
```

- [ ] **Step 4: Check docs for stale direct-flow names**

Run:

```bash
rg -n "YierdisFastCommandProcessor|NettyCommandExecutor|NettyCommandDrainLoop|NettyCommandExecutionSupport" docs/request-execution-flow.md docs/module-architecture.md
```

Expected: Any remaining matches must be intentional historical explanation. If a match describes the current main request path, update it to `YierdisEngine`, `CommandExecutor`, `CommandExecutorDrainLoop`, or `CommandExecutorExecutionSupport`.

- [ ] **Step 5: Checkpoint**

Do not commit unless the user explicitly requested commits for implementation work. If commits are requested, use:

```bash
git add docs/module-architecture.md docs/request-execution-flow.md
git commit -m "docs: describe engine-centric request flow"
```

---

## Task 7: Verify Guards And End-To-End Behavior

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Re-run the engine-centric guard**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest#serverBootstrapMustWireCommandExecutionThroughYierdisEngine test
```

Expected: PASS. This proves bootstrap no longer wires direct `YierdisFastCommandProcessor` execution or direct `maintenance.maintenanceTick()` calls.

- [ ] **Step 2: Run all architecture boundary tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest test
```

Expected: PASS. This catches accidental regressions in runtime, protocol, executor, and server ownership boundaries.

- [ ] **Step 3: Run the focused engine and server suites**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-engine,yierdis-server -am -Dtest=DefaultYierdisEngineTest,YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest,ClosingSkipSideEffectsIntegrationTest,CustomProtocolResyncIntegrationTest,NettyExecutionAdapterIntegrationTest test
```

Expected: PASS. This proves the new module, bootstrap wiring, close path, protocol resync, and Netty execution adapter still work.

- [ ] **Step 4: Run full verification**

Run:

```bash
mvn test
```

Expected: PASS. Do not claim completion if any module fails.

- [ ] **Step 5: Final implementation commit**

Only after all verification commands pass, commit the complete Phase 1 change:

```bash
git add pom.xml \
  yierdis-core/pom.xml \
  yierdis-core/yierdis-core-engine/pom.xml \
  yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java \
  yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java \
  yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java \
  yierdis-server/pom.xml \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  docs/module-architecture.md \
  docs/request-execution-flow.md
git commit -m "refactor: introduce yierdis engine facade"
```

---

## Follow-Up Plans

After this phase lands, create separate implementation plans in this order:

1. `EngineSession` ownership migration: move selected DB and transaction state out of executor-owned session types.
2. Complete `CommandSpec<T>` migration: remove legacy `CommandModule.Handler` registration surfaces.
3. Remove deprecated request compatibility: delete `Command` execution overload and `CommandSupport` compatibility checks.
4. Storage key identity normalization: move TTL and maxmemory hot paths to `KeyHandle` or opaque key refs.
5. Stronger architecture guards: prevent future command parsing or transaction ownership from moving into server/executor/runtime/storage.

Each follow-up plan should have its own failing guard, focused unit tests, and full verification command.
