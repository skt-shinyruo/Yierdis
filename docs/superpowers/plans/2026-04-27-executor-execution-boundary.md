# Executor Execution Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `yierdis-executor-core`'s direct dependency on `yierdis-core-command` by introducing a narrow execution interface and adapting concrete command processors at composition roots.

**Architecture:** `yierdis-executor-core` will own scheduling, backpressure, drain, session, and reply lifecycle while depending only on `yierdis-core-contract` for request/context/reply contracts. `yierdis-server` and tests will adapt `YierdisFastCommandProcessor` through `processor::execute`, and executor-core tests will use a tiny local `CommandExecutionEngine` test double instead of constructing a real DB and command processor.

**Tech Stack:** Java 25, Maven, JUnit 4, existing `ExecutionRequest`, `CommandContext`, `ReplyWriterFactory`, and `CommandExecutor` abstractions

---

## File Structure

### Create

- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java`
  Narrow command execution interface used by executor-core.

### Modify

- `yierdis-executor-core/pom.xml`
  Remove compile dependency on `yierdis-core-command` and test dependency on `yierdis-core-db` after executor tests stop constructing real command processors.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
  Accept `CommandExecutionEngine` instead of `YierdisFastCommandProcessor`.
- `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
  Store and invoke `CommandExecutionEngine`.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java`
  Replace `ProcessorHandle` with a local engine test double that supports `PING` and `QUIT`.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`
  Construct `CommandExecutor` with the local engine test double.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java`
  Construct `CommandExecutor` with the local engine test double.
- `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java`
  Construct `CommandExecutor` with the local engine test double.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  Pass `processor::execute` into `CommandExecutor`.
- `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`
  Pass `processor::execute` into `CommandExecutor` test constructions.
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  Pass `processor::execute` into initializer test executor construction.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  Add a guard that executor-core does not depend on `yierdis-core-command` or import `yier.bubu.redis.command.*`.

---

## Task 1: Add Failing Executor-Core Boundary Guard

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add the failing architecture test**

Insert this test method before the existing `executorCoreMustNotDependOnNetty()` test:

```java
    @Test
    public void executorCoreMustNotDependOnCoreCommand() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path executorRoot = repoRoot.getParent().resolve("yierdis-executor-core").normalize();
        Path executorPom = executorRoot.resolve("pom.xml");
        Assert.assertTrue("缺少 yierdis-executor-core/pom.xml", Files.isRegularFile(executorPom));

        String pom = Files.readString(executorPom, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "yierdis-executor-core must not depend on yierdis-core-command",
                pom.contains("<artifactId>yierdis-core-command</artifactId>")
        );

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                executorRoot.resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.command.",
                "yier.bubu.redis.command."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-executor-core Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-executor-core 依赖 core-command（executor-core 应只依赖执行契约）：\n"
                            + String.join("\n", offenders)
            );
        }
    }
```

- [ ] **Step 2: Run the new guard and verify it fails**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest#executorCoreMustNotDependOnCoreCommand test
```

Expected: FAIL. The failure should mention `yierdis-core-command` in `yierdis-executor-core/pom.xml` or imports of `yier.bubu.redis.command.*` in executor-core production source.

- [ ] **Step 3: Checkpoint**

Do not commit unless the user explicitly requested commits for this implementation. If commits are requested, use:

```bash
git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "test: guard executor core command boundary"
```

---

## Task 2: Introduce CommandExecutionEngine And Rewire Production Executor

**Files:**
- Create: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`

- [ ] **Step 1: Create the narrow execution interface**

Create `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java` with this content:

```java
package yier.bubu.redis.executor;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

/**
 * Transport-neutral command execution boundary used by executor-core.
 */
@FunctionalInterface
public interface CommandExecutionEngine {
    void execute(ExecutionRequest request, CommandContext context);
}
```

- [ ] **Step 2: Update `CommandExecutor` to accept the interface**

In `CommandExecutor.java`, remove this import:

```java
import yier.bubu.redis.command.YierdisFastCommandProcessor;
```

Replace the constructor parameter:

```java
            YierdisFastCommandProcessor commandProcessor,
```

with:

```java
            CommandExecutionEngine commandProcessor,
```

No import is needed for `CommandExecutionEngine` because it is in the same package.

- [ ] **Step 3: Update `CommandExecutorExecutionSupport` to use the interface**

In `CommandExecutorExecutionSupport.java`, remove this import:

```java
import yier.bubu.redis.command.YierdisFastCommandProcessor;
```

Replace the field:

```java
    private final YierdisFastCommandProcessor commandProcessor;
```

with:

```java
    private final CommandExecutionEngine commandProcessor;
```

Replace the constructor parameter:

```java
            YierdisFastCommandProcessor commandProcessor,
```

with:

```java
            CommandExecutionEngine commandProcessor,
```

Keep the existing execution call unchanged:

```java
            commandProcessor.execute(task.request, context(context.session(), writer));
```

- [ ] **Step 4: Verify production source compiles without compiling tests**

Run:

```bash
jdk25 mvn -pl yierdis-executor-core -Dmaven.test.skip=true test
```

Expected: PASS for executor-core production compilation. Executor-core tests are not compiled in this command because they still reference `ProcessorHandle` and will be fixed in Task 3.

- [ ] **Step 5: Checkpoint**

Do not commit unless the user explicitly requested commits for this implementation. If commits are requested, use:

```bash
git add yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutor.java \
  yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java
git commit -m "refactor: decouple executor from command processor"
```

---

## Task 3: Replace Executor-Core Tests With A Local Execution Engine

**Files:**
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java`
- Modify: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-executor-core/pom.xml`

- [ ] **Step 1: Remove real DB and command processor imports from test support**

In `ExecutorCoreTestSupport.java`, remove these imports:

```java
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
```

- [ ] **Step 2: Replace `processorHandle()` with a simple test engine**

In `ExecutorCoreTestSupport.java`, replace this method:

```java
    static ProcessorHandle processorHandle() {
        return new ProcessorHandle(new YierdisDb());
    }
```

with:

```java
    static CommandExecutionEngine simpleCommandEngine() {
        return (request, ctx) -> {
            if (asciiEqualsIgnoreCase(request, 0, "PING")) {
                ctx.out().simpleString("PONG");
                return;
            }
            if (asciiEqualsIgnoreCase(request, 0, "QUIT")) {
                ctx.out().simpleString("OK");
                ctx.out().requestCloseAfterReply();
                return;
            }
            ctx.out().error("ERR unsupported test command");
        };
    }

    private static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int index, String expectedUpperAscii) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(expectedUpperAscii, "expectedUpperAscii");
        if (request.isNull(index) || request.len(index) != expectedUpperAscii.length()) {
            return false;
        }
        for (int i = 0; i < expectedUpperAscii.length(); i++) {
            int actual = request.byteAt(index, i) & 0xFF;
            if (actual >= 'a' && actual <= 'z') {
                actual -= 32;
            }
            if (actual != expectedUpperAscii.charAt(i)) {
                return false;
            }
        }
        return true;
    }
```

- [ ] **Step 3: Delete `ProcessorHandle` from test support**

Delete this class from `ExecutorCoreTestSupport.java`:

```java
final class ProcessorHandle implements AutoCloseable {
    private final YierdisDb db;
    private final YierdisFastCommandProcessor processor;

    ProcessorHandle(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
        this.processor = new YierdisFastCommandProcessor(db);
    }

    YierdisFastCommandProcessor processor() {
        return processor;
    }

    @Override
    public void close() {
        db.shutdown();
    }
}
```

- [ ] **Step 4: Update `CommandExecutorTest` constructors**

For each test in `CommandExecutorTest.java` that currently starts with this pattern:

```java
        try (ProcessorHandle handle = ExecutorCoreTestSupport.processorHandle()) {
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> {},
                    handle.processor(),
```

replace it with this pattern:

```java
        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
```

Remove the matching closing brace for the deleted try-with-resources block. Keep the existing `executor.close()` calls in the tests that already have them.

For the `startWaitsForOwnerThreadBindingBeforeReturning` test, replace:

```java
        try (ProcessorHandle handle = ExecutorCoreTestSupport.processorHandle()) {
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> {
                        bindStarted.countDown();
                        try {
                            Assert.assertTrue(releaseBind.await(1, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    handle.processor(),
```

with:

```java
        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {
                    bindStarted.countDown();
                    try {
                        Assert.assertTrue(releaseBind.await(1, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                engine,
```

Remove the matching closing brace for the deleted try-with-resources block.

- [ ] **Step 5: Update backpressure and fair scheduling tests**

In `CommandExecutorBackpressureTest.java`, replace:

```java
        try (ProcessorHandle handle = ExecutorCoreTestSupport.processorHandle()) {
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> {},
                    handle.processor(),
```

with:

```java
        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
```

Remove the matching closing brace for the deleted try-with-resources block.

In `CommandExecutorFairSchedulingTest.java`, replace:

```java
        try (ProcessorHandle handle = ExecutorCoreTestSupport.processorHandle()) {
            CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                    () -> {},
                    handle.processor(),
```

with:

```java
        CommandExecutionEngine engine = ExecutorCoreTestSupport.simpleCommandEngine();
        CommandExecutor<TestConnection> executor = new CommandExecutor<>(
                () -> {},
                engine,
```

Remove the matching closing brace for the deleted try-with-resources block.

- [ ] **Step 6: Remove executor-core's command and DB dependencies**

In `yierdis-executor-core/pom.xml`, replace the full `<dependencies>` section with:

```xml
    <dependencies>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-core-contract</artifactId>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
```

- [ ] **Step 7: Run executor-core tests**

Run:

```bash
jdk25 mvn -pl yierdis-executor-core test
```

Expected: PASS. The test output should include successful execution of `CommandExecutorTest`, `CommandExecutorBackpressureTest`, `CommandExecutorFairSchedulingTest`, and `ExecutionConnectionContextTest`.

- [ ] **Step 8: Run the boundary guard again**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest#executorCoreMustNotDependOnCoreCommand test
```

Expected: PASS. The guard should no longer find `yierdis-core-command` in the executor-core POM or `yier.bubu.redis.command.*` imports in executor-core production source.

- [ ] **Step 9: Checkpoint**

Do not commit unless the user explicitly requested commits for this implementation. If commits are requested, use:

```bash
git add yierdis-executor-core/pom.xml \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutorCoreTestSupport.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorTest.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorBackpressureTest.java \
  yierdis-executor-core/src/test/java/yier/bubu/redis/executor/CommandExecutorFairSchedulingTest.java
git commit -m "test: use executor-local command engine"
```

---

## Task 4: Adapt Server Wiring To The New Executor Boundary

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Update bootstrap construction**

In `YierdisServerBootstrap.java`, replace this `CommandExecutor` constructor argument:

```java
                processor,
```

with:

```java
                processor::execute,
```

The surrounding constructor should read:

```java
        executor = new CommandExecutor<>(
                runtimeAccess::bindToCurrentThread,
                processor::execute,
                commandGroup.next(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                executorConfig
        );
```

- [ ] **Step 2: Update Netty adapter integration tests**

In `NettyExecutionAdapterIntegrationTest.java`, replace each `CommandExecutor` constructor argument:

```java
                    processor,
```

with:

```java
                    processor::execute,
```

There are three constructor call sites in this file.

- [ ] **Step 3: Update bootstrap wiring test helper**

In `YierdisServerBootstrapCommandWiringTest.java`, inside `InitializerTestEnv`, replace:

```java
                    processor,
```

with:

```java
                    processor::execute,
```

The constructor block should read:

```java
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    processor::execute,
                    ImmediateEventExecutor.INSTANCE,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 1024, 10, SchedulingPolicy.FAIR)
            );
```

- [ ] **Step 4: Run focused server tests**

Run:

```bash
jdk25 mvn -pl yierdis-server -Dtest=NettyExecutionAdapterIntegrationTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: PASS. This confirms the server still adapts the concrete command processor into executor-core and protocol/command wiring still works.

- [ ] **Step 5: Checkpoint**

Do not commit unless the user explicitly requested commits for this implementation. If commits are requested, use:

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: adapt server processor to executor engine"
```

---

## Task 5: Final Verification For Slice 1

**Files:**
- No new code files. This task verifies the slice against the approved roadmap spec.

- [ ] **Step 1: Run executor-core tests**

Run:

```bash
jdk25 mvn -pl yierdis-executor-core test
```

Expected: PASS.

- [ ] **Step 2: Run server integration tests touched by this slice**

Run:

```bash
jdk25 mvn -pl yierdis-server -Dtest=NettyExecutionAdapterIntegrationTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: PASS.

- [ ] **Step 3: Run architecture guards for executor boundaries**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest#executorCoreMustNotDependOnCoreCommand,ArchitectureBoundaryTest#executorCoreMustNotDependOnNetty test
```

Expected: PASS.

- [ ] **Step 4: Run or explicitly defer the full test suite**

Run:

```bash
jdk25 mvn test
```

Expected when run: PASS. If execution budget prevents this command from running in the current session, record `jdk25 mvn test` as not run and include the focused test results from Steps 1-3 in the handoff.

- [ ] **Step 5: Verify the dependency boundary manually**

Run:

```bash
git grep -n "yier.bubu.redis.command" -- yierdis-executor-core/src/main/java yierdis-executor-core/pom.xml
```

Expected: no output.

- [ ] **Step 6: Check final diff**

Run:

```bash
git diff -- yierdis-executor-core yierdis-server yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
```

Expected: diff only shows executor boundary changes described in this plan.

- [ ] **Step 7: Final checkpoint**

Do not commit unless the user explicitly requested commits for this implementation. If commits are requested, use:

```bash
git add yierdis-executor-core \
  yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
  yierdis-server/src/test/java/yier/bubu/redis/NettyExecutionAdapterIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: decouple executor core from commands"
```

---

## Scope Coverage

This plan implements Slice 1 from `docs/superpowers/specs/2026-04-27-yierdis-architecture-optimization-roadmap-design.md`:

- It adds a narrow executor execution interface.
- It removes `yierdis-executor-core`'s production dependency on `YierdisFastCommandProcessor`.
- It removes executor-core's Maven dependency on `yierdis-core-command`.
- It updates server composition roots to adapt concrete processors via `processor::execute`.
- It adds an architecture guard to prevent the dependency from returning.

It does not implement Slice 2 or later roadmap items: handle-based maxmemory candidates, FFM slab allocation, memory accounting categories, command definition SSOT, or broad documentation updates. Those should each get a separate implementation plan after this slice is verified.
