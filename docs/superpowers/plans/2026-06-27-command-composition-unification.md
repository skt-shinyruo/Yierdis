# Command Composition Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make command registration explicit and uniform across processor, engine, server, embedded, and reusable test entry points by removing implicit transaction command registration from `YierdisFastCommandProcessor`.

**Architecture:** Add neutral assembly primitives in `command-core`, decouple `TransactionCommands` from the processor through a narrow replay interface, then move every reusable runtime/helper entry point to explicit composition roots that assemble a `CommandRegistry` before publishing a processor. Keep transaction queue policy inside `YierdisFastCommandProcessor`, and keep runtime-specific command lists in their owning layers rather than centralizing all profiles into `command-core`.

**Tech Stack:** Java 25, Maven, JUnit 4, `YierdisFastCommandProcessor`, `CommandRegistry`, `DefaultYierdisEngine`, `DefaultCommandModules`, `ServerCommandModule`, `YierdisServerBootstrap`.

## Global Constraints

- Use JDK 25 for every Maven command: prefix Java commands with `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`.
- Do not change Redis command semantics.
- Do not change built-in command implementations inside `DefaultCommandModules`.
- Do not move `HELLO`, `INFO`, or `STATS` into `command-core`.
- Do not move `TransactionQueuePolicy` out of `YierdisFastCommandProcessor`.
- Remove old processor/engine module-assembly constructors in the same implementation round; do not leave parallel implicit and explicit assembly APIs behind.

## File Structure

- Create `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java`: narrow replay interface used by transaction commands.
- Create `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`: shared explicit registry assembly helper.
- Create `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`: server composition root that assembles transaction, built-in, and server-local commands.
- Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java`: reusable integration-test composition root for default command processors plus test extras.
- Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/EmbeddedCommandComposition.java`: reusable embedded/runtime composition root for instance-backed test processors.
- Modify `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`: depend on `QueuedCommandReplayer`, not `YierdisFastCommandProcessor`.
- Modify `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`: accept only explicit `CommandRegistry`, keep execution-path behavior only.
- Modify `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`: accept an already-built processor and stop assembling modules.
- Modify `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`: build the server processor through `ServerCommandComposition`.
- Modify `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java`: delegate to `TestCommandComposition`.
- Modify `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java`: delegate to `EmbeddedCommandComposition`.
- Modify `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`: delegate to `ServerCommandComposition` and explicit engine construction.
- Modify `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java`: rebuild unit tests around explicit registries.
- Modify `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`: validate explicit registry assembly and constructor surface.
- Modify `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`: keep transaction semantics coverage while switching to explicit composition.
- Modify `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`: update engine construction and add explicit-processor ownership assertions.
- Modify `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`: keep server command-surface coverage after composition-root migration.
- Modify `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java`: continue guarding default command metadata through test composition.
- Modify `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`: ensure default test composition still exposes transaction commands.
- Modify `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`: enforce removal of implicit processor assembly and require named composition roots.

---

### Task 1: Introduce Explicit Registry Assembly Primitives

**Files:**
- Create: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java`
- Create: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`

**Interfaces:**
- Consumes: `CommandModule`, `CommandRegistry`, `ExecutionRequest`, `CommandContext`
- Produces: `QueuedCommandReplayer.replay(ExecutionRequest request, CommandContext ctx)`, `CommandRegistries.from(CommandModule... modules)`, `CommandRegistries.from(Iterable<? extends CommandModule> modules)`, `CommandRegistries.registerInto(CommandRegistry registry, CommandModule... modules)`, `CommandRegistries.registerInto(CommandRegistry registry, Iterable<? extends CommandModule> modules)`

- [ ] **Step 1: Write the failing replay/assembly tests**

Add these test methods to `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`:

```java
    @Test
    public void commandRegistriesRegistersModulesInOrder() {
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(
                        "FIRST",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "first"),
                        (request, ctx) -> ctx.out().simpleString("FIRST")
                ),
                registration -> registration.register(
                        "SECOND",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "second"),
                        (request, ctx) -> ctx.out().simpleString("SECOND")
                )
        );

        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        Assert.assertEquals("FIRST", executeSimpleString(processor, "FIRST"));
        Assert.assertEquals("SECOND", executeSimpleString(processor, "SECOND"));
    }

    @Test
    public void commandRegistriesRejectNullModules() {
        try {
            CommandRegistries.from(
                    List.of(
                            registration -> registration.register(
                                    "OK",
                                    CommandDescriptor.of(1, 0, 0, 0),
                                    CommandParsers.exactRequest(1, "ok"),
                                    (request, ctx) -> ctx.out().simpleString("OK")
                            ),
                            null
                    )
            );
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("modules must not contain null", e.getMessage());
        }
    }
```

Add this test method to `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`:

```java
    @Test
    public void transactionCommandsReplayQueuedRequestsThroughNarrowReplayer() {
        ArrayList<String> replayed = new ArrayList<>();
        CommandRegistry registry = CommandRegistries.from(
                new TransactionCommands((request, ctx) -> {
                    replayed.add(arg(request, 0));
                    ctx.out().simpleString("REPLAY_" + arg(request, 0));
                }),
                registration -> registration.register(
                        "WRITE",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "write"),
                        (request, ctx) -> ctx.out().simpleString("WRITE")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        TestSession session = new TestSession();
        CapturingReplyWriter out = new CapturingReplyWriter();
        CommandContext ctx = context(session, out);

        processor.execute(request("MULTI"), ctx);
        out.clear();
        processor.execute(request("WRITE"), ctx);
        Assert.assertEquals("QUEUED", out.simpleString());

        out.clear();
        processor.execute(request("EXEC"), ctx);

        Assert.assertEquals(Integer.valueOf(1), out.arrayHeader());
        Assert.assertEquals(List.of("WRITE"), replayed);
        Assert.assertEquals("REPLAY_WRITE", out.simpleString());
    }
```

- [ ] **Step 2: Run the focused tests to confirm the red state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-core -Dtest=YierdisFastCommandProcessorRegistrationTest,YierdisFastCommandProcessorPolicyTest test
```

Expected: `BUILD FAILURE` because `QueuedCommandReplayer`, `CommandRegistries`, and the new `YierdisFastCommandProcessor(CommandRegistry)` constructor do not exist yet.

- [ ] **Step 3: Add the narrow replay interface**

Create `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java` with this content:

```java
package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

@FunctionalInterface
public interface QueuedCommandReplayer {
    void replay(ExecutionRequest request, CommandContext ctx);
}
```

- [ ] **Step 4: Add explicit registry assembly helpers**

Create `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java` with this content:

```java
package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;

import java.util.Arrays;

public final class CommandRegistries {
    private CommandRegistries() {
    }

    public static CommandRegistry from(CommandModule... modules) {
        CommandRegistry registry = new CommandRegistry();
        registerInto(registry, modules);
        return registry;
    }

    public static CommandRegistry from(Iterable<? extends CommandModule> modules) {
        CommandRegistry registry = new CommandRegistry();
        registerInto(registry, modules);
        return registry;
    }

    public static void registerInto(CommandRegistry registry, CommandModule... modules) {
        if (modules == null) {
            return;
        }
        registerInto(registry, Arrays.asList(modules));
    }

    public static void registerInto(CommandRegistry registry, Iterable<? extends CommandModule> modules) {
        if (registry == null) {
            throw new NullPointerException("registry");
        }
        if (modules == null) {
            return;
        }
        for (CommandModule module : modules) {
            if (module == null) {
                throw new IllegalArgumentException("modules must not contain null");
            }
            module.register(registry);
        }
    }
}
```

- [ ] **Step 5: Decouple `TransactionCommands` from the processor**

Update `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java` so its constructor and `exec(...)` path look like this:

```java
final class TransactionCommands implements CommandModule {
    private final QueuedCommandReplayer replayer;

    TransactionCommands(QueuedCommandReplayer replayer) {
        this.replayer = Objects.requireNonNull(replayer, "replayer");
    }

    // ...

    private void exec(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            wrongArity(out, "exec");
            return;
        }
        TransactionState tx = tx(ctx);
        if (!tx.active()) {
            out.error("ERR EXEC without MULTI");
            return;
        }
        if (tx.aborted()) {
            tx.discard();
            out.error("EXECABORT Transaction discarded because of previous errors.");
            return;
        }

        List<ExecutionRequest> queued = tx.drain();
        out.arrayHeader(queued.size());
        for (ExecutionRequest queuedRequest : queued) {
            try (ExecutionRequest replay = queuedRequest) {
                CommandContext replayCtx = new CommandContext(ctx.sessionCapabilities(), out);
                replayer.replay(replay, replayCtx);
            }
        }
    }
}
```

- [ ] **Step 6: Run the focused kernel tests to verify the green state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-core -Dtest=YierdisFastCommandProcessorRegistrationTest,YierdisFastCommandProcessorPolicyTest test
```

Expected: `BUILD SUCCESS` with both test classes passing.

- [ ] **Step 7: Commit**

```bash
git add \
  yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java \
  yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java \
  yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java \
  yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java \
  yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java
git commit -m "refactor: decouple transaction replay from processor"
```

### Task 2: Make Processor and Engine Execution-Only

**Files:**
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`
- Modify: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`

**Interfaces:**
- Consumes: `CommandRegistry`, `YierdisFastCommandProcessor`, `Runnable maintenanceTick`
- Produces: `new YierdisFastCommandProcessor(CommandRegistry registry)`, `new YierdisFastCommandProcessor(YierdisCommandProcessorOptions options, CommandRegistry registry)`, `new DefaultYierdisEngine(YierdisFastCommandProcessor commandProcessor, Runnable maintenanceTick)`

- [ ] **Step 1: Rewrite processor/engine constructor tests first**

Replace the processor test setup in `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java` with explicit registry construction:

```java
    @Test
    public void emptyRegistryDoesNotRegisterDefaultDataCommands() {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(new CommandRegistry());

        assertUnknownCommand(processor, "PING");
        assertUnknownCommand(processor, "GET");
        assertUnknownCommand(processor, "SET");
    }

    @Test
    public void explicitRegistryCanRegisterAdditionalCommands() {
        CommandRegistry registry = CommandRegistries.from(
                registrar -> registrar.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of());

        CapturingReplyWriter out = new CapturingReplyWriter();
        processor.execute(request, TestCommandContexts.context(out));

        Assert.assertEquals("LOCAL", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }
```

Update the engine tests in `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java` to construct a processor before constructing the engine:

```java
        CommandRegistry registry = CommandRegistries.from(
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        YierdisEngine engine = new DefaultYierdisEngine(processor, () -> {
        });
```

- [ ] **Step 2: Run the processor/engine tests to confirm the red state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-core -am \
  -Dtest=YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,DefaultYierdisEngineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE` because `YierdisFastCommandProcessor` and `DefaultYierdisEngine` do not yet expose the explicit-construction APIs used by the updated tests.

- [ ] **Step 3: Remove processor-owned assembly**

Update `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java` so its construction section looks like this:

```java
public final class YierdisFastCommandProcessor {
    // ...

    public YierdisFastCommandProcessor(CommandRegistry registry) {
        this(CommandChangeEmitter.noop(), registry);
    }

    public YierdisFastCommandProcessor(
            YierdisCommandProcessorOptions options,
            CommandRegistry registry
    ) {
        this(CommandChangeEmitter.fromOptions(options), registry);
    }

    private YierdisFastCommandProcessor(CommandChangeEmitter changeEmitter, CommandRegistry registry) {
        this.changeEmitter = Objects.requireNonNull(changeEmitter, "changeEmitter");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    // keep execute(...) and executeSpec(...) behavior unchanged
}
```

Delete:

- `public YierdisFastCommandProcessor(CommandModule... modules)`
- `public YierdisFastCommandProcessor(YierdisCommandProcessorOptions options, CommandModule... modules)`
- `public YierdisFastCommandProcessor(YierdisCommandProcessorOptions options, Iterable<? extends CommandModule> modules)`
- `registerExtraModules(...)`
- `toArray(...)`

- [ ] **Step 4: Remove engine-owned assembly**

Update `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java` so its public constructor surface becomes:

```java
public final class DefaultYierdisEngine implements YierdisEngine {
    private final YierdisFastCommandProcessor commandProcessor;
    private final Runnable maintenanceTick;

    public DefaultYierdisEngine(
            YierdisFastCommandProcessor commandProcessor,
            Runnable maintenanceTick
    ) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.maintenanceTick = Objects.requireNonNull(maintenanceTick, "maintenanceTick");
    }

    @Override
    public void execute(Session session, ExecutionRequest request, RedisReplyWriter out) {
        commandProcessor.execute(request, new CommandContext(CommandSessionCapabilities.from(session), out));
    }

    @Override
    public void maintenanceTick() {
        maintenanceTick.run();
    }
}
```

Delete the constructors that accept `CommandModule...` or `YierdisCommandProcessorOptions`.

- [ ] **Step 5: Run the processor/engine tests to verify the green state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-core -am \
  -Dtest=YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,DefaultYierdisEngineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add \
  yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java \
  yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java \
  yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java \
  yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java \
  yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java
git commit -m "refactor: make command processor and engine execution-only"
```

### Task 3: Add Explicit Server Command Composition

**Files:**
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`

**Interfaces:**
- Consumes: `YierdisCommandProcessorOptions`, `YierdisDbRouter`, `ServerInfoProvider`, `SlowCommandGovernor`
- Produces: `ServerCommandComposition.createProcessor(YierdisCommandProcessorOptions options, YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor) : YierdisFastCommandProcessor`

- [ ] **Step 1: Write the failing server composition assertion**

Add this test method to `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`:

```java
    @Test
    public void serverCommandCompositionBuildsProcessorWithServerAndDefaultCommands() throws Exception {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(
                YierdisInstanceConfig.builder().databases(1).build()
        )) {
            NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(
                    runtimeConfig(0, 0, 1024, 0, 4, 5)
            );
            YierdisFastCommandProcessor processor = ServerCommandComposition.createProcessor(
                    YierdisCommandProcessorOptions.DEFAULT,
                    TestDbRouters.forInstance(instance),
                    infoProvider,
                    SlowCommandGovernor.DEFAULT
            );

            try (yier.bubu.redis.testutil.FastTestClient client =
                         new yier.bubu.redis.testutil.FastTestClient(processor)) {
                Assert.assertEquals(
                        "PONG",
                        ((yier.bubu.redis.testutil.ReplySimpleString) client.execute(
                                java.util.Arrays.asList("PING".getBytes(StandardCharsets.US_ASCII))
                        )).value()
                );
                Assert.assertTrue(client.execute(
                        java.util.Arrays.asList("HELLO".getBytes(StandardCharsets.US_ASCII))
                ) instanceof yier.bubu.redis.testutil.ReplyMap);
            }
        }
    }
```

- [ ] **Step 2: Run the server-main tests to confirm the red state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main -am \
  -Dtest=YierdisServerBootstrapCommandWiringTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE` because `ServerCommandComposition` does not exist and `TestYierdisEngines` still depends on module-assembling engine constructors.

- [ ] **Step 3: Add the server composition root**

Create `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java` with this content:

```java
package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.TransactionCommands;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;

public final class ServerCommandComposition {
    private ServerCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(
            YierdisCommandProcessorOptions options,
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor
    ) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(options, registry);
        CommandRegistries.registerInto(
                registry,
                new TransactionCommands(processor::execute),
                DefaultCommandModules.create(dbRouter, infoProvider, slowGovernor),
                new ServerCommandModule(infoProvider)
        );
        return processor;
    }
}
```

- [ ] **Step 4: Migrate server bootstrap and server test helpers**

Replace the engine construction in `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java` with:

```java
        YierdisFastCommandProcessor commandProcessor = ServerCommandComposition.createProcessor(
                commandProcessorOptions,
                dbRouter(instance),
                infoProvider,
                slowGovernor
        );
        YierdisEngine commandEngine = new DefaultYierdisEngine(
                commandProcessor,
                maintenanceTick
        );
```

Replace `TestYierdisEngines.forInstance(...)` in `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java` with:

```java
    static YierdisEngine forInstance(YierdisInstance instance) {
        YierdisFastCommandProcessor processor = ServerCommandComposition.createProcessor(
                YierdisCommandProcessorOptions.DEFAULT,
                TestDbRouters.forInstance(instance),
                new NettyServerInfoProvider(runtimeConfig(0, 0, 1024, 0, 4, 5)),
                SlowCommandGovernor.DEFAULT
        );
        return new DefaultYierdisEngine(
                processor,
                () -> {
                }
        );
    }
```

Add this helper to the same file:

```java
    private static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            int transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxInlineBytes,
            int protocolMaxCommandBytes
    ) {
        return new YierdisServerRuntimeConfig(
                0,
                1,
                1,
                1024,
                1024L * 1024L,
                10000L,
                10000L,
                300000,
                67108864,
                10000,
                transactionQueueMaxCommands,
                transactionQueueMaxBytes,
                YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                yier.bubu.redis.storage.api.MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                false,
                protocolMaxBulkBytes,
                protocolMaxArgs,
                protocolMaxInlineBytes,
                protocolMaxCommandBytes,
                0
        );
    }
```

- [ ] **Step 5: Run the server-main tests to verify the green state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main -am \
  -Dtest=YierdisServerBootstrapCommandWiringTest,ClosingSkipSideEffectsIntegrationTest,NettyExecutionAdapterIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: add explicit server command composition"
```

### Task 4: Move Embedded and Reusable Test Entry Points to Composition Roots

**Files:**
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java`
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/EmbeddedCommandComposition.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandRegistryGuardTest.java`

**Interfaces:**
- Consumes: `DbEngine`, `YierdisDbRouter`, `YierdisChangeSink`, `CommandModule... extraModules`
- Produces: `TestCommandComposition.createProcessor(DbEngine db, CommandModule... extraModules)`, `TestCommandComposition.createProcessor(YierdisDbRouter dbRouter, YierdisChangeSink changeSink, CommandModule... extraModules)`, `EmbeddedCommandComposition.createProcessor(YierdisInstance instance)`, `EmbeddedCommandComposition.createProcessor(DbEngine db, CommandModule... extraModules)`

- [ ] **Step 1: Add failing integration assertions that require explicit compositions**

Add this test method to `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java`:

```java
    @Test
    public void testCommandCompositionListsEveryDefaultCommandIncludingTransactions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                assertInteger(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND", "COUNT")));
            }
        });
    }
```

Add this test method to `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`:

```java
    @Test
    public void testCommandCompositionKeepsTransactionCommandsExplicitlyRegistered() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            TestSession session = new TestSession();
            try (FastTestClient client = new FastTestClient(processor, session)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("DISCARD")))).value());
            }
        });
    }
```

- [ ] **Step 2: Run the integration tests to confirm the red state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=DefaultCommandRegistrationTest,TransactionCommandTest,CommandRegistryGuardTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE` because `TestCommandComposition` and `EmbeddedCommandComposition` do not exist yet.

- [ ] **Step 3: Add reusable integration and embedded composition roots**

Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java` with this content:

```java
package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.TransactionCommands;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TestCommandComposition {
    private TestCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(DbEngine db, CommandModule... extraModules) {
        return createProcessor(singleDbRouter(db), YierdisChangeSink.NOOP, extraModules);
    }

    public static YierdisFastCommandProcessor createProcessor(
            YierdisDbRouter dbRouter,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        CommandModule defaults = DefaultCommandModules.create(dbRouter, null);
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(options(changeSink), registry);
        List<CommandModule> modules = new ArrayList<>();
        modules.add(new TransactionCommands(processor::execute));
        modules.add(defaults);
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        CommandRegistries.registerInto(registry, modules);
        return processor;
    }

    private static YierdisDbRouter singleDbRouter(DbEngine db) {
        DbEngine fixed = Objects.requireNonNull(db, "db");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(yier.bubu.redis.execution.api.DbIndexSession session) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }
}
```

Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/EmbeddedCommandComposition.java` with this content:

```java
package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.TransactionCommands;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;

public final class EmbeddedCommandComposition {
    private EmbeddedCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(YierdisInstance instance) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerInto(
                registry,
                new TransactionCommands(processor::execute),
                DefaultCommandModules.create(TestDbRouters.forInstance(instance), null)
        );
        return processor;
    }

    public static YierdisFastCommandProcessor createProcessor(DbEngine db, CommandModule... extraModules) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        List<CommandModule> modules = new ArrayList<>();
        modules.add(new TransactionCommands(processor::execute));
        modules.add(DefaultCommandModules.create(db));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        CommandRegistries.registerInto(registry, modules);
        return processor;
    }
}
```

- [ ] **Step 4: Migrate reusable test processor helpers**

Update `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java` so its public methods delegate to `TestCommandComposition`:

```java
    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return TestCommandComposition.createProcessor(db, extraModules);
    }

    public static YierdisFastCommandProcessor forDbWithChangeSink(
            DbEngine db,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        return TestCommandComposition.createProcessor(singleDbRouter(db), changeSink, extraModules);
    }
```

Update `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java` so it becomes:

```java
    public static YierdisFastCommandProcessor forInstance(YierdisInstance instance) {
        return EmbeddedCommandComposition.createProcessor(instance);
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return EmbeddedCommandComposition.createProcessor(db, extraModules);
    }
```

- [ ] **Step 5: Run the integration tests to verify the green state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=DefaultCommandRegistrationTest,TransactionCommandTest,CommandRegistryGuardTest,CommandProcessorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/EmbeddedCommandComposition.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandRegistryGuardTest.java
git commit -m "refactor: move test command entry points to compositions"
```

### Task 5: Tighten Architecture Guards and Run End-to-End Verification

**Files:**
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`

**Interfaces:**
- Consumes: explicit composition roots added in Tasks 3-4
- Produces: architecture guards that ban implicit processor/engine assembly and require named composition roots

- [ ] **Step 1: Add failing architecture assertions for the new steady state**

Add this test method to `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`:

```java
    @Test
    public void processorNoLongerExposesModuleAssemblyConstructors() {
        for (var constructor : YierdisFastCommandProcessor.class.getConstructors()) {
            for (Class<?> parameterType : constructor.getParameterTypes()) {
                Assert.assertNotEquals(CommandModule[].class, parameterType);
                Assert.assertNotEquals(Iterable.class, parameterType);
            }
        }
    }
```

Update `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java` with guards that:

- fail if `YierdisFastCommandProcessor.java` still contains `new TransactionCommands(this)`
- fail if `DefaultYierdisEngine.java` still contains `new YierdisFastCommandProcessor(`
- fail if `TestCommandProcessors.java`, `TestYierdisEngines.java`, or `YierdisServerBootstrap.java` assemble modules without going through `*CommandComposition`

Use this exact scan block in the architecture test:

```java
        scanFileForForbiddenText(
                repoRoot,
                commandKernelMain(repoRoot).resolve("yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"),
                offenders,
                "new TransactionCommands(this)",
                "registerExtraModules(",
                "CommandModule..."
        );
```

Replace or supplement the existing constructor-surface assertions so they describe the explicit-registry steady state rather than the old implicit-kernel behavior.

- [ ] **Step 2: Run the architecture and regression suite to confirm the red state**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests,yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=ArchitectureBoundaryTest,YierdisFastCommandProcessorRegistrationTest,YierdisServerBootstrapCommandWiringTest,DefaultCommandRegistrationTest,TransactionCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE` until all old implicit assembly call sites and architecture expectations are updated.

- [ ] **Step 3: Update architecture guards to the final explicit-composition state**

Modify `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java` so the relevant sections assert all of the following:

- `YierdisFastCommandProcessor` no longer contains `new TransactionCommands(this)`
- `DefaultYierdisEngine` no longer constructs a processor internally
- `YierdisServerBootstrap` does not import `YierdisFastCommandProcessor` directly and uses `ServerCommandComposition`
- reusable test helpers `TestCommandProcessors` and `TestYierdisEngines` contain `CommandComposition` class references rather than direct `new YierdisFastCommandProcessor(...)` or `new DefaultYierdisEngine(..., CommandModule...)` calls

Keep the existing protections that forbid server-facing commands from drifting into `command-core`.

- [ ] **Step 4: Run the full verification suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-core,yierdis-command/yierdis-command-builtin,yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests,yierdis-tests/yierdis-architecture-tests -am test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java
git commit -m "test: enforce explicit command compositions"
```
