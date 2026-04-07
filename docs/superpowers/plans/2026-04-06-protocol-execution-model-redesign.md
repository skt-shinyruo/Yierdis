# Protocol/Execution Model Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ExecutionRequest` and `ExecutionRecord` the only first-class execution and replay contracts without changing Custom Protocol v1 compatibility, command semantics, transaction semantics, or reply semantics.

**Architecture:** Introduce byte-oriented execution contracts in `yierdis-core-contract`, route the command processor through them behind a temporary `Command` compatibility shim, then move transaction replay, change events, and server handoff onto the new contracts in separate waves. End by tightening architecture guards so production code cannot drift back to server-local `Command` wrappers or raw `byte[][]` replay payloads.

**Tech Stack:** Java 25, Maven multi-module reactor, Netty 4.1, JUnit 4

---

## File Map

- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRequest.java`
  Responsibility: primary byte-oriented execution contract for command processing.
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRecord.java`
  Responsibility: immutable replay record carrying `dbIndex` plus an `ExecutionRequest` snapshot.
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java`
  Responsibility: immutable heap-backed `ExecutionRequest` implementation used by protocol adaptation, transaction queues, change events, and tests.
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/Command.java`
  Responsibility: transitional compatibility surface that extends `ExecutionRequest` and keeps optional frame/offset hooks.
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/TransactionState.java`
  Responsibility: queue `ExecutionRequest` snapshots instead of raw `byte[][]`.
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
  Responsibility: change handler signature from `Command` to `ExecutionRequest`.
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
  Responsibility: resolve handlers/specs directly from `ExecutionRequest`.
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
  Responsibility: expose parsing helpers against `ExecutionRequest`.
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  Responsibility: make `ExecutionRequest` the primary processor input; keep `execute(Command, CommandContext)` as compatibility only.
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
  Responsibility: replay queued `ExecutionRequest` objects directly; remove `QueuedCommand`.
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java`
  Responsibility: carry `ExecutionRecord` instead of `byte[][] argv`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
  Responsibility: hold transaction queue snapshots as `ByteArrayExecutionRequest`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
  Responsibility: adapt `CustomProtocolV1Request` to `ExecutionRequest`, not to a server-local `Command`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
  Responsibility: accept `ExecutionRequest` from the Netty pipeline.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
  Responsibility: execute and account for `ExecutionRequest`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
  Responsibility: make executor submit/drain APIs use `ExecutionRequest`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
  Responsibility: reserve queue budget from `ExecutionRequest.retainedBytes()`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
  Responsibility: drain `ExecutionRequest` tasks and preserve close/backpressure behavior.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java`
  Responsibility: carry `ExecutionRequest` instead of `Command`.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
  Responsibility: server-local command handlers consume `ExecutionRequest`.
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
  Responsibility: expose protocol-agnostic execution helpers around `ExecutionRequest`.
- Create: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`
  Responsibility: verify protocol-to-execution adaptation semantics.
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java`
  Responsibility: lock immutable snapshot, null handling, and retained-byte accounting.
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`
  Responsibility: prove extra modules and default execution work with `ExecutionRequest`.
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java`
  Responsibility: keep registration semantics green after handler signature change.
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
  Responsibility: verify `MULTI/EXEC` replay equivalence and queue snapshot immutability.
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
  Responsibility: verify change events emit `ExecutionRecord` payloads and match direct execution behavior.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
  Responsibility: keep executor lifecycle, retained-byte error handling, and submission semantics green after type migration.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
  Responsibility: preserve backpressure behavior with `ExecutionRequest` tasks.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
  Responsibility: preserve fair scheduling semantics with `ExecutionRequest` tasks.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
  Responsibility: ensure protocol resynchronization still works after adaptation changes.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  Responsibility: lock pipeline wiring after `ExecutionRequest` migration.
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
  Responsibility: keep end-to-end client behavior stable.
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
  Responsibility: forbid command-layer regression back to `Command`-first production flow.
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
  Responsibility: forbid raw `byte[][]` replay payloads and server-local `Command` wrappers from reappearing.
- Modify: `README.md`
  Responsibility: update contract ownership documentation from `Command`-centric wording to `ExecutionRequest`/`ExecutionRecord`.

### Task 1: Introduce `ExecutionRequest` / `ExecutionRecord` And A Temporary Processor Compatibility Shim

**Files:**
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRequest.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRecord.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java`
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/Command.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`

- [ ] **Step 1: Write failing contract and processor-entry tests**

```java
@Test
public void byteArrayExecutionRequestCopiesBytesAndPreservesNulls() {
    List<byte[]> args = new ArrayList<>();
    byte[] value = "v1".getBytes(StandardCharsets.UTF_8);
    args.add("SET".getBytes(StandardCharsets.UTF_8));
    args.add("k".getBytes(StandardCharsets.UTF_8));
    args.add(null);
    args.add(value);

    ByteArrayExecutionRequest request = ByteArrayExecutionRequest.copyOf(args);
    value[1] = (byte) '2';

    Assert.assertEquals(4, request.argc());
    Assert.assertTrue(request.isNull(2));
    Assert.assertArrayEquals("SET".getBytes(StandardCharsets.UTF_8), request.toByteArray(0));
    Assert.assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), request.toByteArray(3));
    Assert.assertEquals(6, request.retainedBytes());
}

@Test
public void executionRecordNormalizesDbIndexAndSnapshotsRequest() {
    ExecutionRecord record = new ExecutionRecord(
            -3,
            ByteArrayExecutionRequest.fromUtf8("DEL", Arrays.asList("k"))
    );

    Assert.assertEquals(0, record.dbIndex());
    Assert.assertArrayEquals("DEL".getBytes(StandardCharsets.UTF_8), record.request().toByteArray(0));
    Assert.assertArrayEquals("k".getBytes(StandardCharsets.UTF_8), record.request().toByteArray(1));
}
```

```java
@Test
public void extraModulesCanExecutePlainExecutionRequests() {
    YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
            new NoopDbEngine(),
            null,
            SlowCommandGovernor.DEFAULT,
            registrar -> registrar.register(
                    "LOCAL",
                    (cmd, ctx) -> ctx.out().simpleString("LOCAL_OK"),
                    CommandDescriptor.of(1, 0, 0, 0)
            )
    );

    CapturingReplyWriter out = new CapturingReplyWriter();
    processor.execute(
            ByteArrayExecutionRequest.fromUtf8("LOCAL", Collections.emptyList()),
            new CommandContext(null, out)
    );

    Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
    Assert.assertNull(out.errorValue);
}
```

- [ ] **Step 2: Run focused tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime -am -Dtest=ExecutionRequestContractTest,YierdisFastCommandProcessorModuleTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `ExecutionRequest`, `ExecutionRecord`, `ByteArrayExecutionRequest`, and `YierdisFastCommandProcessor.execute(ExecutionRequest, CommandContext)` do not exist yet.

- [ ] **Step 3: Add the new core-contract types and make `Command` a compatibility subtype**

```java
public interface ExecutionRequest extends AutoCloseable {
    int argc();

    boolean isNull(int index);

    int len(int index);

    byte byteAt(int index, int offset);

    void copyToByteArray(int index, byte[] dst, int dstOff);

    byte[] toByteArray(int index);

    default int retainedBytes() {
        return 0;
    }

    @Override
    void close();
}
```

```java
public interface Command extends ExecutionRequest {
    default BytesSource frame() {
        return null;
    }

    default int argOffset(int index) {
        return -1;
    }
}
```

```java
public final class ByteArrayExecutionRequest implements ExecutionRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    public static ByteArrayExecutionRequest copyOf(List<byte[]> args) {
        byte[][] argv = new byte[args == null ? 0 : args.size()][];
        int retainedBytes = 0;
        for (int i = 0; i < argv.length; i++) {
            byte[] arg = args.get(i);
            if (arg != null) {
                argv[i] = arg.clone();
                retainedBytes += arg.length;
            }
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes);
    }

    public static ByteArrayExecutionRequest copyOf(ExecutionRequest request) {
        byte[][] argv = new byte[request.argc()][];
        int retainedBytes = 0;
        for (int i = 0; i < argv.length; i++) {
            byte[] arg = request.toByteArray(i);
            if (arg != null) {
                argv[i] = arg;
                retainedBytes += arg.length;
            }
        }
        return new ByteArrayExecutionRequest(argv, retainedBytes);
    }

    public static ByteArrayExecutionRequest fromUtf8(String cmd, List<String> args) {
        ArrayList<byte[]> argv = new ArrayList<>(1 + (args == null ? 0 : args.size()));
        argv.add(cmd == null ? null : cmd.getBytes(StandardCharsets.UTF_8));
        if (args != null) {
            for (String arg : args) {
                argv.add(arg == null ? null : arg.getBytes(StandardCharsets.UTF_8));
            }
        }
        return copyOf(argv);
    }
}
```

```java
public record ExecutionRecord(int dbIndex, ExecutionRequest request) {
    public ExecutionRecord {
        if (dbIndex < 0) {
            dbIndex = 0;
        }
        request = ByteArrayExecutionRequest.copyOf(Objects.requireNonNull(request, "request"));
    }
}
```

- [ ] **Step 4: Add the temporary processor shim and switch test utilities to the new entrypoint**

```java
public void execute(ExecutionRequest request, CommandContext ctx) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(ctx, "ctx");
    Command command = request instanceof Command existing ? existing : new Command() {
        @Override
        public int argc() { return request.argc(); }

        @Override
        public boolean isNull(int index) { return request.isNull(index); }

        @Override
        public int len(int index) { return request.len(index); }

        @Override
        public byte byteAt(int index, int offset) { return request.byteAt(index, offset); }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) { request.copyToByteArray(index, dst, dstOff); }

        @Override
        public byte[] toByteArray(int index) { return request.toByteArray(index); }

        @Override
        public int retainedBytes() { return request.retainedBytes(); }

        @Override
        public void close() { request.close(); }
    };
    executeInternal(command, ctx);
}

public void execute(Command cmd, CommandContext ctx) {
    execute((ExecutionRequest) cmd, ctx);
}
```

```java
public ReplyObject execute(ExecutionRequest request) {
    Objects.requireNonNull(request, "request");
    CapturingReplyWriter writer = new CapturingReplyWriter();
    processor.execute(request, new CommandContext(session, writer));
    request.close();
    return writer.root();
}

public ReplyObject execute(List<byte[]> args) {
    return execute(ByteArrayExecutionRequest.copyOf(args));
}
```

- [ ] **Step 5: Run focused tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime -am -Dtest=ExecutionRequestContractTest,YierdisFastCommandProcessorModuleTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRequest.java \
        yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ExecutionRecord.java \
        yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java \
        yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/Command.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java
git commit -m "refactor: add execution request contracts"
```

### Task 2: Move Command Registration And Handler Dispatch To `ExecutionRequest`

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HllCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Add failing registration and boundary tests that make `ExecutionRequest` the core-command SSOT**

```java
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java"),
        offenders,
        "void execute(Command cmd, CommandContext ctx)"
);
scanForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command"),
        offenders,
        "import yier.bubu.redis.contract.Command;"
);
allowOnly(
        offenders,
        "yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java"
);
```

```java
new YierdisFastCommandProcessor(
        new NoopDbEngine(),
        null,
        SlowCommandGovernor.DEFAULT,
        registrar -> registrar.register(
                "UP",
                (request, ctx) -> ctx.out().simpleString(CommandSupport.utf8(request, 0)),
                CommandDescriptor.of(1, 0, 0, 0)
        )
);
```

- [ ] **Step 2: Run focused command and boundary tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `CommandModule.Handler`, `CommandRegistry`, and helper methods still depend on `Command`.

- [ ] **Step 3: Change registration and parsing helpers to use `ExecutionRequest`**

```java
@FunctionalInterface
interface Handler {
    void execute(ExecutionRequest request, CommandContext ctx);
}
```

```java
CommandSpec spec(ExecutionRequest request) {
    Entry entry = findEntry(request);
    return entry == null ? null : entry.spec;
}

private Entry findEntry(ExecutionRequest request) {
    if (request == null || request.argc() <= 0 || request.isNull(0)) {
        return null;
    }
    int len = request.len(0);
    if (len <= 0) {
        return null;
    }
    long hash = hashUpperAscii(request, 0, len);
    int idx = index(hash);
    for (;;) {
        Entry e = table[idx];
        if (e == null) {
            return null;
        }
        if (e.hash == hash && e.nameUpperAscii.length == len && asciiEqualsIgnoreCase(request, 0, e.nameUpperAscii)) {
            return e;
        }
        idx = (idx + 1) & mask;
    }
}
```

```java
static String utf8(ExecutionRequest request, int argIndex) {
    return utf8(request.toByteArray(argIndex));
}

static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, String literal) {
    if (literal == null || request.isNull(argIndex)) {
        return false;
    }
    int len = request.len(argIndex);
    if (len != literal.length()) {
        return false;
    }
    for (int i = 0; i < len; i++) {
        int b = request.byteAt(argIndex, i) & 0xFF;
        int c = literal.charAt(i);
        if (b >= 'A' && b <= 'Z') {
            b |= 0x20;
        }
        if (c >= 'A' && c <= 'Z') {
            c |= 0x20;
        }
        if (b != c) {
            return false;
        }
    }
    return true;
}
```

- [ ] **Step 4: Refactor processor and all command modules so production handlers no longer require `Command`**

```java
public void execute(ExecutionRequest request, CommandContext ctx) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(ctx, "ctx");
    ReplyWriter out = ctx.out();
    TransactionState tx = txOrNull(ctx);
    if (tx != null && tx.active()) {
        boolean isMulti = CommandSupport.asciiEqualsIgnoreCase(request, 0, "MULTI");
        boolean isExec = CommandSupport.asciiEqualsIgnoreCase(request, 0, "EXEC");
        boolean isDiscard = CommandSupport.asciiEqualsIgnoreCase(request, 0, "DISCARD");
        if (!isMulti && !isExec && !isDiscard) {
            CommandSpec multiSpec = registry.spec(request);
            String disallowedInMultiError = multiSpec == null ? null : multiSpec.disallowedInMultiError();
            if (disallowedInMultiError != null) {
                tx.markAborted();
                out.error(disallowedInMultiError);
                return;
            }
            String enqueueErr = tx.tryEnqueue(request);
            if (enqueueErr != null) {
                out.error(enqueueErr);
                return;
            }
            out.simpleString("QUEUED");
            return;
        }
    }

    CommandSpec spec = registry.spec(request);
    if (spec == null) {
        out.error(unknownCommandMessage(request));
        return;
    }
    spec.handler().execute(request, ctx);
}

@Deprecated(forRemoval = false)
public void execute(Command cmd, CommandContext ctx) {
    execute((ExecutionRequest) cmd, ctx);
}
```

```java
private void ping(ExecutionRequest request, CommandContext ctx) {
    if (request.argc() == 1) {
        ctx.out().simpleString("PONG");
        return;
    }
    if (request.argc() != 2) {
        CommandSupport.wrongArity(ctx.out(), "ping");
        return;
    }
    ctx.out().bulkString(request.toByteArray(1));
}
```

```java
registration.register(
        "HELLO",
        (request, ctx) -> ctx.out().simpleString("HELLO"),
        CommandDescriptor.of(-1, 0, 0, 0)
);
```

- [ ] **Step 5: Run focused command and boundary tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HllCommands.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: make execution requests the command SSOT"
```

### Task 3: Move Transaction Replay And Change Events To Replayable Execution Contracts

**Files:**
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/TransactionState.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add failing replay-equivalence and boundary tests**

```java
ReplyArray exec = (ReplyArray) client.execute(Arrays.asList(b("EXEC")));
Assert.assertNotNull(exec.values());
Assert.assertEquals(1, exec.values().size());
Assert.assertEquals("OK", ((ReplySimpleString) exec.values().get(0)).value());

YierdisChangeEvent event = events.get(0);
Assert.assertEquals(0, event.dbIndex());
Assert.assertArrayEquals("SET".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(0));
Assert.assertArrayEquals("k".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(1));
Assert.assertArrayEquals("v".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(2));
```

```java
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java"),
        offenders,
        "byte[][] argv"
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java"),
        offenders,
        "ArrayList<byte[][]>",
        "List<byte[][]>"
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java"),
        offenders,
        "new QueuedCommand("
);
```

- [ ] **Step 2: Run focused transaction and change-event tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=TransactionCommandTest,YierdisChangeSinkTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because transaction state still queues `byte[][]`, `TransactionCommands` still wraps `QueuedCommand`, and `YierdisChangeEvent` still exposes raw argv arrays.

- [ ] **Step 3: Change transaction state to store immutable `ExecutionRequest` snapshots**

```java
public interface TransactionState {
    boolean active();

    void begin();

    void discard();

    void enqueue(ExecutionRequest request);

    default String tryEnqueue(ExecutionRequest request) {
        enqueue(request);
        return null;
    }

    int size();

    List<ExecutionRequest> drain();
}
```

```java
private final ArrayList<ExecutionRequest> queue = new ArrayList<>();

@Override
public synchronized String tryEnqueue(ExecutionRequest request) {
    if (request == null) {
        return null;
    }
    ByteArrayExecutionRequest snapshot = ByteArrayExecutionRequest.copyOf(request);
    int requestBytes = snapshot.retainedBytes();
    if (maxQueuedCommands > 0 && queue.size() >= maxQueuedCommands) {
        aborted = true;
        return "ERR Transaction queue is full";
    }
    if (maxQueuedBytes > 0 && queuedBytes + requestBytes > maxQueuedBytes) {
        aborted = true;
        return "ERR Transaction queue is full";
    }
    queue.add(snapshot);
    queuedBytes += requestBytes;
    return null;
}

@Override
public synchronized List<ExecutionRequest> drain() {
    ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
    queue.clear();
    active = false;
    aborted = false;
    queuedBytes = 0;
    return out;
}
```

- [ ] **Step 4: Replay queued requests directly and emit `ExecutionRecord` from successful writes**

```java
String enqueueErr = tx.tryEnqueue(request);
if (enqueueErr != null) {
    out.error(enqueueErr);
    return;
}
out.simpleString("QUEUED");
```

```java
List<ExecutionRequest> queued = tx.drain();
out.arrayHeader(queued.size());
for (ExecutionRequest queuedRequest : queued) {
    processor.execute(queuedRequest, ctx);
}
```

```java
public record YierdisChangeEvent(ExecutionRecord record) {
    public YierdisChangeEvent {
        record = Objects.requireNonNull(record, "record");
    }

    public int dbIndex() {
        return record.dbIndex();
    }

    public ExecutionRequest request() {
        return record.request();
    }
}
```

```java
changeSink.onChange(new YierdisChangeEvent(new ExecutionRecord(dbIndex, request)));
```

- [ ] **Step 5: Run focused transaction and change-event tests to verify GREEN**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=TransactionCommandTest,YierdisChangeSinkTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/TransactionState.java \
        yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java \
        yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: replay transactions with execution requests"
```

### Task 4: Migrate Protocol Handoff And Netty Executor Flow To `ExecutionRequest`

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java`
- Create: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`

- [ ] **Step 1: Add failing adaptation and executor tests**

```java
@Test
public void adaptsCustomProtocolRequestToExecutionRequest() {
    EmbeddedChannel channel = new EmbeddedChannel(new ProtocolCommandAdapter());

    Assert.assertTrue(channel.writeInbound(new CustomProtocolV1Request("SET", Arrays.asList("k", null, "v"))));

    Object inbound = channel.readInbound();
    Assert.assertTrue(inbound instanceof ExecutionRequest);
    ExecutionRequest request = (ExecutionRequest) inbound;
    Assert.assertEquals(4, request.argc());
    Assert.assertTrue(request.isNull(2));
    Assert.assertArrayEquals("SET".getBytes(StandardCharsets.UTF_8), request.toByteArray(0));
    Assert.assertEquals(5, request.retainedBytes());
}
```

```java
private static final class ThrowingRetainedBytesRequest implements ExecutionRequest {
    @Override
    public int retainedBytes() {
        throw new RuntimeException("boom");
    }

    @Override
    public int argc() { return 1; }

    @Override
    public boolean isNull(int index) { return false; }

    @Override
    public int len(int index) { return 4; }

    @Override
    public byte byteAt(int index, int offset) { return 'P'; }

    @Override
    public void copyToByteArray(int index, byte[] dst, int dstOff) {}

    @Override
    public byte[] toByteArray(int index) { return "PING".getBytes(StandardCharsets.US_ASCII); }

    @Override
    public void close() {}
}
```

- [ ] **Step 2: Run focused server tests to verify RED**

Run: `mvn -pl yierdis-server,yierdis-client -am -Dtest=ProtocolCommandAdapterTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest,YierdisClientTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the Netty pipeline still hands off `Command`, the executor task model still stores `Command`, and there is no protocol adaptation test yet.

- [ ] **Step 3: Replace the server-side `Command` handoff with `ExecutionRequest` end-to-end**

```java
final class ProtocolCommandAdapter extends SimpleChannelInboundHandler<CustomProtocolV1Request> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CustomProtocolV1Request msg) {
        if (ctx == null || msg == null) {
            return;
        }
        ctx.fireChannelRead(ByteArrayExecutionRequest.fromUtf8(msg.cmd(), msg.args()));
    }
}
```

```java
public final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<ExecutionRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ExecutionRequest msg) {
        NettyCommandExecutor.SubmitRejectReason reject = nettyExecutor.trySubmitWithReason(ctx, msg);
        if (reject == null) {
            return;
        }

        ByteBuf out = ctx.alloc().buffer();
        try {
            String err = "ERR busy " + reject.code();
            ReplyWriter writer = nettyExecutor.newReplyWriter(out, ctx.channel());
            writer.error(err);
            ctx.writeAndFlush(out);
            out = null;
        } finally {
            msg.close();
            if (out != null) {
                out.release();
            }
        }
    }
}
```

```java
final class NettyExecutorTask {
    final ChannelHandlerContext ctx;
    final ExecutionRequest request;
    final int retainedBytes;

    static NettyExecutorTask command(ChannelHandlerContext ctx, ExecutionRequest request, int retainedBytes) {
        return new NettyExecutorTask(ctx, request, retainedBytes);
    }
}
```

```java
void executeCommand(ExecutionRequest request, Channel ch, ReplyWriter writer) {
    Objects.requireNonNull(request, "request");
    ServerSessionState session = ServerConnectionContext.getOrCreate(ch).commandSession();
    commandProcessor.execute(request, context(session, writer));
}

static int safeRetainedBytes(ExecutionRequest request) {
    if (request == null) {
        return 0;
    }
    try {
        return Math.max(0, request.retainedBytes());
    } catch (Throwable ignored) {
        return 0;
    }
}
```

```java
public boolean trySubmit(ChannelHandlerContext ctx, ExecutionRequest request) {
    return trySubmitWithReason(ctx, request) == null;
}

SubmitRejectReason trySubmitWithReason(ChannelHandlerContext ctx, ExecutionRequest request) {
    return submitter.trySubmitWithReason(ctx, request);
}
```

- [ ] **Step 4: Run focused server tests to verify GREEN**

Run: `mvn -pl yierdis-server,yierdis-client -am -Dtest=ProtocolCommandAdapterTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest,YierdisClientTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java \
        yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyExecutorTask.java \
        yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
        yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java
git commit -m "refactor: move server handoff to execution requests"
```

### Task 5: Lock The New Boundaries And Update Repository Documentation

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `README.md`

- [ ] **Step 1: Add failing guard tests that forbid the old production request path**

```java
Assert.assertTrue(
        YierdisFastCommandProcessor.class
                .getDeclaredMethod("execute", Command.class, CommandContext.class)
                .isAnnotationPresent(Deprecated.class)
);
```

```java
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java"),
        offenders,
        "new AdaptedCommand("
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java"),
        offenders,
        "SimpleChannelInboundHandler<Command>"
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java"),
        offenders,
        "ArrayList<byte[][]>"
);
scanFileForForbiddenText(
        repoRoot,
        repoRoot.resolve("yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java"),
        offenders,
        "byte[][] argv"
);
```

- [ ] **Step 2: Run boundary-only tests to verify RED**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime -am -Dtest=CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `execute(Command, CommandContext)` is not marked `@Deprecated` yet and the new guard patterns are not locked in.

- [ ] **Step 3: Mark the remaining `Command` entrypoint as compatibility-only and update README ownership docs**

```java
/**
 * Transitional compatibility overload for existing embedders and tests.
 * Production code should call {@link #execute(ExecutionRequest, CommandContext)}.
 */
@Deprecated(forRemoval = false)
public void execute(Command cmd, CommandContext ctx) {
    execute((ExecutionRequest) cmd, ctx);
}
```

```markdown
- **执行契约（ExecutionRequest/ExecutionRecord/ReplyWriter/Session 契约）**：统一放在 `yierdis-core-contract`（包名 `yier.bubu.redis.contract.*`）。
- **协议请求适配**：`CustomProtocolV1Request` 仅由 `yierdis-server` 适配为 `ExecutionRequest`；事务回放与变更事件统一复用 `ExecutionRecord`，不再引入新的 argv 容器。
```

- [ ] **Step 4: Run the final cross-module regression gate**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server,yierdis-client -am -Dtest=ExecutionRequestContractTest,ProtocolCommandAdapterTest,YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,TransactionCommandTest,YierdisChangeSinkTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest,YierdisClientTest,CoreCommandBoundaryGuardTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
        yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        README.md
git commit -m "test: lock execution request architecture boundaries"
```
