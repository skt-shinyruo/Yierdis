# Command Pipeline Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the pass-through engine/parser/preparer/writer command chain with one sealed command registry, one dispatcher, one parse-to-invocation contract, and centrally rendered semantic replies while preserving all external behavior and ownership guarantees.

**Architecture:** `server-api` owns semantic replies, prepared execution, rendering, and result contracts; `command-api` owns syntax, arguments, handlers, invocations, and specifications; `command-core` owns sealed registration, dispatch, transaction queueing, and replay. Builtin commands return semantic values, the executor alone reserves and renders them, and `server-main` wires `CommandDispatcher::prepare` directly while scheduling runtime maintenance independently.

**Tech Stack:** Java 25, Maven reactor, JUnit 4, Netty 4.1, RESP2/RESP3, FFM-backed storage, existing `ExecutionRequest`, `MutationContext`, ordered reply slots, and explicit `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` for every Java or Maven command.

## Global Constraints

- This plan implements only stage 1 of the approved four-stage simplification program: request and command path.
- Preserve Java 25, Netty, RESP2 and basic RESP3, one serial owner thread, FFM-backed storage, TTL, maxmemory, transactions, backpressure, and explicit resource ownership.
- Keep the Netty thread limited to decode/submit work; command execution and DB access remain on the serial owner thread.
- Preserve supported commands, wire replies, error text, configuration, transaction behavior, and connection-close behavior.
- Do not change decoder, ingress, executor scheduling, fairness, backpressure, reply-gate, shutdown, DB semantics, mutation staging, memory accounting, native storage, or FFM lifecycle behavior.
- Do not merge, rename, add, or remove Maven modules. Keep the current Maven dependency graph; dependency/topology simplification belongs to stage 4.
- Internal Java source and binary compatibility is not required. Temporary adapters are allowed only in intermediate compiling commits and must be absent from the final tree.
- Do not use deprecated compatibility APIs, annotations, reflection, or code generation to discover or register commands; the final tree has one explicit command path only.
- Parsing receives only `CommandArgs`; it must not call the DB router, session, mutation context, server provider, or any captured runtime service.
- A mutation starts only after its reply envelope is reserved. Capacity waiting reuses the same prepared command; stale work is closed before re-preparation; execution is never retried after it starts.
- Transaction queueing retains `ExecutionRequest`; it must not create a second transaction-only command representation.
- Bulk, sequence, and map replies stay streamable. Do not materialize a second complete payload, and keep RESP sizing in `yierdis-networking-resp`.
- Every request, prepared command, retained source, transaction child, and reply slot has one owner and is closed or transferred exactly once on success and every failure path.
- Builtin command implementations must not import or invoke `RedisReplyWriter` after their migration task.
- Use a conventional JDK map sealed before traffic. Runtime command-name string allocation is accepted.
- Performance, throughput, latency, allocation counts, and benchmark results are not acceptance gates.
- Touched production Java files must show a net line-count reduction relative to commit `50b01059`.
- Every Java and Maven command uses JDK 25. No ignored, disabled, or quarantined regression test counts as completion.

---

## File Structure And Migration Map

### Shared semantic reply and prepared execution contracts

- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReply.java`: sealed protocol-neutral reply data, shape, and streaming emission contracts.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplies.java`: validated factories for every scalar, aggregate, bulk, streamed sequence, streamed map, and control-error reply.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java`: the only production traversal from `RedisReply` to `RedisReplyWriter`.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandResult.java`: semantic reply plus close-after-reply decision.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommands.java`: ready, action, owned, validation, and cleanup factories.
- Modify `PreparedCommand.java`: final `reservationShape()`, `validateBeforeExecute()`, and `CommandResult execute(CommandExecutionContext)` contract.
- Modify `CommandExecutionContext.java`: retain only `CommandSession` and request-scoped `MutationContext`.
- Keep `ReplyShape`, `ReplyShapes`, `ReplySizer`, `RedisReplyWriter`, and `RedisReplyWriterFactory`; the writer remains the protocol adapter used by the renderer and non-command protocol-error paths.
- Delete `CommandPreparationContext.java` after all call sites use `CommandSession` directly.

### Command API and dispatcher

- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArgs.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseException.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandHandler.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandInvocation.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java`.
- Modify `CommandArity.java`, `CommandModule.java`, `ServerInfoProvider.java`, and `SlowCommandGovernor.java` to use the final contracts.
- Rewrite `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java` around a sealed `Map<String, CommandSpec>`.
- Create `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java` as the sole command-layer entry.
- Rewrite `CommandRegistries.java` and `TransactionCommands.java` for direct dispatcher composition and replay.
- Delete `CommandDefinition.java`, `CommandParser.java`, `CommandPreparer.java`, `CommandParsers.java`, `CommandParseResult.java`, `CommandParseError.java`, and `ArgReader.java` after the final builtin migration.
- Delete `YierdisFastCommandProcessor.java`, `TransactionQueuePolicy.java`, `CommandExceptionTranslator.java`, `CommandRequestSupport.java`, and the command-core duplicate `PreparedCommands.java`.

### Builtin commands and DB-result adapters

- Create `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DbReplies.java`: adapt `ByteValue`, `ByteSequenceSource`, and `ByteMapSource` to semantic streaming replies without adding a DB dependency to `server-api`.
- Modify `BulkStringReplyAdapter.java` to depend on `ReplySink`.
- Reduce `CommandSupport.java` to DB routing, command-facing services, and expected DB-command failure translation; remove its reply writer, ASCII, integer, request-slice scratch, and prepared-command duplication.
- Migrate `CoreConnectionCommands.java`, `StringCommands.java`, `KeyCommands.java`, `ListCommands.java`, `HashCommands.java`, `SetCommands.java`, `ZSetCommands.java`, `HllCommands.java`, and `CollectionScanCommandSupport.java` to `CommandSpec` and semantic replies.

### Server, executor, tests, and docs

- Modify `ServerCommandModule.java`, `NettyServerInfoProvider.java`, `ServerCommandComposition.java`, and `YierdisServerBootstrap.java` for semantic server replies and direct dispatcher wiring.
- Delete `YierdisEngine.java`, `DefaultYierdisEngine.java`, and `DefaultYierdisEngineTest.java`; keep `EngineSession.java` and its Maven module unchanged.
- Modify `CommandExecutorExecutionSupport.java`, executor fixtures, RESP reply tests, integration composition helpers, and `FastTestClient.java` for central rendering.
- Rename test helpers from processor/engine terminology to dispatcher terminology where they are touched; do not retain forwarding aliases.
- Update `ArchitectureBoundaryTest.java` and current project documentation so the target path is the only documented and guarded path.

## Final Interfaces

The implementation must converge on these exact public contracts:

```java
public record CommandSpec(CommandSyntax syntax, CommandHandler handler) {
}

@FunctionalInterface
public interface CommandHandler {
    CommandInvocation parse(CommandArgs args) throws CommandParseException;
}

@FunctionalInterface
public interface CommandInvocation {
    PreparedCommand prepare(CommandSession session);
}

public interface PreparedCommand extends AutoCloseable {
    ReplyShape reservationShape();
    ValidationResult validateBeforeExecute();
    CommandResult execute(CommandExecutionContext context);
    @Override
    void close();
}

public record CommandResult(RedisReply reply, boolean closeAfterReply) {
    public CommandResult {
        Objects.requireNonNull(reply, "reply");
    }

    public static CommandResult reply(RedisReply reply) {
        return new CommandResult(reply, false);
    }

    public static CommandResult error(String message) {
        return reply(RedisReplies.error(message));
    }

    public static CommandResult controlError(String message) {
        return reply(RedisReplies.controlError(message));
    }

    public static CommandResult closeAfterReply(RedisReply reply) {
        return new CommandResult(reply, true);
    }
}
```

`CommandExecutionContext` has only these command-facing accessors:

```java
public CommandSession session();
public MutationContext mutationContext();
```

`CommandDispatcher` exposes one public entry and one package-private replay entry:

```java
public PreparedCommand prepare(CommandSession session, ExecutionRequest request);
PreparedCommand prepareReplay(CommandSession session, ExecutionRequest request);
```

---

### Task 1: Introduce Semantic Replies And Central Rendering

**Files:**

- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReply.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplies.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandResult.java`
- Test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/RedisReplyTest.java`
- Test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/RedisReplyRendererTest.java`

**Interfaces:**

- Consumes: existing `ReplyShape`, `ReplyShapes`, `ReplySink`, and `RedisReplyWriter`.
- Produces: `RedisReply.shape()`, `RedisReply.PayloadEmitter`, all `RedisReplies` factories, `RedisReplyRenderer.render(...)`, and `CommandResult` for Tasks 2-14.

- [ ] **Step 1: Write failing semantic-value tests**

Create table-driven tests that assert both data and shape for every reply kind. Include these representative assertions and add equivalent cases for boolean, double, big number, verbatim string, blob error, null array, set, push, attribute, and control error:

```java
@Test
public void exactRepliesCarryTheirOwnShape() {
    RedisReply simple = RedisReplies.simpleString("OK");
    RedisReply bulk = RedisReplies.bulkString("value".getBytes(StandardCharsets.US_ASCII));
    RedisReply nested = RedisReplies.array(List.of(
            simple,
            RedisReplies.map(List.of(RedisReplies.bulkString(bytes("k")), RedisReplies.integer(7)))
    ));

    Assert.assertEquals(ReplyShapes.simpleString("OK"), simple.shape());
    Assert.assertEquals(ReplyShapes.bulkString(5, 0), bulk.shape());
    Assert.assertEquals(ReplyShape.AggregateKind.ARRAY,
            ((ReplyShape.Aggregate) nested.shape()).kind());
}

@Test
public void streamingReplyDoesNotMaterializeItsPayload() {
    AtomicInteger emitted = new AtomicInteger();
    RedisReply reply = RedisReplies.sequence(
            2,
            19L,
            lengths -> { lengths.accept(1); lengths.accept(-1); },
            sink -> emitted.incrementAndGet()
    );

    Assert.assertEquals(0, emitted.get());
    Assert.assertEquals(19L, reply.shape().retainedSourceBytes());
}
```

- [ ] **Step 2: Run the new tests and verify the missing types fail compilation**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api -am -Dtest=RedisReplyTest,RedisReplyRendererTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL during test compilation because `RedisReply`, `RedisReplies`, `RedisReplyRenderer`, and `CommandResult` do not exist.

- [ ] **Step 3: Implement the sealed reply algebra and factories**

Use this variant surface; constructors/factories must reject negative counts, negative retained bytes, odd map elements, null emitters, and null aggregate elements:

```java
public sealed interface RedisReply permits
        RedisReply.SimpleString, RedisReply.Error, RedisReply.ControlError,
        RedisReply.IntegerValue, RedisReply.BooleanValue, RedisReply.DoubleValue,
        RedisReply.BigNumber, RedisReply.VerbatimString, RedisReply.BlobError,
        RedisReply.BulkString, RedisReply.NullValue, RedisReply.NullArray,
        RedisReply.Aggregate, RedisReply.ByteSequence, RedisReply.ByteMap {

    ReplyShape shape();

    @FunctionalInterface
    interface PayloadEmitter {
        void emit(ReplySink sink);
    }

    record SimpleString(String value) implements RedisReply { }
    record Error(String message) implements RedisReply { }
    record ControlError(String message) implements RedisReply { }
    record IntegerValue(long value) implements RedisReply { }
    record BooleanValue(boolean value) implements RedisReply { }
    record DoubleValue(double value) implements RedisReply { }
    record BigNumber(String ascii) implements RedisReply { }
    record VerbatimString(String format, byte[] data) implements RedisReply { }
    record BlobError(String message) implements RedisReply { }
    record BulkString(int payloadLength, long retainedSourceBytes, PayloadEmitter emitter)
            implements RedisReply { }
    record NullValue() implements RedisReply { }
    record NullArray() implements RedisReply { }
    record Aggregate(ReplyShape.AggregateKind kind, List<RedisReply> elements)
            implements RedisReply { }
    record ByteSequence(int elementCount, long retainedSourceBytes,
                        ReplyShape.PayloadLengths payloadLengths, PayloadEmitter emitter)
            implements RedisReply { }
    record ByteMap(int pairCount, long retainedSourceBytes,
                   ReplyShape.PayloadLengths payloadLengths, PayloadEmitter emitter)
            implements RedisReply { }
}
```

`RedisReplies.bulkString(byte[])` must clone its input before capture. The length/emitter overload must retain streaming behavior:

```java
public static RedisReply bulkString(
        int payloadLength,
        long retainedSourceBytes,
        RedisReply.PayloadEmitter emitter
);
```

Make `ControlError.shape()` return `ReplyShapes.maximum()` so top-level execution errors select the control reservation. Ordinary `Error.shape()` remains exact.

- [ ] **Step 4: Implement the exhaustive renderer and result factories**

Implement one switch in `RedisReplyRenderer`:

```java
public static void render(RedisReply reply, RedisReplyWriter out) {
    Objects.requireNonNull(reply, "reply");
    Objects.requireNonNull(out, "out");
    switch (reply) {
        case RedisReply.SimpleString value -> out.simpleString(value.value());
        case RedisReply.Error value -> out.error(value.message());
        case RedisReply.ControlError value -> out.controlError(value.message());
        case RedisReply.IntegerValue value -> out.integer(value.value());
        case RedisReply.BooleanValue value -> out.booleanValue(value.value());
        case RedisReply.DoubleValue value -> out.doubleValue(value.value());
        case RedisReply.BigNumber value -> out.bigNumberAscii(value.ascii());
        case RedisReply.VerbatimString value -> out.verbatimString(value.format(), value.data());
        case RedisReply.BlobError value -> out.blobError(value.message());
        case RedisReply.BulkString value -> value.emitter().emit(out);
        case RedisReply.NullValue ignored -> out.nullValue();
        case RedisReply.NullArray ignored -> out.nullArray();
        case RedisReply.Aggregate value -> renderAggregate(value, out);
        case RedisReply.ByteSequence value -> {
            out.arrayHeader(value.elementCount());
            value.emitter().emit(out);
        }
        case RedisReply.ByteMap value -> {
            out.mapHeader(value.pairCount());
            value.emitter().emit(out);
        }
    }
}
```

`renderAggregate` writes the matching array/map/set/push/attribute header and recursively renders elements in order. Add `CommandResult.reply`, `CommandResult.error`, `CommandResult.controlError`, and `CommandResult.closeAfterReply` factories; only the last factory sets the flag.

- [ ] **Step 5: Run semantic reply tests**

Run the Step 2 command.

Expected: PASS; the recording writer sees the exact scalar/header/emission order, `ControlError` calls only `controlError`, and no stream emits while its shape is inspected.

- [ ] **Step 6: Commit the semantic reply model**

```bash
git add yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReply.java yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplies.java yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyRenderer.java yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandResult.java yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/RedisReplyTest.java yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/RedisReplyRendererTest.java
git commit -m "feat: add semantic command replies"
```

### Task 2: Centralize Prepared Command Construction Behind A Temporary Renderer Bridge

**Files:**

- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommands.java`
- Test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/PreparedCommandsTest.java`

**Interfaces:**

- Consumes: Task 1 `CommandResult` and `RedisReplyRenderer`; current pre-flip `PreparedCommand.replyShape()` and `void execute(CommandExecutionContext)`.
- Produces: stable factory call sites that Task 14 can switch internally to the final result-returning contract without revisiting every command.

- [ ] **Step 1: Write failing factory lifecycle tests**

Cover `ready`, `action`, `owned`, and `ownedAction` with these counters:

```java
@Test
public void ownedActionValidatesExecutesAndClosesExactlyOnce() {
    AtomicInteger validated = new AtomicInteger();
    AtomicInteger executed = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    PreparedCommand prepared = PreparedCommands.ownedAction(
            ReplyShapes.integerUpperBound(),
            closed::incrementAndGet,
            () -> { validated.incrementAndGet(); return ValidationResult.VALID; },
            context -> {
                executed.incrementAndGet();
                return CommandResult.reply(RedisReplies.integer(3));
            }
    );

    Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
    executeWithRecordingWriter(prepared);
    prepared.close();
    prepared.close();
    Assert.assertEquals(1, validated.get());
    Assert.assertEquals(1, executed.get());
    Assert.assertEquals(1, closed.get());
}
```

Add tests proving a stale command does not run, an action returning null fails, an action throwing does not close early, checked `AutoCloseable` failures become `IllegalStateException`, and a close failure never permits a second close attempt.

- [ ] **Step 2: Run the factory test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api -am -Dtest=PreparedCommandsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL during test compilation because `PreparedCommands` does not exist.

- [ ] **Step 3: Implement the stable factories with the old execution adapter isolated inside one class**

Expose exactly these methods:

```java
public static PreparedCommand ready(RedisReply reply);
public static PreparedCommand ready(CommandResult result);
public static PreparedCommand action(
        ReplyShape reservationShape,
        Function<CommandExecutionContext, CommandResult> action
);
public static PreparedCommand owned(CommandResult result, AutoCloseable owner);
public static PreparedCommand ownedAction(
        ReplyShape reservationShape,
        AutoCloseable owner,
        Supplier<ValidationResult> validation,
        Function<CommandExecutionContext, CommandResult> action
);
```

For this intermediate commit only, `execute` obtains the `CommandResult`, calls `RedisReplyRenderer.render(result.reply(), context.reply())`, and calls `context.reply().requestCloseAfterReply()` when the result flag is true. Put both calls in `PreparedCommands`; commands must never reproduce this bridge.

- [ ] **Step 4: Run the factory and server-api tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api -am test
```

Expected: PASS, including existing `CoreContractSmokeTest` under the unchanged execution contract.

- [ ] **Step 5: Commit the prepared factory bridge**

```bash
git add yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommands.java yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/PreparedCommandsTest.java
git commit -m "refactor: centralize prepared command lifecycle"
```

### Task 3: Add The Parse-To-Invocation Command API And One Argument Reader

**Files:**

- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArgs.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseException.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandHandler.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandInvocation.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArity.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandModule.java`
- Create test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandArgsTest.java`
- Create test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandSpecTest.java`
- Modify test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandContractTest.java`

**Interfaces:**

- Consumes: `ExecutionRequest`, `BytesSlice`, `CommandSession`, `PreparedCommand`, existing `CommandSyntax`, and existing `CommandArity` metadata.
- Produces: the exact `CommandSpec -> CommandHandler -> CommandInvocation` contract and one argument implementation for every later command migration.

- [ ] **Step 1: Write failing `CommandArgs` behavior tests**

Test null, byte, slice, case-insensitive ASCII, UTF-8, integer endpoints, overflow, sign-only values, range helpers, and immutable byte lists:

```java
@Test
public void parsesLongEndpointsAndRejectsOverflowWithExplicitRedisError() throws Exception {
    Assert.assertEquals(Long.MIN_VALUE, args("CMD", Long.toString(Long.MIN_VALUE)).longAt(1));
    Assert.assertEquals(Long.MAX_VALUE, args("CMD", Long.toString(Long.MAX_VALUE)).longAt(1));

    for (String invalid : List.of("", "+", "-", "9223372036854775808", "x")) {
        CommandParseException failure = Assert.assertThrows(
                CommandParseException.class,
                () -> args("CMD", invalid).longAt(1)
        );
        Assert.assertEquals("ERR value is not an integer or out of range", failure.replyMessage());
    }
}

@Test
public void sliceReadsTheRequestWithoutOwningOrClosingIt() {
    TrackingRequest request = request("CMD", "value");
    CommandArgs args = CommandArgs.of(request);
    Assert.assertEquals(5, args.slice(1).length());
    Assert.assertEquals('v', args.slice(1).getByte(0));
    Assert.assertEquals(0, request.closeCount());
}
```

- [ ] **Step 2: Write failing command contract tests**

Assert `CommandSpec` rejects null syntax/handler, invalid `CommandSyntax` metadata (blank/non-ASCII name and null arity/key/transaction policy) fails immediately, `CommandParseException` retains its exact reply message, `CommandArity.validate` throws the canonical lower-case arity message, and parsing has no session parameter:

```java
@Test
public void handlerOnlyReceivesCommandArgs() throws Exception {
    Method parse = CommandHandler.class.getMethod("parse", CommandArgs.class);
    Assert.assertEquals(CommandInvocation.class, parse.getReturnType());
    Assert.assertArrayEquals(new Class<?>[]{CommandArgs.class}, parse.getParameterTypes());
}
```

- [ ] **Step 3: Run command-api tests and verify missing types fail compilation**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-api -am -Dtest=CommandArgsTest,CommandSpecTest,CommandContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL during test compilation on the new contract names.

- [ ] **Step 4: Implement `CommandArgs` and explicit parse errors**

Expose this final surface:

```java
public static CommandArgs of(ExecutionRequest request);
public ExecutionRequest request();
public int argc();
public boolean isNull(int index);
public int length(int index);
public byte byteAt(int index, int offset);
public BytesSlice slice(int index);
public byte[] bytes(int index);
public String utf8(int index);
public boolean is(int index, String asciiLiteral);
public long longAt(int index) throws CommandParseException;
public long nonNegativeLongAt(int index) throws CommandParseException;
public long positiveLongAt(int index) throws CommandParseException;
public int intClampedAt(int index) throws CommandParseException;
public List<byte[]> byteArraysFrom(int firstIndex);
```

`bytes` delegates to `readOnlyByteArray`; `slice` is a small request/index view; `byteArraysFrom` returns an unmodifiable list of those read-only arrays. None of these methods retains or closes the request. All index violations remain framework defects; only user-controlled numeric failures become `CommandParseException`.

- [ ] **Step 5: Implement the handler, invocation, spec, arity, and registration surfaces**

Use the exact final interfaces shown above. Change `CommandArity` from returning `CommandParseError` to:

```java
public void validate(String commandLower, CommandArgs args) throws CommandParseException {
    if (!accepts(args.argc())) {
        throw new CommandParseException(
                "ERR wrong number of arguments for '" + commandLower + "' command"
        );
    }
}
```

Add `void register(CommandSpec spec)` and `CommandSpec specByUpperName(String nameUpper)` to `CommandModule.Registration` while retaining the legacy registration overload only until Task 15. Do not add session-aware parser overloads.

- [ ] **Step 6: Run all command-api tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-api -am test
```

Expected: PASS; legacy tests still compile during migration and all new final contracts are covered.

- [ ] **Step 7: Commit the new command API**

```bash
git add yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api
git commit -m "feat: add direct command handler contracts"
```

### Task 4: Replace The Custom Registry And Split Policies With One Dispatcher

**Files:**

- Create: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandDispatcher.java`
- Create temporarily: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/LegacyCommandAdapter.java`
- Rewrite: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java`
- Modify temporarily: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionQueuePolicy.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandExceptionTranslator.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRequestSupport.java`
- Create test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/CommandRegistryTest.java`
- Create test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/CommandDispatcherTest.java`
- Replace test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java` with dispatcher assertions in `CommandDispatcherTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandSyntaxRegistryTest.java`

**Interfaces:**

- Consumes: Task 3 command contracts, `TransactionState`, `WrongTypeException`, `YierdisCommandException`, and Task 2 prepared factories.
- Produces: sealed normalized registry, direct preparation, transaction preflight/queueing, and replay. The temporary processor and legacy adapter exist only so unmigrated command modules compile.

- [ ] **Step 1: Write failing sealed-registry tests**

Cover metadata-name normalization, duplicate/null rejection, sorting, metadata identity, seal-before-lookup, and registration-after-seal:

```java
@Test
public void sealFreezesOneNormalizedSpecMap() {
    CommandRegistry registry = new CommandRegistry();
    CommandSpec ping = spec("ping", CommandArity.exact(1), args -> session ->
            PreparedCommands.ready(RedisReplies.simpleString("PONG")));
    registry.register(ping);
    registry.seal();

    Assert.assertSame(ping, registry.specByUpperName(" PiNg "));
    Assert.assertArrayEquals(new String[]{"PING"}, registry.upperNamesSorted());
    Assert.assertThrows(IllegalStateException.class, () -> registry.register(spec("GET")));
}
```

- [ ] **Step 2: Write failing table-driven dispatcher tests**

Use a recording `CommandSession`/`TransactionState` and assert these cases with exact replies and handler counters:

| Request/state | Reply | Parse calls | Prepare calls | Transaction effect |
| --- | --- | ---: | ---: | --- |
| empty argv or empty/null argv[0] | `ERR empty command` | 0 | 0 | abort active tx after reservation |
| illegal null argument | `ERR Protocol error: null bulk string` | 0 | 0 | abort active tx after reservation |
| unknown printable command | `ERR unknown command 'NAME'` | 0 | 0 | abort active tx after reservation |
| wrong arity | canonical lower-case arity error | 0 | 0 | abort active tx after reservation |
| handler throws `CommandParseException` | exception reply text | 1 | 0 | abort active tx after reservation |
| queueable command in `MULTI` | `QUEUED` | 1 | 0 | retained request added after reservation |
| disallowed command in `MULTI` | `ERR NAME is not allowed in MULTI` | 0 | 0 | abort after reservation |
| transaction control | handler result | 1 | 1 | immediate |
| replay entry | handler result | 1 | 1 | never queues again |

Add fault cases for parse, prepare, `tryEnqueue`, and close. Assert arbitrary `IllegalArgumentException` escapes as an internal defect, while `WrongTypeException` and `YierdisCommandException` become semantic errors.
Add a request-path case proving `argv[0] = " PING "` remains an unknown command even though metadata lookup `specByUpperName(" PiNg ")` trims and normalizes its string argument.

- [ ] **Step 3: Run dispatcher tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-core -am -Dtest=CommandRegistryTest,CommandDispatcherTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the JDK-map registry and dispatcher do not exist.

- [ ] **Step 4: Rewrite `CommandRegistry` around a sealable JDK map**

Use one mutable `LinkedHashMap<String, CommandSpec>` during composition and `Map.copyOf` on `seal()`. Registration keys come from the already validated `CommandSyntax.nameUpper()`. Preserve the current metadata API behavior by normalizing `specByUpperName` and `containsUpperName` string arguments with `trim().toUpperCase(Locale.ROOT)`. Remove FNV hashing, table slots, masks, resize thresholds, byte-name arrays, and request-byte lookup.

During migration only, implement `register(CommandDefinition<?>)` by converting through `LegacyCommandAdapter`; the adapter must call the legacy parser using the same request, translate `CommandParseError.toReplyMessage()` into `CommandParseException`, and return an invocation that invokes the legacy preparer with `new CommandPreparationContext(session)`. Mark its deletion in Task 15 by filename, not with deprecation APIs.

- [ ] **Step 5: Implement the direct dispatcher sequence**

Implement this order in one class:

```java
public PreparedCommand prepare(CommandSession session, ExecutionRequest request) {
    return prepare(session, request, true);
}

PreparedCommand prepareReplay(CommandSession session, ExecutionRequest request) {
    return prepare(session, request, false);
}
```

The private method performs empty/null validation, exact-byte command-name conversion to uppercase ASCII without trimming, lookup, arity validation, transaction policy, `handler.parse(args)`, and `invocation.prepare(session)`. A non-ASCII command byte produces no lookup match and follows the existing safe unknown-command message path; it is not a normalization exception. Queueable preflight parses but does not prepare. Use the shared `yier.bubu.redis.execution.api.PreparedCommands.action(...)` to delay `markAborted()` and `tryEnqueue(request)` until after reservation. Catch only `CommandParseException`, `WrongTypeException`, and `YierdisCommandException`; remove the broad `IllegalArgumentException` translation.

The existing command-core class with the same `PreparedCommands` simple name remains only for unmigrated code and tests, so new command-core code in Tasks 4-5 must use the shared factory's fully qualified name. Task 6 deletes the shadowing class and replaces those fully qualified references with a normal import.

- [ ] **Step 6: Turn `YierdisFastCommandProcessor` into a temporary one-method delegate and remove split policy classes**

Its temporary body is limited to:

```java
public final class YierdisFastCommandProcessor {
    private final CommandDispatcher dispatcher;

    public YierdisFastCommandProcessor(CommandRegistry registry) {
        dispatcher = new CommandDispatcher(registry);
    }

    public PreparedCommand prepare(ExecutionRequest request, CommandPreparationContext context) {
        return dispatcher.prepare(context.session(), request);
    }

    PreparedCommand prepareQueued(ExecutionRequest request, CommandPreparationContext context) {
        return dispatcher.prepareReplay(context.session(), request);
    }
}
```

Delete the three policy/helper classes listed above. Make every production and test composition explicitly call `registry.seal()` after all current modules register.

- [ ] **Step 7: Run command-core and registry integration tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-core,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandRegistryTest,CommandDispatcherTest,CommandSyntaxRegistryTest,DefaultCommandDispatchErrorTest,TransactionCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with unchanged error strings and transaction outcomes.

- [ ] **Step 8: Commit the dispatcher and registry replacement**

```bash
git add yierdis-command/yierdis-command-core yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandSyntaxRegistryTest.java
git commit -m "refactor: replace command processor policies with dispatcher"
```

### Task 5: Move Transaction Control And Replay Onto The Dispatcher

**Files:**

- Rewrite: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`
- Rewrite: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Extend test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/CommandDispatcherTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TransactionQueueCleanupTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java`

**Interfaces:**

- Consumes: `CommandDispatcher.prepareReplay`, retained `ExecutionRequest`, and the current pre-flip `PreparedCommand.replyShape()` plus `void execute(CommandExecutionContext)` contract.
- Produces: `MULTI`, `DISCARD`, and `EXEC` as `CommandSpec`; dispatcher-owned replay; single-child exact planning; multi-child maximum planning; and exhaustive cleanup through the temporary writer-based execution bridge.

- [ ] **Step 1: Add failing transaction parse-isolation and ownership tests**

Add cases that prove queue preflight calls only `handler.parse`, retains the original request once only after `QUEUED` executes, and does not call `CommandInvocation.prepare`. Keep the assertions on the pre-flip writer contract in this task:

```java
@Test
public void multiChildExecUsesTheWriterBridgeAndClosesEveryOwnerOnce() {
    TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
    TrackingPrepared first = writingChild(
            ReplyShapes.bulkString(3, 0), out -> out.bulkString(bytes("one")));
    TrackingPrepared second = writingChild(
            ReplyShapes.integer(2), out -> out.integer(2));
    PreparedCommand exec = prepareExec(tx, first, second);

    CapturingReplyWriter out = executeWithWriter(exec);
    Assert.assertEquals(List.of("array:2", "bulk:one", "integer:2"), out.events());
    Assert.assertEquals(1, first.closeCount());
    Assert.assertEquals(1, second.closeCount());
    Assert.assertEquals(1, tx.request(0).closeCount());
    Assert.assertEquals(1, tx.request(1).closeCount());

    exec.close();
    Assert.assertEquals(1, first.closeCount());
    Assert.assertEquals(1, second.closeCount());
}
```

Cover a stale single child, a stale dynamically prepared child, child parse/prepare/validate/execute failure, close failure suppression, unconsumed queue-tail cleanup, and writer event order. At this stage a successful child is closed after its direct writer call returns, because rendering is still part of `PreparedCommand.execute`; do not assert semantic child results, retained post-execute sources, `ControlError` conversion, or result-based `closeAfterReply` propagation until Task 14.

- [ ] **Step 2: Run the transaction-focused tests and verify the new assertions fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandDispatcherTest,TransactionCommandTest,TransactionQueueCleanupTest,ReplyPreflightCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `TransactionCommands` still registers legacy definitions, depends on the temporary processor for replay, and does not satisfy the new dispatcher/cleanup assertions.

- [ ] **Step 3: Register transaction commands through `CommandSpec`**

Use one registration expression per command:

```java
registration.register(new CommandSpec(syntax("MULTI"), this::multi));
registration.register(new CommandSpec(syntax("DISCARD"), this::discard));
registration.register(new CommandSpec(syntax("EXEC"), this::exec));
```

`multi`, `discard`, and `exec` return `CommandInvocation`. Session checks occur in `invocation.prepare(session)`, while parse remains session-free. `MULTI` and `DISCARD` use result actions returning `OK`; aborted `EXEC` returns the exact `EXECABORT Transaction discarded because of previous errors.` reply and discards after reservation.

- [ ] **Step 4: Rebuild `PreparedExec` around dispatcher replay and the temporary writer bridge**

Keep the existing planning split:

```java
if (tx.size() <= 1) {
    // Prepare the exact child before reservation and aggregate its reservation shape.
    return preparedExactExec(tx, dispatcher, session);
}
// Earlier mutations can change later replies, so reserve maximum and prepare in order.
return preparedDynamicExec(tx, dispatcher, session);
```

Keep `PreparedExec.execute` returning `void` in this task. It drains the retained requests, writes the array header to `context.reply()`, and executes each child with a request-scoped child context that uses the same writer. The exact path reuses its pre-prepared child; the dynamic path calls `dispatcher.prepareReplay(session, request)` in order and closes a stale child before preparing that same request again. Close each successful child and queued request only after its writer call returns, then close the unconsumed tail and remaining children on failure. Preserve the primary throwable and attach later cleanup failures as suppressed exceptions.

Do not inspect or collect `CommandResult`, build a semantic aggregate, invoke `RedisReplyRenderer` from `TransactionCommands`, or retain successfully rendered children past `execute` in this task. Those changes require the final result-returning execution contract and belong together in Task 14.

- [ ] **Step 5: Make `CommandRegistries.dispatcher(...)` the complete composition operation**

Expose these final factories:

```java
public static CommandDispatcher dispatcher(CommandModule... modules);
public static CommandDispatcher dispatcher(Iterable<? extends CommandModule> modules);
```

Each creates one registry and dispatcher, registers `new TransactionCommands(dispatcher)` first, registers supplied modules, seals the registry, and returns the dispatcher. Keep the old `registerInto` methods only until Task 6 updates every composition call.

- [ ] **Step 6: Run the transaction-focused tests**

Run the Step 2 command.

Expected: PASS; single-child replies use exact reservation, multiple children use maximum reservation, parse-invalid children abort before queueing, and every tracked owner closes once.

- [ ] **Step 7: Commit dispatcher-owned transactions**

```bash
git add yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/CommandDispatcherTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TransactionQueueCleanupTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java
git commit -m "refactor: move transactions into command dispatcher"
```

### Task 6: Wire The Dispatcher Directly And Delete Engine And Processor Facades

**Files:**

- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Delete: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java`
- Delete: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- Delete: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/PreparedCommands.java`
- Delete: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorArchitectureTest.java`
- Delete: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java`
- Delete: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`
- Delete: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`
- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestCommandDispatchers.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCloseTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`
- Modify: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/EmbeddedCommandComposition.java`
- Rename: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java` to `TestCommandDispatchers.java`
- Rename: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java` to `TestCommandDispatchers.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandComposition.java`
- Modify: all integration tests importing either renamed helper or `YierdisFastCommandProcessor`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`

**Interfaces:**

- Consumes: Task 5 `CommandRegistries.dispatcher(...)` and the executor's existing `CommandExecutionEngine` functional boundary.
- Produces: production flow `CommandExecutor -> CommandDispatcher::prepare`, with maintenance owned directly by bootstrap/runtime. No engine or processor facade remains.

- [ ] **Step 1: Change wiring tests to require the direct path**

Update `YierdisServerBootstrapCommandWiringTest` so it asserts:

```java
CommandDispatcher dispatcher = ServerCommandComposition.createDispatcher(router, info, governor);
CommandExecutionEngine execution = dispatcher::prepare;
PreparedCommand prepared = execution.prepare(session, request("PING"));
Assert.assertNotNull(prepared);
```

Add source assertions that `YierdisServerBootstrap.java` contains `dispatcher::prepare` and a direct `maintenanceTick.run()` inside `executeMaintenance`, and contains no `YierdisEngine`, `DefaultYierdisEngine`, or `YierdisFastCommandProcessor`.

- [ ] **Step 2: Run wiring tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest,NettyExecutionAdapterIntegrationTest,ContractsIntegrationSmokeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because composition still returns the processor and bootstrap still creates the engine facade.

- [ ] **Step 3: Return a fully sealed dispatcher from every composition root**

Change the production signature to:

```java
public static CommandDispatcher createDispatcher(
        YierdisDbRouter dbRouter,
        ServerInfoProvider infoProvider,
        SlowCommandGovernor slowGovernor
);
```

Its body calls `CommandRegistries.dispatcher(DefaultCommandModules.create(...), new ServerCommandModule(infoProvider))`. Apply the same direct factory to embedded and integration composition helpers. Remove the old `registerInto` and `registerTransactionSupport` methods once no caller remains.

- [ ] **Step 4: Remove engine ownership from bootstrap without changing lifecycle order**

Keep `CommandExecutionEngine` and the test decorator. Build it with:

```java
CommandDispatcher dispatcher = ServerCommandComposition.createDispatcher(
        dbRouter(instance), infoProvider, slowGovernor);
CommandExecutionEngine executionEngine = Objects.requireNonNull(
        commandEngineDecorator.apply(dispatcher::prepare),
        "commandEngineDecorator result"
);
```

Store no dispatcher or engine field because neither is closeable. Keep `Runnable maintenanceTick = new YierdisInstanceMaintenance(instance)::maintenanceTick` in bootstrap and invoke it inside the existing coalesced `executor.executeMaintenance` callback. Remove only the engine close block; do not reorder executor, child, budget, runtime, or event-loop shutdown.

- [ ] **Step 5: Convert test clients and helpers directly to `CommandDispatcher`**

`FastTestClient` holds `CommandDispatcher`, calls `dispatcher.prepare(session, request)`, and retains its current temporary recording-writer execution until Task 14. Rename helper classes/files using `git mv`; update variable names to `dispatcher` and delete forwarding methods with processor/engine names.

- [ ] **Step 6: Delete the facade classes and the shadowing migration helper**

Move dispatcher preparation/error tests from `DefaultYierdisEngineTest` into `CommandDispatcherTest`. Keep session/transaction-state assertions in `EngineSessionTest`. Then delete the files listed above. After deleting command-core's `PreparedCommands`, replace the fully qualified shared-factory references introduced in Tasks 4-5 with `import yier.bubu.redis.execution.api.PreparedCommands`. Do not edit module POMs or remove `yierdis-server-core`.

- [ ] **Step 7: Run server, executor-adapter, and integration command smoke tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandDispatcherTest,EngineSessionTest,YierdisServerBootstrapCommandWiringTest,YierdisServerBootstrapCloseTest,ClosingSkipSideEffectsIntegrationTest,NettyExecutionAdapterIntegrationTest,ContractsIntegrationSmokeTest,CommandProcessorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS and `rg -n 'YierdisFastCommandProcessor|YierdisEngine|DefaultYierdisEngine' --glob '*.java'` reports no production or current test references.

- [ ] **Step 8: Commit direct dispatcher composition**

```bash
git add yierdis-command/yierdis-command-core yierdis-server/yierdis-server-core yierdis-server/yierdis-server-main yierdis-tests/yierdis-integration-tests
git commit -m "refactor: wire command dispatcher directly"
```

### Task 7: Adapt DB Results To Semantic Streaming Replies

**Files:**

- Create: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DbReplies.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/BulkStringReplyAdapter.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Create test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/DbRepliesTest.java`
- Replace test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandSupportFastPathTest.java` with `CommandSupportTest.java`

**Interfaces:**

- Consumes: Task 1 semantic streaming emitters, Task 2 owned prepared factories, `ByteValue`, `ByteSequenceSource`, `ByteMapSource`, and `PreparedMutation`.
- Produces: DB-neutral `RedisReply` values while the prepared command remains the owner of each DB source.

- [ ] **Step 1: Write failing streaming adapter tests**

Use tracked DB result sources to verify shape inspection visits only lengths, rendering emits once, and prepared close owns the source:

```java
@Test
public void sequenceReplyStreamsThroughReplySinkAndClosesWithPreparedOwner() {
    TrackingSequence source = new TrackingSequence(bytes("a"), null, bytes("ccc"));
    RedisReply reply = DbReplies.sequence(source);
    PreparedCommand prepared = PreparedCommands.owned(CommandResult.reply(reply), source);

    Assert.assertEquals(0, source.emitCount());
    Assert.assertEquals(3, ((ReplyShape.ByteSequence) reply.shape()).elementCount());
    render(prepared);
    Assert.assertEquals(1, source.emitCount());
    prepared.close();
    prepared.close();
    Assert.assertEquals(1, source.closeCount());
}
```

Add equivalent tests for null/heap/slice/long/owned `ByteValue`, map pair counts, render failure, stale close without render, and source close failure.

- [ ] **Step 2: Run builtin adapter tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin -am -Dtest=DbRepliesTest,CommandSupportTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `DbReplies` and the `ReplySink` adapter do not exist.

- [ ] **Step 3: Implement DB semantic reply adapters**

Expose package-level methods:

```java
static RedisReply value(ByteValue value);
static RedisReply sequence(ByteSequenceSource source);
static RedisReply map(ByteMapSource source);
```

Each factory captures the existing source and emits through `new BulkStringReplyAdapter(sink)`. It does not close the source and does not copy its payload. Change `BulkStringReplyAdapter` to hold `ReplySink`, not `RedisReplyWriter`.

- [ ] **Step 4: Add the final command-runtime helper path while retaining unmigrated methods temporarily**

Add `commandDb(CommandSession session)` and keep `commandDb(CommandExecutionContext context)`. Implement mutation preparation through:

```java
public static PreparedCommand preparedMutation(
        ReplyShape reservationShape,
        PreparedMutation<?> mutation,
        Function<CommandExecutionContext, CommandResult> action
) {
    return PreparedCommands.ownedAction(
            reservationShape,
            mutation,
            () -> mutation.isCurrent() ? ValidationResult.VALID : ValidationResult.STALE,
            context -> translateExpectedCommandFailure(() -> action.apply(context))
    );
}
```

`translateExpectedCommandFailure` converts only `WrongTypeException` and `YierdisCommandException` to `CommandResult.controlError`. Do not translate `IllegalArgumentException`. Keep old writer/scratch helpers only until the relevant family task removes its last caller.

- [ ] **Step 5: Run builtin adapter tests**

Run the Step 2 command.

Expected: PASS and the DB sources are neither copied nor closed by `DbReplies` itself.

- [ ] **Step 6: Commit the DB reply adapter boundary**

```bash
git add yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DbReplies.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/BulkStringReplyAdapter.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults
git commit -m "refactor: adapt database results to semantic replies"
```

### Task 8: Migrate Connection And Command-Metadata Commands

**Files:**

- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- Create test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandMetadataRegressionTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandVariantCoverageTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/EmptyBulkStringCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java`

**Interfaces:**

- Consumes: `CommandSpec`, `CommandArgs`, `CommandInvocation`, `CommandModule.Registration.specByUpperName`, semantic replies, and owned retained requests.
- Produces: `PING`, `ECHO`, `COMMAND`, `SELECT`, `QUIT`, `CLIENT`, `AUTH`, and `FLUSHDB` without legacy definitions or direct writer use.

- [ ] **Step 1: Add a failing source guard for the first migrated command family**

`CommandPipelineArchitectureTest` reads `CoreConnectionCommands.java` and rejects:

```java
for (String forbidden : List.of(
        "CommandDefinition", "CommandParsers", "ArgReader",
        "CommandPreparationContext", "RedisReplyWriter",
        ".reply()", "new PreparedCommand"
)) {
    Assert.assertFalse("legacy command path remains: " + forbidden, source.contains(forbidden));
}
Assert.assertEquals(8, occurrences(source, "new CommandSpec("));
```

Extend metadata behavior tests for `COMMAND`, `COMMAND COUNT`, mixed-case `COMMAND INFO`, unknown metadata, `QUIT` close-after-reply, and null `PING`/`ECHO`.

- [ ] **Step 2: Run connection-family tests and verify the source guard fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandPipelineArchitectureTest,CommandMetadataRegressionTest,CommandVariantCoverageTest,EmptyBulkStringCommandTest,DefaultCommandRegistrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because connection commands still register `CommandDefinition` and write replies through execution context.

- [ ] **Step 3: Replace all eight registrations with `CommandSpec`**

Use this exact shape for every registration:

```java
registration.register(new CommandSpec(
        syntax("PING", CommandArity.oneOf(1, 2)),
        this::ping
));
```

The handler returns an invocation. Parse numeric `SELECT`, `CLIENT` subcommand arity, and `FLUSHDB` option syntax before returning it. Throw these exact parse messages: canonical `client|setname`/`client|getname` arity errors, `ERR syntax error`, and `ERR value is not an integer or out of range`.

- [ ] **Step 4: Return semantic results and retain reply-backed request arguments**

Use `PreparedCommands.ready` for fixed replies, `PreparedCommands.action` for session/DB changes, and `CommandResult.closeAfterReply(RedisReplies.simpleString("OK"))` for `QUIT`. For `PING message` and `ECHO`, retain the request in `invocation.prepare` and return an owned bulk/null reply whose retained bytes equal the retained request's `admittedMemoryBytes()`.

Build `COMMAND INFO` as nested semantic arrays from `CommandSpec.syntax()`. `CommandInfo` stores reply data, not `ReplyShape` plus a `write` method:

```java
private record CommandInfo(byte[] name, long arity, int firstKey, int lastKey, int keyStep) {
    RedisReply reply() {
        return RedisReplies.array(List.of(
                RedisReplies.bulkString(name), RedisReplies.integer(arity),
                RedisReplies.array(List.of()), RedisReplies.integer(firstKey),
                RedisReplies.integer(lastKey), RedisReplies.integer(keyStep)
        ));
    }
}
```

- [ ] **Step 5: Run connection and metadata tests**

Run the Step 2 command.

Expected: PASS with exactly eight `CommandSpec` registrations and unchanged wire semantics.

- [ ] **Step 6: Commit connection command migration**

```bash
git add yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command
git commit -m "refactor: migrate connection commands to semantic results"
```

### Task 9: Migrate HELLO, INFO, STATS, And Command-Facing Server Services

**Files:**

- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/SlowCommandGovernor.java`
- Rewrite: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`
- Rewrite reply construction: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespHandshakeIntegrationTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolIntegrationTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MemoryStatsCommandTest.java`

**Interfaces:**

- Consumes: semantic aggregate replies, `CommandArgs`, `CommandSession`, and existing server/executor/runtime snapshots.
- Produces: server services with no preparation wrapper and no writer dependency:

```java
public interface ServerInfoProvider {
    RedisReply info(CommandArgs args, CommandSession session);
    RedisReply stats(CommandSession session);
    YierdisMemoryStats memoryStats(CommandSession session);
}

public interface SlowCommandGovernor {
    long keysTimeBudgetNanos(CommandSession session);
    default int keysMaxResults(CommandSession session) { return Integer.MAX_VALUE; }
}
```

- [ ] **Step 1: Add failing server-command contract and source assertions**

Add reflection assertions for the exact interfaces above. Extend the wiring source test to reject `CommandPreparationContext`, `RedisReplyWriter`, `CommandExecutionContext`, `ReplyShapes.maximum()`, and `maximumReply(` in both server command files.

Add behavior assertions for RESP2/RESP3 `HELLO`, `HELLO SETNAME`, unsupported version, `INFO`, `INFO health`, `INFO yierdis`, `STATS`, not-ready provider errors, and global/per-DB `MEMORY STATS` selection.

- [ ] **Step 2: Run server command tests and verify the contract fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-api,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandContractTest,YierdisServerBootstrapCommandWiringTest,RespHandshakeIntegrationTest,RespProtocolIntegrationTest,MemoryStatsCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because server services still return prepared commands and render through a writer.

- [ ] **Step 3: Migrate the service contracts and bootstrap governor**

Replace all `CommandPreparationContext` parameters with `CommandSession`. Update `CommandSupport.commandDb(session)`, memory stats callers, default/unbounded governor implementations, and bootstrap's configured governor. No adapter overload remains.

- [ ] **Step 4: Parse HELLO without session access and prepare all three commands semantically**

Parse to:

```java
private record HelloArgs(Integer requestedVersion, boolean setClientName, String clientName) { }
```

The invocation chooses `requestedVersion == null ? session.respVersion() : requestedVersion`, applies session changes only inside a result action after reservation, and returns the same five-pair map. `INFO` and `STATS` invocations call the provider during preparation and wrap its reply with `PreparedCommands.ready`.

- [ ] **Step 5: Convert `NettyServerInfoProvider` writer helpers to semantic builders**

Use a flattened field/value list for maps:

```java
private static void addPair(List<RedisReply> fields, byte[] key, long value) {
    fields.add(RedisReplies.bulkString(key));
    fields.add(RedisReplies.integer(value));
}

private static RedisReply mapReply(List<RedisReply> fields) {
    return RedisReplies.map(fields);
}
```

`info(...)` returns `ERR INFO not ready`, a structured health/yierdis map, or the existing UTF-8 bulk payload. `stats(...)` snapshots once and returns the same 69 or 80 pairs. Preserve all existing keys, pair counts, ordering, text sections, and error messages. Remove `maximumReply`, all writer imports, and all direct header/value calls.

- [ ] **Step 6: Run server command and protocol tests**

Run the Step 2 command.

Expected: PASS; RESP2/RESP3 bytes and session changes are unchanged, but server commands now produce semantic values.

- [ ] **Step 7: Commit server command migration**

```bash
git add yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/SlowCommandGovernor.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java yierdis-server/yierdis-server-main/src/test yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MemoryStatsCommandTest.java
git commit -m "refactor: return semantic server command replies"
```

### Task 10: Migrate String And Bitmap Commands

**Files:**

- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- Extend test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Create test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandParseIsolationTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/StringCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/BitmapCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MaxmemoryDoubleReplyRegressionTest.java`

**Interfaces:**

- Consumes: `CommandArgs.slice`, explicit parse exceptions, `DbReplies.value`, prepared mutations, and semantic integer/null/simple replies.
- Produces: `SET`, `GET`, `STRLEN`, `APPEND`, `SETBIT`, `GETBIT`, `BITCOUNT`, `INCR`, and `DECR` on the final command contract.

- [ ] **Step 1: Add failing source and parse-isolation coverage**

Extend the source guard to require nine `CommandSpec` registrations and reject all legacy/writer tokens in `StringCommands.java`. Create `CommandParseIsolationTest` with a DB router that throws on every call, then obtain each registered string spec and call only `handler().parse(...)` with:

```java
Map.ofEntries(
        entry("SET", argv("SET", "k", "v", "PX", "10", "GET")),
        entry("GET", argv("GET", "k")),
        entry("STRLEN", argv("STRLEN", "k")),
        entry("APPEND", argv("APPEND", "k", "v")),
        entry("SETBIT", argv("SETBIT", "k", "1", "0")),
        entry("GETBIT", argv("GETBIT", "k", "1")),
        entry("BITCOUNT", argv("BITCOUNT", "k", "0", "2")),
        entry("INCR", argv("INCR", "k")),
        entry("DECR", argv("DECR", "k"))
)
```

Assert zero router/provider/session calls and a non-null invocation for every entry. Add invalid cases in and outside `MULTI` for SET option conflicts, invalid expiry, SETBIT offset/bit, GETBIT offset, and BITCOUNT bounds.

- [ ] **Step 2: Run string/bitmap tests and verify migration guards fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandPipelineArchitectureTest,CommandParseIsolationTest,StringCommandTest,BitmapCommandTest,TransactionCommandTest,MaxmemoryDoubleReplyRegressionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because string commands still use legacy parser/preparer pairs and execution writers.

- [ ] **Step 3: Convert registrations and all user-input checks to parse methods**

Register one `CommandSpec` per name. Preserve the existing typed `SetArgs`, but replace `CommandParseResult` returns with `CommandInvocation` and checked failures. Add small parsed records for bitmap/range commands:

```java
private record SetBitArgs(byte[] key, long offset, int value) { }
private record GetBitArgs(byte[] key, long offset) { }
private record BitCountArgs(byte[] key, Long start, Long end) { }
```

Use exact messages: `ERR syntax error`, `ERR value is not an integer or out of range`, `ERR invalid expire time in 'set' command`, `ERR bit is not an integer or out of range`, and `ERR string exceeds maximum allowed size`. Do not catch arbitrary `IllegalArgumentException` from DB/runtime code.

- [ ] **Step 4: Prepare reads and mutations without a writer**

Use `args.slice(index)` for DB views/slices. For a `ByteValue value`, return `PreparedCommands.owned(CommandResult.reply(DbReplies.value(value)), value)` so its native/request-backed owner survives rendering; scalar reads use exact ready replies. Mutation actions use conservative `ReplyShapes.integerUpperBound()` and return `CommandResult.reply(RedisReplies.integer(value))`.

For `SET`, keep the `PreparedMutation` and preview. Build the final semantic result as:

```java
RedisReply reply = getOld
        ? DbReplies.value(preview.oldValue())
        : preview.applied() ? RedisReplies.simpleString("OK") : RedisReplies.nullValue();
return CommandSupport.preparedMutation(
        reply.shape(), mutation,
        execution -> {
            mutation.commit(execution.mutationContext());
            return CommandResult.reply(reply);
        }
);
```

Ensure the mutation is committed once only inside the post-reservation action and owned preview values close with the prepared command.

- [ ] **Step 5: Run string, bitmap, transaction, and maxmemory regressions**

Run the Step 2 command.

Expected: PASS; invalid syntax aborts an active transaction during preflight, and mutation errors produce one control error without a second reply.

- [ ] **Step 6: Commit string-family migration**

```bash
git add yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command
git commit -m "refactor: migrate string commands to direct handlers"
```

### Task 11: Migrate Keyspace, TTL, MEMORY, OBJECT, KEYS, And SCAN

**Files:**

- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- Extend test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Extend test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandParseIsolationTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OffHeapKeysCommandSmokeTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ScanCursorContractTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ExpireSemanticsTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MemoryStatsCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OffHeapKeysZeroCopyReadPathTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java`

**Interfaces:**

- Consumes: session-based governor/provider services, semantic streamed sequences, owned/stale validation, and command argument byte lists.
- Produces: all 14 keyspace registrations on `CommandSpec` with unchanged TTL, cursor, memory, and stale-window behavior.

- [ ] **Step 1: Add failing source, parse-isolation, and stale-window tests**

Require 14 `CommandSpec` registrations and no legacy/writer tokens. Extend the valid parse table with `TYPE`, both `MEMORY` forms, `OBJECT ENCODING`, `KEYS`, `SCAN` with `MATCH`/`COUNT`, `DEL`, `EXISTS`, four expiry commands, `PERSIST`, `TTL`, and `PTTL`.

Add invalid transaction preflight cases for malformed MEMORY/OBJECT subcommands, negative/overflow SCAN cursor, missing MATCH/COUNT values, non-positive COUNT, and invalid TTL integers. Extend `ReplyPreflightCommandTest` to prove a stale `KeyScanWindow` closes before the original request is prepared again.

- [ ] **Step 2: Run keyspace tests and verify migration guards fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandPipelineArchitectureTest,CommandParseIsolationTest,OffHeapKeysCommandSmokeTest,ScanCursorContractTest,ExpireSemanticsTest,MemoryStatsCommandTest,OffHeapKeysZeroCopyReadPathTest,ReplyPreflightCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on legacy registrations and reply writer use.

- [ ] **Step 3: Parse all syntax and numeric input before invocation preparation**

Keep `ScanArgs(long cursor, byte[] match, int count)`. Add explicit parsed variants for `MEMORY USAGE`, `MEMORY STATS`, `OBJECT ENCODING`, and TTL values. `KEYS` captures only its pattern; it calls `SlowCommandGovernor` after the invocation receives `CommandSession`. `DEL` captures `args.byteArraysFrom(1)` and removes shared mutable scratch usage.

Use exact failures already emitted by the file: canonical subcommand arity, `ERR syntax error`, `ERR value is not an integer or out of range`, `ERR key discovery window changed before reply preflight`, and `ERR scan window changed before reply preflight`.

- [ ] **Step 4: Express scalar, memory-map, and scan-window replies once**

Return exact scalar replies for TYPE, MEMORY USAGE, OBJECT, EXISTS, TTL, and PTTL. Build `MEMORY STATS` with one semantic flattened map. For key windows:

```java
RedisReply elements = DbReplies.sequence(window);
RedisReply reply = RedisReplies.array(List.of(
        RedisReplies.bulkString(window.nextCursor().toAsciiBytes()),
        elements
));
return PreparedCommands.ownedAction(
        reply.shape(),
        window,
        () -> window.current() ? ValidationResult.VALID : ValidationResult.STALE,
        context -> CommandResult.reply(reply)
);
```

For `KEYS`, use the sequence reply directly. Keep the existing bounded discovery retry/deadline behavior exactly; only replace its output representation.

- [ ] **Step 5: Move mutations to post-reservation result actions**

DEL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, and PERSIST declare `ReplyShapes.integerUpperBound()` and return the actual integer result. No parse or preparation path invokes writes. Do not change TTL or expiry DB APIs.

- [ ] **Step 6: Run keyspace and preflight tests**

Run the Step 2 command.

Expected: PASS with byte-identical cursor replies, preserved expiration behavior, and exactly-once stale-window cleanup.

- [ ] **Step 7: Commit keyspace migration**

```bash
git add yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command
git commit -m "refactor: migrate keyspace commands to semantic replies"
```

### Task 12: Migrate List, Hash, Set, And Shared Collection SCAN Commands

**Files:**

- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CollectionScanCommandSupport.java`
- Extend test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Extend test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandParseIsolationTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ListCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/HashCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/SetCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CollectionScanCommandTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ReplySourceThreadAffinityIntegrationTest.java`

**Interfaces:**

- Consumes: `DbReplies.value/sequence/map`, `CommandArgs.byteArraysFrom`, prepared mutation ownership, and shared collection scan parsing.
- Produces: five list, six hash, and six set commands plus one shared scan helper on the final path.

- [ ] **Step 1: Add failing family guards and parse-only cases**

Add all four files to `CommandPipelineArchitectureTest`; require 5, 6, and 6 `CommandSpec` occurrences respectively and reject legacy/writer/scratch calls. Extend valid parsing for every registration, including HSCAN/SSCAN options. Add invalid preflight cases for LPOP/RPOP count, HSCAN/SSCAN cursor, missing MATCH/COUNT values, non-positive COUNT, and unsupported `NOVALUES` outside HSCAN.

- [ ] **Step 2: Run collection-family tests and verify migration guards fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandPipelineArchitectureTest,CommandParseIsolationTest,ListCommandTest,HashCommandTest,SetCommandTest,CollectionScanCommandTest,ReplySourceThreadAffinityIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because all three command families still use the old definition/preparer/writer path.

- [ ] **Step 3: Rewrite shared collection scan parsing and replies**

Change the parser to:

```java
public static Arguments parse(CommandArgs args, boolean allowNoValues)
        throws CommandParseException;

public static PreparedCommand prepareReply(CollectionScanWindow window) {
    byte[] cursor = window.nextCursor().toAsciiBytes();
    RedisReply reply = RedisReplies.array(List.of(
            RedisReplies.bulkString(cursor), DbReplies.sequence(window)));
    return PreparedCommands.owned(CommandResult.reply(reply), window);
}
```

Preserve default COUNT 10 and the exact integer/syntax errors. The helper neither imports a writer nor closes the window before render.

- [ ] **Step 4: Migrate list commands and prepared pop results**

Capture push value lists in parse and return post-reservation integer results. LRANGE prepares an owned sequence. Parse optional pop count explicitly; after commit, return null, one bulk value, or an array sequence exactly as before. The prepared pop mutation owns its `PoppedValueSequence` through render and closes it once with the outer prepared command.

- [ ] **Step 5: Migrate hash and set commands without reusable scratch state**

HSET/HDEL/SADD/SREM capture immutable byte-array lists and return conservative integer actions. HGET returns `DbReplies.value`, HGETALL returns `DbReplies.map`, SMEMBERS returns `DbReplies.sequence`, and scalar reads return exact integers. Wrap every `ByteValue`, `ByteMapSource`, and `ByteSequenceSource` reply with `PreparedCommands.owned(CommandResult.reply(reply), source)` so the source closes only after rendering. HSCAN/SSCAN use the rewritten shared helper. Remove each family's `sliceResetFromRequest`, `slice`, and `clearScratch` calls.

- [ ] **Step 6: Run collection-family and source-affinity tests**

Run the Step 2 command.

Expected: PASS; streaming sources emit on the owner thread, mutation results are returned once, and no source is copied into a second aggregate payload.

- [ ] **Step 7: Commit collection-family migration**

```bash
git add yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CollectionScanCommandSupport.java yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ReplySourceThreadAffinityIntegrationTest.java
git commit -m "refactor: migrate collection commands to direct handlers"
```

### Task 13: Migrate Sorted-Set And HyperLogLog Commands

**Files:**

- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- Rewrite: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Extend test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Extend test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandParseIsolationTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ZSetCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/HllCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`

**Interfaces:**

- Consumes: shared scan replies, explicit score parsing, semantic sequences, immutable argument lists, and conservative mutation results.
- Produces: nine sorted-set and three HLL commands on `CommandSpec`; every builtin command is migrated after this task.

- [ ] **Step 1: Add failing final-family guards and parse-only coverage**

Require nine and three `CommandSpec` registrations, reject legacy/writer/scratch tokens, and extend the valid parse table for all ZSET/HLL commands. Add invalid preflight cases for ZADD NaN/infinity/non-number scores, ZRANGE ranks/options, score bounds, LIMIT values, ZREM ranges, ZSCAN options, and HLL arity.

For each invalid request inside `MULTI`, assert the existing exact message (`ERR value is not a valid float`, `ERR min or max is not a float`, integer error, or syntax error), zero DB calls, zero queued requests, and subsequent `EXECABORT`.

- [ ] **Step 2: Run ZSET/HLL tests and verify migration guards fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-builtin,yierdis-tests/yierdis-integration-tests -am -Dtest=CommandPipelineArchitectureTest,CommandParseIsolationTest,ZSetCommandTest,HllCommandTest,TransactionCommandTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on old definitions, old parse result types, and writer calls.

- [ ] **Step 3: Move score and range parsing into checked command handlers**

Keep typed records for range/rank/score queries. Move `ScoreBound` beside `ZSetCommands` and make `parseScoreBound` throw `CommandParseException("ERR min or max is not a float")`. Validate every ZADD score with `Double.parseDouble`, rejecting NaN/infinity with `ERR value is not a valid float`, before returning the invocation; pass the unchanged raw pairs to the existing DB API during execution.

- [ ] **Step 4: Return semantic ZSET reads, scans, and mutation results**

ZRANGE variants return owned `DbReplies.sequence`; ZSCAN uses shared scan support. ZADD, ZREM, and both range-removal commands capture immutable arguments and return integer upper-bound actions. Preserve WITHSCORES, REV, LIMIT, cursor, ordering, and all current DB calls.

- [ ] **Step 5: Migrate HLL and remove the last command scratch users**

PFADD/PFMERGE capture `args.byteArraysFrom(...)` and execute only after reservation; PFCOUNT reads during preparation and returns an exact integer. Remove now-unused mutable list scratch fields and methods from `CommandSupport`. Keep only DB routing, provider/governor access, prepared mutation support, and expected DB exception translation.

- [ ] **Step 6: Run final builtin family tests and a global source scan**

Run the Step 2 command, then run:

```bash
rg -n "CommandDefinition|CommandParsers|CommandParseResult|CommandParseError|ArgReader|CommandPreparationContext|RedisReplyWriter|\.reply\(\)" yierdis-command/yierdis-command-builtin/src/main/java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java
```

Expected: tests PASS and the scan prints no matches.

- [ ] **Step 7: Commit the final builtin migration**

```bash
git add yierdis-command/yierdis-command-builtin yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command
git commit -m "refactor: complete builtin command handler migration"
```

### Task 14: Flip Prepared Execution To Return Results And Render Only In The Executor

**Files:**

- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandExecutionContext.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommands.java`
- Modify test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/CoreContractSmokeTest.java`
- Modify test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/PreparedCommandsTest.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/PreparedCommandTestSupport.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/CommandDispatcherTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TransactionQueueCleanupTest.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- Modify test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplySizerTest.java`
- Create test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RedisReplyRespContractTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestPreparedCommands.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/ReplyResultUnknownTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespIngressLifecycleIntegrationTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`

**Interfaces:**

- Consumes: all commands returning `CommandResult` through Task 2 factories and Task 5 dispatcher replay, reservation planning, and temporary writer-based transaction execution.
- Produces: the final `PreparedCommand` and `CommandExecutionContext` contracts, semantic transaction aggregation, central renderer ownership, result-based close behavior, and no command-layer writer access.

- [ ] **Step 1: Change contract tests to require the final signatures**

Assert the exact methods by reflection:

```java
@Test
public void preparedExecutionReturnsACommandResultWithoutAWriterContext() throws Exception {
    Assert.assertEquals(ReplyShape.class,
            PreparedCommand.class.getMethod("reservationShape").getReturnType());
    Assert.assertEquals(CommandResult.class,
            PreparedCommand.class.getMethod("execute", CommandExecutionContext.class).getReturnType());
    Assert.assertThrows(NoSuchMethodException.class,
            () -> PreparedCommand.class.getMethod("replyShape"));
    Assert.assertThrows(NoSuchMethodException.class,
            () -> CommandExecutionContext.class.getMethod("reply"));
}
```

Update `PreparedCommandsTest` so it directly asserts returned results instead of recording a writer.

- [ ] **Step 2: Add failing executor result/render lifecycle tests**

Add focused cases for:

```java
@Test
public void executorRendersResultAfterExecutionAndAppliesResultCloseFlag() {
    PreparedCommand command = prepared(
            ReplyShapes.simpleString("OK"),
            context -> CommandResult.closeAfterReply(RedisReplies.simpleString("OK"))
    );
    ExecutionOutcome outcome = execute(command);
    Assert.assertEquals("+OK\r\n", outcome.bytes());
    Assert.assertTrue(outcome.connectionClosing());
    Assert.assertEquals(1, outcome.executeCount());
    Assert.assertEquals(1, outcome.preparedCloseCount());
}
```

Also prove: capacity waiting does not prepare again; stale validation closes then re-prepares; action runs once; null result is internal failure; expected control error uses the existing control reservation; an unexpected execute failure before a visible result emits the existing `ERR internal error` control reply and closes; `ResultUnknownException` and render failure after an action never retry the mutation and close; reservation-bound violation closes; source closes after render success/failure; request closes after the prepared owner; and the reply slot is marked ready or cancelled and released exactly once on every branch.

- [ ] **Step 3: Add failing final transaction-result lifecycle tests**

Replace Task 5's temporary writer-lifetime assertions with result-based assertions in `CommandDispatcherTest`, `TransactionCommandTest`, and `TransactionQueueCleanupTest`:

```java
@Test
public void multiChildExecRetainsChildrenAndRequestsUntilAggregateRenderingCanFinish() {
    TrackingTransactionState tx = transactionWith("FIRST", "SECOND");
    TrackingPrepared first = resultChild(
            CommandResult.reply(RedisReplies.bulkString(bytes("one"))));
    TrackingPrepared second = resultChild(
            CommandResult.reply(RedisReplies.integer(2)));
    PreparedCommand exec = prepareExec(tx, first, second);

    CommandResult result = execute(exec);
    RedisReply.Aggregate aggregate = (RedisReply.Aggregate) result.reply();
    Assert.assertEquals(ReplyShape.AggregateKind.ARRAY, aggregate.kind());
    Assert.assertEquals(2, aggregate.elements().size());
    Assert.assertEquals(0, first.closeCount());
    Assert.assertEquals(0, second.closeCount());
    Assert.assertEquals(0, tx.request(0).closeCount());

    exec.close();
    Assert.assertEquals(1, first.closeCount());
    Assert.assertEquals(1, second.closeCount());
    Assert.assertEquals(1, tx.request(0).closeCount());
    Assert.assertEquals(1, tx.request(1).closeCount());
}
```

Also assert result order, a child `ControlError` converted to an ordinary `Error` array element, logical-OR propagation of child `closeAfterReply`, a streamed child source remaining open through rendering and closing once afterward, stale exact/dynamic children, parse/prepare/validate/execute failures, completed-child plus unconsumed-tail cleanup, and primary-failure preservation with cleanup failures suppressed.

- [ ] **Step 4: Add failing RESP2/RESP3 semantic reply contract coverage**

In `RedisReplyRespContractTest`, enumerate every `RedisReply` variant, including nested aggregate, null elements, streamed sequence/map, and control error. For ordinary replies:

```java
ReplyPlan plan = sizer.plan(session(version), reply.shape());
RedisReplyRenderer.render(reply, writer(version, bytes));
Assert.assertFalse(plan.reserveMaximum());
Assert.assertTrue(plan.encodedUpperBoundBytes() >= bytes.size());
```

Assert equality for exact replies, retained-source byte totals for streams, `reserveMaximum()` for control errors, RESP2 map-as-array encoding, and RESP3 native map/set/push/attribute encoding.

- [ ] **Step 5: Run final-contract tests and verify compilation fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-executor,yierdis-networking/yierdis-networking-resp,yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CoreContractSmokeTest,PreparedCommandsTest,CommandDispatcherTest,CommandExecutorTest,ReplyCapacityBlockedSchedulingTest,RedisReplyRespContractTest,TransactionCommandTest,TransactionQueueCleanupTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the production interfaces still expose `replyShape`, writer-bearing context, and void execution.

- [ ] **Step 6: Flip the shared contracts and remove the temporary renderer bridge**

Rename `replyShape()` to `reservationShape()` and return `CommandResult` from `execute`. Change construction to:

```java
public static CommandExecutionContext forRequest(
        CommandSession session,
        ExecutionRequest request
);
```

Remove the writer field/accessor. In `PreparedCommands`, actions return their result directly; remove all `RedisReplyRenderer` and `requestCloseAfterReply` calls. Preserve its validation and exactly-once owner close behavior.

- [ ] **Step 7: Make transaction replay return one aggregate result without rendering children**

`PreparedExec.execute` returns:

```java
return anyChildCloses
        ? CommandResult.closeAfterReply(RedisReplies.array(childReplies))
        : CommandResult.reply(RedisReplies.array(childReplies));
```

Execute children in queue order and collect their returned replies. Keep every successfully executed child, its reply source, and every drained queued request owned until `PreparedExec.close`, which the executor calls only after rendering; close a stale child before replaying that same request. Convert a child `ControlError` to an ordinary `Error` before insertion, propagate `closeAfterReply` if any child requests it, and retain the Task 5 exact-single-child versus maximum-multi-child reservation split. On failure, close completed children and the unconsumed queue tail in reverse ownership order while preserving the primary throwable and suppressing cleanup failures. Remove the last renderer/writer reference from command-core.

- [ ] **Step 8: Render and apply close state centrally in executor execution support**

Keep preparation, capacity waiting, and validation branches unchanged. Replace the execution block with this ordering:

```java
CommandResult result;
try (CommandExecutionContext execution = CommandExecutionContext.forRequest(
        connection.session(), task.request)) {
    result = Objects.requireNonNull(
            task.prepared.execute(execution),
            "prepared command returned null result"
    );
    executed = true;
}
RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), task.reply.sink());
RedisReplyRenderer.render(result.reply(), writer);
if (result.closeAfterReply()) {
    context.recordCloseAfterReply();
    closeAfterReply.increment();
    connection.markClosing();
}
task.reply.markReady(result.closeAfterReply());
```

Do not move any queue, fairness, waiting, input-pause, capacity-registration, or shutdown code. Setting `executed` before rendering ensures any render/bound failure follows the existing result-unknown close path.

- [ ] **Step 9: Update all test fakes and the protocol-neutral test client**

Every fake returns `CommandResult`, exposes `reservationShape`, and constructs context without a writer. `FastTestClient` calls `prepared.execute`, renders the returned reply into its existing capturing writer, and exposes close-after-reply from the result rather than writer state.

- [ ] **Step 10: Run executor, RESP, transaction, preflight, and result-unknown tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-executor,yierdis-networking/yierdis-networking-resp,yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=CoreContractSmokeTest,PreparedCommandsTest,CommandDispatcherTest,CommandExecutorTest,ReplyCapacityBlockedSchedulingTest,RespReplySizerTest,RedisReplyRespContractTest,TransactionCommandTest,TransactionQueueCleanupTest,ReplyPreflightCommandTest,ReplyResultUnknownTest,RespIngressLifecycleIntegrationTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; scheduling tests retain their existing counts, render failures close the connection, and every tracked source closes exactly once.

- [ ] **Step 11: Commit the final prepared execution contract**

```bash
git add yierdis-server/yierdis-server-api yierdis-server/yierdis-server-executor yierdis-networking/yierdis-networking-resp/src/test yierdis-command/yierdis-command-core yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java yierdis-tests/yierdis-integration-tests/src/test yierdis-server/yierdis-server-main/src/test
git commit -m "refactor: render command results centrally"
```

### Task 15: Delete Legacy Contracts, Lock Architecture, Update Docs, And Verify Stage 1

**Files:**

- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandDefinition.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParser.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandPreparer.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParsers.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseResult.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParseError.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ArgReader.java`
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandPreparationContext.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/LegacyCommandAdapter.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandModule.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Modify: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandContractTest.java`
- Delete test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandDefinitionTest.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandPipelineArchitectureTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Create test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerCommandParseIsolationTest.java`
- Finalize test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandParseIsolationTest.java`
- Modify docs: `docs/project-docs/request-execution-flow.md`
- Modify docs: `docs/project-docs/command-parsing-and-dispatch.md`
- Modify docs: `docs/project-docs/transaction-and-replay.md`
- Modify docs: `docs/project-docs/executor-and-backpressure.md`
- Modify docs: `docs/project-docs/module-architecture.md`
- Modify docs: `docs/project-docs/main-path-walkthrough.md`
- Modify docs: `docs/project-docs/core-logic-index.md`
- Modify docs: `docs/project-docs/project-overview.md`
- Modify docs: `docs/project-docs/development-navigation.md`
- Modify docs: `docs/project-docs/testing-and-debugging.md`
- Modify docs: `docs/project-docs/glossary.md`
- Modify docs: `docs/project-docs/commands-and-data-model.md`
- Modify docs: `docs/project-docs/protocol-reference.md`
- Modify docs: `docs/project-docs/bytes-and-fast-paths.md`
- Finalize plan tracking: `docs/superpowers/plans/2026-07-28-command-pipeline-simplification.md`

**Interfaces:**

- Consumes: the fully migrated production path from Tasks 1-14.
- Produces: no legacy/temporary API, architecture enforcement of the target path, current documentation, a clean full test run, unchanged Maven/network/DB boundaries, and net production Java reduction.

- [x] **Step 1: Change architecture tests to require the final-only tree**

Replace assertions that require old files with absence checks:

```java
for (String deleted : List.of(
        "CommandDefinition.java", "CommandParser.java", "CommandPreparer.java",
        "CommandParsers.java", "CommandParseResult.java", "CommandParseError.java",
        "ArgReader.java"
)) {
    Assert.assertFalse(Files.exists(commandApi.resolve(deleted)));
}
Assert.assertFalse(Files.exists(executionApi.resolve("CommandPreparationContext.java")));
Assert.assertFalse(Files.exists(commandCore.resolve("LegacyCommandAdapter.java")));
Assert.assertFalse(Files.exists(commandCore.resolve("PreparedCommands.java")));
Assert.assertFalse(Files.exists(commandCore.resolve("YierdisFastCommandProcessor.java")));
Assert.assertFalse(Files.exists(engineRoot.resolve("YierdisEngine.java")));
Assert.assertFalse(Files.exists(engineRoot.resolve("DefaultYierdisEngine.java")));
```

Add global production scans that reject direct `RedisReplyWriter` use from `command-builtin`, `command-core`, and server command implementations; legacy types from all production Java; custom registry table terms (`FNV64`, `Entry[] table`, `resizeThreshold`); and more than one argument/ASCII/integer helper implementation. Require `CommandExecutorExecutionSupport` to contain `RedisReplyRenderer.render(result.reply(), writer)` and `ServerCommandComposition` to contain `CommandDispatcher`.

- [x] **Step 2: Complete parse-isolation coverage for every production registration**

Make `CommandParseIsolationTest` compare its valid-request key set with all names registered by `DefaultCommandModules`; every handler parse must return an invocation while throwing router/provider services remain untouched. `ServerCommandParseIsolationTest` covers `HELLO`, `INFO`, and `STATS` with a provider that throws if called during parse. `CommandDispatcherTest` covers `MULTI`, `DISCARD`, and `EXEC` the same way.

This produces an explicit name-set equality failure whenever a new command lacks a parse-only fixture.

- [x] **Step 3: Run architecture tests and verify legacy-file assertions fail before deletion**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=ArchitectureBoundaryTest,CommandPipelineArchitectureTest,CommandParseIsolationTest,ServerCommandParseIsolationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the temporary legacy files and registration overload still exist.

- [x] **Step 4: Delete all legacy files and migration-only overloads**

Delete the files listed above. Reduce `CommandModule.Registration` to:

```java
interface Registration {
    void register(CommandSpec spec);
    int commandCount();
    boolean containsUpperName(String nameUpper);
    CommandSpec specByUpperName(String nameUpper);
    String[] upperNamesSorted();
}
```

Remove the legacy registry overload/map, writer-based/fixed/owned helper methods, duplicate ASCII/integer parsing, mutable argv scratch, and unused imports. Keep `CommandSupport` only where it removes real DB/service/mutation duplication; the shadowing command-core factory was already deleted in Task 6.

- [x] **Step 5: Update project documentation to the final path**

Use this exact flow consistently in core-path, transaction, module, executor, glossary, navigation, and testing docs:

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(CommandArgs)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve -> validate -> execute(context)
  -> CommandResult -> RedisReplyRenderer
```

Document queue preflight/replay ownership, result-based QUIT close, semantic streaming sources, and the fact that `EngineSession` remains only the connection session owner. Remove links and commands naming deleted processor/engine/parser/preparer files. In protocol docs, retain `RedisReplyWriter` only as the renderer's RESP-facing port.

- [x] **Step 6: Run targeted architecture and command suites**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests -am test
```

Expected: PASS.

- [x] **Step 7: Prove deleted APIs and forbidden boundaries are absent**

Run:

```bash
rg -n "CommandDefinition|CommandParser|CommandPreparer|CommandParsers|CommandParseResult|CommandParseError|ArgReader|CommandPreparationContext|YierdisFastCommandProcessor|DefaultYierdisEngine|YierdisEngine" yierdis-command yierdis-server yierdis-tests --glob '*.java' --glob '!ArchitectureBoundaryTest.java' --glob '!CommandPipelineArchitectureTest.java'
```

Expected: no output.

Run:

```bash
rg -n "RedisReplyWriter|\.reply\(\)" yierdis-command/yierdis-command-builtin/src/main/java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java
```

Expected: no output.

Run:

```bash
rg -n "RedisReplyWriter" yierdis-command/yierdis-command-core/src/main/java
```

Expected: no output; transaction replay returns semantic child results and does not retain its temporary writer bridge.

- [x] **Step 8: Prove stage boundaries and Maven topology stayed unchanged**

Run:

```bash
git diff --exit-code 50b01059 -- ':(glob)**/pom.xml'
```

Expected: exit 0 and no output.

Run:

```bash
git diff --name-only 50b01059 -- yierdis-db yierdis-memory yierdis-networking/yierdis-networking-netty
```

Expected: no output; stage 1 did not modify DB/FFM or Netty state-machine implementation.

- [x] **Step 9: Run the complete JDK 25 Maven suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: BUILD SUCCESS with no test failures or errors.

- [x] **Step 10: Verify touched production Java has a net line-count reduction**

Run:

```bash
git diff --numstat 50b01059 -- ':(glob)**/src/main/java/**/*.java' | awk '{ added += $1; deleted += $2 } END { printf "added=%d deleted=%d net=%d\n", added, deleted, added-deleted; exit !(deleted > added) }'
```

Expected: exit 0 and a negative `net` value.

- [x] **Step 11: Commit final deletion, guards, and documentation**

```bash
git add yierdis-command yierdis-server yierdis-tests docs/project-docs docs/superpowers/plans/2026-07-28-command-pipeline-simplification.md
git commit -m "refactor: remove legacy command pipeline"
```

- [x] **Step 12: Confirm the final commit is clean and reproducible**

Run:

```bash
git status --short
git log -12 --oneline
```

Expected: empty status output and a sequence of focused migration commits ending in `refactor: remove legacy command pipeline`.
