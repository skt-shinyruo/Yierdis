# Command Contract Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn command registration into one executable contract that binds metadata, parsing, handling, and transaction policy.

**Architecture:** Keep `CommandDescriptor` as `COMMAND INFO` metadata and extend `CommandSpec` into the executable contract. Add small parser primitives, make `YierdisFastCommandProcessor` run parse-before-handle, then migrate commands from legacy request handlers to typed parsed handlers in controlled slices.

**Tech Stack:** Java 25, Maven, JUnit 4, existing `ExecutionRequest`/`ReplyWriter` command stack.

---

## File Structure

Create focused parser contract files under `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/`:

- `ArgReader.java`: low-allocation wrapper over `ExecutionRequest`.
- `CommandParser.java`: parser functional interface.
- `CommandHandler.java`: typed handler functional interface.
- `CommandParseResult.java`: success/error result container.
- `CommandParseError.java`: stable parse error model and reply-message mapping.
- `CommandArity.java`: reusable arity validators.
- `CommandParsers.java`: stock parsers for exact/min/range/one-of/pair-tail shapes.

Modify existing command contract files:

- `CommandSpec.java`: add parser and typed handler while preserving legacy registration.
- `CommandModule.java`: add typed registration overloads and keep legacy overloads.
- `CommandRegistry.java`: store executable specs and preserve metadata helpers.
- `YierdisFastCommandProcessor.java`: centralize parse-before-handle and transaction parse-before-queue.

Migrate command modules in slices:

- `HashCommands.java` and `ZSetCommands.java`: move pair-tail validation for `HSET` and `ZADD` into parser layer.
- `KeyCommands.java`: migrate `SCAN`.
- `ZSetCommands.java`: migrate `ZRANGE`, `ZRANGEBYSCORE`, and `ZREVRANGEBYSCORE`.
- `StringCommands.java`: migrate `SET` after the smaller parser migrations.

Tests:

- `CommandContractTest.java`: parser primitive unit tests.
- `CommandErrorTest.java`: parse error compatibility.
- `TransactionCommandTest.java`: parse-before-queue behavior.
- `CommandMetadataRegressionTest.java` and `CommandDescriptorRegistryTest.java`: metadata and registration compatibility.
- `ArchitectureBoundaryTest.java`: final guardrail after migration.

## Task 1: Parser Contract Unit Tests

**Files:**
- Create: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/CommandContractTest.java`
- Later create: parser contract production files in Task 2

- [ ] **Step 1: Write failing tests for parse errors, arity, and arg reading**

Create `CommandContractTest.java`:

```java
package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Arrays;
import java.util.List;

public class CommandContractTest {
    @Test
    public void parseErrorsMapToStableReplyMessages() {
        Assert.assertEquals(
                "ERR wrong number of arguments for 'get' command",
                CommandParseError.wrongArity("get").toReplyMessage()
        );
        Assert.assertEquals("ERR syntax error", CommandParseError.syntax().toReplyMessage());
        Assert.assertEquals(
                "ERR value is not an integer or out of range",
                CommandParseError.integerOutOfRange().toReplyMessage()
        );
        Assert.assertEquals(
                "ERR invalid expire time in 'set' command",
                CommandParseError.custom("ERR invalid expire time in 'set' command").toReplyMessage()
        );
    }

    @Test
    public void arityValidatorsReturnNullWhenValidAndErrorsWhenInvalid() {
        Assert.assertNull(CommandArity.exact(2, "get").validate(args("GET", "k")));
        Assert.assertEquals(
                "ERR wrong number of arguments for 'get' command",
                CommandArity.exact(2, "get").validate(args("GET")).toReplyMessage()
        );

        Assert.assertNull(CommandArity.min(3, "del").validate(args("DEL", "a", "b")));
        Assert.assertEquals(
                "ERR wrong number of arguments for 'del' command",
                CommandArity.min(3, "del").validate(args("DEL", "a")).toReplyMessage()
        );

        Assert.assertNull(CommandArity.range(4, 6, "zrange").validate(args("ZRANGE", "z", "0", "-1")));
        Assert.assertNull(CommandArity.range(4, 6, "zrange").validate(args("ZRANGE", "z", "0", "-1", "WITHSCORES", "REV")));
        Assert.assertEquals(
                "ERR wrong number of arguments for 'zrange' command",
                CommandArity.range(4, 6, "zrange").validate(args("ZRANGE", "z", "0", "-1", "WITHSCORES", "REV", "X")).toReplyMessage()
        );

        Assert.assertNull(CommandArity.oneOf("ping", 1, 2).validate(args("PING")));
        Assert.assertNull(CommandArity.oneOf("ping", 1, 2).validate(args("PING", "hello")));
        Assert.assertEquals(
                "ERR wrong number of arguments for 'ping' command",
                CommandArity.oneOf("ping", 1, 2).validate(args("PING", "a", "b")).toReplyMessage()
        );

        Assert.assertNull(CommandArity.pairTail(4, 2, "hset").validate(args("HSET", "h", "f", "v")));
        Assert.assertNull(CommandArity.pairTail(4, 2, "hset").validate(args("HSET", "h", "f1", "v1", "f2", "v2")));
        Assert.assertEquals(
                "ERR wrong number of arguments for 'hset' command",
                CommandArity.pairTail(4, 2, "hset").validate(args("HSET", "h", "f")).toReplyMessage()
        );
        Assert.assertEquals(
                "ERR wrong number of arguments for 'hset' command",
                CommandArity.pairTail(4, 2, "hset").validate(args("HSET", "h", "f1", "v1", "f2")).toReplyMessage()
        );
    }

    @Test
    public void argReaderKeepsAsciiAndNumericParsingCentralized() {
        ArgReader reader = args("SET", "k", "v", "EX", "42");

        Assert.assertEquals(5, reader.argc());
        Assert.assertTrue(reader.is(3, "ex"));
        Assert.assertFalse(reader.is(3, "px"));
        Assert.assertArrayEquals(bytes("k"), reader.bytes(1));
        Assert.assertEquals(42L, reader.longAt(4));
        Assert.assertEquals(42L, reader.positiveLongAt(4));
        Assert.assertEquals(42L, reader.nonNegativeLongAt(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void argReaderRejectsNegativePositiveLong() {
        args("SCAN", "0", "COUNT", "-1").positiveLongAt(3);
    }

    @Test
    public void parseResultCarriesSuccessOrError() {
        CommandParseResult<String> ok = CommandParseResult.ok("parsed");
        Assert.assertTrue(ok.ok());
        Assert.assertEquals("parsed", ok.value());
        Assert.assertNull(ok.error());

        CommandParseResult<String> error = CommandParseResult.error(CommandParseError.syntax());
        Assert.assertFalse(error.ok());
        Assert.assertNull(error.value());
        Assert.assertEquals("ERR syntax error", error.error().toReplyMessage());
    }

    private static ArgReader args(String command, String... rest) {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8(command, Arrays.asList(rest));
        return ArgReader.of(request);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=CommandContractTest test
```

Expected: compilation fails because `ArgReader`, `CommandArity`,
`CommandParseError`, and `CommandParseResult` do not exist.

- [ ] **Step 3: Commit the failing test**

```bash
git add yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/CommandContractTest.java
git commit -m "test: define command parser contract behavior"
```

## Task 2: Parser Contract Production Types

**Files:**
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ArgReader.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParser.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandHandler.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParseResult.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParseError.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandArity.java`
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParsers.java`
- Test: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/CommandContractTest.java`

- [ ] **Step 1: Add the parser and handler interfaces**

Create `CommandParser.java`:

```java
package yier.bubu.redis.command;

@FunctionalInterface
interface CommandParser<T> {
    CommandParseResult<T> parse(ArgReader args);
}
```

Create `CommandHandler.java`:

```java
package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;

@FunctionalInterface
interface CommandHandler<T> {
    void execute(T args, CommandContext ctx);
}
```

- [ ] **Step 2: Add parse result and parse error**

Create `CommandParseResult.java`:

```java
package yier.bubu.redis.command;

import java.util.Objects;

final class CommandParseResult<T> {
    private final T value;
    private final CommandParseError error;

    private CommandParseResult(T value, CommandParseError error) {
        this.value = value;
        this.error = error;
    }

    static <T> CommandParseResult<T> ok(T value) {
        return new CommandParseResult<>(Objects.requireNonNull(value, "value"), null);
    }

    static <T> CommandParseResult<T> error(CommandParseError error) {
        return new CommandParseResult<>(null, Objects.requireNonNull(error, "error"));
    }

    boolean ok() {
        return error == null;
    }

    T value() {
        return value;
    }

    CommandParseError error() {
        return error;
    }
}
```

Create `CommandParseError.java`:

```java
package yier.bubu.redis.command;

import java.util.Objects;

final class CommandParseError {
    private enum Kind {
        WRONG_ARITY,
        SYNTAX,
        INTEGER_OUT_OF_RANGE,
        CUSTOM
    }

    private final Kind kind;
    private final String commandLower;
    private final String message;

    private CommandParseError(Kind kind, String commandLower, String message) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.commandLower = commandLower;
        this.message = message;
    }

    static CommandParseError wrongArity(String commandLower) {
        if (commandLower == null || commandLower.isBlank()) {
            throw new IllegalArgumentException("commandLower must not be blank");
        }
        return new CommandParseError(Kind.WRONG_ARITY, commandLower, null);
    }

    static CommandParseError syntax() {
        return new CommandParseError(Kind.SYNTAX, null, null);
    }

    static CommandParseError integerOutOfRange() {
        return new CommandParseError(Kind.INTEGER_OUT_OF_RANGE, null, null);
    }

    static CommandParseError custom(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return new CommandParseError(Kind.CUSTOM, null, message);
    }

    String toReplyMessage() {
        return switch (kind) {
            case WRONG_ARITY -> "ERR wrong number of arguments for '" + commandLower + "' command";
            case SYNTAX -> "ERR syntax error";
            case INTEGER_OUT_OF_RANGE -> "ERR value is not an integer or out of range";
            case CUSTOM -> message;
        };
    }
}
```

- [ ] **Step 3: Add `ArgReader`**

Create `ArgReader.java`:

```java
package yier.bubu.redis.command;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

final class ArgReader {
    private final ExecutionRequest request;

    private ArgReader(ExecutionRequest request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    static ArgReader of(ExecutionRequest request) {
        return new ArgReader(request);
    }

    ExecutionRequest request() {
        return request;
    }

    int argc() {
        return request.argc();
    }

    boolean isNull(int index) {
        return request.isNull(index);
    }

    int len(int index) {
        return request.len(index);
    }

    byte[] bytes(int index) {
        return request.readOnlyByteArray(index);
    }

    boolean is(int index, String literal) {
        return CommandSupport.asciiEqualsIgnoreCase(request, index, literal);
    }

    long longAt(int index) {
        return CommandSupport.parseLong(request, index, "value");
    }

    long nonNegativeLongAt(int index) {
        return CommandSupport.parseNonNegativeLong(request, index, "value");
    }

    long positiveLongAt(int index) {
        long v = longAt(index);
        if (v <= 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
        return v;
    }

    int intClampedAt(int index) {
        return CommandSupport.parseIntClamped(request, index, "value");
    }

    BytesView view(CommandSupport support, int index) {
        return support.argView(request, index);
    }

    BytesSlice slice(CommandSupport support, int index) {
        return support.argSlice(request, index);
    }
}
```

- [ ] **Step 4: Add arity validators and stock parsers**

Create `CommandArity.java`:

```java
package yier.bubu.redis.command;

import java.util.Arrays;

final class CommandArity {
    private enum Kind {
        EXACT,
        MIN,
        RANGE,
        ONE_OF,
        PAIR_TAIL
    }

    private final Kind kind;
    private final String commandLower;
    private final int first;
    private final int second;
    private final int[] allowed;

    private CommandArity(Kind kind, String commandLower, int first, int second, int[] allowed) {
        this.kind = kind;
        this.commandLower = commandLower;
        this.first = first;
        this.second = second;
        this.allowed = allowed;
    }

    static CommandArity exact(int argc, String commandLower) {
        return new CommandArity(Kind.EXACT, commandLower, argc, 0, null);
    }

    static CommandArity min(int minArgc, String commandLower) {
        return new CommandArity(Kind.MIN, commandLower, minArgc, 0, null);
    }

    static CommandArity range(int minArgc, int maxArgc, String commandLower) {
        return new CommandArity(Kind.RANGE, commandLower, minArgc, maxArgc, null);
    }

    static CommandArity oneOf(String commandLower, int... allowedArgc) {
        if (allowedArgc == null || allowedArgc.length == 0) {
            throw new IllegalArgumentException("allowedArgc must not be empty");
        }
        return new CommandArity(Kind.ONE_OF, commandLower, 0, 0, Arrays.copyOf(allowedArgc, allowedArgc.length));
    }

    static CommandArity pairTail(int minArgc, int tailStartIndex, String commandLower) {
        return new CommandArity(Kind.PAIR_TAIL, commandLower, minArgc, tailStartIndex, null);
    }

    CommandParseError validate(ArgReader args) {
        int argc = args.argc();
        boolean ok = switch (kind) {
            case EXACT -> argc == first;
            case MIN -> argc >= first;
            case RANGE -> argc >= first && argc <= second;
            case ONE_OF -> contains(argc);
            case PAIR_TAIL -> argc >= first && ((argc - second) & 1) == 0;
        };
        return ok ? null : CommandParseError.wrongArity(commandLower);
    }

    private boolean contains(int argc) {
        for (int value : allowed) {
            if (value == argc) {
                return true;
            }
        }
        return false;
    }
}
```

Create `CommandParsers.java`:

```java
package yier.bubu.redis.command;

import java.util.Objects;
import java.util.function.Function;

final class CommandParsers {
    private CommandParsers() {
    }

    static CommandParser<ArgReader> passThrough() {
        return args -> CommandParseResult.ok(args);
    }

    static CommandParser<ArgReader> exact(int argc, String commandLower) {
        return arity(CommandArity.exact(argc, commandLower));
    }

    static CommandParser<ArgReader> min(int minArgc, String commandLower) {
        return arity(CommandArity.min(minArgc, commandLower));
    }

    static CommandParser<ArgReader> range(int minArgc, int maxArgc, String commandLower) {
        return arity(CommandArity.range(minArgc, maxArgc, commandLower));
    }

    static CommandParser<ArgReader> oneOf(String commandLower, int... allowedArgc) {
        return arity(CommandArity.oneOf(commandLower, allowedArgc));
    }

    static CommandParser<ArgReader> pairTail(int minArgc, int tailStartIndex, String commandLower) {
        return arity(CommandArity.pairTail(minArgc, tailStartIndex, commandLower));
    }

    static <T> CommandParser<T> arity(CommandArity arity, Function<ArgReader, T> mapper) {
        Objects.requireNonNull(arity, "arity");
        Objects.requireNonNull(mapper, "mapper");
        return args -> {
            CommandParseError error = arity.validate(args);
            if (error != null) {
                return CommandParseResult.error(error);
            }
            return CommandParseResult.ok(mapper.apply(args));
        };
    }

    private static CommandParser<ArgReader> arity(CommandArity arity) {
        return arity(arity, Function.identity());
    }
}
```

- [ ] **Step 5: Run the focused parser tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=CommandContractTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit parser primitives**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ArgReader.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParser.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandHandler.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParseResult.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParseError.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandArity.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParsers.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/CommandContractTest.java
git commit -m "feat: add command parser contract primitives"
```

## Task 3: Make `CommandSpec` Executable With Legacy Compatibility

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java`
- Test: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandDescriptorRegistryTest.java`

- [ ] **Step 1: Add compatibility tests for typed spec registration**

Extend `CommandMetadataRegressionTest.registrationInterfaceExposesUnifiedCommandSpecRegistration()` with:

```java
Class<?> parserType = Class.forName("yier.bubu.redis.command.CommandParser");
Class<?> handlerType = Class.forName("yier.bubu.redis.command.CommandHandler");
Assert.assertNotNull(CommandSpec.class.getMethod(
        "of",
        CommandDescriptor.class,
        parserType,
        handlerType
));
```

Add this test to `YierdisFastCommandProcessorRegistrationTest`:

```java
@Test
public void typedCommandSpecCanBeRegisteredAndExecuted() {
    YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
            TEST_ROUTER,
            null,
            YierdisChangeSink.NOOP,
            null,
            registration -> registration.register(
                    "TYPED",
                    CommandSpec.of(
                            CommandDescriptor.of(2, 0, 0, 0),
                            CommandParsers.exact(2, "typed"),
                            (args, ctx) -> ctx.out().bulkString(args.bytes(1))
                    )
            )
    );

    Assert.assertEquals("value", executeBulkString(processor, "TYPED", "value"));
}
```

Add this helper to the same test class:

```java
private static String executeBulkString(YierdisFastCommandProcessor processor, String command, String arg) {
    TestReplyWriter out = new TestReplyWriter();
    processor.execute(ByteArrayExecutionRequest.fromUtf8(command, List.of(arg)), new CommandContext(null, out));
    Assert.assertNull(out.error());
    return out.bulkString();
}
```

Extend `TestReplyWriter` in the same file with a bulk-string field and getter:

```java
private String bulkString;

private String bulkString() {
    return bulkString;
}
```

Replace the `bulkString(byte[] data)` override in `TestReplyWriter` with:

```java
@Override
public void bulkString(byte[] data) {
    this.bulkString = data == null ? null : new String(data, StandardCharsets.UTF_8);
}
```

- [ ] **Step 2: Run registration tests and verify they fail**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=YierdisFastCommandProcessorRegistrationTest test
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandMetadataRegressionTest test
```

Expected: compilation fails because `CommandSpec.of(...)` and typed registration support do not exist.

- [ ] **Step 3: Replace `CommandSpec` with typed executable contract**

Replace `CommandSpec.java` with:

```java
package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

/**
 * Unified command registration shape: parser + handler + metadata + MULTI policy.
 */
public final class CommandSpec<T> {
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandDescriptor descriptor;
    private final String disallowedInMultiError;

    public CommandSpec(
            CommandModule.Handler handler,
            CommandDescriptor descriptor,
            String disallowedInMultiError
    ) {
        this(
                args -> CommandParseResult.ok(args.request()),
                (args, ctx) -> handler.execute(args, ctx),
                descriptor,
                disallowedInMultiError
        );
    }

    private CommandSpec(
            CommandParser<T> parser,
            CommandHandler<T> handler,
            CommandDescriptor descriptor,
            String disallowedInMultiError
    ) {
        this.parser = Objects.requireNonNull(parser, "parser");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.disallowedInMultiError = disallowedInMultiError;
    }

    public static <T> CommandSpec<T> of(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler
    ) {
        return new CommandSpec<>(parser, handler, descriptor, null);
    }

    public static <T> CommandSpec<T> disallowedInMulti(
            CommandDescriptor descriptor,
            CommandParser<T> parser,
            CommandHandler<T> handler,
            String errorMessage
    ) {
        return new CommandSpec<>(parser, handler, descriptor, errorMessage);
    }

    CommandParseResult<T> parse(ExecutionRequest request) {
        return parser.parse(ArgReader.of(request));
    }

    void executeParsed(Object parsed, CommandContext ctx) {
        @SuppressWarnings("unchecked")
        T typed = (T) parsed;
        handler.execute(typed, ctx);
    }

    CommandModule.Handler handler() {
        return (request, ctx) -> {
            CommandParseResult<T> result = parse(request);
            if (!result.ok()) {
                ctx.out().error(result.error().toReplyMessage());
                return;
            }
            handler.execute(result.value(), ctx);
        };
    }

    public CommandDescriptor descriptor() {
        return descriptor;
    }

    public String disallowedInMultiError() {
        return disallowedInMultiError;
    }
}
```

- [ ] **Step 4: Add typed registration overloads**

Modify `CommandModule.Registration` by adding these default methods below `register(String, CommandSpec spec)`:

```java
default <T> void register(
        String name,
        CommandDescriptor descriptor,
        CommandParser<T> parser,
        CommandHandler<T> handler
) {
    register(name, CommandSpec.of(descriptor, parser, handler));
}

default <T> void registerDisallowedInMulti(
        String name,
        CommandDescriptor descriptor,
        CommandParser<T> parser,
        CommandHandler<T> handler,
        String errorMessage
) {
    register(name, CommandSpec.disallowedInMulti(descriptor, parser, handler, errorMessage));
}
```

Keep all existing legacy overloads in place.

- [ ] **Step 5: Preserve registry storage and helper behavior**

Modify `CommandRegistry.registerInternal(...)` so the inserted spec preserves the full executable spec rather than rebuilding only handler and descriptor:

```java
insert(new Entry(
        ascii,
        hash,
        spec
));
```

Keep the existing blank `disallowedInMultiError` validation before insertion.

- [ ] **Step 6: Run metadata and registration tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=YierdisFastCommandProcessorRegistrationTest,CommandContractTest test
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandMetadataRegressionTest,CommandDescriptorRegistryTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Commit executable spec compatibility**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSpec.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandMetadataRegressionTest.java
git commit -m "feat: make command specs executable"
```

## Task 4: Move Processor to Parse-Before-Handle

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Test: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java`

- [ ] **Step 1: Add a focused parse-before-handle test**

Add to `YierdisFastCommandProcessorRegistrationTest`:

```java
@Test
public void processorReturnsParseErrorBeforeTypedHandlerRuns() {
    final boolean[] handlerCalled = {false};
    YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
            TEST_ROUTER,
            null,
            YierdisChangeSink.NOOP,
            null,
            registration -> registration.register(
                    "STRICT",
                    CommandSpec.of(
                            CommandDescriptor.of(2, 0, 0, 0),
                            CommandParsers.exact(2, "strict"),
                            (args, ctx) -> {
                                handlerCalled[0] = true;
                                ctx.out().simpleString("OK");
                            }
                    )
            )
    );

    TestReplyWriter out = new TestReplyWriter();
    processor.execute(ByteArrayExecutionRequest.fromUtf8("STRICT", List.of()), new CommandContext(null, out));

    Assert.assertFalse(handlerCalled[0]);
    Assert.assertEquals("ERR wrong number of arguments for 'strict' command", out.error());
}
```

- [ ] **Step 2: Run the test and verify current pipeline fails**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=YierdisFastCommandProcessorRegistrationTest test
```

Expected: failure before processor centralizes parse handling.

- [ ] **Step 3: Add parse execution helper methods**

In `YierdisFastCommandProcessor`, add private helpers near `execute(...)`:

```java
private void executeSpec(CommandSpec<?> spec, ExecutionRequest request, CommandContext ctx) {
    CommandParseResult<?> parsed = spec.parse(request);
    if (!parsed.ok()) {
        ctx.out().error(parsed.error().toReplyMessage());
        return;
    }
    executeParsedSpec(spec, parsed.value(), ctx);
}

private void executeParsedSpec(CommandSpec<?> spec, Object parsed, CommandContext ctx) {
    spec.executeParsed(parsed, ctx);
}
```

- [ ] **Step 4: Use parse-before-handle in the normal path**

Replace this block:

```java
CommandModule.Handler handler = spec.handler();
boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
boolean changed = false;
if (sinkEnabled) {
    try (YierdisChangeTracking.Scope ignored = YierdisChangeTracking.beginScope()) {
        handler.execute(request, ctx);
        changed = YierdisChangeTracking.changedAny();
    }
} else {
    handler.execute(request, ctx);
}
```

with:

```java
boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
boolean changed = false;
if (sinkEnabled) {
    try (YierdisChangeTracking.Scope ignored = YierdisChangeTracking.beginScope()) {
        executeSpec(spec, request, ctx);
        changed = YierdisChangeTracking.changedAny();
    }
} else {
    executeSpec(spec, request, ctx);
}
```

- [ ] **Step 5: Run focused command tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command -Dtest=YierdisFastCommandProcessorRegistrationTest,CommandContractTest test
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandErrorTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit processor parse-before-handle**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorRegistrationTest.java
git commit -m "feat: parse command specs before handling"
```

## Task 5: Validate Transaction Queue Requests Before `QUEUED`

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`

- [ ] **Step 1: Add transaction parse validation tests**

Add to `TransactionCommandTest`:

```java
@Test
public void syntaxErrorInsideMultiAbortsBeforeExec() {
    forEachDb(db -> {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                db,
                null,
                SlowCommandGovernor.DEFAULT,
                registration -> registration.register(
                        "STRICT",
                        CommandSpec.of(
                                CommandDescriptor.of(2, 0, 0, 0),
                                CommandParsers.exact(2, "strict"),
                                (args, ctx) -> ctx.out().simpleString("OK")
                        )
                )
        );
        TestSession session = new TestSession();
        try (FastTestClient client = new FastTestClient(processor, session)) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

            ReplyObject wrongArity = client.execute(Arrays.asList(b("STRICT")));
            Assert.assertTrue(wrongArity instanceof ReplyError);
            Assert.assertEquals("ERR wrong number of arguments for 'strict' command", ((ReplyError) wrongArity).message());
            Assert.assertEquals(0, session.transactionState().size());

            ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
            Assert.assertTrue(exec instanceof ReplyError);
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
        }
    });
}

@Test
public void unknownCommandInsideMultiAbortsBeforeExec() {
    forEachDb(db -> {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        TestSession session = new TestSession();
        try (FastTestClient client = new FastTestClient(processor, session)) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

            ReplyObject unknown = client.execute(Arrays.asList(b("NO_SUCH_COMMAND")));
            Assert.assertTrue(unknown instanceof ReplyError);
            Assert.assertEquals("ERR unknown command 'NO_SUCH_COMMAND'", ((ReplyError) unknown).message());
            Assert.assertEquals(0, session.transactionState().size());

            ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
            Assert.assertTrue(exec instanceof ReplyError);
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
        }
    });
}
```

- [ ] **Step 2: Run transaction tests and verify failure**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=TransactionCommandTest test
```

Expected: the new typed wrong-arity test fails until the transaction path parses before queuing.

- [ ] **Step 3: Add transaction parse-before-queue helper**

Add this helper to `YierdisFastCommandProcessor`:

```java
private boolean validateBeforeQueue(CommandSpec<?> spec, ExecutionRequest request, TransactionState tx, ReplyWriter out) {
    CommandParseResult<?> parsed = spec.parse(request);
    if (parsed.ok()) {
        return true;
    }
    tx.markAborted();
    out.error(parsed.error().toReplyMessage());
    return false;
}
```

- [ ] **Step 4: Update the MULTI queue branch**

Replace the existing inner branch:

```java
CommandSpec spec = registry.spec(request);
String disallowedInMultiError = spec == null ? null : spec.disallowedInMultiError();
if (disallowedInMultiError != null) {
    tx.markAborted();
    out.error(disallowedInMultiError);
    return;
}
String enqueueErr = tx.tryEnqueue(request);
```

with:

```java
CommandSpec<?> spec = registry.spec(request);
if (spec == null) {
    tx.markAborted();
    out.error(unknownCommandMessage(request));
    return;
}
String disallowedInMultiError = spec.disallowedInMultiError();
if (disallowedInMultiError != null) {
    tx.markAborted();
    out.error(disallowedInMultiError);
    return;
}
if (!validateBeforeQueue(spec, request, tx, out)) {
    return;
}
String enqueueErr = tx.tryEnqueue(request);
```

- [ ] **Step 5: Run transaction and metadata tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=TransactionCommandTest,CommandErrorTest,CommandDescriptorRegistryTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit transaction validation**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java
git commit -m "feat: validate queued transaction commands"
```

## Task 6: Migrate Simple Arity and Pair-Tail Commands

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HashCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java`

- [ ] **Step 1: Add command-layer pair-tail assertions**

Extend `CommandErrorTest.arityAndSyntaxErrorsMatchExpectedMessages()` with:

```java
ReplyError hsetMissingValue = (ReplyError) client.execute(Arrays.asList(b("HSET"), b("k"), b("f1"), b("v1"), b("f2")));
Assert.assertEquals("ERR wrong number of arguments for 'hset' command", hsetMissingValue.message());

ReplyError zaddMissingMember = (ReplyError) client.execute(Arrays.asList(b("ZADD"), b("k"), b("1"), b("a"), b("2")));
Assert.assertEquals("ERR wrong number of arguments for 'zadd' command", zaddMissingMember.message());
```

- [ ] **Step 2: Migrate `HSET` registration and handler**

In `HashCommands.register(...)`, replace:

```java
registration.register("HSET", this::hset, CommandDescriptor.of(-4, 1, 1, 1));
```

with:

```java
registration.register(
        "HSET",
        CommandDescriptor.of(-4, 1, 1, 1),
        CommandParsers.pairTail(4, 2, "hset"),
        this::hset
);
```

Change `hset` signature and remove its arity check:

```java
private void hset(ArgReader args, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    int pairsLen = args.argc() - 2;
    ExecutionRequest request = args.request();
    support.sliceResetFromRequest(request, 2, pairsLen);
    try {
        long added = support.dbWrites(ctx).hashes().hset(request.readOnlyByteArray(1), support.slice());
        out.integer(added);
    } finally {
        support.clearScratch(pairsLen);
    }
}
```

Add this import if needed:

```java
import yier.bubu.redis.contract.ExecutionRequest;
```

- [ ] **Step 3: Migrate `ZADD` registration and handler**

In `ZSetCommands.register(...)`, replace:

```java
registration.register("ZADD", this::zadd, CommandDescriptor.of(-4, 1, 1, 1));
```

with:

```java
registration.register(
        "ZADD",
        CommandDescriptor.of(-4, 1, 1, 1),
        CommandParsers.pairTail(4, 2, "zadd"),
        this::zadd
);
```

Change `zadd` signature and remove its arity check:

```java
private void zadd(ArgReader args, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    int pairsLen = args.argc() - 2;
    ExecutionRequest request = args.request();
    support.sliceResetFromRequest(request, 2, pairsLen);
    try {
        long added = support.dbWrites(ctx).zsets().zadd(request.readOnlyByteArray(1), support.slice());
        out.integer(added);
    } finally {
        support.clearScratch(pairsLen);
    }
}
```

- [ ] **Step 4: Migrate low-risk exact arity string commands**

In `StringCommands.register(...)`, replace exact-arity registrations:

```java
registration.register("GET", this::get, CommandDescriptor.of(2, 1, 1, 1));
registration.register("STRLEN", this::strlen, CommandDescriptor.of(2, 1, 1, 1));
registration.register("APPEND", this::append, CommandDescriptor.of(3, 1, 1, 1));
registration.register("SETBIT", this::setbit, CommandDescriptor.of(4, 1, 1, 1));
registration.register("GETBIT", this::getbit, CommandDescriptor.of(3, 1, 1, 1));
registration.register("INCR", this::incr, CommandDescriptor.of(2, 1, 1, 1));
registration.register("DECR", this::decr, CommandDescriptor.of(2, 1, 1, 1));
```

with:

```java
registration.register("GET", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "get"), this::get);
registration.register("STRLEN", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "strlen"), this::strlen);
registration.register("APPEND", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exact(3, "append"), this::append);
registration.register("SETBIT", CommandDescriptor.of(4, 1, 1, 1), CommandParsers.exact(4, "setbit"), this::setbit);
registration.register("GETBIT", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exact(3, "getbit"), this::getbit);
registration.register("INCR", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "incr"), this::incr);
registration.register("DECR", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "decr"), this::decr);
```

For each migrated handler, change the first parameter to `ArgReader args`, set `ExecutionRequest request = args.request();`, and remove the top-level arity check.

- [ ] **Step 5: Add built-in transaction validation coverage after `GET` migration**

Add to `TransactionCommandTest`:

```java
@Test
public void builtInWrongArityInsideMultiAbortsBeforeExecAfterParserMigration() {
    forEachDb(db -> {
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        TestSession session = new TestSession();
        try (FastTestClient client = new FastTestClient(processor, session)) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("MULTI")))).value());

            ReplyObject wrongArity = client.execute(Arrays.asList(b("GET")));
            Assert.assertTrue(wrongArity instanceof ReplyError);
            Assert.assertEquals("ERR wrong number of arguments for 'get' command", ((ReplyError) wrongArity).message());
            Assert.assertEquals(0, session.transactionState().size());

            ReplyObject exec = client.execute(Arrays.asList(b("EXEC")));
            Assert.assertTrue(exec instanceof ReplyError);
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", ((ReplyError) exec).message());
        }
    });
}
```

- [ ] **Step 6: Run focused behavior tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandErrorTest,CommandProcessorTest,ZSetCommandTest,HashCommandTest,TransactionCommandTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Commit simple command migration**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HashCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TransactionCommandTest.java
git commit -m "refactor: migrate simple commands to command parsers"
```

## Task 7: Migrate `SCAN` and Sorted-Set Range Parsers

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java`
- Test: existing `ScanCursorContractTest`, `ZSetCommandTest`, and `Milestone1CompatTest`

- [ ] **Step 1: Add option parser behavior tests**

Extend `CommandErrorTest.arityAndSyntaxErrorsMatchExpectedMessages()` with:

```java
ReplyError scanMissingMatch = (ReplyError) client.execute(Arrays.asList(b("SCAN"), b("0"), b("MATCH")));
Assert.assertEquals("ERR syntax error", scanMissingMatch.message());

ReplyError scanBadCount = (ReplyError) client.execute(Arrays.asList(b("SCAN"), b("0"), b("COUNT"), b("x")));
Assert.assertEquals("ERR value is not an integer or out of range", scanBadCount.message());

ReplyError zrangeDuplicateUnknown = (ReplyError) client.execute(Arrays.asList(b("ZRANGE"), b("k"), b("0"), b("-1"), b("WITHSCORES"), b("BAD")));
Assert.assertEquals("ERR syntax error", zrangeDuplicateUnknown.message());
```

- [ ] **Step 2: Add local parsed records in `KeyCommands`**

Inside `KeyCommands`, add near the `scan` method:

```java
private record ScanArgs(long cursor, byte[] match, int count) {
}
```

Add parser method:

```java
private CommandParseResult<ScanArgs> parseScan(ArgReader args) {
    CommandParseError arity = CommandArity.min(2, "scan").validate(args);
    if (arity != null) {
        return CommandParseResult.error(arity);
    }
    long cursor;
    try {
        cursor = args.nonNegativeLongAt(1);
    } catch (IllegalArgumentException e) {
        return CommandParseResult.error(CommandParseError.integerOutOfRange());
    }

    byte[] match = null;
    int count = 10;
    for (int i = 2; i < args.argc(); i++) {
        if (args.is(i, "MATCH")) {
            if (i + 1 >= args.argc()) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            match = args.bytes(++i);
            continue;
        }
        if (args.is(i, "COUNT")) {
            if (i + 1 >= args.argc()) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            long v;
            try {
                v = args.nonNegativeLongAt(++i);
            } catch (IllegalArgumentException e) {
                return CommandParseResult.error(CommandParseError.integerOutOfRange());
            }
            if (v <= 0 || v > Integer.MAX_VALUE) {
                return CommandParseResult.error(CommandParseError.integerOutOfRange());
            }
            count = (int) v;
            continue;
        }
        return CommandParseResult.error(CommandParseError.syntax());
    }
    return CommandParseResult.ok(new ScanArgs(cursor, match, count));
}
```

- [ ] **Step 3: Register and handle `SCAN` with parsed args**

Replace `SCAN` registration:

```java
registration.register("SCAN", this::scan, CommandDescriptor.of(-2, 0, 0, 0));
```

with:

```java
registration.register("SCAN", CommandDescriptor.of(-2, 0, 0, 0), this::parseScan, this::scan);
```

Replace the `scan` method body with:

```java
private void scan(ScanArgs args, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    List<byte[]> keys = new ArrayList<>();
    ScanCursorV2 next = support.dbReads(ctx).keyspace().scan(
            ScanCursorV2.of(args.cursor()),
            args.match(),
            args.count(),
            keys
    );

    out.arrayHeader(2);
    out.bulkString(next.toBulkStringAscii());
    out.bulkStringArray(keys);
}
```

- [ ] **Step 4: Add local parsed record and parser for `ZRANGE`**

Inside `ZSetCommands`, add:

```java
private record ZRangeArgs(byte[] key, long start, long stop, boolean withScores, boolean rev) {
}
```

Add:

```java
private CommandParseResult<ZRangeArgs> parseZRange(ArgReader args) {
    CommandParseError arity = CommandArity.range(4, 6, "zrange").validate(args);
    if (arity != null) {
        return CommandParseResult.error(arity);
    }
    long start;
    long stop;
    try {
        start = args.longAt(2);
        stop = args.longAt(3);
    } catch (IllegalArgumentException e) {
        return CommandParseResult.error(CommandParseError.integerOutOfRange());
    }
    boolean withScores = false;
    boolean rev = false;
    for (int i = 4; i < args.argc(); i++) {
        if (args.is(i, "WITHSCORES")) {
            if (withScores) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            withScores = true;
            continue;
        }
        if (args.is(i, "REV")) {
            if (rev) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            rev = true;
            continue;
        }
        return CommandParseResult.error(CommandParseError.syntax());
    }
    return CommandParseResult.ok(new ZRangeArgs(args.bytes(1), start, stop, withScores, rev));
}
```

Change `ZRANGE` registration:

```java
registration.register("ZRANGE", CommandDescriptor.of(-4, 1, 1, 1), this::parseZRange, this::zrange);
```

Change handler:

```java
private void zrange(ZRangeArgs args, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    BulkStringSequence seq = args.rev()
            ? support.dbReads(ctx).zsets().zrevrange(args.key(), args.start(), args.stop(), args.withScores())
            : support.dbReads(ctx).zsets().zrange(args.key(), args.start(), args.stop(), args.withScores());
    int count = seq.count();
    out.arrayHeader(count);
    if (count == 0) {
        return;
    }
    seq.emitTo(new BulkStringReplyAdapter(out));
}
```

- [ ] **Step 5: Migrate score range option parsers**

Add this shared record in `ZSetCommands`:

```java
private record ZRangeByScoreArgs(
        byte[] key,
        CommandSupport.ScoreBound min,
        CommandSupport.ScoreBound max,
        boolean withScores,
        long offset,
        long count
) {
}
```

Add this parser:

```java
private CommandParseResult<ZRangeByScoreArgs> parseZRangeByScore(ArgReader args, boolean reverse) {
    String commandLower = reverse ? "zrevrangebyscore" : "zrangebyscore";
    CommandParseError arity = CommandArity.min(4, commandLower).validate(args);
    if (arity != null) {
        return CommandParseResult.error(arity);
    }
    CommandSupport.ScoreBound first = CommandSupport.parseScoreBound(args.bytes(2));
    CommandSupport.ScoreBound second = CommandSupport.parseScoreBound(args.bytes(3));
    CommandSupport.ScoreBound min = reverse ? second : first;
    CommandSupport.ScoreBound max = reverse ? first : second;
    boolean withScores = false;
    long offset = 0;
    long count = Long.MAX_VALUE;

    int i = 4;
    while (i < args.argc()) {
        if (args.is(i, "WITHSCORES")) {
            if (withScores) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            withScores = true;
            i++;
            continue;
        }
        if (args.is(i, "LIMIT")) {
            if (i + 2 >= args.argc()) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            try {
                offset = args.nonNegativeLongAt(i + 1);
                count = args.nonNegativeLongAt(i + 2);
            } catch (IllegalArgumentException e) {
                return CommandParseResult.error(CommandParseError.integerOutOfRange());
            }
            i += 3;
            continue;
        }
        return CommandParseResult.error(CommandParseError.syntax());
    }
    return CommandParseResult.ok(new ZRangeByScoreArgs(args.bytes(1), min, max, withScores, offset, count));
}
```

Register:

```java
registration.register(
        "ZRANGEBYSCORE",
        CommandDescriptor.of(-4, 1, 1, 1),
        args -> parseZRangeByScore(args, false),
        this::zrangebyscore
);
registration.register(
        "ZREVRANGEBYSCORE",
        CommandDescriptor.of(-4, 1, 1, 1),
        args -> parseZRangeByScore(args, true),
        this::zrevrangebyscore
);
```

Change both handlers to accept `ZRangeByScoreArgs` and use fields from the record. Keep the existing DB calls and reply emission logic.

- [ ] **Step 6: Run range and scan tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandErrorTest,ScanCursorContractTest,ZSetCommandTest,Milestone1CompatTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Commit option parser migration**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java
git commit -m "refactor: migrate scan and zset range parsing"
```

## Task 8: Migrate `SET` Parser

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java`
- Test: existing `CommandProcessorTest`, `Milestone1CompatTest`, `YierdisChangeSinkTest`

- [ ] **Step 1: Add `SET` parser edge-case tests**

Extend `CommandErrorTest.arityAndSyntaxErrorsMatchExpectedMessages()` with:

```java
ReplyError setModeConflict = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")));
Assert.assertEquals("ERR syntax error", setModeConflict.message());

ReplyError setMissingExpire = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX")));
Assert.assertEquals("ERR syntax error", setMissingExpire.message());

ReplyError setInvalidExpire = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("0")));
Assert.assertEquals("ERR invalid expire time in 'set' command", setInvalidExpire.message());

ReplyError setDuplicateGet = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("GET"), b("GET")));
Assert.assertEquals("ERR syntax error", setDuplicateGet.message());
```

- [ ] **Step 2: Add parsed `SetArgs` record**

Inside `StringCommands`, add:

```java
private record SetArgs(ExecutionRequest request, byte[] key, int valueIndex, SetMode mode, ExpireOption expire, boolean getOld) {
}
```

- [ ] **Step 3: Add `SET` parser method**

Add:

```java
private CommandParseResult<SetArgs> parseSet(ArgReader args) {
    CommandParseError arity = CommandArity.min(3, "set").validate(args);
    if (arity != null) {
        return CommandParseResult.error(arity);
    }

    byte[] key = args.bytes(1);
    SetMode mode = SetMode.NORMAL;
    ExpireOption expire = null;
    boolean getOld = false;

    for (int i = 3; i < args.argc(); i++) {
        if (args.is(i, "NX")) {
            if (mode != SetMode.NORMAL) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            mode = SetMode.NX;
            continue;
        }
        if (args.is(i, "XX")) {
            if (mode != SetMode.NORMAL) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            mode = SetMode.XX;
            continue;
        }
        if (args.is(i, "GET")) {
            if (getOld) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            getOld = true;
            continue;
        }
        if (args.is(i, "KEEPTTL")) {
            if (expire != null) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            expire = ExpireOption.keepTtl();
            continue;
        }
        if (args.is(i, "EX") || args.is(i, "PX") || args.is(i, "EXAT") || args.is(i, "PXAT")) {
            if (expire != null || i + 1 >= args.argc()) {
                return CommandParseResult.error(CommandParseError.syntax());
            }
            String option = CommandSupport.utf8(args.bytes(i));
            long value;
            try {
                value = args.longAt(++i);
            } catch (IllegalArgumentException e) {
                return CommandParseResult.error(CommandParseError.integerOutOfRange());
            }
            if (value <= 0) {
                return CommandParseResult.error(CommandParseError.custom("ERR invalid expire time in 'set' command"));
            }
            if ("EX".equalsIgnoreCase(option)) {
                expire = ExpireOption.ex(value);
                continue;
            }
            if ("PX".equalsIgnoreCase(option)) {
                expire = ExpireOption.px(value);
                continue;
            }
            if ("EXAT".equalsIgnoreCase(option)) {
                long expireAtMillis;
                try {
                    expireAtMillis = Math.multiplyExact(value, 1000L);
                } catch (ArithmeticException e) {
                    expireAtMillis = Long.MAX_VALUE;
                }
                if (expireAtMillis <= System.currentTimeMillis()) {
                    return CommandParseResult.error(CommandParseError.custom("ERR invalid expire time in 'set' command"));
                }
                expire = ExpireOption.exAt(value);
                continue;
            }
            if (value <= System.currentTimeMillis()) {
                return CommandParseResult.error(CommandParseError.custom("ERR invalid expire time in 'set' command"));
            }
            expire = ExpireOption.pxAt(value);
            continue;
        }
        return CommandParseResult.error(CommandParseError.syntax());
    }

    return CommandParseResult.ok(new SetArgs(args.request(), key, 2, mode, expire, getOld));
}
```

- [ ] **Step 4: Register and handle `SET` with parsed args**

Replace `SET` registration:

```java
registration.register("SET", this::set, CommandDescriptor.of(-3, 1, 1, 1));
```

with:

```java
registration.register("SET", CommandDescriptor.of(-3, 1, 1, 1), this::parseSet, this::set);
```

Change `set` handler:

```java
private void set(SetArgs args, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    var result = support.dbWrites(ctx).strings().set(
            args.key(),
            support.argSlice(args.request(), args.valueIndex()),
            args.mode(),
            args.expire(),
            args.getOld()
    );
    if (!result.applied()) {
        out.bulkString((byte[]) null);
        return;
    }
    if (args.getOld()) {
        out.bulkString(result.oldValue());
        return;
    }
    out.simpleString("OK");
}
```

- [ ] **Step 5: Run `SET` behavior tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandErrorTest,CommandProcessorTest,Milestone1CompatTest,YierdisChangeSinkTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit `SET` parser migration**

```bash
git add yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandErrorTest.java
git commit -m "refactor: migrate set option parsing"
```

## Task 9: Guardrails and Legacy Cleanup

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Optionally modify: `docs/commands-and-data-model.md`
- Optionally modify: `docs/request-execution-flow.md`

- [ ] **Step 1: Add architecture guard for direct syntax errors in migrated command files**

In `ArchitectureBoundaryTest`, add a test that scans migrated command files for direct syntax replies:

```java
@Test
public void migratedCommandsDoNotWriteSyntaxErrorsDirectly() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("无法定位仓库根目录", repoRoot);
    List<String> offenders = new ArrayList<>();
    for (String relative : List.of(
            "yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java",
            "yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java",
            "yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java"
    )) {
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve(relative),
                offenders,
                "out.error(\"ERR syntax error\")"
        );
    }
    Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
}
```

- [ ] **Step 2: Add architecture guard for DB-owned request syntax**

In the same test class, add a scan that prevents pair-count syntax from staying in DB ops after `HSET` and `ZADD` are parser-owned:

```java
@Test
public void dbLayerDoesNotOwnCommandPairTailSyntax() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("无法定位仓库根目录", repoRoot);
    List<String> offenders = new ArrayList<>();
    scanFileForForbiddenText(
            repoRoot,
            repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java"),
            offenders,
            "wrong number of arguments for 'hset' command"
    );
    scanFileForForbiddenText(
            repoRoot,
            repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java"),
            offenders,
            "wrong number of arguments for 'zadd' command"
    );
    Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
}
```

Replace the corresponding pair-count checks in `YierdisHashOps.hset(...)` with an API invariant message:

```java
if ((fieldValuePairs.size() & 1) != 0) {
    throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
}
```

Replace the corresponding pair-count checks in `YierdisZSetOps.zadd(...)` with an API invariant message:

```java
if ((scoreMemberPairs.size() & 1) != 0) {
    throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
}
```

- [ ] **Step 3: Run architecture tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest,CommandErrorTest test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Run broader command and server wiring tests**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-command test
mvn -pl yierdis-core/yierdis-core-runtime -Dtest=CommandErrorTest,CommandProcessorTest,TransactionCommandTest,CommandDescriptorRegistryTest,CommandMetadataRegressionTest,ZSetCommandTest,ScanCursorContractTest,Milestone1CompatTest test
mvn -pl yierdis-server -Dtest=YierdisServerBootstrapCommandWiringTest test
```

Expected: `BUILD SUCCESS` for all three commands.

- [ ] **Step 5: Commit guardrails**

```bash
git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java
git commit -m "test: guard command parsing ownership"
```

## Task 10: Final Verification

**Files:**
- No planned source edits

- [ ] **Step 1: Run full Maven verification**

Run:

```bash
mvn test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 2: Inspect remaining ad hoc command syntax writes**

Run:

```bash
rg -n "wrongArity|ERR syntax error|wrong number of arguments" yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java
```

Expected:

- `CommandSupport.wrongArity(...)` remains while legacy commands still use it.
- Migrated `SET`, `SCAN`, and `ZRANGE` paths do not write `ERR syntax error` directly.
- Server-local commands may still have server-owned arity checks until they migrate to typed specs.

- [ ] **Step 3: Inspect final diff**

Run:

```bash
git status --short
git log --oneline -10
```

Expected:

- `git status --short` shows no uncommitted changes from this implementation.
- Recent commits include each task commit from this plan.

## Self-Review Checklist

- Spec coverage: Tasks 2-4 implement the executable command contract and processor lifecycle; Task 5 implements transaction parse-before-queue; Tasks 6-8 migrate simple, option-heavy, and `SET` parsing; Task 9 adds ownership guardrails.
- No placeholder terms are intentionally used in implementation steps.
- Type consistency: `CommandParser<T>` returns `CommandParseResult<T>`, `CommandHandler<T>` accepts `T`, `CommandSpec<T>` binds both, and `ArgReader` is the default parsed value for stock arity parsers.
