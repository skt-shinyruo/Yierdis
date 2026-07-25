package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.List;

public class CommandDefinitionTest {
    @Test
    public void definitionHasOnlyFinalSyntaxParserPreparerComponents() {
        Assert.assertTrue(CommandDefinition.class.isRecord());
        Assert.assertArrayEquals(
                new Class<?>[]{CommandSyntax.class, CommandParser.class, CommandPreparer.class},
                java.util.Arrays.stream(CommandDefinition.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getType)
                        .toArray(Class<?>[]::new)
        );
        Assert.assertThrows(NoSuchMethodException.class,
                () -> CommandDefinition.class.getMethod("handler"));
        Assert.assertThrows(NoSuchMethodException.class,
                () -> CommandDefinition.class.getMethod("replyPlanner"));
    }

    @Test
    public void definitionValidatesItsSyntaxBeforeInvokingTheCustomParser() {
        java.util.concurrent.atomic.AtomicInteger parserCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        CommandSyntax syntax = new CommandSyntax(
                "AUTH", CommandArity.min(2), CommandKeySpec.NONE,
                TransactionPolicy.QUEUEABLE);
        CommandDefinition<ArgReader> definition = new CommandDefinition<>(
                syntax,
                args -> {
                    parserCalls.incrementAndGet();
                    return CommandParseResult.ok(args);
                },
                (args, context) -> null
        );

        CommandParseResult<ArgReader> invalid = definition.parse(request("AUTH"));
        Assert.assertFalse(invalid.ok());
        Assert.assertEquals(0, parserCalls.get());
        Assert.assertSame(syntax, definition.syntax());
    }

    private static CommandSyntax syntax(String name, CommandArity arity) {
        return new CommandSyntax(name, arity, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE);
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], List.of(args).subList(1, args.length));
    }
}
