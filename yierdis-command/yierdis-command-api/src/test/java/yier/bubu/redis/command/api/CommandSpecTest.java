package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;

import java.lang.reflect.Method;
import java.util.Arrays;

public class CommandSpecTest {
    @Test
    public void specContainsOnlySyntaxAndHandler() {
        Assert.assertTrue(CommandSpec.class.isRecord());
        Assert.assertArrayEquals(
                new Class<?>[]{CommandSyntax.class, CommandHandler.class},
                Arrays.stream(CommandSpec.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getType)
                        .toArray(Class<?>[]::new)
        );
    }

    @Test
    public void specRejectsNullSyntaxAndHandler() {
        CommandSyntax syntax = syntax("GET");
        CommandHandler handler = args -> session -> null;

        Assert.assertThrows(NullPointerException.class, () -> new CommandSpec(null, handler));
        Assert.assertThrows(NullPointerException.class, () -> new CommandSpec(syntax, null));
    }

    @Test
    public void syntaxRejectsInvalidMetadataImmediately() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandSyntax("  ", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandSyntax("G\u00c9T", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE));
        Assert.assertThrows(NullPointerException.class,
                () -> new CommandSyntax("GET", null, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE));
        Assert.assertThrows(NullPointerException.class,
                () -> new CommandSyntax("GET", CommandArity.exact(1), null, TransactionPolicy.QUEUEABLE));
        Assert.assertThrows(NullPointerException.class,
                () -> new CommandSyntax("GET", CommandArity.exact(1), CommandKeySpec.NONE, null));
    }

    @Test
    public void parseExceptionRetainsItsExactReplyMessage() {
        CommandParseException failure = new CommandParseException("ERR syntax error");

        Assert.assertEquals("ERR syntax error", failure.replyMessage());
        Assert.assertEquals("ERR syntax error", failure.getMessage());
    }

    @Test
    public void invocationOnlyReceivesTheCommandSession() throws Exception {
        Method prepare = CommandInvocation.class.getMethod("prepare", CommandSession.class);

        Assert.assertEquals(PreparedCommand.class, prepare.getReturnType());
        Assert.assertArrayEquals(new Class<?>[]{CommandSession.class}, prepare.getParameterTypes());
        Assert.assertEquals(1, Arrays.stream(CommandInvocation.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("prepare"))
                .count());
    }

    @Test
    public void registrationExposesDirectSpecsAndTheLegacyDefinitionBridge() {
        Assert.assertTrue(hasMethod("register", void.class, CommandSpec.class));
        Assert.assertTrue(hasMethod("register", void.class, CommandDefinition.class));
        Assert.assertTrue(hasMethod("specByUpperName", CommandSpec.class, String.class));
        Assert.assertEquals(2, Arrays.stream(CommandModule.Registration.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("register"))
                .count());
    }

    private static boolean hasMethod(String name, Class<?> returnType, Class<?>... parameterTypes) {
        return Arrays.stream(CommandModule.Registration.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().equals(name)
                        && method.getReturnType().equals(returnType)
                        && Arrays.equals(method.getParameterTypes(), parameterTypes));
    }

    private static CommandSyntax syntax(String nameUpper) {
        return new CommandSyntax(
                nameUpper,
                CommandArity.exact(2),
                CommandKeySpec.NONE,
                TransactionPolicy.QUEUEABLE
        );
    }
}
