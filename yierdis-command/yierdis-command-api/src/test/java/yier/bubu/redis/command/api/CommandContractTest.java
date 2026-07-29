package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.lang.reflect.Method;
import java.util.Arrays;

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
    public void arityValidatorsAcceptValidCountsAndThrowCanonicalErrors() throws Exception {
        CommandArity.exact(2).validate("get", commandArgs("GET", "k"));
        assertWrongArity("get", CommandArity.exact(2), commandArgs("GET"));

        CommandArity.min(3).validate("del", commandArgs("DEL", "a", "b"));
        assertWrongArity("del", CommandArity.min(3), commandArgs("DEL", "a"));

        CommandArity.range(4, 6).validate("zrange", commandArgs("ZRANGE", "z", "0", "-1"));
        CommandArity.range(4, 6).validate(
                "zrange", commandArgs("ZRANGE", "z", "0", "-1", "WITHSCORES", "REV"));
        assertWrongArity(
                "zrange",
                CommandArity.range(4, 6),
                commandArgs("ZRANGE", "z", "0", "-1", "WITHSCORES", "REV", "X")
        );

        CommandArity.oneOf(1, 2).validate("ping", commandArgs("PING"));
        CommandArity.oneOf(1, 2).validate("ping", commandArgs("PING", "hello"));
        assertWrongArity("ping", CommandArity.oneOf(1, 2), commandArgs("PING", "a", "b"));

        CommandArity.pairTail(4, 2).validate("hset", commandArgs("HSET", "h", "f", "v"));
        CommandArity.pairTail(4, 2).validate(
                "hset", commandArgs("HSET", "h", "f1", "v1", "f2", "v2"));
        assertWrongArity("hset", CommandArity.pairTail(4, 2), commandArgs("HSET", "h", "f"));
        assertWrongArity(
                "hset",
                CommandArity.pairTail(4, 2),
                commandArgs("HSET", "h", "f1", "v1", "f2")
        );
    }

    @Test
    public void handlerOnlyReceivesCommandArgs() throws Exception {
        Method parse = CommandHandler.class.getMethod("parse", CommandArgs.class);

        Assert.assertEquals(CommandInvocation.class, parse.getReturnType());
        Assert.assertArrayEquals(new Class<?>[]{CommandArgs.class}, parse.getParameterTypes());
        Assert.assertArrayEquals(new Class<?>[]{CommandParseException.class}, parse.getExceptionTypes());
        Assert.assertEquals(1, Arrays.stream(CommandHandler.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("parse"))
                .count());
    }

    @Test
    public void serverServicesUseSemanticRepliesAndSessions() throws Exception {
        Method info = ServerInfoProvider.class.getMethod("info", CommandArgs.class, CommandSession.class);
        Method stats = ServerInfoProvider.class.getMethod("stats", CommandSession.class);
        Method memoryStats = ServerInfoProvider.class.getMethod("memoryStats", CommandSession.class);
        Method keysBudget = SlowCommandGovernor.class.getMethod("keysTimeBudgetNanos", CommandSession.class);
        Method keysMaxResults = SlowCommandGovernor.class.getMethod("keysMaxResults", CommandSession.class);

        Assert.assertEquals(RedisReply.class, info.getReturnType());
        Assert.assertEquals(RedisReply.class, stats.getReturnType());
        Assert.assertEquals(YierdisMemoryStats.class, memoryStats.getReturnType());
        Assert.assertEquals(long.class, keysBudget.getReturnType());
        Assert.assertEquals(int.class, keysMaxResults.getReturnType());
        Assert.assertEquals(3, Arrays.stream(ServerInfoProvider.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .count());
        Assert.assertEquals(2, Arrays.stream(SlowCommandGovernor.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .count());
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

    private static CommandArgs commandArgs(String command, String... rest) {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8(command, Arrays.asList(rest));
        return CommandArgs.of(request);
    }

    private static void assertWrongArity(String commandLower, CommandArity arity, CommandArgs args) {
        CommandParseException failure = Assert.assertThrows(
                CommandParseException.class,
                () -> arity.validate(commandLower, args)
        );
        Assert.assertEquals(
                "ERR wrong number of arguments for '" + commandLower + "' command",
                failure.replyMessage()
        );
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
