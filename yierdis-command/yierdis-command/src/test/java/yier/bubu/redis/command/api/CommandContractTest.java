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

        Assert.assertEquals(java.util.function.Function.class, parse.getReturnType());
        Assert.assertArrayEquals(new Class<?>[]{CommandArgs.class}, parse.getParameterTypes());
        Assert.assertEquals(1, Arrays.stream(CommandHandler.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("parse"))
                .count());
    }

    @Test
    public void serverServicesUseSemanticRepliesAndSessions() throws Exception {
        Method info = ServerInfoProvider.class.getMethod("info", CommandArgs.class, CommandSession.class);
        Method stats = ServerInfoProvider.class.getMethod("stats", CommandSession.class);
        Method memoryStats = ServerInfoProvider.class.getMethod("memoryStats", CommandSession.class);
        Assert.assertEquals(RedisReply.class, info.getReturnType());
        Assert.assertEquals(RedisReply.class, stats.getReturnType());
        Assert.assertEquals(YierdisMemoryStats.class, memoryStats.getReturnType());
        Assert.assertEquals(3, Arrays.stream(ServerInfoProvider.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .count());
    }

    private static CommandArgs commandArgs(String command, String... rest) {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8(command, Arrays.asList(rest));
        return new CommandArgs(request);
    }

    private static void assertWrongArity(String commandLower, CommandArity arity, CommandArgs args) {
        CommandParseException failure = Assert.assertThrows(
                CommandParseException.class,
                () -> arity.validate(commandLower, args)
        );
        Assert.assertEquals(
                "ERR wrong number of arguments for '" + commandLower + "' command",
                failure.getMessage()
        );
    }
}
