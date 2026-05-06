package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

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
