package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class DefaultCommandDispatchErrorTest {
    private static final List<CommandErrorCase> REGISTERED_COMMAND_ERROR_CASES = registeredCommandErrorCases();

    @Test
    public void registeredCommandsRejectRepresentativeInvalidRequests() {
        withClient(client -> {
            for (CommandErrorCase testCase : REGISTERED_COMMAND_ERROR_CASES) {
                assertError(testCase, client.execute(cmd(testCase.args())));
            }
        });
    }

    @Test
    public void commandProcessorRejectsEmptyAndUnknownCommandsBeforeDispatch() {
        withClient(client -> {
            assertError("ERR empty command", client.execute(Collections.emptyList()));
            assertError("ERR empty command", client.execute(Arrays.asList(b(""))));
            assertError("ERR unknown command 'NOPE'", client.execute(cmd("NOPE")));
            assertError("ERR unknown command", client.execute(Arrays.asList(new byte[]{0})));
            assertError("ERR Protocol error: null bulk string", client.execute(Arrays.asList(b("SET"), b("k"), null)));
        });
    }

    private static List<CommandErrorCase> registeredCommandErrorCases() {
        List<CommandErrorCase> cases = new ArrayList<>();
        errorCase(cases, wrongArity("auth"), "AUTH");
        errorCase(cases, wrongArity("append"), "APPEND");
        errorCase(cases, wrongArity("bitcount"), "BITCOUNT");
        errorCase(cases, wrongArity("client"), "CLIENT");
        errorCase(cases, wrongArity("command"), "COMMAND", "INFO");
        errorCase(cases, wrongArity("decr"), "DECR");
        errorCase(cases, wrongArity("del"), "DEL");
        errorCase(cases, wrongArity("discard"), "DISCARD", "extra");
        errorCase(cases, wrongArity("echo"), "ECHO");
        errorCase(cases, wrongArity("exec"), "EXEC", "extra");
        errorCase(cases, wrongArity("exists"), "EXISTS");
        errorCase(cases, wrongArity("expire"), "EXPIRE", "key");
        errorCase(cases, wrongArity("expireat"), "EXPIREAT", "key");
        errorCase(cases, wrongArity("flushdb"), "FLUSHDB", "SYNC", "EXTRA");
        errorCase(cases, wrongArity("get"), "GET");
        errorCase(cases, wrongArity("getbit"), "GETBIT", "key");
        errorCase(cases, wrongArity("hdel"), "HDEL", "hash");
        errorCase(cases, wrongArity("hget"), "HGET", "hash");
        errorCase(cases, wrongArity("hgetall"), "HGETALL");
        errorCase(cases, wrongArity("hlen"), "HLEN");
        errorCase(cases, wrongArity("hscan"), "HSCAN", "hash");
        errorCase(cases, wrongArity("hset"), "HSET", "hash", "field");
        errorCase(cases, wrongArity("incr"), "INCR");
        errorCase(cases, wrongArity("keys"), "KEYS");
        errorCase(cases, wrongArity("lpop"), "LPOP");
        errorCase(cases, wrongArity("lpush"), "LPUSH", "list");
        errorCase(cases, wrongArity("lrange"), "LRANGE", "list", "0");
        errorCase(cases, wrongArity("memory"), "MEMORY");
        errorCase(cases, wrongArity("multi"), "MULTI", "extra");
        errorCase(cases, wrongArity("object"), "OBJECT");
        errorCase(cases, wrongArity("persist"), "PERSIST");
        errorCase(cases, wrongArity("pexpire"), "PEXPIRE", "key");
        errorCase(cases, wrongArity("pexpireat"), "PEXPIREAT", "key");
        errorCase(cases, wrongArity("pfadd"), "PFADD", "hll");
        errorCase(cases, wrongArity("pfcount"), "PFCOUNT");
        errorCase(cases, wrongArity("pfmerge"), "PFMERGE", "dst");
        errorCase(cases, wrongArity("ping"), "PING", "a", "b");
        errorCase(cases, wrongArity("pttl"), "PTTL");
        errorCase(cases, wrongArity("quit"), "QUIT", "extra");
        errorCase(cases, wrongArity("rpop"), "RPOP");
        errorCase(cases, wrongArity("rpush"), "RPUSH", "list");
        errorCase(cases, wrongArity("sadd"), "SADD", "set");
        errorCase(cases, wrongArity("scan"), "SCAN");
        errorCase(cases, wrongArity("scard"), "SCARD");
        errorCase(cases, wrongArity("select"), "SELECT");
        errorCase(cases, wrongArity("set"), "SET", "key");
        errorCase(cases, wrongArity("setbit"), "SETBIT", "key", "0");
        errorCase(cases, wrongArity("sismember"), "SISMEMBER", "set");
        errorCase(cases, wrongArity("smembers"), "SMEMBERS");
        errorCase(cases, wrongArity("srem"), "SREM", "set");
        errorCase(cases, wrongArity("sscan"), "SSCAN", "set");
        errorCase(cases, wrongArity("strlen"), "STRLEN");
        errorCase(cases, wrongArity("ttl"), "TTL");
        errorCase(cases, wrongArity("type"), "TYPE");
        errorCase(cases, wrongArity("zadd"), "ZADD", "zset", "1");
        errorCase(cases, wrongArity("zrange"), "ZRANGE", "zset", "0");
        errorCase(cases, wrongArity("zrangebyscore"), "ZRANGEBYSCORE", "zset", "0");
        errorCase(cases, wrongArity("zrem"), "ZREM", "zset");
        errorCase(cases, wrongArity("zremrangebyrank"), "ZREMRANGEBYRANK", "zset", "0");
        errorCase(cases, wrongArity("zremrangebyscore"), "ZREMRANGEBYSCORE", "zset", "0");
        errorCase(cases, wrongArity("zrevrange"), "ZREVRANGE", "zset", "0");
        errorCase(cases, wrongArity("zrevrangebyscore"), "ZREVRANGEBYSCORE", "zset", "0");
        errorCase(cases, wrongArity("zscan"), "ZSCAN", "zset");
        return Collections.unmodifiableList(cases);
    }

    private static void errorCase(List<CommandErrorCase> cases, String expected, String... args) {
        cases.add(new CommandErrorCase(args[0], expected, args));
    }

    private static String wrongArity(String commandLower) {
        return "ERR wrong number of arguments for '" + commandLower + "' command";
    }

    private static void withClient(ClientCase test) {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                test.run(client);
            }
        });
    }

    private static void assertError(CommandErrorCase testCase, ReplyObject reply) {
        Assert.assertTrue(testCase.command() + " expected error reply", reply instanceof ReplyError);
        Assert.assertEquals(testCase.command(), testCase.expected(), ((ReplyError) reply).message());
    }

    private static void assertError(String expected, ReplyObject reply) {
        Assert.assertTrue("expected error reply", reply instanceof ReplyError);
        Assert.assertEquals(expected, ((ReplyError) reply).message());
    }

    private record CommandErrorCase(String command, String expected, String[] args) {
    }

    @FunctionalInterface
    private interface ClientCase {
        void run(FastTestClient client);
    }
}
