package yier.bubu.redis.integration.command;

import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyObject;

import static yier.bubu.redis.testutil.ReplyAssertions.assertArraySize;
import static yier.bubu.redis.testutil.ReplyAssertions.assertErrorContaining;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.ReplyAssertions.assertNull;
import static yier.bubu.redis.testutil.ReplyAssertions.assertSimpleString;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// 命令整数参数必须与已存 string 的 INCR 解析使用同一 Redis string2ll 方言：
// 拒绝 '+' 前缀与前导零等非规范写法，错误文案为 Redis 风格的整数错误。
public class IntegerDialectCommandTest {
    private static final String INTEGER_ERROR = "not an integer or out of range";

    @Test
    public void setExpiryRejectsNonCanonicalIntegers() {
        withClient(client -> {
            assertIntegerError(client.execute(cmd("SET", "k", "v", "EX", "+60")));
            assertIntegerError(client.execute(cmd("SET", "k", "v", "EX", "007")));
            assertIntegerError(client.execute(cmd("SET", "k", "v", "PX", "+60000")));
            assertIntegerError(client.execute(cmd("SET", "k", "v", "PX", "060000")));
            assertIntegerError(client.execute(cmd("SET", "k", "v", "EX", "-0")));
            assertNull(client.execute(cmd("GET", "k")));

            assertSimpleString("OK", client.execute(cmd("SET", "k", "v", "EX", "60")));
        });
    }

    @Test
    public void expireRejectsNonCanonicalIntegers() {
        withClient(client -> {
            assertSimpleString("OK", client.execute(cmd("SET", "k", "v")));

            assertIntegerError(client.execute(cmd("EXPIRE", "k", "+60")));
            assertIntegerError(client.execute(cmd("EXPIRE", "k", "060")));
            assertInteger(1, client.execute(cmd("EXPIRE", "k", "60")));
        });
    }

    @Test
    public void rangeCommandsRejectNonCanonicalIntegersButAcceptNegatives() {
        withClient(client -> {
            assertInteger(2, client.execute(cmd("RPUSH", "l", "a", "b")));
            assertInteger(2, client.execute(cmd("ZADD", "z", "1", "a", "2", "b")));

            assertIntegerError(client.execute(cmd("LRANGE", "l", "+0", "-1")));
            assertIntegerError(client.execute(cmd("LRANGE", "l", "00", "-1")));
            assertIntegerError(client.execute(cmd("ZRANGE", "z", "+0", "-1")));
            assertIntegerError(client.execute(cmd("ZRANGE", "z", "0", "007")));
            assertIntegerError(client.execute(cmd("ZRANGE", "z", "-0", "-1")));

            assertArraySize(2, client.execute(cmd("LRANGE", "l", "0", "-1")));
            assertArraySize(2, client.execute(cmd("ZRANGE", "z", "0", "-1")));
        });
    }

    @Test
    public void scanCountRejectsNonCanonicalIntegers() {
        withClient(client -> {
            assertIntegerError(client.execute(cmd("SCAN", "0", "COUNT", "+10")));
            assertIntegerError(client.execute(cmd("SCAN", "0", "COUNT", "010")));

            assertArraySize(2, client.execute(cmd("SCAN", "0", "COUNT", "10")));
        });
    }

    private static void assertIntegerError(ReplyObject reply) {
        assertErrorContaining(INTEGER_ERROR, reply);
    }

    private static void withClient(ClientCase test) {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                test.run(client);
            }
        });
    }

    @FunctionalInterface
    private interface ClientCase {
        void run(FastTestClient client);
    }
}
