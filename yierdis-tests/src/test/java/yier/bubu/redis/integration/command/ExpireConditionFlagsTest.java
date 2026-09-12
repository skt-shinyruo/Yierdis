package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// EXPIRE 系列的 NX/XX/GT/LT 条件标志（Redis 7.0+，issue #81）：期望值取自 reference/redis
// 的 expireGenericCommand 语义与 issue 中对真实 Redis 8.2 的差分结果；无 TTL 视为无限 TTL，
// 条件不满足返回 :0 且键与旧 TTL 保留。
public class ExpireConditionFlagsTest {
    @Test
    public void issueReproMatrixRelativeSequence() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "100"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e", "200", "NX"));
            Assert.assertEquals(100L, integer(client, "TTL", "e"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "200", "XX"));
            Assert.assertEquals(200L, integer(client, "TTL", "e"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "300", "GT"));
            Assert.assertEquals(300L, integer(client, "TTL", "e"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "250", "LT"));
            Assert.assertEquals(250L, integer(client, "TTL", "e"));

            Assert.assertEquals("OK", simple(client, "SET", "e2", "v"));
            // 无 TTL 视为无限 TTL：GT 必然失败，LT 必然成功。
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e2", "100", "GT"));
            Assert.assertEquals(-1L, integer(client, "TTL", "e2"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e2", "100", "LT"));
            Assert.assertEquals(100L, integer(client, "TTL", "e2"));
        });
    }

    @Test
    public void issueReproMatrixExpireAtSequence() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e5", "v"));
            Assert.assertEquals(1L, integer(client, "PEXPIREAT", "e5", "99999999999999", "NX"));
            // XX 先把过期时间改写为 9999999999 秒；随后 GT/LT 面对相等的新值都失败。
            Assert.assertEquals(1L, integer(client, "EXPIREAT", "e5", "9999999999", "XX"));
            Assert.assertEquals(0L, integer(client, "EXPIREAT", "e5", "9999999999", "GT"));
            Assert.assertEquals(0L, integer(client, "EXPIREAT", "e5", "9999999999", "LT"));
        });
    }

    @Test
    public void gtRejectsSmallerAndLtRejectsLargerExpiry() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "300"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e", "250", "GT"));
            Assert.assertEquals(300L, integer(client, "TTL", "e"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e", "400", "LT"));
            Assert.assertEquals(300L, integer(client, "TTL", "e"));
            // PEXPIRE 走同一组条件语义。
            Assert.assertEquals(1L, integer(client, "PEXPIRE", "e", "250000", "XX"));
            Assert.assertEquals(0L, integer(client, "PEXPIRE", "e", "200000", "GT"));
            Assert.assertEquals(250L, integer(client, "TTL", "e"));
        });
    }

    @Test
    public void conditionsRejectMissingKeys() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            for (String flag : new String[]{"NX", "XX", "GT", "LT"}) {
                Assert.assertEquals(0L, integer(client, "EXPIRE", "missing", "100", flag));
                Assert.assertEquals(0L, integer(client, "PEXPIREAT", "missing", "99999999999999", flag));
            }
        });
    }

    @Test
    public void failingConditionKeepsKeyWhenNewExpiryIsInPast() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "100"));
            // 过去的新时间 <= 当前 TTL，GT 失败：键保留、TTL 不变。
            Assert.assertEquals(0L, integer(client, "PEXPIREAT", "e", "1", "GT"));
            Assert.assertEquals(1L, integer(client, "EXISTS", "e"));
            Assert.assertEquals(100L, integer(client, "TTL", "e"));
            // 过去的新时间必然小于现有 TTL，LT 通过后才删除。
            Assert.assertEquals(1L, integer(client, "PEXPIREAT", "e", "1", "LT"));
            Assert.assertEquals(0L, integer(client, "EXISTS", "e"));

            Assert.assertEquals("OK", simple(client, "SET", "e2", "v"));
            Assert.assertEquals(0L, integer(client, "PEXPIREAT", "e2", "1", "XX"));
            Assert.assertEquals(1L, integer(client, "EXISTS", "e2"));
            Assert.assertEquals(1L, integer(client, "PEXPIREAT", "e2", "1", "NX"));
            Assert.assertEquals(0L, integer(client, "EXISTS", "e2"));
        });
    }

    @Test
    public void unknownOptionRejectedWithVerbatimFlag() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            assertError("ERR Unsupported option FOO", client.execute(cmd("EXPIRE", "e", "100", "FOO")));
            assertError("ERR Unsupported option foo", client.execute(cmd("EXPIRE", "e", "100", "foo")));
            assertError("ERR Unsupported option BOGUS", client.execute(cmd("PEXPIREAT", "e", "1", "BOGUS")));
        });
    }

    @Test
    public void incompatibleFlagCombinationsRejected() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            assertError("ERR NX and XX, GT or LT options at the same time are not compatible",
                    client.execute(cmd("EXPIRE", "e", "100", "NX", "XX")));
            assertError("ERR NX and XX, GT or LT options at the same time are not compatible",
                    client.execute(cmd("EXPIRE", "e", "100", "GT", "NX")));
            assertError("ERR NX and XX, GT or LT options at the same time are not compatible",
                    client.execute(cmd("EXPIRE", "e", "100", "NX", "LT")));
            assertError("ERR GT and LT options at the same time are not compatible",
                    client.execute(cmd("EXPIRE", "e", "100", "GT", "LT")));
            assertError("ERR GT and LT options at the same time are not compatible",
                    client.execute(cmd("EXPIRE", "e", "100", "LT", "GT")));
        });
    }

    @Test
    public void repeatedAndCombinableFlagsAccepted() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            // 相同标志重复出现等价于只出现一次。
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "100", "NX", "NX"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e", "200", "XX", "XX"));

            // Redis 允许 XX 与 GT/LT 组合成交集条件。
            Assert.assertEquals("OK", simple(client, "SET", "e2", "v"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e2", "100", "XX", "GT"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e2", "100"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e2", "50", "XX", "GT"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "e2", "200", "XX", "GT"));
            Assert.assertEquals(200L, integer(client, "TTL", "e2"));

            // 标志大小写不敏感。
            Assert.assertEquals(1L, integer(client, "expire", "e2", "300", "gt"));
            Assert.assertEquals(0L, integer(client, "EXPIRE", "e2", "400", "Lt"));
            Assert.assertEquals(300L, integer(client, "TTL", "e2"));
        });
    }

    @Test
    public void flagErrorsPrecedeIntegerParsing() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "e", "v"));
            // Redis 先解析标志再解析整数：非法标志优先于整数错误。
            assertError("ERR Unsupported option BADFLAG", client.execute(cmd("EXPIRE", "e", "abc", "BADFLAG")));
            assertError("ERR value is not an integer or out of range",
                    client.execute(cmd("EXPIRE", "e", "abc", "NX")));
        });
    }

    private static FastTestClient newClient(YierdisDb db) {
        CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
        return new FastTestClient(dispatcher);
    }

    private static long integer(FastTestClient client, String... args) {
        return ((ReplyInteger) client.execute(cmd(args))).value();
    }

    private static String simple(FastTestClient client, String... args) {
        return ((ReplySimpleString) client.execute(cmd(args))).value();
    }

    private static void assertError(String expectedMessage, ReplyObject reply) {
        Assert.assertTrue("expected error but got " + reply, reply instanceof ReplyError);
        Assert.assertEquals(expectedMessage, ((ReplyError) reply).message());
    }
}
