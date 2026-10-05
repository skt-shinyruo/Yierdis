package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// 过期时间溢出（issue #126）。期望回复取自 Redis 8.2 src/expire.c expireGenericCommand
// 与 src/t_string.c getExpireMillisecondsOrReply；审计对已存在键的原始回复是
// EXPIRE ovx 9223372036854775 → ERR invalid expire time in 'expire' command。
// 9223372036854775 = Long.MAX_VALUE/1000，乘 1000 仍在 long 内，与当前毫秒相加才溢出。
public class ExpireOverflowTest {
    @Test
    public void expireRejectsAdditionOverflowAndLeavesKeyUnchanged() {
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'expire' command",
                    client.execute(cmd("EXPIRE", "ovx", "9223372036854775"))
            );
            Assert.assertEquals(-1L, integer(client, "TTL", "ovx"));
            Assert.assertEquals("v", bulk(client, "GET", "ovx"));
        });
    }

    @Test
    public void pexpireRejectsAdditionOverflowAndLeavesKeyUnchanged() {
        // Redis 8.2：PEXPIRE 不做秒换算，只在 when > LLONG_MAX - now 时回复
        // ERR invalid expire time in 'pexpire' command。Long.MAX_VALUE 毫秒必然越过当前时间。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'pexpire' command",
                    client.execute(cmd("PEXPIRE", "ovx", "9223372036854775807"))
            );
            Assert.assertEquals(-1L, integer(client, "TTL", "ovx"));
            Assert.assertEquals("v", bulk(client, "GET", "ovx"));
        });
    }

    @Test
    public void expireAtRejectsSecondConversionOverflow() {
        // Redis 8.2：EXPIREAT 的 basetime 为 0，溢出只来自秒×1000。
        // 9223372036854776 超过 Long.MAX_VALUE/1000，原始回复是
        // ERR invalid expire time in 'expireat' command。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'expireat' command",
                    client.execute(cmd("EXPIREAT", "ovx", "9223372036854776"))
            );
            Assert.assertEquals(-1L, integer(client, "TTL", "ovx"));
        });
    }

    @Test
    public void setExRejectsAdditionOverflowWithoutCreatingKey() {
        // Redis 8.2 getExpireMillisecondsOrReply：SET EX 在秒×1000 后加上当前时间，
        // 结果不能表示时回复 ERR invalid expire time in 'set' command，并且不写入键。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            assertError(
                    "ERR invalid expire time in 'set' command",
                    client.execute(cmd("SET", "ovx", "v", "EX", "9223372036854775"))
            );
            // 9223372036854776 在秒×1000 时就已经溢出，同样不写入。
            assertError(
                    "ERR invalid expire time in 'set' command",
                    client.execute(cmd("SET", "ovx", "v", "EX", "9223372036854776"))
            );
            Assert.assertTrue(client.execute(cmd("GET", "ovx")) instanceof ReplyNull);
        });
    }

    @Test
    public void ordinaryRelativeExpireStillApplies() {
        // Redis 8.2：未溢出的 EXPIRE 60 / PEXPIRE 60000 回 :1，TTL 约为所给时长。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "sec", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "sec", "60"));
            long ttl = integer(client, "TTL", "sec");
            Assert.assertTrue("TTL " + ttl, ttl >= 59L && ttl <= 60L);

            Assert.assertEquals("OK", simple(client, "SET", "ms", "v"));
            Assert.assertEquals(1L, integer(client, "PEXPIRE", "ms", "60000"));
            long pttl = integer(client, "PTTL", "ms");
            Assert.assertTrue("PTTL " + pttl, pttl > 59_000L && pttl <= 60_000L);
        });
    }

    @Test
    public void expireRejectsSecondConversionOverflow() {
        // Redis 8.2：9223372036854776 大于 Long.MAX_VALUE/1000，秒×1000 本身溢出，
        // 回复 ERR invalid expire time in 'expire' command。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'expire' command",
                    client.execute(cmd("EXPIRE", "ovx", "9223372036854776"))
            );
            Assert.assertEquals(-1L, integer(client, "TTL", "ovx"));
        });
    }

    @Test
    public void negativeExpireDeletesWhenConversionFitsAndErrorsWhenItDoesNot() {
        // Redis 8.2：-1 与 -9223372036854775（Long.MIN_VALUE/1000）换算成功且落在过去，删除键并回 :1。
        // -9223372036854776 小于该下界，回复 ERR invalid expire time in 'expire' command，键保留。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "gone", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "gone", "-1"));
            Assert.assertEquals(-2L, integer(client, "TTL", "gone"));

            Assert.assertEquals("OK", simple(client, "SET", "past", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIRE", "past", "-9223372036854775"));
            Assert.assertEquals(-2L, integer(client, "TTL", "past"));

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'expire' command",
                    client.execute(cmd("EXPIRE", "ovx", "-9223372036854776"))
            );
            Assert.assertEquals("v", bulk(client, "GET", "ovx"));
        });
    }

    @Test
    public void expireAtAcceptsLargestConvertibleSeconds() {
        // Redis 8.2：9223372036854775 乘 1000 仍在 long 内，EXPIREAT 的 basetime 为 0，因此接受。
        // 这和 EXPIRE 同数字会因加上当前时间而报错不是一回事。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "far", "v"));
            Assert.assertEquals(1L, integer(client, "EXPIREAT", "far", "9223372036854775"));
            long pttl = integer(client, "PTTL", "far");
            Assert.assertTrue("PTTL " + pttl, pttl > 3_000_000_000_000L);
        });
    }

    @Test
    public void pexpireAtAcceptsLongMaxMillis() {
        // Redis 8.2：PEXPIREAT 接收绝对毫秒，basetime 为 0。Long.MAX_VALUE 合法，回 :1。
        // 超出 long 解析范围的 9223372036854775808 仍是整数错误，不是 invalid expire time。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "far", "v"));
            Assert.assertEquals(1L, integer(client, "PEXPIREAT", "far", "9223372036854775807"));
            long pttl = integer(client, "PTTL", "far");
            Assert.assertTrue("PTTL " + pttl, pttl > 3_000_000_000_000L);

            assertError(
                    "ERR value is not an integer or out of range",
                    client.execute(cmd("PEXPIREAT", "far", "9223372036854775808"))
            );
            Assert.assertTrue(integer(client, "PTTL", "far") > 3_000_000_000_000L);
        });
    }

    @Test
    public void setPxRejectsAdditionOverflowAndSetPxAtAcceptsLongMax() {
        // Redis 8.2：SET PX 9223372036854775807 回复 ERR invalid expire time in 'set' command，不改已有值。
        // SET PXAT 9223372036854775807 回 +OK，绝对毫秒 Long.MAX_VALUE 被接受。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "keep", "old"));
            assertError(
                    "ERR invalid expire time in 'set' command",
                    client.execute(cmd("SET", "keep", "new", "PX", "9223372036854775807"))
            );
            Assert.assertEquals("old", bulk(client, "GET", "keep"));

            Assert.assertEquals("OK", simple(client, "SET", "far", "v", "PXAT", "9223372036854775807"));
            Assert.assertEquals("v", bulk(client, "GET", "far"));
            long pttl = integer(client, "PTTL", "far");
            Assert.assertTrue("PTTL " + pttl, pttl > 3_000_000_000_000L);
        });
    }

    @Test
    public void setExAtRejectsSecondConversionOverflow() {
        // Redis 8.2：SET EXAT 与 EXPIREAT 一样只做秒×1000。9223372036854776 回复
        // ERR invalid expire time in 'set' command，不写入键。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            assertError(
                    "ERR invalid expire time in 'set' command",
                    client.execute(cmd("SET", "ovx", "v", "EXAT", "9223372036854776"))
            );
            Assert.assertTrue(client.execute(cmd("GET", "ovx")) instanceof ReplyNull);
        });
    }

    @Test
    public void missingKeyChecksOptionThenIntegerThenOverflowBeforeAbsence() {
        // Redis 8.2 expireGenericCommand 在缺失键上的顺序：非法标志、整数字面量、溢出，最后才是 :0。
        // 原始回复分别是 Unsupported option、not an integer or out of range、
        // invalid expire time in 'expire' command、以及整数 0。溢出和“键不存在”分开断言。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            assertError(
                    "ERR Unsupported option BADFLAG",
                    client.execute(cmd("EXPIRE", "missing", "abc", "BADFLAG"))
            );
            assertError(
                    "ERR value is not an integer or out of range",
                    client.execute(cmd("EXPIRE", "missing", "abc"))
            );
            assertError(
                    "ERR invalid expire time in 'expire' command",
                    client.execute(cmd("EXPIRE", "missing", "9223372036854776"))
            );
            Assert.assertEquals(0L, integer(client, "EXPIRE", "missing", "60"));
        });
    }

    @Test
    public void expireOverflowOnAlreadyExpiredKeyIsAnErrorNotZero() {
        // 已过期键在查键时会被当成不存在。Redis 8.2 仍先报溢出，不回 :0。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            long pastSeconds = System.currentTimeMillis() / 1000L - 60L;
            Assert.assertEquals("OK", simple(client, "SET", "gone", "v", "EXAT", Long.toString(pastSeconds)));
            assertError(
                    "ERR invalid expire time in 'expire' command",
                    client.execute(cmd("EXPIRE", "gone", "9223372036854776"))
            );
        });
    }

    @Test
    public void pexpireNegativeDeletesAndExpireAtKeepsItsOwnNegativeBounds() {
        // Redis 8.2：PEXPIRE 负数不做换算，-1 删除键并回 :1。
        // EXPIREAT -9223372036854776 低于 Long.MIN_VALUE/1000，回复 invalid expire time in 'expireat' command；
        // 范围内的过去秒时间戳则删除。这和 EXPIRE 的相对秒不是同一条时间语义。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "ms", "v"));
            Assert.assertEquals(1L, integer(client, "PEXPIRE", "ms", "-1"));
            Assert.assertEquals(-2L, integer(client, "TTL", "ms"));

            Assert.assertEquals("OK", simple(client, "SET", "past", "v"));
            long pastSeconds = System.currentTimeMillis() / 1000L - 60L;
            Assert.assertEquals(1L, integer(client, "EXPIREAT", "past", Long.toString(pastSeconds)));
            Assert.assertEquals(-2L, integer(client, "TTL", "past"));

            Assert.assertEquals("OK", simple(client, "SET", "ovx", "v"));
            assertError(
                    "ERR invalid expire time in 'expireat' command",
                    client.execute(cmd("EXPIREAT", "ovx", "-9223372036854776"))
            );
            Assert.assertEquals("v", bulk(client, "GET", "ovx"));
        });
    }

    @Test
    public void setExAtAcceptsLargestConvertibleSeconds() {
        // Redis 8.2：SET EXAT 9223372036854775 乘 1000 仍在 long 内，回 +OK。
        // 同数字的 SET EX 会因为再加上当前时间而报错。
        forEachDb(db -> {
            FastTestClient client = newClient(db);

            Assert.assertEquals("OK", simple(client, "SET", "far", "v", "EXAT", "9223372036854775"));
            Assert.assertEquals("v", bulk(client, "GET", "far"));
            long pttl = integer(client, "PTTL", "far");
            Assert.assertTrue("PTTL " + pttl, pttl > 3_000_000_000_000L);
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

    private static String bulk(FastTestClient client, String... args) {
        return ((ReplyBulkString) client.execute(cmd(args))).asString();
    }

    private static void assertError(String expectedMessage, ReplyObject reply) {
        Assert.assertTrue("expected error but got " + reply, reply instanceof ReplyError);
        Assert.assertEquals(expectedMessage, ((ReplyError) reply).message());
    }
}
