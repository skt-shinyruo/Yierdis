package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ZSetCommandTest {
    @Test
    public void zsetCommandsUseReadWriteBoundariesInsteadOfLegacyValueOps() throws IOException {
        String source = CommandSourceFiles.readCommandDefaults("ZSetCommands.java");

        Assert.assertFalse(source.contains("eviction().prepareWrite("));
        Assert.assertFalse(source.contains("values().zsets()."));
    }

    @Test
    public void zaddRejectsInvalidScores() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            FastTestClient client = new FastTestClient(dispatcher);
            try {
                byte[] key = b("z");

                ReplyObject err1 = client.execute(Arrays.asList(b("ZADD"), key, b("NaN"), b("a")));
                Assert.assertTrue(err1 instanceof ReplyError);
                Assert.assertEquals("ERR value is not a valid float", ((ReplyError) err1).message());

                ReplyObject err2 = client.execute(Arrays.asList(b("ZADD"), key, b("Infinity"), b("a")));
                Assert.assertTrue(err2 instanceof ReplyError);
                Assert.assertEquals("ERR value is not a valid float", ((ReplyError) err2).message());

                ReplyObject err3 = client.execute(Arrays.asList(b("ZADD"), key, b("nope"), b("a")));
                Assert.assertTrue(err3 instanceof ReplyError);
                Assert.assertEquals("ERR value is not a valid float", ((ReplyError) err3).message());
            } finally {
                client.close();
            }
        });
    }

    @Test
    public void zrevrangeParsesRanksBeforeOptions() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyError error = (ReplyError) client.execute(
                        List.of(b("ZREVRANGE"), b("z"), b("bad"), b("-1"), b("UNKNOWN")));

                Assert.assertEquals("ERR value is not an integer or out of range", error.message());
            }
        });
    }

    @Test
    public void oversizedRangePreparationRemainsACommandError() {
        CommandDispatcher dispatcher = oversizedRangeDispatcher();
        try (FastTestClient client = new FastTestClient(dispatcher)) {
            for (List<byte[]> command : List.of(
                    List.of(b("ZRANGE"), b("z"), b("0"), b("-1"), b("WITHSCORES")),
                    List.of(b("ZRANGEBYSCORE"), b("z"), b("-inf"), b("+inf"), b("WITHSCORES"))
            )) {
                ReplyError error = (ReplyError) client.execute(command);
                Assert.assertEquals("ERR response is too large", error.message());
            }

            Assert.assertEquals("PONG", ((ReplySimpleString) client.execute(List.of(b("PING")))).value());
        }
    }

    @Test
    public void zrangeTieBreakIsRawByteLexAndBoundsWork() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = new byte[]{'z', 0};

        byte[] m1 = new byte[]{0};
        byte[] m2 = new byte[]{0, 0};
        byte[] m3 = new byte[]{0, 1};
        byte[] m4 = new byte[]{(byte) 0xFF};

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                b("ZADD"),
                key,
                b("1"), m4,
                b("1"), m3,
                b("1"), m2,
                b("1"), m1
        ));
        Assert.assertEquals(4, added.value());

        ReplyArray all = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(4, all.values().size());
        Assert.assertArrayEquals(m1, ((ReplyBulkString) all.values().get(0)).data());
        Assert.assertArrayEquals(m2, ((ReplyBulkString) all.values().get(1)).data());
        Assert.assertArrayEquals(m3, ((ReplyBulkString) all.values().get(2)).data());
        Assert.assertArrayEquals(m4, ((ReplyBulkString) all.values().get(3)).data());

        ReplyArray withScores = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("1"), b("WITHSCORES")));
        Assert.assertEquals(4, withScores.values().size());
        Assert.assertArrayEquals(m1, ((ReplyBulkString) withScores.values().get(0)).data());
        Assert.assertEquals("1", ((ReplyBulkString) withScores.values().get(1)).asString());
        Assert.assertArrayEquals(m2, ((ReplyBulkString) withScores.values().get(2)).data());
        Assert.assertEquals("1", ((ReplyBulkString) withScores.values().get(3)).asString());

        ReplyArray startTooLarge = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("10"), b("20")));
        Assert.assertTrue(startTooLarge.values().isEmpty());

        ReplyArray stopHuge = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("9223372036854775807")));
        Assert.assertEquals(4, stopHuge.values().size());

        ReplyArray startHugeNegative = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("-9223372036854775808"), b("-1")));
        Assert.assertEquals(4, startHugeNegative.values().size());

        ReplyArray startAfterStop = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("2"), b("1")));
        Assert.assertTrue(startAfterStop.values().isEmpty());

            }
        });
    }

    @Test
    public void zrevrangeAndZrangeRevReturnReverseOrder() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrev");

        byte[] m1 = new byte[]{0};
        byte[] m2 = new byte[]{0, 0};
        byte[] m3 = new byte[]{0, 1};
        byte[] m4 = new byte[]{(byte) 0xFF};

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                b("ZADD"),
                key,
                b("1"), m4,
                b("1"), m3,
                b("1"), m2,
                b("1"), m1
        ));
        Assert.assertEquals(4, added.value());

        ReplyArray rev = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(4, rev.values().size());
        Assert.assertArrayEquals(m4, ((ReplyBulkString) rev.values().get(0)).data());
        Assert.assertArrayEquals(m3, ((ReplyBulkString) rev.values().get(1)).data());
        Assert.assertArrayEquals(m2, ((ReplyBulkString) rev.values().get(2)).data());
        Assert.assertArrayEquals(m1, ((ReplyBulkString) rev.values().get(3)).data());

        ReplyArray revViaZrange = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1"), b("REV")));
        Assert.assertEquals(4, revViaZrange.values().size());
        Assert.assertArrayEquals(m4, ((ReplyBulkString) revViaZrange.values().get(0)).data());
        Assert.assertArrayEquals(m3, ((ReplyBulkString) revViaZrange.values().get(1)).data());
        Assert.assertArrayEquals(m2, ((ReplyBulkString) revViaZrange.values().get(2)).data());
        Assert.assertArrayEquals(m1, ((ReplyBulkString) revViaZrange.values().get(3)).data());

        ReplyArray revWithScores = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("1"), b("WITHSCORES")));
        Assert.assertEquals(4, revWithScores.values().size());
        Assert.assertArrayEquals(m4, ((ReplyBulkString) revWithScores.values().get(0)).data());
        Assert.assertEquals("1", ((ReplyBulkString) revWithScores.values().get(1)).asString());
        Assert.assertArrayEquals(m3, ((ReplyBulkString) revWithScores.values().get(2)).data());
        Assert.assertEquals("1", ((ReplyBulkString) revWithScores.values().get(3)).asString());

            }
        });
    }

    @Test
    public void zremDeletesKeyWhenEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = new byte[]{0, 'z'};
        byte[] member = new byte[]{0, 1, 2};

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("ZADD"), key, b("1"), member));
        Assert.assertEquals(1, added.value());

        ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("ZREM"), key, member));
        Assert.assertEquals(1, removed.value());

        ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

        ReplySimpleString type = (ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key));
        Assert.assertEquals("none", type.value());
            }

        });
    }

    @Test
    public void zsetUpgradesAfterManyElementsAndKeepsOrder() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("big-zset");
        int n = 129; // > ZSetValue.LISTPACK_MAX_ENTRIES

        ArrayList<byte[]> args = new ArrayList<>(2 + n * 2);
        args.add(b("ZADD"));
        args.add(key);
        for (int i = 0; i < n; i++) {
            args.add(b(Integer.toString(i))); // score
            args.add(b(String.format("m%03d", i))); // member
        }

        ReplyInteger added = (ReplyInteger) client.execute(args);
        Assert.assertEquals(n, added.value());

        ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(n, range.values().size());
        Assert.assertEquals("m000", ((ReplyBulkString) range.values().get(0)).asString());
        Assert.assertEquals("m128", ((ReplyBulkString) range.values().get(n - 1)).asString());

        ReplyArray rev = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGE"), key, b("0"), b("1")));
        Assert.assertEquals(2, rev.values().size());
        Assert.assertEquals("m128", ((ReplyBulkString) rev.values().get(0)).asString());
        Assert.assertEquals("m127", ((ReplyBulkString) rev.values().get(1)).asString());

            }
        });
    }

    @Test
    public void zsetUpgradesWhenMemberIsTooLargeForListpack() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zset:big-member");
        byte[] small = b("a");
        byte[] big = new byte[65];
        Arrays.fill(big, (byte) 'x');

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), big,
                b("0"), small
        ));
        Assert.assertEquals(2, added.value());

        ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(2, range.values().size());
        Assert.assertArrayEquals(small, ((ReplyBulkString) range.values().get(0)).data());
        Assert.assertArrayEquals(big, ((ReplyBulkString) range.values().get(1)).data());

            }
        });
    }

    @Test
    public void zrangeByScoreRespectsBoundsLimitAndWithScores() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zbyscore");
        client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), b("a"),
                b("2"), b("b"),
                b("2"), b("c"),
                b("3"), b("d")
        ));

        ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("2"), b("3")));
        Assert.assertEquals(3, range.values().size());
        Assert.assertEquals("b", ((ReplyBulkString) range.values().get(0)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) range.values().get(1)).asString());
        Assert.assertEquals("d", ((ReplyBulkString) range.values().get(2)).asString());

        ReplyArray exMin = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("(2"), b("3")));
        Assert.assertEquals(1, exMin.values().size());
        Assert.assertEquals("d", ((ReplyBulkString) exMin.values().get(0)).asString());

        ReplyArray exMax = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("2"), b("(3")));
        Assert.assertEquals(2, exMax.values().size());
        Assert.assertEquals("b", ((ReplyBulkString) exMax.values().get(0)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) exMax.values().get(1)).asString());

        ReplyArray limit = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("2"), b("3"), b("LIMIT"), b("1"), b("1")));
        Assert.assertEquals(1, limit.values().size());
        Assert.assertEquals("c", ((ReplyBulkString) limit.values().get(0)).asString());

        ReplyArray withScores = (ReplyArray) client.execute(Arrays.asList(
                b("ZRANGEBYSCORE"), key, b("-inf"), b("+inf"),
                b("WITHSCORES"), b("LIMIT"), b("0"), b("2")
        ));
        Assert.assertEquals(4, withScores.values().size());
        Assert.assertEquals("a", ((ReplyBulkString) withScores.values().get(0)).asString());
        Assert.assertEquals("1", ((ReplyBulkString) withScores.values().get(1)).asString());
        Assert.assertEquals("b", ((ReplyBulkString) withScores.values().get(2)).asString());
        Assert.assertEquals("2", ((ReplyBulkString) withScores.values().get(3)).asString());

        ReplyArray emptyWhenMinGreaterThanMax = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("3"), b("2")));
        Assert.assertEquals(0, emptyWhenMinGreaterThanMax.values().size());

        ReplyArray emptyWhenCountZero = (ReplyArray) client.execute(Arrays.asList(
                b("ZRANGEBYSCORE"), key, b("-inf"), b("+inf"),
                b("LIMIT"), b("0"), b("0")
        ));
        Assert.assertEquals(0, emptyWhenCountZero.values().size());

            }
        });
    }

    @Test
    public void zremrangeByScoreRemovesAndDeletesKeyWhenEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrembyscore");
        client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), b("a"),
                b("2"), b("b"),
                b("3"), b("c")
        ));

        ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYSCORE"), key, b("2"), b("3")));
        Assert.assertEquals(2, removed.value());

        ReplyArray remaining = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(1, remaining.values().size());
        Assert.assertEquals("a", ((ReplyBulkString) remaining.values().get(0)).asString());

        ReplyInteger removedAll = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYSCORE"), key, b("-inf"), b("+inf")));
        Assert.assertEquals(1, removedAll.value());

        ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

            }
        });
    }

    @Test
    public void zrangeByScoreWorksAfterUpgradeToSkiplist() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zbyscore:upgrade");
        byte[] big = new byte[65];
        Arrays.fill(big, (byte) 'x');

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), big,
                b("0"), b("a")
        ));
        Assert.assertEquals(2, added.value());

        ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZRANGEBYSCORE"), key, b("0"), b("1")));
        Assert.assertEquals(2, range.values().size());
        Assert.assertEquals("a", ((ReplyBulkString) range.values().get(0)).asString());
        Assert.assertArrayEquals(big, ((ReplyBulkString) range.values().get(1)).data());

            }
        });
    }

    @Test
    public void zrevrangeByScoreRespectsBoundsLimitAndWithScores() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrevbyscore");
        client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), b("a"),
                b("2"), b("b"),
                b("2"), b("c"),
                b("3"), b("d")
        ));

        ReplyArray range = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("3"), b("2")));
        Assert.assertEquals(3, range.values().size());
        Assert.assertEquals("d", ((ReplyBulkString) range.values().get(0)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) range.values().get(1)).asString());
        Assert.assertEquals("b", ((ReplyBulkString) range.values().get(2)).asString());

        ReplyArray exMax = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("(3"), b("2")));
        Assert.assertEquals(2, exMax.values().size());
        Assert.assertEquals("c", ((ReplyBulkString) exMax.values().get(0)).asString());
        Assert.assertEquals("b", ((ReplyBulkString) exMax.values().get(1)).asString());

        ReplyArray limit = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("3"), b("2"), b("LIMIT"), b("1"), b("1")));
        Assert.assertEquals(1, limit.values().size());
        Assert.assertEquals("c", ((ReplyBulkString) limit.values().get(0)).asString());

        ReplyArray withScores = (ReplyArray) client.execute(Arrays.asList(
                b("ZREVRANGEBYSCORE"), key, b("+inf"), b("-inf"),
                b("WITHSCORES"), b("LIMIT"), b("0"), b("2")
        ));
        Assert.assertEquals(4, withScores.values().size());
        Assert.assertEquals("d", ((ReplyBulkString) withScores.values().get(0)).asString());
        Assert.assertEquals("3", ((ReplyBulkString) withScores.values().get(1)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) withScores.values().get(2)).asString());
        Assert.assertEquals("2", ((ReplyBulkString) withScores.values().get(3)).asString());

        ReplyArray exMin = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("3"), b("(2")));
        Assert.assertEquals(1, exMin.values().size());
        Assert.assertEquals("d", ((ReplyBulkString) exMin.values().get(0)).asString());

        ReplyArray emptyWhenMaxLessThanMin = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("1"), b("2")));
        Assert.assertEquals(0, emptyWhenMaxLessThanMin.values().size());

        ReplyArray offsetPastEnd = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("3"), b("2"), b("LIMIT"), b("10"), b("1")));
        Assert.assertEquals(0, offsetPastEnd.values().size());

            }
        });
    }

    @Test
    public void scoreRangeLimitNegativeCountReturnsAllRemainingMembers() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrangebyscore:unbounded-limit");
        client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), b("a"),
                b("2"), b("b"),
                b("2"), b("c"),
                b("3"), b("d")
        ));

        ReplyArray forward = (ReplyArray) client.execute(Arrays.asList(
                b("ZRANGEBYSCORE"), key, b("-inf"), b("+inf"), b("LIMIT"), b("1"), b("-1")
        ));
        Assert.assertEquals(3, forward.values().size());
        Assert.assertEquals("b", ((ReplyBulkString) forward.values().get(0)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) forward.values().get(1)).asString());
        Assert.assertEquals("d", ((ReplyBulkString) forward.values().get(2)).asString());

        ReplyArray otherNegativeForward = (ReplyArray) client.execute(Arrays.asList(
                b("ZRANGEBYSCORE"), key, b("-inf"), b("+inf"), b("LIMIT"), b("1"), b("-2")
        ));
        Assert.assertEquals(3, otherNegativeForward.values().size());
        Assert.assertEquals("b", ((ReplyBulkString) otherNegativeForward.values().get(0)).asString());
        Assert.assertEquals("c", ((ReplyBulkString) otherNegativeForward.values().get(1)).asString());
        Assert.assertEquals("d", ((ReplyBulkString) otherNegativeForward.values().get(2)).asString());

        ReplyArray reverse = (ReplyArray) client.execute(Arrays.asList(
                b("ZREVRANGEBYSCORE"), key, b("+inf"), b("-inf"), b("LIMIT"), b("1"), b("-1")
        ));
        Assert.assertEquals(3, reverse.values().size());
        Assert.assertEquals("c", ((ReplyBulkString) reverse.values().get(0)).asString());
        Assert.assertEquals("b", ((ReplyBulkString) reverse.values().get(1)).asString());
        Assert.assertEquals("a", ((ReplyBulkString) reverse.values().get(2)).asString());

            }
        });
    }

    private static CommandDispatcher oversizedRangeDispatcher() {
        ZSetReadOps zsets = interfaceProxy(ZSetReadOps.class, (proxy, method, args) -> {
            throw new IllegalArgumentException("response is too large");
        });
        DbReads reads = interfaceProxy(DbReads.class, (proxy, method, args) -> {
            if ("zsets".equals(method.getName())) {
                return zsets;
            }
            throw new AssertionError("unexpected DB read: " + method.getName());
        });
        DbEngine engine = interfaceProxy(DbEngine.class, (proxy, method, args) -> {
            if ("reads".equals(method.getName())) {
                return reads;
            }
            throw new AssertionError("unexpected DB access: " + method.getName());
        });
        return CommandRegistries.dispatcher(DefaultCommandModules.create(engine));
    }

    private static <T> T interfaceProxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                handler));
    }

    @Test
    public void zremrangeByRankRemovesAndDeletesKeyWhenEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrembyrank");
        client.execute(Arrays.asList(
                b("ZADD"), key,
                b("1"), b("a"),
                b("2"), b("b"),
                b("3"), b("c"),
                b("4"), b("d")
        ));

        ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYRANK"), key, b("1"), b("2")));
        Assert.assertEquals(2, removed.value());

        ReplyArray remaining = (ReplyArray) client.execute(Arrays.asList(b("ZRANGE"), key, b("0"), b("-1")));
        Assert.assertEquals(2, remaining.values().size());
        Assert.assertEquals("a", ((ReplyBulkString) remaining.values().get(0)).asString());
        Assert.assertEquals("d", ((ReplyBulkString) remaining.values().get(1)).asString());

        ReplyInteger removedLast = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYRANK"), key, b("-1"), b("-1")));
        Assert.assertEquals(1, removedLast.value());

        ReplyInteger removedAll = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYRANK"), key, b("0"), b("-1")));
        Assert.assertEquals(1, removedAll.value());

        ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

            }
        });
    }

    @Test
    public void zrevrangeByScoreAndZremrangeByRankWorkAfterUpgradeToSkiplist() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {

        byte[] key = b("zrange:upgrade2");
        byte[] big = new byte[65];
        Arrays.fill(big, (byte) 'x');

        ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                b("ZADD"), key,
                b("0"), b("a"),
                b("1"), big
        ));
        Assert.assertEquals(2, added.value());

        ReplyArray rev = (ReplyArray) client.execute(Arrays.asList(b("ZREVRANGEBYSCORE"), key, b("1"), b("0")));
        Assert.assertEquals(2, rev.values().size());
        Assert.assertArrayEquals(big, ((ReplyBulkString) rev.values().get(0)).data());
        Assert.assertEquals("a", ((ReplyBulkString) rev.values().get(1)).asString());

        ReplyInteger removedAll = (ReplyInteger) client.execute(Arrays.asList(b("ZREMRANGEBYRANK"), key, b("0"), b("-1")));
        Assert.assertEquals(2, removedAll.value());

        ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
        Assert.assertEquals(0, exists.value());

            }
        });
    }
}
