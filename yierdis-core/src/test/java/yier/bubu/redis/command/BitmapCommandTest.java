package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class BitmapCommandTest {
    @Test
    public void getbitSetbitBasicSemantics() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("k");

                RespInteger miss = (RespInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("0")));
                Assert.assertEquals(0, miss.value());

                RespInteger old0 = (RespInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1")));
                Assert.assertEquals(0, old0.value());

                RespInteger now1 = (RespInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("0")));
                Assert.assertEquals(1, now1.value());

                // offset=7 对应第 1 个字节的最低位
                RespInteger old7 = (RespInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("7"), b("1")));
                Assert.assertEquals(0, old7.value());

                RespInteger get7 = (RespInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("7")));
                Assert.assertEquals(1, get7.value());
            }
        });
    }

    @Test
    public void bitcountRangeFollowsRedisByteRangeRules() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("k");

                client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1")));
                client.execute(Arrays.asList(b("SETBIT"), key, b("15"), b("1")));

                RespInteger all = (RespInteger) client.execute(Arrays.asList(b("BITCOUNT"), key));
                Assert.assertEquals(2, all.value());

                RespInteger b0 = (RespInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("0"), b("0")));
                Assert.assertEquals(1, b0.value());

                RespInteger b1 = (RespInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("1"), b("1")));
                Assert.assertEquals(1, b1.value());

                RespInteger last = (RespInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("-1"), b("-1")));
                Assert.assertEquals(1, last.value());
            }
        });
    }

    @Test
    public void setbitZeroFillsGrownBytesWithinCapacity() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("k");

                client.execute(cmd("SET", "k", "a"));
                client.execute(cmd("APPEND", "k", "b"));
                client.execute(cmd("SET", "k", "a"));

                RespInteger old = (RespInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("8"), b("0")));
                Assert.assertEquals(0, old.value());

                RespInteger count = (RespInteger) client.execute(Arrays.asList(b("BITCOUNT"), key));
                Assert.assertEquals(Integer.bitCount('a'), count.value());
            }
        });
    }

    @Test
    public void bitmapCommandsErrorOnWrongType() {
        forEachDb(db -> {
            // 复用现有 list 命令制造非 string 类型
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(Arrays.asList(b("LPUSH"), b("k"), b("x")));

                RespObject err = client.execute(Arrays.asList(b("GETBIT"), b("k"), b("0")));
                Assert.assertTrue(err instanceof RespError);
                Assert.assertEquals(new YierdisDb.WrongTypeException().getMessage(), ((RespError) err).message());
            }
        });
    }
}

