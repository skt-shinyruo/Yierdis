package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class HllCommandTest {
    @Test
    public void pfaddCreatesAndUpdates() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                RespInteger add1 = (RespInteger) client.execute(cmd("PFADD", "h", "a"));
                Assert.assertEquals(1, add1.value());

                RespInteger add2 = (RespInteger) client.execute(cmd("PFADD", "h", "a"));
                Assert.assertEquals(0, add2.value());

                RespInteger count = (RespInteger) client.execute(cmd("PFCOUNT", "h"));
                Assert.assertEquals(1, count.value());
            }
        });
    }

    @Test
    public void pfcountAndPfmergeWorkOnUnion() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("PFADD", "h1", "foo", "bar"));
                client.execute(cmd("PFADD", "h2", "bar", "baz"));

                RespInteger c1 = (RespInteger) client.execute(cmd("PFCOUNT", "h1"));
                Assert.assertEquals(2, c1.value());

                RespInteger c2 = (RespInteger) client.execute(cmd("PFCOUNT", "h2"));
                Assert.assertEquals(2, c2.value());

                RespInteger cu = (RespInteger) client.execute(cmd("PFCOUNT", "h1", "h2"));
                Assert.assertEquals(3, cu.value());

                RespObject ok = client.execute(cmd("PFMERGE", "hu", "h1", "h2"));
                Assert.assertTrue(ok instanceof RespSimpleString);

                RespInteger merged = (RespInteger) client.execute(cmd("PFCOUNT", "hu"));
                Assert.assertEquals(3, merged.value());
            }
        });
    }

    @Test
    public void denseHllSupportsInPlacePfaddAfterPfmerge() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("PFADD", "src", "a", "b"));

                // PFMERGE 总是写 dense，这样后续 PFADD 会走 dense 原地更新分支。
                client.execute(cmd("PFMERGE", "dense", "src"));

                RespInteger add = (RespInteger) client.execute(cmd("PFADD", "dense", "c"));
                Assert.assertEquals(1, add.value());

                RespInteger count = (RespInteger) client.execute(cmd("PFCOUNT", "dense"));
                Assert.assertEquals(3, count.value());
            }
        });
    }

    @Test
    public void pfaddErrorsOnNonHllString() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "k", "v"));

                RespObject err = client.execute(Arrays.asList(b("PFADD"), b("k"), b("x")));
                Assert.assertTrue(err instanceof RespError);
                Assert.assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", ((RespError) err).message());
            }
        });
    }
}

