package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class HllCommandTest {
    private static final long DENSE_HLL_PHYSICAL_MAXMEMORY_BYTES = 700_000L;

    @Test
    public void pfaddCreatesAndUpdates() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyInteger add1 = (ReplyInteger) client.execute(cmd("PFADD", "h", "a"));
                Assert.assertEquals(1, add1.value());

                ReplyInteger add2 = (ReplyInteger) client.execute(cmd("PFADD", "h", "a"));
                Assert.assertEquals(0, add2.value());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "h"));
                Assert.assertEquals(1, count.value());
            }
        });
    }

    @Test
    public void pfcountAndPfmergeWorkOnUnion() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("PFADD", "h1", "foo", "bar"));
                client.execute(cmd("PFADD", "h2", "bar", "baz"));

                ReplyInteger c1 = (ReplyInteger) client.execute(cmd("PFCOUNT", "h1"));
                Assert.assertEquals(2, c1.value());

                ReplyInteger c2 = (ReplyInteger) client.execute(cmd("PFCOUNT", "h2"));
                Assert.assertEquals(2, c2.value());

                ReplyInteger cu = (ReplyInteger) client.execute(cmd("PFCOUNT", "h1", "h2"));
                Assert.assertEquals(3, cu.value());

                ReplyObject ok = client.execute(cmd("PFMERGE", "hu", "h1", "h2"));
                Assert.assertTrue(ok instanceof ReplySimpleString);

                ReplyInteger merged = (ReplyInteger) client.execute(cmd("PFCOUNT", "hu"));
                Assert.assertEquals(3, merged.value());
            }
        });
    }

    @Test
    public void denseHllSupportsInPlacePfaddAfterPfmergeUnderFfmStorage() {
        YierdisDb db = openFfm(0L);
        try {
            db.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("PFADD", "src", "a", "b"));

                // PFMERGE 总是写 dense，这样后续 PFADD 会走 dense 原地更新分支。
                client.execute(cmd("PFMERGE", "dense", "src"));

                ReplyInteger add = (ReplyInteger) client.execute(cmd("PFADD", "dense", "c"));
                Assert.assertEquals(1, add.value());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "dense"));
                Assert.assertEquals(3, count.value());
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void densePfaddNearMaxmemoryDoesNotFalseOom() {
        YierdisDb db = openFfm(DENSE_HLL_PHYSICAL_MAXMEMORY_BYTES);
        db.bindToCurrentThread();
        try {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                ReplyObject sourceAdd = client.execute(cmd("PFADD", "src", "a", "b"));
                Assert.assertTrue("initial PFADD reply: " + replyDescription(sourceAdd), sourceAdd instanceof ReplyInteger);
                ReplyObject merge = client.execute(cmd("PFMERGE", "dense", "src"));
                Assert.assertTrue("PFMERGE reply: " + replyDescription(merge), merge instanceof ReplySimpleString);
                ReplyObject delete = client.execute(cmd("DEL", "src"));
                Assert.assertTrue("DEL reply: " + replyDescription(delete), delete instanceof ReplyInteger);

                ReplyObject add = client.execute(cmd("PFADD", "dense", "c"));
                Assert.assertTrue("PFADD reply: " + replyDescription(add), add instanceof ReplyInteger);
                Assert.assertEquals(1, ((ReplyInteger) add).value());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "dense"));
                Assert.assertEquals(3, count.value());
            }
        } finally {
            db.shutdown();
        }
    }

    private static YierdisDb openFfm(long maxmemoryBytes) {
        return createFfmDb(new DbEngineConfig(
                0,
                maxmemoryBytes,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ), 0);
    }

    @Test
    public void pfaddErrorsOnNonHllString() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                client.execute(cmd("SET", "k", "v"));

                ReplyObject err = client.execute(Arrays.asList(b("PFADD"), b("k"), b("x")));
                Assert.assertTrue(err instanceof ReplyError);
                Assert.assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", ((ReplyError) err).message());
            }
        });
    }

    private static String replyDescription(ReplyObject reply) {
        return reply instanceof ReplyError error ? error.message() : String.valueOf(reply);
    }
}
