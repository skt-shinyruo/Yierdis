package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class HllCommandTest {
    @Test
    public void hllCommandsUseReadWriteBoundariesInsteadOfLegacyValueOps() throws IOException {
        String source = Files.readString(Path.of(
                "..", "..", "yierdis-command", "yierdis-command-defaults", "src", "main", "java", "yier", "bubu", "redis", "command", "HllCommands.java"
        ));

        Assert.assertFalse(source.contains("eviction().prepareWrite("));
        Assert.assertFalse(source.contains("values().hll()."));
    }

    @Test
    public void pfaddCreatesAndUpdates() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
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
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
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
    }

    @Test
    public void densePfaddNearMaxmemoryDoesNotFalseOom() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 13000, "noeviction", 5, 5, 5);
            db.bindToCurrentThread();
            try {
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    client.execute(cmd("PFADD", "src", "a", "b"));
                    client.execute(cmd("PFMERGE", "dense", "src"));
                    client.execute(cmd("DEL", "src"));

                    ReplyObject add = client.execute(cmd("PFADD", "dense", "c"));
                    Assert.assertTrue(add instanceof ReplyInteger);
                    Assert.assertEquals(1, ((ReplyInteger) add).value());

                    ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "dense"));
                    Assert.assertEquals(3, count.value());
                }
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void pfaddErrorsOnNonHllString() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                client.execute(cmd("SET", "k", "v"));

                ReplyObject err = client.execute(Arrays.asList(b("PFADD"), b("k"), b("x")));
                Assert.assertTrue(err instanceof ReplyError);
                Assert.assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", ((ReplyError) err).message());
            }
        });
    }
}
