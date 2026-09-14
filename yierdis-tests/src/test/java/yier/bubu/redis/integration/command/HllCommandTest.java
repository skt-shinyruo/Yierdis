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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// PFCOUNT/PFMERGE 的期望值来自真实 Redis（8.2.1）对相同 member 集合的响应，
// 不再是 Yierdis 私有哈希夹具：相同 member -> 相同寄存器 -> 相同计数。
public class HllCommandTest {
    private static final long DENSE_HLL_PHYSICAL_MAXMEMORY_BYTES = 700_000L;

    @Test
    public void pfaddCreatesAndUpdates() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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
    public void pfaddHashesEmptyMemberLikeRedis() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                // Redis：PFADD h "" 在新 key 上返回 1，PFCOUNT 计入空 member。
                ReplyInteger add = (ReplyInteger) client.execute(cmd("PFADD", "h", ""));
                Assert.assertEquals(1, add.value());
                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "h"));
                Assert.assertEquals(1, count.value());

                ReplyInteger again = (ReplyInteger) client.execute(cmd("PFADD", "h", ""));
                Assert.assertEquals(0, again.value());

                ReplyInteger withA = (ReplyInteger) client.execute(cmd("PFADD", "h", "", "a"));
                Assert.assertEquals(1, withA.value());
                ReplyInteger count2 = (ReplyInteger) client.execute(cmd("PFCOUNT", "h"));
                Assert.assertEquals(2, count2.value());
            }
        });
    }

    @Test
    public void pfcountAndPfmergeWorkOnUnion() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
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
    public void pfcountAndPfmergeMatchRedisForFixedMemberSets() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);

                // Redis：member:0..999 -> PFCOUNT 1002，sparse，STRLEN 1898。
                client.execute(pfaddArgs("big1", 0, 1000));
                Assert.assertEquals(1002, ((ReplyInteger) client.execute(cmd("PFCOUNT", "big1"))).value());
                Assert.assertEquals(1898, ((ReplyInteger) client.execute(cmd("STRLEN", "big1"))).value());

                // Redis：member:0..999 ∪ member:500..1499 -> 1503。
                client.execute(pfaddArgs("big3", 500, 1000));
                Assert.assertEquals(
                        1503,
                        ((ReplyInteger) client.execute(cmd("PFCOUNT", "big1", "big3"))).value()
                );
                ReplyObject merge = client.execute(cmd("PFMERGE", "u1", "big1", "big3"));
                Assert.assertTrue(merge instanceof ReplySimpleString);
                Assert.assertEquals(1503, ((ReplyInteger) client.execute(cmd("PFCOUNT", "u1"))).value());
                // sparse union：Redis PFMERGE 结果 STRLEN 2732。
                Assert.assertEquals(2732, ((ReplyInteger) client.execute(cmd("STRLEN", "u1"))).value());

                // Redis：member:100000..199999 -> PFCOUNT 100410，dense，STRLEN 12304。
                client.execute(pfaddArgs("big2", 100000, 100000));
                Assert.assertEquals(100410, ((ReplyInteger) client.execute(cmd("PFCOUNT", "big2"))).value());
                Assert.assertEquals(12304, ((ReplyInteger) client.execute(cmd("STRLEN", "big2"))).value());

                // Redis：member:200000..299999 经 PFMERGE -> 98874，dense source 使 dest 为 dense。
                client.execute(pfaddArgs("src2", 200000, 100000));
                Assert.assertTrue(client.execute(cmd("PFMERGE", "dd", "src2")) instanceof ReplySimpleString);
                Assert.assertEquals(98874, ((ReplyInteger) client.execute(cmd("PFCOUNT", "dd"))).value());
                Assert.assertEquals(12304, ((ReplyInteger) client.execute(cmd("STRLEN", "dd"))).value());
            }
        });
    }

    @Test
    public void pfmergeIncludesExistingDestinationAndKeepsItsTtl() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                // Redis pfmergeCommand 从 argv[1] 起合并：dest 自身参与 union；dest 对象被复用，TTL 保留。
                client.execute(cmd("PFADD", "d1", "only-in-dest"));
                client.execute(cmd("PFADD", "s1", "only-in-src"));
                client.execute(cmd("PEXPIRE", "d1", "500000"));

                Assert.assertTrue(client.execute(cmd("PFMERGE", "d1", "s1")) instanceof ReplySimpleString);
                Assert.assertEquals(2, ((ReplyInteger) client.execute(cmd("PFCOUNT", "d1"))).value());
                long pttl = ((ReplyInteger) client.execute(cmd("PTTL", "d1"))).value();
                Assert.assertTrue("PTTL after PFMERGE: " + pttl, pttl > 0);
            }
        });
    }

    @Test
    public void pfmergeWithOnlyMissingSourcesCreatesEmptyDestination() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                // Redis：source 全部缺失时仍创建空 sparse HLL dest（18 字节），PFCOUNT 为 0。
                Assert.assertTrue(client.execute(cmd("PFMERGE", "md", "nosuch1", "nosuch2")) instanceof ReplySimpleString);
                Assert.assertEquals(1, ((ReplyInteger) client.execute(cmd("EXISTS", "md"))).value());
                Assert.assertEquals(0, ((ReplyInteger) client.execute(cmd("PFCOUNT", "md"))).value());
                Assert.assertEquals(18, ((ReplyInteger) client.execute(cmd("STRLEN", "md"))).value());
            }
        });
    }

    @Test
    public void denseHllSupportsInPlacePfaddAfterPfmergeUnderFfmStorage() {
        YierdisDb db = openFfm(0L);
        try {
            db.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                // Redis：member:300000..301999 -> 1987，sparse 超 3000 字节晋升 dense；
                // source 为 dense 时 PFMERGE 的 dest 也是 dense，后续 PFADD 走 dense 更新分支。
                client.execute(pfaddArgs("src", 300000, 2000));
                client.execute(cmd("PFMERGE", "dense", "src"));

                ReplyInteger add = (ReplyInteger) client.execute(cmd("PFADD", "dense", "member:999999"));
                Assert.assertEquals(1, add.value());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "dense"));
                Assert.assertEquals(1988, count.value());
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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplyObject prefill = client.execute(pfaddArgs("dense", 300000, 2000));
                Assert.assertTrue("dense prefill reply: " + replyDescription(prefill), prefill instanceof ReplyInteger);

                ReplyObject add = client.execute(cmd("PFADD", "dense", "member:999999"));
                Assert.assertTrue("PFADD reply: " + replyDescription(add), add instanceof ReplyInteger);
                Assert.assertEquals(1, ((ReplyInteger) add).value());

                ReplyInteger count = (ReplyInteger) client.execute(cmd("PFCOUNT", "dense"));
                Assert.assertEquals(1988, count.value());
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
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                client.execute(cmd("SET", "k", "v"));

                ReplyObject err = client.execute(Arrays.asList(b("PFADD"), b("k"), b("x")));
                Assert.assertTrue(err instanceof ReplyError);
                Assert.assertEquals("WRONGTYPE Key is not a valid HyperLogLog string value.", ((ReplyError) err).message());

                ReplyObject countErr = client.execute(Arrays.asList(b("PFCOUNT"), b("k")));
                Assert.assertTrue(countErr instanceof ReplyError);
                Assert.assertEquals("WRONGTYPE Key is not a valid HyperLogLog string value.", ((ReplyError) countErr).message());

                client.execute(cmd("PFADD", "real-hll", "a"));
                ReplyObject mergeErr = client.execute(Arrays.asList(b("PFMERGE"), b("k"), b("real-hll")));
                Assert.assertTrue(mergeErr instanceof ReplyError);
                Assert.assertEquals("WRONGTYPE Key is not a valid HyperLogLog string value.", ((ReplyError) mergeErr).message());
            }
        });
    }

    private static List<byte[]> pfaddArgs(String key, int memberStartInclusive, int memberCount) {
        List<byte[]> argv = new ArrayList<>(memberCount + 2);
        argv.add(b("PFADD"));
        argv.add(b(key));
        for (int i = 0; i < memberCount; i++) {
            argv.add(b("member:" + (memberStartInclusive + i)));
        }
        return argv;
    }

    private static String replyDescription(ReplyObject reply) {
        return reply instanceof ReplyError error ? error.message() : String.valueOf(reply);
    }
}
