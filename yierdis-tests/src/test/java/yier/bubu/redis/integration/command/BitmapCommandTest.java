package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class BitmapCommandTest {
    @Test
    public void getbitSetbitBasicSemantics() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                byte[] key = b("k");

                ReplyInteger miss = (ReplyInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("0")));
                Assert.assertEquals(0, miss.value());

                ReplyInteger old0 = (ReplyInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1")));
                Assert.assertEquals(0, old0.value());

                ReplyInteger now1 = (ReplyInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("0")));
                Assert.assertEquals(1, now1.value());

                // offset=7 对应第 1 个字节的最低位
                ReplyInteger old7 = (ReplyInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("7"), b("1")));
                Assert.assertEquals(0, old7.value());

                ReplyInteger get7 = (ReplyInteger) client.execute(Arrays.asList(b("GETBIT"), key, b("7")));
                Assert.assertEquals(1, get7.value());
            }
        });
    }

    @Test
    public void bitcountRangeFollowsRedisByteRangeRules() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                byte[] key = b("k");

                client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1")));
                client.execute(Arrays.asList(b("SETBIT"), key, b("15"), b("1")));

                ReplyInteger all = (ReplyInteger) client.execute(Arrays.asList(b("BITCOUNT"), key));
                Assert.assertEquals(2, all.value());

                ReplyInteger b0 = (ReplyInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("0"), b("0")));
                Assert.assertEquals(1, b0.value());

                ReplyInteger b1 = (ReplyInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("1"), b("1")));
                Assert.assertEquals(1, b1.value());

                ReplyInteger last = (ReplyInteger) client.execute(Arrays.asList(b("BITCOUNT"), key, b("-1"), b("-1")));
                Assert.assertEquals(1, last.value());
            }
        });
    }

    @Test
    public void setbitZeroFillsGrownBytesWithinCapacity() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                byte[] key = b("k");

                client.execute(cmd("SET", "k", "a"));
                client.execute(cmd("APPEND", "k", "b"));
                client.execute(cmd("SET", "k", "a"));

                ReplyInteger old = (ReplyInteger) client.execute(Arrays.asList(b("SETBIT"), key, b("8"), b("0")));
                Assert.assertEquals(0, old.value());

                ReplyInteger count = (ReplyInteger) client.execute(Arrays.asList(b("BITCOUNT"), key));
                Assert.assertEquals(Integer.bitCount('a'), count.value());
            }
        });
    }

    @Test
    public void bitmapCommandsErrorOnWrongType() {
        forEachDb(db -> {
            // 复用现有 list 命令制造非 string 类型
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                client.execute(Arrays.asList(b("LPUSH"), b("k"), b("x")));

                ReplyObject err = client.execute(Arrays.asList(b("GETBIT"), b("k"), b("0")));
                Assert.assertTrue(err instanceof ReplyError);
                Assert.assertEquals(new WrongTypeException().getMessage(), ((ReplyError) err).message());
            }
        });
    }

    @Test
    public void bitmapCommandsRejectInvalidArgumentsBeforeExecution() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                assertError("not an integer or out of range", client.execute(cmd("SETBIT", "k", "-1", "0")));
                assertError("bit is not an integer or out of range", client.execute(cmd("SETBIT", "k", "0", "2")));
                assertError("bit is not an integer or out of range", client.execute(cmd("SETBIT", "k", "0", "nope")));
                assertError("not an integer or out of range", client.execute(cmd("GETBIT", "k", "-1")));
                assertError("not an integer or out of range", client.execute(cmd("BITCOUNT", "k", "from", "2")));
            }
        });
    }

    private static void assertError(String message, ReplyObject reply) {
        Assert.assertTrue(reply instanceof ReplyError);
        Assert.assertTrue(((ReplyError) reply).message(), ((ReplyError) reply).message().contains(message));
    }
}
