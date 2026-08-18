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
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;

public class MaxmemoryDoubleReplyRegressionTest {
    @Test
    public void appendUnderMaxmemoryReturnsSingleErrorReply() {
        YierdisDb db = createFfmDb(new DbEngineConfig(
                0,
                40L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ), 0);
        try {
            db.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplyObject reply = client.execute(Arrays.asList(
                        b("APPEND"),
                        b("k"),
                        b("0123456789")
                ));
                Assert.assertTrue(reply instanceof ReplyError);
                Assert.assertEquals(
                        "OOM command not allowed when used memory > 'maxmemory'.",
                        ((ReplyError) reply).message()
                );
            }
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void setbitUnderMaxmemoryReturnsSingleErrorReply() {
        YierdisDb db = createFfmDb(new DbEngineConfig(
                0,
                40L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ), 0);
        try {
            db.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                ReplyObject reply = client.execute(Arrays.asList(
                        b("SETBIT"),
                        b("k"),
                        b("80"),
                        b("1")
                ));
                Assert.assertTrue(reply instanceof ReplyError);
                Assert.assertEquals(
                        "OOM command not allowed when used memory > 'maxmemory'.",
                        ((ReplyError) reply).message()
                );
            }
        } finally {
            db.shutdown();
        }
    }
}
