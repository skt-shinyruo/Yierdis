package yier.bubu.redis.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class MaxmemoryDoubleReplyRegressionTest {
    @Test
    public void appendUnderMaxmemoryReturnsSingleErrorReply() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 40, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
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
    }
}
