package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class MaxmemoryDoubleReplyRegressionTest {
    @Test
    public void appendUnderMaxmemoryReturnsSingleErrorReply() {
        // Intentionally run with the unsafe off-heap backend: it can under-estimate additional bytes due to
        // size class rounding, which previously could trigger "normal reply + OOM error reply" corruption.
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, 40, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                RespObject reply = client.execute(Arrays.asList(
                        b("APPEND"),
                        b("k"),
                        b("0123456789")
                ));
                Assert.assertTrue(reply instanceof RespError);
                Assert.assertEquals(
                        "OOM command not allowed when used memory > 'maxmemory'.",
                        ((RespError) reply).message()
                );
            }
        } finally {
            db.shutdown();
        }
    }
}

