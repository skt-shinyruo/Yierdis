package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.OffHeapKeyCopyDiagnostics;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapKeysZeroCopyReadPathTest {
    @Test
    public void readPathDoesNotCopyCanonicalKeyBytesWhenKeysStoredOffHeap() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, true, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                OffHeapKeyCopyDiagnostics.reset();
                Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);

                // 只统计“从 off-heap 复制 canonical key bytes 到 heap”的次数；SET 本身不应触发该路径。
                OffHeapKeyCopyDiagnostics.reset();

                ReplyBulkString v = (ReplyBulkString) client.execute(cmd("GET", "k"));
                Assert.assertEquals("v", v.asString());

                ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "k"));
                Assert.assertEquals(1L, exists.value());

                ReplySimpleString type = (ReplySimpleString) client.execute(cmd("TYPE", "k"));
                Assert.assertEquals("string", type.value());

                ReplyInteger ttl = (ReplyInteger) client.execute(cmd("TTL", "k"));
                Assert.assertEquals(-1L, ttl.value());

                Assert.assertEquals(0L, OffHeapKeyCopyDiagnostics.heapKeyCopies());
            }
        } finally {
            db.shutdown();
        }
    }
}
