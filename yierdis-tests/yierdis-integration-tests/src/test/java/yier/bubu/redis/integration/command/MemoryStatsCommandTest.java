package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyMap;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class MemoryStatsCommandTest {
    @Test
    public void memoryStatsReturnsStableKeyValuePairs() {
        forEachDb(db -> {
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyObject resp = client.execute(cmd("MEMORY", "STATS"));
                Assert.assertTrue(resp instanceof ReplyMap);
                List<ReplyMap.Entry> entries = ((ReplyMap) resp).entries();
                Assert.assertNotNull(entries);
                Assert.assertEquals(20, entries.size());

                Map<String, ReplyObject> map = toObjectMap(entries);
                Assert.assertTrue(map.containsKey("maxmemory_bytes"));
                Assert.assertTrue(map.containsKey("used_bytes_for_maxmemory"));
                Assert.assertTrue(map.containsKey("effective_used_bytes_for_maxmemory"));
                Assert.assertTrue(map.containsKey("ledger_used_bytes"));
                Assert.assertTrue(map.containsKey("ledger_reserved_bytes"));
                Assert.assertTrue(map.containsKey("offheap_used_bytes"));
                Assert.assertTrue(map.containsKey("offheap_included_in_maxmemory"));
                Assert.assertTrue(map.containsKey("total_estimated_bytes"));
                Assert.assertTrue(map.containsKey("keyspace_rehashing"));
                Assert.assertTrue(map.containsKey("keyspace_table0_capacity"));
                Assert.assertTrue(map.containsKey("expire_rehashing"));

                assertLongValue(map.get("maxmemory_bytes"));
                assertLongValue(map.get("used_bytes_for_maxmemory"));
                assertLongValue(map.get("effective_used_bytes_for_maxmemory"));
                assertLongValue(map.get("ledger_used_bytes"));
                assertLongValue(map.get("ledger_reserved_bytes"));
                assertLongValue(map.get("offheap_used_bytes"));
                assertLongValue(map.get("total_estimated_bytes"));
                assertLongValue(map.get("key_count"));
                assertLongValue(map.get("expire_count"));
            }
        });
    }

    private static Map<String, ReplyObject> toObjectMap(List<ReplyMap.Entry> entries) {
        Map<String, ReplyObject> map = new HashMap<>();
        for (ReplyMap.Entry e : entries) {
            Assert.assertTrue(e.key() instanceof ReplyBulkString);
            String k = ((ReplyBulkString) e.key()).asString();
            map.put(k, e.value());
        }
        return map;
    }

    private static void assertLongValue(ReplyObject obj) {
        Assert.assertNotNull(obj);
        if (obj instanceof ReplyInteger i) {
            return;
        }
        if (obj instanceof ReplyBulkString bs) {
            Long.parseLong(bs.asString());
            return;
        }
        Assert.fail("expected integer-like reply, got: " + obj.getClass().getSimpleName());
    }
}
