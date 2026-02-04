package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class MemoryStatsCommandTest {
    @Test
    public void memoryStatsReturnsStableKeyValuePairs() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                RespObject resp = client.execute(cmd("MEMORY", "STATS"));
                Assert.assertTrue(resp instanceof RespArray);
                List<RespObject> values = ((RespArray) resp).values();
                Assert.assertNotNull(values);
                Assert.assertEquals(40, values.size());

                Map<String, RespObject> map = toObjectMap(values);
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

    private static Map<String, RespObject> toObjectMap(List<RespObject> values) {
        Map<String, RespObject> map = new HashMap<>();
        for (int i = 0; i + 1 < values.size(); i += 2) {
            Assert.assertTrue(values.get(i) instanceof RespBulkString);
            String k = ((RespBulkString) values.get(i)).asString();
            map.put(k, values.get(i + 1));
        }
        return map;
    }

    private static void assertLongValue(RespObject obj) {
        Assert.assertNotNull(obj);
        if (obj instanceof RespInteger i) {
            return;
        }
        if (obj instanceof RespBulkString bs) {
            Long.parseLong(bs.asString());
            return;
        }
        Assert.fail("expected integer-like reply, got: " + obj.getClass().getSimpleName());
    }
}
