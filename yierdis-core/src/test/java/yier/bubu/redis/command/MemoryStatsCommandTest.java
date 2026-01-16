package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
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
                Assert.assertEquals(34, values.size());

                Map<String, String> map = toStringMap(values);
                Assert.assertTrue(map.containsKey("maxmemory_bytes"));
                Assert.assertTrue(map.containsKey("used_bytes_for_maxmemory"));
                Assert.assertTrue(map.containsKey("heap_data_bytes_estimate"));
                Assert.assertTrue(map.containsKey("offheap_used_bytes"));
                Assert.assertTrue(map.containsKey("total_estimated_bytes"));
                Assert.assertTrue(map.containsKey("keyspace_rehashing"));
                Assert.assertTrue(map.containsKey("keyspace_table0_capacity"));
                Assert.assertTrue(map.containsKey("expire_rehashing"));

                assertLong(map.get("maxmemory_bytes"));
                assertLong(map.get("used_bytes_for_maxmemory"));
                assertLong(map.get("heap_data_bytes_estimate"));
                assertLong(map.get("offheap_used_bytes"));
                assertLong(map.get("total_estimated_bytes"));
                assertLong(map.get("key_count"));
                assertLong(map.get("expire_count"));
            }
        });
    }

    private static Map<String, String> toStringMap(List<RespObject> values) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i + 1 < values.size(); i += 2) {
            Assert.assertTrue(values.get(i) instanceof RespBulkString);
            Assert.assertTrue(values.get(i + 1) instanceof RespBulkString);
            String k = ((RespBulkString) values.get(i)).asString();
            String v = ((RespBulkString) values.get(i + 1)).asString();
            map.put(k, v);
        }
        return map;
    }

    private static void assertLong(String s) {
        Assert.assertNotNull(s);
        Long.parseLong(s);
    }
}

