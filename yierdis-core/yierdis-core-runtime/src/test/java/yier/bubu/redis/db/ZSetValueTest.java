package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZSetValueTest {
    @Test
    public void packedZSetKeepsScoreOrderingAndSupportsUpdates() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-test")) {
            ZSetValue zv = new ZSetValue(runtime);
            try {
                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zv.encoding());

                List<byte[]> args = Arrays.asList(
                        b("1"), b("a"),
                        b("1"), b("b"),
                        b("0"), b("c")
                );
                Assert.assertEquals(3, zv.zaddMany(args));

                List<byte[]> range = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range.size());
                Assert.assertArrayEquals(b("c"), range.get(0));
                Assert.assertArrayEquals(b("a"), range.get(1));
                Assert.assertArrayEquals(b("b"), range.get(2));

                Assert.assertEquals(0, zv.zaddMany(Arrays.asList(b("2"), b("a"))));
                List<byte[]> range2 = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range2.size());
                Assert.assertArrayEquals(b("c"), range2.get(0));
                Assert.assertArrayEquals(b("b"), range2.get(1));
                Assert.assertArrayEquals(b("a"), range2.get(2));

                Assert.assertEquals(1, zv.zrem(List.of(b("b"))));
                Assert.assertEquals(2, zv.size());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void zsetUpgradesAfterTooManyEntries() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-test")) {
            ZSetValue zv = new ZSetValue(runtime);
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                Assert.assertEquals(200, zv.size());
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zv.encoding());
            } finally {
                zv.close();
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
