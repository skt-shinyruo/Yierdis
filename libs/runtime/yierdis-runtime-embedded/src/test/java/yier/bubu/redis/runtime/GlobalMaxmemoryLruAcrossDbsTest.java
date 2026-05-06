package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.TestCommandProcessors;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class GlobalMaxmemoryLruAcrossDbsTest {
    @Test
    public void globalLruEvictsLeastRecentlyUsedAcrossDbs() {
        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'a');

        long writeUpperBound = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + value.length;
        long maxmemoryBytes = maxmemoryBudgetThatFitsThreeKeysButNotFour(value, writeUpperBound);

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                // samples >= total global keys to force deterministic scan (avoid test flakiness)
                .maxmemorySamples(10)
                .evictionTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forRouter(TestDbRouters.forInstance(instance));

            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("a"), value))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("b"), value))).value());

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("c"), value))).value());

                // Touch "a" so that "b" becomes the global LRU.
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("0")))).value());
                Assert.assertArrayEquals(value, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("a")))).data());

                // Trigger eviction from DB1 and ensure it can evict from DB0.
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());
                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SET"), b("d"), value))).value());

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("0")))).value());
                Assert.assertSame(ReplyNull.INSTANCE, client.execute(Arrays.asList(b("GET"), b("b"))));
                Assert.assertArrayEquals(value, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("a")))).data());

                Assert.assertEquals("OK", ((ReplySimpleString) client.execute(Arrays.asList(b("SELECT"), b("1")))).value());
                Assert.assertArrayEquals(value, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("c")))).data());
                Assert.assertArrayEquals(value, ((ReplyBulkString) client.execute(Arrays.asList(b("GET"), b("d")))).data());
            }
        }
    }

    private static long maxmemoryBudgetThatFitsThreeKeysButNotFour(byte[] value, long writeUpperBound) {
        long usedAfterTwoKeys = probeGlobalUsedBytes(value, false);
        long usedAfterThreeKeys = probeGlobalUsedBytes(value, true);
        long lowerBound = usedAfterTwoKeys + writeUpperBound;
        long upperExclusive = usedAfterThreeKeys + writeUpperBound;
        Assert.assertTrue("probe budget must leave room between 3rd and 4th write", upperExclusive > lowerBound);
        long span = upperExclusive - lowerBound;
        return lowerBound + Math.max(0L, (span - 1L) / 2L);
    }

    private static long probeGlobalUsedBytes(byte[] value, boolean includeThirdKey) {
        YierdisInstanceConfig probeConfig = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(1_000_000)
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                .maxmemorySamples(10)
                .evictionTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(probeConfig)) {
            instance.bindToCurrentThread();
            YierdisDb db0 = (YierdisDb) instance.engine(0);
            YierdisDb db1 = (YierdisDb) instance.engine(1);
            db0.writes().strings().setString(b("a"), value, SetMode.NORMAL, null);
            db0.writes().strings().setString(b("b"), value, SetMode.NORMAL, null);
            if (includeThirdKey) {
                db1.writes().strings().setString(b("c"), value, SetMode.NORMAL, null);
            }
            return db0.usedBytesForMaxmemory() + db1.usedBytesForMaxmemory() + instance.runtimeMemoryRuntime().usedBytes();
        }
    }
}
