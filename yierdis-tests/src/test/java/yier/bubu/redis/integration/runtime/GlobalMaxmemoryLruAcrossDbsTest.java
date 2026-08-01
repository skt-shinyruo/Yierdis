package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.integration.command.TestCommandDispatchers;
import yier.bubu.redis.storage.api.MaxmemoryErrors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplySimpleString;
import yier.bubu.redis.testutil.TestYierdisInstances;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;

public class GlobalMaxmemoryLruAcrossDbsTest {
    @Test
    public void globalLruEvictsLeastRecentlyUsedAcrossDbs() {
        byte[] value = new byte[64 * 1024];
        Arrays.fill(value, (byte) 'a');

        long maxmemoryBytes = minGlobalMaxmemoryThatAllowsKeyCount(value, 4) - 1L;

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
                // samples >= total global keys to force deterministic scan (avoid test flakiness)
                .maxmemorySamples(10)
                .evictionTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(config)) {
            instance.bindToCurrentThread();
            CommandDispatcher dispatcher = TestCommandDispatchers.forRouter(TestDbRouters.forInstance(instance));

            try (FastTestClient client = new FastTestClient(dispatcher)) {
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

    private static long minGlobalMaxmemoryThatAllowsKeyCount(byte[] value, int count) {
        long high = 1L;
        while (!allowsGlobalKeyCount(high, value, count)) {
            high = Math.multiplyExact(high, 2L);
        }

        long low = 0L;
        while (low + 1L < high) {
            long mid = low + (high - low) / 2L;
            if (allowsGlobalKeyCount(mid, value, count)) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return high;
    }

    private static boolean allowsGlobalKeyCount(long maxmemoryBytes, byte[] value, int count) {
        YierdisInstanceConfig probeConfig = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
                .maxmemorySamples(10)
                .evictionTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(probeConfig)) {
            instance.bindToCurrentThread();
            for (int i = 0; i < count; i++) {
                instance.engine(i % 2).writes().strings().setString(b("probe-" + i), value, SetMode.NORMAL, null);
            }
            return true;
        } catch (YierdisCommandException e) {
            if (MaxmemoryErrors.OOM_ERR.equals(e.getMessage())) {
                return false;
            }
            throw e;
        }
    }
}
