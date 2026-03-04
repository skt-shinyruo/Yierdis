package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.ops.DbMemoryConstants;
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

        long entryBytes = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + value.length;
        long maxmemoryBytes = entryBytes * 3L + 1L;

        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(2)
                .maxmemoryScope(YierdisInstanceConfig.MaxmemoryScope.GLOBAL)
                .maxmemoryBytes(maxmemoryBytes)
                .maxmemoryPolicy("allkeys-lru")
                // samples >= total global keys to force deterministic scan (avoid test flakiness)
                .maxmemorySamples(10)
                .evictionTimeLimitMillis(1000)
                .build();

        try (YierdisInstance instance = YierdisInstance.create(config)) {
            instance.bindToCurrentThread();
            YierdisFastCommandProcessor processor = instance.newCommandProcessor();

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
}

