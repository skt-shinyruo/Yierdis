package yier.bubu.redis.runtime.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRecord;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class YierdisChangeSinkTest {
    @Test
    public void changeEventExposesExecutionRecordFacts() {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("SET", Arrays.asList("key", "value"));

        YierdisChangeEvent event = new YierdisChangeEvent(new ExecutionRecord(-4, request));

        Assert.assertEquals(0, event.dbIndex());
        Assert.assertNotNull(event.record());
        Assert.assertArrayEquals("SET".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(0));
        Assert.assertArrayEquals("key".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(1));
        Assert.assertArrayEquals("value".getBytes(StandardCharsets.US_ASCII), event.request().toByteArray(2));
    }

    @Test
    public void noopSinkAndRequireNonNullExposeStableApiHelpers() {
        YierdisChangeSink sink = YierdisChangeSink.noop();
        YierdisChangeSink same = YierdisChangeSink.requireNonNull(sink);
        YierdisChangeEvent event = new YierdisChangeEvent(
                new ExecutionRecord(0, ByteArrayExecutionRequest.fromUtf8("SET", Arrays.asList("k", "v")))
        );

        Assert.assertSame(YierdisChangeSink.NOOP, sink);
        Assert.assertSame(sink, same);
        sink.onChange(event);

        try {
            YierdisChangeSink.requireNonNull(null);
            Assert.fail("expected null sink to be rejected");
        } catch (NullPointerException expected) {
            Assert.assertEquals("sink", expected.getMessage());
        }
    }

    @Test
    public void instanceConfigBuilderNormalizesDefaultsAndValidation() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(-8)
                .maxmemoryScope(null)
                .maxmemoryPolicy((MaxmemoryPolicy) null)
                .build();

        Assert.assertEquals(1, config.databases());
        Assert.assertEquals(YierdisInstanceConfig.MaxmemoryScope.PER_DB, config.maxmemoryScope());
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, config.maxmemoryPolicy());
        Assert.assertEquals(5, config.maxmemorySamples());
        Assert.assertEquals(5L, config.evictionTimeLimitMillis());
        Assert.assertEquals(5L, config.expireCleanupTimeLimitMillis());

        Assert.assertEquals(
                MaxmemoryPolicy.ALLKEYS_LRU,
                YierdisInstanceConfig.builder().maxmemoryPolicy("allkeys-lru").build().maxmemoryPolicy()
        );

        expectIllegalArgument("maxmemoryBytes must be >= 0",
                () -> YierdisInstanceConfig.builder().maxmemoryBytes(-1).build());
        expectIllegalArgument("maxmemorySamples must be > 0",
                () -> YierdisInstanceConfig.builder().maxmemorySamples(0).build());
        expectIllegalArgument("evictionTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().evictionTimeLimitMillis(0).build());
        expectIllegalArgument("expireCleanupTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().expireCleanupTimeLimitMillis(0).build());
    }

    private static void expectIllegalArgument(String expectedMessage, Runnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals(expectedMessage, expected.getMessage());
        }
    }
}
