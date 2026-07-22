package yier.bubu.redis.runtime.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.Arrays;

public class YierdisChangeSinkTest {
    @Test
    public void changeEventExposesExecutionRecordFacts() {
        ExecutionRequest request = ByteArrayExecutionRequest.fromUtf8("SET", Arrays.asList("key", "value"));

        YierdisChangeEvent event = new YierdisChangeEvent(new ExecutionRecord(-4, request));

        Assert.assertEquals(0, event.dbIndex());
        Assert.assertEquals(YierdisChangeKind.USER_COMMAND, event.kind());
        Assert.assertFalse(event.synthetic());
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
        Assert.assertSame(YierdisChangeSink.NOOP, config.changeSink());
        Assert.assertEquals(new DbDefragConfig(false, 64L * 1024L, 64L, 1L), config.defrag());
        Assert.assertEquals(8_192, config.commitStreamMaxEvents());
        Assert.assertEquals(64L * 1024L * 1024L, config.commitStreamMaxRetainedBytes());
        Assert.assertEquals(5_000L, config.commitStreamShutdownTimeoutMillis());

        YierdisInstanceConfig defragConfig = YierdisInstanceConfig.builder()
                .defrag(new DbDefragConfig(true, 1024L, 7L, 3L))
                .build();
        Assert.assertEquals(new DbDefragConfig(true, 1024L, 7L, 3L), defragConfig.defrag());

        YierdisInstanceConfig streamConfig = YierdisInstanceConfig.builder()
                .commitStreamMaxEvents(7)
                .commitStreamMaxRetainedBytes(8_192L)
                .commitStreamShutdownTimeoutMillis(13L)
                .build();
        Assert.assertEquals(7, streamConfig.commitStreamMaxEvents());
        Assert.assertEquals(8_192L, streamConfig.commitStreamMaxRetainedBytes());
        Assert.assertEquals(13L, streamConfig.commitStreamShutdownTimeoutMillis());

        Assert.assertEquals(
                MaxmemoryPolicy.ALLKEYS_LRU,
                YierdisInstanceConfig.builder().maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU).build().maxmemoryPolicy()
        );
        YierdisChangeSink sink = event -> {
        };
        Assert.assertSame(sink, YierdisInstanceConfig.builder().changeSink(sink).build().changeSink());
        Assert.assertSame(YierdisChangeSink.NOOP, YierdisInstanceConfig.builder().changeSink(null).build().changeSink());

        expectIllegalArgument("maxmemoryBytes must be >= 0",
                () -> YierdisInstanceConfig.builder().maxmemoryBytes(-1).build());
        expectIllegalArgument("maxmemorySamples must be > 0",
                () -> YierdisInstanceConfig.builder().maxmemorySamples(0).build());
        expectIllegalArgument("evictionTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().evictionTimeLimitMillis(0).build());
        expectIllegalArgument("expireCleanupTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().expireCleanupTimeLimitMillis(0).build());
        expectIllegalArgument("defrag limits must be non-negative",
                () -> new DbDefragConfig(false, -1L, 0L, 0L));
        expectIllegalArgument("defrag limits must be non-negative",
                () -> new DbDefragConfig(false, 0L, -1L, 0L));
        expectIllegalArgument("defrag limits must be non-negative",
                () -> new DbDefragConfig(false, 0L, 0L, -1L));
        NullPointerException nullDefrag = Assert.assertThrows(
                NullPointerException.class,
                () -> YierdisInstanceConfig.builder().defrag(null)
        );
        Assert.assertEquals("defrag", nullDefrag.getMessage());
        expectIllegalArgument("commitStreamMaxEvents must be > 0",
                () -> YierdisInstanceConfig.builder().commitStreamMaxEvents(0).build());
        expectIllegalArgument("commitStreamMaxRetainedBytes must be > 0",
                () -> YierdisInstanceConfig.builder().commitStreamMaxRetainedBytes(0L).build());
        expectIllegalArgument("commitStreamShutdownTimeoutMillis must be > 0",
                () -> YierdisInstanceConfig.builder().commitStreamShutdownTimeoutMillis(0L).build());
    }

    @Test
    public void instanceConfigDoesNotExposeLegacyEngineFactoryOwnedResourceApi() {
        for (Field field : YierdisInstanceConfig.class.getDeclaredFields()) {
            Assert.assertFalse(
                    "EngineFactoryBinding owns runtime resources now: " + field,
                    field.getName().equals("engineFactoryOwnedResource")
            );
        }
        for (Method method : YierdisInstanceConfig.class.getDeclaredMethods()) {
            Assert.assertFalse(
                    "EngineFactoryBinding owns runtime resources now: " + method,
                    method.getName().equals("engineFactoryOwnedResource")
            );
        }
        for (Method method : YierdisInstanceConfig.Builder.class.getDeclaredMethods()) {
            Assert.assertFalse(
                    "EngineFactoryBinding owns runtime resources now: " + method,
                    method.getName().equals("engineFactoryOwnedResource")
            );
            Assert.assertFalse(
                    "legacy offheap bridge should not be exposed: " + method,
                    method.getName().equals("offHeapAllocator")
                            || method.getName().equals("owns" + "Off" + "HeapAllocator")
                            || method.getName().equals("offHeapKeysEnabled")
            );
        }
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
