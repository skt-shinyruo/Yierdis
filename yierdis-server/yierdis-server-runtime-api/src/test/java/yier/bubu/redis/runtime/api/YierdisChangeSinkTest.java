package yier.bubu.redis.runtime.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeKind;
import yier.bubu.redis.storage.api.DbChangeListener;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public void changeEventBridgeConvertsDbChangesToSyntheticEvents() {
        List<YierdisChangeEvent> events = new ArrayList<>();
        DbChangeListener bridge = YierdisChangeEventBridge.forSink(events::add);

        bridge.onDbChange(DbChange.syntheticDelete(2, DbChangeKind.EXPIRED, bytes("dead")));

        Assert.assertEquals(1, events.size());
        YierdisChangeEvent event = events.get(0);
        Assert.assertEquals(2, event.dbIndex());
        Assert.assertEquals(YierdisChangeKind.EXPIRED, event.kind());
        Assert.assertTrue(event.synthetic());
        Assert.assertEquals("DEL", arg(event, 0));
        Assert.assertEquals("dead", arg(event, 1));
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
        Assert.assertFalse(config.nativeDefragEnabled());
        Assert.assertEquals(64L * 1024L, config.nativeDefragMaxMoveBytes());
        Assert.assertEquals(64L, config.nativeDefragMaxObjects());
        Assert.assertEquals(1L, config.nativeDefragTimeLimitMillis());

        YierdisInstanceConfig defragConfig = YierdisInstanceConfig.builder()
                .nativeDefragEnabled(true)
                .nativeDefragMaxMoveBytes(1024)
                .nativeDefragMaxObjects(7)
                .nativeDefragTimeLimitMillis(3)
                .build();
        Assert.assertTrue(defragConfig.nativeDefragEnabled());
        Assert.assertEquals(1024L, defragConfig.nativeDefragMaxMoveBytes());
        Assert.assertEquals(7L, defragConfig.nativeDefragMaxObjects());
        Assert.assertEquals(3L, defragConfig.nativeDefragTimeLimitMillis());

        Assert.assertEquals(
                MaxmemoryPolicy.ALLKEYS_LRU,
                YierdisInstanceConfig.builder().maxmemoryPolicy("allkeys-lru").build().maxmemoryPolicy()
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
        expectIllegalArgument("nativeDefragMaxMoveBytes must be >= 0",
                () -> YierdisInstanceConfig.builder().nativeDefragMaxMoveBytes(-1).build());
        expectIllegalArgument("nativeDefragMaxObjects must be >= 0",
                () -> YierdisInstanceConfig.builder().nativeDefragMaxObjects(-1).build());
        expectIllegalArgument("nativeDefragTimeLimitMillis must be >= 0",
                () -> YierdisInstanceConfig.builder().nativeDefragTimeLimitMillis(-1).build());
    }

    private static void expectIllegalArgument(String expectedMessage, Runnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals(expectedMessage, expected.getMessage());
        }
    }

    private static String arg(YierdisChangeEvent event, int index) {
        return new String(event.request().toByteArray(index), StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
