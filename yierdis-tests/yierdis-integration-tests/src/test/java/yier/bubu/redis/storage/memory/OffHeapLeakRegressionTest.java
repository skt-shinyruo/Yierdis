package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.runtime.embedded.TestCommandDispatchers;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.memory.YierdisDbBackendConfig;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapLeakRegressionTest {
    private static final int SPAN_VALUE_BYTES = 512 * 1024;

    @Test
    public void ffmEvictionAndExpireDoNotLeak() {
        try (FfmDbFixture fixture = openFfm(1_100_000L)) {
            YierdisDb db = fixture.db();
            YierdisFfmStableMemoryBackend backend = fixture.backend();
            db.bindToCurrentThread();
            warmMetadataBaseline(backend);
            MemoryUsageSnapshot baseline = backend.memoryUsage();
            long baselineLiveRegions = backend.liveRegionCount();
            MemoryUsageSnapshot highWater = baseline;
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                byte[] spanValue = repeat((byte) 'x', SPAN_VALUE_BYTES);

                ReplyObject firstSet = client.execute(List.of(b("SET"), b("a"), spanValue));
                Assert.assertTrue("first SET reply: " + replyDescription(firstSet), firstSet instanceof ReplySimpleString);
                MemoryUsageSnapshot afterA = backend.memoryUsage();
                highWater = highWater(highWater, afterA);
                Assert.assertTrue(afterA.nativeDataLiveBytes() > baseline.nativeDataLiveBytes());
                assertGlobalMemoryIncludesBackend(afterA, fixture.globalEngine().memoryUsage());

                Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), spanValue)) instanceof ReplySimpleString);
                ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b"));
                Assert.assertEquals(1L, exists.value());
                MemoryUsageSnapshot afterEviction = backend.memoryUsage();
                highWater = highWater(highWater, afterEviction);
                Assert.assertTrue(
                        "eviction should free some off-heap bytes",
                        afterEviction.nativeDataLiveBytes() <= afterA.nativeDataLiveBytes()
                );

                Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof ReplySimpleString);
                Assert.assertEquals(1L, ((ReplyInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                Assert.assertEquals(-2L, ((ReplyInteger) client.execute(cmd("TTL", "e"))).value());
                highWater = highWater(highWater, backend.memoryUsage());
                client.execute(cmd("DEL", "a", "b"));
            }
            assertTrimmedToBaseline(backend, baseline, baselineLiveRegions, highWater);
        }
    }

    @Test
    public void ffmEvictionDeleteAndExpireDoNotLeak() {
        try (FfmDbFixture fixture = openFfm(1_600_000L)) {
            YierdisDb db = fixture.db();
            YierdisFfmStableMemoryBackend backend = fixture.backend();
            db.bindToCurrentThread();
            warmMetadataBaseline(backend);
            MemoryUsageSnapshot baseline = backend.memoryUsage();
            long baselineLiveRegions = backend.liveRegionCount();
            MemoryUsageSnapshot highWater = baseline;
            CommandDispatcher dispatcher = TestCommandDispatchers.forDb(db);
            try (FastTestClient client = new FastTestClient(dispatcher)) {
                byte[] spanValue = repeat((byte) 'x', SPAN_VALUE_BYTES);

                ReplyObject firstSet = client.execute(Arrays.asList(b("SET"), b("a"), spanValue));
                Assert.assertTrue("first SET reply: " + replyDescription(firstSet), firstSet instanceof ReplySimpleString);
                highWater = highWater(highWater, backend.memoryUsage());
                ReplyObject secondSet = client.execute(Arrays.asList(b("SET"), b("b"), spanValue));
                Assert.assertTrue("second SET reply: " + replyDescription(secondSet), secondSet instanceof ReplySimpleString);
                highWater = highWater(highWater, backend.memoryUsage());
                ReplyObject thirdSet = client.execute(Arrays.asList(b("SET"), b("c"), spanValue));
                Assert.assertTrue("third SET reply: " + replyDescription(thirdSet), thirdSet instanceof ReplySimpleString);
                highWater = highWater(highWater, backend.memoryUsage());

                ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b", "c"));
                Assert.assertTrue("third SET must trigger eviction", exists.value() < 3L);
                Assert.assertEquals(2L, exists.value());

                client.execute(cmd("DEL", "a", "b", "c"));
                Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof ReplySimpleString);
                Assert.assertEquals(1L, ((ReplyInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                Assert.assertEquals(-2L, ((ReplyInteger) client.execute(cmd("TTL", "e"))).value());
                highWater = highWater(highWater, backend.memoryUsage());
            }
            assertTrimmedToBaseline(backend, baseline, baselineLiveRegions, highWater);
        }
    }

    private static FfmDbFixture openFfm(long maxmemoryBytes) {
        List<YierdisFfmStableMemoryBackend> createdBackends = new ArrayList<>(1);
        StableMemoryBackendFactory backendFactory = (name, maxSlots, owner) -> {
            YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
            createdBackends.add(backend);
            return backend;
        };
        RuntimeDbEngine engine = new YierdisDbEngineFactory(
                backendFactory,
                new YierdisDbBackendConfig(0)
        ).create(new DbEngineConfig(
                0,
                maxmemoryBytes,
                MaxmemoryPolicy.ALLKEYS_RANDOM,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        if (!(engine instanceof GlobalMaxmemoryDbEngine globalEngine)) {
            engine.shutdown();
            throw new IllegalStateException("FFM test DB must expose GlobalMaxmemoryDbEngine");
        }
        if (!(engine instanceof YierdisDb db)) {
            engine.shutdown();
            throw new IllegalStateException("YierdisDbEngineFactory did not create YierdisDb");
        }
        Assert.assertEquals(1, createdBackends.size());
        return new FfmDbFixture(db, globalEngine, createdBackends.get(0));
    }

    private static void assertTrimmedToBaseline(
            YierdisFfmStableMemoryBackend backend,
            MemoryUsageSnapshot baseline,
            long baselineLiveRegions,
            MemoryUsageSnapshot highWater
    ) {
        backend.trimEmptyPages(MemoryPressureBudget.unlimited());
        MemoryUsageSnapshot afterTrim = backend.memoryUsage();
        Assert.assertEquals(baseline.nativeDataLiveBytes(), afterTrim.nativeDataLiveBytes());
        Assert.assertEquals(
                "after trim=" + afterTrim + ", allocator stats=" + backend.stats(),
                baselineLiveRegions,
                backend.liveRegionCount()
        );
        Assert.assertTrue(afterTrim.nativeMetadataCommittedBytes() <= highWater.nativeMetadataCommittedBytes());
        Assert.assertTrue(afterTrim.nativeDataCommittedBytes() <= highWater.nativeDataCommittedBytes());
    }

    private static void warmMetadataBaseline(YierdisFfmStableMemoryBackend backend) {
        NativeHandle handle = backend.allocate(NativeObjectKind.GENERIC, 1);
        backend.free(handle);
        backend.trimEmptyPages(MemoryPressureBudget.unlimited());
    }

    private static void assertGlobalMemoryIncludesBackend(
            MemoryUsageSnapshot backendUsage,
            MemoryUsageSnapshot globalUsage
    ) {
        Assert.assertTrue(globalUsage.heapEstimatedBytes() >= backendUsage.heapEstimatedBytes());
        Assert.assertEquals(
                backendUsage.nativeMetadataCommittedBytes(),
                globalUsage.nativeMetadataCommittedBytes()
        );
        Assert.assertEquals(backendUsage.nativeDataCommittedBytes(), globalUsage.nativeDataCommittedBytes());
        Assert.assertEquals(backendUsage.nativeDataLiveBytes(), globalUsage.nativeDataLiveBytes());
        Assert.assertEquals(backendUsage.nativeReclaimableBytes(), globalUsage.nativeReclaimableBytes());
    }

    private static MemoryUsageSnapshot highWater(MemoryUsageSnapshot current, MemoryUsageSnapshot candidate) {
        return new MemoryUsageSnapshot(
                Math.max(current.heapEstimatedBytes(), candidate.heapEstimatedBytes()),
                Math.max(current.nativeMetadataCommittedBytes(), candidate.nativeMetadataCommittedBytes()),
                Math.max(current.nativeDataCommittedBytes(), candidate.nativeDataCommittedBytes()),
                Math.max(current.nativeDataLiveBytes(), candidate.nativeDataLiveBytes()),
                Math.max(current.nativeReclaimableBytes(), candidate.nativeReclaimableBytes())
        );
    }

    private record FfmDbFixture(
            YierdisDb db,
            GlobalMaxmemoryDbEngine globalEngine,
            YierdisFfmStableMemoryBackend backend
    ) implements AutoCloseable {
        @Override
        public void close() {
            db.shutdown();
        }
    }

    private static byte[] repeat(byte b, int len) {
        byte[] out = new byte[len];
        Arrays.fill(out, b);
        return out;
    }

    private static String replyDescription(ReplyObject reply) {
        return reply instanceof ReplyError error ? error.message() : String.valueOf(reply);
    }
}
