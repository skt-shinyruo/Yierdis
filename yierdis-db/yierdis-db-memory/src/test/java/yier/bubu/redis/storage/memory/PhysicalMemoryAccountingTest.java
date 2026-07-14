package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PhysicalMemoryAccountingTest {
    @Test
    public void dbSnapshotCountsAllocatorCommittedMemoryOnce() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("physical-accounting")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);

                MemoryUsageSnapshot usage = db.memoryUsage();
                MemoryUsageSnapshot allocatorUsage = db.nativeAllocator().memoryUsage();
                NativeAllocatorStats allocatorStats = db.nativeAllocator().stats();

                Assert.assertEquals(
                        allocatorStats.metadataCommittedBytes(),
                        usage.nativeMetadataCommittedBytes()
                );
                Assert.assertEquals(allocatorStats.committedBytes(), usage.nativeDataCommittedBytes());
                Assert.assertEquals(allocatorStats.reservedBytes(), usage.nativeDataLiveBytes());
                Assert.assertEquals(allocatorUsage.nativeReclaimableBytes(), usage.nativeReclaimableBytes());
                Assert.assertTrue(usage.heapEstimatedBytes() >= allocatorUsage.heapEstimatedBytes());
                Assert.assertEquals(usage.effectiveBytesForMaxmemory(), db.usedBytesForMaxmemory());
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void dbSnapshotCountsAllocatorAndFfmExpiryRegionsExactlyOnce() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("physical-accounting-expiry")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                db.writes().strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);
                db.writes().ttl().pexpire(view("key"), 60_000L);

                MemoryUsageSnapshot usage = db.memoryUsage();
                NativeAllocatorStats allocator = db.nativeAllocator().stats();
                long expiryRegionBytes = runtime.usedBytes()
                        - allocator.committedBytes()
                        - allocator.metadataCommittedBytes();

                Assert.assertTrue("PEXPIRE must allocate a dedicated expiry region", expiryRegionBytes > 0L);
                Assert.assertEquals(allocator.metadataCommittedBytes(), usage.nativeMetadataCommittedBytes());
                Assert.assertEquals(
                        allocator.committedBytes() + expiryRegionBytes,
                        usage.nativeDataCommittedBytes()
                );
                Assert.assertEquals(
                        MemoryUsageSnapshot.addSaturating(
                                usage.heapEstimatedBytes(),
                                MemoryUsageSnapshot.addSaturating(
                                        usage.nativeMetadataCommittedBytes(),
                                        usage.nativeDataCommittedBytes()
                                )
                        ),
                        usage.effectiveBytesForMaxmemory()
                );
            } finally {
                db.shutdown();
            }
        }
    }

    @Test
    public void snapshotUsesRetainedHeapCountersWithoutWalkingCollections() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("physical-accounting-counters")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    0,
                    MaxmemoryPolicy.NOEVICTION,
                    5,
                    5,
                    5
            );
            db.bindToCurrentThread();
            try {
                for (int i = 0; i < 2_500; i++) {
                    db.writes().hashes().hset(bytes("hash:" + i), List.of(bytes("field"), bytes("value")));
                    db.writes().sets().sadd(bytes("set:" + i), List.of(bytes("member")));
                    db.writes().zsets().zadd(bytes("zset:" + i), List.of(bytes("1"), bytes("member")));
                    db.writes().lists().rpush(bytes("list:" + i), List.of(bytes("value")));
                }

                MemoryUsageSnapshot expected = db.memoryUsage();
                db.armMemoryUsageIterationTrapsForTesting();
                try {
                    Assert.assertEquals(expected, db.memoryUsage());
                } finally {
                    db.disarmMemoryUsageIterationTrapsForTesting();
                }
            } finally {
                db.shutdown();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static yier.bubu.redis.bytes.BytesView view(String value) {
        byte[] bytes = bytes(value);
        return new yier.bubu.redis.bytes.BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }
}
