package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PhysicalMemoryAccountingTest {
    @Test
    public void dbSnapshotCountsAllocatorCommittedMemoryOnce() {
        try (TestBackend runtime = TestBackend.open("physical-accounting")) {
            YierdisDb db = TestDbSupport.open(
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
                MemoryUsageSnapshot allocatorUsage = db.stableMemoryBackend().memoryUsage();
                NativeAllocatorStats allocatorStats = db.stableMemoryBackend().stats();

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
    public void deadlineOnlyTtlKeepsPhysicalMemoryAndSnapshotAccountingStable() {
        try (TestBackend runtime = TestBackend.open("physical-accounting-expiry")) {
            YierdisDb db = TestDbSupport.open(
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
                MemoryUsageSnapshot beforeTtl = db.memoryUsage();
                NativeAllocatorStats allocatorBeforeTtl = db.stableMemoryBackend().stats();
                long backendBytesBeforeTtl = runtime.usedBytes();

                Assert.assertTrue(db.writes().ttl().pexpire(view("key"), 60_000L).value());

                MemoryUsageSnapshot usage = db.memoryUsage();
                NativeAllocatorStats allocator = db.stableMemoryBackend().stats();
                long ttlMillis = db.reads().ttl().ttlMillis(view("key"));

                Assert.assertTrue(ttlMillis > 0L && ttlMillis <= 60_000L);
                Assert.assertEquals(1, db.memory().memoryStats().expireCount());
                Assert.assertEquals(backendBytesBeforeTtl, runtime.usedBytes());
                Assert.assertEquals(
                        allocatorBeforeTtl.metadataCommittedBytes(),
                        allocator.metadataCommittedBytes()
                );
                Assert.assertEquals(allocatorBeforeTtl.committedBytes(), allocator.committedBytes());
                Assert.assertEquals(
                        beforeTtl.nativeMetadataCommittedBytes(),
                        usage.nativeMetadataCommittedBytes()
                );
                Assert.assertEquals(beforeTtl.nativeDataCommittedBytes(), usage.nativeDataCommittedBytes());
                Assert.assertEquals(allocator.metadataCommittedBytes(), usage.nativeMetadataCommittedBytes());
                Assert.assertEquals(allocator.committedBytes(), usage.nativeDataCommittedBytes());
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
        try (TestBackend runtime = TestBackend.open("physical-accounting-counters")) {
            YierdisDb db = TestDbSupport.open(
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
