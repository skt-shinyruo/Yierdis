package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.nio.charset.StandardCharsets;

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
                db.strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);

                MemoryUsageSnapshot usage = db.memoryUsage();
                MemoryUsageSnapshot allocatorUsage = KeyLifecycleTestAccess.backend(db).memoryUsage();
                NativeAllocatorStats allocatorStats = KeyLifecycleTestAccess.backend(db).stats();

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
                db.strings().setString(bytes("key"), bytes("value"), SetMode.NORMAL, null);
                MemoryUsageSnapshot beforeTtl = db.memoryUsage();
                NativeAllocatorStats allocatorBeforeTtl = KeyLifecycleTestAccess.backend(db).stats();
                long backendBytesBeforeTtl = runtime.usedBytes();

                Assert.assertTrue(db.ttl().pexpire(view("key"), 60_000L).value());

                MemoryUsageSnapshot usage = db.memoryUsage();
                NativeAllocatorStats allocator = KeyLifecycleTestAccess.backend(db).stats();
                long ttlMillis = db.ttl().ttlMillis(view("key"));

                Assert.assertTrue(ttlMillis > 0L && ttlMillis <= 60_000L);
                Assert.assertEquals(1, db.memoryStats().expireCount());
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
