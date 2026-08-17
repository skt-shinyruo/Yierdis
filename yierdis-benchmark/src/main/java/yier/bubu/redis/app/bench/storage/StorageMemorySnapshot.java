package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.util.Objects;
import java.util.OptionalLong;

public record StorageMemorySnapshot(
        long heapEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes,
        long nativeDataLiveBytes,
        long nativeReclaimableBytes,
        long liveObjectCount,
        int pendingHashTableCount,
        int keyCount,
        OptionalLong rssBytes
) {
    public StorageMemorySnapshot {
        requireNonNegative(heapEstimatedBytes, "heapEstimatedBytes");
        requireNonNegative(nativeMetadataCommittedBytes, "nativeMetadataCommittedBytes");
        requireNonNegative(nativeDataCommittedBytes, "nativeDataCommittedBytes");
        requireNonNegative(nativeDataLiveBytes, "nativeDataLiveBytes");
        requireNonNegative(nativeReclaimableBytes, "nativeReclaimableBytes");
        requireNonNegative(liveObjectCount, "liveObjectCount");
        if (pendingHashTableCount < 0) {
            throw new IllegalArgumentException("pendingHashTableCount must be >= 0");
        }
        if (keyCount < 0) {
            throw new IllegalArgumentException("keyCount must be >= 0");
        }
        rssBytes = Objects.requireNonNull(rssBytes, "rssBytes");
        if (rssBytes.isPresent() && rssBytes.getAsLong() < 0L) {
            throw new IllegalArgumentException("rssBytes must be >= 0 when present");
        }
    }

    static StorageMemorySnapshot from(
            MemoryUsageSnapshot usage,
            YierdisMemoryStats stats,
            OptionalLong rssBytes
    ) {
        MemoryUsageSnapshot requiredUsage = Objects.requireNonNull(usage, "usage");
        YierdisMemoryStats requiredStats = Objects.requireNonNull(stats, "stats");
        return new StorageMemorySnapshot(
                requiredUsage.heapEstimatedBytes(),
                requiredUsage.nativeMetadataCommittedBytes(),
                requiredUsage.nativeDataCommittedBytes(),
                requiredUsage.nativeDataLiveBytes(),
                requiredUsage.nativeReclaimableBytes(),
                requiredStats.nativeLiveObjects(),
                requiredStats.pendingHashTableCount(),
                requiredStats.keyCount(),
                rssBytes
        );
    }

    public long accountedBytes() {
        return MemoryUsageSnapshot.addSaturating(
                heapEstimatedBytes,
                MemoryUsageSnapshot.addSaturating(
                        nativeMetadataCommittedBytes,
                        nativeDataCommittedBytes
                )
        );
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
