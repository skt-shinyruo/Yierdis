package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocator;

public final class MutationMemoryEstimator {
    private MutationMemoryEstimator() {
    }

    public static long nativeAllocationScopeBookkeepingBytes(
            NativeAllocator allocator,
            int expectedNativeAllocationCount
    ) {
        Objects.requireNonNull(allocator, "allocator");
        if (expectedNativeAllocationCount < 0) {
            throw new IllegalArgumentException("expectedNativeAllocationCount must be >= 0");
        }
        long bytes = allocator.estimateAllocationScopeBookkeepingBytes(expectedNativeAllocationCount);
        if (bytes < 0L) {
            throw new IllegalStateException("native allocation scope bookkeeping estimate must be non-negative");
        }
        return bytes;
    }

    public static long peakAdditionalBytes(
            NativeAllocator allocator,
            long ffmRegionGrowthBytes,
            long heapGrowthBytes,
            int... nativeAllocationSizes
    ) {
        Objects.requireNonNull(allocator, "allocator");
        requireNonNegative(ffmRegionGrowthBytes, "ffmRegionGrowthBytes");
        requireNonNegative(heapGrowthBytes, "heapGrowthBytes");
        int[] physicalSizes = physicalSizes(nativeAllocationSizes);
        long total = MemoryUsageSnapshot.addSaturating(
                nativeAllocationScopeBookkeepingBytes(allocator, physicalSizes.length),
                MemoryUsageSnapshot.addSaturating(ffmRegionGrowthBytes, heapGrowthBytes)
        );
        if (physicalSizes.length == 0) {
            return total;
        }
        return MemoryUsageSnapshot.addSaturating(
                total,
                allocator.estimateAdditionalGrowth(physicalSizes).effectiveBytes()
        );
    }

    private static int[] physicalSizes(int[] nativeAllocationSizes) {
        if (nativeAllocationSizes == null || nativeAllocationSizes.length == 0) {
            return new int[0];
        }
        int[] physical = new int[nativeAllocationSizes.length];
        for (int i = 0; i < nativeAllocationSizes.length; i++) {
            int size = nativeAllocationSizes[i];
            if (size < 0) {
                throw new IllegalArgumentException("nativeAllocationSizes must be non-negative");
            }
            physical[i] = Math.max(1, size);
        }
        return physical;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
