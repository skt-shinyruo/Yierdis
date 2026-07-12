package yier.bubu.redis.storage.memory.internal.ledger;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocator;

public final class MutationMemoryEstimator {
    private static final long NATIVE_ALLOCATION_SCOPE_BOOKKEEPING_BYTES = 4096L;

    private MutationMemoryEstimator() {
    }

    public static long nativeAllocationScopeBookkeepingBytes() {
        return NATIVE_ALLOCATION_SCOPE_BOOKKEEPING_BYTES;
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
        long total = MemoryUsageSnapshot.addSaturating(
                nativeAllocationScopeBookkeepingBytes(),
                MemoryUsageSnapshot.addSaturating(ffmRegionGrowthBytes, heapGrowthBytes)
        );
        int[] physicalSizes = physicalSizes(nativeAllocationSizes);
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
