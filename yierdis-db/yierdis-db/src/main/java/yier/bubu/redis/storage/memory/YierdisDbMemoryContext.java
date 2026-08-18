package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.value.NativeBytesSlice;
import yier.bubu.redis.storage.memory.internal.value.NativeListEntryRef;
import yier.bubu.redis.storage.memory.internal.value.PinnedPoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.value.PreparedPoppedValueSequence;

/**
 * 集中封装 family 与维护任务共享的 stable-memory 能力，避免 allocator 细节重新渗入 mutation kernel。
 */
final class YierdisDbMemoryContext {
    private final MemoryLedger ledger;
    private final StableMemoryBackend stableMemoryBackend;

    YierdisDbMemoryContext(MemoryLedger ledger, StableMemoryBackend stableMemoryBackend) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
    }

    long nativeAllocationPeakAdditionalBytes(
            long ffmRegionGrowthBytes,
            long heapGrowthBytes,
            int... nativeAllocationSizes
    ) {
        return MutationMemoryEstimator.peakAdditionalBytes(
                stableMemoryBackend,
                ffmRegionGrowthBytes,
                heapGrowthBytes,
                nativeAllocationSizes
        );
    }

    long nativeAllocationScopeBookkeepingBytes(int expectedNativeAllocationCount) {
        return MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(
                stableMemoryBackend,
                expectedNativeAllocationCount
        );
    }

    NativeEpochScope beginScanEpoch() {
        return stableMemoryBackend.beginEpoch();
    }

    NativeBytesSlice keyBytesSlice(AllocatorKeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return new NativeBytesSlice(
                stableMemoryBackend,
                keyHandle.nativeHandle(),
                0,
                keyHandle.length()
        );
    }

    PinnedPoppedValueSequence capturePoppedValues(NativeListEntryRef[] entries) {
        return PinnedPoppedValueSequence.capture(stableMemoryBackend, entries);
    }

    PreparedPoppedValueSequence ownPoppedValues(NativeListEntryRef[] entries) {
        return PreparedPoppedValueSequence.owned(stableMemoryBackend, entries);
    }

    MemoryReclaimResult trimEmptyNativePages(MemoryPressureBudget budget) {
        return stableMemoryBackend.trimEmptyPages(Objects.requireNonNull(budget, "budget"));
    }

    MemoryUsageSnapshot nativeMemoryUsage() {
        return stableMemoryBackend.memoryUsage();
    }

    NativeAllocatorStats nativeAllocatorStats() {
        return stableMemoryBackend.stats();
    }

    long nativeLiveRegionCount() {
        return stableMemoryBackend.liveRegionCount();
    }

    void trimEmptyNativePagesAfterPreparedPreviewClose() {
        if (ledger.maxmemoryEnabled()) {
            trimEmptyNativePages(MemoryPressureBudget.UNLIMITED);
        }
    }
}
