package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class YierdisDbMemoryEstimatorTest {
    @Test
    public void estimatesStringEntryBytesFromNativeKeyHandle() {
        withNativeKey("abc", key -> {
            YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator();
            EntryRecord record = record(ValueEncoding.STRING_RAW, DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE);

            long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;

            Assert.assertEquals(expected, estimator.estimateEntryBytes(key, record));
        });
    }

    @Test
    public void estimatesRawStringEntryBytesWithoutAddingNativeKeyBytes() {
        withNativeKey("abc", key -> {
            YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator();
            EntryRecord record = record(ValueEncoding.STRING_RAW, DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE);

            long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;

            Assert.assertEquals(expected, estimator.estimateEntryBytes(key, record));
        });
    }

    @Test
    public void estimatesIntegerEncodedStringPayloadAsLongBytes() {
        withNativeKey("n", key -> {
            YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator();
            EntryRecord record = record(ValueEncoding.STRING_INT, DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + Long.BYTES);

            long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + Long.BYTES;

            Assert.assertEquals(expected, estimator.estimateEntryBytes(key, record));
        });
    }

    @Test
    public void estimatesWriteUpperBoundsAndByteSums() {
        Assert.assertEquals(
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 3L + 5L,
                YierdisDbMemoryEstimator.estimateStringWriteUpperBound(3, 5)
        );
        Assert.assertEquals(6L, YierdisDbMemoryEstimator.sumByteLengths(List.of(b("a"), b("bc"), b("def"))));
        Assert.assertEquals(4L, YierdisDbMemoryEstimator.sumZSetMemberByteLengths(List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    @Test
    public void estimatesSetAndZSetCreationUpperBounds() {
        long setExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + (2L * 3L) + (2L * 32L);
        long zsetExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + (4L * 4L) + (2L * 96L);

        Assert.assertEquals(setExpected, YierdisDbMemoryEstimator.estimateSetWriteUpperBound(1, List.of(b("a"), b("bc"))));
        Assert.assertEquals(zsetExpected, YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(1, List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    @Test
    public void nativePeakUsesTheAllocatorScopeBookkeepingEstimate() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-estimator");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 2_048)) {
            allocator.bindToCurrentThread();
            NativeHandle anchor = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            try {
                int[] allocations = new int[1_024];
                Arrays.fill(allocations, 32);
                long scopeBookkeeping = allocator.estimateAllocationScopeBookkeepingBytes(allocations.length);

                Assert.assertTrue(scopeBookkeeping > 4_096L);
                Assert.assertEquals(
                        scopeBookkeeping,
                        MutationMemoryEstimator.peakAdditionalBytes(allocator, 0L, 0L, allocations)
                );
            } finally {
                allocator.free(anchor);
            }
        }
    }

    @Test
    public void nativePeakMatchesTheKnownAllocationScopePeakAfterPageTrim() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-peak-after-trim");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 2_048)) {
            allocator.bindToCurrentThread();
            int[] allocations = {1, 56, 40_000};
            NativeHandle key = allocator.allocate(NativeObjectKind.KEY_BYTES, allocations[0]);
            NativeHandle entry = allocator.allocate(NativeObjectKind.ENTRY_RECORD, allocations[1]);
            NativeHandle value = allocator.allocate(NativeObjectKind.STRING_BYTES, allocations[2]);
            allocator.free(key);
            allocator.free(entry);
            allocator.free(value);
            allocator.trimEmptyPages(MemoryPressureBudget.unlimited());

            long expectedPeak = MutationMemoryEstimator.peakAdditionalBytes(allocator, 0L, 0L, allocations);
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                allocator.allocate(NativeObjectKind.KEY_BYTES, allocations[0]);
                allocator.allocate(NativeObjectKind.ENTRY_RECORD, allocations[1]);
                allocator.allocate(NativeObjectKind.STRING_BYTES, allocations[2]);

                Assert.assertTrue(expectedPeak >= scope.growth().effectiveBytes());
            }
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static EntryRecord record(ValueEncoding encoding, long estimatedBytes) {
        return new EntryRecord(
                1L,
                valueHandle(1L),
                1,
                ValueType.STRING,
                encoding,
                0,
                -1L,
                estimatedBytes,
                0L
        );
    }

    private static ValueHandle valueHandle(long slotId) {
        NativeObjectKind kind = NativeObjectKind.STRING_BYTES;
        return ValueHandle.fromNativeHandle(NativeHandle.of(kind.domain(), kind, slotId, 1, 0));
    }

    private static void withNativeKey(String value, KeyAssertion assertion) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("memory-estimator-key");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             NativeKeyDirectory directory = new NativeKeyDirectory(allocator)) {
            EntryHandle entry = EntryHandle.fromNativeHandle(allocator.allocate(NativeObjectKind.ENTRY_RECORD, 32));
            try {
                byte[] keyBytes = b(value);
                directory.compute(keyBytes, (ignored, old) -> entry);
                assertion.accept(directory.getKeyHandle(keyBytes));
            } finally {
                allocator.free(entry.nativeHandle());
            }
        }
    }

    @FunctionalInterface
    private interface KeyAssertion {
        void accept(KeyHandle keyHandle);
    }
}
