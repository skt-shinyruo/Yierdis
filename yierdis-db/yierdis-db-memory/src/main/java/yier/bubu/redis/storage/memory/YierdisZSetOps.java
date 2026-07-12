package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.EntryMutationResult;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue.ZAddResult;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class YierdisZSetOps implements ZSetReadOps, ZSetWriteOps {
    private static final int ENTRY_RECORD_NATIVE_BYTES = 56;
    private static final int ZSET_ROOT_NATIVE_BYTES = Long.BYTES;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ZSetRoot zsetRoot;

    YierdisZSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.zsetRoot = Objects.requireNonNull(keyLifecycle.zsetRoot(), "zsetRoot");
    }

    @Override
    public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        internals.checkThread();
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
        }
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs, now);
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }
                if (current != null) {
                    requireZSet(current);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }
                    ZSetRoot.PreparedAddResult preparedAdd = zsetRoot.prepareAdd(
                            current == null ? null : requireZSetHandle(current),
                            scoreMemberPairs
                    );
                    replacement = preparedAdd.handle();
                    ZAddResult added = preparedAdd.result();
                    MutationOutcome outcome = added.changedAny()
                            ? MutationOutcome.VALUE_CHANGED
                            : MutationOutcome.NONE;
                    if (replacement == null) {
                        return preparedNoEntry(WriteResult.of(0L, outcome), outcome);
                    }

                    EntryRecord next = zsetRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of((long) added.added(), outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            true,
                            PreparedTtlMutation.NONE
                    );
                    staged = null;
                    replacement = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, replacement, failure);
                    throw failure;
                }
            }
        });
    }

    private CurrentEntry currentEntry(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = entryHandle == null ? null : keyLifecycle.keyHandle(keyBytes);
        return new CurrentEntry(entryHandle, keyHandle, record);
    }

    private StagedEntry stageNewEntry(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryTable().reserve();
        NativeKeyDirectory.StagedInsert stagedKey = null;
        try {
            stagedKey = keyLifecycle.keyDirectory().stageInsert(keyBytes);
            return new StagedEntry(entryHandle, stagedKey);
        } catch (RuntimeException | Error failure) {
            try {
                keyLifecycle.entryTable().release(entryHandle);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            if (stagedKey != null) {
                try {
                    stagedKey.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private <T> PreparedEntryMutation<T> preparedNoEntry(T result, MutationOutcome outcome) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                0L,
                0L,
                outcome,
                null,
                null,
                null,
                null,
                null,
                false,
                PreparedTtlMutation.NONE
        );
    }

    private void abortStaged(StagedEntry staged, ValueHandle replacement, Throwable failure) {
        if (replacement != null) {
            try {
                zsetRoot.release(replacement);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        if (staged != null) {
            try {
                staged.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            try {
                keyLifecycle.entryTable().release(staged.entryHandle());
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
    }

    private long nativePeak(long heapGrowthBytes, int... nativeAllocationSizes) {
        return MutationMemoryEstimator.peakAdditionalBytes(
                keyLifecycle.nativeAllocator(),
                0L,
                Math.max(0L, heapGrowthBytes),
                nativeAllocationSizes
        );
    }

    private static long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes()
        );
    }

    private static int appendPayloadSizes(int[] sizes, int offset, int[] payloadSizes) {
        if (payloadSizes == null || payloadSizes.length == 0) {
            return offset;
        }
        int next = offset;
        for (int size : payloadSizes) {
            sizes[next++] = Math.max(1, size);
        }
        return next;
    }

    private static int appendMemberPayloadSizes(int[] sizes, int offset, List<byte[]> scoreMemberPairs) {
        if (scoreMemberPairs == null || scoreMemberPairs.isEmpty()) {
            return offset;
        }
        int next = offset;
        for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
            byte[] member = scoreMemberPairs.get(i);
            if (member != null) {
                sizes[next++] = Math.max(1, member.length);
            }
        }
        return next;
    }

    private static int nonNullMemberCount(List<byte[]> scoreMemberPairs) {
        if (scoreMemberPairs == null || scoreMemberPairs.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
            if (scoreMemberPairs.get(i) != null) {
                count++;
            }
        }
        return count;
    }

    private int[] zsetAllocationSizes(
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int[] existingPayloadSizes,
            List<byte[]> scoreMemberPairs
    ) {
        int existingCount = existingPayloadSizes == null ? 0 : existingPayloadSizes.length;
        int newCount = nonNullMemberCount(scoreMemberPairs);
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int[] sizes = new int[metadataCount + 1 + existingCount * 3 + newCount * 3];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = ENTRY_RECORD_NATIVE_BYTES;
        }
        sizes[next++] = ZSET_ROOT_NATIVE_BYTES;
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendMemberPayloadSizes(sizes, next, scoreMemberPairs);
        next = appendMemberPayloadSizes(sizes, next, scoreMemberPairs);
        next = appendMemberPayloadSizes(sizes, next, scoreMemberPairs);
        if (next != sizes.length) {
            throw new IllegalStateException("zset allocation size estimate count mismatch");
        }
        return sizes;
    }

    private long newZSetUpperBound(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        long stagedHeapBytes = keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes();
        int[] allocationSizes = zsetAllocationSizes(
                true,
                keyBytes,
                new int[0],
                scoreMemberPairs
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(
                keyBytes.length,
                scoreMemberPairs
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long existingZSetUpperBound(
            byte[] keyBytes,
            EntryRecord existing,
            List<byte[]> scoreMemberPairs
    ) {
        ValueHandle handle = requireZSetHandle(existing);
        int[] allocationSizes = zsetAllocationSizes(
                false,
                keyBytes,
                zsetRoot.nativePayloadSizes(handle),
                scoreMemberPairs
        );
        long nativeUpperBound = nativePeak(0L, allocationSizes);
        long logicalUpperBound = addSaturating(
                YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(0, scoreMemberPairs),
                zsetRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long estimateZSetWriteUpperBoundForMutation(
            byte[] keyBytes,
            List<byte[]> scoreMemberPairs,
            long nowMillis
    ) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newZSetUpperBound(keyBytes, scoreMemberPairs);
        }
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newZSetUpperBound(keyBytes, scoreMemberPairs);
        }
        if (existing.type() != ValueType.ZSET) {
            return withScopeBookkeeping(0L);
        }
        return existingZSetUpperBound(keyBytes, existing, scoreMemberPairs);
    }

    private record CurrentEntry(
            EntryHandle entryHandle,
            KeyHandle keyHandle,
            EntryRecord record
    ) {
    }

    private record StagedEntry(
            EntryHandle entryHandle,
            NativeKeyDirectory.StagedInsert stagedKey
    ) implements AutoCloseable {
        private KeyHandle keyHandle() {
            return stagedKey.keyHandle();
        }

        private long stagedHeapBytes() {
            return stagedKey.stagedHeapBytes();
        }

        @Override
        public void close() {
            stagedKey.close();
        }
    }

    @Override
    public BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        return sequenceOf(
                () -> zrangeCount(keyBytes, start, stop, withScores),
                out -> zrangeWriteTo(keyBytes, start, stop, withScores, out)
        );
    }

    @Override
    public BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        return sequenceOf(
                () -> zrevrangeCount(keyBytes, start, stop, withScores),
                out -> zrevrangeWriteTo(keyBytes, start, stop, withScores, out)
        );
    }

    @Override
    public BulkStringSequence zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        internals.checkThread();
        return sequenceOf(
                () -> zrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                out -> zrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
        );
    }

    @Override
    public BulkStringSequence zrevrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        internals.checkThread();
        return sequenceOf(
                () -> zrevrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                out -> zrevrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
        );
    }

    @Override
    public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return removeInternal(keyBytes, now, handle -> zsetRoot.zrem(handle, members));
    }

    @Override
    public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return removeInternal(keyBytes, now, handle -> zsetRoot.zremrangeByRank(handle, start, stop));
    }

    @Override
    public WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return removeInternal(keyBytes, now, handle -> zsetRoot.zremrangeByScore(handle, min, minExclusive, max, maxExclusive));
    }

    private WriteResult<Long> removeInternal(byte[] keyBytes, long now, ZSetRemoval removal) {
        return internals.executeMutation(new YierdisDbMutationExecutor.LegacyMutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> mutation =
                        keyLifecycle.computeIfPresentWithHandleResult(keyBytes, (k, oldRecord) -> {
                            EntryRecord current = oldRecord;
                            long oldEstimate = estimateRecordBytes(k, current);
                            long deltaBytes = 0L;
                            if (keyLifecycle.isKeyExpired(k, now)) {
                                keyLifecycle.removeExpire(k);
                                deltaBytes -= oldEstimate;
                                return mutationResult(null, WriteResult.of(0L, MutationOutcome.NONE), deltaBytes);
                            }
                            requireZSet(current);
                            ValueHandle handle = requireZSetHandle(current);
                            int removed = removal.remove(handle);
                            MutationOutcome outcome = removed > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                            if (zsetRoot.size(handle) == 0) {
                                keyLifecycle.removeExpire(k);
                                deltaBytes -= oldEstimate;
                                return mutationResult(null, WriteResult.of((long) removed, outcome), deltaBytes);
                            }
                            EntryRecord next = zsetRecord(k, handle, current.expireAtMillis(), current);
                            deltaBytes -= oldEstimate;
                            deltaBytes += estimateRecordBytes(k, next);
                            return mutationResult(next, WriteResult.of((long) removed, outcome), deltaBytes);
                        });
                return mutation == null
                        ? YierdisDbMutationExecutor.MutationResult.of(WriteResult.of(0L, MutationOutcome.NONE), 0L)
                        : mutation;
            }
        });
    }

    private int zrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrangeCount(requireZSetHandle(record), start, stop, withScores);
    }

    private void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrangeWriteTo(requireZSetHandle(record), start, stop, withScores, out);
    }

    private int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrevrangeCount(requireZSetHandle(record), start, stop, withScores);
    }

    private void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrevrangeWriteTo(requireZSetHandle(record), start, stop, withScores, out);
    }

    private int zrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrangeByScoreCount(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrangeByScoreWriteTo(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private int zrevrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrevrangeByScoreCount(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrevrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrevrangeByScoreWriteTo(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type() != ValueType.ZSET) {
            return 0L;
        }
        long upperBound = YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(0, scoreMemberPairs);
        ValueHandle handle = existing.valueHandle();
        if (zsetRoot.encoding(handle) == ValueEncoding.ZSET_PACKED) {
            upperBound = addSaturating(upperBound, zsetRoot.estimatedBytes(handle));
        }
        return upperBound;
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private EntryRecord liveZSetRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireZSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord zsetRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.ZSET,
                zsetRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireZSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!zsetRoot.contains(handle)) {
            throw new IllegalStateException("native zset value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireZSet(EntryRecord record) {
        if (record.type() != ValueType.ZSET) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
    }

    private static BulkStringSequence sequenceOf(IntSupplier countSupplier, BulkEmitter emitter) {
        Objects.requireNonNull(countSupplier, "countSupplier");
        Objects.requireNonNull(emitter, "emitter");
        return new BulkStringSequence() {
            @Override
            public int count() {
                int count = countSupplier.getAsInt();
                return Math.max(count, 0);
            }

            @Override
            public void emitTo(BulkStringSink out) {
                emitter.emitTo(out);
            }
        };
    }

    private static <T> EntryMutationResult<YierdisDbMutationExecutor.MutationResult<T>> mutationResult(
            EntryRecord record,
            T value,
            long deltaBytes
    ) {
        return EntryMutationResult.of(record, YierdisDbMutationExecutor.MutationResult.of(value, deltaBytes));
    }

    @FunctionalInterface
    private interface BulkEmitter {
        void emitTo(BulkStringSink out);
    }

    @FunctionalInterface
    private interface ZSetRemoval {
        int remove(ValueHandle handle);
    }
}
