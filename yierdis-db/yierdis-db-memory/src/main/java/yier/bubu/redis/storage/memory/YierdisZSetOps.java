package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.BulkStringMetrics;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequences;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue.ZAddResult;

import java.util.List;
import java.util.Objects;

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
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs, now);
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
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
                            addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    preparedAdd.stagedNonNativeGrowthBytes()
                            ),
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

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(keyLifecycle.nativeAllocator(), 0)
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
        int[] allocationSizes = zsetAllocationSizes(
                true,
                keyBytes,
                new int[0],
                scoreMemberPairs
        );
        long stagedHeapBytes = addSaturating(
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes(),
                zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                        null,
                        scoreMemberPairs,
                        allocationSizes.length
                )
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
        long stagedHeapBytes = zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                handle,
                scoreMemberPairs,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
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
    public MeasuredBulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return sequenceOf(out -> { });
        }
        ValueHandle handle = requireZSetHandle(record);
        return sequenceOf(out -> zsetRoot.zrangeWriteTo(handle, start, stop, withScores, out));
    }

    @Override
    public MeasuredBulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return sequenceOf(out -> { });
        }
        ValueHandle handle = requireZSetHandle(record);
        return sequenceOf(out -> zsetRoot.zrevrangeWriteTo(handle, start, stop, withScores, out));
    }

    @Override
    public MeasuredBulkStringSequence zrangeByScore(
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
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return sequenceOf(out -> { });
        }
        ValueHandle handle = requireZSetHandle(record);
        return sequenceOf(out -> zsetRoot.zrangeByScoreWriteTo(
                handle,
                min,
                minExclusive,
                max,
                maxExclusive,
                withScores,
                offset,
                count,
                out
        ));
    }

    @Override
    public MeasuredBulkStringSequence zrevrangeByScore(
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
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return sequenceOf(out -> { });
        }
        ValueHandle handle = requireZSetHandle(record);
        return sequenceOf(out -> zsetRoot.zrevrangeByScoreWriteTo(
                handle,
                min,
                minExclusive,
                max,
                maxExclusive,
                withScores,
                offset,
                count,
                out
        ));
    }

    @Override
    public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countExistingMembers(handle, members);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zrem(handle, members);
            }
        });
    }

    @Override
    public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countRemovalsByRank(handle, start, stop);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zremrangeByRank(handle, start, stop);
            }
        });
    }

    @Override
    public WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countRemovalsByScore(handle, min, minExclusive, max, maxExclusive);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zremrangeByScore(handle, min, minExclusive, max, maxExclusive);
            }
        });
    }

    private WriteResult<Long> removeInternal(byte[] keyBytes, ZSetRemoval removal) {
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current == null) {
                    return preparedNoEntry(WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }
                requireZSet(current);
                ValueHandle handle = requireZSetHandle(current);
                int removed = removal.count(handle);
                if (removed == 0) {
                    return preparedNoEntry(WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }

                MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                WriteResult<Long> result = WriteResult.of((long) removed, outcome);
                if (removed >= zsetRoot.size(handle)) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        return preparedDelete(currentEntry, current, result, outcome, ttlMutation);
                    } catch (RuntimeException | Error failure) {
                        try {
                            ttlMutation.abort();
                        } catch (RuntimeException | Error abortFailure) {
                            failure.addSuppressed(abortFailure);
                        }
                        throw failure;
                    }
                }

                EntryRecord next = zsetRecord(
                        currentEntry.keyHandle(),
                        handle,
                        current.expireAtMillis(),
                        current
                );
                long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                        - estimateRecordBytes(currentEntry.keyHandle(), current);
                if (deltaBytes > 0L) {
                    throw new IllegalStateException("prepared zset removal must not commit positive growth");
                }
                return new PreparedCallbackMutation<>(
                        result,
                        deltaBytes,
                        0L,
                        outcome,
                        () -> {
                            int actualRemoved = removal.remove(handle);
                            if (actualRemoved != removed) {
                                throw new IllegalStateException("prepared zset removal removed " + actualRemoved
                                        + " members instead of " + removed);
                            }
                            keyLifecycle.replaceEntry(currentEntry.entryHandle(), current, next);
                        },
                        null,
                        null
                );
            }
        });
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
        EntryRecord record = internals.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireZSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private void reclaimExpiredBeforeMutation(byte[] keyBytes, long nowMillis) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyHandle == null) {
            return;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record != null && keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            internals.reclaimExpired(keyHandle, record, nowMillis);
        }
    }

    private <T> PreparedEntryMutation<T> preparedDelete(
            CurrentEntry currentEntry,
            EntryRecord current,
            T result,
            MutationOutcome outcome,
            PreparedTtlMutation ttlMutation
    ) {
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                -estimateRecordBytes(currentEntry.keyHandle(), current),
                0L,
                outcome,
                currentEntry.entryHandle(),
                null,
                null,
                current,
                null,
                true,
                ttlMutation
        );
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

    private static MeasuredBulkStringSequence sequenceOf(BulkEmitter emitter) {
        Objects.requireNonNull(emitter, "emitter");
        BulkStringMetrics metrics = new BulkStringMetrics();
        emitter.emitTo(metrics);
        return MeasuredBulkStringSequences.of(
                metrics.count(),
                metrics.encodedElementBytes(),
                0L,
                emitter::emitTo
        );
    }

    @FunctionalInterface
    private interface BulkEmitter {
        void emitTo(BulkStringSink out);
    }

    private interface ZSetRemoval {
        int count(ValueHandle handle);

        int remove(ValueHandle handle);
    }
}
