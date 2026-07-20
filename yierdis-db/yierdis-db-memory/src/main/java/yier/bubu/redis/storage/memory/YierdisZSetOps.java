package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.BulkStringMetrics;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
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
            private ZSetRoot.AddPlan cachedAddPlan;
            private boolean addPlanInitialized;

            @Override
            public long upperBoundBytes() {
                EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
                if (existing == null) {
                    return newZSetUpperBound(keyBytes, addPlan(null));
                }
                KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
                if (keyLifecycle.isKeyExpired(keyHandle, now)) {
                    return newZSetUpperBound(keyBytes, addPlan(null));
                }
                if (existing.type() != ValueType.ZSET) {
                    return withScopeBookkeeping(0L);
                }
                ValueHandle handle = requireZSetHandle(existing);
                return existingZSetUpperBound(keyBytes, handle, scoreMemberPairs, addPlan(handle));
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
                ZSetRoot.PreparedAddResult preparedAdd = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }
                    ValueHandle sourceHandle = current == null ? null : requireZSetHandle(current);
                    preparedAdd = zsetRoot.prepareAdd(addPlan(sourceHandle));
                    ZAddResult added = preparedAdd.result();
                    MutationOutcome outcome = added.changedAny()
                            ? MutationOutcome.VALUE_CHANGED
                            : MutationOutcome.NONE;
                    if (preparedAdd.stableHandle() && !preparedAdd.changedAny()) {
                        preparedAdd.close();
                        preparedAdd = null;
                        return preparedNoEntry(WriteResult.of(0L, outcome), outcome);
                    }

                    boolean stableHandle = preparedAdd.stableHandle();
                    ValueHandle nextHandle = preparedAdd.handle();
                    replacement = stableHandle ? null : nextHandle;
                    EntryRecord next = zsetRecord(
                            targetKey,
                            nextHandle,
                            current == null ? -1L : current.expireAtMillis(),
                            current,
                            preparedAdd.targetEncoding()
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
                            !stableHandle,
                            stableHandle ? preparedAdd::releaseSuperseded : null,
                            PreparedTtlMutation.NONE,
                            stableHandle ? preparedAdd : null
                    );
                    if (stableHandle) {
                        prepared.beforeEntryPublish(preparedAdd::commit);
                    }
                    staged = null;
                    replacement = null;
                    preparedAdd = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (preparedAdd != null && preparedAdd.stableHandle()) {
                        try {
                            preparedAdd.close();
                        } catch (RuntimeException | Error closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                    abortStaged(staged, replacement, failure);
                    throw failure;
                }
            }

            private ZSetRoot.AddPlan addPlan(ValueHandle source) {
                if (!addPlanInitialized) {
                    cachedAddPlan = zsetRoot.planAdd(source, scoreMemberPairs);
                    addPlanInitialized = true;
                    return cachedAddPlan;
                }
                if (!Objects.equals(cachedAddPlan.source(), source)) {
                    throw new IllegalStateException("prepared ZADD source changed after admission");
                }
                return cachedAddPlan;
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

    private int[] zsetAllocationSizes(
            boolean includeKeyAndEntry,
            boolean includeRoot,
            byte[] keyBytes,
            int[] replacementAllocationSizes
    ) {
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int payloadCount = replacementAllocationSizes == null ? 0 : replacementAllocationSizes.length;
        int[] sizes = new int[metadataCount + (includeRoot ? 1 : 0) + payloadCount];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = ENTRY_RECORD_NATIVE_BYTES;
        }
        if (includeRoot) {
            sizes[next++] = ZSET_ROOT_NATIVE_BYTES;
        }
        if (replacementAllocationSizes != null) {
            for (int size : replacementAllocationSizes) {
                sizes[next++] = Math.max(1, size);
            }
        }
        return sizes;
    }

    private long newZSetUpperBound(byte[] keyBytes, ZSetRoot.AddPlan addPlan) {
        int[] allocationSizes = zsetAllocationSizes(
                true,
                true,
                keyBytes,
                zsetRoot.preparedAddNativeAllocationSizes(addPlan)
        );
        long stagedHeapBytes = addSaturating(
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes(),
                zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                        addPlan,
                        allocationSizes.length
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(
                keyBytes.length,
                addPlan.scoreMemberPairs()
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long existingZSetUpperBound(
            byte[] keyBytes,
            ValueHandle handle,
            List<byte[]> scoreMemberPairs,
            ZSetRoot.AddPlan addPlan
    ) {
        int[] allocationSizes = zsetAllocationSizes(
                false,
                !addPlan.stableHandle(),
                keyBytes,
                zsetRoot.preparedAddNativeAllocationSizes(addPlan)
        );
        long stagedHeapBytes = zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                addPlan,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = addSaturating(
                YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(0, scoreMemberPairs),
                zsetRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
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
    public CollectionScanWindow zscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count) {
        internals.checkThread();
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
        }
        return zsetRoot.zscan(
                requireZSetHandle(record),
                cursor == null ? ScanCursorV2.start() : cursor,
                globPattern,
                count
        );
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
        return zsetRecord(
                keyHandle,
                handle,
                expireAtMillis,
                previous,
                zsetRoot.encoding(handle)
        );
    }

    private EntryRecord zsetRecord(
            KeyHandle keyHandle,
            ValueHandle handle,
            long expireAtMillis,
            EntryRecord previous,
            ValueEncoding encoding
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.ZSET,
                encoding,
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
