package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringMetrics;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequences;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.List;
import java.util.Objects;

public final class YierdisSetOps implements SetReadOps, SetWriteOps {
    private static final int ENTRY_RECORD_NATIVE_BYTES = 56;
    private static final int SET_ROOT_NATIVE_BYTES = Long.BYTES;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final SetRoot setRoot;

    YierdisSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.setRoot = Objects.requireNonNull(keyLifecycle.setRoot(), "setRoot");
    }

    @Override
    public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimateSetAddUpperBound(keyBytes, members, now);
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null) {
                    requireSet(current);
                    int expectedAdditions = setRoot.countAdditions(requireSetHandle(current), members);
                    if (expectedAdditions == 0) {
                        return preparedNoEntry(WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                    }
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                long rootHeapBefore = setRoot.retainedHeapBytes();
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    replacement = setRoot.create();
                    if (current != null) {
                        setRoot.sadd(replacement, setRoot.members(requireSetHandle(current)));
                    }
                    int added = setRoot.sadd(replacement, members);
                    MutationOutcome outcome = added > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                    if (added == 0) {
                        WriteResult<Long> result = WriteResult.of(0L, outcome);
                        if (replacement != null) {
                            setRoot.release(replacement);
                            replacement = null;
                        }
                        if (staged != null) {
                            staged.close();
                            keyLifecycle.entryTable().release(staged.entryHandle());
                            staged = null;
                        }
                        return preparedNoEntry(result, outcome);
                    }
                    EntryRecord next = setRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of((long) added, outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            MemoryUsageSnapshot.addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    setRoot.positiveRetainedHeapGrowthBytes(rootHeapBefore)
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
                    abortStaged(staged, replacement, PreparedTtlMutation.NONE, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
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
                requireSet(current);
                ValueHandle handle = requireSetHandle(current);
                int removed = setRoot.countExistingMembers(handle, members);
                if (removed == 0) {
                    return preparedNoEntry(WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }

                MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                WriteResult<Long> result = WriteResult.of((long) removed, outcome);
                if (removed >= setRoot.size(handle)) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        return preparedDelete(currentEntry, current, result, outcome, true, ttlMutation);
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                EntryRecord next = setRecord(currentEntry.keyHandle(), handle, current.expireAtMillis(), current);
                long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                        - estimateRecordBytes(currentEntry.keyHandle(), current);
                return new PreparedCallbackMutation<>(
                        result,
                        deltaBytes,
                        0L,
                        outcome,
                        () -> {
                            int actualRemoved = setRoot.srem(handle, members);
                            if (actualRemoved != removed) {
                                throw new IllegalStateException("prepared SREM removed " + actualRemoved
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

    @Override
    public MeasuredBulkStringSequence smembers(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return sequenceOf(out -> { });
        }
        ValueHandle handle = requireSetHandle(record);
        return sequenceOf(out -> setRoot.membersInto(handle, out));
    }

    @Override
    public boolean sismember(byte[] keyBytes, byte[] member) {
        internals.checkThread();
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return false;
        }
        return setRoot.contains(requireSetHandle(record), member);
    }

    @Override
    public long scard(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return setRoot.size(requireSetHandle(record));
    }

    private long estimateSetAddUpperBound(byte[] keyBytes, List<byte[]> members, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newSetUpperBound(keyBytes, members);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newSetUpperBound(keyBytes, members);
        }
        if (existing.type() != ValueType.SET) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireSetHandle(existing);
        int[] allocationSizes = setAllocationSizes(
                false,
                keyBytes,
                setRoot.nativePayloadSizes(handle),
                members
        );
        long stagedHeapBytes = setRoot.estimatedPreparedAddHeapGrowthBytes(
                handle,
                members,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = MemoryUsageSnapshot.addSaturating(
                YierdisDbMemoryEstimator.estimateSetWriteUpperBound(0, members),
                setRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long newSetUpperBound(byte[] keyBytes, List<byte[]> members) {
        int[] allocationSizes = setAllocationSizes(
                true,
                keyBytes,
                new int[0],
                members
        );
        long stagedHeapBytes = MemoryUsageSnapshot.addSaturating(
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes(),
                setRoot.estimatedPreparedAddHeapGrowthBytes(
                        null,
                        members,
                        allocationSizes.length
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = YierdisDbMemoryEstimator.estimateSetWriteUpperBound(keyBytes.length, members);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] setAllocationSizes(
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int[] existingPayloadSizes,
            List<byte[]> members
    ) {
        int existingPayloadCount = existingPayloadSizes == null ? 0 : existingPayloadSizes.length;
        int newPayloadCount = nonNullValueCount(members);
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int[] sizes = new int[metadataCount + 1 + existingPayloadCount + newPayloadCount];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = ENTRY_RECORD_NATIVE_BYTES;
        }
        sizes[next++] = SET_ROOT_NATIVE_BYTES;
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendValuePayloadSizes(sizes, next, members);
        if (next != sizes.length) {
            throw new IllegalStateException("set allocation size estimate count mismatch");
        }
        return sizes;
    }

    private EntryRecord liveSetRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = internals.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord setRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.SET,
                setRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!setRoot.contains(handle)) {
            throw new IllegalStateException("native set value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireSet(EntryRecord record) {
        if (record.type() != ValueType.SET) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
    }

    private CurrentEntry currentEntry(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = entryHandle == null ? null : keyLifecycle.keyHandle(keyBytes);
        return new CurrentEntry(entryHandle, keyHandle, record);
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

    private <T> PreparedEntryMutation<T> preparedDelete(
            CurrentEntry currentEntry,
            EntryRecord current,
            T result,
            MutationOutcome outcome,
            boolean releaseOldValue,
            PreparedTtlMutation ttlMutation
    ) {
        long deltaBytes = -estimateRecordBytes(currentEntry.keyHandle(), current);
        return new PreparedEntryMutation<>(
                keyLifecycle,
                result,
                deltaBytes,
                0L,
                outcome,
                currentEntry.entryHandle(),
                null,
                null,
                current,
                null,
                releaseOldValue,
                ttlMutation
        );
    }

    private void abortStaged(
            StagedEntry staged,
            ValueHandle replacement,
            PreparedTtlMutation ttlMutation,
            Throwable failure
    ) {
        abortTtl(ttlMutation, failure);
        if (replacement != null) {
            try {
                setRoot.release(replacement);
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

    private static void abortTtl(PreparedTtlMutation ttlMutation, Throwable failure) {
        if (ttlMutation == null) {
            return;
        }
        try {
            ttlMutation.abort();
        } catch (RuntimeException | Error abortFailure) {
            failure.addSuppressed(abortFailure);
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

    private static int nonNullValueCount(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (byte[] value : values) {
            if (value != null) {
                count++;
            }
        }
        return count;
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

    private static int appendValuePayloadSizes(int[] sizes, int offset, List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return offset;
        }
        int next = offset;
        for (byte[] value : values) {
            if (value != null) {
                sizes[next++] = Math.max(1, value.length);
            }
        }
        return next;
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
            Throwable failure = null;
            try {
                stagedKey.close();
            } catch (RuntimeException | Error e) {
                failure = e;
            }
            if (failure != null) {
                rethrow(failure);
            }
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }
}
