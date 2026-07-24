package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteMapSources;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.SemanticResultSupport;

import java.util.List;
import java.util.Objects;

public final class YierdisHashOps implements HashReadOps, HashWriteOps {
    private static final long HASH_PAIR_OVERHEAD_BYTES_ESTIMATE = 256L;
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashRoot hashRoot;

    YierdisHashOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.hashRoot = Objects.requireNonNull(keyLifecycle.hashRoot(), "hashRoot");
    }

    @Override
    public WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        if (fieldValuePairs.size() % 2 != 0) {
            throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
        }
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            private HashRoot.SetPlan cachedSetPlan;
            private boolean setPlanInitialized;

            @Override
            public long upperBoundBytes() {
                EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
                if (existing == null) {
                    return newHashUpperBound(keyBytes, setPlan(null));
                }
                KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
                if (keyLifecycle.isKeyExpired(keyHandle, now)) {
                    return newHashUpperBound(keyBytes, setPlan(null));
                }
                if (existing.type() != ValueType.HASH) {
                    return withScopeBookkeeping(0L);
                }
                ValueHandle handle = requireHashHandle(existing);
                return existingHashUpperBound(keyBytes, fieldValuePairs, handle, setPlan(handle));
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null) {
                    requireHash(current);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                HashRoot.PreparedSetResult preparedSet = null;
                long rootHeapBefore = hashRoot.retainedHeapBytes();
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    ValueHandle sourceHandle = current == null ? null : requireHashHandle(current);
                    preparedSet = hashRoot.prepareSet(setPlan(sourceHandle));
                    if (!preparedSet.changedAny()) {
                        preparedSet.close();
                        preparedSet = null;
                        return preparedNoEntry(
                                WriteResult.of((long) setPlan(sourceHandle).added(), MutationOutcome.NONE),
                                MutationOutcome.NONE
                        );
                    }
                    boolean stableHandle = preparedSet.stableHandle();
                    ValueHandle nextHandle = preparedSet.replacementHandle();
                    replacement = stableHandle ? null : nextHandle;
                    EntryRecord next = hashRecord(
                            targetKey,
                            nextHandle,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of(
                            (long) preparedSet.added(),
                            MutationOutcome.VALUE_CHANGED
                    );
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            MemoryUsageSnapshot.addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    MemoryUsageSnapshot.addSaturating(
                                            hashRoot.positiveRetainedHeapGrowthBytes(rootHeapBefore),
                                            preparedSet.stagedHeapBytes()
                                    )
                            ),
                            MutationOutcome.VALUE_CHANGED,
                            currentEntry.entryHandle(),
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            current,
                            next,
                            !stableHandle,
                            stableHandle ? preparedSet::releaseSuperseded : null,
                            PreparedTtlMutation.NONE,
                            stableHandle ? preparedSet : null
                    );
                    if (stableHandle) {
                        prepared.beforeEntryPublish(preparedSet::commit);
                    }
                    staged = null;
                    replacement = null;
                    preparedSet = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (preparedSet != null && preparedSet.stableHandle()) {
                        try {
                            preparedSet.close();
                        } catch (RuntimeException | Error closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                    abortStaged(staged, replacement, PreparedTtlMutation.NONE, failure);
                    throw failure;
                }
            }

            private HashRoot.SetPlan setPlan(ValueHandle source) {
                if (!setPlanInitialized) {
                    cachedSetPlan = hashRoot.planSet(source, fieldValuePairs);
                    setPlanInitialized = true;
                    return cachedSetPlan;
                }
                if (!Objects.equals(cachedSetPlan.source(), source)) {
                    throw new IllegalStateException("prepared HSET source changed after admission");
                }
                return cachedSetPlan;
            }
        });
    }

    @Override
    public ByteValue hget(byte[] keyBytes, byte[] fieldBytes) {
        internals.checkThread();
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return ByteValue.nullValue();
        }
        return hashRoot.hgetValue(requireHashHandle(record), fieldBytes);
    }

    @Override
    public ByteMapSource hgetall(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return ByteMapSources.empty();
        }
        ValueHandle handle = requireHashHandle(record);
        return ByteMapSources.of(
                hashRoot.size(handle),
                0L,
                out -> hashRoot.hgetallPairsInto(handle, SemanticResultSupport.lengthSink(out)),
                out -> hashRoot.hgetallPairsInto(handle, out)
        );
    }

    @Override
    public long hlen(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return hashRoot.size(requireHashHandle(record));
    }

    @Override
    public CollectionScanWindow hscan(
            byte[] keyBytes,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count,
            boolean noValues
    ) {
        internals.checkThread();
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
        }
        return hashRoot.hscan(
                requireHashHandle(record),
                cursor == null ? ScanCursorV2.start() : cursor,
                globPattern,
                count,
                noValues
        );
    }

    @Override
    public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
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
                requireHash(current);
                ValueHandle handle = requireHashHandle(current);
                int removed = hashRoot.countExistingFields(handle, fields);
                if (removed == 0) {
                    return preparedNoEntry(WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }

                MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                WriteResult<Long> result = WriteResult.of((long) removed, outcome);
                if (removed >= hashRoot.size(handle)) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        return preparedDelete(currentEntry, current, result, outcome, true, ttlMutation);
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                EntryRecord next = hashRecord(currentEntry.keyHandle(), handle, current.expireAtMillis(), current);
                long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                        - estimateRecordBytes(currentEntry.keyHandle(), current);
                return new PreparedCallbackMutation<>(
                        result,
                        deltaBytes,
                        0L,
                        outcome,
                        () -> {
                            int actualRemoved = hashRoot.hdel(handle, fields);
                            if (actualRemoved != removed) {
                                throw new IllegalStateException("prepared HDEL removed " + actualRemoved
                                        + " fields instead of " + removed);
                            }
                            keyLifecycle.replaceEntry(currentEntry.entryHandle(), current, next);
                        },
                        null,
                        null
                );
            }
        });
    }

    private long existingHashUpperBound(
            byte[] keyBytes,
            List<byte[]> fieldValuePairs,
            ValueHandle handle,
            HashRoot.SetPlan setPlan
    ) {
        int[] allocationSizes = hashAllocationSizes(
                false,
                !setPlan.stableHandle(),
                keyBytes,
                hashRoot.preparedSetNativeAllocationSizes(setPlan)
        );
        long stagedHeapBytes = hashRoot.estimatedPreparedSetHeapGrowthBytes(
                setPlan,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = MemoryUsageSnapshot.addSaturating(
                estimateHashWriteUpperBound(0, fieldValuePairs),
                hashRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long newHashUpperBound(byte[] keyBytes, HashRoot.SetPlan setPlan) {
        int[] allocationSizes = hashAllocationSizes(
                true,
                true,
                keyBytes,
                hashRoot.preparedSetNativeAllocationSizes(setPlan)
        );
        long stagedHeapBytes = MemoryUsageSnapshot.addSaturating(
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes(),
                hashRoot.estimatedPreparedSetHeapGrowthBytes(
                        setPlan,
                        allocationSizes.length
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = estimateHashWriteUpperBound(keyBytes.length, setPlan.replacementPairs());
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] hashAllocationSizes(
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
            sizes[next++] = NativeStorageLayout.ENTRY_RECORD_BYTES;
        }
        if (includeRoot) {
            sizes[next++] = NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES;
        }
        if (replacementAllocationSizes != null) {
            for (int size : replacementAllocationSizes) {
                sizes[next++] = Math.max(1, size);
            }
        }
        return sizes;
    }

    private EntryRecord liveHashRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = internals.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireHash(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord hashRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.HASH,
                hashRoot.encoding(handle),
                expireAtMillis,
                previous
        );
    }

    private ValueHandle requireHashHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!hashRoot.contains(handle)) {
            throw new IllegalStateException("native hash value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
        }
        return handle;
    }

    private static void requireHash(EntryRecord record) {
        if (record.type() != ValueType.HASH) {
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
                hashRoot.release(replacement);
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
                keyLifecycle.stableMemoryBackend(),
                0L,
                Math.max(0L, heapGrowthBytes),
                nativeAllocationSizes
        );
    }

    private static long estimateHashWriteUpperBound(int keyLength, List<byte[]> fieldValuePairs) {
        int pairCount = fieldValuePairs == null ? 0 : fieldValuePairs.size() / 2;
        return YierdisDbMemoryEstimator.estimateCollectionWriteUpperBound(
                keyLength,
                YierdisDbMemoryEstimator.sumByteLengths(fieldValuePairs),
                Math.multiplyExact((long) pairCount, HASH_PAIR_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(keyLifecycle.stableMemoryBackend(), 0)
        );
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
