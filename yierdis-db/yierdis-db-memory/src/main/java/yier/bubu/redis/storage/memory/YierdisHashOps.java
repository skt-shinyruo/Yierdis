package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringMapPairs;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
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
import java.util.function.IntSupplier;

public final class YierdisHashOps implements HashReadOps, HashWriteOps {
    private static final long HASH_PAIR_OVERHEAD_BYTES_ESTIMATE = 256L;
    private static final int ENTRY_RECORD_NATIVE_BYTES = 56;
    private static final int HASH_ROOT_NATIVE_BYTES = Long.BYTES;

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
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimateHashSetUpperBound(keyBytes, fieldValuePairs, now);
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
                    requireHash(current);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    replacement = hashRoot.create();
                    if (current != null) {
                        hashRoot.hsetMany(replacement, hashRoot.hgetallPairs(requireHashHandle(current)));
                    }
                    int added = hashRoot.hsetMany(replacement, fieldValuePairs);
                    EntryRecord next = hashRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of((long) added, MutationOutcome.VALUE_CHANGED);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            MutationOutcome.VALUE_CHANGED,
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
    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        internals.checkThread();
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return null;
        }
        return hashRoot.hget(requireHashHandle(record), fieldBytes);
    }

    @Override
    public BulkStringMapPairs hgetall(byte[] keyBytes) {
        internals.checkThread();
        return pairsOf(
                () -> hgetallCount(keyBytes),
                out -> hgetallWriteTo(keyBytes, out)
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
    public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
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
                if (keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        return preparedDelete(
                                currentEntry,
                                current,
                                WriteResult.of(0L, MutationOutcome.NONE),
                                MutationOutcome.NONE,
                                true,
                                ttlMutation
                        );
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
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
                            keyLifecycle.entryTable().replace(currentEntry.entryHandle(), next);
                        },
                        null,
                        null
                );
            }
        });
    }

    private int hgetallCount(byte[] keyBytes) {
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return hashRoot.hgetallCount(requireHashHandle(record));
    }

    private void hgetallWriteTo(byte[] keyBytes, BulkStringSink out) {
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return;
        }
        hashRoot.hgetallPairsInto(requireHashHandle(record), out);
    }

    private long estimateHashSetUpperBound(byte[] keyBytes, List<byte[]> fieldValuePairs, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newHashUpperBound(keyBytes, fieldValuePairs);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newHashUpperBound(keyBytes, fieldValuePairs);
        }
        if (existing.type() != ValueType.HASH) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireHashHandle(existing);
        int[] allocationSizes = hashAllocationSizes(
                false,
                keyBytes,
                hashRoot.nativePayloadSizes(handle),
                fieldValuePairs
        );
        long nativeUpperBound = nativePeak(0L, allocationSizes);
        long logicalUpperBound = estimateHashWriteUpperBound(0, fieldValuePairs);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long newHashUpperBound(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        long stagedHeapBytes = keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes();
        int[] allocationSizes = hashAllocationSizes(
                true,
                keyBytes,
                new int[0],
                fieldValuePairs
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = estimateHashWriteUpperBound(keyBytes.length, fieldValuePairs);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] hashAllocationSizes(
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int[] existingPayloadSizes,
            List<byte[]> fieldValuePairs
    ) {
        int existingPayloadCount = existingPayloadSizes == null ? 0 : existingPayloadSizes.length;
        int newPayloadCount = nonNullValueCount(fieldValuePairs);
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int[] sizes = new int[metadataCount + 1 + existingPayloadCount * 2 + newPayloadCount * 2];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = ENTRY_RECORD_NATIVE_BYTES;
        }
        sizes[next++] = HASH_ROOT_NATIVE_BYTES;
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendHashPairPayloadSizes(sizes, next, fieldValuePairs);
        next = appendHashPairPayloadSizes(sizes, next, fieldValuePairs);
        if (next != sizes.length) {
            throw new IllegalStateException("hash allocation size estimate count mismatch");
        }
        return sizes;
    }

    private EntryRecord liveHashRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
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
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireHashHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!hashRoot.contains(handle)) {
            throw new IllegalStateException("native hash value handle is not available: " + (handle == null ? "null" : handle.raw()));
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
                keyLifecycle.nativeAllocator(),
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

    private static long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes()
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

    private static int appendHashPairPayloadSizes(int[] sizes, int offset, List<byte[]> fieldValuePairs) {
        if (fieldValuePairs == null || fieldValuePairs.isEmpty()) {
            return offset;
        }
        int next = offset;
        for (byte[] value : fieldValuePairs) {
            if (value != null) {
                sizes[next++] = Math.max(1, value.length);
            }
        }
        return next;
    }

    private static BulkStringMapPairs pairsOf(IntSupplier countSupplier, BulkEmitter emitter) {
        Objects.requireNonNull(countSupplier, "countSupplier");
        Objects.requireNonNull(emitter, "emitter");
        return new BulkStringMapPairs() {
            @Override
            public int pairCount() {
                int count = countSupplier.getAsInt();
                return Math.max(count / 2, 0);
            }

            @Override
            public void emitPairsTo(BulkStringSink out) {
                emitter.emitTo(out);
            }
        };
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
