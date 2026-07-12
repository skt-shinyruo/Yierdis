package yier.bubu.redis.storage.memory;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.PreparedPoppedValueSequence;

public final class YierdisListOps implements ListReadOps, ListWriteOps {
    private static final long LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final int ENTRY_RECORD_NATIVE_BYTES = 56;
    private static final int LIST_ROOT_NATIVE_BYTES = Long.BYTES;
    private static final int QUICKLIST_NODE_NATIVE_BYTES = 48;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ListRoot listRoot;

    YierdisListOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.listRoot = Objects.requireNonNull(keyLifecycle.listRoot(), "listRoot");
    }

    @Override
    public WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        return pushInternal(keyBytes, values, true);
    }

    @Override
    public WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        return pushInternal(keyBytes, values, false);
    }

    @Override
    public BulkStringSequence lrange(byte[] keyBytes, int start, int stop) {
        internals.checkThread();
        return sequenceOf(
                () -> lrangeCount(keyBytes, start, stop),
                out -> lrangeWriteTo(keyBytes, start, stop, out)
        );
    }

    @Override
    public WriteResult<PoppedValueSequence> lpop(byte[] keyBytes, int count) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        return popInternal(keyBytes, count, true);
    }

    @Override
    public WriteResult<PoppedValueSequence> rpop(byte[] keyBytes, int count) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        return popInternal(keyBytes, count, false);
    }

    private WriteResult<Long> pushInternal(byte[] keyBytes, List<byte[]> values, boolean left) {
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimatePushUpperBound(keyBytes, values, now);
            }

            @Override
            public PreparedEntryMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null && keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    keyLifecycle.removeIfExpired(currentEntry.keyHandle(), current, now);
                    currentEntry = currentEntry(keyBytes);
                    current = currentEntry.record();
                }
                if (current != null) {
                    requireList(current);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    replacement = listRoot.create();
                    if (current != null) {
                        listRoot.rpush(replacement, listRoot.range(requireListHandle(current), 0, -1));
                    }
                    if (left) {
                        listRoot.lpush(replacement, values);
                    } else {
                        listRoot.rpush(replacement, values);
                    }

                    int len = listRoot.size(replacement);
                    EntryRecord next = listRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of((long) len, MutationOutcome.VALUE_CHANGED);
                    long deltaBytes = estimateRecordBytes(targetKey, next) - estimateRecordBytes(targetKey, current);
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

    private WriteResult<PoppedValueSequence> popInternal(byte[] keyBytes, int count, boolean left) {
        if (count == 0) {
            return WriteResult.unchanged(PreparedPoppedValueSequence.empty());
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }

        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                if (isPopReclamation(keyBytes, count, now)) {
                    return 0L;
                }
                return estimatePopUpperBound(keyBytes, count, now);
            }

            @Override
            public AdmissionMode admissionMode() {
                return isPopReclamation(keyBytes, count, now) ? AdmissionMode.RECLAMATION : AdmissionMode.NORMAL;
            }

            @Override
            public PreparedEntryMutation<WriteResult<PoppedValueSequence>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current == null) {
                    return preparedNoEntry(
                            WriteResult.unchanged(PreparedPoppedValueSequence.nullValue()),
                            MutationOutcome.NONE
                    );
                }

                if (keyLifecycle.isKeyExpired(currentEntry.keyHandle(), now)) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        WriteResult<PoppedValueSequence> result = WriteResult.unchanged(
                                PreparedPoppedValueSequence.nullValue()
                        );
                        return preparedDelete(currentEntry, current, result, MutationOutcome.NONE, true, ttlMutation);
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                requireList(current);
                ValueHandle oldHandle = requireListHandle(current);
                int oldSize = listRoot.size(oldHandle);
                if (oldSize == 0) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        WriteResult<PoppedValueSequence> result = WriteResult.unchanged(
                                PreparedPoppedValueSequence.empty()
                        );
                        return preparedDelete(currentEntry, current, result, MutationOutcome.NONE, true, ttlMutation);
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                int popCount = Math.min(count, oldSize);
                PoppedValueSequence popped = PreparedPoppedValueSequence.owned(listRoot, oldHandle, popCount, left);
                WriteResult<PoppedValueSequence> result = WriteResult.of(popped, MutationOutcome.VALUE_CHANGED);
                int remaining = oldSize - popCount;
                if (remaining == 0) {
                    PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                    try {
                        ttlMutation = keyLifecycle.prepareRemoveExpire(currentEntry.keyHandle());
                        return preparedDelete(
                                currentEntry,
                                current,
                                result,
                                MutationOutcome.VALUE_CHANGED,
                                false,
                                ttlMutation
                        );
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                ValueHandle replacement = null;
                try {
                    replacement = listRoot.create();
                    copyRemainingAfterPop(oldHandle, replacement, oldSize, popCount, left);
                    EntryRecord next = listRecord(
                            currentEntry.keyHandle(),
                            replacement,
                            current.expireAtMillis(),
                            current
                    );
                    long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                            - estimateRecordBytes(currentEntry.keyHandle(), current);
                    PreparedEntryMutation<WriteResult<PoppedValueSequence>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            0L,
                            MutationOutcome.VALUE_CHANGED,
                            currentEntry.entryHandle(),
                            null,
                            null,
                            current,
                            next,
                            false,
                            PreparedTtlMutation.NONE
                    );
                    replacement = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (replacement != null) {
                        try {
                            listRoot.release(replacement);
                        } catch (RuntimeException | Error releaseFailure) {
                            failure.addSuppressed(releaseFailure);
                        }
                    }
                    throw failure;
                }
            }
        });
    }

    private int lrangeCount(byte[] keyBytes, int start, int stop) {
        EntryRecord record = liveListRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return listRoot.rangeCount(requireListHandle(record), start, stop);
    }

    private void lrangeWriteTo(byte[] keyBytes, int start, int stop, BulkStringSink out) {
        EntryRecord record = liveListRecord(keyBytes);
        if (record == null) {
            return;
        }
        listRoot.rangeInto(requireListHandle(record), start, stop, out);
    }

    private void copyRemainingAfterPop(
            ValueHandle oldHandle,
            ValueHandle replacement,
            int oldSize,
            int popCount,
            boolean left
    ) {
        if (left) {
            listRoot.rpush(replacement, listRoot.range(oldHandle, popCount, -1));
            return;
        }
        int stop = oldSize - popCount - 1;
        listRoot.rpush(replacement, listRoot.range(oldHandle, 0, stop));
    }

    private long estimatePushUpperBound(byte[] keyBytes, List<byte[]> values, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newListUpperBound(keyBytes, values);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newListUpperBound(keyBytes, values);
        }
        if (existing.type() != ValueType.LIST) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireListHandle(existing);
        int oldSize = listRoot.size(handle);
        int totalElements = oldSize + valueCount(values);
        int[] allocationSizes = listAllocationSizes(
                false,
                keyBytes,
                listRoot.nativePayloadSizes(handle),
                values,
                totalElements
        );
        long nativeUpperBound = nativePeak(0L, allocationSizes);
        long logicalUpperBound = estimateListWriteUpperBound(0, values);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long estimatePopUpperBound(byte[] keyBytes, int count, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return withScopeBookkeeping(0L);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis) || existing.type() != ValueType.LIST) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireListHandle(existing);
        int oldSize = listRoot.size(handle);
        int popCount = Math.min(count, oldSize);
        int remaining = oldSize - popCount;
        if (remaining <= 0) {
            return 0L;
        }

        int[] allocationSizes = listAllocationSizes(
                false,
                keyBytes,
                listRoot.nativePayloadSizes(handle),
                null,
                remaining
        );
        return withScopeBookkeeping(nativePeak(0L, allocationSizes));
    }

    private boolean isPopReclamation(byte[] keyBytes, int count, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return false;
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyHandle != null && keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return true;
        }
        if (existing.type() != ValueType.LIST) {
            return false;
        }

        ValueHandle handle = requireListHandle(existing);
        int oldSize = listRoot.size(handle);
        return oldSize == 0 || Math.min(count, oldSize) == oldSize;
    }

    private long newListUpperBound(byte[] keyBytes, List<byte[]> values) {
        long stagedHeapBytes = keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes();
        int[] allocationSizes = listAllocationSizes(
                true,
                keyBytes,
                new int[0],
                values,
                valueCount(values)
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = estimateListWriteUpperBound(keyBytes.length, values);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] listAllocationSizes(
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int[] existingPayloadSizes,
            List<byte[]> values,
            int nodeEstimate
    ) {
        int existingPayloadCount = existingPayloadSizes == null ? 0 : existingPayloadSizes.length;
        int newPayloadCount = nonNullValueCount(values);
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int nodeCount = Math.max(0, nodeEstimate);
        int[] sizes = new int[metadataCount + 1 + existingPayloadCount + newPayloadCount + nodeCount];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = ENTRY_RECORD_NATIVE_BYTES;
        }
        sizes[next++] = LIST_ROOT_NATIVE_BYTES;
        if (existingPayloadSizes != null) {
            for (int size : existingPayloadSizes) {
                sizes[next++] = Math.max(1, size);
            }
        }
        next = appendValuePayloadSizes(sizes, next, values);
        for (int i = 0; i < nodeCount; i++) {
            sizes[next++] = QUICKLIST_NODE_NATIVE_BYTES;
        }
        return sizes;
    }

    private EntryRecord liveListRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireList(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord listRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.LIST,
                listRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireListHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!listRoot.contains(handle)) {
            throw new IllegalStateException("native list value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireList(EntryRecord record) {
        if (record.type() != ValueType.LIST) {
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
                listRoot.release(replacement);
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

    private static long estimateListWriteUpperBound(int keyLength, List<byte[]> values) {
        int itemCount = values == null ? 0 : values.size();
        return YierdisDbMemoryEstimator.estimateCollectionWriteUpperBound(
                keyLength,
                YierdisDbMemoryEstimator.sumByteLengths(values),
                Math.multiplyExact((long) itemCount, LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    private static long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes()
        );
    }

    private static int valueCount(List<byte[]> values) {
        return values == null ? 0 : values.size();
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
