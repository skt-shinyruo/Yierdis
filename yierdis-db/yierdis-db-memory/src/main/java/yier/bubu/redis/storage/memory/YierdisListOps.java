package yier.bubu.redis.storage.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSources;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.PreparedPoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.value.PinnedPoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.value.ListValue;
import yier.bubu.redis.storage.memory.internal.value.SemanticResultSupport;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public final class YierdisListOps implements ListReadOps, ListWriteOps {
    private static final long LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE = 32L;
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
        Objects.requireNonNull(values, "values");
        return pushInternal(keyBytes, values, true);
    }

    @Override
    public WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(values, "values");
        return pushInternal(keyBytes, values, false);
    }

    @Override
    public ByteSequenceSource lrange(byte[] keyBytes, int start, int stop) {
        internals.checkThread();
        EntryRecord record = liveListRecord(keyBytes);
        if (record == null) {
            return ByteSequenceSources.empty();
        }
        ValueHandle handle = requireListHandle(record);
        int count = listRoot.rangeCount(handle, start, stop);
        return ByteSequenceSources.of(
                count,
                0L,
                out -> listRoot.rangeInto(handle, start, stop, SemanticResultSupport.lengthSink(out)),
                out -> listRoot.rangeInto(handle, start, stop, out)
        );
    }

    @Override
    public PreparedMutation<PoppedValueSequence> preparePop(byte[] keyBytes, int count, boolean left) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        byte[] preparedKey = java.util.Arrays.copyOf(keyBytes, keyBytes.length);
        if (count == 0) {
            return new PreparedPopMutation(
                    preparedKey,
                    count,
                    left,
                    preparedEntryState(preparedKey),
                    PinnedPoppedValueSequence.empty()
            );
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        PreparedEntryState state = preparedEntryState(preparedKey);
        EntryRecord record = state.liveRecord();
        PoppedValueSequence preview;
        if (record == null) {
            preview = PinnedPoppedValueSequence.nullValue();
        } else {
            requireList(record);
            preview = PinnedPoppedValueSequence.capture(
                    keyLifecycle.stableMemoryBackend(),
                    listRoot.popEntries(requireListHandle(record), count, left)
            );
        }
        return new PreparedPopMutation(preparedKey, count, left, state, preview);
    }

    private WriteResult<Long> pushInternal(byte[] keyBytes, List<byte[]> values, boolean left) {
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimatePushUpperBound(keyBytes, values, left, now);
            }

            @Override
            public PreparedEntryMutation<WriteResult<Long>> prepare() {
                CurrentEntry currentEntry = currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null) {
                    requireList(current);
                    return prepareExistingPush(currentEntry, current, values, left);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                long rootHeapBefore = listRoot.retainedHeapBytes();
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = stageNewEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    List<byte[]> orderedValues = pushedValues(values, left);
                    replacement = listRoot.build(orderedValues);

                    int len = listRoot.size(replacement);
                    EntryRecord next = listRecord(
                            targetKey,
                            replacement,
                            -1L,
                            null
                    );
                    WriteResult<Long> result = WriteResult.of((long) len, MutationOutcome.VALUE_CHANGED);
                    long deltaBytes = estimateRecordBytes(targetKey, next);
                    PreparedEntryMutation<WriteResult<Long>> prepared = new PreparedEntryMutation<>(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            MemoryUsageSnapshot.addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    listRoot.positiveRetainedHeapGrowthBytes(rootHeapBefore)
                            ),
                            MutationOutcome.VALUE_CHANGED,
                            null,
                            staged == null ? null : staged.entryHandle(),
                            staged == null ? null : staged.stagedKey(),
                            null,
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
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                if (isPopReclamation(keyBytes, count, now)) {
                    return 0L;
                }
                return estimatePopUpperBound(keyBytes, count, left, now);
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
                PreparedPoppedValueSequence popped = PreparedPoppedValueSequence.owned(
                        keyLifecycle.stableMemoryBackend(),
                        listRoot.popEntries(oldHandle, popCount, left)
                );
                WriteResult<PoppedValueSequence> result = WriteResult.of(popped, MutationOutcome.VALUE_CHANGED);
                int remaining = oldSize - popCount;
                Runnable releaseOldListToPopped = releaseOldListToPopped(oldHandle, popped);
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
                                ttlMutation,
                                releaseOldListToPopped
                        );
                    } catch (RuntimeException | Error failure) {
                        abortTtl(ttlMutation, failure);
                        throw failure;
                    }
                }

                return prepareExistingPop(
                        currentEntry,
                        current,
                        oldHandle,
                        popCount,
                        left,
                        popped,
                        result
                );
            }
        });
    }

    private PreparedEntryMutation<WriteResult<Long>> prepareExistingPush(
            CurrentEntry currentEntry,
            EntryRecord current,
            List<byte[]> values,
            boolean left
    ) {
        ValueHandle handle = requireListHandle(current);
        if (values.isEmpty()) {
            return preparedNoEntry(
                    WriteResult.unchanged((long) listRoot.size(handle)),
                    MutationOutcome.NONE
            );
        }
        ListValue.PreparedMutation valueMutation = null;
        try {
            valueMutation = listRoot.preparePush(handle, values, left);
            EntryRecord next = listRecord(
                    currentEntry.keyHandle(),
                    handle,
                    valueMutation.encoding(),
                    current.expireAtMillis(),
                    current
            );
            long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                    - estimateRecordBytes(currentEntry.keyHandle(), current);
            ListValue.PreparedMutation transferred = valueMutation;
            return new PreparedEntryMutation<>(
                    keyLifecycle,
                    WriteResult.of((long) valueMutation.size(), MutationOutcome.VALUE_CHANGED),
                    deltaBytes,
                    valueMutation.stagedHeapBytes(),
                    MutationOutcome.VALUE_CHANGED,
                    currentEntry.entryHandle(),
                    null,
                    null,
                    current,
                    next,
                    false,
                    transferred::releaseSuperseded,
                    PreparedTtlMutation.NONE,
                    transferred
            ).beforeEntryPublish(transferred::commit);
        } catch (RuntimeException | Error failure) {
            closePreparedValueMutation(valueMutation, failure);
            throw failure;
        }
    }

    private PreparedEntryMutation<WriteResult<PoppedValueSequence>> prepareExistingPop(
            CurrentEntry currentEntry,
            EntryRecord current,
            ValueHandle handle,
            int popCount,
            boolean left,
            PreparedPoppedValueSequence popped,
            WriteResult<PoppedValueSequence> result
    ) {
        ListValue.PreparedMutation valueMutation = null;
        try {
            valueMutation = listRoot.preparePop(handle, popCount, left);
            EntryRecord next = listRecord(
                    currentEntry.keyHandle(),
                    handle,
                    valueMutation.encoding(),
                    current.expireAtMillis(),
                    current
            );
            long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                    - estimateRecordBytes(currentEntry.keyHandle(), current);
            ListValue.PreparedMutation transferred = valueMutation;
            return new PreparedEntryMutation<>(
                    keyLifecycle,
                    result,
                    deltaBytes,
                    valueMutation.stagedHeapBytes(),
                    MutationOutcome.VALUE_CHANGED,
                    currentEntry.entryHandle(),
                    null,
                    null,
                    current,
                    next,
                    false,
                    () -> releasePreparedPopToReply(transferred, popped),
                    PreparedTtlMutation.NONE,
                    transferred
            ).beforeEntryPublish(transferred::commit);
        } catch (RuntimeException | Error failure) {
            closePreparedValueMutation(valueMutation, failure);
            throw failure;
        }
    }

    private static List<byte[]> pushedValues(List<byte[]> values, boolean left) {
        ArrayList<byte[]> ordered = new ArrayList<>(values.size());
        if (left) {
            for (int index = values.size() - 1; index >= 0; index--) {
                ordered.add(values.get(index));
            }
        } else {
            ordered.addAll(values);
        }
        return ordered;
    }

    private long estimatePushUpperBound(byte[] keyBytes, List<byte[]> values, boolean left, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newListUpperBound(keyBytes, values, left);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newListUpperBound(keyBytes, values, left);
        }
        if (existing.type() != ValueType.LIST) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireListHandle(existing);
        int[] allocationSizes = listRoot.preparedPushNativeAllocationSizes(handle, values, left);
        long stagedHeapBytes = listRoot.estimatedPreparedPushHeapGrowthBytes(
                handle,
                values,
                left,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = estimateListWriteUpperBound(0, values);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long estimatePopUpperBound(byte[] keyBytes, int count, boolean left, long nowMillis) {
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

        int[] allocationSizes = listRoot.preparedPopNativeAllocationSizes(handle, popCount, left);
        long stagedHeapBytes = listRoot.estimatedPreparedPopHeapGrowthBytes(handle, popCount, left);
        return withScopeBookkeeping(nativePeak(stagedHeapBytes, allocationSizes));
    }

    private boolean isPopReclamation(byte[] keyBytes, int count, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return true;
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

    private long newListUpperBound(byte[] keyBytes, List<byte[]> values, boolean left) {
        int[] allocationSizes = listAllocationSizes(
                keyBytes,
                listRoot.preparedPushNativeAllocationSizes(null, values, left)
        );
        long stagedHeapBytes = MemoryUsageSnapshot.addSaturating(
                keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes(),
                listRoot.estimatedPreparedPushHeapGrowthBytes(
                        null,
                        values,
                        left,
                        3
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = estimateListWriteUpperBound(keyBytes.length, values);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] listAllocationSizes(
            byte[] keyBytes,
            int[] replacementAllocationSizes
    ) {
        int payloadCount = replacementAllocationSizes == null ? 0 : replacementAllocationSizes.length;
        int[] sizes = new int[3 + payloadCount];
        int next = 0;
        sizes[next++] = Math.max(1, keyBytes.length);
        sizes[next++] = NativeStorageLayout.ENTRY_RECORD_BYTES;
        sizes[next++] = NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES;
        if (replacementAllocationSizes != null) {
            for (int size : replacementAllocationSizes) {
                sizes[next++] = Math.max(1, size);
            }
        }
        return sizes;
    }

    private EntryRecord liveListRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = internals.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireList(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord listRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return listRecord(keyHandle, handle, listRoot.encoding(handle), expireAtMillis, previous);
    }

    private EntryRecord listRecord(
            KeyHandle keyHandle,
            ValueHandle handle,
            ValueEncoding encoding,
            long expireAtMillis,
            EntryRecord previous
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.LIST,
                encoding,
                expireAtMillis,
                previous
        );
    }

    private ValueHandle requireListHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!listRoot.contains(handle)) {
            throw new IllegalStateException("native list value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
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

    private PreparedEntryState preparedEntryState(byte[] keyBytes) {
        EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
        EntryRecord record = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        boolean expired = record != null
                && keyHandle != null
                && keyLifecycle.isKeyExpired(keyHandle, System.currentTimeMillis());
        return new PreparedEntryState(entryHandle, record, record == null ? 0L : record.version(), expired);
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
        return preparedDelete(
                currentEntry,
                current,
                result,
                outcome,
                releaseOldValue,
                ttlMutation,
                null
        );
    }

    private <T> PreparedEntryMutation<T> preparedDelete(
            CurrentEntry currentEntry,
            EntryRecord current,
            T result,
            MutationOutcome outcome,
            boolean releaseOldValue,
            PreparedTtlMutation ttlMutation,
            Runnable releaseReplacedValueHook
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
                releaseReplacedValueHook,
                ttlMutation
        );
    }

    private Runnable releaseOldListToPopped(ValueHandle oldHandle, PreparedPoppedValueSequence popped) {
        return () -> {
            // 先声明 reply 对 retained block 的唯一 ownership；后续 root/node free 失败也不能让 block 失去 owner。
            popped.activateOwnership();
            try {
                listRoot.releaseExcept(oldHandle, popped);
            } catch (RuntimeException | Error failure) {
                try {
                    popped.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        };
    }

    private void releasePreparedPopToReply(
            ListValue.PreparedMutation valueMutation,
            PreparedPoppedValueSequence popped
    ) {
        popped.activateOwnership();
        try {
            valueMutation.releaseSuperseded(popped);
        } catch (RuntimeException | Error failure) {
            try {
                popped.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static void closePreparedValueMutation(
            ListValue.PreparedMutation valueMutation,
            Throwable failure
    ) {
        if (valueMutation == null) {
            return;
        }
        try {
            valueMutation.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
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
                keyLifecycle.stableMemoryBackend(),
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

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(keyLifecycle.stableMemoryBackend(), 0)
        );
    }

    private static int valueCount(List<byte[]> values) {
        return values == null ? 0 : values.size();
    }

    private record CurrentEntry(
            EntryHandle entryHandle,
            KeyHandle keyHandle,
            EntryRecord record
    ) {
    }

    private record PreparedEntryState(
            EntryHandle entryHandle,
            EntryRecord record,
            long version,
            boolean expired
    ) {
        private EntryRecord liveRecord() {
            return expired ? null : record;
        }
    }

    private final class PreparedPopMutation implements PreparedMutation<PoppedValueSequence> {
        private final byte[] keyBytes;
        private final int count;
        private final boolean left;
        private final PreparedEntryState expectedState;
        private final PoppedValueSequence preview;
        private boolean closed;
        private boolean committed;
        private boolean trimNativePagesAfterClose;

        private PreparedPopMutation(
                byte[] keyBytes,
                int count,
                boolean left,
                PreparedEntryState expectedState,
                PoppedValueSequence preview
        ) {
            this.keyBytes = keyBytes;
            this.count = count;
            this.left = left;
            this.expectedState = expectedState;
            this.preview = preview;
        }

        @Override
        public PoppedValueSequence preview() {
            return preview;
        }

        @Override
        public boolean isCurrent() {
            internals.checkThread();
            PreparedEntryState current = preparedEntryState(keyBytes);
            return Objects.equals(expectedState.entryHandle(), current.entryHandle())
                    && expectedState.version() == current.version()
                    && expectedState.expired() == current.expired();
        }

        @Override
        public MutationOutcome commit(MutationContext context) {
            internals.checkThread();
            Objects.requireNonNull(context, "context");
            requireCommittable();
            WriteResult<PoppedValueSequence> result = internals.withMutationContext(
                    context,
                    () -> popInternal(keyBytes, count, left)
            );
            committed = true;
            trimNativePagesAfterClose = result.mutationOutcome().changedAny();
            try {
                return result.mutationOutcome();
            } finally {
                result.value().close();
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            preview.close();
            if (trimNativePagesAfterClose) {
                internals.trimEmptyNativePagesAfterPreparedPreviewClose();
            }
        }

        private void requireCommittable() {
            if (closed) {
                throw new IllegalStateException("prepared mutation is closed");
            }
            if (committed) {
                throw new IllegalStateException("prepared mutation is already committed");
            }
            if (!isCurrent()) {
                throw new IllegalStateException("prepared mutation is stale");
            }
        }
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
