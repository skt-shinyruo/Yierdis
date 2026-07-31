package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.EntryMutationEntries.CurrentEntry;
import yier.bubu.redis.storage.memory.EntryMutationEntries.StagedEntry;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.HllReadOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.MutationMemoryEstimator;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.YierdisHyperLogLog;

import java.util.List;
import java.util.Objects;

public final class YierdisHllOps implements HllReadOps, HllWriteOps {
    private static final long HLL_REGISTER_HEAP_BYTES = (long) YierdisHyperLogLog.REGISTERS * Integer.BYTES;

    private final YierdisDbRuntimeInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final StringRoot stringRoot;

    YierdisHllOps(YierdisDbRuntimeInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.stringRoot = Objects.requireNonNull(keyLifecycle.stringRoot(), "stringRoot");
    }

    @Override
    public WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements) {
        internals.checkThread();
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(keyBytes, now);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimatePfaddUpperBound(keyBytes, elements, now);
            }

            @Override
            public PreparedDbMutation<WriteResult<Integer>> prepare() {
                CurrentEntry currentEntry = EntryMutationEntries.current(keyLifecycle, keyBytes);
                EntryRecord current = currentEntry.record();
                byte[] currentBytes = null;
                if (current != null) {
                    requireString(current);
                    currentBytes = stringRoot.copy(requireHllHandle(current));
                }
                byte[] replacementBytes = YierdisHyperLogLog.prepareAdd(currentBytes, elements);
                boolean changed = replacementBytes != null;
                if (current != null && !changed) {
                    return preparedNoEntry(WriteResult.of(0, MutationOutcome.NONE), MutationOutcome.NONE);
                }
                if (current == null && replacementBytes == null) {
                    replacementBytes = YierdisHyperLogLog.newSparse();
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = EntryMutationEntries.stage(keyLifecycle, keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    replacement = stringRoot.store(replacementBytes);
                    EntryRecord next = hllRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    MutationOutcome outcome = changed ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                    WriteResult<Integer> result = WriteResult.of(changed ? 1 : 0, outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Integer>> prepared = EntryMutationEntries.upsert(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            true
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

    @Override
    public long pfcount(List<byte[]> keys) {
        internals.checkThread();
        if (keys == null || keys.isEmpty()) {
            return 0L;
        }

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        for (byte[] keyBytes : keys) {
            EntryRecord record = liveStringRecord(keyBytes);
            if (record == null) {
                continue;
            }
            ValueHandle handle = requireHllHandle(record);
            YierdisHyperLogLog.mergeHllIntoRegisters(stringRoot.slice(handle), registers);
        }
        return YierdisHyperLogLog.estimateCardinality(registers);
    }

    @Override
    public WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
        internals.checkThread();
        Objects.requireNonNull(destKeyBytes, "destKeyBytes");
        if (sourceKeys == null || sourceKeys.isEmpty()) {
            throw new IllegalArgumentException("sourceKeys must not be empty");
        }

        long now = System.currentTimeMillis();
        reclaimExpiredBeforeMutation(destKeyBytes, now);
        for (byte[] sourceKey : sourceKeys) {
            reclaimExpiredBeforeMutation(sourceKey, now);
        }
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return estimatePfmergeUpperBound(destKeyBytes, sourceKeys, now);
            }

            @Override
            public PreparedDbMutation<WriteResult<Void>> prepare() {
                MergeRegisters merged = mergeSourceRegisters(sourceKeys, now);
                CurrentEntry currentEntry = EntryMutationEntries.current(keyLifecycle, destKeyBytes);
                EntryRecord current = currentEntry.record();
                byte[] currentBytes = null;
                ValueHandle currentHandle = null;
                if (current != null) {
                    requireString(current);
                    currentHandle = requireHllHandle(current);
                    currentBytes = stringRoot.copy(currentHandle);
                }

                byte[] replacementBytes = YierdisHyperLogLog.prepareMerge(currentBytes, merged.registers());
                boolean valueChanged = replacementBytes != null;
                boolean ttlChanged = current != null && current.expireAtMillis() >= 0L;
                MutationOutcome outcome = MutationOutcome.of(valueChanged, ttlChanged);
                if (current != null && !outcome.changedAny()) {
                    return preparedNoEntry(WriteResult.<Void>unchanged(null), MutationOutcome.NONE);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = EntryMutationEntries.stage(keyLifecycle, destKeyBytes);
                        targetKey = staged.keyHandle();
                    }

                    if (valueChanged) {
                        replacement = stringRoot.store(replacementBytes);
                    } else {
                        replacement = currentHandle;
                    }
                    EntryRecord next = hllRecord(targetKey, replacement, -1L, current);
                    WriteResult<Void> result = WriteResult.of(null, outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Void>> prepared = EntryMutationEntries.upsert(
                            keyLifecycle,
                            result,
                            deltaBytes,
                            staged == null ? 0L : staged.stagedHeapBytes(),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            valueChanged
                    );
                    staged = null;
                    if (valueChanged) {
                        replacement = null;
                    }
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, valueChanged ? replacement : null, failure);
                    throw failure;
                }
            }
        });
    }

    private long estimatePfaddUpperBound(byte[] keyBytes, List<byte[]> elements, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        int replacementLength = YierdisHyperLogLog.denseLength();
        if (existing == null) {
            return hllNativeUpperBound(
                    addSaturating(HLL_REGISTER_HEAP_BYTES, replacementLength),
                    true,
                    keyBytes,
                    replacementLength
            );
        }
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return hllNativeUpperBound(
                    addSaturating(HLL_REGISTER_HEAP_BYTES, replacementLength),
                    true,
                    keyBytes,
                    replacementLength
            );
        }
        if (existing.type() != ValueType.STRING) {
            return withScopeBookkeeping(0L);
        }
        ValueHandle handle = requireHllHandle(existing);
        int existingLen = stringRoot.length(handle);
        int targetLength = pfaddReplacementLengthUpperBound(existingLen, elements);
        long heapGrowthBytes = addSaturating(
                HLL_REGISTER_HEAP_BYTES,
                addSaturating(existingLen, targetLength)
        );
        return hllNativeUpperBound(heapGrowthBytes, false, keyBytes, targetLength);
    }

    private long estimatePfmergeUpperBound(byte[] keyBytes, List<byte[]> sourceKeys, long nowMillis) {
        int replacementLength = YierdisHyperLogLog.denseLength();
        long sourceCopyBytes = sourceCopyBytesUpperBound(sourceKeys, nowMillis);
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return hllNativeUpperBound(
                    addSaturating(HLL_REGISTER_HEAP_BYTES, addSaturating(sourceCopyBytes, replacementLength)),
                    true,
                    keyBytes,
                    replacementLength
            );
        }
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return hllNativeUpperBound(
                    addSaturating(HLL_REGISTER_HEAP_BYTES, addSaturating(sourceCopyBytes, replacementLength)),
                    true,
                    keyBytes,
                    replacementLength
            );
        }
        if (existing.type() != ValueType.STRING) {
            return withScopeBookkeeping(0L);
        }
        ValueHandle handle = requireHllHandle(existing);
        long heapGrowthBytes = addSaturating(
                HLL_REGISTER_HEAP_BYTES,
                addSaturating(
                        sourceCopyBytes,
                        addSaturating(stringRoot.length(handle), replacementLength)
                )
        );
        return hllNativeUpperBound(heapGrowthBytes, false, keyBytes, replacementLength);
    }

    private long hllNativeUpperBound(
            long heapGrowthBytes,
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int replacementLength
    ) {
        long stagedKeyDirectoryGrowthBytes = includeKeyAndEntry
                ? keyLifecycle.keyDirectory().estimatedInsertHeapGrowthBytes()
                : 0L;
        int valueLength = Math.max(1, replacementLength);
        int[] sizes = includeKeyAndEntry
                ? new int[]{
                        Math.max(1, keyBytes == null ? 0 : keyBytes.length),
                        NativeStorageLayout.ENTRY_RECORD_BYTES,
                        valueLength
                }
                : new int[]{valueLength};
        return withScopeBookkeeping(MutationMemoryEstimator.peakAdditionalBytes(
                keyLifecycle.stableMemoryBackend(),
                0L,
                addSaturating(Math.max(0L, heapGrowthBytes), stagedKeyDirectoryGrowthBytes),
                sizes
        ));
    }

    private static int pfaddReplacementLengthUpperBound(int existingLen, List<byte[]> elements) {
        int batchSparseLength = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
        long additionalSparseBytes = Math.max(0L, (long) batchSparseLength - YierdisHyperLogLog.HEADER_BYTES);
        long sparseUpperBound = addSaturating(existingLen, additionalSparseBytes);
        return (int) Math.min(YierdisHyperLogLog.denseLength(), sparseUpperBound);
    }

    private long sourceCopyBytesUpperBound(List<byte[]> sourceKeys, long nowMillis) {
        long bytes = 0L;
        if (sourceKeys == null) {
            return bytes;
        }
        for (byte[] sourceKey : sourceKeys) {
            EntryRecord record = keyLifecycle.entryRecord(sourceKey);
            if (record == null) {
                continue;
            }
            KeyHandle keyHandle = keyLifecycle.keyHandle(sourceKey);
            if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
                continue;
            }
            requireString(record);
            bytes = addSaturating(bytes, stringRoot.length(requireHllHandle(record)));
        }
        return bytes;
    }

    private MergeRegisters mergeSourceRegisters(List<byte[]> sourceKeys, long nowMillis) {
        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        long copiedBytes = 0L;
        for (byte[] sourceKey : sourceKeys) {
            EntryRecord record = liveStringRecordForPrepare(sourceKey, nowMillis);
            if (record == null) {
                continue;
            }
            byte[] raw = stringRoot.copy(requireHllHandle(record));
            copiedBytes = addSaturating(copiedBytes, raw.length);
            YierdisHyperLogLog.mergeHllIntoRegisters(raw, registers);
        }
        return new MergeRegisters(registers, copiedBytes);
    }

    private EntryRecord liveStringRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = internals.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireString(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord liveStringRecordForPrepare(byte[] keyBytes, long nowMillis) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record == null || keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return null;
        }
        requireString(record);
        return record;
    }

    private EntryRecord hllRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                expireAtMillis,
                previous
        );
    }

    private ValueHandle requireHllHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!stringRoot.contains(handle)) {
            throw new IllegalStateException("native hll value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
        }
        if (!YierdisHyperLogLog.isHllString(stringRoot, handle)) {
            throw new WrongTypeException();
        }
        return handle;
    }

    private static void requireString(EntryRecord record) {
        if (record.type() != ValueType.STRING) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
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

    private <T> PreparedEntryMutation<T> preparedNoEntry(T result, MutationOutcome outcome) {
        return PreparedEntryMutation.unchanged(keyLifecycle, result, outcome);
    }

    private void abortStaged(
            StagedEntry staged,
            ValueHandle replacement,
            Throwable failure
    ) {
        if (replacement != null) {
            try {
                stringRoot.release(replacement);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        EntryMutationEntries.abortStaged(keyLifecycle, staged, failure);
    }

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                MutationMemoryEstimator.nativeAllocationScopeBookkeepingBytes(keyLifecycle.stableMemoryBackend(), 0)
        );
    }

    private static long addSaturating(long left, long right) {
        if (left < 0L || right < 0L || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private record MergeRegisters(int[] registers, long copiedBytes) {
    }
}
