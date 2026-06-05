package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.HllReadOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.EntryMutationResult;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.YierdisHyperLogLog;

import java.util.List;
import java.util.Objects;

public final class YierdisHllOps implements HllReadOps, HllWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final StringRoot stringRoot;

    YierdisHllOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.stringRoot = Objects.requireNonNull(keyLifecycle.stringRoot(), "stringRoot");
    }

    @Override
    public WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimatePfaddUpperBound(keyBytes, elements);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Integer>> apply() {
                return keyLifecycle.computeWithHandleResult(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    long deltaBytes = 0L;
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                    }

                    ValueHandle handle;
                    if (current == null) {
                        handle = stringRoot.store(YierdisHyperLogLog.newSparse());
                    } else {
                        requireString(current);
                        handle = requireHllHandle(current);
                    }

                    boolean ok = false;
                    try {
                        boolean changed = YierdisHyperLogLog.pfAdd(stringRoot, handle, elements);
                        EntryRecord next = hllRecord(k, handle, current == null ? -1L : current.expireAtMillis(), current);
                        deltaBytes -= oldEstimate;
                        deltaBytes += estimateRecordBytes(k, next);
                        ok = true;
                        MutationOutcome outcome = changed ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                        return mutationResult(next, WriteResult.of(changed ? 1 : 0, outcome), deltaBytes);
                    } finally {
                        if (!ok && current == null) {
                            stringRoot.release(handle);
                        }
                    }
                });
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
        if (sourceKeys == null || sourceKeys.isEmpty()) {
            throw new IllegalArgumentException("sourceKeys must not be empty");
        }

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        for (byte[] keyBytes : sourceKeys) {
            EntryRecord record = liveStringRecord(keyBytes);
            if (record == null) {
                continue;
            }
            ValueHandle handle = requireHllHandle(record);
            YierdisHyperLogLog.mergeHllIntoRegisters(stringRoot.slice(handle), registers);
        }

        byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        long now = System.currentTimeMillis();
        long upperBound = estimatePfmergeUpperBound(destKeyBytes, mergedDense.length);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Void>> apply() {
                record PfMergeComputation(
                        KeyHandle keyHandle,
                        YierdisDbMutationExecutor.MutationResult<WriteResult<Void>> mutation
                ) {
                }

                PfMergeComputation computation = keyLifecycle.computeWithHandleResult(destKeyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    long deltaBytes = 0L;
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                    }

                    ValueHandle handle;
                    if (current == null) {
                        handle = stringRoot.store(mergedDense);
                    } else {
                        requireString(current);
                        handle = requireHllHandle(current);
                        stringRoot.overwrite(handle, mergedDense);
                    }

                    EntryRecord next = hllRecord(k, handle, -1L, current);
                    deltaBytes -= oldEstimate;
                    deltaBytes += estimateRecordBytes(k, next);
                    YierdisDbMutationExecutor.MutationResult<WriteResult<Void>> mutation =
                            YierdisDbMutationExecutor.MutationResult.of(
                                    WriteResult.of(null, MutationOutcome.VALUE_CHANGED),
                                    deltaBytes
                            );
                    return EntryMutationResult.of(next, new PfMergeComputation(k, mutation));
                });
                keyLifecycle.removeExpire(currentKeyHandle(destKeyBytes, computation.keyHandle()));
                return computation.mutation();
            }
        });
    }

    private long estimatePfaddUpperBound(byte[] keyBytes, List<byte[]> elements) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            int upperValueLength = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
            return YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength);
        }
        if (existing.type() != ValueType.STRING) {
            return 0L;
        }
        ValueHandle handle = requireHllHandle(existing);
        if (YierdisHyperLogLog.isDense(stringRoot, handle)) {
            return 0L;
        }
        int existingLen = stringRoot.length(handle);
        int sparseUpperBound = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
        int targetLength = Math.min(
                YierdisHyperLogLog.denseLength(),
                Math.max(existingLen, existingLen + sparseUpperBound - YierdisHyperLogLog.HEADER_BYTES)
        );
        return Math.max(0L, (long) targetLength - existingLen);
    }

    private long estimatePfmergeUpperBound(byte[] keyBytes, int mergedDenseLength) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength);
        }
        if (existing.type() != ValueType.STRING) {
            return 0L;
        }
        ValueHandle handle = requireHllHandle(existing);
        int existingLen = stringRoot.length(handle);
        return Math.max(0L, (long) mergedDenseLength - existingLen);
    }

    private EntryRecord liveStringRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireString(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord hllRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireHllHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!stringRoot.contains(handle)) {
            throw new IllegalStateException("native hll value handle is not available: " + (handle == null ? "null" : handle.raw()));
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

    private KeyHandle currentKeyHandle(byte[] keyBytes, KeyHandle fallback) {
        KeyHandle current = keyLifecycle.keyHandle(keyBytes);
        return current == null ? fallback : current;
    }

    private static <T> EntryMutationResult<YierdisDbMutationExecutor.MutationResult<T>> mutationResult(
            EntryRecord record,
            T value,
            long deltaBytes
    ) {
        return EntryMutationResult.of(record, YierdisDbMutationExecutor.MutationResult.of(value, deltaBytes));
    }
}
