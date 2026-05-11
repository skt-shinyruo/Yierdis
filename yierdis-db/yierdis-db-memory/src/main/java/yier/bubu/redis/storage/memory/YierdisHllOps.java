package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.HllReadOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;

import java.util.List;
import java.util.Objects;
import java.util.function.ToLongBiFunction;

public final class YierdisHllOps implements HllReadOps, HllWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final StringRoot stringRoot;
    private final ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator;

    YierdisHllOps(YierdisDbInternals internals, ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.stringRoot = Objects.requireNonNull(keyLifecycle.stringRoot(), "stringRoot");
        this.entryBytesEstimator = Objects.requireNonNull(entryBytesEstimator, "entryBytesEstimator");
    }

    @Override
    public WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimatePfaddUpperBound(keyBytes, elements);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Integer>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Integer>> apply() {
                final boolean[] changed = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeObjectWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        old = YierdisObject.newString(stringRoot, YierdisHyperLogLog.newSparse());
                        keyLifecycle.touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        keyLifecycle.touch(old);
                    }

                    EntryRecord record = keyLifecycle.entryRecord(k);
                    if (canReadFromRoot(old, record)) {
                        changed[0] = YierdisHyperLogLog.pfAdd(stringRoot, record.valueHandle(), elements);
                    } else if (old.hasStringRoot()) {
                        changed[0] = YierdisHyperLogLog.pfAdd(stringRoot, old.valueHandle(), elements);
                    } else {
                        changed[0] = YierdisHyperLogLog.pfAdd(old, keyLifecycle.offHeapAllocator(), elements);
                    }

                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                MutationOutcome outcome = changed[0] ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(changed[0] ? 1 : 0, outcome),
                        deltaBytes[0]
                );
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
            YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
            if (object == null) {
                continue;
            }
            if (object.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            EntryRecord record = keyLifecycle.entryRecord(keyBytes);
            if (canReadFromRoot(object, record)) {
                if (!YierdisHyperLogLog.isHllString(stringRoot, record.valueHandle())) {
                    throw new WrongTypeException();
                }
                YierdisHyperLogLog.mergeHllIntoRegisters(stringRoot.slice(record.valueHandle()), registers);
                continue;
            }
            if (!YierdisHyperLogLog.isHllString(object)) {
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(object.stringBytesView(), registers);
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
            YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
            if (object == null) {
                continue;
            }
            if (object.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            EntryRecord record = keyLifecycle.entryRecord(keyBytes);
            if (canReadFromRoot(object, record)) {
                if (!YierdisHyperLogLog.isHllString(stringRoot, record.valueHandle())) {
                    throw new WrongTypeException();
                }
                YierdisHyperLogLog.mergeHllIntoRegisters(stringRoot.slice(record.valueHandle()), registers);
                continue;
            }
            if (!YierdisHyperLogLog.isHllString(object)) {
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(object.stringBytesView(), registers);
        }

        byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        long now = System.currentTimeMillis();
        long upperBound = estimatePfmergeUpperBound(destKeyBytes, mergedDense.length);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Void>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Void>> apply() {
                final long[] deltaBytes = new long[]{0};
                final boolean[] changed = new boolean[]{false};
                keyLifecycle.computeObjectWithHandle(destKeyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        YierdisObject next = YierdisObject.newString(stringRoot, mergedDense);
                        keyLifecycle.touch(next);
                        refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        changed[0] = true;
                        return next;
                    }

                    old.overwriteWithString(stringRoot, mergedDense);
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    changed[0] = true;
                    return old;
                });
                keyLifecycle.removeExpire(destKeyBytes);
                MutationOutcome outcome = changed[0] ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(null, outcome),
                        deltaBytes[0]
                );
            }
        });
    }

    private long estimatePfaddUpperBound(byte[] keyBytes, List<byte[]> elements) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            int upperValueLength = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
            return YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        if (YierdisHyperLogLog.isDense(existing)) {
            return 0L;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        int existingLen = canReadFromRoot(existing, record)
                ? stringRoot.length(record.valueHandle())
                : existing.rawLen;
        int sparseUpperBound = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
        int targetLength = Math.min(
                YierdisHyperLogLog.denseLength(),
                Math.max(existingLen, existingLen + sparseUpperBound - YierdisHyperLogLog.HEADER_BYTES)
        );
        return Math.max(0L, (long) targetLength - existingLen);
    }

    private long estimatePfmergeUpperBound(byte[] keyBytes, int mergedDenseLength) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        int existingLen = canReadFromRoot(existing, record)
                ? stringRoot.length(record.valueHandle())
                : existing.rawLen;
        return Math.max(0L, (long) mergedDenseLength - existingLen);
    }

    private void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
        if (object == null) {
            return;
        }
        object.estimatedBytes = entryBytesEstimator.applyAsLong(keyHandle, object);
    }

    private boolean canReadFromRoot(YierdisObject object, EntryRecord record) {
        return object != null
                && object.hasStringRoot()
                && record != null
                && record.type() == ValueType.STRING
                && record.valueHandle() != null
                && object.valueHandle() != null
                && record.valueHandle().raw() == object.valueHandle().raw();
    }
}
