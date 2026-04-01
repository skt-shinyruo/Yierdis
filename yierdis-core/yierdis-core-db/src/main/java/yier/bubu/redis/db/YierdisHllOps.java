package yier.bubu.redis.db;

import yier.bubu.redis.ops.HllReadOps;
import yier.bubu.redis.ops.HllWriteOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.List;
import java.util.Objects;

final class YierdisHllOps implements HllReadOps, HllWriteOps {
    private final YierdisDbInternals internals;

    YierdisHllOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
    }

    @Override
    public int pfadd(byte[] keyBytes, List<byte[]> elements) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimatePfaddUpperBound(keyBytes, elements);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final boolean[] changed = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                internals.store().computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        old = YierdisObject.newString(internals.offHeapAllocator(), YierdisHyperLogLog.newSparse());
                        internals.touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        internals.touch(old);
                    }

                    changed[0] = YierdisHyperLogLog.pfAdd(old, internals.offHeapAllocator(), elements);

                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changed[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(changed[0] ? 1 : 0, deltaBytes[0]);
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
            YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
            if (object == null) {
                continue;
            }
            if (object.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            if (!YierdisHyperLogLog.isHllString(object)) {
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(object.stringBytesView(), registers);
        }
        return YierdisHyperLogLog.estimateCardinality(registers);
    }

    @Override
    public void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
        internals.checkThread();
        if (sourceKeys == null || sourceKeys.isEmpty()) {
            throw new IllegalArgumentException("sourceKeys must not be empty");
        }

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        for (byte[] keyBytes : sourceKeys) {
            YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
            if (object == null) {
                continue;
            }
            if (object.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            if (!YierdisHyperLogLog.isHllString(object)) {
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(object.stringBytesView(), registers);
        }

        byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        long now = System.currentTimeMillis();
        long upperBound = estimatePfmergeUpperBound(destKeyBytes, mergedDense.length);
        internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                final long[] deltaBytes = new long[]{0};
                internals.store().computeWithHandle(destKeyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        YierdisObject next = YierdisObject.newString(internals.offHeapAllocator(), mergedDense);
                        internals.touch(next);
                        internals.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }

                    old.overwriteWithString(internals.offHeapAllocator(), mergedDense);
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                internals.removeExpire(destKeyBytes);
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
            }
        });
    }

    private long estimatePfaddUpperBound(byte[] keyBytes, List<byte[]> elements) {
        YierdisObject existing = internals.getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            int upperValueLength = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
            return YierdisDb.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        if (YierdisHyperLogLog.isDense(existing)) {
            return 0L;
        }
        int sparseUpperBound = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
        int targetLength = Math.min(
                YierdisHyperLogLog.denseLength(),
                Math.max(existing.rawLen, existing.rawLen + sparseUpperBound - YierdisHyperLogLog.HEADER_BYTES)
        );
        return Math.max(0L, (long) targetLength - existing.rawLen);
    }

    private long estimatePfmergeUpperBound(byte[] keyBytes, int mergedDenseLength) {
        YierdisObject existing = internals.getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return YierdisDb.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        return Math.max(0L, (long) mergedDenseLength - existing.rawLen);
    }
}
