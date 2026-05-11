package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.ToLongBiFunction;

public final class YierdisListOps implements ListReadOps, ListWriteOps {
    private static final long LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE = 32L;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator;

    YierdisListOps(YierdisDbInternals internals, ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.entryBytesEstimator = Objects.requireNonNull(entryBytesEstimator, "entryBytesEstimator");
    }

    @Override
    public WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        long upperBound = estimateListWriteUpperBoundForMutation(keyBytes, values);
        return pushInternal(keyBytes, values, true, upperBound);
    }

    @Override
    public WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        long upperBound = estimateListWriteUpperBoundForMutation(keyBytes, values);
        return pushInternal(keyBytes, values, false, upperBound);
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
    public WriteResult<List<byte[]>> lpop(byte[] keyBytes, int count) {
        internals.checkThread();
        return popInternal(keyBytes, count, true, 0L);
    }

    @Override
    public WriteResult<List<byte[]>> rpop(byte[] keyBytes, int count) {
        internals.checkThread();
        return popInternal(keyBytes, count, false, 0L);
    }

    private WriteResult<Long> pushInternal(byte[] keyBytes, List<byte[]> values, boolean left, long upperBound) {
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                var memoryRuntime = keyLifecycle.memoryRuntime();
                final int[] len = new int[]{0};
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
                        ListValue lv = new ListValue(memoryRuntime);
                        if (left) {
                            lv.lpushAll(values);
                        } else {
                            lv.rpushAll(values);
                        }
                        len[0] = lv.size();
                        YierdisObject next = YierdisObject.newList(lv);
                        keyLifecycle.touch(next);
                        refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }

                    if (old.type != ValueType.LIST) {
                        throw new WrongTypeException();
                    }
                    ListValue lv = (ListValue) old.payload;
                    if (left) {
                        lv.lpushAll(values);
                    } else {
                        lv.rpushAll(values);
                    }
                    len[0] = lv.size();
                    old.refreshCompositeEncodingFromPayload();
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) len[0], MutationOutcome.VALUE_CHANGED),
                        deltaBytes[0]
                );
            }
        });
    }

    private int lrangeCount(byte[] keyBytes, int start, int stop) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        return ((ListValue) object.payload).rangeCount(start, stop);
    }

    private void lrangeWriteTo(byte[] keyBytes, int start, int stop, BulkStringSink out) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return;
        }
        if (object.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        ((ListValue) object.payload).rangeInto(start, stop, out);
    }

    private WriteResult<List<byte[]>> popInternal(byte[] keyBytes, int count, boolean left, long upperBound) {
        if (count == 0) {
            return WriteResult.unchanged(Collections.emptyList());
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<List<byte[]>>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<List<byte[]>>> apply() {
                final List<byte[]>[] popped = new List[]{null};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeObjectIfPresentWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old.estimatedBytes;
                    if (keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (old.type != ValueType.LIST) {
                        throw new WrongTypeException();
                    }
                    ListValue lv = (ListValue) old.payload;
                    popped[0] = left ? lv.lpop(count) : lv.rpop(count);
                    if (lv.size() == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    old.refreshCompositeEncodingFromPayload();
                    keyLifecycle.touch(old);
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes - oldEstimate;
                    return old;
                });
                WriteResult<List<byte[]>> result = popped[0] != null && !popped[0].isEmpty()
                        ? WriteResult.of(popped[0], MutationOutcome.VALUE_CHANGED)
                        : WriteResult.unchanged(popped[0]);
                return YierdisDbMutationExecutor.MutationResult.of(result, deltaBytes[0]);
            }
        });
    }

    private long estimateListWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> values) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return estimateListWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, values);
        }
        if (existing.type != ValueType.LIST) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.sumByteLengths(values);
    }

    private void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
        if (object == null) {
            return;
        }
        object.estimatedBytes = entryBytesEstimator.applyAsLong(keyHandle, object);
    }

    private static long estimateListWriteUpperBound(int keyLength, List<byte[]> values) {
        int itemCount = values == null ? 0 : values.size();
        return YierdisDbMemoryEstimator.estimateCollectionWriteUpperBound(
                keyLength,
                YierdisDbMemoryEstimator.sumByteLengths(values),
                Math.multiplyExact((long) itemCount, LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE)
        );
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
}
