package yier.bubu.redis.db;

import yier.bubu.redis.ops.ListReadOps;
import yier.bubu.redis.ops.ListWriteOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

final class YierdisListOps implements ListReadOps, ListWriteOps {
    private final YierdisDbInternals internals;

    YierdisListOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
    }

    @Override
    public long lpush(byte[] keyBytes, List<byte[]> values) {
        internals.checkThread();
        long upperBound = estimateListWriteUpperBoundForMutation(keyBytes, values);
        return pushInternal(keyBytes, values, true, upperBound);
    }

    @Override
    public long rpush(byte[] keyBytes, List<byte[]> values) {
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
    public List<byte[]> lpop(byte[] keyBytes, int count) {
        internals.checkThread();
        return popInternal(keyBytes, count, true, 0L);
    }

    @Override
    public List<byte[]> rpop(byte[] keyBytes, int count) {
        internals.checkThread();
        return popInternal(keyBytes, count, false, 0L);
    }

    private int pushInternal(byte[] keyBytes, List<byte[]> values, boolean left, long upperBound) {
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                var memoryRuntime = internals.memoryRuntime();
                final int[] len = new int[]{0};
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
                        ListValue lv = new ListValue(memoryRuntime);
                        if (left) {
                            lv.lpushAll(values);
                        } else {
                            lv.rpushAll(values);
                        }
                        len[0] = lv.size();
                        YierdisObject next = YierdisObject.newList(lv);
                        internals.touch(next);
                        internals.refreshEstimatedBytes(k, next);
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
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(len[0], deltaBytes[0]);
            }
        });
    }

    private int lrangeCount(byte[] keyBytes, int start, int stop) {
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        return ((ListValue) object.payload).rangeCount(start, stop);
    }

    private void lrangeWriteTo(byte[] keyBytes, int start, int stop, BulkStringSink out) {
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return;
        }
        if (object.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        ((ListValue) object.payload).rangeInto(start, stop, out);
    }

    private List<byte[]> popInternal(byte[] keyBytes, int count, boolean left, long upperBound) {
        if (count == 0) {
            return Collections.emptyList();
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<List<byte[]>> apply() {
                final List<byte[]>[] popped = new List[]{null};
                final long[] deltaBytes = new long[]{0};
                internals.store().computeIfPresentWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old.estimatedBytes;
                    if (internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
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
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    old.refreshCompositeEncodingFromPayload();
                    internals.touch(old);
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes - oldEstimate;
                    return old;
                });
                if (popped[0] != null && !popped[0].isEmpty()) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(popped[0], deltaBytes[0]);
            }
        });
    }

    private long estimateListWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> values) {
        YierdisObject existing = internals.getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return YierdisDb.estimateListWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, values);
        }
        if (existing.type != ValueType.LIST) {
            return 0L;
        }
        return YierdisDb.sumByteLengths(values);
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
