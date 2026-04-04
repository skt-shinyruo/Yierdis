package yier.bubu.redis.db;

import yier.bubu.redis.ops.HashReadOps;
import yier.bubu.redis.ops.HashWriteOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

final class YierdisHashOps implements HashReadOps, HashWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisHashOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public long hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        internals.checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateHashWriteUpperBoundForMutation(keyBytes, fieldValuePairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                var memoryRuntime = keyLifecycle.memoryRuntime();
                final int[] added = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    if (old == null) {
                        HashValue hv = new HashValue(memoryRuntime);
                        added[0] = hv.hsetMany(fieldValuePairs);
                        YierdisObject next = YierdisObject.newHash(hv);
                        keyLifecycle.touch(next);
                        internals.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    if (old.type != ValueType.HASH) {
                        throw new WrongTypeException();
                    }
                    added[0] = ((HashValue) old.payload).hsetMany(fieldValuePairs);
                    old.refreshCompositeEncodingFromPayload();
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        internals.checkThread();
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return null;
        }
        if (object.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) object.payload).hget(fieldBytes);
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
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) object.payload).size();
    }

    @Override
    public long hdel(byte[] keyBytes, List<byte[]> fields) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final int[] removed = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old.estimatedBytes;
                    if (keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (old.type != ValueType.HASH) {
                        throw new WrongTypeException();
                    }
                    HashValue hv = (HashValue) old.payload;
                    removed[0] = hv.hdel(fields);
                    if (hv.size() == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    old.refreshCompositeEncodingFromPayload();
                    keyLifecycle.touch(old);
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes - oldEstimate;
                    return old;
                });
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    private int hgetallCount(byte[] keyBytes) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) object.payload).hgetallCount();
    }

    private void hgetallWriteTo(byte[] keyBytes, BulkStringSink out) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return;
        }
        if (object.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        ((HashValue) object.payload).hgetallPairsInto(out);
    }

    private long estimateHashWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return YierdisDb.estimateHashWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, fieldValuePairs);
        }
        if (existing.type != ValueType.HASH) {
            return 0L;
        }
        return YierdisDb.sumByteLengths(fieldValuePairs);
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
}
