package yier.bubu.redis.db;

import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.ops.SetReadOps;
import yier.bubu.redis.ops.SetWriteOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

final class YierdisSetOps implements SetReadOps, SetWriteOps {
    private final YierdisDbInternals internals;

    YierdisSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
    }

    @Override
    public long sadd(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateSetWriteUpperBoundForMutation(keyBytes, members);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                OffHeapAddressAllocator addressAllocator =
                        internals.offHeapAllocator() instanceof OffHeapAddressAllocator a ? a : null;
                final int[] added = new int[]{0};
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
                        SetValue sv = addressAllocator != null ? new SetValue(addressAllocator) : new SetValue();
                        added[0] = sv.addAll(members);
                        YierdisObject next = YierdisObject.newSet(sv);
                        internals.touch(next);
                        internals.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    if (old.type != ValueType.SET) {
                        throw new WrongTypeException();
                    }
                    added[0] = ((SetValue) old.payload).addAll(members);
                    old.refreshCompositeEncodingFromPayload();
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (added[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public long srem(byte[] keyBytes, List<byte[]> members) {
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
                internals.store().computeIfPresentWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old.estimatedBytes;
                    if (internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (old.type != ValueType.SET) {
                        throw new WrongTypeException();
                    }
                    SetValue sv = (SetValue) old.payload;
                    removed[0] = sv.removeAll(members);
                    if (sv.size() == 0) {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public BulkStringSequence smembers(byte[] keyBytes) {
        internals.checkThread();
        return sequenceOf(
                () -> smembersCount(keyBytes),
                out -> smembersWriteTo(keyBytes, out)
        );
    }

    @Override
    public boolean sismember(byte[] keyBytes, byte[] member) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return false;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) object.payload).contains(member);
    }

    @Override
    public long scard(byte[] keyBytes) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) object.payload).size();
    }

    private int smembersCount(byte[] keyBytes) {
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) object.payload).size();
    }

    private void smembersWriteTo(byte[] keyBytes, BulkStringSink out) {
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        ((SetValue) object.payload).membersInto(out);
    }

    private long estimateSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> members) {
        YierdisObject existing = internals.getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return YierdisDb.estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members);
        }
        if (existing.type != ValueType.SET) {
            return 0L;
        }
        return YierdisDb.sumByteLengths(members);
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
