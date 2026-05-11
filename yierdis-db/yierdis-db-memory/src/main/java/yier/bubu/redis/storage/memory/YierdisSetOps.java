package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.ToLongBiFunction;

public final class YierdisSetOps implements SetReadOps, SetWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator;

    YierdisSetOps(YierdisDbInternals internals, ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.entryBytesEstimator = Objects.requireNonNull(entryBytesEstimator, "entryBytesEstimator");
    }

    @Override
    public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateSetWriteUpperBoundForMutation(keyBytes, members);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                var memoryRuntime = keyLifecycle.memoryRuntime();
                final int[] added = new int[]{0};
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
                        SetValue sv = new SetValue(memoryRuntime);
                        added[0] = sv.addAll(members);
                        YierdisObject next = YierdisObject.newSet(sv);
                        keyLifecycle.touch(next);
                        refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    if (old.type != ValueType.SET) {
                        throw new WrongTypeException();
                    }
                    added[0] = ((SetValue) old.payload).addAll(members);
                    old.refreshCompositeEncodingFromPayload();
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                MutationOutcome outcome = added[0] > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) added[0], outcome),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] removed = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeObjectIfPresentWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old.estimatedBytes;
                    if (keyLifecycle.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
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
                MutationOutcome outcome = removed[0] > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) removed[0], outcome),
                        deltaBytes[0]
                );
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
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
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
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) object.payload).size();
    }

    private int smembersCount(byte[] keyBytes) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) object.payload).size();
    }

    private void smembersWriteTo(byte[] keyBytes, BulkStringSink out) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return;
        }
        if (object.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        ((SetValue) object.payload).membersInto(out);
    }

    private long estimateSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> members) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members);
        }
        if (existing.type != ValueType.SET) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.sumByteLengths(members);
    }

    private void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
        if (object == null) {
            return;
        }
        object.estimatedBytes = entryBytesEstimator.applyAsLong(keyHandle, object);
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
