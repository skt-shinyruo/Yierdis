package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.ToLongBiFunction;

public final class YierdisZSetOps implements ZSetReadOps, ZSetWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ZSetRoot zsetRoot;
    private final ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator;

    YierdisZSetOps(YierdisDbInternals internals, ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.zsetRoot = Objects.requireNonNull(keyLifecycle.zsetRoot(), "zsetRoot");
        this.entryBytesEstimator = Objects.requireNonNull(entryBytesEstimator, "entryBytesEstimator");
    }

    @Override
    public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        internals.checkThread();
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] added = new int[]{0};
                final boolean[] changedAny = new boolean[]{false};
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
                        YierdisObject next = YierdisObject.newZSet(zsetRoot);
                        boolean ok = false;
                        try {
                            ValueHandle handle = next.valueHandle();
                            added[0] = zsetRoot.zadd(handle, scoreMemberPairs, changedAny);
                            next.useZSetHandle(zsetRoot, handle);
                            keyLifecycle.touch(next);
                            refreshEstimatedBytes(k, next);
                            deltaBytes[0] += next.estimatedBytes;
                            ok = true;
                            return next;
                        } finally {
                            if (!ok) {
                                next.releasePayloadIfAny();
                            }
                        }
                    }
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = zsetHandle(old, k);
                    if (handle == null && old.payload instanceof ZSetValue) {
                        old.moveZSetToRoot(zsetRoot);
                        handle = old.valueHandle();
                    }
                    if (handle != null) {
                        added[0] = zsetRoot.zadd(handle, scoreMemberPairs, changedAny);
                        old.useZSetHandle(zsetRoot, handle);
                    } else {
                        added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs, changedAny);
                        old.refreshCompositeEncodingFromPayload();
                    }
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                MutationOutcome outcome = changedAny[0] ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) added[0], outcome),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        return sequenceOf(
                () -> zrangeCount(keyBytes, start, stop, withScores),
                out -> zrangeWriteTo(keyBytes, start, stop, withScores, out)
        );
    }

    @Override
    public BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        internals.checkThread();
        return sequenceOf(
                () -> zrevrangeCount(keyBytes, start, stop, withScores),
                out -> zrevrangeWriteTo(keyBytes, start, stop, withScores, out)
        );
    }

    @Override
    public BulkStringSequence zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        internals.checkThread();
        return sequenceOf(
                () -> zrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                out -> zrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
        );
    }

    @Override
    public BulkStringSequence zrevrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        internals.checkThread();
        return sequenceOf(
                () -> zrevrangeByScoreCount(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count),
                out -> zrevrangeByScoreWriteTo(keyBytes, min, minExclusive, max, maxExclusive, withScores, offset, count, out)
        );
    }

    @Override
    public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = zsetHandle(old, k);
                    if (handle == null && old.payload instanceof ZSetValue) {
                        old.moveZSetToRoot(zsetRoot);
                        handle = old.valueHandle();
                    }
                    int size;
                    if (handle != null) {
                        removed[0] = zsetRoot.zrem(handle, members);
                        size = zsetRoot.size(handle);
                    } else {
                        ZSetValue zv = (ZSetValue) old.payload;
                        removed[0] = zv.zrem(members);
                        size = zv.size();
                    }
                    if (size == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (handle != null) {
                        old.useZSetHandle(zsetRoot, handle);
                    } else {
                        old.refreshCompositeEncodingFromPayload();
                    }
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
    public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = zsetHandle(old, k);
                    if (handle == null && old.payload instanceof ZSetValue) {
                        old.moveZSetToRoot(zsetRoot);
                        handle = old.valueHandle();
                    }
                    int size;
                    if (handle != null) {
                        removed[0] = zsetRoot.zremrangeByRank(handle, start, stop);
                        size = zsetRoot.size(handle);
                    } else {
                        ZSetValue zv = (ZSetValue) old.payload;
                        removed[0] = zv.zremrangeByRank(start, stop);
                        size = zv.size();
                    }
                    if (size == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (handle != null) {
                        old.useZSetHandle(zsetRoot, handle);
                    } else {
                        old.refreshCompositeEncodingFromPayload();
                    }
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
    public WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = zsetHandle(old, k);
                    if (handle == null && old.payload instanceof ZSetValue) {
                        old.moveZSetToRoot(zsetRoot);
                        handle = old.valueHandle();
                    }
                    int size;
                    if (handle != null) {
                        removed[0] = zsetRoot.zremrangeByScore(handle, min, minExclusive, max, maxExclusive);
                        size = zsetRoot.size(handle);
                    } else {
                        ZSetValue zv = (ZSetValue) old.payload;
                        removed[0] = zv.zremrangeByScore(min, minExclusive, max, maxExclusive);
                        size = zv.size();
                    }
                    if (size == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (handle != null) {
                        old.useZSetHandle(zsetRoot, handle);
                    } else {
                        old.refreshCompositeEncodingFromPayload();
                    }
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

    private int zrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            return zsetRoot.zrangeCount(handle, start, stop, withScores);
        }
        return ((ZSetValue) object.payload).zrangeCount(start, stop, withScores);
    }

    private void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            zsetRoot.zrangeWriteTo(handle, start, stop, withScores, out);
            return;
        }
        ((ZSetValue) object.payload).zrangeWriteTo(start, stop, withScores, out);
    }

    private int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            return zsetRoot.zrevrangeCount(handle, start, stop, withScores);
        }
        return ((ZSetValue) object.payload).zrevrangeCount(start, stop, withScores);
    }

    private void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            zsetRoot.zrevrangeWriteTo(handle, start, stop, withScores, out);
            return;
        }
        ((ZSetValue) object.payload).zrevrangeWriteTo(start, stop, withScores, out);
    }

    private int zrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            return zsetRoot.zrangeByScoreCount(handle, min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return ((ZSetValue) object.payload).zrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            zsetRoot.zrangeByScoreWriteTo(handle, min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        ((ZSetValue) object.payload).zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private int zrevrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            return zsetRoot.zrevrangeByScoreCount(handle, min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return ((ZSetValue) object.payload).zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrevrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ValueHandle handle = zsetHandle(object, keyBytes);
        if (handle != null) {
            zsetRoot.zrevrangeByScoreWriteTo(handle, min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        ((ZSetValue) object.payload).zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private YierdisObject readZSet(byte[] keyBytes) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return null;
        }
        if (object.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return object;
    }

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type != ValueType.ZSET) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.sumZSetMemberByteLengths(scoreMemberPairs);
    }

    private void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
        if (object == null) {
            return;
        }
        object.estimatedBytes = entryBytesEstimator.applyAsLong(keyHandle, object);
    }

    private ValueHandle zsetHandle(YierdisObject object, byte[] keyBytes) {
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        return readableZSetHandle(object, record);
    }

    private ValueHandle zsetHandle(YierdisObject object, KeyHandle keyHandle) {
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        return readableZSetHandle(object, record);
    }

    private ValueHandle readableZSetHandle(YierdisObject object, EntryRecord record) {
        if (canReadFromRoot(object, record)) {
            return record.valueHandle();
        }
        if (object != null && object.hasZSetRoot()) {
            return object.valueHandle();
        }
        return null;
    }

    private boolean canReadFromRoot(YierdisObject object, EntryRecord record) {
        return object != null
                && object.hasZSetRoot()
                && record != null
                && record.type() == ValueType.ZSET
                && record.valueHandle() != null
                && object.valueHandle() != null
                && record.valueHandle().raw() == object.valueHandle().raw();
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
