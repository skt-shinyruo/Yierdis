package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringMapPairs;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.ToLongBiFunction;

public final class YierdisHashOps implements HashReadOps, HashWriteOps {
    private static final long HASH_PAIR_OVERHEAD_BYTES_ESTIMATE = 64L;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashRoot hashRoot;
    private final ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator;

    YierdisHashOps(YierdisDbInternals internals, ToLongBiFunction<KeyHandle, YierdisObject> entryBytesEstimator) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.hashRoot = Objects.requireNonNull(keyLifecycle.hashRoot(), "hashRoot");
        this.entryBytesEstimator = Objects.requireNonNull(entryBytesEstimator, "entryBytesEstimator");
    }

    @Override
    public WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        internals.checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateHashWriteUpperBoundForMutation(keyBytes, fieldValuePairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
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
                        YierdisObject next = YierdisObject.newHash(hashRoot);
                        boolean ok = false;
                        try {
                            ValueHandle handle = next.valueHandle();
                            added[0] = hashRoot.hsetMany(handle, fieldValuePairs);
                            next.useHashHandle(hashRoot, handle);
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
                    if (old.type != ValueType.HASH) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = hashHandle(old, k);
                    if (handle == null && old.payload instanceof HashValue) {
                        old.moveHashToRoot(hashRoot);
                        handle = old.valueHandle();
                    }
                    if (handle != null) {
                        added[0] = hashRoot.hsetMany(handle, fieldValuePairs);
                        old.useHashHandle(hashRoot, handle);
                    } else {
                        added[0] = ((HashValue) old.payload).hsetMany(fieldValuePairs);
                        old.refreshCompositeEncodingFromPayload();
                    }
                    keyLifecycle.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) added[0], MutationOutcome.VALUE_CHANGED),
                        deltaBytes[0]
                );
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
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        ValueHandle handle = readableHashHandle(object, record);
        if (handle != null) {
            return hashRoot.hget(handle, fieldBytes);
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
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        ValueHandle handle = readableHashHandle(object, record);
        if (handle != null) {
            return hashRoot.size(handle);
        }
        return ((HashValue) object.payload).size();
    }

    @Override
    public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
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
                    if (old.type != ValueType.HASH) {
                        throw new WrongTypeException();
                    }
                    ValueHandle handle = hashHandle(old, k);
                    if (handle == null && old.payload instanceof HashValue) {
                        old.moveHashToRoot(hashRoot);
                        handle = old.valueHandle();
                    }
                    int size;
                    if (handle != null) {
                        removed[0] = hashRoot.hdel(handle, fields);
                        size = hashRoot.size(handle);
                    } else {
                        HashValue hv = (HashValue) old.payload;
                        removed[0] = hv.hdel(fields);
                        size = hv.size();
                    }
                    if (size == 0) {
                        old.releasePayloadIfAny();
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    if (handle != null) {
                        old.useHashHandle(hashRoot, handle);
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

    private int hgetallCount(byte[] keyBytes) {
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        ValueHandle handle = readableHashHandle(object, record);
        if (handle != null) {
            return hashRoot.hgetallCount(handle);
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
        EntryRecord record = keyLifecycle.entryRecord(keyBytes);
        ValueHandle handle = readableHashHandle(object, record);
        if (handle != null) {
            hashRoot.hgetallPairsInto(handle, out);
            return;
        }
        ((HashValue) object.payload).hgetallPairsInto(out);
    }

    private long estimateHashWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        YierdisObject existing = keyLifecycle.getLiveObject(keyBytes);
        if (existing == null) {
            return estimateHashWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, fieldValuePairs);
        }
        if (existing.type != ValueType.HASH) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.sumByteLengths(fieldValuePairs);
    }

    private void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
        if (object == null) {
            return;
        }
        object.estimatedBytes = entryBytesEstimator.applyAsLong(keyHandle, object);
    }

    private ValueHandle hashHandle(YierdisObject object, KeyHandle keyHandle) {
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        return readableHashHandle(object, record);
    }

    private ValueHandle readableHashHandle(YierdisObject object, EntryRecord record) {
        if (canReadFromRoot(object, record)) {
            return record.valueHandle();
        }
        if (object != null && object.hasHashRoot()) {
            return object.valueHandle();
        }
        return null;
    }

    private boolean canReadFromRoot(YierdisObject object, EntryRecord record) {
        return object != null
                && object.hasHashRoot()
                && record != null
                && record.type() == ValueType.HASH
                && record.valueHandle() != null
                && object.valueHandle() != null
                && record.valueHandle().raw() == object.valueHandle().raw();
    }

    private static long estimateHashWriteUpperBound(int keyLength, List<byte[]> fieldValuePairs) {
        int pairCount = fieldValuePairs == null ? 0 : fieldValuePairs.size() / 2;
        return YierdisDbMemoryEstimator.estimateCollectionWriteUpperBound(
                keyLength,
                YierdisDbMemoryEstimator.sumByteLengths(fieldValuePairs),
                Math.multiplyExact((long) pairCount, HASH_PAIR_OVERHEAD_BYTES_ESTIMATE)
        );
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
