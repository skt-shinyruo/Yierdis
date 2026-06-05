package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringMapPairs;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.EntryMutationResult;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class YierdisHashOps implements HashReadOps, HashWriteOps {
    private static final long HASH_PAIR_OVERHEAD_BYTES_ESTIMATE = 64L;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashRoot hashRoot;

    YierdisHashOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.hashRoot = Objects.requireNonNull(keyLifecycle.hashRoot(), "hashRoot");
    }

    @Override
    public WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        internals.checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateHashWriteUpperBoundForMutation(keyBytes, fieldValuePairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
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
                        handle = hashRoot.create();
                    } else {
                        requireHash(current);
                        handle = requireHashHandle(current);
                    }

                    boolean ok = false;
                    try {
                        int added = hashRoot.hsetMany(handle, fieldValuePairs);
                        EntryRecord next = hashRecord(k, handle, current == null ? -1L : current.expireAtMillis(), current);
                        deltaBytes -= oldEstimate;
                        deltaBytes += estimateRecordBytes(k, next);
                        ok = true;
                        return mutationResult(
                                next,
                                WriteResult.of((long) added, MutationOutcome.VALUE_CHANGED),
                                deltaBytes
                        );
                    } finally {
                        if (!ok && current == null) {
                            hashRoot.release(handle);
                        }
                    }
                });
            }
        });
    }

    @Override
    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        internals.checkThread();
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return null;
        }
        return hashRoot.hget(requireHashHandle(record), fieldBytes);
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
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return hashRoot.size(requireHashHandle(record));
    }

    @Override
    public WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> mutation =
                        keyLifecycle.computeIfPresentWithHandleResult(keyBytes, (k, oldRecord) -> {
                            EntryRecord current = oldRecord;
                            long oldEstimate = estimateRecordBytes(k, current);
                            long deltaBytes = 0L;
                            if (keyLifecycle.isKeyExpired(k, now)) {
                                keyLifecycle.removeExpire(k);
                                deltaBytes -= oldEstimate;
                                return mutationResult(null, WriteResult.of(0L, MutationOutcome.NONE), deltaBytes);
                            }
                            requireHash(current);
                            ValueHandle handle = requireHashHandle(current);
                            int removed = hashRoot.hdel(handle, fields);
                            MutationOutcome outcome = removed > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                            if (hashRoot.size(handle) == 0) {
                                keyLifecycle.removeExpire(k);
                                deltaBytes -= oldEstimate;
                                return mutationResult(null, WriteResult.of((long) removed, outcome), deltaBytes);
                            }
                            EntryRecord next = hashRecord(k, handle, current.expireAtMillis(), current);
                            deltaBytes -= oldEstimate;
                            deltaBytes += estimateRecordBytes(k, next);
                            return mutationResult(next, WriteResult.of((long) removed, outcome), deltaBytes);
                        });
                return mutation == null
                        ? YierdisDbMutationExecutor.MutationResult.of(WriteResult.of(0L, MutationOutcome.NONE), 0L)
                        : mutation;
            }
        });
    }

    private int hgetallCount(byte[] keyBytes) {
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return hashRoot.hgetallCount(requireHashHandle(record));
    }

    private void hgetallWriteTo(byte[] keyBytes, BulkStringSink out) {
        EntryRecord record = liveHashRecord(keyBytes);
        if (record == null) {
            return;
        }
        hashRoot.hgetallPairsInto(requireHashHandle(record), out);
    }

    private long estimateHashWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return estimateHashWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, fieldValuePairs);
        }
        if (existing.type() != ValueType.HASH) {
            return 0L;
        }
        return estimateHashWriteUpperBound(0, fieldValuePairs);
    }

    private EntryRecord liveHashRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireHash(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord hashRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.HASH,
                hashRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireHashHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!hashRoot.contains(handle)) {
            throw new IllegalStateException("native hash value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireHash(EntryRecord record) {
        if (record.type() != ValueType.HASH) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
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

    private static <T> EntryMutationResult<YierdisDbMutationExecutor.MutationResult<T>> mutationResult(
            EntryRecord record,
            T value,
            long deltaBytes
    ) {
        return EntryMutationResult.of(record, YierdisDbMutationExecutor.MutationResult.of(value, deltaBytes));
    }

    @FunctionalInterface
    private interface BulkEmitter {
        void emitTo(BulkStringSink out);
    }
}
