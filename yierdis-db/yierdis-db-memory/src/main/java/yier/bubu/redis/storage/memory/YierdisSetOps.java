package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.EntryMutationResult;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class YierdisSetOps implements SetReadOps, SetWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final SetRoot setRoot;

    YierdisSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.setRoot = Objects.requireNonNull(keyLifecycle.setRoot(), "setRoot");
    }

    @Override
    public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateSetWriteUpperBoundForMutation(keyBytes, members);
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
                        handle = setRoot.create();
                    } else {
                        requireSet(current);
                        handle = requireSetHandle(current);
                    }

                    boolean ok = false;
                    try {
                        int added = setRoot.sadd(handle, members);
                        EntryRecord next = setRecord(k, handle, current == null ? -1L : current.expireAtMillis(), current);
                        deltaBytes -= oldEstimate;
                        deltaBytes += estimateRecordBytes(k, next);
                        ok = true;
                        MutationOutcome outcome = added > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                        return mutationResult(next, WriteResult.of((long) added, outcome), deltaBytes);
                    } finally {
                        if (!ok && current == null) {
                            setRoot.release(handle);
                        }
                    }
                });
            }
        });
    }

    @Override
    public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
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
                            requireSet(current);
                            ValueHandle handle = requireSetHandle(current);
                            int removed = setRoot.srem(handle, members);
                            MutationOutcome outcome = removed > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                            if (setRoot.size(handle) == 0) {
                                keyLifecycle.removeExpire(k);
                                deltaBytes -= oldEstimate;
                                return mutationResult(null, WriteResult.of((long) removed, outcome), deltaBytes);
                            }
                            EntryRecord next = setRecord(k, handle, current.expireAtMillis(), current);
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
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return false;
        }
        return setRoot.contains(requireSetHandle(record), member);
    }

    @Override
    public long scard(byte[] keyBytes) {
        internals.checkThread();
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return setRoot.size(requireSetHandle(record));
    }

    private int smembersCount(byte[] keyBytes) {
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return setRoot.size(requireSetHandle(record));
    }

    private void smembersWriteTo(byte[] keyBytes, BulkStringSink out) {
        EntryRecord record = liveSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        setRoot.membersInto(requireSetHandle(record), out);
    }

    private long estimateSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> members) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members);
        }
        if (existing.type() != ValueType.SET) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.estimateSetWriteUpperBound(0, members);
    }

    private EntryRecord liveSetRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord setRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.SET,
                setRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!setRoot.contains(handle)) {
            throw new IllegalStateException("native set value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireSet(EntryRecord record) {
        if (record.type() != ValueType.SET) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
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
