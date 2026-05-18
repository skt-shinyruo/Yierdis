package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class YierdisListOps implements ListReadOps, ListWriteOps {
    private static final long LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE = 32L;

    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ListRoot listRoot;

    YierdisListOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.listRoot = Objects.requireNonNull(keyLifecycle.listRoot(), "listRoot");
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
        return popInternal(keyBytes, count, true);
    }

    @Override
    public WriteResult<List<byte[]>> rpop(byte[] keyBytes, int count) {
        internals.checkThread();
        return popInternal(keyBytes, count, false);
    }

    private WriteResult<Long> pushInternal(byte[] keyBytes, List<byte[]> values, boolean left, long upperBound) {
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] len = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (current != null && keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        current = null;
                        oldEstimate = 0L;
                    }

                    ValueHandle handle;
                    if (current == null) {
                        handle = listRoot.create();
                    } else {
                        requireList(current);
                        handle = requireListHandle(current);
                    }

                    boolean ok = false;
                    try {
                        if (left) {
                            listRoot.lpush(handle, values);
                        } else {
                            listRoot.rpush(handle, values);
                        }
                        len[0] = listRoot.size(handle);
                        EntryRecord next = listRecord(k, handle, current == null ? -1L : current.expireAtMillis(), current);
                        deltaBytes[0] -= oldEstimate;
                        deltaBytes[0] += estimateRecordBytes(k, next);
                        ok = true;
                        return next;
                    } finally {
                        if (!ok && current == null) {
                            listRoot.release(handle);
                        }
                    }
                });
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of((long) len[0], MutationOutcome.VALUE_CHANGED),
                        deltaBytes[0]
                );
            }
        });
    }

    private int lrangeCount(byte[] keyBytes, int start, int stop) {
        EntryRecord record = liveListRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return listRoot.rangeCount(requireListHandle(record), start, stop);
    }

    private void lrangeWriteTo(byte[] keyBytes, int start, int stop, BulkStringSink out) {
        EntryRecord record = liveListRecord(keyBytes);
        if (record == null) {
            return;
        }
        listRoot.rangeInto(requireListHandle(record), start, stop, out);
    }

    private WriteResult<List<byte[]>> popInternal(byte[] keyBytes, int count, boolean left) {
        if (count == 0) {
            return WriteResult.unchanged(Collections.emptyList());
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        long now = System.currentTimeMillis();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<List<byte[]>>> apply() {
                final List<byte[]>[] popped = new List[]{null};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeIfPresentWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    requireList(current);
                    ValueHandle handle = requireListHandle(current);
                    popped[0] = left ? listRoot.lpop(handle, count) : listRoot.rpop(handle, count);
                    if (listRoot.size(handle) == 0) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    EntryRecord next = listRecord(k, handle, current.expireAtMillis(), current);
                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    return next;
                });
                WriteResult<List<byte[]>> result = popped[0] != null && !popped[0].isEmpty()
                        ? WriteResult.of(popped[0], MutationOutcome.VALUE_CHANGED)
                        : WriteResult.unchanged(popped[0]);
                return YierdisDbMutationExecutor.MutationResult.of(result, deltaBytes[0]);
            }
        });
    }

    private long estimateListWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> values) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return estimateListWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, values);
        }
        if (existing.type() != ValueType.LIST) {
            return 0L;
        }
        return estimateListWriteUpperBound(0, values);
    }

    private EntryRecord liveListRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireList(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord listRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.LIST,
                listRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireListHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!listRoot.contains(handle)) {
            throw new IllegalStateException("native list value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireList(EntryRecord record) {
        if (record.type() != ValueType.LIST) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
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
