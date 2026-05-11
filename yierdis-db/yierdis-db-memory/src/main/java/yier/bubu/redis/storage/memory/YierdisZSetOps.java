package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class YierdisZSetOps implements ZSetReadOps, ZSetWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ZSetRoot zsetRoot;

    YierdisZSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
        this.zsetRoot = Objects.requireNonNull(keyLifecycle.zsetRoot(), "zsetRoot");
    }

    @Override
    public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        internals.checkThread();
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] added = new int[]{0};
                final boolean[] changedAny = new boolean[]{false};
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
                        handle = zsetRoot.create();
                    } else {
                        requireZSet(current);
                        handle = requireZSetHandle(current);
                    }

                    boolean ok = false;
                    try {
                        added[0] = zsetRoot.zadd(handle, scoreMemberPairs, changedAny);
                        EntryRecord next = zsetRecord(k, handle, current == null ? -1L : current.expireAtMillis(), current);
                        deltaBytes[0] -= oldEstimate;
                        deltaBytes[0] += estimateRecordBytes(k, next);
                        ok = true;
                        return next;
                    } finally {
                        if (!ok && current == null) {
                            zsetRoot.release(handle);
                        }
                    }
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
        return removeInternal(keyBytes, now, handle -> zsetRoot.zrem(handle, members));
    }

    @Override
    public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return removeInternal(keyBytes, now, handle -> zsetRoot.zremrangeByRank(handle, start, stop));
    }

    @Override
    public WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        return removeInternal(keyBytes, now, handle -> zsetRoot.zremrangeByScore(handle, min, minExclusive, max, maxExclusive));
    }

    private WriteResult<Long> removeInternal(byte[] keyBytes, long now, ZSetRemoval removal) {
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                final int[] removed = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                keyLifecycle.computeIfPresentWithHandle(keyBytes, (k, oldRecord) -> {
                    EntryRecord current = oldRecord;
                    long oldEstimate = estimateRecordBytes(k, current);
                    if (keyLifecycle.isKeyExpired(k, now)) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    requireZSet(current);
                    ValueHandle handle = requireZSetHandle(current);
                    removed[0] = removal.remove(handle);
                    if (zsetRoot.size(handle) == 0) {
                        keyLifecycle.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        return null;
                    }
                    EntryRecord next = zsetRecord(k, handle, current.expireAtMillis(), current);
                    deltaBytes[0] -= oldEstimate;
                    deltaBytes[0] += estimateRecordBytes(k, next);
                    return next;
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
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrangeCount(requireZSetHandle(record), start, stop, withScores);
    }

    private void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrangeWriteTo(requireZSetHandle(record), start, stop, withScores, out);
    }

    private int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrevrangeCount(requireZSetHandle(record), start, stop, withScores);
    }

    private void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrevrangeWriteTo(requireZSetHandle(record), start, stop, withScores, out);
    }

    private int zrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrangeByScoreCount(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrangeByScoreWriteTo(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private int zrevrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return 0;
        }
        return zsetRoot.zrevrangeByScoreCount(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrevrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        EntryRecord record = liveZSetRecord(keyBytes);
        if (record == null) {
            return;
        }
        zsetRoot.zrevrangeByScoreWriteTo(requireZSetHandle(record), min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type() != ValueType.ZSET) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.sumZSetMemberByteLengths(scoreMemberPairs);
    }

    private EntryRecord liveZSetRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        requireZSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord zsetRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.ZSET,
                zsetRoot.encoding(handle),
                expireAtMillis,
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE,
                previous
        );
    }

    private ValueHandle requireZSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!zsetRoot.contains(handle)) {
            throw new IllegalStateException("native zset value handle is not available: " + (handle == null ? "null" : handle.raw()));
        }
        return handle;
    }

    private static void requireZSet(EntryRecord record) {
        if (record.type() != ValueType.ZSET) {
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

    @FunctionalInterface
    private interface BulkEmitter {
        void emitTo(BulkStringSink out);
    }

    @FunctionalInterface
    private interface ZSetRemoval {
        int remove(ValueHandle handle);
    }
}
