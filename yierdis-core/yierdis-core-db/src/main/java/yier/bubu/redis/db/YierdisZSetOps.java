package yier.bubu.redis.db;

import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.ZSetReadOps;
import yier.bubu.redis.ops.ZSetWriteOps;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;

final class YierdisZSetOps implements ZSetReadOps, ZSetWriteOps {
    private final YierdisDbInternals internals;

    YierdisZSetOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
    }

    @Override
    public long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        internals.checkThread();
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                var memoryRuntime = internals.memoryRuntime();
                final int[] added = new int[]{0};
                final boolean[] changedAny = new boolean[]{false};
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
                        ZSetValue zv = new ZSetValue(memoryRuntime);
                        try {
                            added[0] = zv.zaddMany(scoreMemberPairs, changedAny);
                        } catch (RuntimeException e) {
                            zv.close();
                            throw e;
                        }
                        YierdisObject next = YierdisObject.newZSet(zv);
                        internals.touch(next);
                        internals.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs, changedAny);
                    old.refreshCompositeEncodingFromPayload();
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changedAny[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
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
    public long zrem(byte[] keyBytes, List<byte[]> members) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ZSetValue zv = (ZSetValue) old.payload;
                    removed[0] = zv.zrem(members);
                    if (zv.size() == 0) {
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
    public long zremrangeByRank(byte[] keyBytes, long start, long stop) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ZSetValue zv = (ZSetValue) old.payload;
                    removed[0] = zv.zremrangeByRank(start, stop);
                    if (zv.size() == 0) {
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
    public long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
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
                    if (old.type != ValueType.ZSET) {
                        throw new WrongTypeException();
                    }
                    ZSetValue zv = (ZSetValue) old.payload;
                    removed[0] = zv.zremrangeByScore(min, minExclusive, max, maxExclusive);
                    if (zv.size() == 0) {
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

    private int zrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        return ((ZSetValue) object.payload).zrangeCount(start, stop, withScores);
    }

    private void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ((ZSetValue) object.payload).zrangeWriteTo(start, stop, withScores, out);
    }

    private int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        return ((ZSetValue) object.payload).zrevrangeCount(start, stop, withScores);
    }

    private void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ((ZSetValue) object.payload).zrevrangeWriteTo(start, stop, withScores, out);
    }

    private int zrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        return ((ZSetValue) object.payload).zrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ((ZSetValue) object.payload).zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private int zrevrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return 0;
        }
        return ((ZSetValue) object.payload).zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    private void zrevrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        YierdisObject object = readZSet(keyBytes);
        if (object == null) {
            return;
        }
        ((ZSetValue) object.payload).zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private YierdisObject readZSet(byte[] keyBytes) {
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return null;
        }
        if (object.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return object;
    }

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        YierdisObject existing = internals.getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return YierdisDb.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type != ValueType.ZSET) {
            return 0L;
        }
        long memberBytes = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                byte[] member = scoreMemberPairs.get(i);
                if (member != null) {
                    memberBytes += member.length;
                }
            }
        }
        return memberBytes;
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
