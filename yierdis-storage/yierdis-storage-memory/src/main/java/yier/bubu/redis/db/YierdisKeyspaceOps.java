package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.KeyspaceReadOps;
import yier.bubu.redis.ops.KeyspaceWriteOps;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class YierdisKeyspaceOps implements KeyspaceReadOps, KeyspaceWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisKeyspaceOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public long del(Collection<byte[]> keys) {
        internals.checkThread();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Long>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Long> apply() {
                long now = System.currentTimeMillis();
                long removed = 0;
                long deltaBytes = 0;
                for (byte[] keyBytes : keys) {
                    KeyHandle handle = keyLifecycle.keyHandle(keyBytes);
                    if (handle == null) {
                        continue;
                    }
                    YierdisObject e = keyLifecycle.getStoredObject(handle);
                    if (e == null) {
                        continue;
                    }
                    if (keyLifecycle.removeIfExpired(handle, e, now)) {
                        continue;
                    }
                    keyLifecycle.removeExpire(handle);
                    if (keyLifecycle.remove(handle, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                        removed++;
                    }
                }
                if (removed > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed, deltaBytes);
            }
        });
    }

    @Override
    public ValueType typeOf(BytesView keyView) {
        internals.checkThread();
        YierdisObject e = keyLifecycle.getLiveObject(keyView);
        return e == null ? null : e.type;
    }

    @Override
    public boolean existsKey(BytesView keyView) {
        internals.checkThread();
        return keyLifecycle.getLiveObject(keyView) != null;
    }

    @Override
    public List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
        internals.checkThread();
        if (globPattern == null) {
            return Collections.emptyList();
        }
        int limit = maxMatches <= 0 ? 0 : maxMatches;
        if (limit == 0) {
            return Collections.emptyList();
        }

        long deadlineNanos = Long.MAX_VALUE;
        if (timeBudgetNanos > 0) {
            long nowNanos = System.nanoTime();
            try {
                deadlineNanos = Math.addExact(nowNanos, timeBudgetNanos);
            } catch (ArithmeticException e) {
                deadlineNanos = Long.MAX_VALUE;
            }
        }
        final long deadline = deadlineNanos;

        long nowMillis = System.currentTimeMillis();
        List<byte[]> out = new ArrayList<>();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        final boolean[] timedOut = new boolean[]{false};

        ScanCursorV2 cursor = ScanCursorV2.start();
        int guard = 0;
        while (true) {
            if (System.nanoTime() >= deadline) {
                timedOut[0] = true;
                break;
            }
            ScanCursorV2 next = keyLifecycle.scan(cursor, 1024, (k, e) -> {
                if (k == null || e == null) {
                    return true;
                }
                if (keyLifecycle.isKeyExpired(k, nowMillis)) {
                    expiredKeys.add(YierdisDb.toByteArray(k));
                    expiredValues.add(e);
                    return true;
                }
                if (YierdisGlobMatcher.matches(globPattern, k)) {
                    out.add(YierdisDb.toByteArray(k));
                    if (out.size() >= limit) {
                        return false;
                    }
                }
                if (System.nanoTime() >= deadline) {
                    timedOut[0] = true;
                    return false;
                }
                return true;
            });
            cursor = next;
            if (cursor.value() == 0) {
                break;
            }
            if (out.size() >= limit || timedOut[0]) {
                break;
            }
            if (++guard > 1_000_000) {
                throw new IllegalStateException("KEYS scan did not make progress");
            }
        }

        for (int i = 0; i < expiredKeys.size(); i++) {
            byte[] key = expiredKeys.get(i);
            keyLifecycle.removeExpire(key);
            if (keyLifecycle.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                internals.ledger().commit(null, -expiredValues.get(i).estimatedBytes);
            }
        }
        return out;
    }

    @Override
    public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
        internals.checkThread();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long now = System.currentTimeMillis();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        int maxSteps = Math.max(64, count * 10);
        final int[] remaining = new int[]{count};

        ScanCursorV2 next = keyLifecycle.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (k, e) -> {
            if (k == null || e == null) {
                return true;
            }
            if (keyLifecycle.isKeyExpired(k, now)) {
                expiredKeys.add(YierdisDb.toByteArray(k));
                expiredValues.add(e);
                return true;
            }
            if (globPattern == null || YierdisGlobMatcher.matches(globPattern, k)) {
                out.add(YierdisDb.toByteArray(k));
                remaining[0]--;
                if (remaining[0] <= 0) {
                    return false;
                }
            }
            return true;
        });

        for (int i = 0; i < expiredKeys.size(); i++) {
            byte[] key = expiredKeys.get(i);
            keyLifecycle.removeExpire(key);
            if (keyLifecycle.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                internals.ledger().commit(null, -expiredValues.get(i).estimatedBytes);
            }
        }
        return next;
    }
}
