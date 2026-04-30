package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.TtlReadOps;
import yier.bubu.redis.ops.TtlWriteOps;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.Objects;

final class YierdisTtlOps implements TtlReadOps, TtlWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisTtlOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public boolean expire(BytesView keyView, long seconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return false;
        }
        if (seconds <= 0) {
            return deleteImmediately(handle, e);
        }

        long expireAtMillis = safeExpireAtMillis(nowMillis, seconds);
        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                keyLifecycle.setExpireAtMillis(handle, expireAtMillis);
                keyLifecycle.touch(e);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    @Override
    public boolean pexpire(BytesView keyView, long milliseconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return false;
        }
        keyLifecycle.touch(e);

        if (milliseconds <= 0) {
            return deleteImmediately(handle, e);
        }

        long expireAtMillis = safeAddMillis(nowMillis, milliseconds);
        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                keyLifecycle.setExpireAtMillis(handle, expireAtMillis);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    @Override
    public boolean expireAtSeconds(BytesView keyView, long unixSeconds) {
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyView, expireAtMillis);
    }

    @Override
    public boolean expireAtMillis(BytesView keyView, long unixMillis) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, now)) {
            return false;
        }
        keyLifecycle.touch(e);

        if (unixMillis <= now) {
            return deleteImmediately(handle, e);
        }

        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                keyLifecycle.setExpireAtMillis(handle, unixMillis);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    @Override
    public boolean persist(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return false;
        }

        keyLifecycle.touch(e);
        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return false;
        }
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                keyLifecycle.removeExpire(handle);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    @Override
    public long ttlSeconds(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return -2;
        }

        long now = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, now)) {
            return -2;
        }
        keyLifecycle.touch(e);

        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis / 1000L;
    }

    @Override
    public long ttlMillis(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return -2;
        }

        long now = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, now)) {
            return -2;
        }
        keyLifecycle.touch(e);

        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis;
    }

    private boolean deleteImmediately(KeyHandle handle, YierdisObject e) {
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                long deltaBytes = 0;
                keyLifecycle.removeExpire(handle);
                if (keyLifecycle.remove(handle, e)) {
                    e.releasePayloadIfAny();
                    deltaBytes -= e.estimatedBytes;
                }
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
            }
        });
    }

    private static long safeExpireAtMillis(long nowMillis, long seconds) {
        long deltaMillis;
        try {
            deltaMillis = Math.multiplyExact(seconds, 1000L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long safeAddMillis(long nowMillis, long deltaMillis) {
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long ttlEntryBytesEstimate() {
        return yier.bubu.redis.ops.DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }
}
