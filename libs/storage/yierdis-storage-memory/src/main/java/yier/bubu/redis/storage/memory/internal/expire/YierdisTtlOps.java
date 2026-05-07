package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.TtlReadOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.WriteResult;

import java.util.Objects;

public final class YierdisTtlOps implements TtlReadOps, TtlWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    public YierdisTtlOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public WriteResult<Boolean> expire(BytesView keyView, long seconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        if (seconds <= 0) {
            return deleteImmediately(handle, e);
        }

        long expireAtMillis = safeExpireAtMillis(nowMillis, seconds);
        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Boolean>> apply() {
                keyLifecycle.setExpireAtMillis(handle, expireAtMillis);
                keyLifecycle.touch(e);
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED),
                        0
                );
            }
        });
    }

    @Override
    public WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        keyLifecycle.touch(e);

        if (milliseconds <= 0) {
            return deleteImmediately(handle, e);
        }

        long expireAtMillis = safeAddMillis(nowMillis, milliseconds);
        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Boolean>> apply() {
                keyLifecycle.setExpireAtMillis(handle, expireAtMillis);
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED),
                        0
                );
            }
        });
    }

    @Override
    public WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds) {
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyView, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        long now = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, now)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        keyLifecycle.touch(e);

        if (unixMillis <= now) {
            return deleteImmediately(handle, e);
        }

        long upperBound = keyLifecycle.expireAtMillis(handle) == null ? ttlEntryBytesEstimate() : 0;
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Boolean>> apply() {
                keyLifecycle.setExpireAtMillis(handle, unixMillis);
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED),
                        0
                );
            }
        });
    }

    @Override
    public WriteResult<Boolean> persist(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        YierdisObject e = keyLifecycle.getStoredObject(handle);
        if (e == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.removeIfExpired(handle, e, nowMillis)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        keyLifecycle.touch(e);
        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Boolean>> apply() {
                keyLifecycle.removeExpire(handle);
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED),
                        0
                );
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

    private WriteResult<Boolean> deleteImmediately(KeyHandle handle, YierdisObject e) {
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Boolean>> apply() {
                long deltaBytes = 0;
                keyLifecycle.removeExpire(handle);
                if (keyLifecycle.remove(handle, e)) {
                    e.releasePayloadIfAny();
                    deltaBytes -= e.estimatedBytes;
                }
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.VALUE_CHANGED),
                        deltaBytes
                );
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
        return yier.bubu.redis.storage.api.DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }
}
