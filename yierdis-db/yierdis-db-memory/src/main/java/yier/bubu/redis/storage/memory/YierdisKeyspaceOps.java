package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.api.KeyspaceReadOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class YierdisKeyspaceOps implements KeyspaceReadOps, KeyspaceWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisKeyspaceOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public WriteResult<Long> del(Collection<byte[]> keys) {
        internals.checkThread();
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<WriteResult<Long>> apply() {
                long now = System.currentTimeMillis();
                long removed = 0;
                long deltaBytes = 0;
                for (byte[] keyBytes : keys) {
                    KeyHandle handle = keyLifecycle.keyHandle(keyBytes);
                    if (handle == null) {
                        continue;
                    }
                    EntryRecord record = keyLifecycle.entryRecord(handle);
                    if (record == null) {
                        continue;
                    }
                    if (keyLifecycle.removeIfExpired(handle, record, now)) {
                        continue;
                    }
                    long removalBytes = keyLifecycle.estimatedBytesForRemoval(handle, record);
                    keyLifecycle.removeExpireIndexOnly(handle);
                    if (keyLifecycle.removeEntry(handle, record)) {
                        deltaBytes -= removalBytes;
                        removed++;
                    }
                }
                MutationOutcome outcome = removed > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                return YierdisDbMutationExecutor.MutationResult.of(
                        WriteResult.of(removed, outcome),
                        deltaBytes
                );
            }
        });
    }

    @Override
    public ValueType typeOf(BytesView keyView) {
        internals.checkThread();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        return record == null ? null : record.type();
    }

    @Override
    public boolean existsKey(BytesView keyView) {
        internals.checkThread();
        return keyLifecycle.liveEntryRecord(keyView) != null;
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

        try (NativeEpochScope ignored = keyLifecycle.nativeAllocator().beginEpoch(NativeEpochKind.SCAN)) {
            long nowMillis = System.currentTimeMillis();
            List<byte[]> out = new ArrayList<>();
            List<byte[]> expiredKeys = new ArrayList<>();
            DeadlineState deadlineState = new DeadlineState(deadline);

            ScanCursorV2 cursor = ScanCursorV2.start();
            int guard = 0;
            while (true) {
                if (deadlineState.reached()) {
                    deadlineState.markTimedOut();
                    break;
                }
                ScanCursorV2 next = keyLifecycle.scan(cursor, 1024, (k, record) -> {
                    if (k == null) {
                        return true;
                    }
                    if (record == null) {
                        return true;
                    }
                    if (keyLifecycle.isKeyExpired(k, nowMillis)) {
                        expiredKeys.add(YierdisDb.toByteArray(k));
                        return true;
                    }
                    if (YierdisGlobMatcher.matches(globPattern, k)) {
                        out.add(YierdisDb.toByteArray(k));
                        if (out.size() >= limit) {
                            return false;
                        }
                    }
                    if (deadlineState.reached()) {
                        deadlineState.markTimedOut();
                        return false;
                    }
                    return true;
                });
                cursor = next;
                if (cursor.value() == 0) {
                    break;
                }
                if (out.size() >= limit || deadlineState.timedOut()) {
                    break;
                }
                if (++guard > 1_000_000) {
                    throw new IllegalStateException("KEYS scan did not make progress");
                }
            }

            for (int i = 0; i < expiredKeys.size(); i++) {
                byte[] key = expiredKeys.get(i);
                KeyHandle handle = keyLifecycle.keyHandle(key);
                if (handle == null) {
                    continue;
                }
                EntryRecord record = keyLifecycle.entryRecord(handle);
                if (record != null) {
                    keyLifecycle.removeIfExpired(handle, record, nowMillis);
                }
            }
            return out;
        }
    }

    @Override
    public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
        internals.checkThread();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        try (NativeEpochScope ignored = keyLifecycle.nativeAllocator().beginEpoch(NativeEpochKind.SCAN)) {
            long now = System.currentTimeMillis();
            List<byte[]> expiredKeys = new ArrayList<>();
            int maxSteps = Math.max(64, count * 10);
            RemainingLimit remaining = new RemainingLimit(count);

            ScanCursorV2 next = keyLifecycle.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (k, record) -> {
                if (k == null) {
                    return true;
                }
                if (record == null) {
                    return true;
                }
                if (keyLifecycle.isKeyExpired(k, now)) {
                    expiredKeys.add(YierdisDb.toByteArray(k));
                    return true;
                }
                if (globPattern == null || YierdisGlobMatcher.matches(globPattern, k)) {
                    out.add(YierdisDb.toByteArray(k));
                    if (!remaining.consume()) {
                        return false;
                    }
                }
                return true;
            });

            for (int i = 0; i < expiredKeys.size(); i++) {
                byte[] key = expiredKeys.get(i);
                KeyHandle handle = keyLifecycle.keyHandle(key);
                if (handle == null) {
                    continue;
                }
                EntryRecord record = keyLifecycle.entryRecord(handle);
                if (record != null) {
                    keyLifecycle.removeIfExpired(handle, record, now);
                }
            }
            return next;
        }
    }

    private static final class DeadlineState {
        private final long deadlineNanos;
        private boolean timedOut;

        DeadlineState(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        boolean reached() {
            return System.nanoTime() >= deadlineNanos;
        }

        void markTimedOut() {
            timedOut = true;
        }

        boolean timedOut() {
            return timedOut;
        }
    }

    private static final class RemainingLimit {
        private int remaining;

        RemainingLimit(int remaining) {
            this.remaining = remaining;
        }

        boolean consume() {
            remaining--;
            return remaining > 0;
        }
    }
}
