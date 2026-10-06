package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan.AdmissionMode;

import java.util.Objects;

final class YierdisDbKernel {
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbHealth health;

    YierdisDbKernel(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbHealth health
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.health = Objects.requireNonNull(health, "health");
    }

    <R> R execute(MutationPlan<R> plan) {
        return mutationExecutor.execute(Objects.requireNonNull(plan, "plan"));
    }

    void checkOwner() {
        threadChecker.run();
    }

    void bindToCurrentThread() {
        keyLifecycle.bindToCurrentThread();
    }

    void close() {
        keyLifecycle.close();
    }

    <T> PreparedEntryMutation<T> unchanged(T result) {
        return PreparedEntryMutation.unchanged(keyLifecycle, result);
    }

    <T> PreparedEntryMutation<T> insert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            StagedEntry stagedEntry,
            EntryRecord newRecord
    ) {
        return PreparedEntryMutation.insert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                stagedEntry,
                newRecord
        );
    }

    <T> PreparedEntryMutation<T> replace(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.replace(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                existingEntryHandle,
                oldRecord,
                newRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedEntryMutation<T> delete(
            T result,
            long actualDeltaBytes,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.delete(
                keyLifecycle,
                result,
                actualDeltaBytes,
                existingEntryHandle,
                oldRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedEntryMutation<T> upsert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            CurrentEntry current,
            StagedEntry staged,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.upsert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                current,
                staged,
                newRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedDbMutation<T> callback(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            Runnable commit,
            Runnable releaseSuperseded,
            Runnable abort,
            boolean trimNativePagesAfterCommit
    ) {
        return new PreparedCallbackMutation<>(
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                commit,
                releaseSuperseded,
                abort,
                trimNativePagesAfterCommit
        );
    }

    <T> PreparedDbMutation<T> batch(
            PreparedDbMutation<?>[] changes,
            int count,
            T result,
            long actualDeltaBytes
    ) {
        return new PreparedBatchMutation<>(changes, count, result, actualDeltaBytes);
    }

    EntryRecord liveEntryRecord(AllocatorKeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return record;
        }
        // requireWritable 先于 admissionMode，degraded 下 RECLAMATION 也会被拒。
        // 读路径因此不能调用 reclaimExpired，否则 GET 已过期 key 会收到 MISCONF。
        // 跳过回收、按不存在应答，物理记录保持不动，恢复后再由可写路径回收。
        if (health.degraded()) {
            return null;
        }
        reclaimExpired(keyHandle, record, nowMillis);
        return null;
    }

    boolean reclaimExpired(AllocatorKeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return false;
        }
        return reclaim(keyHandle, expectedRecord, nowMillis, true);
    }

    boolean evict(AllocatorKeyHandle keyHandle, EntryRecord expectedRecord) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return reclaim(keyHandle, expectedRecord, 0L, false);
    }

    void reclaimExpiredBeforeMutation(byte[] keyBytes, long nowMillis) {
        AllocatorKeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyHandle == null) {
            return;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record != null && keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            reclaimExpired(keyHandle, record, nowMillis);
        }
    }

    private boolean reclaim(
            AllocatorKeyHandle keyHandle,
            EntryRecord expectedRecord,
            long nowMillis,
            boolean requireExpired
    ) {
        return execute(new MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<Boolean> prepare() {
                EntryRecord current = keyLifecycle.entryRecord(keyHandle);
                if (current == null || (expectedRecord != null && !expectedRecord.equals(current))) {
                    return preparedNoDeletion();
                }
                if (requireExpired && !keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
                    return preparedNoDeletion();
                }
                byte[] keyBytes = keyLifecycle.copyKeyBytes(keyHandle);
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
                if (entryHandle == null) {
                    return preparedNoDeletion();
                }
                long removalBytes = keyLifecycle.estimatedBytesForRemoval(keyHandle, current);
                return YierdisDbKernel.this.delete(
                        Boolean.TRUE,
                        -removalBytes,
                        entryHandle,
                        current,
                        true
                );
            }

            private PreparedDbMutation<Boolean> preparedNoDeletion() {
                return unchanged(Boolean.FALSE);
            }
        });
    }

}
