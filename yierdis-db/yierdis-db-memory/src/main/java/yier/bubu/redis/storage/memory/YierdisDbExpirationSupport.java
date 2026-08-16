package yier.bubu.redis.storage.memory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.PostCommitMutationException;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.KeyScanResult;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

final class YierdisDbExpirationSupport {
    private static final long CLEANUP_SCAN_CHUNK_SLOTS = 32L;
    private static final long CLEANUP_MAX_INSPECTED_SLOTS = 320L;
    private static final int CLEANUP_MAX_CANDIDATES = 20;

    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final long expireCleanupTimeLimitNanos;
    private ScanCursorV2 cursor = ScanCursorV2.start();
    private long cursorTableGeneration = Long.MIN_VALUE;

    YierdisDbExpirationSupport(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            long expireCleanupTimeLimitNanos
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        if (expireCleanupTimeLimitNanos < 0L) {
            throw new IllegalArgumentException("expireCleanupTimeLimitNanos must be >= 0");
        }
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    void cleanupExpired() {
        cleanupExpired(0L);
    }

    void cleanupExpired(long nowMillis) {
        kernel.maintain(scope -> {
            cleanupExpired(scope, nowMillis);
            return null;
        });
    }

    private void cleanupExpired(MaintenanceScope scope, long nowMillis) {
        if (keyLifecycle.expireCount() == 0) {
            resetCursorState();
            return;
        }
        if (cursorTableGeneration != keyLifecycle.directoryState().tableGeneration()) {
            resetCursorState();
        }

        long startedNanos = System.nanoTime();
        long nowFixed = nowMillis <= 0L ? System.currentTimeMillis() : nowMillis;
        ScanCursorV2 batchStart = null;
        ScanCursorV2 next = cursor;
        long batchTableGeneration = Long.MIN_VALUE;
        long inspectedSlots = 0L;
        List<ExpirationCandidate> candidates = new ArrayList<>(CLEANUP_MAX_CANDIDATES);
        Set<NativeHandle> candidateIdentities = new HashSet<>(CLEANUP_MAX_CANDIDATES);

        do {
            long remainingSlots = CLEANUP_MAX_INSPECTED_SLOTS - inspectedSlots;
            long chunkSlots = Math.min(CLEANUP_SCAN_CHUNK_SLOTS, remainingSlots);
            KeyScanResult result = keyLifecycle.scanWithWork(next, chunkSlots, (key, record) -> {
                // scan callback 只收集候选；目录删除必须等遍历返回后执行。
                if (isExpired(record, nowFixed) && candidateIdentities.add(record.keyHandle())) {
                    candidates.add(new ExpirationCandidate(
                            key,
                            keyLifecycle.copyKeyBytes(key),
                            record
                    ));
                }
                return candidates.size() < CLEANUP_MAX_CANDIDATES
                        && !timeLimitReached(startedNanos);
            });
            if (batchStart == null) {
                batchStart = result.startCursor();
                batchTableGeneration = result.tableGeneration();
            } else if (batchTableGeneration != result.tableGeneration()) {
                throw new IllegalStateException("key-directory generation changed during expiration scan");
            }
            inspectedSlots += result.inspectedSlots();
            next = result.nextCursor();
            if (result.inspectedSlots() == 0L
                    || candidates.size() >= CLEANUP_MAX_CANDIDATES
                    || next.value() == 0L
                    || timeLimitReached(startedNanos)) {
                break;
            }
        } while (inspectedSlots < CLEANUP_MAX_INSPECTED_SLOTS);

        boolean retryBatch = false;
        try {
            for (ExpirationCandidate candidate : candidates) {
                if (!keyLifecycle.isCurrentExpiredCandidate(
                        candidate.keyBytes(),
                        candidate.keyHandle(),
                        candidate.record(),
                        nowFixed
                )) {
                    continue;
                }
                if (scope.reclaimExpired(candidate.keyHandle(), candidate.record(), nowFixed)) {
                    continue;
                }
                if (keyLifecycle.hasCurrentExpiredEntry(
                        candidate.keyBytes(),
                        candidate.keyHandle(),
                        nowFixed
                )) {
                    retryBatch = true;
                    break;
                }
            }
        } catch (PostCommitMutationException failure) {
            retainCursor(next, batchTableGeneration);
            throw failure;
        } catch (RuntimeException | Error failure) {
            retainCursor(batchStart, batchTableGeneration);
            throw failure;
        }

        // 候选处理失败时重试当前批次；全部删除或证实失效后才推进 cursor。
        retainCursor(retryBatch ? batchStart : next, batchTableGeneration);
        if (keyLifecycle.expireCount() == 0) {
            resetCursorState();
        }
    }

    void resetCursor() {
        kernel.maintain(ignored -> {
            resetCursorState();
            return null;
        });
    }

    private void resetCursorState() {
        retainCursor(ScanCursorV2.start(), keyLifecycle.directoryState().tableGeneration());
    }

    private void retainCursor(ScanCursorV2 nextCursor, long tableGeneration) {
        cursor = Objects.requireNonNull(nextCursor, "nextCursor");
        cursorTableGeneration = tableGeneration;
    }

    private boolean timeLimitReached(long startedNanos) {
        return System.nanoTime() - startedNanos >= expireCleanupTimeLimitNanos;
    }

    private static boolean isExpired(EntryRecord record, long nowMillis) {
        return record != null
                && record.expireAtMillis() >= 0L
                && record.expireAtMillis() <= nowMillis;
    }

    private record ExpirationCandidate(KeyHandle keyHandle, byte[] keyBytes, EntryRecord record) {
    }
}
