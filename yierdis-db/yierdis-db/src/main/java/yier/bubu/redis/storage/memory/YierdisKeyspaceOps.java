package yier.bubu.redis.storage.memory;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.function.IntConsumer;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan.AdmissionMode;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.DirectoryState;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.KeyScanResult;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.api.KeyspaceOps;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

import java.util.Collection;
import java.util.Objects;

final class YierdisKeyspaceOps implements KeyspaceOps {
    private static final long KEYS_SCAN_CHUNK_SLOTS = 1024L;
    private static final long SCAN_MIN_SLOT_BUDGET = 64L;
    private static final long SCAN_SLOT_MULTIPLIER = 10L;

    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;

    YierdisKeyspaceOps(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
    }

    @Override
    public WriteResult<Long> del(Collection<byte[]> keys) {
        kernel.checkOwner();
        Objects.requireNonNull(keys, "keys");
        long nowMillis = System.currentTimeMillis();
        reclaimExpiredBeforeDeletion(keys, nowMillis);
        return kernel.execute(new MutationPlan<WriteResult<Long>>() {
            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare() {
                PreparedDeletion[] deletions = new PreparedDeletion[keys.size()];
                int deletionCount = 0;
                long deltaBytes = 0L;
                for (byte[] keyBytes : keys) {
                    Objects.requireNonNull(keyBytes, "key");
                    if (containsDeletionForKey(deletions, deletionCount, keyBytes)) {
                        continue;
                    }
                    AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyBytes);
                    if (handle == null) {
                        continue;
                    }
                    EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
                    EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                    if (current == null) {
                        continue;
                    }
                    if (keyLifecycle.isKeyExpired(handle, nowMillis)) {
                        throw new IllegalStateException("expired key was not reclaimed before DEL preparation");
                    }
                    try {
                        long removalBytes = keyLifecycle.estimatedBytesForRemoval(handle, current);
                        PreparedDbMutation<Void> mutation = kernel.delete(
                                null,
                                -removalBytes,
                                entryHandle,
                                current,
                                true
                        );
                        deletions[deletionCount++] = new PreparedDeletion(keyBytes, mutation);
                        deltaBytes = Math.addExact(deltaBytes, -removalBytes);
                    } catch (RuntimeException | Error failure) {
                        abortPreparedDeletions(deletions, deletionCount, failure);
                        throw failure;
                    }
                }
                MutationOutcome outcome = deletionCount > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                PreparedDbMutation<?>[] changes = new PreparedDbMutation<?>[deletionCount];
                for (int index = 0; index < deletionCount; index++) {
                    changes[index] = deletions[index].mutation;
                }
                return kernel.batch(
                        changes,
                        changes.length,
                        WriteResult.of((long) deletionCount, outcome),
                        deltaBytes
                );
            }
        });
    }

    private void reclaimExpiredBeforeDeletion(Collection<byte[]> keys, long nowMillis) {
        for (byte[] keyBytes : keys) {
            Objects.requireNonNull(keyBytes, "key");
            kernel.reclaimExpiredBeforeMutation(keyBytes, nowMillis);
        }
    }

    private static boolean containsDeletionForKey(PreparedDeletion[] deletions, int deletionCount, byte[] keyBytes) {
        for (int index = 0; index < deletionCount; index++) {
            if (java.util.Arrays.equals(deletions[index].keyBytes, keyBytes)) {
                return true;
            }
        }
        return false;
    }

    private static void abortPreparedDeletions(PreparedDeletion[] deletions, int deletionCount, Throwable failure) {
        for (int index = 0; index < deletionCount; index++) {
            try {
                deletions[index].mutation.abort();
            } catch (RuntimeException | Error abortFailure) {
                failure.addSuppressed(abortFailure);
            }
        }
    }

    @Override
    public ValueType typeOf(BytesView keyView) {
        kernel.checkOwner();
        EntryRecord record = kernel.liveEntryRecord(keyLifecycle.keyHandle(keyView));
        return record == null ? null : record.type();
    }

    @Override
    public boolean existsKey(BytesView keyView) {
        kernel.checkOwner();
        return kernel.liveEntryRecord(keyLifecycle.keyHandle(keyView)) != null;
    }

    @Override
    public KeyScanWindow keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
        kernel.checkOwner();
        return keysWithinRead(globPattern, maxMatches, timeBudgetNanos);
    }

    private KeyScanWindow keysWithinRead(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
        int limit = maxMatches <= 0 ? 0 : maxMatches;
        long nowMillis = System.currentTimeMillis();
        if (globPattern == null || limit == 0) {
            return emptyWindow(ScanCursorV2.start(), globPattern, nowMillis);
        }

        long deadlineNanos = deadlineNanos(timeBudgetNanos);
        NativeEpochScope epoch = memoryContext.beginScanEpoch();
        boolean transferred = false;
        try {
            ScanCursorV2 next = ScanCursorV2.start();
            ScanCursorV2 start = next;
            long inspected = 0L;
            long generation = keyLifecycle.directoryState().tableGeneration();
            KeyDiscovery discovery = new KeyDiscovery();
            boolean firstStep = true;

            while (firstStep || (next.value() != 0L && !deadlineReached(deadlineNanos) && discovery.count < limit)) {
                firstStep = false;
                if (deadlineReached(deadlineNanos)) {
                    KeyScanResult initial = keyLifecycle.scanWithWork(next, 0L, (key, record) -> true);
                    start = initial.startCursor();
                    next = initial.nextCursor();
                    generation = initial.tableGeneration();
                    break;
                }
                KeyScanResult step = keyLifecycle.scanWithWork(next, KEYS_SCAN_CHUNK_SLOTS, (key, record) -> {
                    if (matchesForWindow(globPattern, key, record, nowMillis)) {
                        discovery.record(key.length());
                        if (discovery.count >= limit) {
                            return false;
                        }
                    }
                    return !deadlineReached(deadlineNanos);
                });
                if (inspected == 0L) {
                    start = step.startCursor();
                }
                inspected = addSaturating(inspected, step.inspectedSlots());
                next = step.nextCursor();
                generation = step.tableGeneration();
                if (step.inspectedSlots() == 0L) {
                    break;
                }
            }

            DirectoryState directoryState = keyLifecycle.directoryState();
            KeyScanWindow window = new KeyWindow(
                    epoch,
                    start,
                    next,
                    globPattern,
                    discovery.count,
                    inspected,
                    generation,
                    directoryState.activeCapacity(),
                    directoryState.oldCapacity(),
                    nowMillis
            );
            // window 持有 SCAN epoch，直到响应同步重放完成或被丢弃，避免 key handle 在此期间被回收。
            transferred = true;
            return window;
        } finally {
            if (!transferred) {
                epoch.close();
            }
        }
    }

    @Override
    public KeyScanWindow scan(ScanCursorV2 cursor, byte[] globPattern, int count) {
        kernel.checkOwner();
        return scanWithinRead(cursor, globPattern, count);
    }

    private KeyScanWindow scanWithinRead(ScanCursorV2 cursor, byte[] globPattern, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long nowMillis = System.currentTimeMillis();
        NativeEpochScope epoch = memoryContext.beginScanEpoch();
        boolean transferred = false;
        try {
            KeyDiscovery discovery = new KeyDiscovery();
            KeyScanResult result = keyLifecycle.scanWithWork(
                    cursor == null ? ScanCursorV2.start() : cursor,
                    scanSlotBudget(count),
                    (key, record) -> {
                        if (!matchesForWindow(globPattern, key, record, nowMillis)) {
                            return true;
                        }
                        discovery.record(key.length());
                        return discovery.count < count;
                    }
            );
            DirectoryState directoryState = keyLifecycle.directoryState();
            KeyScanWindow window = new KeyWindow(
                    epoch,
                    result.startCursor(),
                    result.nextCursor(),
                    globPattern,
                    discovery.count,
                    result.inspectedSlots(),
                    result.tableGeneration(),
                    directoryState.activeCapacity(),
                    directoryState.oldCapacity(),
                    nowMillis
            );
            // 与 KEYS 相同，epoch 的所有权随 window 转移给命令层的 try-with-resources。
            transferred = true;
            return window;
        } finally {
            if (!transferred) {
                epoch.close();
            }
        }
    }

    private KeyScanWindow emptyWindow(ScanCursorV2 cursor, byte[] globPattern, long nowMillis) {
        KeyScanResult result = keyLifecycle.scanWithWork(cursor, 0L, (key, record) -> true);
        DirectoryState directoryState = keyLifecycle.directoryState();
        return new KeyWindow(
                null,
                result.startCursor(),
                result.nextCursor(),
                globPattern,
                0,
                0L,
                result.tableGeneration(),
                directoryState.activeCapacity(),
                directoryState.oldCapacity(),
                nowMillis
        );
    }

    private boolean matchesForWindow(byte[] globPattern, AllocatorKeyHandle key, EntryRecord record, long expiryEvaluationMillis) {
        return key != null
                && record != null
                && !keyLifecycle.isKeyExpiredForScan(key, expiryEvaluationMillis)
                && (globPattern == null || YierdisGlobMatcher.matches(globPattern, key));
    }

    private static long deadlineNanos(long timeBudgetNanos) {
        if (timeBudgetNanos <= 0L) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(System.nanoTime(), timeBudgetNanos);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static boolean deadlineReached(long deadlineNanos) {
        return deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos;
    }

    private static long scanSlotBudget(int count) {
        return Math.max(SCAN_MIN_SLOT_BUDGET, (long) count * SCAN_SLOT_MULTIPLIER);
    }

    private static final class PreparedDeletion {
        private final byte[] keyBytes;
        private final PreparedDbMutation<Void> mutation;

        private PreparedDeletion(byte[] keyBytes, PreparedDbMutation<Void> mutation) {
            this.keyBytes = keyBytes;
            this.mutation = mutation;
        }
    }

    private static final class KeyDiscovery {
        private int count;

        void record(int payloadLength) {
            if (count == Integer.MAX_VALUE) {
                throw new IllegalStateException("key scan count exceeds Integer.MAX_VALUE");
            }
            count++;
        }
    }

    private final class KeyWindow implements KeyScanWindow {
        private final NativeEpochScope epoch;
        private final ScanCursorV2 startCursor;
        private final ScanCursorV2 nextCursor;
        private final byte[] globPattern;
        private final int count;
        private final long inspectedSlots;
        private final long tableGeneration;
        private final int activeTableCapacity;
        private final int oldTableCapacity;
        private final long expiryEvaluationMillis;
        private boolean closed;

        private KeyWindow(
                NativeEpochScope epoch,
                ScanCursorV2 startCursor,
                ScanCursorV2 nextCursor,
                byte[] globPattern,
                int count,
                long inspectedSlots,
                long tableGeneration,
                int activeTableCapacity,
                int oldTableCapacity,
                long expiryEvaluationMillis
        ) {
            this.epoch = epoch;
            this.startCursor = Objects.requireNonNull(startCursor, "startCursor");
            this.nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
            this.globPattern = globPattern;
            this.count = count;
            this.inspectedSlots = inspectedSlots;
            this.tableGeneration = tableGeneration;
            this.activeTableCapacity = activeTableCapacity;
            this.oldTableCapacity = oldTableCapacity;
            this.expiryEvaluationMillis = expiryEvaluationMillis;
        }

        @Override
        public ScanCursorV2 nextCursor() {
            return nextCursor;
        }

        @Override
        public long inspectedSlots() {
            return inspectedSlots;
        }

        @Override
        public long tableGeneration() {
            return tableGeneration;
        }

        @Override
        public long expiryEvaluationMillis() {
            return expiryEvaluationMillis;
        }

        @Override
        public boolean current() {
            return !closed && keyLifecycle.directoryStateIsCurrent(
                    tableGeneration,
                    activeTableCapacity,
                    oldTableCapacity
            );
        }

        @Override
        public int elementCount() {
            return count;
        }

        @Override
        public long retainedMemoryBytes() {
            return 0L;
        }

        @Override
        public void visitElementLengths(IntConsumer out) {
            Objects.requireNonNull(out, "out");
            replay(key -> out.accept(key.length()));
        }

        @Override
        public void emitTo(ByteValueSink out) {
            Objects.requireNonNull(out, "out");
            replay(key -> out.value(memoryContext.keyBytesSlice(key)));
        }

        private void replay(KeyEmitter emitter) {
            ensureOpen();
            if (count == 0 || !current()) {
                return;
            }
            ReplayDiscovery replay = new ReplayDiscovery();
            KeyScanResult result = keyLifecycle.scanWithWork(startCursor, inspectedSlots, (key, record) -> {
                if (!matchesForWindow(globPattern, key, record, expiryEvaluationMillis)) {
                    return true;
                }
                if (replay.count < count) {
                    emitter.accept(key);
                    replay.count++;
                } else {
                    replay.extraMatch = true;
                }
                // 即使已经输出 count 个元素，也要走完 discovery 的物理 slot 范围，才能验证同一结束游标。
                return true;
            });
            if (replay.count != count || replay.extraMatch || result.nextCursor().value() != nextCursor.value()) {
                throw new IllegalStateException(
                        "key scan window changed before replay: expected count=" + count
                                + ", actual count=" + replay.count
                                + ", extra match=" + replay.extraMatch
                                + ", expected next=" + nextCursor.value()
                                + ", actual next=" + result.nextCursor().value()
                                + ", start=" + startCursor.value()
                                + ", inspected=" + inspectedSlots
                                + ", generation=" + keyLifecycle.directoryState().tableGeneration()
                                + ", old capacity=" + keyLifecycle.directoryState().oldCapacity()
                );
            }
        }

        @FunctionalInterface
        private interface KeyEmitter {
            void accept(AllocatorKeyHandle key);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (epoch != null) {
                epoch.close();
            }
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("key scan window is closed");
            }
        }
    }

    private static final class ReplayDiscovery {
        private int count;
        private boolean extraMatch;
    }
}
