package yier.bubu.redis.runtime.embedded;

import java.time.Duration;
import java.util.Objects;
import yier.bubu.redis.common.command.CommandRecordView;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeKind;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitPublisher;
import yier.bubu.redis.storage.api.DbCommitReservation;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;

/**
 * 单 producer、单 consumer 的固定容量提交流。
 *
 * <p>reservation 在 DB 可见性之前填充 slot；{@link #publish(DbCommitReservation)} 只转换已验证 token
 * 的状态，因此不能在该路径加入分配、回调或新的容量判断。</p>
 */
public final class CommitStream implements DbCommitPublisher, AutoCloseable {
    public static final int DEFAULT_MAX_EVENTS = 8_192;
    public static final long DEFAULT_MAX_RETAINED_BYTES = 64L * 1024L * 1024L;
    public static final long DEFAULT_DRAIN_TIMEOUT_MILLIS = 5_000L;

    private final Object lock = new Object();
    private final CommitStreamSlot[] slots;
    private final ImmutableCommandRecord[] cleanupRecords;
    private final YierdisChangeSink sink;
    private final long maxRetainedBytes;
    private final long drainTimeoutMillis;
    private final Thread worker;

    private CommitStreamState state = CommitStreamState.RUNNING;
    private Thread producerOwner;
    private int head;
    private int tail;
    private long reservedEvents;
    private long reservedBytes;
    private long rejectedWrites;
    private long lastAssignedSequence;
    private long lastAcknowledgedSequence;
    private CommitStreamReservation producerReservation;
    private boolean callbackActive;
    private boolean started;
    private boolean shutdownRequested;
    private boolean shutdownTimedOut;
    private CleanupOwner cleanupOwner = CleanupOwner.NONE;
    private String firstFailureType;
    private String firstFailureMessage;

    public CommitStream(YierdisChangeSink sink) {
        this(sink, DEFAULT_MAX_EVENTS, DEFAULT_MAX_RETAINED_BYTES, DEFAULT_DRAIN_TIMEOUT_MILLIS);
    }

    public CommitStream(
            YierdisChangeSink sink,
            int maxEvents,
            long maxRetainedBytes,
            long drainTimeoutMillis
    ) {
        this(sink, maxEvents, maxRetainedBytes, drainTimeoutMillis, 0L);
    }

    CommitStream(
            YierdisChangeSink sink,
            int maxEvents,
            long maxRetainedBytes,
            long drainTimeoutMillis,
            long initialLastAssignedSequence
    ) {
        this(sink, maxEvents, maxRetainedBytes, drainTimeoutMillis, initialLastAssignedSequence, true);
    }

    static CommitStream prepare(
            YierdisChangeSink sink,
            int maxEvents,
            long maxRetainedBytes,
            long drainTimeoutMillis
    ) {
        return new CommitStream(sink, maxEvents, maxRetainedBytes, drainTimeoutMillis, 0L, false);
    }

    private CommitStream(
            YierdisChangeSink sink,
            int maxEvents,
            long maxRetainedBytes,
            long drainTimeoutMillis,
            long initialLastAssignedSequence,
            boolean startImmediately
    ) {
        this.sink = Objects.requireNonNull(sink, "sink");
        if (sink == YierdisChangeSink.NOOP) {
            throw new IllegalArgumentException("NOOP sink must use DbCommitPublisher.NOOP");
        }
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be > 0");
        }
        if (maxRetainedBytes <= 0L) {
            throw new IllegalArgumentException("maxRetainedBytes must be > 0");
        }
        if (drainTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("drainTimeoutMillis must be > 0");
        }
        if (initialLastAssignedSequence < 0L) {
            throw new IllegalArgumentException("initialLastAssignedSequence must be non-negative");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.drainTimeoutMillis = drainTimeoutMillis;
        this.lastAssignedSequence = initialLastAssignedSequence;
        this.slots = new CommitStreamSlot[maxEvents];
        this.cleanupRecords = new ImmutableCommandRecord[maxEvents];
        for (int index = 0; index < maxEvents; index++) {
            slots[index] = new CommitStreamSlot();
        }
        this.worker = Thread.ofPlatform()
                .daemon(true)
                .name("yierdis-commit-stream")
                .unstarted(this::runWorker);
        if (startImmediately) {
            start();
        }
    }

    void start() {
        synchronized (lock) {
            if (started) {
                return;
            }
            if (state != CommitStreamState.RUNNING) {
                throw new IllegalStateException("commit stream cannot start after shutdown");
            }
            started = true;
            worker.start();
        }
    }

    @Override
    public DbCommitReservation reserve(
            int dbIndex,
            DbCommitKind kind,
            ImmutableCommandRecord record,
            long committedMemoryDelta,
            long commitAttemptTimestampMillis
    ) {
        if (dbIndex < 0) {
            throw new IllegalArgumentException("dbIndex must be non-negative");
        }
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(record, "record");
        long recordBytes = record.retainedMemoryBytes();
        if (recordBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
        }

        synchronized (lock) {
            if (!started
                    || state != CommitStreamState.RUNNING
                    || producerReservation != null
                    || reservedEvents >= slots.length
                    || recordBytes > maxRetainedBytes
                    || reservedBytes > maxRetainedBytes - recordBytes
                    || lastAssignedSequence == Long.MAX_VALUE) {
                rejectedWrites++;
                throw new DbCommitStreamUnavailableException();
            }
            requireProducerThread();

            CommitStreamSlot slot = slots[tail];
            if (slot.state != SlotState.FREE) {
                throw new IllegalStateException("commit stream ring tail is not free");
            }
            long generation = nextGeneration(slot.generation);
            CommitStreamReservation reservation = new CommitStreamReservation(this, tail, generation, recordBytes);
            ImmutableCommandRecord retained = record.retain();
            boolean retainedOwned = true;
            try {
                if (retained.retainedMemoryBytes() != recordBytes) {
                    throw new IllegalArgumentException("retained command record changed its memory estimate");
                }
                slot.generation = generation;
                slot.state = SlotState.RESERVED;
                slot.dbIndex = dbIndex;
                slot.kind = kind;
                slot.record = retained;
                slot.recordBytes = recordBytes;
                slot.committedMemoryDelta = committedMemoryDelta;
                slot.commitAttemptTimestampMillis = commitAttemptTimestampMillis;
                slot.candidateSequence = lastAssignedSequence + 1L;
                reservedEvents++;
                reservedBytes += recordBytes;
                producerReservation = reservation;
                retainedOwned = false;
                return reservation;
            } finally {
                if (retainedOwned) {
                    retained.close();
                }
            }
        }
    }

    @Override
    public long publish(DbCommitReservation reservation) {
        if (!(reservation instanceof CommitStreamReservation token) || token.stream != this) {
            throw new IllegalArgumentException("reservation does not belong to this commit stream");
        }
        synchronized (lock) {
            CommitStreamSlot slot = requireReservedSlot(token);
            slot.state = SlotState.QUEUED;
            lastAssignedSequence = slot.candidateSequence;
            producerReservation = null;
            token.consumed = true;
            tail = nextIndex(tail);
            lock.notifyAll();
            return lastAssignedSequence;
        }
    }

    @Override
    public void failAfterCommit(DbCommitReservation reservation) {
        if (!(reservation instanceof CommitStreamReservation token) || token.stream != this) {
            return;
        }
        synchronized (lock) {
            if (token.consumed || !tokenMatchesReservedSlot(token)) {
                return;
            }
            CommitStreamSlot slot = slots[token.slotIndex];
            slot.state = SlotState.HELD_FAILED;
            token.consumed = true;
            producerReservation = null;
            recordFailureLocked("DbPostCommitInvariantFailure", "publication did not complete after storage commit");
            state = CommitStreamState.FAILED;
            lock.notifyAll();
        }
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public boolean available() {
        synchronized (lock) {
            return started && state == CommitStreamState.RUNNING;
        }
    }

    public CommitStreamState state() {
        synchronized (lock) {
            return state;
        }
    }

    public CommitStreamStats stats() {
        synchronized (lock) {
            return new CommitStreamStats(
                    state,
                    reservedEvents,
                    reservedBytes,
                    rejectedWrites,
                    lastAssignedSequence,
                    lastAcknowledgedSequence,
                    firstFailureType,
                    firstFailureMessage,
                    callbackActive,
                    shutdownTimedOut
            );
        }
    }

    /**
     * Drains only acknowledged events. A reservation that has not reached {@link #publish(DbCommitReservation)}
     * is a command-owner lifecycle violation, not an event that can be discarded as part of normal draining.
     */
    public boolean shutdownGracefully(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }

        int cleanupCount;
        synchronized (lock) {
            cleanupCount = beginShutdownLocked();
        }
        closeDetachedRecords(cleanupCount);

        if (!started) {
            synchronized (lock) {
                return shutdownSucceededLocked();
            }
        }
        if (Thread.currentThread() == worker) {
            return false;
        }

        try {
            joinWorker(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            int interruptedCleanup;
            synchronized (lock) {
                interruptedCleanup = failShutdownLocked(
                        "InterruptedException",
                        "commit stream drain interrupted",
                        false
                );
            }
            worker.interrupt();
            closeDetachedRecords(interruptedCleanup);
            return false;
        }

        if (!worker.isAlive()) {
            int completedCleanup;
            boolean succeeded;
            synchronized (lock) {
                completedCleanup = completeStoppedShutdownLocked();
                succeeded = shutdownSucceededLocked();
            }
            closeDetachedRecords(completedCleanup);
            return succeeded;
        }

        int timeoutCleanup;
        synchronized (lock) {
            timeoutCleanup = failShutdownLocked(
                    "CommitStreamDrainTimeout",
                    "commit stream drain timed out",
                    true
            );
        }
        worker.interrupt();
        closeDetachedRecords(timeoutCleanup);
        return false;
    }

    /**
     * Compatibility entry point for callers that use the configured instance drain timeout.
     */
    public boolean shutdown() {
        return shutdownGracefully(Duration.ofMillis(drainTimeoutMillis));
    }

    @Override
    public void close() {
        shutdown();
    }

    private void runWorker() {
        for (;;) {
            CommitStreamSlot slot = null;
            YierdisChangeEvent event = null;
            int terminalCleanup = 0;
            boolean terminate = false;
            synchronized (lock) {
                for (;;) {
                    if (state == CommitStreamState.CLOSED) {
                        lock.notifyAll();
                        return;
                    }
                    if (state == CommitStreamState.FAILED) {
                        if (shutdownRequested) {
                            terminalCleanup = claimTerminalCleanupLocked(CleanupOwner.WORKER);
                        }
                        lock.notifyAll();
                        terminate = true;
                        break;
                    }
                    slot = slots[head];
                    if (slot.state == SlotState.QUEUED) {
                        slot.state = SlotState.IN_FLIGHT;
                        callbackActive = true;
                        try {
                            CallbackRecordView view = new CallbackRecordView(slot, slot.generation, Thread.currentThread());
                            event = YierdisChangeEvent.borrowed(
                                    slot.candidateSequence,
                                    slot.dbIndex,
                                    changeKind(slot.kind),
                                    slot.kind != DbCommitKind.USER,
                                    view,
                                    slot.committedMemoryDelta,
                                    slot.commitAttemptTimestampMillis,
                                    view
                            );
                        } catch (Throwable failure) {
                            callbackActive = false;
                            if (slot.state == SlotState.IN_FLIGHT) {
                                slot.state = SlotState.QUEUED;
                            }
                            recordFailureLocked(failure);
                            state = CommitStreamState.FAILED;
                            if (shutdownRequested) {
                                terminalCleanup = claimTerminalCleanupLocked(CleanupOwner.WORKER);
                            }
                            lock.notifyAll();
                            terminate = true;
                        }
                        break;
                    }
                    if (state == CommitStreamState.DRAINING && reservedEvents == 0L) {
                        state = CommitStreamState.CLOSED;
                        lock.notifyAll();
                        return;
                    }
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        recordFailureLocked("InterruptedException", "commit stream worker interrupted");
                        state = CommitStreamState.FAILED;
                        lock.notifyAll();
                        return;
                    }
                }
            }
            if (terminate) {
                closeDetachedRecords(terminalCleanup);
                return;
            }

            Throwable callbackFailure = null;
            try {
                sink.onChange(event);
            } catch (Throwable failure) {
                callbackFailure = failure;
            } finally {
                event.close();
            }

            ImmutableCommandRecord completedRecord = null;
            int callbackCleanup = 0;
            boolean callbackTerminated = false;
            synchronized (lock) {
                callbackActive = false;
                if (callbackFailure != null) {
                    if (slot.state == SlotState.IN_FLIGHT) {
                        slot.state = SlotState.QUEUED;
                    }
                    recordFailureLocked(callbackFailure);
                    state = CommitStreamState.FAILED;
                    if (shutdownRequested) {
                        callbackCleanup = claimTerminalCleanupLocked(CleanupOwner.WORKER);
                    }
                    lock.notifyAll();
                    callbackTerminated = true;
                } else if (state == CommitStreamState.FAILED) {
                    if (slot.state == SlotState.IN_FLIGHT) {
                        // The callback has returned and its borrowed view is closed, so this is no longer an
                        // active slot even though it must not be acknowledged after a failed drain.
                        slot.state = SlotState.QUEUED;
                    }
                    if (shutdownRequested) {
                        callbackCleanup = claimTerminalCleanupLocked(CleanupOwner.WORKER);
                    }
                    lock.notifyAll();
                    callbackTerminated = true;
                } else if (slot.state != SlotState.IN_FLIGHT) {
                    recordFailureLocked("CommitStreamSlotState", "in-flight slot changed during callback");
                    state = CommitStreamState.FAILED;
                    if (shutdownRequested) {
                        callbackCleanup = claimTerminalCleanupLocked(CleanupOwner.WORKER);
                    }
                    lock.notifyAll();
                    callbackTerminated = true;
                } else {
                    long acknowledgedSequence = slot.candidateSequence;
                    completedRecord = clearSlotLocked(slot);
                    head = nextIndex(head);
                    lastAcknowledgedSequence = Math.max(lastAcknowledgedSequence, acknowledgedSequence);
                    if (state == CommitStreamState.DRAINING && reservedEvents == 0L) {
                        state = CommitStreamState.CLOSED;
                        callbackTerminated = true;
                    }
                    lock.notifyAll();
                }
            }
            if (completedRecord != null) {
                completedRecord.close();
            }
            closeDetachedRecords(callbackCleanup);
            if (callbackTerminated) {
                return;
            }
        }
    }

    private void cancelReservation(CommitStreamReservation token) {
        ImmutableCommandRecord canceledRecord = null;
        synchronized (lock) {
            if (token.consumed || !tokenMatchesReservedSlot(token)) {
                token.consumed = true;
                return;
            }
            CommitStreamSlot slot = slots[token.slotIndex];
            canceledRecord = clearSlotLocked(slot);
            producerReservation = null;
            token.consumed = true;
            lock.notifyAll();
        }
        canceledRecord.close();
    }

    private CommitStreamSlot requireReservedSlot(CommitStreamReservation token) {
        if (token.consumed || !tokenMatchesReservedSlot(token)) {
            throw new IllegalStateException("commit reservation is no longer live");
        }
        return slots[token.slotIndex];
    }

    private boolean tokenMatchesReservedSlot(CommitStreamReservation token) {
        if (token.slotIndex < 0 || token.slotIndex >= slots.length) {
            return false;
        }
        CommitStreamSlot slot = slots[token.slotIndex];
        return slot.state == SlotState.RESERVED && slot.generation == token.generation;
    }

    private void requireProducerThread() {
        Thread current = Thread.currentThread();
        if (producerOwner == null) {
            producerOwner = current;
            return;
        }
        if (producerOwner != current) {
            throw new IllegalStateException("commit stream belongs to another producer thread");
        }
    }

    private int beginShutdownLocked() {
        if (state == CommitStreamState.CLOSED) {
            return 0;
        }
        shutdownRequested = true;
        if (state == CommitStreamState.RUNNING) {
            state = CommitStreamState.DRAINING;
        }
        if (state == CommitStreamState.DRAINING && hasReservedSlotLocked()) {
            recordFailureLocked(
                    "CommitStreamOutstandingReservation",
                    "commit stream shutdown found an unpublished reservation"
            );
            state = CommitStreamState.FAILED;
        }

        int cleanupCount = 0;
        if (!started && state == CommitStreamState.DRAINING && reservedEvents == 0L) {
            state = CommitStreamState.CLOSED;
        } else if (state == CommitStreamState.FAILED) {
            cleanupCount = claimTerminalCleanupLocked(CleanupOwner.SHUTDOWN);
        }
        lock.notifyAll();
        return cleanupCount;
    }

    private int completeStoppedShutdownLocked() {
        if (state == CommitStreamState.DRAINING && reservedEvents == 0L) {
            state = CommitStreamState.CLOSED;
        }
        if (state == CommitStreamState.CLOSED) {
            lock.notifyAll();
            return 0;
        }
        if (state != CommitStreamState.FAILED) {
            recordFailureLocked("CommitStreamWorkerStopped", "commit stream worker stopped before drain completed");
            state = CommitStreamState.FAILED;
        }
        int cleanupCount = claimTerminalCleanupLocked(CleanupOwner.SHUTDOWN);
        lock.notifyAll();
        return cleanupCount;
    }

    private int failShutdownLocked(String type, String message, boolean timedOut) {
        if (timedOut) {
            shutdownTimedOut = true;
        }
        if (state != CommitStreamState.CLOSED) {
            recordFailureLocked(type, message);
            state = CommitStreamState.FAILED;
        }
        int cleanupCount = claimTerminalCleanupLocked(CleanupOwner.SHUTDOWN);
        lock.notifyAll();
        return cleanupCount;
    }

    private boolean shutdownSucceededLocked() {
        return state == CommitStreamState.CLOSED
                && reservedEvents == 0L
                && reservedBytes == 0L
                && !callbackActive
                && cleanupOwner == CleanupOwner.NONE;
    }

    private int claimTerminalCleanupLocked(CleanupOwner owner) {
        if (cleanupOwner != CleanupOwner.NONE || callbackActive || hasInFlightSlotLocked()) {
            return 0;
        }
        cleanupOwner = owner;
        return detachSlotsForTerminalCleanupLocked();
    }

    private int detachSlotsForTerminalCleanupLocked() {
        int count = 0;
        for (CommitStreamSlot slot : slots) {
            if (slot.state == SlotState.FREE) {
                continue;
            }
            cleanupRecords[count++] = clearSlotLocked(slot);
        }
        head = 0;
        tail = 0;
        producerReservation = null;
        return count;
    }

    private void closeDetachedRecords(int count) {
        Throwable failure = null;
        for (int index = 0; index < count; index++) {
            ImmutableCommandRecord record = cleanupRecords[index];
            cleanupRecords[index] = null;
            if (record == null) {
                continue;
            }
            try {
                record.close();
            } catch (Throwable closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("commit stream record cleanup failed", failure);
        }
    }

    private boolean hasReservedSlotLocked() {
        for (CommitStreamSlot slot : slots) {
            if (slot.state == SlotState.RESERVED) {
                return true;
            }
        }
        return false;
    }

    private boolean hasInFlightSlotLocked() {
        for (CommitStreamSlot slot : slots) {
            if (slot.state == SlotState.IN_FLIGHT) {
                return true;
            }
        }
        return false;
    }

    private void joinWorker(Duration timeout) throws InterruptedException {
        if (!worker.isAlive() || timeout.isZero()) {
            return;
        }
        long millis;
        try {
            millis = Math.addExact(
                    Math.multiplyExact(timeout.getSeconds(), 1_000L),
                    timeout.getNano() / 1_000_000L
            );
        } catch (ArithmeticException ignored) {
            millis = Long.MAX_VALUE;
        }
        if (millis == Long.MAX_VALUE) {
            worker.join(Long.MAX_VALUE);
            return;
        }
        worker.join(millis, timeout.getNano() % 1_000_000);
    }

    private ImmutableCommandRecord clearSlotLocked(CommitStreamSlot slot) {
        ImmutableCommandRecord record = slot.record;
        if (record == null || slot.state == SlotState.FREE) {
            throw new IllegalStateException("attempted to clear an empty commit stream slot");
        }
        reservedEvents--;
        reservedBytes -= slot.recordBytes;
        slot.clear();
        return record;
    }

    private void recordFailureLocked(Throwable failure) {
        String type = failure.getClass().getSimpleName();
        String message = failure.getMessage();
        recordFailureLocked(type.isEmpty() ? failure.getClass().getName() : type, message);
    }

    private void recordFailureLocked(String type, String message) {
        if (firstFailureType != null) {
            return;
        }
        firstFailureType = type == null || type.isBlank() ? "CommitStreamFailure" : type;
        if (message == null) {
            firstFailureMessage = "";
            return;
        }
        firstFailureMessage = sanitizeFailureMessage(message);
    }

    private static String sanitizeFailureMessage(String message) {
        int length = Math.min(512, message.length());
        StringBuilder sanitized = new StringBuilder(length);
        for (int index = 0; index < length; index++) {
            char ch = message.charAt(index);
            sanitized.append(ch < ' ' || ch == 127 ? ' ' : ch);
        }
        return sanitized.toString();
    }

    private int nextIndex(int index) {
        return index + 1 == slots.length ? 0 : index + 1;
    }

    private static long nextGeneration(long current) {
        if (current == Long.MAX_VALUE) {
            return 1L;
        }
        return current + 1L;
    }

    private static YierdisChangeKind changeKind(DbCommitKind kind) {
        return switch (kind) {
            case USER -> YierdisChangeKind.USER_COMMAND;
            case EXPIRED -> YierdisChangeKind.EXPIRED;
            case EVICTED -> YierdisChangeKind.EVICTED;
        };
    }

    private enum CleanupOwner {
        NONE,
        WORKER,
        SHUTDOWN
    }

    private enum SlotState {
        FREE,
        RESERVED,
        QUEUED,
        IN_FLIGHT,
        HELD_FAILED
    }

    private static final class CommitStreamSlot {
        private SlotState state = SlotState.FREE;
        private long generation;
        private int dbIndex;
        private DbCommitKind kind;
        private ImmutableCommandRecord record;
        private long recordBytes;
        private long committedMemoryDelta;
        private long commitAttemptTimestampMillis;
        private long candidateSequence;

        private void clear() {
            state = SlotState.FREE;
            dbIndex = 0;
            kind = null;
            record = null;
            recordBytes = 0L;
            committedMemoryDelta = 0L;
            commitAttemptTimestampMillis = 0L;
            candidateSequence = 0L;
        }
    }

    private static final class CommitStreamReservation implements DbCommitReservation {
        private final CommitStream stream;
        private final int slotIndex;
        private final long generation;
        private final long reservedMemoryBytes;
        private boolean consumed;

        private CommitStreamReservation(CommitStream stream, int slotIndex, long generation, long reservedMemoryBytes) {
            this.stream = stream;
            this.slotIndex = slotIndex;
            this.generation = generation;
            this.reservedMemoryBytes = reservedMemoryBytes;
        }

        @Override
        public long reservedMemoryBytes() {
            return reservedMemoryBytes;
        }

        @Override
        public boolean noop() {
            return false;
        }

        @Override
        public void close() {
            stream.cancelReservation(this);
        }
    }

    private final class CallbackRecordView implements CommandRecordView, AutoCloseable {
        private final CommitStreamSlot slot;
        private final long generation;
        private final Thread owner;
        private boolean open = true;

        private CallbackRecordView(CommitStreamSlot slot, long generation, Thread owner) {
            this.slot = slot;
            this.generation = generation;
            this.owner = owner;
        }

        @Override
        public int argc() {
            return record().argc();
        }

        @Override
        public boolean isNull(int index) {
            return record().isNull(index);
        }

        @Override
        public int len(int index) {
            return record().len(index);
        }

        @Override
        public byte byteAt(int index, int offset) {
            return record().byteAt(index, offset);
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            record().copyToByteArray(index, dst, dstOff);
        }

        @Override
        public long retainedMemoryBytes() {
            return record().retainedMemoryBytes();
        }

        @Override
        public void close() {
            open = false;
        }

        private ImmutableCommandRecord record() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("commit callback view belongs to another thread");
            }
            synchronized (lock) {
                if (!open || !callbackActive || slot.generation != generation || slot.state != SlotState.IN_FLIGHT) {
                    throw new IllegalStateException("commit callback view is no longer active");
                }
                return slot.record;
            }
        }
    }
}
