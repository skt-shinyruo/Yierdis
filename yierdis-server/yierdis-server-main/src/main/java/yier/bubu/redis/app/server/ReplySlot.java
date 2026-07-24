package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplyTooLargeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 一个已按接收顺序注册的回复槽位。
 *
 * <p>槽位创建前已经取得控制额度；终止路径只能有一个 cleanup owner 归还该额度和其持有资源。</p>
 */
final class ReplySlot implements ExecutionReply {
    private final long sequence;
    private final OutboundMemoryLease lease;
    private final ConnectionReplySequencer sequencer;
    private final ReplySinkFactory sinkFactory;
    private final ReplyEgressStats replyEgressStats;
    private final AtomicReference<ReplySlotState> state = new AtomicReference<>(ReplySlotState.REGISTERED);
    private final AtomicReference<ReplyCleanupOwner> cleanupOwner = new AtomicReference<>(ReplyCleanupOwner.NONE);
    private final AtomicReference<CapacityWait> capacityWait = new AtomicReference<>();
    private final Object contentLock = new Object();
    private final List<ReplyChunk> pendingChunks = new ArrayList<>();
    private final List<OwnedResource> ownedResources = new ArrayList<>();
    private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();

    private volatile boolean closeAfterReply;
    private BytesSink sink;
    private int inFlightChunks;
    private int pendingResourceCloses;
    private boolean leaseClosed;
    private Throwable cleanupFailure;

    ReplySlot(
            long sequence,
            OutboundMemoryLease lease,
            ConnectionReplySequencer sequencer,
            ReplySinkFactory sinkFactory,
            ReplyEgressStats replyEgressStats
    ) {
        this.sequence = sequence;
        this.lease = Objects.requireNonNull(lease, "lease");
        this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
        this.sinkFactory = Objects.requireNonNull(sinkFactory, "sinkFactory");
        this.replyEgressStats = Objects.requireNonNull(replyEgressStats, "replyEgressStats");
    }

    long sequence() {
        return sequence;
    }

    ReplySlotState state() {
        return state.get();
    }

    ReplyCleanupOwner cleanupOwner() {
        return cleanupOwner.get();
    }

    OutboundMemoryLease lease() {
        return lease;
    }

    boolean closeAfterReply() {
        return closeAfterReply;
    }

    void addChunk(ByteBuf chunk) {
        addChunk(chunk, 0L);
    }

    void addChunk(ByteBuf chunk, long allocatedBytes) {
        Objects.requireNonNull(chunk, "chunk");
        if (allocatedBytes < 0L) {
            chunk.release();
            throw new IllegalArgumentException("allocatedBytes must be non-negative");
        }
        synchronized (contentLock) {
            if (cleanupOwner.get() != ReplyCleanupOwner.NONE || !enterProducing()) {
                chunk.release();
                releaseAllocation(allocatedBytes);
                throw new IllegalStateException("reply slot is not accepting chunks");
            }
            pendingChunks.add(new ReplyChunk(chunk, allocatedBytes));
            replyEgressStats.chunkAdded();
        }
    }

    void addOwnedResource(AutoCloseable resource) {
        addOwnedResource(resource, ReplySlot::closeSynchronously);
    }

    void addOwnedResource(
            AutoCloseable resource,
            Function<AutoCloseable, CompletableFuture<Void>> closer
    ) {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(closer, "closer");
        OwnedResource owned = new OwnedResource(resource, closer);
        boolean closeNow;
        boolean added = false;
        synchronized (contentLock) {
            closeNow = cleanupOwner.get() != ReplyCleanupOwner.NONE;
            if (!closeNow) {
                ownedResources.add(owned);
                added = true;
            }
        }
        if (added) {
            replyEgressStats.sourceAdded();
        }
        if (closeNow) {
            closeWithoutAccounting(owned);
        }
    }

    CompletableFuture<Void> cleanupCompletion() {
        return cleanupCompletion;
    }

    void markWaitingForCapacity() {
        synchronized (contentLock) {
            if (cleanupOwner.get() == ReplyCleanupOwner.NONE) {
                state.compareAndSet(ReplySlotState.REGISTERED, ReplySlotState.WAITING_CAPACITY);
            }
        }
    }

    void waitForAdditionalCapacity(long additionalBytes, long singleReplyLimitBytes) {
        if (additionalBytes <= 0L || singleReplyLimitBytes <= 0L) {
            throw new IllegalArgumentException("reply capacity wait must be positive");
        }
        CapacityWait requested = new CapacityWait(additionalBytes, singleReplyLimitBytes);
        CapacityWait previous = capacityWait.getAndSet(requested);
        if (previous != null && !previous.equals(requested)) {
            lease.cancelAdditionalCapacityWaiter();
        }
    }

    void clearCapacityWaitAfterReservation() {
        capacityWait.set(null);
    }

    void cancelCapacityWait() {
        if (capacityWait.getAndSet(null) != null) {
            lease.cancelAdditionalCapacityWaiter();
        }
    }

    @Override
    public BytesSink sink() {
        synchronized (contentLock) {
            if (sink == null) {
                if (cleanupOwner.get() != ReplyCleanupOwner.NONE || isTerminal(state.get())) {
                    throw new IllegalStateException("reply slot has already terminated");
                }
                sink = sinkFactory.create(this);
            }
            return sink;
        }
    }

    @Override
    public ReplyReservationResult tryReserve(ReplyPlan plan) {
        try {
            BytesSink current = sink();
            if (!(current instanceof ReplyReservationSink reservationSink)) {
                throw new IllegalStateException("reply sink does not support reservation");
            }
            reservationSink.require(Objects.requireNonNull(plan, "plan"));
            return ReplyReservationResult.RESERVED;
        } catch (ReplyCapacityUnavailableException unavailable) {
            return ReplyReservationResult.WAITING;
        } catch (ReplyTooLargeException tooLarge) {
            return ReplyReservationResult.TOO_LARGE;
        } catch (IllegalStateException closedSlot) {
            if (cleanupOwner.get() == ReplyCleanupOwner.NONE) {
                throw closedSlot;
            }
            return ReplyReservationResult.CLOSED;
        }
    }

    @Override
    public CapacityRegistration onCapacityAvailable(Runnable wakeup) {
        Objects.requireNonNull(wakeup, "wakeup");
        CapacityWait waiting = capacityWait.get();
        if (waiting == null || cleanupOwner.get() != ReplyCleanupOwner.NONE || isTerminal(state.get())) {
            return CapacityRegistration.NONE;
        }
        AtomicBoolean active = new AtomicBoolean(true);
        boolean retained = lease.awaitAdditionalCapacity(
                waiting.additionalBytes(),
                waiting.singleReplyLimitBytes(),
                () -> {
                    if (active.compareAndSet(true, false)) {
                        wakeup.run();
                    }
                }
        );
        if (!retained) {
            active.set(false);
            return CapacityRegistration.NONE;
        }
        return () -> {
            if (active.compareAndSet(true, false)) {
                cancelCapacityWait();
            }
        };
    }

    @Override
    public boolean hasWrittenBytes() {
        BytesSink currentSink;
        synchronized (contentLock) {
            currentSink = sink;
        }
        return currentSink instanceof ReplyReservationSink reservationSink && reservationSink.writtenBytes() > 0L;
    }

    @Override
    public void markResultUnknown() {
        replyEgressStats.resultUnknownClose();
    }

    void recordOversizedReply() {
        replyEgressStats.oversizedReply();
    }

    @Override
    public void markReady(boolean closeAfterReply) {
        boolean ready = false;
        synchronized (contentLock) {
            if (cleanupOwner.get() != ReplyCleanupOwner.NONE) {
                return;
            }
            this.closeAfterReply = closeAfterReply;
            while (true) {
                ReplySlotState current = state.get();
                if (isTerminal(current) || current == ReplySlotState.WRITING || current == ReplySlotState.READY) {
                    return;
                }
                if (state.compareAndSet(current, ReplySlotState.READY)) {
                    ready = true;
                    break;
                }
            }
        }
        if (ready) {
            sequencer.onSlotReady(this);
        }
    }

    @Override
    public void cancel() {
        cancel(ReplyCleanupOwner.SEQUENCER);
    }

    void cancel(ReplyCleanupOwner owner) {
        sequencer.cancelSlot(this, owner);
    }

    @Override
    public void close() {
        cancel();
    }

    List<ReplyChunk> takeChunksForWrite() {
        synchronized (contentLock) {
            if (cleanupOwner.get() != ReplyCleanupOwner.NONE
                    || !state.compareAndSet(ReplySlotState.READY, ReplySlotState.WRITING)) {
                return null;
            }
            List<ReplyChunk> chunks = List.copyOf(pendingChunks);
            pendingChunks.clear();
            inFlightChunks += chunks.size();
            return chunks;
        }
    }

    void chunkWriteCompleted(ReplyChunk chunk) {
        if (chunk == null || !chunk.complete()) {
            return;
        }
        replyEgressStats.chunkReleased();
        releaseAllocation(chunk.allocatedBytes());
        completeInFlightChunk();
    }

    void chunkWriteAborted(ReplyChunk chunk) {
        if (chunk == null || !chunk.release()) {
            return;
        }
        replyEgressStats.chunkReleased();
        releaseAllocation(chunk.allocatedBytes());
        completeInFlightChunk();
    }

    private void completeInFlightChunk() {
        boolean closeLease;
        synchronized (contentLock) {
            if (inFlightChunks <= 0) {
                throw new IllegalStateException("reply slot chunk completion underflow");
            }
            inFlightChunks--;
            closeLease = cleanupOwner.get() != ReplyCleanupOwner.NONE
                    && inFlightChunks == 0
                    && pendingResourceCloses == 0
                    && !leaseClosed;
            if (closeLease) {
                leaseClosed = true;
            }
        }
        if (closeLease) {
            closeLeaseAndCompleteCleanup();
        }
    }

    boolean finish(ReplyCleanupOwner owner) {
        return cleanup(owner, ReplySlotState.COMPLETED);
    }

    boolean fail(ReplyCleanupOwner owner) {
        return cleanup(owner, ReplySlotState.FAILED);
    }

    boolean cancelNow(ReplyCleanupOwner owner) {
        return cleanup(owner, ReplySlotState.CANCELLED);
    }

    void runProducerAction(Runnable action) {
        Objects.requireNonNull(action, "action");
        synchronized (contentLock) {
            ReplySlotState current = state.get();
            if (cleanupOwner.get() != ReplyCleanupOwner.NONE
                    || current == ReplySlotState.READY
                    || current == ReplySlotState.WRITING
                    || isTerminal(current)) {
                throw new IllegalStateException("reply slot is not accepting producer writes");
            }
            action.run();
        }
    }

    private boolean enterProducing() {
        while (true) {
            ReplySlotState current = state.get();
            if (current == ReplySlotState.PRODUCING) {
                return true;
            }
            if (current != ReplySlotState.REGISTERED && current != ReplySlotState.WAITING_CAPACITY) {
                return false;
            }
            if (state.compareAndSet(current, ReplySlotState.PRODUCING)) {
                return true;
            }
        }
    }

    private boolean cleanup(ReplyCleanupOwner owner, ReplySlotState terminalState) {
        Objects.requireNonNull(owner, "owner");
        if (owner == ReplyCleanupOwner.NONE || !cleanupOwner.compareAndSet(ReplyCleanupOwner.NONE, owner)) {
            return false;
        }

        cancelCapacityWait();

        List<ReplyChunk> chunks;
        List<OwnedResource> resources;
        boolean closeLease;
        synchronized (contentLock) {
            chunks = List.copyOf(pendingChunks);
            pendingChunks.clear();
            resources = List.copyOf(ownedResources);
            ownedResources.clear();
            pendingResourceCloses += resources.size();
            closeLease = inFlightChunks == 0 && pendingResourceCloses == 0 && !leaseClosed;
            if (closeLease) {
                leaseClosed = true;
            }
            // 先发布 terminal state，再让其他线程离开 contentLock，避免 cleanup 缝隙中重新进入 producer。
            state.set(terminalState);
        }
        for (ReplyChunk chunk : chunks) {
            if (chunk.release()) {
                replyEgressStats.chunkReleased();
            }
            releaseAllocation(chunk.allocatedBytes());
        }
        for (OwnedResource resource : resources) {
            closeResource(resource).whenComplete((ignored, failure) -> resourceCloseCompleted(failure));
        }
        if (terminalState == ReplySlotState.CANCELLED) {
            replyEgressStats.cancelledSlot();
        } else if (terminalState == ReplySlotState.FAILED) {
            replyEgressStats.failedSlot();
        }
        if (closeLease) {
            closeLeaseAndCompleteCleanup();
        }
        return true;
    }

    private void resourceCloseCompleted(Throwable failure) {
        boolean closeLease;
        synchronized (contentLock) {
            if (pendingResourceCloses <= 0) {
                throw new IllegalStateException("reply slot source completion underflow");
            }
            pendingResourceCloses--;
            cleanupFailure = recordFailure(cleanupFailure, unwrapCompletionFailure(failure));
            closeLease = cleanupOwner.get() != ReplyCleanupOwner.NONE
                    && inFlightChunks == 0
                    && pendingResourceCloses == 0
                    && !leaseClosed;
            if (closeLease) {
                leaseClosed = true;
            }
        }
        replyEgressStats.sourceReleased();
        if (closeLease) {
            closeLeaseAndCompleteCleanup();
        }
    }

    private void closeLeaseAndCompleteCleanup() {
        Throwable failure;
        try {
            lease.close();
        } catch (Throwable closeFailure) {
            synchronized (contentLock) {
                cleanupFailure = recordFailure(cleanupFailure, closeFailure);
            }
        }
        synchronized (contentLock) {
            failure = cleanupFailure;
        }
        if (failure == null) {
            cleanupCompletion.complete(null);
        } else {
            cleanupCompletion.completeExceptionally(failure);
        }
    }

    private void releaseAllocation(long bytes) {
        if (bytes > 0L) {
            lease.releaseAllocated(bytes);
        }
    }

    private static boolean isTerminal(ReplySlotState candidate) {
        return candidate == ReplySlotState.COMPLETED
                || candidate == ReplySlotState.CANCELLED
                || candidate == ReplySlotState.FAILED;
    }

    private static CompletableFuture<Void> closeResource(OwnedResource owned) {
        try {
            CompletableFuture<Void> completion = owned.closer().apply(owned.resource());
            return completion == null
                    ? CompletableFuture.failedFuture(new IllegalStateException("reply source closer returned null"))
                    : completion;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void closeWithoutAccounting(OwnedResource owned) {
        closeResource(owned).exceptionally(ignored -> null);
    }

    private static CompletableFuture<Void> closeSynchronously(AutoCloseable resource) {
        try {
            resource.close();
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        if (failure instanceof java.util.concurrent.CompletionException completion && completion.getCause() != null) {
            return completion.getCause();
        }
        return failure;
    }

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        if (current != next) {
            current.addSuppressed(next);
        }
        return current;
    }

    static final class ReplyChunk {
        private final ByteBuf buffer;
        private final long allocatedBytes;
        private final AtomicBoolean completed = new AtomicBoolean();

        private ReplyChunk(ByteBuf buffer, long allocatedBytes) {
            this.buffer = buffer;
            this.allocatedBytes = allocatedBytes;
        }

        ByteBuf buffer() {
            return buffer;
        }

        long allocatedBytes() {
            return allocatedBytes;
        }

        boolean complete() {
            return completed.compareAndSet(false, true);
        }

        boolean release() {
            if (completed.compareAndSet(false, true)) {
                buffer.release();
                return true;
            }
            return false;
        }
    }

    @FunctionalInterface
    interface ReplySinkFactory {
        BytesSink create(ReplySlot slot);
    }

    private record OwnedResource(
            AutoCloseable resource,
            Function<AutoCloseable, CompletableFuture<Void>> closer
    ) {
    }

    private record CapacityWait(long additionalBytes, long singleReplyLimitBytes) {
    }
}
