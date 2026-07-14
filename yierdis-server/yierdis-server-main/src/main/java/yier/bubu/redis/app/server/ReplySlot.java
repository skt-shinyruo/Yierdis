package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.executor.ExecutionReply;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private final List<AutoCloseable> ownedResources = new ArrayList<>();

    private volatile boolean closeAfterReply;
    private BytesSink sink;
    private int inFlightChunks;
    private boolean leaseClosed;

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
        if (!enterProducing()) {
            chunk.release();
            releaseAllocation(allocatedBytes);
            throw new IllegalStateException("reply slot is not accepting chunks");
        }
        synchronized (contentLock) {
            if (isTerminal(state.get())) {
                chunk.release();
                releaseAllocation(allocatedBytes);
                throw new IllegalStateException("reply slot has already terminated");
            }
            pendingChunks.add(new ReplyChunk(chunk, allocatedBytes));
            replyEgressStats.chunkAdded();
        }
    }

    void addOwnedResource(AutoCloseable resource) {
        Objects.requireNonNull(resource, "resource");
        boolean closeNow;
        boolean added = false;
        synchronized (contentLock) {
            closeNow = cleanupOwner.get() != ReplyCleanupOwner.NONE;
            if (!closeNow) {
                ownedResources.add(resource);
                added = true;
            }
        }
        if (added) {
            replyEgressStats.sourceAdded();
        }
        if (closeNow) {
            closeQuietly(resource);
        }
    }

    void markWaitingForCapacity() {
        state.compareAndSet(ReplySlotState.REGISTERED, ReplySlotState.WAITING_CAPACITY);
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
                if (isTerminal(state.get())) {
                    throw new IllegalStateException("reply slot has already terminated");
                }
                sink = sinkFactory.create(this);
            }
            return sink;
        }
    }

    @Override
    public boolean awaitCapacity(Runnable wakeup) {
        Objects.requireNonNull(wakeup, "wakeup");
        CapacityWait waiting = capacityWait.get();
        if (waiting == null || isTerminal(state.get())) {
            return false;
        }
        return lease.awaitAdditionalCapacity(waiting.additionalBytes(), waiting.singleReplyLimitBytes(), wakeup);
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
        this.closeAfterReply = closeAfterReply;
        while (true) {
            ReplySlotState current = state.get();
            if (isTerminal(current) || current == ReplySlotState.WRITING || current == ReplySlotState.READY) {
                return;
            }
            if (state.compareAndSet(current, ReplySlotState.READY)) {
                sequencer.onSlotReady(this);
                return;
            }
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
        if (!state.compareAndSet(ReplySlotState.READY, ReplySlotState.WRITING)) {
            return List.of();
        }
        synchronized (contentLock) {
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
        boolean closeLease;
        synchronized (contentLock) {
            if (inFlightChunks <= 0) {
                throw new IllegalStateException("reply slot chunk completion underflow");
            }
            inFlightChunks--;
            closeLease = cleanupOwner.get() != ReplyCleanupOwner.NONE && inFlightChunks == 0 && !leaseClosed;
            if (closeLease) {
                leaseClosed = true;
            }
        }
        if (closeLease) {
            lease.close();
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
        List<AutoCloseable> resources;
        boolean closeLease;
        synchronized (contentLock) {
            chunks = List.copyOf(pendingChunks);
            pendingChunks.clear();
            resources = List.copyOf(ownedResources);
            ownedResources.clear();
            closeLease = inFlightChunks == 0 && !leaseClosed;
            if (closeLease) {
                leaseClosed = true;
            }
        }
        for (ReplyChunk chunk : chunks) {
            if (chunk.release()) {
                replyEgressStats.chunkReleased();
            }
            releaseAllocation(chunk.allocatedBytes());
        }
        for (AutoCloseable resource : resources) {
            closeQuietly(resource);
            replyEgressStats.sourceReleased();
        }
        state.set(terminalState);
        if (terminalState == ReplySlotState.CANCELLED) {
            replyEgressStats.cancelledSlot();
        } else if (terminalState == ReplySlotState.FAILED) {
            replyEgressStats.failedSlot();
        }
        if (closeLease) {
            lease.close();
        }
        return true;
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

    private static void closeQuietly(AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception ignored) {
            // 资源清理不能阻断同一槽位其余所有权的归还。
        }
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

    private record CapacityWait(long additionalBytes, long singleReplyLimitBytes) {
    }
}
