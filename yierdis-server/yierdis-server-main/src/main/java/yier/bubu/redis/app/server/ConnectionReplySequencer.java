package yier.bubu.redis.app.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.PromiseCombiner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * 连接事件循环拥有的回复顺序器。
 *
 * <p>生产者只能让槽位变为 READY；本类是该连接唯一可以调用 {@link Channel#write(Object)} 的出口。</p>
 */
final class ConnectionReplySequencer implements AutoCloseable {
    private final Object lock = new Object();
    private final Channel channel;
    private final OutboundConnectionMemory connectionMemory;
    private final Runnable disableInput;
    private final ReplySlot.ReplySinkFactory sinkFactory;
    private final ReplyEgressStats replyEgressStats;
    private final ArrayDeque<ReplySlot> slots = new ArrayDeque<>();
    private final Set<ReplySlot> inFlightSlots = new LinkedHashSet<>();
    private final Set<ReplySlot> liveSlots = new LinkedHashSet<>();

    private long nextSequence;
    private boolean acceptingRegistrations = true;
    private boolean draining;
    private boolean drainRequested;
    private boolean shutdownRequested;
    private boolean shutdownCloseRequested;
    private boolean channelClosed;
    private Throwable terminationFailure;
    private final CompletableFuture<Void> shutdownDrained = new CompletableFuture<>();

    ConnectionReplySequencer(Channel channel, OutboundConnectionMemory connectionMemory, Runnable disableInput) {
        this(channel, connectionMemory, disableInput, slot -> {
            throw new IllegalStateException("reply slot has no configured sink");
        });
    }

    ConnectionReplySequencer(
            Channel channel,
            OutboundConnectionMemory connectionMemory,
            Runnable disableInput,
            ReplySlot.ReplySinkFactory sinkFactory
    ) {
        this(channel, connectionMemory, disableInput, sinkFactory, ReplyEgressStats.noop());
    }

    ConnectionReplySequencer(
            Channel channel,
            OutboundConnectionMemory connectionMemory,
            Runnable disableInput,
            ReplySlot.ReplySinkFactory sinkFactory,
            ReplyEgressStats replyEgressStats
    ) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.connectionMemory = Objects.requireNonNull(connectionMemory, "connectionMemory");
        this.disableInput = disableInput == null ? () -> { } : disableInput;
        this.sinkFactory = Objects.requireNonNull(sinkFactory, "sinkFactory");
        this.replyEgressStats = Objects.requireNonNull(replyEgressStats, "replyEgressStats");
        channel.closeFuture().addListener(future -> {
            cancelAll(ReplyCleanupOwner.CONNECTION_CLOSE);
            markChannelClosed(future.isSuccess() ? null : future.cause());
        });
    }

    Optional<ReplySlot> register(OutboundMemoryLease lease) {
        Objects.requireNonNull(lease, "lease");
        ReplySlot slot;
        synchronized (lock) {
            if (!acceptingRegistrations || !channel.isOpen() || nextSequence == Long.MAX_VALUE) {
                lease.close();
                return Optional.empty();
            }
            slot = new ReplySlot(nextSequence++, lease, this, sinkFactory, replyEgressStats);
            slots.addLast(slot);
            liveSlots.add(slot);
        }
        slot.cleanupCompletion().whenComplete((ignored, failure) -> slotCleanupCompleted(slot, failure));
        return Optional.of(slot);
    }

    boolean acceptingRegistrations() {
        synchronized (lock) {
            return acceptingRegistrations;
        }
    }

    void onSlotReady(ReplySlot slot) {
        if (slot == null) {
            return;
        }
        executeOnEventLoop(() -> readyOnEventLoop(slot));
    }

    void cancelSlot(ReplySlot slot, ReplyCleanupOwner owner) {
        if (slot == null) {
            return;
        }
        if (slot.cancelNow(owner)) {
            executeOnEventLoop(this::drainOnEventLoop);
        }
    }

    @Override
    public void close() {
        cancelAll(ReplyCleanupOwner.SHUTDOWN);
    }

    CompletableFuture<Void> shutdownGracefully() {
        synchronized (lock) {
            acceptingRegistrations = false;
            shutdownRequested = true;
        }
        disableInput.run();
        if (!channel.isOpen() || channel.closeFuture().isDone()) {
            cancelAll(ReplyCleanupOwner.SHUTDOWN);
            markChannelClosed(null);
            return shutdownDrained;
        }
        executeOnEventLoop(this::beginShutdownOnEventLoop);
        return shutdownDrained;
    }

    CompletableFuture<Void> terminationFuture() {
        return shutdownDrained;
    }

    private void readyOnEventLoop(ReplySlot slot) {
        if (slot.closeAfterReply()) {
            synchronized (lock) {
                acceptingRegistrations = false;
            }
            disableInput.run();
            cancelSlotsAfter(slot);
        }
        drainOnEventLoop();
    }

    private void drainOnEventLoop() {
        if (draining) {
            // Netty 的 EmbeddedChannel 和某些 transport 可以同步完成最后一次 write；把该回调合并到当前 drain 结束后继续。
            drainRequested = true;
            return;
        }
        do {
            draining = true;
            drainRequested = false;
            try {
                boolean flushRequired = false;
                while (true) {
                    ReplySlot head = head();
                    if (head == null) {
                        break;
                    }
                    ReplySlotState state = head.state();
                    if (state == ReplySlotState.CANCELLED || state == ReplySlotState.FAILED || state == ReplySlotState.COMPLETED) {
                        removeHead(head);
                        continue;
                    }
                    if (state != ReplySlotState.READY) {
                        break;
                    }
                    moveHeadToInFlight(head);
                    try {
                        writeHead(head);
                        flushRequired = true;
                    } catch (RuntimeException | Error failure) {
                        replyEgressStats.writeFailure();
                        head.fail(ReplyCleanupOwner.SEQUENCER);
                        removeInFlight(head);
                        flushBeforeFailureClose();
                        cancelAll(ReplyCleanupOwner.SEQUENCER);
                        channel.close();
                        break;
                    }
                    if (head.closeAfterReply()) {
                        break;
                    }
                }
                if (flushRequired && channel.isOpen()) {
                    try {
                        channel.flush();
                    } catch (RuntimeException | Error failure) {
                        replyEgressStats.writeFailure();
                        cancelAll(ReplyCleanupOwner.SEQUENCER);
                        channel.close();
                    }
                }
            } finally {
                draining = false;
            }
        } while (drainRequested);
        completeShutdownIfDrained();
    }

    private void beginShutdownOnEventLoop() {
        List<ReplySlot> toCancel = new ArrayList<>();
        synchronized (lock) {
            for (ReplySlot slot : slots) {
                ReplySlotState state = slot.state();
                if (state != ReplySlotState.READY && state != ReplySlotState.WRITING) {
                    toCancel.add(slot);
                }
            }
        }
        for (ReplySlot slot : toCancel) {
            slot.cancelNow(ReplyCleanupOwner.SHUTDOWN);
        }
        drainOnEventLoop();
    }

    private void completeShutdownIfDrained() {
        boolean closeChannel = false;
        synchronized (lock) {
            if (shutdownRequested && !shutdownCloseRequested && slots.isEmpty() && inFlightSlots.isEmpty()) {
                shutdownCloseRequested = true;
                closeChannel = true;
            }
        }
        if (!closeChannel) {
            return;
        }
        if (channel.isOpen()) {
            channel.close();
        } else {
            markChannelClosed(null);
        }
    }

    private void writeHead(ReplySlot slot) {
        List<ReplySlot.ReplyChunk> chunks = slot.takeChunksForWrite();
        if (chunks == null) {
            removeInFlight(slot);
            return;
        }
        PromiseCombiner writes = new PromiseCombiner(channel.eventLoop());
        if (chunks.isEmpty()) {
            writes.add(channel.write(Unpooled.EMPTY_BUFFER));
        } else {
            int submitted = 0;
            try {
                for (ReplySlot.ReplyChunk chunk : chunks) {
                    ChannelFuture chunkWrite = channel.write(chunk.buffer());
                    submitted++;
                    chunkWrite.addListener(ignored -> slot.chunkWriteCompleted(chunk));
                    writes.add(chunkWrite);
                }
            } catch (RuntimeException | Error failure) {
                for (int index = submitted; index < chunks.size(); index++) {
                    slot.chunkWriteAborted(chunks.get(index));
                }
                throw failure;
            }
        }
        ChannelPromise aggregate = channel.newPromise();
        writes.finish(aggregate);
        aggregate.addListener(future -> {
            if (future.isSuccess()) {
                slot.finish(ReplyCleanupOwner.FINAL_WRITE_FUTURE);
                removeInFlight(slot);
                if (slot.closeAfterReply()) {
                    channel.close();
                    return;
                }
                drainOnEventLoop();
                return;
            }
            replyEgressStats.writeFailure();
            slot.fail(ReplyCleanupOwner.FINAL_WRITE_FUTURE);
            removeInFlight(slot);
            flushBeforeFailureClose();
            cancelAll(ReplyCleanupOwner.SEQUENCER);
            channel.close();
        });
    }

    private void cancelSlotsAfter(ReplySlot slot) {
        List<ReplySlot> later = new ArrayList<>();
        synchronized (lock) {
            boolean found = false;
            for (ReplySlot candidate : slots) {
                if (candidate == slot) {
                    found = true;
                } else if (found) {
                    later.add(candidate);
                }
            }
        }
        for (ReplySlot candidate : later) {
            candidate.cancelNow(ReplyCleanupOwner.SEQUENCER);
        }
    }

    private void cancelAll(ReplyCleanupOwner owner) {
        List<ReplySlot> pending;
        synchronized (lock) {
            acceptingRegistrations = false;
            Set<ReplySlot> allSlots = new LinkedHashSet<>(slots);
            allSlots.addAll(inFlightSlots);
            pending = new ArrayList<>(allSlots);
        }
        for (ReplySlot slot : pending) {
            slot.cancelNow(owner);
        }
        connectionMemory.close();
    }

    private ReplySlot head() {
        synchronized (lock) {
            return slots.peekFirst();
        }
    }

    private void removeHead(ReplySlot expected) {
        synchronized (lock) {
            if (slots.peekFirst() == expected) {
                slots.removeFirst();
            } else {
                slots.remove(expected);
            }
        }
    }

    private void moveHeadToInFlight(ReplySlot slot) {
        synchronized (lock) {
            if (slots.peekFirst() == slot) {
                slots.removeFirst();
            } else {
                slots.remove(slot);
            }
            inFlightSlots.add(slot);
        }
    }

    private void removeInFlight(ReplySlot slot) {
        synchronized (lock) {
            inFlightSlots.remove(slot);
        }
    }

    private void slotCleanupCompleted(ReplySlot slot, Throwable failure) {
        synchronized (lock) {
            liveSlots.remove(slot);
            terminationFailure = recordFailure(terminationFailure, unwrapCompletionFailure(failure));
        }
        completeTerminationIfPossible();
    }

    private void markChannelClosed(Throwable failure) {
        synchronized (lock) {
            channelClosed = true;
            terminationFailure = recordFailure(terminationFailure, failure);
        }
        completeTerminationIfPossible();
    }

    private void completeTerminationIfPossible() {
        Throwable failure;
        synchronized (lock) {
            if (!channelClosed || !liveSlots.isEmpty() || shutdownDrained.isDone()) {
                return;
            }
            failure = terminationFailure;
        }
        if (failure == null) {
            shutdownDrained.complete(null);
        } else {
            shutdownDrained.completeExceptionally(failure);
        }
    }

    private void flushBeforeFailureClose() {
        if (!channel.isOpen()) {
            return;
        }
        try {
            channel.flush();
        } catch (RuntimeException | Error ignored) {
            // 原始写失败仍是此连接的终止 egress 结果。
        }
    }

    private void executeOnEventLoop(Runnable action) {
        try {
            if (channel.eventLoop().inEventLoop()) {
                action.run();
                return;
            }
            channel.eventLoop().execute(action);
        } catch (RuntimeException | Error schedulingFailure) {
            failAfterEventLoopRejection(schedulingFailure);
        }
    }

    private void failAfterEventLoopRejection(Throwable failure) {
        replyEgressStats.writeFailure();
        synchronized (lock) {
            terminationFailure = recordFailure(terminationFailure, failure);
        }
        cancelAll(ReplyCleanupOwner.SEQUENCER);
        try {
            if (channel.isRegistered()) {
                channel.close();
            } else {
                channel.unsafe().closeForcibly();
            }
        } catch (Throwable closeFailure) {
            synchronized (lock) {
                terminationFailure = recordFailure(terminationFailure, closeFailure);
            }
            try {
                channel.unsafe().closeForcibly();
            } catch (Throwable forceCloseFailure) {
                synchronized (lock) {
                    terminationFailure = recordFailure(terminationFailure, forceCloseFailure);
                }
            }
        }
        if (!channel.isOpen()) {
            markChannelClosed(null);
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
}
