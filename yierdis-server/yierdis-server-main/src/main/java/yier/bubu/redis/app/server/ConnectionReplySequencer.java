package yier.bubu.redis.app.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

    private long nextSequence;
    private boolean acceptingRegistrations = true;
    private boolean draining;
    private boolean drainRequested;
    private boolean shutdownRequested;
    private boolean shutdownCloseRequested;
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
        channel.closeFuture().addListener(ignored -> {
            cancelAll(ReplyCleanupOwner.CONNECTION_CLOSE);
            shutdownDrained.complete(null);
        });
    }

    Optional<ReplySlot> register(OutboundMemoryLease lease) {
        Objects.requireNonNull(lease, "lease");
        synchronized (lock) {
            if (!acceptingRegistrations || !channel.isOpen() || nextSequence == Long.MAX_VALUE) {
                lease.close();
                return Optional.empty();
            }
            ReplySlot slot = new ReplySlot(nextSequence++, lease, this, sinkFactory, replyEgressStats);
            slots.addLast(slot);
            return Optional.of(slot);
        }
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
        executeOnEventLoop(this::beginShutdownOnEventLoop);
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
                    writeHead(head);
                    break;
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
            if (shutdownRequested && !shutdownCloseRequested && slots.isEmpty()) {
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
            shutdownDrained.complete(null);
        }
    }

    private void writeHead(ReplySlot slot) {
        List<ReplySlot.ReplyChunk> chunks = slot.takeChunksForWrite();
        ChannelFuture lastWrite = null;
        if (chunks.isEmpty()) {
            lastWrite = channel.writeAndFlush(Unpooled.EMPTY_BUFFER);
        } else {
            for (int index = 0; index < chunks.size(); index++) {
                ReplySlot.ReplyChunk chunk = chunks.get(index);
                ChannelFuture chunkWrite;
                if (index + 1 == chunks.size()) {
                    chunkWrite = channel.writeAndFlush(chunk.buffer());
                } else {
                    chunkWrite = channel.write(chunk.buffer());
                }
                lastWrite = chunkWrite;
                chunkWrite.addListener(ignored -> slot.chunkWriteCompleted(chunk));
            }
        }
        ChannelFuture finalWrite = lastWrite;
        finalWrite.addListener(future -> {
            if (future.isSuccess()) {
                slot.finish(ReplyCleanupOwner.FINAL_WRITE_FUTURE);
                removeHead(slot);
                if (slot.closeAfterReply()) {
                    channel.close();
                    return;
                }
                drainOnEventLoop();
                return;
            }
            replyEgressStats.writeFailure();
            slot.fail(ReplyCleanupOwner.FINAL_WRITE_FUTURE);
            removeHead(slot);
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
            pending = new ArrayList<>(slots);
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

    private void executeOnEventLoop(Runnable action) {
        if (channel.eventLoop().inEventLoop()) {
            action.run();
            return;
        }
        channel.eventLoop().execute(action);
    }
}
