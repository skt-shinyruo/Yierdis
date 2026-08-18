package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Tracks accepted child channels until their close futures complete. */
final class ChildChannelRegistry {
    enum AdmissionResult {
        ACCEPTED,
        REJECTED_CLOSING,
        REJECTED_MAX_CLIENTS
    }

    record StatsSnapshot(
            int activeConnections,
            long acceptedConnections,
            long rejectedClosingConnections,
            long rejectedMaxClientsConnections
    ) {
        long rejectedConnections() {
            return saturatedAdd(rejectedClosingConnections, rejectedMaxClientsConnections);
        }

        private static long saturatedAdd(long left, long right) {
            return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
        }
    }

    private final Object lock = new Object();
    private final Map<Channel, ChannelLifecycle> channels = new IdentityHashMap<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final int maxClients;
    private long acceptedConnections;
    private long rejectedClosingConnections;
    private long rejectedMaxClientsConnections;
    private boolean closing;

    ChildChannelRegistry() {
        this(Integer.MAX_VALUE);
    }

    ChildChannelRegistry(int maxClients) {
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be > 0");
        }
        this.maxClients = maxClients;
    }

    boolean register(Channel channel) {
        AdmissionResult result = admit(channel);
        if (result == AdmissionResult.ACCEPTED) {
            bindLifecycle(channel, transportCloseFuture(channel));
        }
        return result == AdmissionResult.ACCEPTED;
    }

    AdmissionResult admit(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        AdmissionResult result;
        boolean added = false;
        synchronized (lock) {
            if (closing) {
                rejectedClosingConnections = saturatedIncrement(rejectedClosingConnections);
                result = AdmissionResult.REJECTED_CLOSING;
            } else if (channels.containsKey(channel)) {
                result = AdmissionResult.ACCEPTED;
            } else if (channels.size() >= maxClients) {
                rejectedMaxClientsConnections = saturatedIncrement(rejectedMaxClientsConnections);
                result = AdmissionResult.REJECTED_MAX_CLIENTS;
            } else {
                channels.put(channel, new ChannelLifecycle());
                acceptedConnections = saturatedIncrement(acceptedConnections);
                added = true;
                result = AdmissionResult.ACCEPTED;
            }
        }
        if (result != AdmissionResult.ACCEPTED) {
            pauseInput(channel);
            closeChannel(channel);
            return result;
        }
        if (!added) {
            return result;
        }
        channel.closeFuture().addListener(ignored -> markTransportClosed(channel));
        if (!channel.isOpen()) {
            markTransportClosed(channel);
        }
        return result;
    }

    void bindLifecycle(Channel channel, CompletableFuture<Void> lifecycle) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(lifecycle, "lifecycle");
        synchronized (lock) {
            ChannelLifecycle tracked = channels.get(channel);
            if (tracked == null) {
                return;
            }
            if (tracked.lifecycleBound) {
                return;
            }
            tracked.lifecycleBound = true;
        }
        lifecycle.whenComplete((ignored, failure) -> markLifecycleComplete(channel));
    }

    void initializationFailed(Channel channel) {
        if (channel == null) {
            return;
        }
        synchronized (lock) {
            ChannelLifecycle tracked = channels.get(channel);
            if (tracked != null && !tracked.lifecycleBound) {
                tracked.lifecycleBound = true;
                tracked.lifecycleComplete = true;
                removeIfComplete(channel, tracked);
            }
        }
        closeChannel(channel);
    }

    List<Channel> beginShutdown() {
        List<Channel> snapshot;
        synchronized (lock) {
            closing = true;
            snapshot = List.copyOf(channels.keySet());
            completeIfDrained();
        }
        for (Channel channel : snapshot) {
            pauseInput(channel);
        }
        return snapshot;
    }

    void forceClose() {
        for (Channel channel : beginShutdown()) {
            closeChannel(channel);
        }
    }

    CompletableFuture<Void> drainedFuture() {
        return drained;
    }

    int activeChannelCount() {
        synchronized (lock) {
            return channels.size();
        }
    }

    StatsSnapshot statsSnapshot() {
        synchronized (lock) {
            return new StatsSnapshot(
                    channels.size(),
                    acceptedConnections,
                    rejectedClosingConnections,
                    rejectedMaxClientsConnections
            );
        }
    }

    boolean closing() {
        synchronized (lock) {
            return closing;
        }
    }

    List<Channel> channelsForTests() {
        synchronized (lock) {
            return List.copyOf(channels.keySet());
        }
    }

    private void markTransportClosed(Channel channel) {
        synchronized (lock) {
            ChannelLifecycle tracked = channels.get(channel);
            if (tracked == null) {
                return;
            }
            tracked.transportClosed = true;
            removeIfComplete(channel, tracked);
        }
    }

    private void markLifecycleComplete(Channel channel) {
        synchronized (lock) {
            ChannelLifecycle tracked = channels.get(channel);
            if (tracked == null) {
                return;
            }
            tracked.lifecycleComplete = true;
            removeIfComplete(channel, tracked);
        }
    }

    private void removeIfComplete(Channel channel, ChannelLifecycle tracked) {
        if (tracked.transportClosed && tracked.lifecycleBound && tracked.lifecycleComplete) {
            channels.remove(channel);
            completeIfDrained();
        }
    }

    private void completeIfDrained() {
        if (closing && channels.isEmpty()) {
            drained.complete(null);
        }
    }

    private static void pauseInput(Channel channel) {
        if (channel == null) {
            return;
        }
        Runnable pause = () -> {
            try {
                channel.config().setAutoRead(false);
            } catch (Throwable ignored) {
                // Closing may race with event-loop teardown.
            }
        };
        try {
            if (channel.eventLoop().inEventLoop()) {
                pause.run();
            } else {
                channel.eventLoop().execute(pause);
            }
        } catch (Throwable ignored) {
            pause.run();
        }
    }

    private static void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            if (!channel.isRegistered()) {
                channel.unsafe().closeForcibly();
                return;
            }
            channel.close();
        } catch (Throwable ignored) {
            try {
                channel.unsafe().closeForcibly();
            } catch (Throwable ignoredAgain) {
                // The channel is already tearing down.
            }
        }
    }

    private static long saturatedIncrement(long value) {
        return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1L;
    }

    private static CompletableFuture<Void> transportCloseFuture(Channel channel) {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        channel.closeFuture().addListener(future -> {
            if (future.isSuccess()) {
                closed.complete(null);
            } else {
                closed.completeExceptionally(future.cause());
            }
        });
        return closed;
    }

    private static final class ChannelLifecycle {
        private boolean lifecycleBound;
        private boolean lifecycleComplete;
        private boolean transportClosed;
    }
}
