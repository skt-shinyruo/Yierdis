package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Tracks accepted child channels until their close futures complete. */
final class ChildChannelRegistry {
    private final Object lock = new Object();
    private final Set<Channel> channels = Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private boolean closing;

    boolean register(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        boolean accepted;
        synchronized (lock) {
            accepted = !closing;
            if (accepted) {
                channels.add(channel);
            }
        }
        if (!accepted) {
            pauseInput(channel);
            closeChannel(channel);
            return false;
        }
        channel.closeFuture().addListener(ignored -> remove(channel));
        if (!channel.isOpen()) {
            remove(channel);
        }
        return true;
    }

    List<Channel> beginShutdown() {
        List<Channel> snapshot;
        synchronized (lock) {
            closing = true;
            snapshot = List.copyOf(channels);
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

    boolean closing() {
        synchronized (lock) {
            return closing;
        }
    }

    List<Channel> channelsForTests() {
        synchronized (lock) {
            return List.copyOf(channels);
        }
    }

    private void remove(Channel channel) {
        synchronized (lock) {
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
}
