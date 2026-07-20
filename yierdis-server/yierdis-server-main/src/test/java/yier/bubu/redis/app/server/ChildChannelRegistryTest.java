package yier.bubu.redis.app.server;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;

public class ChildChannelRegistryTest {
    @Test
    public void transportCloseDoesNotDrainAnInitializedChildBeforeItsReplyLifecycleCompletes() {
        ChildChannelRegistry registry = new ChildChannelRegistry();
        EmbeddedChannel child = new EmbeddedChannel();
        CompletableFuture<Void> replyLifecycle = new CompletableFuture<>();
        try {
            Assert.assertEquals(
                    ChildChannelRegistry.AdmissionResult.ACCEPTED,
                    registry.admit(child)
            );
            registry.bindLifecycle(child, replyLifecycle);
            registry.beginShutdown();

            child.close();
            child.runPendingTasks();

            Assert.assertFalse(registry.drainedFuture().isDone());
            Assert.assertEquals(1, registry.activeChannelCount());

            replyLifecycle.complete(null);

            Assert.assertTrue(registry.drainedFuture().isDone());
            Assert.assertEquals(0, registry.activeChannelCount());
        } finally {
            replyLifecycle.complete(null);
            child.finishAndReleaseAll();
        }
    }

    @Test
    public void shutdownRejectsLateChildrenPausesRegisteredInputAndCompletesAfterEveryCloseFuture() {
        ChildChannelRegistry registry = new ChildChannelRegistry();
        EmbeddedChannel first = new EmbeddedChannel();
        EmbeddedChannel late = new EmbeddedChannel();
        try {
            Assert.assertTrue(registry.register(first));
            List<Channel> children = registry.beginShutdown();

            Assert.assertEquals(List.of(first), children);
            first.runPendingTasks();
            Assert.assertFalse(first.config().isAutoRead());
            Assert.assertFalse(registry.register(late));
            late.runPendingTasks();
            Assert.assertFalse(late.isOpen());
            Assert.assertFalse(registry.drainedFuture().isDone());

            first.close();
            first.runPendingTasks();
            Assert.assertTrue(registry.drainedFuture().isDone());
            Assert.assertEquals(0, registry.activeChannelCount());
        } finally {
            first.finishAndReleaseAll();
            late.finishAndReleaseAll();
        }
    }
}
