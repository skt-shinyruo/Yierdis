package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.v1.CustomCommand;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NettyCommandExecutorFairSchedulingTest {
    @Test
    public void fairSchedulingProcessesDifferentChannelsWithinSameTick() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                eventExecutor,
                new JsonLineReplyWriterFactory(),
                1024,
                0,
                256,
                128,
                0,
                0,
                2,
                1000,
                SchedulingPolicy.FAIR
        );
        executor.start();

        // Block the executor thread so we can observe the result of the first drain tick.
        CountDownLatch blocker1Started = new CountDownLatch(1);
        CountDownLatch unblock1 = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blocker1Started.countDown();
            unblock1.await();
            return null;
        });
        Assert.assertTrue(blocker1Started.await(1, TimeUnit.SECONDS));

        CountDownLatch blocker2Started = new CountDownLatch(1);
        CountDownLatch unblock2 = new CountDownLatch(1);

        EmbeddedChannel ch1 = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
        EmbeddedChannel ch2 = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
        try {
            // Enqueue commands while the executor is blocked.
            ch1.writeInbound(new CustomCommand("PING", null));
            ch1.writeInbound(new CustomCommand("PING", null));
            ch2.writeInbound(new CustomCommand("PING", null));

            // Queue a second blocker behind the first drain tick.
            eventExecutor.submit(() -> {
                blocker2Started.countDown();
                unblock2.await();
                return null;
            });

            // Allow the first drain tick to run, then block again.
            unblock1.countDown();
            Assert.assertTrue(blocker2Started.await(1, TimeUnit.SECONDS));

            // With maxDrainCommands=2 and fair scheduling, both channels should have a reply after the first tick.
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch1, 1000));
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch2, 1000));

            Assert.assertNull("second reply for ch1 must wait for next tick", readOutbound(ch1));
            Assert.assertNull("ch2 should have only one reply", readOutbound(ch2));

            // Allow the next drain tick to run and produce the second reply for ch1.
            unblock2.countDown();
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch1, 1000));
        } finally {
            unblock1.countDown();
            unblock2.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch1.finishAndReleaseAll();
            ch2.finishAndReleaseAll();
        }
    }

    private static byte[] awaitOutbound(EmbeddedChannel ch, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();

            byte[] out = readOutbound(ch);
            if (out != null) {
                return out;
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for outbound");
            }
            Thread.sleep(5);
        }
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        ByteBuf out = ch.readOutbound();
        if (out == null) {
            return null;
        }
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
