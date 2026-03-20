package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.v1.CustomCommand;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ClosingSkipSideEffectsIntegrationTest {
    @Test
    public void internalErrorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                eventExecutor,
                new JsonLineReplyWriterFactory(),
                16,
                0,
                256,
                128,
                0,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
        );
        executor.start();

        // Block the executor thread so we can enqueue commands before any drain tick runs.
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
        try {
            // Enqueue commands while executor is blocked (no replies yet).
            ch.writeInbound(new CustomCommand("PING", null));
            ch.writeInbound(new CustomCommand("PING", null));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ServerSessionState session = ServerSessionState.getOrCreate(ch);
            ServerRuntimeState rt = session.runtime();
            Assert.assertEquals(2L, rt.commandsEnqueuedCounter().get());

            // Trigger an internal error: handler should mark closing and close the channel after replying.
            ch.pipeline().fireExceptionCaught(new RuntimeException("boom"));
            Assert.assertArrayEquals(
                    ascii("{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":\"ERR internal error\"}}\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected runtime closing flag to be set", rt.isClosing());

            // Allow the executor to drain: already-queued commands must be skipped to avoid side effects.
            unblock.countDown();

            awaitEquals(rt.commandsSkippedClosingCounter(), 2L, 1000);
            Assert.assertEquals(0L, rt.commandsExecutedCounter().get());
            Assert.assertNull("no command reply should be produced after closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void internalErrorFromExecutorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                eventExecutor,
                new JsonLineReplyWriterFactory(),
                16,
                0,
                256,
                128,
                0,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
        );
        executor.start();

        // Block the executor thread so we can enqueue commands before any drain tick runs.
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
        try {
            // Enqueue commands while executor is blocked (no replies yet).
            ch.writeInbound(new ExplodingCommand());
            ch.writeInbound(new CustomCommand("PING", null));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ServerSessionState session = ServerSessionState.getOrCreate(ch);
            ServerRuntimeState rt = session.runtime();
            Assert.assertEquals(2L, rt.commandsEnqueuedCounter().get());

            // Allow the executor to drain: the first task triggers an internal error on the executor thread.
            // This should mark closing and close the connection, so already-queued commands are skipped.
            unblock.countDown();

            Assert.assertArrayEquals(
                    ascii("{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":\"ERR internal error\"}}\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected runtime closing flag to be set", rt.isClosing());

            awaitEquals(rt.commandsSkippedClosingCounter(), 1L, 1000);
            Assert.assertEquals(1L, rt.commandsExecutedCounter().get());
            Assert.assertNull("no command reply should be produced after internal error closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    private static final class ExplodingCommand implements Command {
        @Override
        public int argc() {
            throw new RuntimeException("boom");
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return 0;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return 0;
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            // no-op
        }

        @Override
        public byte[] toByteArray(int index) {
            return null;
        }

        @Override
        public int retainedBytes() {
            return 0;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static void awaitEquals(AtomicLong counter, long expected, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            if (counter.get() == expected) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for counter: expected=" + expected + ", actual=" + counter.get());
            }
            Thread.sleep(5);
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
