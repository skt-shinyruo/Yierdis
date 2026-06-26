package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.ExecutionConnectionContext;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespCommandRequest;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.RespCommandAdapter;
import yier.bubu.redis.protocol.resp.netty.RespProtocolError;
import yier.bubu.redis.protocol.resp.netty.RespProtocolErrorReplyHandler;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClosingSkipSideEffectsIntegrationTest {
    @Test
    public void protocolErrorReplyHandlerMarksClosingAndClosesAfterReply() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                eventExecutor,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        EmbeddedChannel ch = new EmbeddedChannel(
                protocolErrorHandler(replyWriterFactory),
                new RespCommandAdapter(),
                new YierdisFastCommandHandler(executor, replyWriterFactory)
        );
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 16, 1024);
            ch.writeInbound(request("PING"));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());

            Assert.assertFalse(ch.writeInbound(new RespProtocolError("ERR Protocol error: invalid inline command", true)));
            Assert.assertArrayEquals(
                    ascii("-ERR Protocol error: invalid inline command\r\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected protocol error handler to mark connection closing", context.statsSnapshot().closing());
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();
            Assert.assertFalse("protocol error handler should close after replying", ch.isOpen());

            unblock.countDown();

            awaitCounter(context, c -> c.statsSnapshot().commandsSkippedClosing(), 1L, 1000);
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after protocol close begins", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void commandHandlerTreatsDecoderWrappedProtocolFailuresAsInternalErrors() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                eventExecutor,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor, replyWriterFactory));
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 16, 1024);

            ch.pipeline().fireExceptionCaught(new DecoderException(
                    new IllegalArgumentException("Protocol error: invalid inline command")
            ));

            Assert.assertArrayEquals(ascii("-ERR internal error\r\n"), awaitOutbound(ch, 1000));
            Assert.assertTrue("expected command handler fallback to mark connection closing", connection.context().statsSnapshot().closing());
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();
            Assert.assertFalse("internal error fallback should close after replying", ch.isOpen());
        } finally {
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void internalErrorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                eventExecutor,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandAdapter(), new YierdisFastCommandHandler(executor, replyWriterFactory));
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 16, 1024);
            // Enqueue commands while executor is blocked (no replies yet).
            ch.writeInbound(request("PING"));
            ch.writeInbound(request("PING"));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(2L, context.statsSnapshot().commandsEnqueued());

            // Trigger an internal error: handler should mark closing and close the channel after replying.
            ch.pipeline().fireExceptionCaught(new RuntimeException("boom"));
            Assert.assertArrayEquals(
                    ascii("-ERR internal error\r\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected runtime closing flag to be set", context.statsSnapshot().closing());

            // Allow the executor to drain: already-queued commands must be skipped to avoid side effects.
            unblock.countDown();

            awaitCounter(context, c -> c.statsSnapshot().commandsSkippedClosing(), 2L, 1000);
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void internalErrorFromExecutorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                engine::execute,
                eventExecutor,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
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

        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandAdapter(), new YierdisFastCommandHandler(executor, replyWriterFactory));
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(ch, 16, 1024);
            // Enqueue commands while executor is blocked (no replies yet).
            ch.writeInbound(new ExplodingCommand());
            ch.writeInbound(request("PING"));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(2L, context.statsSnapshot().commandsEnqueued());

            // Allow the executor to drain: the first task triggers an internal error on the executor thread.
            // This should mark closing and close the connection, so already-queued commands are skipped.
            unblock.countDown();

            Assert.assertArrayEquals(
                    ascii("-ERR internal error\r\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected runtime closing flag to be set", context.statsSnapshot().closing());

            awaitCounter(context, c -> c.statsSnapshot().commandsSkippedClosing(), 1L, 1000);
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after internal error closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    private static final class ExplodingCommand implements ExecutionRequest {
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

    private static void awaitCounter(
            ExecutionConnectionContext context,
            java.util.function.ToLongFunction<ExecutionConnectionContext> counter,
            long expected,
            long timeoutMillis
    ) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            long value = counter.applyAsLong(context);
            if (value == expected) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for counter: expected=" + expected + ", actual=" + value);
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

    private static RespProtocolErrorReplyHandler protocolErrorHandler(RespReplyWriterFactory replyWriterFactory) {
        return new RespProtocolErrorReplyHandler(replyWriterFactory, ctx -> {
            NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
            if (connection != null && connection.markClosing()) {
                ctx.channel().config().setAutoRead(false);
            }
        });
    }

    private static RespCommandRequest request(String cmd, String... args) {
        byte[][] argv = new byte[args.length + 1][];
        int retainedBytes = 0;
        argv[0] = ascii(cmd);
        retainedBytes += argv[0].length;
        for (int i = 0; i < args.length; i++) {
            argv[i + 1] = ascii(args[i]);
            retainedBytes += argv[i + 1].length;
        }
        return RespCommandRequest.wrapReadOnly(argv, retainedBytes);
    }
}
