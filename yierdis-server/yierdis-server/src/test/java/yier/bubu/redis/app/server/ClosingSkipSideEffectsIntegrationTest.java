package yier.bubu.redis.app.server;

import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.ExecutionConnectionContext;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.protocol.resp.RespReplyWriter;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClosingSkipSideEffectsIntegrationTest {
    @Test
    public void peerDisconnectClosesWithoutSchedulingAnInternalErrorReply() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
        BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance.runtimeAccess()::bindToCurrentThread,
                dispatcher::prepare,
                new NettySerialOwnerExecutor(eventExecutor),
                new RespReplySizer(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        OrderedReplyTestFixture replies = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            EmbeddedChannel channel = replies.channel();

            channel.pipeline().fireExceptionCaught(new SocketException("Connection reset"));

            Assert.assertNull("peer disconnect must not synthesize an internal error reply", readOutbound(channel));
            Assert.assertFalse("peer disconnect must close the channel", channel.isOpen());
        } finally {
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
        }
    }

    @Test
    public void productionIngressMarksProtocolErrorClosingAndClosesAfterReply() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
        BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance.runtimeAccess()::bindToCurrentThread,
                dispatcher::prepare,
                new NettySerialOwnerExecutor(eventExecutor),
                new RespReplySizer(),
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

        OrderedReplyTestFixture replies = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            EmbeddedChannel ch = replies.channel();
            NettyExecutionConnection connection = replies.connection();
            replies.write(request("PING"));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());

            replies.writeProtocolError("ERR Protocol error: invalid inline command");
            Assert.assertNull("terminal reply must wait for the earlier registered slot", readOutbound(ch));

            unblock.countDown();
            awaitCounter(context, c -> c.statsSnapshot().commandsSkippedClosing(), 1L, 1000);
            Assert.assertArrayEquals(
                    ascii("-ERR Protocol error: invalid inline command\r\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertTrue("expected production ingress to mark connection closing", context.statsSnapshot().closing());
            Assert.assertTrue(
                    "production ingress should close after replying",
                    awaitChannelClosed(ch, 1000)
            );

            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after protocol close begins", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
        }
    }

    @Test
    public void commandHandlerFallbackStillTreatsThrownDecoderFailuresAsInternalErrors() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
        BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance.runtimeAccess()::bindToCurrentThread,
                dispatcher::prepare,
                new NettySerialOwnerExecutor(eventExecutor),
                new RespReplySizer(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
        );
        executor.start();

        OrderedReplyTestFixture replies = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            EmbeddedChannel ch = replies.channel();
            NettyExecutionConnection connection = replies.connection();

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
            replies.close();
        }
    }

    @Test
    public void internalErrorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
        BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance.runtimeAccess()::bindToCurrentThread,
                dispatcher::prepare,
                new NettySerialOwnerExecutor(eventExecutor),
                new RespReplySizer(),
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

        OrderedReplyTestFixture replies = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            EmbeddedChannel ch = replies.channel();
            NettyExecutionConnection connection = replies.connection();
            // Enqueue commands while executor is blocked (no replies yet).
            replies.write(request("PING"));
            replies.write(request("PING"));
            Assert.assertNull("expected no reply while executor is blocked", readOutbound(ch));

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(2L, context.statsSnapshot().commandsEnqueued());

            // Trigger an internal error: handler should mark closing and close the channel after replying.
            ch.pipeline().fireExceptionCaught(new RuntimeException("boom"));
            Assert.assertTrue("expected runtime closing flag to be set", context.statsSnapshot().closing());

            // Allow the executor to drain: already-queued commands must be skipped to avoid side effects.
            unblock.countDown();

            awaitCounter(context, c -> c.statsSnapshot().commandsSkippedClosing(), 2L, 1000);
            Assert.assertArrayEquals(
                    ascii("-ERR internal error\r\n"),
                    awaitOutbound(ch, 1000)
            );
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
        }
    }

    @Test
    public void internalErrorFromExecutorMarksClosingAndSkipsAlreadyQueuedCommands() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
        BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory = RespReplyWriter::new;
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance.runtimeAccess()::bindToCurrentThread,
                dispatcher::prepare,
                new NettySerialOwnerExecutor(eventExecutor),
                new RespReplySizer(),
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

        OrderedReplyTestFixture replies = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            EmbeddedChannel ch = replies.channel();
            NettyExecutionConnection connection = replies.connection();
            // Enqueue commands while executor is blocked (no replies yet).
            replies.write(new ExplodingCommand());
            replies.write(request("PING"));
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
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("no command reply should be produced after internal error closing is requested", readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            replies.close();
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

    private static boolean awaitChannelClosed(EmbeddedChannel ch, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (ch.isOpen() && System.nanoTime() < deadline) {
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();
            Thread.sleep(5);
        }
        return !ch.isOpen();
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

    private static ByteArrayExecutionRequest request(String cmd, String... args) {
        byte[][] argv = new byte[args.length + 1][];
        int retainedBytes = 0;
        argv[0] = ascii(cmd);
        retainedBytes += argv[0].length;
        for (int i = 0; i < args.length; i++) {
            argv[i + 1] = ascii(args[i]);
            retainedBytes += argv[i + 1].length;
        }
        return ByteArrayExecutionRequest.wrapReadOnly(argv, retainedBytes);
    }
}
