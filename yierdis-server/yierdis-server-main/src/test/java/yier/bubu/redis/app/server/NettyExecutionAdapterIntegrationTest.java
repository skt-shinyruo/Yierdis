package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.ExecutionConnectionContext;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespCommandRequest;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.RespCommandAdapter;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyExecutionAdapterIntegrationTest {
    @Test
    public void handlerSubmitsThroughNettyExecutionConnection() {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    Runnable::run,
                    new RespReplyWriterFactory(),
                    ioAdapter,
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            EmbeddedChannel channel = new EmbeddedChannel(
                    new YierdisFastCommandHandler(executor, new RespReplyWriterFactory())
            );
            try {
                NettyExecutionConnection.getOrCreate(channel, 16, 1024);
                channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));

                Assert.assertArrayEquals(
                        "+PONG\r\n".getBytes(StandardCharsets.UTF_8),
                        readOutbound(channel)
                );
            } finally {
                channel.finishAndReleaseAll();
                executor.close();
            }
        }
    }

    @Test
    public void queueFullReturnsBusyAndClosesRejectedExecutionRequest() throws Exception {
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
                new CommandExecutorConfig(1, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
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

        EmbeddedChannel channel = new EmbeddedChannel(new YierdisFastCommandHandler(executor, replyWriterFactory));
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 16, 1024);

            channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));
            Assert.assertNull("first command should stay queued while owner executor is blocked", channel.readOutbound());

            TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING");
            channel.writeInbound(rejected);

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(1L, context.statsSnapshot().commandsRejected());
            Assert.assertEquals(1, rejected.closeCalls());
            Assert.assertArrayEquals(
                    "-ERR busy queue_full\r\n"
                            .getBytes(StandardCharsets.UTF_8),
                    readOutbound(channel)
            );
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void quitClosesConnectionAndSkipsFollowupCommands() throws Exception {
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
                new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
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

        EmbeddedChannel channel = new EmbeddedChannel(
                new RespCommandAdapter(),
                new YierdisFastCommandHandler(executor, replyWriterFactory)
        );
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 16, 1024);

            channel.writeInbound(request("QUIT"));
            channel.writeInbound(request("PING"));

            unblock.countDown();

            Assert.assertArrayEquals(
                    "+OK\r\n".getBytes(StandardCharsets.UTF_8),
                    awaitOutbound(channel, 1000)
            );
            Assert.assertNull("follow-up command should be skipped after close-after-reply", channel.readOutbound());

            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            ExecutionConnectionContext context = connection.context();
            Assert.assertEquals(1L, context.statsSnapshot().closeAfterReply());
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(1L, context.statsSnapshot().commandsSkippedClosing());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            channel.finishAndReleaseAll();
        }
    }

    private static byte[] readOutbound(EmbeddedChannel channel) {
        ByteBuf out = channel.readOutbound();
        Assert.assertNotNull("expected reply", out);
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    private static byte[] awaitOutbound(EmbeddedChannel channel, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
            ByteBuf out = channel.readOutbound();
            if (out != null) {
                try {
                    byte[] bytes = new byte[out.readableBytes()];
                    out.readBytes(bytes);
                    return bytes;
                } finally {
                    out.release();
                }
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for outbound");
            }
            Thread.sleep(5);
        }
    }

    private static RespCommandRequest request(String cmd, String... args) {
        byte[][] argv = new byte[args.length + 1][];
        int retainedBytes = 0;
        argv[0] = utf8(cmd);
        retainedBytes += argv[0].length;
        for (int i = 0; i < args.length; i++) {
            argv[i + 1] = utf8(args[i]);
            retainedBytes += argv[i + 1].length;
        }
        return RespCommandRequest.wrapReadOnly(argv, retainedBytes);
    }

    private static final class TrackingExecutionRequest implements ExecutionRequest {
        private final byte[][] argv;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private TrackingExecutionRequest(byte[][] argv) {
            this.argv = argv;
        }

        static TrackingExecutionRequest ofUtf8(String cmd, String... args) {
            byte[][] argv = new byte[args.length + 1][];
            argv[0] = utf8(cmd);
            for (int i = 0; i < args.length; i++) {
                argv[i + 1] = args[i] == null ? null : utf8(args[i]);
            }
            return new TrackingExecutionRequest(argv);
        }

        int closeCalls() {
            return closeCalls.get();
        }

        @Override
        public int argc() {
            return argv.length;
        }

        @Override
        public boolean isNull(int index) {
            return argv[index] == null;
        }

        @Override
        public int len(int index) {
            byte[] arg = argv[index];
            return arg == null ? -1 : arg.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] arg = argv[index];
            System.arraycopy(arg, 0, dst, dstOff, arg.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            byte[] arg = argv[index];
            return arg == null ? null : arg.clone();
        }

        @Override
        public int retainedBytes() {
            int retainedBytes = 0;
            for (byte[] arg : argv) {
                retainedBytes += arg == null ? 0 : arg.length;
            }
            return retainedBytes;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        private static byte[] utf8(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
