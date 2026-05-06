package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.engine.YierdisEngine;
import yier.bubu.redis.executor.CommandExecutor;
import yier.bubu.redis.executor.CommandExecutorConfig;
import yier.bubu.redis.executor.ExecutionConnectionContext;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.netty.ProtocolCommandAdapter;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyExecutionAdapterIntegrationTest {
    @Test
    public void handlerSubmitsThroughNettyExecutionConnection() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    Runnable::run,
                    new JsonLineReplyWriterFactory(),
                    ioAdapter,
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            EmbeddedChannel channel = new EmbeddedChannel(
                    new YierdisFastCommandHandler(executor, new JsonLineReplyWriterFactory())
            );
            try {
                NettyExecutionConnection.getOrCreate(channel, 16, 1024);
                channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));

                Assert.assertArrayEquals(
                        "{\"ok\":true,\"result\":\"PONG\"}\n".getBytes(StandardCharsets.UTF_8),
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

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        JsonLineReplyWriterFactory replyWriterFactory = new JsonLineReplyWriterFactory();
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
                    "{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy queue_full\"}}\n"
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

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisEngine engine = TestYierdisEngines.forInstance(instance);
        JsonLineReplyWriterFactory replyWriterFactory = new JsonLineReplyWriterFactory();
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
                new ProtocolCommandAdapter(),
                new YierdisFastCommandHandler(executor, replyWriterFactory)
        );
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.getOrCreate(channel, 16, 1024);

            channel.writeInbound(request("QUIT"));
            channel.writeInbound(request("PING"));

            unblock.countDown();

            Assert.assertArrayEquals(
                    "{\"ok\":true,\"result\":\"OK\"}\n".getBytes(StandardCharsets.UTF_8),
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

    private static CustomProtocolV1Request request(String cmd, String... args) {
        return new CustomProtocolV1Request(cmd, Arrays.asList(args));
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
}
