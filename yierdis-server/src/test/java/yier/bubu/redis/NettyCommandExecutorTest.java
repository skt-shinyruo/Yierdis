package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyCommandExecutorTest {
    @Test
    public void queueFullReturnsErrBusy() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                eventExecutor,
                new JsonLineReplyWriterFactory(),
                1,
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

        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        eventExecutor.submit(() -> {
            blockerStarted.countDown();
            unblock.await();
            return null;
        });
        Assert.assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));

        CountDownLatch blocker2Started = new CountDownLatch(1);
        CountDownLatch unblock2 = new CountDownLatch(1);

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            ch.writeInbound(request("PING"));
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(0L, context.statsSnapshot().commandsRejected());
            Assert.assertNull("first command should be queued (no reply yet)", ch.readOutbound());

            ch.writeInbound(request("PING"));
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(1L, context.statsSnapshot().commandsRejected());
            Assert.assertArrayEquals(ascii("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy queue_full\"}}\n"), readOutbound(ch));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void safeRetainedBytesFallsBackToZeroWhenRequestThrows() {
        Assert.assertEquals(0, NettyCommandExecutor.safeRetainedBytes(new ThrowingRetainedBytesRequest()));
    }

    @Test
    public void handlerClosesRejectedDirectExecutionRequests() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();

        YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                instance::bindToCurrentThread,
                processor,
                eventExecutor,
                new JsonLineReplyWriterFactory(),
                1,
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
            ch.writeInbound(TrackingExecutionRequest.ofUtf8("PING"));
            TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING");
            ch.writeInbound(rejected);

            Assert.assertEquals(1, rejected.closeCalls());
            Assert.assertEquals(1L, ServerConnectionContext.getOrCreate(ch).statsSnapshot().commandsRejected());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void handlerExecutesDirectExecutionRequestMessages() throws Exception {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
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

            EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("PING");
                ch.writeInbound(request);
                Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch, 1000));
                Assert.assertEquals(1, request.closeCalls());
                Assert.assertEquals(1L, ServerConnectionContext.getOrCreate(ch).statsSnapshot().commandsExecuted());
            } finally {
                executor.close();
                ch.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void closingChannelsRecycleQueuedExecutionRequestsWithoutExecuting() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
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

            EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
                Assert.assertTrue(context.markClosing());

                TrackingExecutionRequest request = TrackingExecutionRequest.ofUtf8("PING");
                ch.writeInbound(request);

                Assert.assertNull(readOutbound(ch));
                Assert.assertEquals(1, request.closeCalls());
                Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
                Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());
                Assert.assertEquals(1L, context.statsSnapshot().commandsSkippedClosing());
            } finally {
                executor.close();
                ch.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void executionFailureClosesAcceptedExecutionRequests() throws Exception {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
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

            EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                TrackingExecutionRequest request = TrackingExecutionRequest.failingOnCommandRead("PING");
                ch.writeInbound(request);

                Assert.assertArrayEquals(
                        ascii("{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":\"ERR internal error\"}}\n"),
                        awaitOutbound(ch, 1000)
                );
                Assert.assertEquals(1, request.closeCalls());
                ch.runPendingTasks();
                ch.runScheduledPendingTasks();
                Assert.assertFalse(ch.isActive());
            } finally {
                executor.close();
                ch.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void maxDrainCommandsLimitsPerTick() throws Exception {
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
                1,
                1000,
                SchedulingPolicy.FAIR
        );
        executor.start();

        // Block the executor thread so we can control when drain ticks run.
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

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            // Enqueue 2 commands while executor is blocked.
            ch.writeInbound(request("PING"));
            ch.writeInbound(request("PING"));
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
            Assert.assertEquals(2L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0L, context.statsSnapshot().commandsExecuted());

            // Queue a second blocker behind the first drain tick, so we can observe intermediate state.
            eventExecutor.submit(() -> {
                blocker2Started.countDown();
                unblock2.await();
                return null;
            });

            // Allow one drain tick to run (maxDrainCommands=1), then the second blocker takes over.
            unblock1.countDown();
            Assert.assertTrue(blocker2Started.await(1, TimeUnit.SECONDS));

            // Only the first command should have been processed in the first drain tick.
            byte[] r1 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), r1);
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertNull("second reply must wait for next drain tick", readOutbound(ch));

            // Allow the second drain tick to run and produce the second reply.
            unblock2.countDown();
            byte[] r2 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), r2);
            Assert.assertEquals(2L, context.statsSnapshot().commandsExecuted());
        } finally {
            unblock2.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void autoReadIsDisabledAndReenabledWithHysteresis() throws Exception {
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
                1,
                0,
                0,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
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

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            ch.writeInbound(request("PING"));
            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());

            ch.runPendingTasks();
            Assert.assertFalse("autoRead should be disabled after reaching high watermark", ch.config().isAutoRead());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureEnter());
            Assert.assertEquals(0L, context.statsSnapshot().backpressureExit());

            unblock.countDown();

            // Wait for a reply and allow scheduled tasks (flush + autoRead re-enable) to run.
            byte[] reply = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), reply);

            ch.runPendingTasks();
            Assert.assertTrue("autoRead should be re-enabled after backlog drains", ch.config().isAutoRead());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureEnter());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureExit());
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void queuedBytesBudgetRejectsAndRecoversAfterDrain() throws Exception {
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
                7,
                256,
                128,
                0,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
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

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            ch.writeInbound(request("PING"));
            Assert.assertNull("first command should be queued (no reply yet)", ch.readOutbound());

            NettyCommandExecutor.StatsSnapshot s1 = executor.statsSnapshot();
            Assert.assertTrue("expected queued bytes > 0 when queueMaxBytes is enabled", s1.queuedBytes > 0);

            ch.writeInbound(request("PING"));
            Assert.assertArrayEquals(ascii("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy bytes_budget\"}}\n"), readOutbound(ch));

            NettyCommandExecutor.StatsSnapshot s2 = executor.statsSnapshot();
            Assert.assertEquals("reject path must not leak queued bytes", s1.queuedBytes, s2.queuedBytes);

            unblock.countDown();

            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch, 1000));

            NettyCommandExecutor.StatsSnapshot s3 = executor.statsSnapshot();
            Assert.assertEquals("after drain, queued tasks should be released", 0, s3.queuedTasks);
            Assert.assertEquals("after drain, queued bytes should be released", 0L, s3.queuedBytes);

            ch.writeInbound(request("PING"));
            Assert.assertArrayEquals("budget should recover after drain", ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch, 1000));
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void autoReadIsDisabledAndReenabledWithBytesHysteresis() throws Exception {
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
                1,
                0,
                128,
                10,
                SchedulingPolicy.FAIR
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

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            ch.writeInbound(request("PING"));

            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(1, context.statsSnapshot().pending());
            Assert.assertTrue("sanity: pending should be below high watermark, so bytes must be the trigger", context.statsSnapshot().pending() < 256);
            Assert.assertTrue("sanity: pending bytes should be > 0", context.statsSnapshot().pendingBytes() > 0);

            ch.runPendingTasks();
            Assert.assertFalse("autoRead should be disabled after reaching bytes high watermark", ch.config().isAutoRead());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureEnter());
            Assert.assertEquals(0L, context.statsSnapshot().backpressureExit());

            unblock.countDown();

            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"PONG\"}\n"), awaitOutbound(ch, 1000));

            ch.runPendingTasks();
            Assert.assertTrue("autoRead should be re-enabled after bytes backlog drains", ch.config().isAutoRead());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureEnter());
            Assert.assertEquals(1L, context.statsSnapshot().backpressureExit());
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(0, context.statsSnapshot().pending());
            Assert.assertEquals(0L, context.statsSnapshot().pendingBytes());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void quitClosesConnectionAndSkipsFollowupCommands() throws Exception {
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
                128,
                10,
                SchedulingPolicy.FAIR
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

        EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter(), new YierdisFastCommandHandler(executor));
        try {
            // Enqueue QUIT + PING while executor is blocked, so both are accepted.
            ch.writeInbound(request("QUIT"));
            ch.writeInbound(request("PING"));

            ServerConnectionContext context = ServerConnectionContext.getOrCreate(ch);
            Assert.assertEquals(2L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0L, context.statsSnapshot().closeAfterReply());

            unblock.countDown();

            byte[] r1 = awaitOutbound(ch, 1000);
            Assert.assertArrayEquals(ascii("{\"ok\":true,\"result\":\"OK\"}\n"), r1);

            // No reply for the followup PING; it should be skipped after closing is requested.
            Assert.assertNull(readOutbound(ch));

            // Allow close/flush and any scheduled tasks to run.
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();

            Assert.assertEquals(1L, context.statsSnapshot().closeAfterReply());
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(1L, context.statsSnapshot().commandsSkippedClosing());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().syncUninterruptibly();
            executor.executor().submit(instance::close).syncUninterruptibly();
            group.shutdownGracefully().syncUninterruptibly();
            ch.finishAndReleaseAll();
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

    private static CustomProtocolV1Request request(String cmd, String... args) {
        return new CustomProtocolV1Request(cmd, Arrays.asList(args));
    }

    private static final class ThrowingRetainedBytesRequest implements ExecutionRequest {
        @Override
        public int retainedBytes() {
            throw new RuntimeException("boom");
        }

        @Override
        public int argc() {
            return 1;
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return 4;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return 'P';
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            // no-op
        }

        @Override
        public byte[] toByteArray(int index) {
            return ascii("PING");
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class TrackingExecutionRequest implements ExecutionRequest {
        private final byte[][] argv;
        private final int retainedBytes;
        private final boolean failOnCommandRead;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private TrackingExecutionRequest(byte[][] argv, int retainedBytes, boolean failOnCommandRead) {
            this.argv = argv;
            this.retainedBytes = retainedBytes;
            this.failOnCommandRead = failOnCommandRead;
        }

        static TrackingExecutionRequest ofUtf8(String cmd, String... args) {
            return fromUtf8(false, cmd, args);
        }

        static TrackingExecutionRequest failingOnCommandRead(String cmd, String... args) {
            return fromUtf8(true, cmd, args);
        }

        private static TrackingExecutionRequest fromUtf8(boolean failOnCommandRead, String cmd, String... args) {
            byte[][] argv = new byte[args.length + 1][];
            int retainedBytes = 0;

            argv[0] = utf8(cmd);
            retainedBytes += argv[0].length;

            for (int i = 0; i < args.length; i++) {
                if (args[i] == null) {
                    continue;
                }
                argv[i + 1] = utf8(args[i]);
                retainedBytes += argv[i + 1].length;
            }
            return new TrackingExecutionRequest(argv, retainedBytes, failOnCommandRead);
        }

        int closeCalls() {
            return closeCalls.get();
        }

        @Override
        public int retainedBytes() {
            return retainedBytes;
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
            if (failOnCommandRead && index == 0) {
                throw new RuntimeException("boom");
            }
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            byte[] arg = argv[index];
            if (arg == null) {
                throw new IllegalStateException("arg is null");
            }
            System.arraycopy(arg, 0, dst, dstOff, arg.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            byte[] arg = argv[index];
            if (arg == null) {
                return null;
            }
            return arg.clone();
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
