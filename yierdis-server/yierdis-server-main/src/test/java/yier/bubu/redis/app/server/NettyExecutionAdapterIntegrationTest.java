package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
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
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyExecutionAdapterIntegrationTest {
    @Test
    public void registeredRequestSubmitsThroughNettyExecutionConnection() {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    Runnable::run,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            OrderedReplyTestFixture fixture = OrderedReplyTestFixture.open(executor, replyWriterFactory);
            try {
                fixture.write(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));
                fixture.drain();

                Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(fixture));
            } finally {
                fixture.close();
                executor.close();
            }
        }
    }

    @Test
    public void queueFullPausesIngressAndRetriesTheRegisteredRequest() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();
        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                TestYierdisEngines.forInstance(instance)::execute,
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

        OrderedReplyTestFixture fixture = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            fixture.write(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));
            TrackingExecutionRequest rejected = TrackingExecutionRequest.ofUtf8("PING");
            fixture.write(rejected);

            ExecutionConnectionContext context = fixture.connection().context();
            Assert.assertEquals(1L, context.statsSnapshot().commandsEnqueued());
            Assert.assertEquals(0L, context.statsSnapshot().commandsRejected());
            Assert.assertEquals(0, rejected.closeCalls());
            Assert.assertNull(fixture.channel().readOutbound());

            unblock.countDown();

            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(fixture, 1_000));
            Assert.assertArrayEquals(ascii("+PONG\r\n"), awaitOutbound(fixture, 1_000));
            Assert.assertEquals(1, rejected.closeCalls());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            fixture.close();
        }
    }

    @Test
    public void quitClosesAfterItsRegisteredReplyAndSkipsTheLaterSlot() throws Exception {
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        EventExecutor eventExecutor = group.next();
        YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
        RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                instance::bindToCurrentThread,
                TestYierdisEngines.forInstance(instance)::execute,
                eventExecutor,
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
                new CommandExecutorConfig(1_024, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
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

        OrderedReplyTestFixture fixture = OrderedReplyTestFixture.open(executor, replyWriterFactory);
        try {
            fixture.write(request("QUIT"));
            fixture.write(request("PING"));
            unblock.countDown();

            Assert.assertArrayEquals(ascii("+OK\r\n"), awaitOutbound(fixture, 1_000));
            fixture.drain();
            Assert.assertNull(fixture.channel().readOutbound());
            Assert.assertFalse(fixture.channel().isOpen());

            ExecutionConnectionContext context = fixture.connection().context();
            Assert.assertEquals(1L, context.statsSnapshot().closeAfterReply());
            Assert.assertEquals(1L, context.statsSnapshot().commandsExecuted());
            Assert.assertEquals(1L, context.statsSnapshot().commandsSkippedClosing());
        } finally {
            unblock.countDown();
            executor.shutdownGracefully().join();
            executor.executeOwnerTask(instance::close).join();
            group.shutdownGracefully().syncUninterruptibly();
            fixture.close();
        }
    }

    @Test
    public void echoNullBulkStringUsesTheRegisteredRequestSlot() {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
            RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    TestYierdisEngines.forInstance(instance)::execute,
                    Runnable::run,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            OrderedReplyTestFixture fixture = OrderedReplyTestFixture.open(executor, replyWriterFactory);
            try {
                fixture.write(ByteArrayExecutionRequest.wrapReadOnly(new byte[][]{ascii("ECHO"), null}, 4));
                fixture.drain();

                Assert.assertArrayEquals(ascii("$-1\r\n"), readOutbound(fixture));
            } finally {
                fixture.close();
                executor.close();
            }
        }
    }

    @Test
    public void setNullBulkStringUsesTheRegisteredRequestSlot() {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build())) {
            RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    TestYierdisEngines.forInstance(instance)::execute,
                    Runnable::run,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            OrderedReplyTestFixture fixture = OrderedReplyTestFixture.open(executor, replyWriterFactory);
            try {
                fixture.write(ByteArrayExecutionRequest.wrapReadOnly(
                        new byte[][]{ascii("SET"), ascii("k"), null},
                        4
                ));
                fixture.drain();

                Assert.assertArrayEquals(ascii("-ERR Protocol error: null bulk string\r\n"), readOutbound(fixture));
            } finally {
                fixture.close();
                executor.close();
            }
        }
    }

    private static ByteArrayExecutionRequest request(String command, String... arguments) {
        byte[][] argv = new byte[arguments.length + 1][];
        int retainedBytes = 0;
        argv[0] = ascii(command);
        retainedBytes += argv[0].length;
        for (int i = 0; i < arguments.length; i++) {
            argv[i + 1] = ascii(arguments[i]);
            retainedBytes += argv[i + 1].length;
        }
        return ByteArrayExecutionRequest.wrapReadOnly(argv, retainedBytes);
    }

    private static byte[] readOutbound(OrderedReplyTestFixture fixture) {
        fixture.drain();
        ByteBuf out = fixture.channel().readOutbound();
        Assert.assertNotNull("expected reply", out);
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }

    private static byte[] awaitOutbound(OrderedReplyTestFixture fixture, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            fixture.drain();
            ByteBuf out = fixture.channel().readOutbound();
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

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TrackingExecutionRequest implements ExecutionRequest {
        private final byte[][] argv;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private TrackingExecutionRequest(byte[][] argv) {
            this.argv = argv;
        }

        static TrackingExecutionRequest ofUtf8(String command, String... arguments) {
            byte[][] argv = new byte[arguments.length + 1][];
            argv[0] = ascii(command);
            for (int i = 0; i < arguments.length; i++) {
                argv[i + 1] = ascii(arguments[i]);
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
            return argv[index] == null ? -1 : argv[index].length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return argv[index][offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] destination, int destinationOffset) {
            System.arraycopy(argv[index], 0, destination, destinationOffset, argv[index].length);
        }

        @Override
        public byte[] toByteArray(int index) {
            return argv[index] == null ? null : argv[index].clone();
        }

        @Override
        public int retainedBytes() {
            int bytes = 0;
            for (byte[] argument : argv) {
                bytes += argument == null ? 0 : argument.length;
            }
            return bytes;
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }
}
