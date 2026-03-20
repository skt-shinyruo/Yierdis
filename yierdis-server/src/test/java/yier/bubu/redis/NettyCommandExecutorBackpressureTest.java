package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.v1.CustomCommand;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;

public class NettyCommandExecutorBackpressureTest {
    @Test
    public void queueFullReturnsErrBusy() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    new JsonLineReplyWriterFactory(),
                    1,
                    0,
                    1,
                    0,
                    0,
                    0,
                    1,
                    1,
                    SchedulingPolicy.FAIR
            );

            EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                // 第一次入队成功（执行器未 start，因此不会产生响应）。
                ch.writeInbound(new CustomCommand("PING", null));
                Assert.assertNull(ch.readOutbound());

                // 第二次入队失败，立刻返回 busy 错误。
                ch.writeInbound(new CustomCommand("PING", null));
                Assert.assertArrayEquals(ascii("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy queue_full\"}}\n"), readOutbound(ch));
            } finally {
                executor.close();
                ch.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void globalBackpressureRecoversAutoReadOnRejectedChannel() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    new JsonLineReplyWriterFactory(),
                    1,
                    0,
                    256,
                    128,
                    0,
                    0,
                    1024,
                    10,
                    SchedulingPolicy.FAIR
            );

            EmbeddedChannel ch1 = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            EmbeddedChannel ch2 = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                Assert.assertTrue(ch1.config().isAutoRead());
                Assert.assertTrue(ch2.config().isAutoRead());

                // Fill the global queue with ch1 while executor is not started (no drain).
                ch1.writeInbound(new CustomCommand("PING", null));
                Assert.assertNull(ch1.readOutbound());

                // ch2 is rejected: should return busy and enter backpressure (autoRead disabled).
                ch2.writeInbound(new CustomCommand("PING", null));
                Assert.assertArrayEquals(ascii("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR busy queue_full\"}}\n"), readOutbound(ch2));
                ch1.runPendingTasks();
                ch2.runPendingTasks();
                Assert.assertFalse(ch2.config().isAutoRead());

                // Start draining: once the backlog clears, global recovery should re-enable autoRead for ch2.
                executor.start();
                ch1.runPendingTasks();
                ch2.runPendingTasks();
                Assert.assertTrue(ch2.config().isAutoRead());
            } finally {
                executor.close();
                ch1.finishAndReleaseAll();
                ch2.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void backpressureDisablesAndReenablesAutoRead() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    new JsonLineReplyWriterFactory(),
                    16,
                    0,
                    2,
                    1,
                    0,
                    0,
                    16,
                    50,
                    SchedulingPolicy.FAIR
            );

            EmbeddedChannel ch = new EmbeddedChannel(new YierdisFastCommandHandler(executor));
            try {
                Assert.assertTrue(ch.config().isAutoRead());

                // executor 未 start 时不会 drain，pending 会累积并触发 backpressure。
                ch.writeInbound(new CustomCommand("PING", null));
                ch.writeInbound(new CustomCommand("PING", null));
                ch.runPendingTasks();
                Assert.assertFalse("autoRead should be disabled when pending >= high watermark", ch.config().isAutoRead());

                // start 后 drain 队列，pending 降到 low watermark 以下，应恢复 autoRead。
                executor.start();
                ch.runPendingTasks();
                Assert.assertTrue("autoRead should be re-enabled when pending <= low watermark", ch.config().isAutoRead());
            } finally {
                executor.close();
                ch.finishAndReleaseAll();
            }
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
