package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

import java.nio.charset.StandardCharsets;

public class NettyCommandExecutorBackpressureTest {
    @Test
    public void queueFullReturnsErrBusy() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                ImmediateEventExecutor.INSTANCE,
                1,
                0,
                1,
                0,
                0,
                0,
                1,
                1
        );

        EmbeddedChannel ch = new EmbeddedChannel(
                new RespCommandDecoder(),
                new YierdisFastCommandHandler(processor, executor)
        );
        try {
            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // 第一次入队成功（执行器未 start，因此不会产生响应）。
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            Assert.assertNull(ch.readOutbound());

            // 第二次入队失败，立刻返回 busy 错误。
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            Assert.assertArrayEquals(ascii("-ERR busy\r\n"), readOutbound(ch));
        } finally {
            executor.close();
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void backpressureDisablesAndReenablesAutoRead() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                processor,
                ImmediateEventExecutor.INSTANCE,
                16,
                0,
                2,
                1,
                0,
                0,
                16,
                50
        );

        EmbeddedChannel ch = new EmbeddedChannel(
                new RespCommandDecoder(),
                new YierdisFastCommandHandler(processor, executor)
        );
        try {
            Assert.assertTrue(ch.config().isAutoRead());

            byte[] ping = ascii("*1\r\n$4\r\nPING\r\n");

            // executor 未 start 时不会 drain，pending 会累积并触发 backpressure。
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
            ch.writeInbound(Unpooled.wrappedBuffer(ping));
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
