package yier.bubu.redis.testutil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.offheap.netty.YierdisNettyByteBufSink;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespWriter;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;
import yier.bubu.redis.protocol.netty.RespDecoder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * 测试辅助：通过 fast RESP pipeline 执行命令，并将响应解码为 {@link RespObject} 供断言使用。
 * <p>
 * 目的：让测试覆盖真实的协议路径（RESP 解码 → 命令执行 → RESP 编码），避免出现双实现语义漂移。
 */
public final class FastTestClient implements AutoCloseable {
    private final YierdisFastCommandProcessor processor;
    private final EmbeddedChannel decodeChannel;

    public FastTestClient(YierdisFastCommandProcessor processor) {
        Objects.requireNonNull(processor, "processor");
        this.processor = processor;
        this.decodeChannel = new EmbeddedChannel(new RespCommandDecoder());
    }

    public RespObject execute(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        byte[] wire = encodeCommand(args);
        decodeChannel.writeInbound(Unpooled.wrappedBuffer(wire));
        decodeChannel.runPendingTasks();

        Object msg = decodeChannel.readInbound();
        Assert.assertNotNull("expected command", msg);
        Assert.assertTrue("expected RespCommand", msg instanceof RespCommand);
        RespCommand cmd = (RespCommand) msg;

        ByteBuf out = Unpooled.buffer();
        try {
            processor.execute(cmd, new RespWriter(new YierdisNettyByteBufSink(out)));
            byte[] replyBytes = readAll(out);
            Assert.assertNotNull("expected reply", replyBytes);
            return decodeOne(replyBytes);
        } finally {
            out.release();
            cmd.recycle();
        }
    }

    private static RespObject decodeOne(byte[] replyBytes) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            Assert.assertTrue(ch.writeInbound(Unpooled.wrappedBuffer(replyBytes)));
            Object msg = ch.readInbound();
            Assert.assertNotNull(msg);
            Assert.assertTrue(msg instanceof RespObject);
            return (RespObject) msg;
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] readAll(ByteBuf buf) {
        if (buf == null) {
            return null;
        }
        int len = buf.readableBytes();
        byte[] bytes = new byte[len];
        if (len > 0) {
            buf.getBytes(buf.readerIndex(), bytes);
        }
        return bytes;
    }

    private static byte[] encodeCommand(List<byte[]> args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeAscii(baos, "*");
        writeAscii(baos, Integer.toString(args.size()));
        writeAscii(baos, "\r\n");
        for (byte[] arg : args) {
            writeAscii(baos, "$");
            if (arg == null) {
                writeAscii(baos, "-1\r\n");
                continue;
            }
            writeAscii(baos, Integer.toString(arg.length));
            writeAscii(baos, "\r\n");
            if (arg.length > 0) {
                baos.writeBytes(arg);
            }
            writeAscii(baos, "\r\n");
        }
        return baos.toByteArray();
    }

    private static void writeAscii(ByteArrayOutputStream baos, String s) {
        baos.writeBytes(s.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void close() {
        decodeChannel.finishAndReleaseAll();
    }
}
