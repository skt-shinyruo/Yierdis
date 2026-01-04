package yier.bubu.redis.testutil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import yier.bubu.redis.YierdisFastCommandHandler;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.protocol.RespCommandDecoder;
import yier.bubu.redis.protocol.RespDecoder;
import yier.bubu.redis.protocol.RespObject;

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
    private final EmbeddedChannel serverChannel;

    public FastTestClient(YierdisFastCommandProcessor processor) {
        Objects.requireNonNull(processor, "processor");
        this.serverChannel = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));
    }

    public RespObject execute(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        byte[] wire = encodeCommand(args);
        serverChannel.writeInbound(Unpooled.wrappedBuffer(wire));
        serverChannel.runPendingTasks();

        byte[] replyBytes = readOutboundBytes(serverChannel);
        Assert.assertNotNull("expected reply", replyBytes);
        return decodeOne(replyBytes);
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

    private static byte[] readOutboundBytes(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        if (out == null) {
            return null;
        }
        Assert.assertTrue(out instanceof ByteBuf);
        ByteBuf buf = (ByteBuf) out;
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
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
        serverChannel.finishAndReleaseAll();
    }
}
