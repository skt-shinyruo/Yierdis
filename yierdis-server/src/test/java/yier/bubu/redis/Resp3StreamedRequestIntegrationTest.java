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

public class Resp3StreamedRequestIntegrationTest {
    @Test
    public void streamedBlobStringArgExecutesEchoCommand() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] req = concat(
                    ascii("*2\r\n"),
                    ascii("+ECHO\r\n"),
                    ascii("$?\r\n"),
                    ascii(";5\r\n"),
                    ascii("hello\r\n"),
                    ascii(";0\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            Assert.assertArrayEquals(bulk(ascii("hello")), readOutbound(ch));
            Assert.assertTrue(ch.isActive());
        }
    }

    @Test
    public void invalidStreamedBlobChunkPrefixIsProtocolErrorAndClosesConnection() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] req = concat(
                    ascii("*2\r\n"),
                    ascii("+ECHO\r\n"),
                    ascii("$?\r\n"),
                    ascii(":1\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            Assert.assertArrayEquals(
                    ascii("-ERR Protocol error: invalid streamed blob chunk prefix\r\n"),
                    readOutbound(ch)
            );
            Assert.assertFalse("protocol error must close the connection", ch.isActive());
        }
    }

    private static final class TestEnv implements AutoCloseable {
        private final YierdisDb db;
        private final NettyCommandExecutor executor;
        private final EmbeddedChannel ch;

        private TestEnv() {
            this.db = new YierdisDb();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            this.executor = new NettyCommandExecutor(
                    db,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    1024,
                    0,
                    256,
                    128,
                    0,
                    0,
                    1024,
                    10
            );
            executor.start();
            this.ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
        }

        @Override
        public void close() {
            try {
                executor.close();
            } finally {
                db.shutdown();
                ch.finishAndReleaseAll();
            }
        }
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull("expected reply", out);
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

    private static byte[] bulk(byte[] data) {
        if (data == null) {
            return ascii("$-1\r\n");
        }
        byte[] header = ascii("$" + data.length + "\r\n");
        return concat(header, data, ascii("\r\n"));
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}

