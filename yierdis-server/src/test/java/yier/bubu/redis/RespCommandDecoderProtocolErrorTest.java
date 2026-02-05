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

public class RespCommandDecoderProtocolErrorTest {
    @Test
    public void decoderTreatsRespTypePrefixesAsInlineCommandInsteadOfProtocolError() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            // Redis-like behavior: top-level non-array is parsed as inline command.
            ch.writeInbound(Unpooled.wrappedBuffer(ascii("%0\r\n")));

            Assert.assertArrayEquals(ascii("-ERR unknown command '%0'\r\n"), readOutbound(ch));
            Assert.assertTrue("unknown command must keep the connection open", ch.isActive());
        }
    }

    @Test
    public void decoderRejectsInvalidControlCharsAsProtocolError() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{0x01, '\r', '\n'}));

            Assert.assertArrayEquals(ascii("-ERR Protocol error: invalid request\r\n"), readOutbound(ch));
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

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
