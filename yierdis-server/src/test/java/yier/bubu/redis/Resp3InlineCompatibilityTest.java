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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Resp3InlineCompatibilityTest {
    @Test
    public void inlinePingWorksThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("PING\r\n")));
            Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void inlineEchoWorksThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO hello\r\n")));
            Assert.assertArrayEquals(ascii("$5\r\nhello\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void inlineEchoSupportsDoubleQuotesWithSpaces() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO \"hello world\"\r\n")));
            Assert.assertArrayEquals(bulk(ascii("hello world")), readOutbound(ch));
        }
    }

    @Test
    public void inlineEchoSupportsSingleQuotesWithSpaces() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO 'hello world'\r\n")));
            Assert.assertArrayEquals(bulk(ascii("hello world")), readOutbound(ch));
        }
    }

    @Test
    public void inlineEchoSupportsEscapesAndHexInDoubleQuotes() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO \"a\\n\\x00b\"\r\n")));
            Assert.assertArrayEquals(bulk(new byte[]{'a', '\n', 0, 'b'}), readOutbound(ch));
        }
    }

    @Test
    public void inlineCommandRejectsUnbalancedQuotes() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO \"unterminated\r\n")));
            Assert.assertArrayEquals(
                    ascii("-ERR Protocol error: unbalanced quotes in inline command\r\n"),
                    readOutbound(ch)
            );
        }
    }

    @Test
    public void inlineCommandRejectsGarbageAfterClosingQuote() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("ECHO \"foo\"bar\r\n")));
            Assert.assertArrayEquals(
                    ascii("-ERR Protocol error: invalid inline command\r\n"),
                    readOutbound(ch)
            );
        }
    }

    @Test
    public void hello3SwitchesToResp3AndMissingBulkStringIsNull() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            // HELLO 3 is sent as normal command (array of bulk strings); replies must be RESP3 map.
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("HELLO")),
                    bulk(ascii("3"))
            )));

            byte[] helloReply = readOutbound(ch);
            byte[] expectedHello = concat(
                    ascii("%5\r\n"),
                    bulk(ascii("server")),
                    bulk(ascii("yierdis")),
                    bulk(ascii("version")),
                    bulk(ascii(loadVersion())),
                    bulk(ascii("proto")),
                    bulk(ascii("3")),
                    bulk(ascii("mode")),
                    bulk(ascii("standalone")),
                    bulk(ascii("role")),
                    bulk(ascii("master"))
            );
            Assert.assertArrayEquals(expectedHello, helloReply);

            // After switching to RESP3, null bulk strings should be encoded as RESP3 null (_).
            ch.writeInbound(Unpooled.wrappedBuffer(concat(
                    ascii("*2\r\n"),
                    bulk(ascii("GET")),
                    bulk(ascii("missing"))
            )));
            Assert.assertArrayEquals(ascii("_\r\n"), readOutbound(ch));
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

    private static String loadVersion() {
        String version = "unknown";
        try (InputStream in = Resp3InlineCompatibilityTest.class.getResourceAsStream("/yierdis-version.properties")) {
            if (in == null) {
                return version;
            }
            Properties props = new Properties();
            props.load(in);
            String v = props.getProperty("version");
            if (v != null && !v.isBlank()) {
                version = v.trim();
            }
        } catch (IOException ignored) {
            // ignore
        }
        return version;
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
