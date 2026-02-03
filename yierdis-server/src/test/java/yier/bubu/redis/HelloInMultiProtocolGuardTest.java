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

public class HelloInMultiProtocolGuardTest {
    @Test
    public void hello3IsRejectedInMultiAndDoesNotSwitchProtocol() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(cmd("MULTI")));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(cmd("HELLO", "3")));
            Assert.assertArrayEquals(ascii("-ERR HELLO is not allowed in MULTI\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(cmd("EXEC")));
            Assert.assertArrayEquals(ascii("-EXECABORT Transaction discarded because of previous errors.\r\n"), readOutbound(ch));

            // 协议未被切换：仍为 RESP2（missing 返回 $-1，而不是 RESP3 的 _）。
            ch.writeInbound(Unpooled.wrappedBuffer(cmd("GET", "missing")));
            Assert.assertArrayEquals(ascii("$-1\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void hello2IsRejectedInMultiAndDoesNotSwitchBackToResp2() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            // 先切换到 RESP3，确保后续 missing 用 RESP3 null（_）作为锚点。
            ch.writeInbound(Unpooled.wrappedBuffer(cmd("HELLO", "3")));
            Assert.assertArrayEquals(expectedHello3Reply(), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(cmd("MULTI")));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            // MULTI 中禁止 HELLO（含 HELLO 2），避免在 EXEC reply 中途切回 RESP2 造成语义漂移。
            ch.writeInbound(Unpooled.wrappedBuffer(cmd("HELLO", "2")));
            Assert.assertArrayEquals(ascii("-ERR HELLO is not allowed in MULTI\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(cmd("EXEC")));
            Assert.assertArrayEquals(ascii("-EXECABORT Transaction discarded because of previous errors.\r\n"), readOutbound(ch));

            // 协议仍为 RESP3（missing 返回 _）。
            ch.writeInbound(Unpooled.wrappedBuffer(cmd("GET", "missing")));
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

    private static byte[] expectedHello3Reply() {
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
        return expectedHello;
    }

    private static String loadVersion() {
        String version = "unknown";
        try (InputStream in = HelloInMultiProtocolGuardTest.class.getResourceAsStream("/yierdis-version.properties")) {
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

    private static byte[] cmd(String... argv) {
        if (argv == null || argv.length == 0) {
            return ascii("*0\r\n");
        }
        byte[][] parts = new byte[1 + argv.length * 3][];
        int p = 0;
        parts[p++] = ascii("*" + argv.length + "\r\n");
        for (String a : argv) {
            byte[] data = a == null ? new byte[0] : a.getBytes(StandardCharsets.US_ASCII);
            parts[p++] = ascii("$" + data.length + "\r\n");
            parts[p++] = data;
            parts[p++] = ascii("\r\n");
        }
        return concat(parts);
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
            if (p != null) {
                total += p.length;
            }
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            if (p == null || p.length == 0) {
                continue;
            }
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
