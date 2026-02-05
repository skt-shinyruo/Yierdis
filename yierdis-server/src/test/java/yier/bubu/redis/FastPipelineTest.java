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

public class FastPipelineTest {
    @Test
    public void pingWorksThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("*1\r\n$4\r\nPING\r\n")));

            Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void pingNullEmptyAndBinaryMessagesArePreservedThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), (byte[]) null)));
            Assert.assertArrayEquals(ascii("$-1\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), b(""))));
            Assert.assertArrayEquals(ascii("$0\r\n\r\n"), readOutbound(ch));

            byte[] bin = new byte[]{0, (byte) 0xFF};
            ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), bin)));
            Assert.assertArrayEquals(concat(ascii("$" + bin.length + "\r\n"), bin, ascii("\r\n")), readOutbound(ch));
        }
    }

    @Test
    public void setGetAreBinarySafeThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] value = new byte[]{0, 1, (byte) 0xFF, '\n'};

            byte[] set = command(b("SET"), key, value);
            ch.writeInbound(Unpooled.wrappedBuffer(set));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            byte[] get = command(b("GET"), key);
            ch.writeInbound(Unpooled.wrappedBuffer(get));

            byte[] resp = readOutbound(ch);
            byte[] expected = concat(
                    ascii("$" + value.length + "\r\n"),
                    value,
                    ascii("\r\n")
            );
            Assert.assertArrayEquals(expected, resp);
        }
    }

    @Test
    public void typeAndExistsAreBinarySafeThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] k1 = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] k2 = b("k2");

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SET"), k1, b("v1"))));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SET"), k2, b("v2"))));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("TYPE"), k1)));
            Assert.assertArrayEquals(ascii("+string\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("EXISTS"), k1, b("missing"), k2)));
            Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void strlenTtlAndExpireAreBinarySafeThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
            byte[] value = b("hello");

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SET"), key, value)));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("STRLEN"), key)));
            Assert.assertEquals(value.length, parseInteger(readOutbound(ch)));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("EXPIRE"), key, b("10"))));
            Assert.assertArrayEquals(ascii(":1\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("TTL"), key)));
            long ttl = parseInteger(readOutbound(ch));
            Assert.assertTrue("ttl should be close to 10 seconds", ttl >= 8 && ttl <= 10);

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("TTL"), b("missing"))));
            Assert.assertEquals(-2L, parseInteger(readOutbound(ch)));
        }
    }

    @Test
    public void getAfterAppendDoesNotLeakCapacityBytesThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] key = b("k");

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SET"), key, b("a"))));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("APPEND"), key, b("b"))));
            Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("GET"), key)));
            Assert.assertArrayEquals(ascii("$2\r\nab\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void lrangeStreamsPackedElementsThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("RPUSH"), b("l"), b("a"), b("b"), b("c"))));
            Assert.assertArrayEquals(ascii(":3\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("LRANGE"), b("l"), b("0"), b("-1"))));
            Assert.assertArrayEquals(ascii("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void hgetallStreamsPackedPairsThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("HSET"), b("h"), b("f1"), b("v1"), b("f2"), b("v2"))));
            Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("HGETALL"), b("h"))));
            Assert.assertArrayEquals(ascii("*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void smembersStreamsIntsetThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SADD"), b("s"), b("2"), b("1"))));
            Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("SMEMBERS"), b("s"))));
            Assert.assertArrayEquals(ascii("*2\r\n$1\r\n1\r\n$1\r\n2\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void zrangeAndRangeByScoreStreamPackedZsetThroughFastPipeline() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZADD"), b("z"), b("1"), b("a"), b("1"), b("b"), b("2"), b("c"))));
            Assert.assertArrayEquals(ascii(":3\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZRANGE"), b("z"), b("0"), b("-1"), b("WITHSCORES"))));
            Assert.assertArrayEquals(ascii("*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n1\r\n$1\r\nc\r\n$1\r\n2\r\n"), readOutbound(ch));

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZRANGEBYSCORE"), b("z"), b("1"), b("1"), b("WITHSCORES"))));
            Assert.assertArrayEquals(ascii("*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n1\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void decoderHandlesPartialFrames() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] full = ascii("*1\r\n$4\r\nPING\r\n");
            ByteBuf part1 = Unpooled.wrappedBuffer(full, 0, 7); // "*1\r\n$4"
            ByteBuf part2 = Unpooled.wrappedBuffer(full, 7, full.length - 7);

            ch.writeInbound(part1);
            Assert.assertNull(ch.readOutbound());

            ch.writeInbound(part2);
            Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void nullBulkStringKeyReturnsErrorAndConnectionStaysOpen() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("GET"), (byte[]) null)));
            Assert.assertArrayEquals(ascii("-ERR Protocol error: null bulk string\r\n"), readOutbound(ch));
            Assert.assertTrue(ch.isOpen());

            ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"))));
            Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
        }
    }

    @Test
    public void protocolErrorsReturnErrAndCloseConnection() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[][] bad = new byte[][]{
                    // invalid array length
                    ascii("*-1\r\n"),
                    // invalid request prefix (control byte)
                    new byte[]{0x01, '\r', '\n'}
            };

            for (byte[] input : bad) {
                try {
                    ch.writeInbound(Unpooled.wrappedBuffer(input));
                } catch (Exception ignored) {
                    // EmbeddedChannel may surface decoder exceptions; the handler should still have produced an error reply.
                }
                Assert.assertTrue(
                        "expected protocol error reply",
                        new String(readOutbound(ch), java.nio.charset.StandardCharsets.US_ASCII).startsWith("-ERR Protocol error:")
                );

                ch.runPendingTasks();
                Assert.assertFalse(ch.isOpen());

                // 每个 case 复用新的 channel（协议错误会关闭连接）。
                ch.finishAndReleaseAll();
                ch = env.newChannel();
            }
        }
    }

    @Test
    public void quitReturnsOkAndCloses() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            ch.writeInbound(Unpooled.wrappedBuffer(ascii("*1\r\n$4\r\nQUIT\r\n")));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.runPendingTasks();
            Assert.assertFalse(ch.isOpen());
        }
    }

    @Test
    public void pipelinePingThenQuitKeepsOrderAndCloses() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] pipelined = concat(
                    command(b("PING")),
                    command(b("QUIT"))
            );
            ch.writeInbound(Unpooled.wrappedBuffer(pipelined));

            Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

            ch.runPendingTasks();
            Assert.assertFalse(ch.isOpen());
        }
    }

    @Test
    public void quitSkipsSubsequentCommandsToAvoidSideEffects() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] key = b("a");

            byte[] pipelined = concat(
                    command(b("SET"), key, b("1")),
                    command(b("QUIT")),
                    command(b("INCR"), key)
            );
            ch.writeInbound(Unpooled.wrappedBuffer(pipelined));

            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch)); // SET
            Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch)); // QUIT

            // QUIT 后连接关闭：后续命令不应被执行，也不应产生额外响应。
            Assert.assertNull(ch.readOutbound());

            ch.runPendingTasks();
            Assert.assertFalse(ch.isOpen());

            Assert.assertArrayEquals(b("1"), env.db.getStringBytes(key));
        }
    }

    private static final class TestEnv implements AutoCloseable {
        private final YierdisDb db;
        private final NettyCommandExecutor executor;
        private EmbeddedChannel ch;

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
            this.ch = newChannel();
        }

        private EmbeddedChannel newChannel() {
            EmbeddedChannel next = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(executor));
            this.ch = next;
            return next;
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

    private static byte[] command(byte[]... args) {
        byte[] header = ascii("*" + args.length + "\r\n");
        byte[][] parts = new byte[args.length + 1][];
        parts[0] = header;
        for (int i = 0; i < args.length; i++) {
            parts[i + 1] = bulk(args[i]);
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

    private static byte[] readOutbound(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull(out);
        Assert.assertTrue(out instanceof ByteBuf);
        ByteBuf buf = (ByteBuf) out;
        byte[] b = new byte[buf.readableBytes()];
        buf.readBytes(b);
        buf.release();
        return b;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static long parseInteger(byte[] resp) {
        String s = new String(resp, StandardCharsets.US_ASCII);
        Assert.assertTrue(s.endsWith("\r\n"));
        Assert.assertTrue(s.length() >= 4);
        Assert.assertEquals(':', s.charAt(0));
        return Long.parseLong(s.substring(1, s.length() - 2));
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
