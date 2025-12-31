package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommandDecoder;

import java.nio.charset.StandardCharsets;

public class FastPipelineTest {
    @Test
    public void pingWorksThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(ascii("*1\r\n$4\r\nPING\r\n")));

        Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));
        ch.finishAndReleaseAll();
    }

    @Test
    public void pingNullEmptyAndBinaryMessagesArePreservedThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), (byte[]) null)));
        Assert.assertArrayEquals(ascii("$-1\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), b(""))));
        Assert.assertArrayEquals(ascii("$0\r\n\r\n"), readOutbound(ch));

        byte[] bin = new byte[]{0, (byte) 0xFF};
        ch.writeInbound(Unpooled.wrappedBuffer(command(b("PING"), bin)));
        Assert.assertArrayEquals(concat(ascii("$" + bin.length + "\r\n"), bin, ascii("\r\n")), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void setGetAreBinarySafeThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

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

        ch.finishAndReleaseAll();
    }

    @Test
    public void typeAndExistsAreBinarySafeThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

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

        ch.finishAndReleaseAll();
    }

    @Test
    public void strlenTtlAndExpireAreBinarySafeThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

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

        ch.finishAndReleaseAll();
    }

    @Test
    public void getAfterAppendDoesNotLeakCapacityBytesThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        byte[] key = b("k");

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("SET"), key, b("a"))));
        Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("APPEND"), key, b("b"))));
        Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("GET"), key)));
        Assert.assertArrayEquals(ascii("$2\r\nab\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void lrangeStreamsPackedElementsThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("RPUSH"), b("l"), b("a"), b("b"), b("c"))));
        Assert.assertArrayEquals(ascii(":3\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("LRANGE"), b("l"), b("0"), b("-1"))));
        Assert.assertArrayEquals(ascii("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void hgetallStreamsPackedPairsThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("HSET"), b("h"), b("f1"), b("v1"), b("f2"), b("v2"))));
        Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("HGETALL"), b("h"))));
        Assert.assertArrayEquals(ascii("*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void smembersStreamsIntsetThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("SADD"), b("s"), b("2"), b("1"))));
        Assert.assertArrayEquals(ascii(":2\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("SMEMBERS"), b("s"))));
        Assert.assertArrayEquals(ascii("*2\r\n$1\r\n1\r\n$1\r\n2\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void zrangeAndRangeByScoreStreamPackedZsetThroughFastPipeline() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZADD"), b("z"), b("1"), b("a"), b("1"), b("b"), b("2"), b("c"))));
        Assert.assertArrayEquals(ascii(":3\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZRANGE"), b("z"), b("0"), b("-1"), b("WITHSCORES"))));
        Assert.assertArrayEquals(ascii("*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n1\r\n$1\r\nc\r\n$1\r\n2\r\n"), readOutbound(ch));

        ch.writeInbound(Unpooled.wrappedBuffer(command(b("ZRANGEBYSCORE"), b("z"), b("1"), b("1"), b("WITHSCORES"))));
        Assert.assertArrayEquals(ascii("*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n1\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decoderHandlesPartialFrames() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        byte[] full = ascii("*1\r\n$4\r\nPING\r\n");
        ByteBuf part1 = Unpooled.wrappedBuffer(full, 0, 7); // "*1\r\n$4"
        ByteBuf part2 = Unpooled.wrappedBuffer(full, 7, full.length - 7);

        ch.writeInbound(part1);
        Assert.assertNull(ch.readOutbound());

        ch.writeInbound(part2);
        Assert.assertArrayEquals(ascii("+PONG\r\n"), readOutbound(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void quitReturnsOkAndCloses() {
        YierdisDb db = new YierdisDb();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder(), new YierdisFastCommandHandler(processor));

        ch.writeInbound(Unpooled.wrappedBuffer(ascii("*1\r\n$4\r\nQUIT\r\n")));
        Assert.assertArrayEquals(ascii("+OK\r\n"), readOutbound(ch));

        ch.runPendingTasks();
        Assert.assertFalse(ch.isOpen());

        ch.finishAndReleaseAll();
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
