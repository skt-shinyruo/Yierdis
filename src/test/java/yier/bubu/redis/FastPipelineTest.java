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
