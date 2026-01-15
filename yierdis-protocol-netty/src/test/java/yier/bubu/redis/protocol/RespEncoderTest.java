package yier.bubu.redis.protocol;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RespEncoderTest {
    @Test
    public void encodeSimpleTypes() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        Assert.assertTrue(ch.writeOutbound(RespSimpleString.of("OK")));
        Assert.assertTrue(ch.writeOutbound(RespError.of("ERR boom")));
        Assert.assertTrue(ch.writeOutbound(RespInteger.of(-123)));

        Assert.assertArrayEquals(ascii("+OK\r\n"), readBytes(ch));
        Assert.assertArrayEquals(ascii("-ERR boom\r\n"), readBytes(ch));
        Assert.assertArrayEquals(ascii(":-123\r\n"), readBytes(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void encodeNilBulkStringAndNullArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        Assert.assertTrue(ch.writeOutbound(RespBulkString.nullString()));
        Assert.assertTrue(ch.writeOutbound(RespNull.INSTANCE));
        Assert.assertTrue(ch.writeOutbound(RespArray.nullArray()));
        Assert.assertTrue(ch.writeOutbound(RespArray.empty()));

        Assert.assertArrayEquals(ascii("$-1\r\n"), readBytes(ch));
        Assert.assertArrayEquals(ascii("$-1\r\n"), readBytes(ch));
        Assert.assertArrayEquals(ascii("*-1\r\n"), readBytes(ch));
        Assert.assertArrayEquals(ascii("*0\r\n"), readBytes(ch));

        ch.finishAndReleaseAll();
    }

    @Test
    public void encodeBinaryBulkString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        byte[] data = new byte[]{0, 1, (byte) 0xFF};
        Assert.assertTrue(ch.writeOutbound(RespBulkString.ofBytes(data)));

        byte[] out = readBytes(ch);
        byte[] expected = new byte[]{
                '$', '3', '\r', '\n',
                0, 1, (byte) 0xFF, '\r', '\n'
        };
        Assert.assertArrayEquals(expected, out);

        ch.finishAndReleaseAll();
    }

    @Test
    public void encodeArrayAndNestedArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        RespArray inner = RespArray.of(Arrays.<RespObject>asList(
                RespBulkString.ofString("a"),
                RespBulkString.nullString()
        ));
        RespArray outer = RespArray.of(Arrays.<RespObject>asList(
                RespSimpleString.of("OK"),
                RespInteger.of(1),
                inner
        ));
        Assert.assertTrue(ch.writeOutbound(outer));

        byte[] out = readBytes(ch);
        byte[] expected = concat(
                ascii("*3\r\n"),
                ascii("+OK\r\n"),
                ascii(":1\r\n"),
                ascii("*2\r\n"),
                ascii("$1\r\na\r\n"),
                ascii("$-1\r\n")
        );
        Assert.assertArrayEquals(expected, out);

        ch.finishAndReleaseAll();
    }

    private static byte[] readBytes(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull(out);
        Assert.assertTrue(out instanceof io.netty.buffer.ByteBuf);
        io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) out;
        byte[] b = new byte[buf.readableBytes()];
        buf.readBytes(b);
        buf.release();
        return b;
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

