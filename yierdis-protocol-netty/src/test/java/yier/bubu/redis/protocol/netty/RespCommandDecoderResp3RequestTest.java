package yier.bubu.redis.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespCommand;

import java.nio.charset.StandardCharsets;

public class RespCommandDecoderResp3RequestTest {
    @Test
    public void decodesResp3AttributesThenCommandArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("|1\r\n"),
                    ascii("+meta\r\n"),
                    ascii(":123\r\n"),
                    ascii("*1\r\n"),
                    ascii("+PING\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals(ascii("PING"), cmd.toByteArray(0));
            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesChainedResp3AttributesThenCommandArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("|0\r\n"),
                    ascii("|1\r\n"),
                    ascii("+meta\r\n"),
                    ascii("$?\r\n"),
                    ascii(";3\r\n"),
                    ascii("foo\r\n"),
                    ascii(";0\r\n"),
                    ascii("*1\r\n"),
                    ascii("+PING\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals(ascii("PING"), cmd.toByteArray(0));
            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesResp3ScalarArgsInsideCommandArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("*6\r\n"),
                    ascii("+ECHO\r\n"),
                    ascii("_\r\n"),
                    ascii(":42\r\n"),
                    ascii(",3.14\r\n"),
                    ascii("#t\r\n"),
                    verbatim("txt", "hello")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(6, cmd.argc());

            Assert.assertArrayEquals(ascii("ECHO"), cmd.toByteArray(0));

            Assert.assertTrue(cmd.isNull(1));
            Assert.assertNull(cmd.toByteArray(1));

            Assert.assertArrayEquals(ascii("42"), cmd.toByteArray(2));
            Assert.assertArrayEquals(ascii("3.14"), cmd.toByteArray(3));
            Assert.assertArrayEquals(ascii("t"), cmd.toByteArray(4));
            Assert.assertArrayEquals(ascii("hello"), cmd.toByteArray(5));

            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void attributesMapCanContainStreamedAggregateValuesAndStillBeSkipped() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("|1\r\n"),
                    ascii("+meta\r\n"),
                    ascii("*?\r\n"),
                    ascii("+a\r\n"),
                    ascii(".\r\n"),
                    ascii("*1\r\n"),
                    ascii("+PING\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals(ascii("PING"), cmd.toByteArray(0));
            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesStreamedBlobStringArgInsideCommandArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("*2\r\n"),
                    ascii("+ECHO\r\n"),
                    ascii("$?\r\n"),
                    ascii(";5\r\n"),
                    ascii("hello\r\n"),
                    ascii(";0\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(2, cmd.argc());
            Assert.assertArrayEquals(ascii("ECHO"), cmd.toByteArray(0));
            Assert.assertArrayEquals(ascii("hello"), cmd.toByteArray(1));
            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesStreamedCommandArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespCommandDecoder());
        try {
            byte[] req = concat(
                    ascii("*?\r\n"),
                    ascii("+PING\r\n"),
                    ascii(".\r\n")
            );
            ch.writeInbound(Unpooled.wrappedBuffer(req));

            RespCommand cmd = ch.readInbound();
            Assert.assertNotNull(cmd);
            Assert.assertEquals(1, cmd.argc());
            Assert.assertArrayEquals(ascii("PING"), cmd.toByteArray(0));
            cmd.recycle();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] verbatim(String format, String data) {
        if (format == null || format.length() != 3) {
            throw new IllegalArgumentException("format must be 3 chars");
        }
        byte[] payload = ascii(format + ":" + data);
        return concat(
                ascii("=" + payload.length + "\r\n"),
                payload,
                ascii("\r\n")
        );
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
