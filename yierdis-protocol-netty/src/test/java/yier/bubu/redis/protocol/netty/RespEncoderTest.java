package yier.bubu.redis.protocol.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespBigNumber;
import yier.bubu.redis.protocol.RespBlobError;
import yier.bubu.redis.protocol.RespBoolean;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespDouble;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespPush;
import yier.bubu.redis.protocol.RespSet;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespVerbatimString;
import yier.bubu.redis.protocol.RespObjectParser;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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
    public void encodeErrorSanitizesCrLf() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        Assert.assertTrue(ch.writeOutbound(RespError.of("ERR a\r\nb")));

        // Avoid RESP response splitting: CR/LF must be sanitized to spaces.
        Assert.assertArrayEquals(ascii("-ERR a  b\r\n"), readBytes(ch));

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

    @Test
    public void encodeResp3ExtendedTypesThenDecodeAndParse() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        ConnectionContext.getOrCreate(ch).setProtocol(RespProtocol.RESP3);

        RespObject reply = RespArray.of(Arrays.<RespObject>asList(
                RespNull.INSTANCE,
                RespBoolean.of(true),
                RespDouble.of(1.25d),
                RespBigNumber.of("12345678901234567890"),
                RespVerbatimString.ofBytes("txt", "hello".getBytes(StandardCharsets.UTF_8)),
                RespBlobError.ofBytes("ERR boom".getBytes(StandardCharsets.UTF_8)),
                RespSet.of(Arrays.<RespObject>asList(
                        RespBulkString.ofString("a"),
                        RespBulkString.ofString("b")
                )),
                RespMap.of(List.of(
                        new RespMap.Entry(RespBulkString.ofString("k"), RespBulkString.ofString("v"))
                )),
                RespPush.of(Arrays.<RespObject>asList(
                        RespSimpleString.of("msg"),
                        RespBulkString.ofString("x")
                )),
                RespAttribute.of(
                        RespMap.of(List.of(
                                new RespMap.Entry(RespBulkString.ofString("attr"), RespBulkString.ofString("1"))
                        )),
                        RespBulkString.ofString("value")
                )
        ));

        Assert.assertTrue(ch.writeOutbound(reply));
        byte[] wire = readBytes(ch);
        ch.finishAndReleaseAll();

        EmbeddedChannel in = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(in.writeInbound(Unpooled.wrappedBuffer(wire)));
        Object msg = in.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof NettyRespFrame);
        NettyRespFrame frame = (NettyRespFrame) msg;
        try {
            RespObject parsed = RespObjectParser.parse(frame);
            Assert.assertTrue(parsed instanceof RespArray);
            RespArray arr = (RespArray) parsed;
            Assert.assertNotNull(arr.values());
            Assert.assertEquals(10, arr.values().size());

            Assert.assertTrue(arr.values().get(0) instanceof RespNull);
            Assert.assertTrue(arr.values().get(1) instanceof RespBoolean);
            Assert.assertTrue(((RespBoolean) arr.values().get(1)).value());
            Assert.assertTrue(arr.values().get(2) instanceof RespDouble);
            Assert.assertEquals(1.25d, ((RespDouble) arr.values().get(2)).value(), 0.0000001);
            Assert.assertTrue(arr.values().get(3) instanceof RespBigNumber);
            Assert.assertEquals("12345678901234567890", ((RespBigNumber) arr.values().get(3)).value());
            Assert.assertTrue(arr.values().get(4) instanceof RespVerbatimString);
            Assert.assertEquals("txt", ((RespVerbatimString) arr.values().get(4)).format());
            Assert.assertEquals("hello", ((RespVerbatimString) arr.values().get(4)).asString());
            Assert.assertTrue(arr.values().get(5) instanceof RespBlobError);
            Assert.assertEquals("ERR boom", ((RespBlobError) arr.values().get(5)).asString());
            Assert.assertTrue(arr.values().get(6) instanceof RespSet);
            Assert.assertEquals(2, ((RespSet) arr.values().get(6)).values().size());
            Assert.assertTrue(arr.values().get(7) instanceof RespMap);
            Assert.assertEquals(1, ((RespMap) arr.values().get(7)).entries().size());
            Assert.assertTrue(arr.values().get(8) instanceof RespPush);
            Assert.assertEquals(2, ((RespPush) arr.values().get(8)).values().size());
            Assert.assertTrue(arr.values().get(9) instanceof RespAttribute);
            RespAttribute attr = (RespAttribute) arr.values().get(9);
            Assert.assertEquals(1, attr.attributes().entries().size());
        } finally {
            frame.close();
            in.finishAndReleaseAll();
        }
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
