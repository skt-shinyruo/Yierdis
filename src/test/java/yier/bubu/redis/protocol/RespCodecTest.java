package yier.bubu.redis.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RespCodecTest {
    @Test
    public void decodeBulkStringArrayCommand() {
        String payload = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);
        RespArray arr = (RespArray) msg;
        Assert.assertEquals(3, arr.values().size());
        Assert.assertEquals("SET", ((RespBulkString) arr.values().get(0)).asString());
        Assert.assertEquals("a", ((RespBulkString) arr.values().get(1)).asString());
        Assert.assertEquals("1", ((RespBulkString) arr.values().get(2)).asString());
        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeBinaryBulkString() {
        // *3 [$3 SET] [$1 a] [$4 0x00 0x01 0x02 0x03]
        byte[] payload = new byte[]{
                '*', '3', '\r', '\n',
                '$', '3', '\r', '\n', 'S', 'E', 'T', '\r', '\n',
                '$', '1', '\r', '\n', 'a', '\r', '\n',
                '$', '4', '\r', '\n', 0, 1, 2, 3, '\r', '\n'
        };
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);
        RespArray arr = (RespArray) msg;
        Assert.assertEquals(3, arr.values().size());
        Assert.assertEquals("SET", ((RespBulkString) arr.values().get(0)).asString());
        Assert.assertEquals("a", ((RespBulkString) arr.values().get(1)).asString());
        Assert.assertArrayEquals(new byte[]{0, 1, 2, 3}, ((RespBulkString) arr.values().get(2)).data());
        ch.finishAndReleaseAll();
    }

    @Test
    public void encodeSimpleResponses() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());

        Assert.assertTrue(ch.writeOutbound(RespSimpleString.of("OK")));
        Assert.assertTrue(ch.writeOutbound(RespInteger.of(123)));
        Assert.assertTrue(ch.writeOutbound(RespBulkString.ofString("hi")));

        byte[] ok = readBytes(ch);
        byte[] i = readBytes(ch);
        byte[] bulk = readBytes(ch);

        Assert.assertArrayEquals("+OK\r\n".getBytes(StandardCharsets.UTF_8), ok);
        Assert.assertArrayEquals(":123\r\n".getBytes(StandardCharsets.UTF_8), i);
        Assert.assertArrayEquals("$2\r\nhi\r\n".getBytes(StandardCharsets.UTF_8), bulk);
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
}
