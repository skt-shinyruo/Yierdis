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
