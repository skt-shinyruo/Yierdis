package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.Arrays;

public class RespRoundTripTest {
    @Test
    public void encodeThenDecodeProducesSingleFrame() {
        byte[] binary = new byte[]{0, 1, (byte) 0xFF, '\n'};

        RespObject original = RespArray.of(Arrays.<RespObject>asList(
                RespSimpleString.of("OK"),
                RespInteger.of(42),
                RespBulkString.ofBytes(binary),
                RespArray.of(Arrays.<RespObject>asList(
                        RespError.of("ERR boom"),
                        RespBulkString.nullString(),
                        RespArray.empty()
                ))
        ));

        byte[] wire = encode(original);
        byte[] decodedFrame = decodeOneFrame(wire);

        Assert.assertArrayEquals(wire, decodedFrame);
    }

    private static byte[] encode(RespObject obj) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        Assert.assertTrue(ch.writeOutbound(obj));
        byte[] out = readBytes(ch);
        ch.finishAndReleaseAll();
        return out;
    }

    private static byte[] decodeOneFrame(byte[] bytes) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(bytes)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof NettyRespFrame);
        NettyRespFrame frame = (NettyRespFrame) msg;
        try {
            ByteBuf buf = frame.unwrap();
            byte[] out = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), out);
            return out;
        } finally {
            frame.close();
            ch.finishAndReleaseAll();
        }
    }

    private static byte[] readBytes(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull(out);
        Assert.assertTrue(out instanceof ByteBuf);
        ByteBuf buf = (ByteBuf) out;
        byte[] b = new byte[buf.readableBytes()];
        buf.readBytes(b);
        buf.release();
        return b;
    }
}

