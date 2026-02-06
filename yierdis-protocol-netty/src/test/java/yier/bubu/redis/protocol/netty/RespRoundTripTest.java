package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespBigNumber;
import yier.bubu.redis.protocol.RespBlobError;
import yier.bubu.redis.protocol.RespBoolean;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespDouble;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespPush;
import yier.bubu.redis.protocol.RespSet;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespVerbatimString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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

    @Test
    public void encodeResp3ThenDecodeProducesSingleFrameAndIsParseable() {
        RespObject original = RespArray.of(Arrays.<RespObject>asList(
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

        byte[] wire = encodeResp3(original);
        byte[] decodedFrame = decodeOneFrame(wire);
        Assert.assertArrayEquals(wire, decodedFrame);

        // 语义一致性兜底：decoder 切出的 wire 必须可被 SSOT parser 解析（用于测试/CLI/调试）。
        try (RespFrame frame = new HeapRespFrame(decodedFrame)) {
            RespObject parsed = RespObjectParser.parse(frame);
            Assert.assertTrue(parsed instanceof RespArray);
            RespArray arr = (RespArray) parsed;
            Assert.assertNotNull(arr.values());
            Assert.assertEquals(10, arr.values().size());
        }
    }

    private static byte[] encode(RespObject obj) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        Assert.assertTrue(ch.writeOutbound(obj));
        byte[] out = readBytes(ch);
        ch.finishAndReleaseAll();
        return out;
    }

    private static byte[] encodeResp3(RespObject obj) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        ConnectionContext.getOrCreate(ch).setProtocol(RespProtocol.RESP3);
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

    private static final class HeapRespFrame implements RespFrame {
        private final byte[] data;

        private HeapRespFrame(byte[] data) {
            this.data = data;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void close() {
            // heap frame: no-op
        }
    }
}
