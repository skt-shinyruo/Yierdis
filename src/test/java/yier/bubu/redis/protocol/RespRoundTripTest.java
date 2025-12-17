package yier.bubu.redis.protocol;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RespRoundTripTest {
    @Test
    public void encodeThenDecodeRoundTripNestedArrayAndBinaryBulk() {
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
        RespObject decoded = decodeOne(wire);

        assertRespEquals(original, decoded);
    }

    private static byte[] encode(RespObject obj) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        Assert.assertTrue(ch.writeOutbound(obj));
        byte[] out = readBytes(ch);
        ch.finishAndReleaseAll();
        return out;
    }

    private static RespObject decodeOne(byte[] bytes) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(io.netty.buffer.Unpooled.copiedBuffer(bytes)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof RespObject);
        ch.finishAndReleaseAll();
        return (RespObject) msg;
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

    private static void assertRespEquals(RespObject a, RespObject b) {
        Assert.assertNotNull(a);
        Assert.assertNotNull(b);
        Assert.assertEquals(a.type(), b.type());

        switch (a.type()) {
            case SIMPLE_STRING:
                Assert.assertEquals(((RespSimpleString) a).value(), ((RespSimpleString) b).value());
                return;
            case ERROR:
                Assert.assertEquals(((RespError) a).message(), ((RespError) b).message());
                return;
            case INTEGER:
                Assert.assertEquals(((RespInteger) a).value(), ((RespInteger) b).value());
                return;
            case BULK_STRING: {
                RespBulkString aa = (RespBulkString) a;
                RespBulkString bb = (RespBulkString) b;
                Assert.assertEquals(aa.isNull(), bb.isNull());
                if (!aa.isNull()) {
                    Assert.assertArrayEquals(aa.data(), bb.data());
                }
                return;
            }
            case ARRAY: {
                RespArray aa = (RespArray) a;
                RespArray bb = (RespArray) b;
                Assert.assertEquals(aa.isNull(), bb.isNull());
                if (aa.isNull()) {
                    return;
                }
                Assert.assertEquals(aa.values().size(), bb.values().size());
                for (int i = 0; i < aa.values().size(); i++) {
                    assertRespEquals(aa.values().get(i), bb.values().get(i));
                }
                return;
            }
            case NULL:
            default:
                return;
        }
    }
}
