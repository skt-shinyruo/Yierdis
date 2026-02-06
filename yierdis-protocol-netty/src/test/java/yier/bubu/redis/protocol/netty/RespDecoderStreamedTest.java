package yier.bubu.redis.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class RespDecoderStreamedTest {
    @Test
    public void decodeStreamedBlobStringFrame() {
        assertOneFrame("$?\r\n;5\r\nhello\r\n;0\r\n");
    }

    @Test
    public void decodeResumesAfterPartialStreamedBlobStringFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$?\r\n;5\r\nhe", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("llo\r\n;0\r\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertArrayEquals(ascii("$?\r\n;5\r\nhello\r\n;0\r\n"), readFrameBytesAndClose(msg));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeStreamedArrayFrame() {
        assertOneFrame("*?\r\n:1\r\n:2\r\n.\r\n");
    }

    @Test
    public void decodeNestedStreamedTypesFrame() {
        assertOneFrame("*?\r\n$?\r\n;3\r\nfoo\r\n;0\r\n.\r\n");
    }

    @Test
    public void decodeAttributesWrappingStreamedValueFrame() {
        assertOneFrame("|1\r\n+meta\r\n:1\r\n$?\r\n;5\r\nhello\r\n;0\r\n");
        assertOneFrame("|0\r\n*?\r\n:1\r\n.\r\n");
    }

    @Test
    public void decodeRejectsStreamedMapMissingValueBeforeEndMarker() {
        assertDecoderThrows("%?\r\n+key\r\n.\r\n", "missing map value");
    }

    @Test
    public void decodeRejectsStreamedBlobStringExceedingMaxBulkBytes() {
        RespDecoder decoder = new RespDecoder(3, 1024, 64, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("$?\r\n;4\r\nabcd\r\n;0\r\n", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("bulk length too large"));
        } finally {
            finishQuietly(ch);
        }
    }

    private static void assertOneFrame(String payload) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        byte[] bytes = ascii(payload);
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(bytes)));

        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertArrayEquals(bytes, readFrameBytesAndClose(msg));
        Assert.assertNull("expected single frame", ch.readInbound());

        ch.finishAndReleaseAll();
    }

    private static void assertDecoderThrows(String payload, String messageContains) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII));
            Assert.fail("Expected decode exception");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains(messageContains));
        } finally {
            finishQuietly(ch);
        }
    }

    private static byte[] readFrameBytesAndClose(Object msg) {
        Assert.assertTrue("expected NettyRespFrame", msg instanceof NettyRespFrame);
        NettyRespFrame frame = (NettyRespFrame) msg;
        try {
            io.netty.buffer.ByteBuf buf = frame.unwrap();
            byte[] out = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), out);
            return out;
        } finally {
            frame.close();
        }
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static Throwable unwrapDecoderCause(DecoderException e) {
        return e.getCause() == null ? e : e.getCause();
    }

    private static void finishQuietly(EmbeddedChannel ch) {
        try {
            ch.finishAndReleaseAll();
        } catch (Exception ignored) {
            // EmbeddedChannel 可能在 close 时重抛 decoder exception
        }
    }
}
