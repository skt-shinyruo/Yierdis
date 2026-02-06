package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class RespDecoderTest {
    @Test
    public void decodeSimpleStringFrame() {
        assertOneFrame("+OK\r\n");
    }

    @Test
    public void decodeErrorFrame() {
        assertOneFrame("-ERR boom\r\n");
    }

    @Test
    public void decodeIntegerFrame() {
        assertOneFrame(":123\r\n");
    }

    @Test
    public void decodeRejectsNonNumericInteger() {
        assertDecoderThrows(":nope\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeRejectsIntegerOverflow() {
        assertDecoderThrows(":9223372036854775808\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeRejectsNonNumericBulkLength() {
        assertDecoderThrows("$x\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeRejectsNonNumericArrayLength() {
        assertDecoderThrows("*x\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeNilBulkStringFrame() {
        assertOneFrame("$-1\r\n");
    }

    @Test
    public void decodeRejectsInvalidNegativeBulkLength() {
        assertDecoderThrows("$-2\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeBinaryBulkStringFrame() {
        // $3 [0x00 0x01 0xFF]
        byte[] payload = new byte[]{
                '$', '3', '\r', '\n',
                0, 1, (byte) 0xFF, '\r', '\n'
        };
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload)));

        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        byte[] decoded = readFrameBytesAndClose(msg);
        Assert.assertArrayEquals(payload, decoded);

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeDoesNotOverflowWhenBulkLengthIsHugeButIncomplete() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder(Integer.MAX_VALUE, 1024, 64, 1024));
        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$2147483647\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());
        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeNullArrayFrame() {
        assertOneFrame("*-1\r\n");
    }

    @Test
    public void decodeResp3NullFrame() {
        assertOneFrame("_\r\n");
    }

    @Test
    public void decodeResp3MapFrame() {
        // %1 [$3 key] [$5 value]
        assertOneFrame("%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    @Test
    public void decodeResp3AttributesWrapperFrame() {
        assertOneFrame("|1\r\n+meta\r\n:123\r\n+OK\r\n");
    }

    @Test
    public void decodeRejectsInvalidNegativeArrayLength() {
        assertDecoderThrows("*-2\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeDoesNotOverflowWhenArrayLengthIsHugeButIncomplete() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder(1024, Integer.MAX_VALUE, 64, 1024));
        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2147483647\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());
        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsBulkStringExceedingMaxBulkBytes() {
        RespDecoder decoder = new RespDecoder(3, 1024, 64, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("$4\r\nabcd\r\n", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("bulk length too large"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeRejectsArrayExceedingMaxArrayLen() {
        RespDecoder decoder = new RespDecoder(1024, 2, 64, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("*3\r\n", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("array length too large"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeRejectsNestedArraysExceedingMaxDepth() {
        RespDecoder decoder = new RespDecoder(1024, 1024, 2, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("*1\r\n*1\r\n*", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("nested arrays too deep"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeRejectsLineExceedingMaxLineBytes() {
        RespDecoder decoder = new RespDecoder(1024, 1024, 64, 4);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            // A line without CRLF that exceeds maxLineBytes + 2 should be rejected.
            ch.writeInbound(Unpooled.copiedBuffer("+aaaaaaa", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("line too long"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeMultipleMessagesFromSingleBuffer() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("+OK\r\n:1\r\n$1\r\na\r\n", StandardCharsets.US_ASCII)));

        Object a = ch.readInbound();
        Object b = ch.readInbound();
        Object c = ch.readInbound();
        Assert.assertNotNull(a);
        Assert.assertNotNull(b);
        Assert.assertNotNull(c);

        Assert.assertArrayEquals(ascii("+OK\r\n"), readFrameBytesAndClose(a));
        Assert.assertArrayEquals(ascii(":1\r\n"), readFrameBytesAndClose(b));
        Assert.assertArrayEquals(ascii("$1\r\na\r\n"), readFrameBytesAndClose(c));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResumesAfterPartialBulkFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$5\r\nhe", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("llo\r\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertArrayEquals(ascii("$5\r\nhello\r\n"), readFrameBytesAndClose(msg));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeWaitsWhenBulkPayloadIsTooShortEvenIfCrlfPresent() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        // Declared len=3 but we only provide 2 bytes + CRLF.
        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$3\r\nab\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeWaitsWhenBulkStringIsMissingLf() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$1\r\na\r", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertArrayEquals(ascii("$1\r\na\r\n"), readFrameBytesAndClose(msg));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResumesAfterPartialArrayFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$3\r\nSET\r\n$1\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("a\r\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertNotNull(msg);
        Assert.assertArrayEquals(ascii("*2\r\n$3\r\nSET\r\n$1\r\na\r\n"), readFrameBytesAndClose(msg));

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsUnknownPrefix() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(new byte[]{'@'}));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause.getMessage().contains("unknown RESP prefix"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeRejectsBadBulkStringCrlf() {
        // Declared len=1, data="a", but uses "\n" instead of "\r\n".
        byte[] payload = new byte[]{
                '$', '1', '\r', '\n',
                'a', '\n', '\n'
        };
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(payload));
            Assert.fail("Expected IllegalArgumentException");
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause.getMessage().contains("bad bulk string CRLF"));
        } finally {
            finishQuietly(ch);
        }
    }

    @Test
    public void decodeArrayOfBulkStringsCommandFrame() {
        // Typical Redis command: *3 [$3 SET] [$1 a] [$1 1]
        assertOneFrame("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
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

    private static void assertDecoderThrows(String payload, Class<? extends Throwable> expected) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII));
            Assert.fail("Expected decode exception: " + expected.getSimpleName());
        } catch (DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue("Expected " + expected.getSimpleName() + " but got " + cause.getClass().getName(),
                    expected.isInstance(cause));
        } finally {
            finishQuietly(ch);
        }
    }

    private static byte[] readFrameBytesAndClose(Object msg) {
        Assert.assertTrue("expected NettyRespFrame", msg instanceof NettyRespFrame);
        NettyRespFrame frame = (NettyRespFrame) msg;
        try {
            ByteBuf buf = frame.unwrap();
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
