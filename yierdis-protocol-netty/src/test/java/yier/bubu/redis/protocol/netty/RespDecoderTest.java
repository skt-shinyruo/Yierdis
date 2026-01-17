package yier.bubu.redis.protocol.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;

public class RespDecoderTest {
    @Test
    public void decodeSimpleString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("+OK\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespSimpleString);
        Assert.assertEquals("OK", ((RespSimpleString) msg).value());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeError() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("-ERR boom\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespError);
        Assert.assertEquals("ERR boom", ((RespError) msg).message());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeInteger() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(":123\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespInteger);
        Assert.assertEquals(123L, ((RespInteger) msg).value());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsNonNumericInteger() {
        assertDecoderThrows(":nope\r\n", NumberFormatException.class);
    }

    @Test
    public void decodeRejectsIntegerOverflow() {
        assertDecoderThrows(":9223372036854775808\r\n", NumberFormatException.class);
    }

    @Test
    public void decodeRejectsNonNumericBulkLength() {
        assertDecoderThrows("$x\r\n", NumberFormatException.class);
    }

    @Test
    public void decodeRejectsNonNumericArrayLength() {
        assertDecoderThrows("*x\r\n", NumberFormatException.class);
    }

    @Test
    public void decodeNilBulkString() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("$-1\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespBulkString);
        Assert.assertTrue(((RespBulkString) msg).isNull());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsInvalidNegativeBulkLength() {
        assertDecoderThrows("$-2\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeBinaryBulkString() {
        // $3 [0x00 0x01 0xFF]
        byte[] payload = new byte[]{
                '$', '3', '\r', '\n',
                0, 1, (byte) 0xFF, '\r', '\n'
        };
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespBulkString);
        Assert.assertArrayEquals(new byte[]{0, 1, (byte) 0xFF}, ((RespBulkString) msg).data());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeDoesNotOverflowWhenBulkLengthIsHugeButIncomplete() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder(Integer.MAX_VALUE, 1024, 64, 1024));
        // The decoder should treat this as an incomplete frame and wait for more bytes,
        // not overflow its length checks or attempt to allocate a huge byte array.
        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$2147483647\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());
        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeNullArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*-1\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);
        Assert.assertTrue(((RespArray) msg).isNull());
        Assert.assertNull(((RespArray) msg).values());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResp3Null() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("_\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespNull);

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResp3Map() {
        // %1 [$3 key] [$5 value]
        String payload = "%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespMap);

        RespMap map = (RespMap) msg;
        Assert.assertEquals(1, map.entries().size());
        Assert.assertEquals("key", ((RespBulkString) map.entries().get(0).key()).asString());
        Assert.assertEquals("value", ((RespBulkString) map.entries().get(0).value()).asString());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsInvalidNegativeArrayLength() {
        assertDecoderThrows("*-2\r\n", IllegalArgumentException.class);
    }

    @Test
    public void decodeDoesNotOverflowWhenArrayLengthIsHugeButIncomplete() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder(1024, Integer.MAX_VALUE, 64, 1024));
        // The decoder should treat this as an incomplete frame and wait for more bytes,
        // not attempt to pre-allocate an enormous ArrayList.
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
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("bulk length too large"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
        }
    }

    @Test
    public void decodeRejectsArrayExceedingMaxArrayLen() {
        RespDecoder decoder = new RespDecoder(1024, 2, 64, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("*3\r\n", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("array length too large"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
        }
    }

    @Test
    public void decodeRejectsNestedArraysExceedingMaxDepth() {
        RespDecoder decoder = new RespDecoder(1024, 1024, 2, 1024);
        EmbeddedChannel ch = new EmbeddedChannel(decoder);
        try {
            ch.writeInbound(Unpooled.copiedBuffer("*1\r\n*1\r\n*", StandardCharsets.US_ASCII));
            Assert.fail("Expected IllegalArgumentException");
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("nested arrays too deep"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
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
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue(cause instanceof IllegalArgumentException);
            Assert.assertTrue(cause.getMessage().contains("line too long"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
        }
    }

    @Test
    public void decodeEmptyArray() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("*0\r\n", StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);
        Assert.assertFalse(((RespArray) msg).isNull());
        Assert.assertTrue(((RespArray) msg).values().isEmpty());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeNestedArray() {
        // *2 [*2 [+OK :1] [$3 foo]]
        String payload = "*2\r\n*2\r\n+OK\r\n:1\r\n$3\r\nfoo\r\n";
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);

        RespArray outer = (RespArray) msg;
        Assert.assertEquals(2, outer.values().size());
        Assert.assertTrue(outer.values().get(0) instanceof RespArray);
        RespArray inner = (RespArray) outer.values().get(0);
        Assert.assertEquals(2, inner.values().size());
        Assert.assertEquals("OK", ((RespSimpleString) inner.values().get(0)).value());
        Assert.assertEquals(1L, ((RespInteger) inner.values().get(1)).value());
        Assert.assertEquals("foo", ((RespBulkString) outer.values().get(1)).asString());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeMultipleMessagesFromSingleBuffer() {
        String payload = "+OK\r\n:1\r\n$1\r\na\r\n";
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));

        Object a = ch.readInbound();
        Object b = ch.readInbound();
        Object c = ch.readInbound();
        Assert.assertTrue(a instanceof RespSimpleString);
        Assert.assertTrue(b instanceof RespInteger);
        Assert.assertTrue(c instanceof RespBulkString);

        Assert.assertEquals("OK", ((RespSimpleString) a).value());
        Assert.assertEquals(1L, ((RespInteger) b).value());
        Assert.assertEquals("a", ((RespBulkString) c).asString());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResumesAfterPartialFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("$5\r\nhe", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("llo\r\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespBulkString);
        Assert.assertEquals("hello", ((RespBulkString) msg).asString());

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
        Assert.assertTrue(msg instanceof RespBulkString);
        Assert.assertEquals("a", ((RespBulkString) msg).asString());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeResumesAfterPartialArrayFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());

        Assert.assertFalse(ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$3\r\nSET\r\n$1\r\n", StandardCharsets.US_ASCII)));
        Assert.assertNull(ch.readInbound());

        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer("a\r\n", StandardCharsets.US_ASCII)));
        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);

        RespArray arr = (RespArray) msg;
        Assert.assertEquals(2, arr.values().size());
        Assert.assertEquals("SET", ((RespBulkString) arr.values().get(0)).asString());
        Assert.assertEquals("a", ((RespBulkString) arr.values().get(1)).asString());

        ch.finishAndReleaseAll();
    }

    @Test
    public void decodeRejectsUnknownPrefix() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(new byte[]{','}));
            Assert.fail("Expected IllegalArgumentException");
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            Assert.assertTrue(cause.getMessage().contains("unknown RESP prefix"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
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
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            Assert.assertTrue(cause.getMessage().contains("bad bulk string CRLF"));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
        }
    }

    @Test
    public void decodeArrayOfBulkStringsCommand() {
        // Typical Redis command: *3 [$3 SET] [$1 a] [$1 1]
        String payload = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        Assert.assertTrue(ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII)));

        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespArray);
        RespArray arr = (RespArray) msg;
        Assert.assertEquals(3, arr.values().size());
        Assert.assertEquals("SET", ((RespBulkString) arr.values().get(0)).asString());
        Assert.assertEquals("a", ((RespBulkString) arr.values().get(1)).asString());
        Assert.assertEquals("1", ((RespBulkString) arr.values().get(2)).asString());

        ch.finishAndReleaseAll();
    }

    private static void assertDecoderThrows(String payload, Class<? extends Throwable> expected) {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        try {
            ch.writeInbound(Unpooled.copiedBuffer(payload, StandardCharsets.US_ASCII));
            Assert.fail("Expected decode exception: " + expected.getSimpleName());
        } catch (io.netty.handler.codec.DecoderException e) {
            Throwable cause = unwrapDecoderCause(e);
            Assert.assertTrue("Expected " + expected.getSimpleName() + " but got " + cause.getClass().getName(),
                    expected.isInstance(cause));
        } finally {
            try {
                ch.finishAndReleaseAll();
            } catch (Exception ignored) {
                // EmbeddedChannel may rethrow stored decoder exceptions on finish/close.
            }
        }
    }

    private static Throwable unwrapDecoderCause(io.netty.handler.codec.DecoderException e) {
        if (e.getCause() == null) {
            return e;
        }
        return e.getCause();
    }
}
