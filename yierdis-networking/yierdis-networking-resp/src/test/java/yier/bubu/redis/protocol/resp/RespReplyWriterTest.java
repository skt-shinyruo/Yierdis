package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationSink;

import java.nio.charset.StandardCharsets;

public class RespReplyWriterTest {
    @Test
    public void resp2ScalarsAndErrorsUseRedisWireFormat() {
        Assert.assertEquals("+OK\r\n", write2(w -> w.simpleString("OK")));
        Assert.assertEquals("-ERR wrong\r\n", write2(w -> w.error("wrong")));
        Assert.assertEquals("-WRONGTYPE bad\r\n", write2(w -> w.error("WRONGTYPE bad")));
        Assert.assertEquals(":42\r\n", write2(w -> w.integer(42)));
        Assert.assertEquals("$3\r\nabc\r\n", write2(w -> w.bulkString(bytes("abc"))));
        Assert.assertEquals("$-1\r\n", write2(w -> w.nullValue()));
    }

    @Test
    public void resp2MapsAndBooleansDowngradeToCompatibleTypes() {
        String out = write2(w -> {
            w.mapHeader(2);
            w.bulkString(bytes("server"));
            w.bulkString(bytes("yierdis"));
            w.bulkString(bytes("proto"));
            w.integer(2);
        });
        Assert.assertEquals("*4\r\n$6\r\nserver\r\n$7\r\nyierdis\r\n$5\r\nproto\r\n:2\r\n", out);
        Assert.assertEquals(":1\r\n", write2(w -> w.booleanValue(true)));
        Assert.assertEquals(":0\r\n", write2(w -> w.booleanValue(false)));
    }

    @Test
    public void resp3UsesNativeTypes() {
        Assert.assertEquals("_\r\n", write3(RespReplyWriter::nullValue));
        Assert.assertEquals("_\r\n", write3(w -> w.nullArray()));
        Assert.assertEquals("#t\r\n", write3(w -> w.booleanValue(true)));
        Assert.assertEquals(",1.5\r\n", write3(w -> w.doubleValue(1.5)));
        String out = write3(w -> {
            w.mapHeader(1);
            w.bulkString(bytes("proto"));
            w.integer(3);
        });
        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", out);
    }

    @Test
    public void internalErrorUsesGivenMessageWithoutDoublePrefix() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, RespProtocolVersion.RESP2);

        writer.internalError("ERR internal error");

        Assert.assertEquals("-ERR internal error\r\n", sink.utf8());
        Assert.assertFalse(writer.closeAfterReplyRequested());
    }

    @Test
    public void controlErrorUsesTheReservationSinkControlPathBeforeEncoding() {
        ControlTrackingSink sink = new ControlTrackingSink();
        RespReplyWriter writer = new RespReplyWriter(sink, RespProtocolVersion.RESP2);

        writer.controlError("OOM command not allowed when used memory > 'maxmemory'.");

        Assert.assertTrue(sink.controlReservationUsed);
        Assert.assertEquals(
                "-OOM command not allowed when used memory > 'maxmemory'.\r\n",
                sink.utf8()
        );
    }

    @Test
    public void bareRedisErrorPrefixesPassThroughUnchanged() {
        Assert.assertEquals("-ERR\r\n", write2(w -> w.error("ERR")));
        Assert.assertEquals("-NOAUTH\r\n", write2(w -> w.error("NOAUTH")));
        Assert.assertEquals("-WRONGTYPE\r\n", write2(w -> w.error("WRONGTYPE")));
    }

    @Test
    public void protocolErrorRequestsCloseAfterReply() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, RespProtocolVersion.RESP2);

        writer.protocolError("ERR Protocol error");

        Assert.assertEquals("-ERR Protocol error\r\n", sink.utf8());
        Assert.assertTrue(writer.closeAfterReplyRequested());
    }

    @Test
    public void resp3EncodesNonFiniteDoublesNativelyAndResp2DowngradesToBulkStrings() {
        Assert.assertEquals(",nan\r\n", write3(w -> w.doubleValue(Double.NaN)));
        Assert.assertEquals(",inf\r\n", write3(w -> w.doubleValue(Double.POSITIVE_INFINITY)));
        Assert.assertEquals(",-inf\r\n", write3(w -> w.doubleValue(Double.NEGATIVE_INFINITY)));
        Assert.assertEquals("$3\r\nnan\r\n", write2(w -> w.doubleValue(Double.NaN)));
        Assert.assertEquals("$3\r\ninf\r\n", write2(w -> w.doubleValue(Double.POSITIVE_INFINITY)));
        Assert.assertEquals("$4\r\n-inf\r\n", write2(w -> w.doubleValue(Double.NEGATIVE_INFINITY)));
    }

    @Test
    public void bulkStringSliceRejectsNegativeLength() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, RespProtocolVersion.RESP2);

        Assert.assertThrows(IllegalArgumentException.class, () -> writer.bulkString(new BytesSlice() {
            @Override
            public int length() {
                return -1;
            }

            @Override
            public void writeTo(BytesSink out) {
                throw new AssertionError("must not write negative length slice");
            }

            @Override
            public byte getByte(int index) {
                return 0;
            }
        }));
    }

    @Test
    public void nullProtocolDefaultsToResp2AndConstructorsRejectMissingDependencies() {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, (RespProtocolVersion) null);

        writer.nullValue();

        Assert.assertEquals("$-1\r\n", sink.utf8());
        Assert.assertThrows(NullPointerException.class,
                () -> new RespReplyWriter(null, RespProtocolVersion.RESP2));
        Assert.assertThrows(NullPointerException.class,
                () -> new RespReplyWriter(new ByteArraySink(), (java.util.function.IntSupplier) null));
    }

    @Test
    public void bigNumbersVerbatimStringsAndBlobErrorsRespectProtocolVersion() {
        Assert.assertEquals("(12345678901234567890\r\n",
                write3(w -> w.bigNumberAscii(" 12345678901234567890 ")));
        Assert.assertEquals("$3\r\n123\r\n", write2(w -> w.bigNumberAscii(" 123 ")));
        Assert.assertEquals("(a b\r\n", write3(w -> w.bigNumberAscii("a\nb")));
        Assert.assertEquals("(\r\n", write3(w -> w.bigNumberAscii(null)));

        Assert.assertEquals("=8\r\ntxt:data\r\n", write3(w -> w.verbatimString("x", bytes("data"))));
        Assert.assertEquals("=7\r\nmar:doc\r\n", write3(w -> w.verbatimString("markdown", bytes("doc"))));
        Assert.assertEquals("=4\r\ntxt:\r\n", write3(w -> w.verbatimString(null, null)));
        Assert.assertEquals("$4\r\ndata\r\n", write2(w -> w.verbatimString("txt", bytes("data"))));

        Assert.assertEquals("!10\r\nERR \u9519\u8bef\r\n", write3(w -> w.blobError("\u9519\u8bef")));
        Assert.assertEquals("-ERR bad\r\n", write2(w -> w.blobError("bad")));
    }

    @Test
    public void bulkStringOverloadsCoverNullOffsetsSlicesAndLongAscii() {
        Assert.assertEquals("$-1\r\n", write2(w -> w.bulkString((byte[]) null)));
        Assert.assertEquals("$-1\r\n", write2(w -> w.bulkString((byte[]) null, 99, -1)));
        Assert.assertEquals("$-1\r\n", write2(w -> w.bulkString((BytesSlice) null)));
        Assert.assertEquals("$3\r\nbcd\r\n", write2(w -> w.bulkString(bytes("abcde"), 1, 3)));
        Assert.assertThrows(IndexOutOfBoundsException.class,
                () -> write2(w -> w.bulkString(bytes("abc"), 2, 2)));

        BytesSlice slice = new BytesSlice() {
            @Override
            public int length() {
                return 3;
            }

            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(bytes("xyz"), 0, 3);
            }

            @Override
            public byte getByte(int index) {
                return bytes("xyz")[index];
            }
        };
        Assert.assertEquals("$3\r\nxyz\r\n", write2(w -> w.bulkString(slice)));
        Assert.assertEquals("$20\r\n-9223372036854775808\r\n",
                write2(w -> w.bulkStringLongAscii(Long.MIN_VALUE)));
    }

    @Test
    public void collectionHeadersUseNativeResp3TypesAndResp2Fallbacks() {
        Assert.assertEquals("*0\r\n*-1\r\n", write2(w -> {
            w.emptyArray();
            w.nullArray();
        }));
        Assert.assertEquals("*0\r\n*0\r\n*0\r\n*0\r\n", write2(w -> {
            w.arrayHeader(-1);
            w.setHeader(-1);
            w.pushHeader(-1);
            w.attributeHeader(-1);
        }));
        Assert.assertEquals("~2\r\n>3\r\n|4\r\n", write3(w -> {
            w.setHeader(2);
            w.pushHeader(3);
            w.attributeHeader(4);
        }));
    }

    @Test
    public void simpleAndErrorLinesAreSanitizedAndUtf8TruncationKeepsCodePointsWhole() {
        Assert.assertEquals("+\r\n", write2(w -> w.simpleString(null)));
        Assert.assertEquals("+a b c\r\n", write2(w -> w.simpleString("a\rb\nc")));
        Assert.assertEquals("-ERR error\r\n", write2(w -> w.error(null)));
        Assert.assertEquals("-ERR lower case\r\n", write2(w -> w.error("lower\rcase")));

        String retained = "ERR " + "a".repeat(496) + "\u00e9\u754c\ud83d\ude00";
        String oversized = retained + "\ud83d\ude00";
        ByteArraySink sink = new ByteArraySink();
        new RespReplyWriter(sink, RespProtocolVersion.RESP3).blobError(oversized);

        byte[] body = retained.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(509, body.length);
        Assert.assertEquals("!509\r\n" + retained + "\r\n", sink.utf8());
        Assert.assertFalse(sink.utf8().contains("\ufffd"));
    }

    private static String write2(WriterAction action) {
        return write(RespProtocolVersion.RESP2, action);
    }

    private static String write3(WriterAction action) {
        return write(RespProtocolVersion.RESP3, action);
    }

    private static String write(RespProtocolVersion version, WriterAction action) {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, version);
        action.write(writer);
        return sink.utf8();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private interface WriterAction {
        void write(RespReplyWriter writer);
    }

    private static class ByteArraySink implements BytesSink {
        private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int off, int len) {
            out.write(src, off, len);
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    private static final class ControlTrackingSink extends ByteArraySink implements ReplyReservationSink {
        private boolean controlReservationUsed;

        @Override
        public void require(ReplyPlan plan) {
        }

        @Override
        public void useControlReservation() {
            controlReservationUsed = true;
        }

        @Override
        public long writtenBytes() {
            return 0L;
        }
    }

}
