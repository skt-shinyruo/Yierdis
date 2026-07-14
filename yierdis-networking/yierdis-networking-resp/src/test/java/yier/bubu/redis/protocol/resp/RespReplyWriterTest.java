package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void measuredReplyVisitorsReserveShapeWriteElementsAndReleaseDetachedSources() {
        AtomicInteger arrayCloses = new AtomicInteger();
        ByteArraySink arraySink = new ByteArraySink();
        RespReplyWriter arrayWriter = new RespReplyWriter(arraySink, RespProtocolVersion.RESP2);

        arrayWriter.writeMeasuredBulkStringArray(
                2,
                15L,
                0L,
                arrayCloses::incrementAndGet,
                out -> {
                    out.bulkString(bytes("a"));
                    out.bulkString(bytes("bb"));
                }
        );

        Assert.assertEquals("*2\r\n$1\r\na\r\n$2\r\nbb\r\n", arraySink.utf8());
        Assert.assertEquals(1, arrayCloses.get());

        AtomicInteger mapCloses = new AtomicInteger();
        ByteArraySink mapSink = new ByteArraySink();
        RespReplyWriter mapWriter = new RespReplyWriter(mapSink, RespProtocolVersion.RESP2);

        mapWriter.writeMeasuredBulkStringMap(
                1,
                14L,
                0L,
                mapCloses::incrementAndGet,
                out -> {
                    out.bulkString(bytes("f"));
                    out.bulkString(bytes("v"));
                }
        );

        Assert.assertEquals("*2\r\n$1\r\nf\r\n$1\r\nv\r\n", mapSink.utf8());
        Assert.assertEquals(1, mapCloses.get());
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

    private static final class ByteArraySink implements BytesSink {
        private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int off, int len) {
            out.write(src, off, len);
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
