package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

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
        Assert.assertEquals("#t\r\n", write3(w -> w.booleanValue(true)));
        Assert.assertEquals(",1.5\r\n", write3(w -> w.doubleValue(1.5)));
        String out = write3(w -> {
            w.mapHeader(1);
            w.bulkString(bytes("proto"));
            w.integer(3);
        });
        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", out);
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
