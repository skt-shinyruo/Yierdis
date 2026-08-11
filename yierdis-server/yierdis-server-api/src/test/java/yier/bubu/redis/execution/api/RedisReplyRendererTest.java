package yier.bubu.redis.execution.api;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;

public class RedisReplyRendererTest {
    @Test
    public void rendererWritesEveryScalarInOrder() {
        byte[] bulkData = bytes("value");
        RedisReply bulk = RedisReplies.bulkString(bulkData);
        bulkData[0] = 'X';
        RedisReply reply = RedisReplies.array(List.of(
                RedisReplies.simpleString("OK"),
                RedisReplies.error("bad"),
                RedisReplies.integer(-7),
                RedisReplies.booleanValue(true),
                RedisReplies.doubleValue(1.5D),
                RedisReplies.bigNumber("12345678901234567890"),
                RedisReplies.verbatimString("txt", bytes("body")),
                RedisReplies.blobError("blob"),
                bulk,
                RedisReplies.nullValue(),
                RedisReplies.nullArray()
        ));
        RecordingWriter writer = new RecordingWriter();

        RedisReplyRenderer.render(reply, writer);

        Assert.assertEquals(List.of(
                "array:11",
                "simple:OK",
                "error:bad",
                "integer:-7",
                "boolean:true",
                "double:1.5",
                "big-number:12345678901234567890",
                "verbatim:txt:body",
                "blob-error:blob",
                "bulk:value",
                "null",
                "null-array"
        ), writer.events());
    }

    @Test
    public void rendererWritesAllAggregateHeadersAndChildrenInOrder() {
        RedisReply reply = RedisReplies.array(List.of(
                RedisReplies.map(List.of(RedisReplies.simpleString("key"), RedisReplies.integer(1))),
                RedisReplies.set(List.of(RedisReplies.integer(2), RedisReplies.integer(3))),
                RedisReplies.push(List.of(RedisReplies.simpleString("message"))),
                RedisReplies.attribute(List.of(
                        RedisReplies.simpleString("meta"), RedisReplies.booleanValue(false)))
        ));
        RecordingWriter writer = new RecordingWriter();

        RedisReplyRenderer.render(reply, writer);

        Assert.assertEquals(List.of(
                "array:4",
                "map:1", "simple:key", "integer:1",
                "set:2", "integer:2", "integer:3",
                "push:1", "simple:message",
                "attribute:1", "simple:meta", "boolean:false"
        ), writer.events());
    }

    @Test
    public void rendererWritesStreamingHeadersBeforePayloadEmission() {
        RedisReply sequence = RedisReplies.sequence(
                2,
                17,
                lengths -> {
                    lengths.accept(1);
                    lengths.accept(-1);
                },
                sink -> {
                    sink.bulkString(bytes("a"));
                    sink.bulkStringNull();
                });
        RedisReply map = RedisReplies.byteMap(
                1,
                23,
                lengths -> {
                    lengths.accept(1);
                    lengths.accept(2);
                },
                sink -> {
                    sink.bulkString(bytes("k"));
                    sink.bulkString(bytes("vv"));
                });
        RedisReply set = RedisReplies.byteSet(
                2,
                11,
                lengths -> {
                    lengths.accept(1);
                    lengths.accept(-1);
                },
                sink -> {
                    sink.bulkString(bytes("a"));
                    sink.bulkStringNull();
                });
        RecordingWriter writer = new RecordingWriter();

        RedisReplyRenderer.render(RedisReplies.array(List.of(sequence, map, set)), writer);

        Assert.assertEquals(List.of(
                "array:3",
                "array:2", "bulk:a", "bulk:null",
                "map:1", "bulk:k", "bulk:vv",
                "set:2", "bulk:a", "bulk:null"
        ), writer.events());
    }

    @Test
    public void controlErrorUsesOnlyTheControlWriterPath() {
        RecordingWriter writer = new RecordingWriter();

        RedisReplyRenderer.render(RedisReplies.controlError("failed"), writer);

        Assert.assertEquals(List.of("control-error:failed"), writer.events());
    }

    @Test
    public void rendererRejectsNullInputs() {
        RecordingWriter writer = new RecordingWriter();

        Assert.assertThrows(NullPointerException.class, () -> RedisReplyRenderer.render(null, writer));
        Assert.assertThrows(NullPointerException.class,
                () -> RedisReplyRenderer.render(RedisReplies.nullValue(), null));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingWriter implements RedisReplyWriter {
        private final List<String> events = new ArrayList<>();

        List<String> events() {
            return List.copyOf(events);
        }

        @Override
        public void requestCloseAfterReply() {
            events.add("close");
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return events.contains("close");
        }

        @Override
        public void controlError(String message) {
            events.add("control-error:" + message);
        }

        @Override
        public void simpleString(String value) {
            events.add("simple:" + value);
        }

        @Override
        public void error(String message) {
            events.add("error:" + message);
        }

        @Override
        public void integer(long value) {
            events.add("integer:" + value);
        }

        @Override
        public void booleanValue(boolean value) {
            events.add("boolean:" + value);
        }

        @Override
        public void doubleValue(double value) {
            events.add("double:" + value);
        }

        @Override
        public void bigNumberAscii(String value) {
            events.add("big-number:" + value);
        }

        @Override
        public void verbatimString(String format, byte[] data) {
            events.add("verbatim:" + format + ':' + new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void blobError(String message) {
            events.add("blob-error:" + message);
        }

        @Override
        public void nullValue() {
            events.add("null");
        }

        @Override
        public void nullArray() {
            events.add("null-array");
        }

        @Override
        public void arrayHeader(int count) {
            events.add("array:" + count);
        }

        @Override
        public void emptyArray() {
            events.add("empty-array");
        }

        @Override
        public void mapHeader(int pairs) {
            events.add("map:" + pairs);
        }

        @Override
        public void setHeader(int count) {
            events.add("set:" + count);
        }

        @Override
        public void pushHeader(int count) {
            events.add("push:" + count);
        }

        @Override
        public void attributeHeader(int pairs) {
            events.add("attribute:" + pairs);
        }

        @Override
        public void bulkString(byte[] data) {
            events.add(data == null
                    ? "bulk:null"
                    : "bulk:" + new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            events.add("bulk:" + new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            slice.writeTo(bytes::write);
            events.add("bulk:" + bytes.toString(StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            events.add("bulk:" + value);
        }
    }
}
