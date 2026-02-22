package yier.bubu.redis.protocol.json;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class JsonWriterTest {
    @Test
    public void writeStringEscapesNewlines() {
        ByteArraySink sink = new ByteArraySink();
        JsonWriter.writeString(sink, "a\nb\r\t");
        String out = sink.utf8();
        Assert.assertEquals("\"a\\nb\\r\\t\"", out);
        Assert.assertEquals(-1, out.indexOf('\n'));
        Assert.assertEquals(-1, out.indexOf('\r'));
    }

    @Test
    public void writeObjectAndArray() {
        ByteArraySink sink = new ByteArraySink();
        JsonWriter.writeValue(
                sink,
                new JsonObject(Map.of(
                        "ok", new JsonBoolean(true),
                        "result", new JsonArray(List.of(new JsonString("x"), new JsonLong(1)))
                ))
        );
        String out = sink.utf8();
        Assert.assertTrue(out.startsWith("{"));
        Assert.assertTrue(out.endsWith("}"));
        Assert.assertTrue(out.contains("\"ok\":true"));
        Assert.assertTrue(out.contains("\"result\":[\"x\",1]"));
    }

    private static final class ByteArraySink implements BytesSink {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            baos.write(src, srcIndex, len);
        }

        String utf8() {
            return baos.toString(StandardCharsets.UTF_8);
        }
    }
}
