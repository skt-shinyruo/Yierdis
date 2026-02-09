package yier.bubu.redis.protocol.v1;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class JsonLineReplyWriterTest {
    @Test
    public void simpleStringProducesSuccessEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.simpleString("OK");
        Assert.assertEquals("{\"ok\":true,\"result\":\"OK\"}\n", sink.utf8());
    }

    @Test
    public void errorProducesErrorEnvelope() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.error("ERR wrong");
        Assert.assertEquals("{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":\"ERR wrong\"}}\n", sink.utf8());
    }

    @Test
    public void arrayWithMixedValuesAndErrors() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.arrayHeader(3);
        w.integer(1);
        w.error("ERR nope");
        w.nullValue();
        Assert.assertEquals("{\"ok\":true,\"result\":[1,{\"error\":{\"kind\":\"command\",\"message\":\"ERR nope\"}},null]}\n", sink.utf8());
    }

    @Test
    public void mapUsesStringKeys() {
        ByteArraySink sink = new ByteArraySink();
        JsonLineReplyWriter w = new JsonLineReplyWriter(sink);
        w.mapHeader(2);
        w.bulkString("a".getBytes(StandardCharsets.UTF_8));
        w.integer(1);
        w.bulkString("b".getBytes(StandardCharsets.UTF_8));
        w.bulkString((byte[]) null);
        Assert.assertEquals("{\"ok\":true,\"result\":{\"a\":1,\"b\":null}}\n", sink.utf8());
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

