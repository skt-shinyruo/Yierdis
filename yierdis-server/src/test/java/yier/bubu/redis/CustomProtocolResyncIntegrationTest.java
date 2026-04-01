package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomProtocolResyncIntegrationTest {
    @Test
    public void invalidFrameReturnsProtocolErrorAndNextFrameStillExecutes() {
        try (TestEnv env = new TestEnv()) {
            EmbeddedChannel ch = env.ch;

            byte[] invalid = ascii("x\n");
            byte[] ping = frame("{\"cmd\":\"PING\",\"args\":[]}");
            ch.writeInbound(Unpooled.wrappedBuffer(concat(invalid, ping)));

            JsonObject err = parseJsonObject(readOutbound(ch));
            Assert.assertEquals(false, booleanField(err, "ok"));
            JsonObject errorObj = objectField(err, "error");
            Assert.assertEquals("protocol", stringField(errorObj, "kind"));

            JsonObject pong = parseJsonObject(readOutbound(ch));
            Assert.assertEquals(true, booleanField(pong, "ok"));
            Assert.assertEquals("PONG", stringField(pong, "result"));

            Assert.assertTrue("protocol error should keep the connection open", ch.isActive());
        }
    }

    private static final class TestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final NettyCommandExecutor executor;
        private final EmbeddedChannel ch;

        private TestEnv() {
            this.instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            this.executor = new NettyCommandExecutor(
                    instance::bindToCurrentThread,
                    processor,
                    ImmediateEventExecutor.INSTANCE,
                    new JsonLineReplyWriterFactory(),
                    1024,
                    0,
                    256,
                    128,
                    0,
                    0,
                    1024,
                    10,
                    SchedulingPolicy.FAIR
            );
            executor.start();
            this.ch = new EmbeddedChannel(
                    new CustomRequestDecoder(1024 * 1024, 1024, 256),
                    new ProtocolCommandAdapter(),
                    new ProtocolErrorReplyHandler(executor),
                    new YierdisFastCommandHandler(executor)
            );
        }

        @Override
        public void close() {
            try {
                executor.close();
            } finally {
                instance.close();
                ch.finishAndReleaseAll();
            }
        }
    }

    private static JsonObject parseJsonObject(byte[] bytes) {
        JsonValue v = JsonParser.parseStrictUtf8(bytes, 0, bytes.length, JsonLimits.DEFAULT);
        Assert.assertTrue("expected JSON object", v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static byte[] readOutbound(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        Assert.assertNotNull("expected reply", out);
        Assert.assertTrue(out instanceof ByteBuf);
        ByteBuf buf = (ByteBuf) out;
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
    }

    private static boolean booleanField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertNotNull("missing field: " + key, v);
        if (v instanceof yier.bubu.redis.protocol.json.JsonBoolean b) {
            return b.value();
        }
        Assert.fail("expected boolean field: " + key);
        return false;
    }

    private static JsonObject objectField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertTrue("expected object field: " + key, v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static String stringField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertTrue("expected string field: " + key, v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static byte[] frame(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        String head = Integer.toString(payload.length) + ":";
        byte[] h = head.getBytes(StandardCharsets.US_ASCII);
        return concat(h, payload, new byte[]{'\n'});
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p == null ? 0 : p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            if (p == null || p.length == 0) {
                continue;
            }
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
