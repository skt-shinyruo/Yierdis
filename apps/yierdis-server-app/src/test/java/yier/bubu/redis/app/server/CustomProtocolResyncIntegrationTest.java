package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;
import yier.bubu.redis.protocol.netty.ProtocolCommandAdapter;
import yier.bubu.redis.protocol.netty.ProtocolErrorReplyHandler;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
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

            JsonObject err = parseJsonObject(awaitOutbound(ch, 1000));
            Assert.assertEquals(false, booleanField(err, "ok"));
            JsonObject errorObj = objectField(err, "error");
            Assert.assertEquals("protocol", stringField(errorObj, "kind"));

            JsonObject pong = parseJsonObject(awaitOutbound(ch, 1000));
            Assert.assertEquals(true, booleanField(pong, "ok"));
            Assert.assertEquals("PONG", stringField(pong, "result"));

            Assert.assertTrue("protocol error should keep the connection open", ch.isActive());
        }
    }

    @Test
    public void malformedFrameStillResyncsAndExecutesNextValidCommandAfterByteBackedParserSwap() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                writeRawFrame(out, "{\"cmd\":}");
                writeFrame(out, "{\"cmd\":\"PING\",\"args\":[]}");

                JsonObject first = parseJsonObject(readReplyLine(in));
                Assert.assertFalse(booleanField(first, "ok"));

                JsonObject second = parseJsonObject(readReplyLine(in));
                Assert.assertTrue(booleanField(second, "ok"));
                Assert.assertEquals("PONG", stringField(second, "result"));
            }
        }
    }

    private static final class TestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final DefaultEventExecutorGroup group;
        private final CommandExecutor<NettyExecutionConnection> executor;
        private final EmbeddedChannel ch;

        private TestEnv() {
            this.instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
            this.group = new DefaultEventExecutorGroup(1);
            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            JsonLineReplyWriterFactory replyWriterFactory = new JsonLineReplyWriterFactory();
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    group.next(),
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 1024, 10, SchedulingPolicy.FAIR)
            );
            executor.start();
            this.ch = new EmbeddedChannel(
                    new CustomRequestDecoder(1024 * 1024, 1024, 256),
                    new ProtocolCommandAdapter(),
                    new ProtocolErrorReplyHandler(replyWriterFactory),
                    new YierdisFastCommandHandler(executor, replyWriterFactory)
            );
            NettyExecutionConnection.getOrCreate(ch, 16, 1024);
        }

        @Override
        public void close() {
            try {
                executor.shutdownGracefully().join();
                executor.executeOwnerTask(instance::close).join();
                group.shutdownGracefully().syncUninterruptibly();
            } finally {
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

    private static byte[] awaitOutbound(EmbeddedChannel ch, long timeoutMillis) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (; ; ) {
            ch.runPendingTasks();
            ch.runScheduledPendingTasks();
            Object out = ch.readOutbound();
            if (out instanceof ByteBuf buf) {
                try {
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    return bytes;
                } finally {
                    buf.release();
                }
            }
            if (System.nanoTime() >= deadline) {
                Assert.fail("timeout waiting for outbound");
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted while waiting for outbound");
            }
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

    private static void writeRawFrame(OutputStream out, String json) throws IOException {
        out.write(frame(json));
        out.flush();
    }

    private static void writeFrame(OutputStream out, String json) throws IOException {
        writeRawFrame(out, json);
    }

    private static byte[] readReplyLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b < 0) {
                if (buf.size() == 0) {
                    return null;
                }
                throw new IOException("unexpected EOF before reply newline");
            }
            if (b == '\n') {
                return buf.toByteArray();
            }
            buf.write(b);
        }
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
