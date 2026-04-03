package yier.bubu.redis;

import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonBoolean;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonLong;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class YierdisServerBootstrapCommandWiringTest {
    @Test
    public void bootstrapWiresServerAndCoreConnectionCommandsTogether() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--databases", "2")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                JsonObject hello = roundTrip(out, in, "{\"cmd\":\"HELLO\",\"args\":[]}");
                Assert.assertTrue(booleanField(hello, "ok"));
                JsonObject helloResult = objectField(hello, "result");
                Assert.assertEquals("yierdis", stringValue(mapValue(helloResult, "server")));
                Assert.assertEquals(1L, longValue(mapValue(helloResult, "proto")));

                JsonObject info = roundTrip(out, in, "{\"cmd\":\"INFO\",\"args\":[\"yierdis\"]}");
                Assert.assertTrue(booleanField(info, "ok"));
                JsonObject infoResult = objectField(info, "result");
                Assert.assertEquals("yierdis", stringValue(mapValue(infoResult, "server")));
                Assert.assertTrue("expected structured INFO fields", mapContainsKey(infoResult, "executor_policy"));

                JsonObject stats = roundTrip(out, in, "{\"cmd\":\"STATS\",\"args\":[]}");
                Assert.assertTrue(booleanField(stats, "ok"));
                JsonObject statsResult = objectField(stats, "result");
                Assert.assertTrue(mapContainsKey(statsResult, "queued_tasks"));
                Assert.assertTrue(mapContainsKey(statsResult, "commands_executed_total"));

                JsonObject command = roundTrip(out, in, "{\"cmd\":\"COMMAND\",\"args\":[\"INFO\",\"HELLO\",\"INFO\",\"STATS\"]}");
                Assert.assertTrue(booleanField(command, "ok"));
                JsonArray commandResult = arrayField(command, "result");
                Assert.assertEquals(3, commandResult.values().size());
                assertCommandInfo(commandResult.values().get(0), "hello", -1L, 0L, 0L, 0L);
                assertCommandInfo(commandResult.values().get(1), "info", -1L, 0L, 0L, 0L);
                assertCommandInfo(commandResult.values().get(2), "stats", 1L, 0L, 0L, 0L);

                JsonObject select = roundTrip(out, in, "{\"cmd\":\"SELECT\",\"args\":[\"1\"]}");
                Assert.assertTrue(booleanField(select, "ok"));
                Assert.assertEquals("OK", stringField(select, "result"));
            }
        }
    }

    @Test
    public void observabilityUsesNormalizedRuntimeConfigValues() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--databases", "2",
                "--ioThreads", "2",
                "--executorSchedulingPolicy", "GLOBAL",
                "--maxmemoryBytes", "4096",
                "--maxmemoryScope", "perdb",
                "--maxmemoryPolicy", "ALLKEYS-LRU"
        )) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                JsonObject info = roundTrip(out, in, "{\"cmd\":\"INFO\",\"args\":[\"yierdis\"]}");
                Assert.assertTrue(booleanField(info, "ok"));
                JsonObject infoResult = objectField(info, "result");
                Assert.assertEquals(2L, longValue(mapValue(infoResult, "io_threads")));
                Assert.assertEquals("GLOBAL", stringValue(mapValue(infoResult, "executor_policy")));

                JsonObject memory = roundTrip(out, in, "{\"cmd\":\"INFO\",\"args\":[\"memory\"]}");
                Assert.assertTrue(booleanField(memory, "ok"));
                String memorySection = stringField(memory, "result");
                Assert.assertTrue(memorySection.contains("maxmemory_policy:allkeys-lru\r\n"));
                Assert.assertTrue(memorySection.contains("yierdis_maxmemory_scope:per-db\r\n"));
            }
        }
    }

    @Test
    public void channelInitializerUsesRuntimeConfigForSessionAndProtocolLimits() throws Exception {
        try (InitializerTestEnv env = new InitializerTestEnv()) {
            YierdisServerRuntimeConfig commandLimitedConfig = runtimeConfig(1, 0, 3, 2, 4);
            NioSocketChannel commandLimitedChannel = new NioSocketChannel();
            try {
                new YierdisServerChannelInitializer(commandLimitedConfig, env.executor).initChannel(commandLimitedChannel);

                TransactionState tx = ServerConnectionContext.getOrCreate(commandLimitedChannel).session().transaction();
                tx.begin();
                Assert.assertNull(tx.tryEnqueue(argv("SET", "k", "v")));
                Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(argv("GET", "k")));
            } finally {
                commandLimitedChannel.unsafe().closeForcibly();
            }

            YierdisServerRuntimeConfig byteLimitedConfig = runtimeConfig(0, 4, 3, 2, 4);
            NioSocketChannel byteLimitedChannel = new NioSocketChannel();
            try {
                new YierdisServerChannelInitializer(byteLimitedConfig, env.executor).initChannel(byteLimitedChannel);

                TransactionState tx = ServerConnectionContext.getOrCreate(byteLimitedChannel).session().transaction();
                tx.begin();
                Assert.assertNull(tx.tryEnqueue(argv("GET", "k")));
                Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(argv("SET", "x", "y")));

                CustomRequestDecoder decoder = byteLimitedChannel.pipeline().get(CustomRequestDecoder.class);
                Assert.assertNotNull(decoder);
                Assert.assertNotNull(byteLimitedChannel.pipeline().get(ProtocolCommandAdapter.class));
                List<String> pipelineNames = byteLimitedChannel.pipeline().names();
                int backpressureIndex = pipelineNames.indexOf("writeBufferBackpressure");
                int decoderIndex = pipelineNames.indexOf("customRequestDecoder");
                int adapterIndex = pipelineNames.indexOf("protocolCommandAdapter");
                int protocolErrorIndex = pipelineNames.indexOf("protocolErrorReply");
                int commandHandlerIndex = pipelineNames.indexOf("commandHandler");
                Assert.assertTrue(backpressureIndex >= 0);
                Assert.assertTrue(decoderIndex > backpressureIndex);
                Assert.assertTrue(adapterIndex > decoderIndex);
                Assert.assertTrue(protocolErrorIndex > adapterIndex);
                Assert.assertTrue(commandHandlerIndex > protocolErrorIndex);
                Assert.assertEquals(3, intField(decoder, "maxPayloadBytes"));
                Assert.assertEquals(2, intField(decoder, "maxArgs"));
                Assert.assertEquals(4, intField(decoder, "maxHeaderBytes"));
            } finally {
                byteLimitedChannel.unsafe().closeForcibly();
            }
        }
    }

    private static JsonObject roundTrip(OutputStream out, InputStream in, String json) throws IOException {
        writeFrame(out, json);
        return parseJsonObject(readReplyLine(in));
    }

    private static void writeFrame(OutputStream out, String json) throws IOException {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        byte[] head = (Integer.toString(payload.length) + ":").getBytes(StandardCharsets.US_ASCII);
        out.write(head);
        out.write(payload);
        out.write('\n');
        out.flush();
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

    private static JsonObject parseJsonObject(byte[] bytes) {
        Assert.assertNotNull("expected JSON reply", bytes);
        JsonValue v = JsonParser.parseStrictUtf8(bytes, 0, bytes.length, JsonLimits.DEFAULT);
        Assert.assertTrue("expected JSON object", v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static boolean booleanField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected boolean field: " + key, v instanceof JsonBoolean);
        return ((JsonBoolean) v).value();
    }

    private static JsonObject objectField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected object field: " + key, v instanceof JsonObject);
        return (JsonObject) v;
    }

    private static JsonArray arrayField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        Assert.assertTrue("expected array field: " + key, v instanceof JsonArray);
        return (JsonArray) v;
    }

    private static String stringField(JsonObject obj, String key) {
        return stringValue(requiredField(obj, key));
    }

    private static String stringValue(JsonValue v) {
        Assert.assertTrue("expected string value", v instanceof JsonString);
        return ((JsonString) v).value();
    }

    private static long longField(JsonObject obj, String key) {
        JsonValue v = requiredField(obj, key);
        return longValue(v);
    }

    private static long longValue(JsonValue v) {
        Assert.assertTrue("expected integer value", v instanceof JsonLong);
        return ((JsonLong) v).value();
    }

    private static boolean mapContainsKey(JsonObject mapObject, String key) {
        try {
            mapValue(mapObject, key);
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }

    private static JsonValue mapValue(JsonObject mapObject, String key) {
        JsonArray entries = arrayField(mapObject, "$map");
        for (JsonValue entryValue : entries.values()) {
            Assert.assertTrue("expected map entry array", entryValue instanceof JsonArray);
            JsonArray entry = (JsonArray) entryValue;
            Assert.assertEquals("expected [key, value] entry", 2, entry.values().size());
            if (key.equals(stringValue(entry.values().get(0)))) {
                return entry.values().get(1);
            }
        }
        Assert.fail("missing map entry: " + key);
        return null;
    }

    private static JsonValue requiredField(JsonObject obj, String key) {
        Assert.assertNotNull(obj);
        Map<String, JsonValue> map = obj.values();
        JsonValue v = map.get(key);
        Assert.assertNotNull("missing field: " + key, v);
        return v;
    }

    private static void assertCommandInfo(
            JsonValue value,
            String expectedName,
            long expectedArity,
            long expectedFirstKey,
            long expectedLastKey,
            long expectedStep
    ) {
        Assert.assertTrue("expected command info array", value instanceof JsonArray);
        JsonArray info = (JsonArray) value;
        Assert.assertEquals(6, info.values().size());
        Assert.assertEquals(expectedName, stringValue(info.values().get(0)));
        Assert.assertEquals(expectedArity, longValue(info.values().get(1)));
        Assert.assertTrue("expected flags array", info.values().get(2) instanceof JsonArray);
        Assert.assertEquals(expectedFirstKey, longValue(info.values().get(3)));
        Assert.assertEquals(expectedLastKey, longValue(info.values().get(4)));
        Assert.assertEquals(expectedStep, longValue(info.values().get(5)));
    }

    private static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxLineBytes
    ) {
        return new YierdisServerRuntimeConfig(
                0,
                1,
                1000,
                1,
                1024,
                0,
                YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.FAIR,
                256,
                128,
                0,
                0,
                128,
                10,
                transactionQueueMaxCommands,
                transactionQueueMaxBytes,
                protocolMaxBulkBytes,
                protocolMaxArgs,
                protocolMaxLineBytes,
                0,
                YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                YierdisServerRuntimeConfig.MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                0,
                0
        );
    }

    private static byte[][] argv(String... values) {
        byte[][] argv = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            argv[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        return argv;
    }

    private static int intField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static final class InitializerTestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final NettyCommandExecutor executor;

        private InitializerTestEnv() {
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
        }

        @Override
        public void close() {
            try {
                executor.close();
            } finally {
                instance.close();
            }
        }
    }
}
