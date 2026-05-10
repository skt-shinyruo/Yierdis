package yier.bubu.redis.app.server;

import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.RespCommandAdapter;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YierdisServerBootstrapCommandWiringTest {
    @Test
    public void bootstrapBindsTransportNeutralExecutorIntoInfoProvider() throws Exception {
        try (YierdisServerBootstrap bootstrap = YierdisServerBootstrap.start("--port", "0")) {
            NettyServerInfoProvider infoProvider = bootstrap.infoProviderForTests();
            Assert.assertNotNull(infoProvider);
            Assert.assertNotNull(infoProvider.boundExecutorForTests());
            Assert.assertEquals("FAIR", infoProvider.boundExecutorForTests().statsSnapshot().schedulingPolicy().name());
        }
    }

    @Test
    public void bootstrapWiresServerAndCoreConnectionCommandsTogether() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--databases", "2")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                Map<String, Object> hello = respMap(roundTrip(out, in, "HELLO"));
                Assert.assertEquals("yierdis", asString(hello.get("server")));
                Assert.assertTrue(hello.containsKey("proto"));

                Map<String, Object> info = respMap(roundTrip(out, in, "INFO", "yierdis"));
                Assert.assertEquals("yierdis", asString(info.get("server")));
                Assert.assertTrue("expected structured INFO fields", info.containsKey("executor_policy"));

                Map<String, Object> stats = respMap(roundTrip(out, in, "STATS"));
                Assert.assertTrue(stats.containsKey("queued_tasks"));
                Assert.assertTrue(stats.containsKey("commands_executed_total"));
                Assert.assertTrue(stats.containsKey("conn_commands_enqueued"));
                Assert.assertTrue(stats.containsKey("conn_commands_executed"));
                Assert.assertTrue(asLong(stats.get("conn_commands_enqueued")) > 0L);
                Assert.assertTrue(asLong(stats.get("conn_commands_executed")) > 0L);
                Assert.assertTrue(
                        "expected STATS to expose non-zero submit totals after accepted commands",
                        asLong(stats.get("submit_accepted_total")) > 0L
                );

                List<Object> command = respArray(roundTrip(out, in, "COMMAND", "INFO", "HELLO", "INFO", "STATS"));
                Assert.assertEquals(3, command.size());
                assertCommandInfo(command.get(0), "hello", -1L, 0L, 0L, 0L);
                assertCommandInfo(command.get(1), "info", -1L, 0L, 0L, 0L);
                assertCommandInfo(command.get(2), "stats", 1L, 0L, 0L, 0L);

                Assert.assertEquals("OK", asString(roundTrip(out, in, "SELECT", "1")));
                Assert.assertEquals("OK", asString(roundTrip(out, in, "SET", "k1", "v1")));

                Assert.assertEquals("OK", asString(roundTrip(out, in, "SELECT", "0")));
                Assert.assertEquals("OK", asString(roundTrip(out, in, "SET", "k0", "v0")));
                Assert.assertEquals(1L, asLong(roundTrip(out, in, "EXPIRE", "k0", "60")));

                String keyspace = asString(roundTrip(out, in, "INFO", "keyspace"));
                Assert.assertTrue(keyspace.contains("db0:keys=1,expires=1\r\n"));
                Assert.assertTrue(keyspace.contains("db1:keys=1,expires=0\r\n"));
            }
        }
    }

    @Test
    public void bootstrapStillProcessesHelloInfoStatsAndDataCommandsAfterByteBackedDecodePath() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--databases", "2")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                Assert.assertTrue(respMap(roundTrip(out, in, "HELLO")).containsKey("server"));
                Assert.assertTrue(respMap(roundTrip(out, in, "INFO", "yierdis")).containsKey("executor_policy"));
                Assert.assertTrue(respMap(roundTrip(out, in, "STATS")).containsKey("queued_tasks"));

                Assert.assertEquals("OK", asString(roundTrip(out, in, "SET", "k", "v")));
                Assert.assertEquals("v", asString(roundTrip(out, in, "GET", "k")));
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

                Map<String, Object> info = respMap(roundTrip(out, in, "INFO", "yierdis"));
                Assert.assertEquals(2L, asLong(info.get("io_threads")));
                Assert.assertEquals("GLOBAL", asString(info.get("executor_policy")));

                String memorySection = asString(roundTrip(out, in, "INFO", "memory"));
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
                new YierdisServerChannelInitializer(commandLimitedConfig, env.executor, env.replyWriterFactory).initChannel(commandLimitedChannel);

                NettyExecutionConnection connection = NettyExecutionConnection.get(commandLimitedChannel);
                Assert.assertNotNull(connection);
                TransactionState tx = connection.session().transaction();
                tx.begin();
                Assert.assertNull(tx.tryEnqueue(request("SET", "k", "v")));
                Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(request("GET", "k")));
            } finally {
                commandLimitedChannel.unsafe().closeForcibly();
            }

            YierdisServerRuntimeConfig byteLimitedConfig = runtimeConfig(0, 4, 3, 2, 4);
            NioSocketChannel byteLimitedChannel = new NioSocketChannel();
            try {
                new YierdisServerChannelInitializer(byteLimitedConfig, env.executor, env.replyWriterFactory).initChannel(byteLimitedChannel);

                NettyExecutionConnection connection = NettyExecutionConnection.get(byteLimitedChannel);
                Assert.assertNotNull(connection);
                TransactionState tx = connection.session().transaction();
                tx.begin();
                Assert.assertNull(tx.tryEnqueue(request("GET", "k")));
                Assert.assertEquals("ERR Transaction queue is full", tx.tryEnqueue(request("SET", "x", "y")));

                RespRequestDecoder decoder = byteLimitedChannel.pipeline().get(RespRequestDecoder.class);
                Assert.assertNotNull(decoder);
                Assert.assertNotNull(byteLimitedChannel.pipeline().get(RespCommandAdapter.class));
                List<String> pipelineNames = byteLimitedChannel.pipeline().names();
                int backpressureIndex = pipelineNames.indexOf("writeBufferBackpressure");
                int decoderIndex = pipelineNames.indexOf("respRequestDecoder");
                int adapterIndex = pipelineNames.indexOf("respCommandAdapter");
                int protocolErrorIndex = pipelineNames.indexOf("respProtocolErrorReply");
                int commandHandlerIndex = pipelineNames.indexOf("commandHandler");
                Assert.assertTrue(backpressureIndex >= 0);
                Assert.assertTrue(decoderIndex > backpressureIndex);
                Assert.assertTrue(adapterIndex > decoderIndex);
                Assert.assertTrue(protocolErrorIndex > adapterIndex);
                Assert.assertTrue(commandHandlerIndex > protocolErrorIndex);
                Assert.assertEquals(3, intField(decoder, "maxBulkBytes"));
                Assert.assertEquals(2, intField(decoder, "maxArgs"));
                Assert.assertEquals(4, intField(decoder, "maxInlineBytes"));
            } finally {
                byteLimitedChannel.unsafe().closeForcibly();
            }
        }
    }

    private static Object roundTrip(OutputStream out, InputStream in, String... args) throws IOException {
        writeCommand(out, args);
        return readResp(in);
    }

    private static void writeCommand(OutputStream out, String... args) throws IOException {
        out.write(('*' + Integer.toString(args.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            out.write(('$' + Integer.toString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(bytes);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        out.flush();
    }

    private static Object readResp(InputStream in) throws IOException {
        int type = in.read();
        if (type < 0) {
            return null;
        }
        return switch (type) {
            case '+' -> readLine(in);
            case '-' -> new RespError(readLine(in));
            case ':' -> Long.parseLong(readLine(in));
            case '$' -> readBulk(in);
            case '*' -> readArray(in);
            case '_' -> {
                expectLineEnd(in);
                yield null;
            }
            default -> throw new IOException("unexpected RESP type: " + (char) type);
        };
    }

    private static String readBulk(InputStream in) throws IOException {
        int len = Integer.parseInt(readLine(in));
        if (len < 0) {
            return null;
        }
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new IOException("unexpected EOF in bulk string");
        }
        expectLineEnd(in);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static List<Object> readArray(InputStream in) throws IOException {
        int len = Integer.parseInt(readLine(in));
        if (len < 0) {
            return null;
        }
        List<Object> values = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            values.add(readResp(in));
        }
        return values;
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before CRLF");
            }
            if (prev == '\r' && b == '\n') {
                byte[] bytes = buf.toByteArray();
                return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
            }
            buf.write(b);
            prev = b;
        }
    }

    private static void expectLineEnd(InputStream in) throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("expected CRLF");
        }
    }

    private static Map<String, Object> respMap(Object value) {
        List<Object> values = respArray(value);
        Assert.assertEquals("expected even RESP2 map array length", 0, values.size() % 2);
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i += 2) {
            map.put(asString(values.get(i)), values.get(i + 1));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> respArray(Object value) {
        Assert.assertTrue("expected RESP array", value instanceof List<?>);
        return (List<Object>) value;
    }

    private static String asString(Object value) {
        Assert.assertTrue("expected string, got " + value, value == null || value instanceof String);
        return (String) value;
    }

    private static long asLong(Object value) {
        Assert.assertTrue("expected integer, got " + value, value instanceof Long);
        return (Long) value;
    }

    private static void assertCommandInfo(
            Object value,
            String expectedName,
            long expectedArity,
            long expectedFirstKey,
            long expectedLastKey,
            long expectedStep
    ) {
        List<Object> info = respArray(value);
        Assert.assertEquals(6, info.size());
        Assert.assertEquals(expectedName, asString(info.get(0)));
        Assert.assertEquals(expectedArity, asLong(info.get(1)));
        Assert.assertTrue("expected flags array", info.get(2) instanceof List<?>);
        Assert.assertEquals(expectedFirstKey, asLong(info.get(3)));
        Assert.assertEquals(expectedLastKey, asLong(info.get(4)));
        Assert.assertEquals(expectedStep, asLong(info.get(5)));
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
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                0,
                0
        );
    }

    private static ByteArrayExecutionRequest request(String... values) {
        return ByteArrayExecutionRequest.fromUtf8(values[0], Arrays.asList(Arrays.copyOfRange(values, 1, values.length)));
    }

    private static int intField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private record RespError(String message) {
    }

    private static final class InitializerTestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final ReplyWriterFactory replyWriterFactory;
        private final CommandExecutor<NettyExecutionConnection> executor;

        private InitializerTestEnv() {
            this.instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
            YierdisEngine engine = TestYierdisEngines.forInstance(instance);
            this.replyWriterFactory = new RespReplyWriterFactory();
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    engine::execute,
                    ImmediateEventExecutor.INSTANCE,
                    replyWriterFactory,
                    new NettyExecutionIoAdapter(),
                    new CommandExecutorConfig(1024, 0, 256, 128, 0, 0, 1024, 10, SchedulingPolicy.FAIR)
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
