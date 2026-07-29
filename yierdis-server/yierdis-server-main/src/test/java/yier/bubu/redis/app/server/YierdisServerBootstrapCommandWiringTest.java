package yier.bubu.redis.app.server;

import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.execution.executor.CommandExecutionEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.InboundByteAccountingHandler;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.kernel.CommandDispatcher;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YierdisServerBootstrapCommandWiringTest {
    @Test
    public void bootstrapBindsTransportNeutralExecutorIntoInfoProvider() throws Exception {
        try (YierdisServerBootstrap bootstrap = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0"
        )) {
            NettyServerInfoProvider infoProvider = bootstrap.infoProviderForTests();
            Assert.assertNotNull(infoProvider);
            Assert.assertNotNull(infoProvider.boundExecutorForTests());
            Assert.assertEquals("FAIR", infoProvider.boundExecutorForTests().statsSnapshot().schedulingPolicy().name());
        }
    }

    @Test
    public void bootstrapWiresServerAndCoreConnectionCommandsTogether() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--databases", "2"
        )) {
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
                Assert.assertTrue("expected INFO to expose ingress capacity", info.containsKey("inbound_capacity_bytes"));
                Assert.assertTrue("expected INFO to expose reply capacity", info.containsKey("reply_global_capacity_bytes"));
                Assert.assertTrue("expected INFO to expose reply drain timeout", info.containsKey("reply_drain_timeout_millis"));
                Assert.assertTrue("expected INFO to expose outbound reservations", info.containsKey("outbound_reserved_bytes"));
                Assert.assertEquals("DISABLED", asString(info.get("commit_stream_state")));
                Assert.assertTrue(info.containsKey("commit_stream_reserved_events"));
                Assert.assertTrue(info.containsKey("commit_stream_first_failure_type"));

                Map<String, Object> stats = respMap(roundTrip(out, in, "STATS"));
                Assert.assertTrue(stats.containsKey("queued_tasks"));
                Assert.assertTrue(stats.containsKey("commands_executed_total"));
                Assert.assertTrue(stats.containsKey("conn_commands_enqueued"));
                Assert.assertTrue(stats.containsKey("conn_commands_executed"));
                Assert.assertTrue(stats.containsKey("inbound_capacity_bytes"));
                Assert.assertTrue(stats.containsKey("inbound_reserved_bytes"));
                Assert.assertTrue(stats.containsKey("inbound_waiting_connections"));
                Assert.assertTrue(stats.containsKey("outbound_active_slots"));
                Assert.assertTrue(stats.containsKey("outbound_active_chunks"));
                Assert.assertTrue(stats.containsKey("live_child_channels"));
                Assert.assertTrue(stats.containsKey("reply_shutdown_timeouts"));
                Assert.assertEquals("DISABLED", asString(stats.get("commit_stream_state")));
                Assert.assertTrue(stats.containsKey("commit_stream_reserved_events"));
                Assert.assertTrue(stats.containsKey("commit_stream_last_acknowledged_sequence"));
                Assert.assertTrue(stats.containsKey("commit_stream_first_failure_message"));
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
    public void serverCommandCompositionBuildsDispatcherWithServerAndDefaultCommands() throws Exception {
        try (YierdisInstance instance = TestYierdisInstances.createWithDefaultMemory(
                YierdisInstanceConfig.builder().databases(1).build()
        )) {
            instance.bindToCurrentThread();
            NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(
                    runtimeConfig(0, 0, 1024, 1, 4, 5)
            );
            CommandDispatcher dispatcher = ServerCommandComposition.createDispatcher(
                    TestDbRouters.forInstance(instance),
                    infoProvider,
                    SlowCommandGovernor.DEFAULT
            );
            CommandExecutionEngine execution = dispatcher::prepare;
            EngineSession session = new EngineSession();
            PreparedCommand prepared = execution.prepare(session, request("PING"));
            Assert.assertNotNull(prepared);
        }
    }

    @Test
    public void bootstrapSourceWiresDispatcherAndMaintenanceDirectly() throws Exception {
        String source = Files.readString(bootstrapSource(), StandardCharsets.UTF_8);

        Assert.assertTrue(source.contains("dispatcher::prepare"));
        Assert.assertTrue(source.contains("maintenanceTick.run()"));
        Assert.assertFalse(source.contains("Yierdis" + "Engine"));
        Assert.assertFalse(source.contains("DefaultYierdis" + "Engine"));
        Assert.assertFalse(source.contains("YierdisFastCommand" + "Processor"));
    }

    @Test
    public void serverCommandSourcesUseSemanticRepliesWithoutWriterBridges() throws Exception {
        for (Path sourcePath : List.of(serverSource("ServerCommandModule.java"),
                serverSource("NettyServerInfoProvider.java"))) {
            String source = Files.readString(sourcePath, StandardCharsets.UTF_8);
            Assert.assertFalse(source.contains("CommandPreparationContext"));
            Assert.assertFalse(source.contains("RedisReplyWriter"));
            Assert.assertFalse(source.contains("CommandExecutionContext"));
            Assert.assertFalse(source.contains("ReplyShapes.maximum()"));
            Assert.assertFalse(source.contains("maximumReply("));
        }
    }

    @Test
    public void unboundInfoProviderReturnsSemanticNotReadyErrors() {
        NettyServerInfoProvider provider = new NettyServerInfoProvider(runtimeConfig(0, 0, 1024, 1, 4, 5));
        EngineSession session = new EngineSession();

        RedisReply info = provider.info(CommandArgs.of(request("INFO")), session);
        RedisReply stats = provider.stats(session);

        Assert.assertEquals("ERR INFO not ready", ((RedisReply.Error) info).message());
        Assert.assertEquals("ERR STATS not ready", ((RedisReply.Error) stats).message());
    }

    @Test
    public void bootstrapStillProcessesHelloInfoStatsAndDataCommandsAfterByteBackedDecodePath() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--databases", "2"
        )) {
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
                Assert.assertTrue(memorySection.contains("yierdis_native_defrag_last_scanned_objects:"));
                Assert.assertTrue(memorySection.contains("yierdis_native_defrag_moved_bytes:"));
                Assert.assertTrue(memorySection.contains("yierdis_native_stale_handle_detections:"));
                Assert.assertTrue(memorySection.contains("yierdis_expired_entries_awaiting_physical_deletion:"));
            }
        }
    }

    @Test
    public void infoVariantsCoverDefaultKnownAndUnknownSections() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0"
        )) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                String defaultInfo = asString(roundTrip(out, in, "INFO"));
                Assert.assertTrue(defaultInfo.contains("redis_version:"));

                Map<String, Object> yierdis = respMap(roundTrip(out, in, "INFO", "yierdis"));
                Assert.assertTrue(yierdis.containsKey("executor_policy"));

                String memory = asString(roundTrip(out, in, "INFO", "memory"));
                Assert.assertTrue(memory.contains("used_memory:"));

                String keyspace = asString(roundTrip(out, in, "INFO", "keyspace"));
                Assert.assertTrue(keyspace.contains("# Keyspace"));

                String unknown = asString(roundTrip(out, in, "INFO", "unknown-section"));
                Assert.assertEquals("", unknown);
            }
        }
    }

    @Test
    public void structuredInfoAndStatsPreflightBeyondTheControlReservation() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--replyControlReservationBytes", "1539"
        )) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                Map<String, Object> info = respMap(roundTrip(out, in, "INFO", "yierdis"));
                Assert.assertEquals("yierdis", asString(info.get("server")));

                Map<String, Object> stats = respMap(roundTrip(out, in, "STATS"));
                Assert.assertTrue(stats.containsKey("outbound_reserved_bytes"));
            }
        }
    }

    @Test
    public void metadataAndSessionRepliesPreflightBeyondTheControlReservation() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--replyControlReservationBytes", "1539"
        )) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                Map<String, Object> memoryStats = respMap(roundTrip(out, in, "MEMORY", "STATS"));
                Assert.assertTrue(memoryStats.containsKey("maxmemory_bytes"));

                List<Object> commands = respArray(roundTrip(out, in, "COMMAND"));
                Assert.assertFalse(commands.isEmpty());

                String name = "n".repeat(1024);
                Assert.assertEquals("OK", asString(roundTrip(out, in, "CLIENT", "SETNAME", name)));
                Assert.assertEquals(name, asString(roundTrip(out, in, "CLIENT", "GETNAME")));
            }
        }
    }

    @Test
    public void uncountedListPopPreflightsThePoppedValueBeforeMutation() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--replyControlReservationBytes", "1539"
        )) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                socket.setSoTimeout(2000);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                String value = "v".repeat(2048);

                Assert.assertEquals(1L, asLong(roundTrip(out, in, "LPUSH", "pop:preflight", value)));
                Assert.assertEquals(value, asString(roundTrip(out, in, "LPOP", "pop:preflight")));
                Assert.assertNull(roundTrip(out, in, "LPOP", "pop:preflight"));
            }
        }
    }

    @Test
    public void channelInitializerUsesRuntimeConfigForSessionAndProtocolLimits() throws Exception {
        try (InitializerTestEnv env = new InitializerTestEnv()) {
            YierdisServerRuntimeConfig commandLimitedConfig = runtimeConfig(1, 0, 3, 2, 4, 5);
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

            YierdisServerRuntimeConfig byteLimitedConfig = runtimeConfig(0, 4, 3, 2, 4, 5);
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
                Assert.assertNotNull(byteLimitedChannel.pipeline().get(InboundReadCreditHandler.class));
                Assert.assertNotNull(byteLimitedChannel.pipeline().get(InboundByteAccountingHandler.class));
                List<String> pipelineNames = byteLimitedChannel.pipeline().names();
                Object backpressureHandler = byteLimitedChannel.pipeline().get("writeBufferBackpressure");
                int backpressureIndex = pipelineNames.indexOf("writeBufferBackpressure");
                int idleTimeoutIndex = pipelineNames.indexOf("idleTimeout");
                int idleTimeoutCloserIndex = pipelineNames.indexOf("idleTimeoutCloser");
                int readCreditIndex = pipelineNames.indexOf("inboundReadCredit");
                int byteAccountingIndex = pipelineNames.indexOf("inboundByteAccounting");
                int decoderIndex = pipelineNames.indexOf("respRequestDecoder");
                int ingressIndex = pipelineNames.indexOf("executionRequestIngress");
                Assert.assertNotNull(backpressureHandler);
                Assert.assertTrue(backpressureIndex >= 0);
                Assert.assertTrue(idleTimeoutIndex > backpressureIndex);
                Assert.assertTrue(idleTimeoutCloserIndex > idleTimeoutIndex);
                Assert.assertTrue(decoderIndex > backpressureIndex);
                Assert.assertTrue(decoderIndex > idleTimeoutCloserIndex);
                Assert.assertTrue(readCreditIndex > idleTimeoutCloserIndex);
                Assert.assertTrue(byteAccountingIndex > readCreditIndex);
                Assert.assertTrue(decoderIndex > byteAccountingIndex);
                Assert.assertTrue(ingressIndex > decoderIndex);
                Assert.assertEquals(3, intField(decoder, "maxBulkBytes"));
                Assert.assertEquals(2, intField(decoder, "maxArgs"));
                Assert.assertEquals(4, intField(decoder, "maxInlineBytes"));
                Assert.assertEquals(5, intField(decoder, "maxCommandBytes"));
                Assert.assertEquals(10000L, longField(backpressureHandler, "outputBufferOverLimitMillis"));
                WriteBufferWaterMark waterMark = byteLimitedChannel.config().getWriteBufferWaterMark();
                Assert.assertEquals(33554432, waterMark.low());
                Assert.assertEquals(67108864, waterMark.high());
            } finally {
                byteLimitedChannel.unsafe().closeForcibly();
            }
        }
    }

    @Test
    public void channelInitializerRegistersChildrenBeforeBuildingTheCommandPipeline() throws Exception {
        try (InitializerTestEnv env = new InitializerTestEnv()) {
            YierdisServerRuntimeConfig config = runtimeConfig(0, 0, 1024, 16, 128, 1024);
            ChildChannelRegistry acceptingRegistry = new ChildChannelRegistry();
            NioSocketChannel accepted = new NioSocketChannel();
            try {
                new YierdisServerChannelInitializer(
                        config,
                        env.executor,
                        env.replyWriterFactory,
                        new InboundMemoryBudget(config.protocolGlobalInFlightBytes()),
                        new OutboundMemoryBudget(config.replyGlobalCapacityBytes()),
                        acceptingRegistry
                ).initChannel(accepted);

                Assert.assertEquals(1, acceptingRegistry.activeChannelCount());
                Assert.assertNotNull(accepted.pipeline().get("executionRequestIngress"));
            } finally {
                accepted.unsafe().closeForcibly();
            }

            ChildChannelRegistry closingRegistry = new ChildChannelRegistry();
            closingRegistry.beginShutdown();
            NioSocketChannel rejected = new NioSocketChannel();
            try {
                new YierdisServerChannelInitializer(
                        config,
                        env.executor,
                        env.replyWriterFactory,
                        new InboundMemoryBudget(config.protocolGlobalInFlightBytes()),
                        new OutboundMemoryBudget(config.replyGlobalCapacityBytes()),
                        closingRegistry
                ).initChannel(rejected);

                Assert.assertNull(NettyExecutionConnection.get(rejected));
                Assert.assertNull(rejected.pipeline().get("executionRequestIngress"));
                Assert.assertEquals(0, closingRegistry.activeChannelCount());
            } finally {
                rejected.unsafe().closeForcibly();
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
            String key = asString(values.get(i));
            Assert.assertFalse("duplicate RESP map key: " + key, map.containsKey(key));
            map.put(key, values.get(i + 1));
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
            int protocolMaxLineBytes,
            int protocolMaxCommandBytes
    ) {
        return new YierdisServerRuntimeConfig(
                "127.0.0.1",
                0,
                1024,
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
                protocolMaxCommandBytes,
                300000,
                67108864,
                10000,
                256L * 1024L * 1024L,
                128L * 1024L * 1024L,
                64L * 1024L * 1024L,
                64 * 1024,
                4L * 1024L,
                5_000L,
                0,
                YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                false,
                65536,
                64,
                1,
                0,
                0,
                0,
                128L * 1024L * 1024L
        );
    }

    private static ByteArrayExecutionRequest request(String... values) {
        return ByteArrayExecutionRequest.fromUtf8(values[0], Arrays.asList(Arrays.copyOfRange(values, 1, values.length)));
    }

    private static Path bootstrapSource() {
        Path moduleRoot = Path.of("").toAbsolutePath().normalize();
        Path fromModule = moduleRoot.resolve(
                "src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java"
        );
        if (Files.isRegularFile(fromModule)) {
            return fromModule;
        }
        Path fromRepo = moduleRoot.resolve(
                "yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java"
        );
        Assert.assertTrue("cannot locate YierdisServerBootstrap.java from " + moduleRoot, Files.isRegularFile(fromRepo));
        return fromRepo;
    }

    private static Path serverSource(String fileName) {
        Path moduleRoot = Path.of("").toAbsolutePath().normalize();
        Path fromModule = moduleRoot.resolve("src/main/java/yier/bubu/redis/app/server").resolve(fileName);
        if (Files.isRegularFile(fromModule)) {
            return fromModule;
        }
        Path fromRepo = moduleRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server")
                .resolve(fileName);
        Assert.assertTrue("cannot locate " + fileName + " from " + moduleRoot, Files.isRegularFile(fromRepo));
        return fromRepo;
    }

    private static void execute(
            CommandDispatcher dispatcher,
            CommandSession session,
            ExecutionRequest request,
            RedisReplyWriter reply
    ) {
        try (PreparedCommand prepared = dispatcher.prepare(session, request)) {
            Assert.assertEquals(ValidationResult.VALID, prepared.validateBeforeExecute());
            try (CommandExecutionContext context = CommandExecutionContext.forRequest(session, reply, request)) {
                prepared.execute(context);
            }
        }
    }

    private static int intField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static long longField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private record RespError(String message) {
    }

    private static final class CapturingReplyWriter implements RedisReplyWriter {
        private String simpleStringValue;
        private Integer mapHeaderCount;
        private List<Object> arrayValues;
        private List<Object> activeAggregate;

        @Override
        public void requestCloseAfterReply() {
        }

        @Override
        public boolean closeAfterReplyRequested() {
            return false;
        }

        @Override
        public void simpleString(String value) {
            this.simpleStringValue = value;
            addValue(value);
        }

        @Override
        public void mapHeader(int count) {
            this.mapHeaderCount = count;
        }

        @Override
        public void error(String message) {
            throw new AssertionError(message);
        }

        @Override
        public void integer(long value) {
            addValue(value);
        }

        @Override
        public void booleanValue(boolean value) {
        }

        @Override
        public void doubleValue(double value) {
        }

        @Override
        public void bigNumberAscii(String value) {
        }

        @Override
        public void verbatimString(String format, byte[] data) {
        }

        @Override
        public void blobError(String message) {
            throw new AssertionError(message);
        }

        @Override
        public void nullValue() {
            addValue(null);
        }

        @Override
        public void nullArray() {
        }

        @Override
        public void arrayHeader(int count) {
            List<Object> values = new ArrayList<>(count);
            this.arrayValues = values;
            this.activeAggregate = values;
        }

        @Override
        public void emptyArray() {
        }

        @Override
        public void setHeader(int count) {
        }

        @Override
        public void pushHeader(int count) {
        }

        @Override
        public void attributeHeader(int pairs) {
        }

        @Override
        public void bulkString(byte[] data) {
            addValue(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            addValue(new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            slice.writeTo(bytes::write);
            addValue(bytes.toString(StandardCharsets.UTF_8));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            addValue(Long.toString(value));
        }

        private void addValue(Object value) {
            if (activeAggregate != null) {
                activeAggregate.add(value);
            }
        }
    }

    private static final class EngineSession implements CommandSession {
        private final TransactionState tx = new TransactionState() {
            private final List<ExecutionRequest> queue = new ArrayList<>();
            private boolean active;
            private boolean aborted;

            @Override
            public synchronized boolean active() {
                return active;
            }

            @Override
            public synchronized boolean aborted() {
                return aborted;
            }

            @Override
            public synchronized void begin() {
                discard();
                active = true;
            }

            @Override
            public synchronized void markAborted() {
                aborted = true;
            }

            @Override
            public synchronized void discard() {
                for (ExecutionRequest request : queue) {
                    request.close();
                }
                queue.clear();
                active = false;
                aborted = false;
            }

            @Override
            public synchronized String tryEnqueue(ExecutionRequest request) {
                if (request != null) {
                    queue.add(ByteArrayExecutionRequest.copyOf(request));
                }
                return null;
            }

            @Override
            public synchronized int size() {
                return queue.size();
            }

            @Override
            public synchronized void forEachQueued(
                    java.util.function.Consumer<? super ExecutionRequest> visitor
            ) {
                java.util.Objects.requireNonNull(visitor, "visitor");
                queue.forEach(visitor);
            }

            @Override
            public synchronized List<ExecutionRequest> drain() {
                List<ExecutionRequest> drained = new ArrayList<>(queue);
                queue.clear();
                active = false;
                aborted = false;
                return drained;
            }

            @Override
            public synchronized void close() {
                discard();
            }
        };

        private int dbIndex;
        private String clientName;
        private boolean authenticated;
        private int respVersion = 2;

        @Override
        public int dbIndex() {
            return dbIndex;
        }

        @Override
        public void setDbIndex(int dbIndex) {
            this.dbIndex = dbIndex;
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return clientName;
        }

        @Override
        public void setClientName(String name) {
            this.clientName = name;
        }

        @Override
        public boolean authenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
            this.authenticated = authenticated;
        }

        @Override
        public int respVersion() {
            return respVersion;
        }

        @Override
        public void setRespVersion(int respVersion) {
            this.respVersion = respVersion;
        }

        @Override
        public TransactionState transaction() {
            return tx;
        }

        @Override
        public ConnectionStatsView connectionStats() {
            return new ConnectionStatsView() {
                @Override
                public int pending() {
                    return 0;
                }

                @Override
                public long pendingBytes() {
                    return 0;
                }

                @Override
                public boolean inputDisabledByExecutor() {
                    return false;
                }

                @Override
                public boolean inputPausedByReply() {
                    return false;
                }

                @Override
                public boolean closing() {
                    return false;
                }

                @Override
                public long commandsEnqueued() {
                    return 0;
                }

                @Override
                public long commandsExecuted() {
                    return 0;
                }

                @Override
                public long commandsRejected() {
                    return 0;
                }

                @Override
                public long commandsSkippedClosing() {
                    return 0;
                }

                @Override
                public long closeAfterReply() {
                    return 0;
                }

                @Override
                public long backpressureEnter() {
                    return 0;
                }

                @Override
                public long backpressureExit() {
                    return 0;
                }
            };
        }
    }

    private static final class InitializerTestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final RedisReplyWriterFactory replyWriterFactory;
        private final CommandExecutor<NettyExecutionConnection> executor;

        private InitializerTestEnv() {
            this.instance = TestYierdisInstances.createWithDefaultMemory(YierdisInstanceConfig.builder().build());
            CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
            this.replyWriterFactory = new RespReplyWriterFactory();
            this.executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    dispatcher::prepare,
                    new NettySerialOwnerExecutor(ImmediateEventExecutor.INSTANCE),
                    new RespReplySizer(),
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
