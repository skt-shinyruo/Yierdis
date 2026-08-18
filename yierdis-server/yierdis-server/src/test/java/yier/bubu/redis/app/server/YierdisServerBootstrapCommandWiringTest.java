package yier.bubu.redis.app.server;

import java.util.function.BiFunction;

import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.engine.EngineSession;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespReplySizer;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.protocol.resp.RespReplyWriter;
import yier.bubu.redis.protocol.resp.netty.InboundByteAccountingHandler;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public void infoHealthKeepsTenPairsAcrossResp2AndResp3() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--databases", "1"
        ); Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Map<String, Object> resp2 = respMap(roundTrip(out, in, "INFO", "health"));
            assertHealthInfo(resp2);

            Assert.assertEquals(3L, asLong(respMap(roundTrip(out, in, "HELLO", "3")).get("proto")));
            Map<String, Object> resp3 = respMap(roundTrip(out, in, "INFO", "health"));
            assertHealthInfo(resp3);
            Assert.assertEquals(resp2.keySet(), resp3.keySet());
        }
    }

    @Test
    public void serverAndDefaultCommandsBuildOneDispatcher() throws Exception {
        try (YierdisInstance instance = YierdisInstance.create(
                YierdisInstanceConfig.builder().databases(1).build()
        )) {
            instance.runtimeAccess().bindToCurrentThread();
            NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(
                    TestCommandDispatchers.runtimeConfig(0, 0, 1024, 1, 4, 5)
            );
            CommandDispatcher dispatcher = CommandRegistries.dispatcher(
                    DefaultCommandModules.create(
                            YierdisServerBootstrap.dbRouter(instance),
                            infoProvider,
                            SlowCommandLimits.DEFAULT
                    ),
                    new ServerCommandModule(infoProvider)
            );
            BiFunction<CommandSession, ExecutionRequest, PreparedCommand> execution = dispatcher::prepare;
            EngineSession session = new EngineSession(0, 0);
            PreparedCommand prepared = execution.apply(session, request("PING"));
            Assert.assertNotNull(prepared);
        }
    }

    @Test
    public void bootstrapSourceWiresDispatcherAndMaintenanceDirectly() throws Exception {
        String source = Files.readString(bootstrapSource(), StandardCharsets.UTF_8);

        Assert.assertTrue(source.contains("dispatcher::prepare"));
        Assert.assertTrue(source.contains("maintenanceTick.run()"));
        Assert.assertTrue(source.contains("runtimeAccess::deferredReclamationTick"));
        Assert.assertFalse(source.contains("Yierdis" + "Engine"));
        Assert.assertFalse(source.contains("DefaultYierdis" + "Engine"));
        Assert.assertFalse(source.contains("YierdisFastCommand" + "Processor"));
    }

    @Test
    public void serverCommandSourcesUseSemanticRepliesWithoutWriterBridges() throws Exception {
        for (Path sourcePath : List.of(serverSource("ServerCommandModule.java"),
                serverSource("NettyServerInfoProvider.java"))) {
            String source = Files.readString(sourcePath, StandardCharsets.UTF_8);
            Assert.assertFalse(source.contains("RedisReplyWriter"));
            Assert.assertFalse(source.contains("ReplyShapes.maximum()"));
            Assert.assertFalse(source.contains("maximumReply("));
        }
    }

    @Test
    public void unboundInfoProviderReturnsSemanticNotReadyErrors() {
        NettyServerInfoProvider provider = new NettyServerInfoProvider(
                TestCommandDispatchers.runtimeConfig(0, 0, 1024, 1, 4, 5));
        EngineSession session = new EngineSession(0, 0);

        RedisReply info = provider.info(new CommandArgs(request("INFO")), session);
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
    public void execWrongTypeElementFitsTheBoundedReplyAndKeepsTheConnectionUsable() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0"
        ); Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Assert.assertEquals("OK", asString(roundTrip(out, in, "SET", "tx:scalar", "scalar")));
            Assert.assertEquals("OK", asString(roundTrip(out, in, "MULTI")));
            Assert.assertEquals("QUEUED", asString(roundTrip(out, in, "LPUSH", "tx:scalar", "value")));

            List<Object> exec = respArray(roundTrip(out, in, "EXEC"));
            Assert.assertEquals(1, exec.size());
            Assert.assertEquals(
                    new RespError("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    exec.get(0)
            );
            Assert.assertEquals("scalar", asString(roundTrip(out, in, "GET", "tx:scalar")));
            Assert.assertEquals("PONG", asString(roundTrip(out, in, "PING")));
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
            YierdisServerRuntimeConfig commandLimitedConfig =
                    TestCommandDispatchers.runtimeConfig(1, 0, 3, 2, 4, 5);
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

            YierdisServerRuntimeConfig byteLimitedConfig =
                    TestCommandDispatchers.runtimeConfig(0, 4, 3, 2, 4, 5);
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
            YierdisServerRuntimeConfig config =
                    TestCommandDispatchers.runtimeConfig(0, 0, 1024, 16, 128, 1024);
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
        RespClientCodec.writeCommand(
                out,
                Arrays.stream(args).map(value -> value.getBytes(StandardCharsets.UTF_8)).toList()
        );
        out.flush();
        return respValue(RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES));
    }

    private static Object respValue(RespClientCodec.RespReply reply) {
        return switch (reply.kind()) {
            case SIMPLE_STRING -> reply.text();
            case ERROR -> new RespError(reply.text());
            case INTEGER -> reply.integer();
            case BULK_STRING -> new String(reply.bytes(), StandardCharsets.UTF_8);
            case NULL -> null;
            case ARRAY, MAP, SET -> reply.values().stream()
                    .map(YierdisServerBootstrapCommandWiringTest::respValue)
                    .toList();
        };
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

    private static void assertHealthInfo(Map<String, Object> health) {
        Assert.assertEquals(10, health.size());
        Assert.assertEquals("RUNNING", asString(health.get("lifecycle_state")));
        Assert.assertEquals(1L, asLong(health.get("ready")));
        Assert.assertEquals(1L, asLong(health.get("writable")));
        Assert.assertEquals(0L, asLong(health.get("degraded_databases")));
        Assert.assertEquals(1L, asLong(health.get("databases")));
        Assert.assertTrue(health.containsKey("first_failure_type"));
        Assert.assertTrue(health.containsKey("first_failure_message"));
        Assert.assertTrue(asLong(health.get("total_connections_received")) >= 1L);
        Assert.assertTrue(health.containsKey("rejected_connections"));
        Assert.assertTrue(health.containsKey("max_clients"));
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
                "yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java"
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
        Path fromRepo = moduleRoot.resolve("yierdis-server/yierdis-server/src/main/java/yier/bubu/redis/app/server")
                .resolve(fileName);
        Assert.assertTrue("cannot locate " + fileName + " from " + moduleRoot, Files.isRegularFile(fromRepo));
        return fromRepo;
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

    private static final class InitializerTestEnv implements AutoCloseable {
        private final YierdisInstance instance;
        private final BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory;
        private final CommandExecutor<NettyExecutionConnection> executor;

        private InitializerTestEnv() {
            this.instance = YierdisInstance.create(YierdisInstanceConfig.builder().build());
            CommandDispatcher dispatcher = TestCommandDispatchers.forInstance(instance);
            this.replyWriterFactory = RespReplyWriter::new;
            this.executor = new CommandExecutor<>(
                    instance.runtimeAccess()::bindToCurrentThread,
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
