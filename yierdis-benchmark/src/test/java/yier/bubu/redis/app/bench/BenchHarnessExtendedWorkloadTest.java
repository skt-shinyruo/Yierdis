package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.suite.IterationResult;
import yier.bubu.redis.app.bench.suite.RedisSuiteTestSupport;
import yier.bubu.redis.app.bench.suite.ScenarioDefinition;
import yier.bubu.redis.app.bench.suite.SuiteArtifact;
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteHarness;
import yier.bubu.redis.app.bench.suite.SuiteProfileName;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class BenchHarnessExtendedWorkloadTest {
    @Test
    public void commandForEachExtendedWorkloadUsesTheExpectedRedisCommand() {
        byte[] value = ascii("payload");

        assertFrameContains(BenchWorkloadKind.MAXMEMORY_EVICTION, 3, 7, value, "SET");
        assertFrameContains(BenchWorkloadKind.TTL_EXPIRATION, 3, 7, value, "EXPIRE");
        assertFrameContains(BenchWorkloadKind.LIST_LPUSH, 3, 7, value, "LPUSH");
        assertFrameContains(BenchWorkloadKind.HASH_HSET, 3, 7, value, "HSET");
        assertFrameContains(BenchWorkloadKind.SET_SADD, 3, 7, value, "SADD");
        assertFrameContains(BenchWorkloadKind.ZSET_ZADD, 3, 7, value, "ZADD");
        assertFrameContains(BenchWorkloadKind.SCAN, 3, 7, value, "SCAN");
        assertFrameContains(BenchWorkloadKind.GET, 3, 7, value, "GET");
        assertFrameContains(BenchWorkloadKind.SET, 3, 7, value, "SET");
        assertFrameContains(BenchWorkloadKind.LARGE_PIPELINED_REPLY, 3, 7, value, "GET");
        assertFrameContains(BenchWorkloadKind.MIXED_READ_WRITE, 3, 11, value, "GET");
        assertFrameContains(BenchWorkloadKind.MIXED_READ_WRITE, 3, 10, value, "SET");
        assertFrameContains(BenchWorkloadKind.SET_GET, 3, 7, value, "GET");
        assertFrameContains(BenchWorkloadKind.SET_GET, 3, 8, value, "SET");
    }

    @Test
    public void extendedWorkloadsAreRecognizedByHarness() {
        for (BenchWorkloadKind workload : List.of(
                BenchWorkloadKind.MAXMEMORY_EVICTION,
                BenchWorkloadKind.TTL_EXPIRATION,
                BenchWorkloadKind.LIST_LPUSH,
                BenchWorkloadKind.HASH_HSET,
                BenchWorkloadKind.SET_SADD,
                BenchWorkloadKind.ZSET_ZADD,
                BenchWorkloadKind.SCAN,
                BenchWorkloadKind.MIXED_READ_WRITE,
                BenchWorkloadKind.SET_GET,
                BenchWorkloadKind.GET,
                BenchWorkloadKind.SET,
                BenchWorkloadKind.LARGE_PIPELINED_REPLY
        )) {
            Assert.assertTrue(workload.name(), BenchHarness.isExtendedWorkload(workload));
        }

        Assert.assertFalse(BenchHarness.isExtendedWorkload(BenchWorkloadKind.PING));
    }

    @Test
    public void extendedLatencyWorkloadReturnsLatencyMetricsAndDoesNotThrowUnsupported() throws Exception {
        try (OkRespServer server = OkRespServer.start()) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.HASH_HSET,
                    "127.0.0.1",
                    server.port(),
                    6,
                    1,
                    3,
                    16,
                    8,
                    true,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);
            Map<String, Double> metrics = metricsByName(result);

            Assert.assertEquals(6, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertTrue(metrics.containsKey("p95_ms"));
            Assert.assertTrue(metrics.containsKey("p99_ms"));
            Assert.assertEquals(6, server.awaitCommands(6));
        }
    }

    @Test
    public void ttlExpirationPrefillsKeysBeforeExpireRequests() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("SET".equals(command)) {
                return ok();
            }
            if ("EXPIRE".equals(command)) {
                return integer(1);
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.TTL_EXPIRATION,
                    "127.0.0.1",
                    server.port(),
                    4,
                    1,
                    2,
                    3,
                    8,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(4, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertEquals(List.of("SET", "SET", "SET", "EXPIRE", "EXPIRE", "EXPIRE", "EXPIRE"),
                    server.awaitCommands(7));
        }
    }

    @Test
    public void ttlExpirationCountsZeroExpireReplyAsStrictFailure() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("SET".equals(command)) {
                return ok();
            }
            if ("EXPIRE".equals(command)) {
                return integer(0);
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.TTL_EXPIRATION,
                    "127.0.0.1",
                    server.port(),
                    1,
                    1,
                    1,
                    1,
                    8,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(1, result.ops());
            Assert.assertTrue("errors=" + result.errors(), result.errors() > 0);
        }
    }

    @Test
    public void scanCountsMalformedArrayReplyAsStrictFailure() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> emptyArray())) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SCAN,
                    "127.0.0.1",
                    server.port(),
                    1,
                    1,
                    1,
                    8,
                    8,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(1, result.ops());
            Assert.assertTrue("errors=" + result.errors(), result.errors() > 0);
        }
    }

    @Test
    public void mixedReadWritePrefillsKeysBeforeTimedCommands() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return bulk("xxxx");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.MIXED_READ_WRITE,
                    "127.0.0.1",
                    server.port(),
                    4,
                    1,
                    2,
                    3,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(4, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertEquals(List.of("SET", "SET", "SET", "SET", "GET", "GET", "GET"),
                    server.awaitCommands(7));
        }
    }

    @Test
    public void setGetPrefillsKeysAndRunsBothSetAndGetCommands() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return bulk("xxxx");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET_GET,
                    "127.0.0.1",
                    server.port(),
                    4,
                    1,
                    2,
                    3,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(4, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertEquals(List.of("SET", "SET", "SET", "SET", "GET", "SET", "GET"),
                    server.awaitCommands(7));
        }
    }

    @Test
    public void isolatedGetAndLargeReplyWorkloadsPrefillKeysBeforeTimedGets() throws Exception {
        assertGetWorkloadPrefillsKeys(BenchWorkloadKind.GET);
        assertGetWorkloadPrefillsKeys(BenchWorkloadKind.LARGE_PIPELINED_REPLY);
    }

    @Test
    public void isolatedSetWorkloadDoesNotNeedAReadPrefill() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> ok())) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    3,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertEquals(List.of("SET", "SET"), server.awaitCommands(2));
        }
    }

    @Test
    public void mixedReadWriteCountsNullGetReplyAsStrictFailure() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return nullBulk();
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.MIXED_READ_WRITE,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    2,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertTrue("errors=" + result.errors(), result.errors() > 0);
        }
    }

    @Test
    public void redisArtifactStartServerFlushesDbAndDoesNotSpawnProcess() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);
            BenchHarness harness = new BenchHarness();
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario(
                    "release-set-get-128b-c32-p4", BenchWorkloadKind.SET_GET, 1, 1, true);
            SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());

            SuiteHarness.RunningServer running = harness.startServer(artifact, scenario, config, artifact.port(), Path.of("target/redis.log"));

            Assert.assertNull(running.handle());
            Assert.assertEquals(artifact.port(), running.port());
            Assert.assertEquals(List.of(
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "PING",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "FLUSHDB"
            ), server.awaitCommands(6));
        }
    }

    @Test
    public void redisArtifactStartServerFailsWhenSelectReplyIsError() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("AUTH".equals(command)) {
                return ok();
            }
            if ("SELECT".equals(command)) {
                return error("ERR DB index is out of range");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);
            BenchHarness harness = new BenchHarness();
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario(
                    "release-set-get-128b-c32-p4", BenchWorkloadKind.SET_GET, 1, 1, true);
            SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());

            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> harness.startServer(artifact, scenario, config, artifact.port(), Path.of("target/redis.log")));

            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("suite server not ready"));
            List<String> commands = server.awaitCommands(2);
            Assert.assertTrue(commands.size() >= 2);
            Assert.assertEquals("AUTH", commands.get(0));
            Assert.assertEquals("SELECT", commands.get(1));
            Assert.assertFalse(commands.contains("PING"));
        }
    }

    @Test
    public void externalRedisWorkloadCountsBootstrapSelectFailureAsError() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("AUTH".equals(command)) {
                return ok();
            }
            if ("SELECT".equals(command)) {
                return error("ERR DB index is out of range");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET_GET,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    2,
                    4,
                    false,
                    true,
                    "bench-user",
                    "bench-secret",
                    4
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(0, result.ops());
            Assert.assertTrue("errors=" + result.errors(), result.errors() > 0);
            Assert.assertEquals(List.of("AUTH", "SELECT"), server.awaitCommands(2));
        }
    }

    @Test
    public void captureObservationForExternalRedisAuthenticatesAndSelectsConfiguredDb() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness();
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4);
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
            SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());

            harness.startServer(artifact, scenario, config, artifact.port(), Path.of("target/redis.log"));
            harness.captureObservation("127.0.0.1", server.port());

            Assert.assertEquals(List.of(
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "PING",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "FLUSHDB",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "INFO",
                    "AUTH BENCH-USER BENCH-SECRET", "SELECT 4", "MEMORY STATS"
            ), server.awaitCommands(12));
        }
    }

    @Test
    public void externalRedisWorkloadAuthenticatesAndSelectsConfiguredDbBeforeBenchmarkCommands() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return bulk("xxxx");
            }
            if ("SELECT 4".equals(command) || "AUTH bench-user bench-secret".equals(command)) {
                return ok();
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET_GET,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    2,
                    4,
                    false,
                    true,
                    "bench-user",
                    "bench-secret",
                    4
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertEquals(0, result.errors());
            Assert.assertEquals(List.of(
                    "AUTH", "SELECT", "SET", "SET",
                    "AUTH", "SELECT", "SET", "GET"
            ), server.awaitCommands(8));
        }
    }

    @Test
    public void redisArtifactReadyCheckAuthenticatesAndSelectsBeforePing() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("PING".equals(command) && index == 2) {
                return "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());

            boolean ready = BenchHarness.waitReady(
                    SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "bench-user", "bench-secret", 4),
                    500,
                    100
            );

            Assert.assertTrue(ready);
            Assert.assertEquals(List.of("AUTH", "SELECT", "PING"), server.awaitCommands(3));
        }
    }

    @Test
    public void redisDensePrefillUsesConfiguredAuthAndDb() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("AUTH".equals(command) || "SELECT".equals(command)) {
                return ok();
            }
            if ("PFADD".equals(command)) {
                return integer(1);
            }
            if ("PFMERGE".equals(command)) {
                return ok();
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness();
            SuiteConfig config = redisCurrentConfig("203.0.113.10", server.port(), "bench-user", "bench-secret", 4);
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE, 1, 1, false);
            SuiteHarness.RunningServer running = new SuiteHarness.RunningServer("redis", scenario.id(), server.port(), Path.of("target/redis.log"));

            harness.prepareScenario(running, scenario, config);

            List<String> commands = server.awaitCommands(103);
            Assert.assertEquals("AUTH", commands.get(0));
            Assert.assertEquals("SELECT", commands.get(1));
            Assert.assertEquals("PFADD", commands.get(2));
            Assert.assertEquals(103, commands.size());
            for (int i = 3; i < commands.size(); i++) {
                Assert.assertEquals("PFMERGE", commands.get(i));
            }
        }
    }

    @Test
    public void redisWorkloadHostUsesArtifactHost() throws Exception {
        SuiteConfig config = redisCurrentConfig("203.0.113.10", 6380);
        SuiteHarness.RunningServer server = new SuiteHarness.RunningServer("redis", "release-hash-hset-c64-p8", 6380, Path.of("target/redis.log"));

        Assert.assertEquals("127.0.0.1", BenchHarness.workloadHost(server, config));
    }

    @Test
    public void redisDensePrefillUsesArtifactHost() throws Exception {
        RecordingDenseHllPreparer preparer = new RecordingDenseHllPreparer();
        BenchHarness harness = new BenchHarness(preparer, 1_000);
        SuiteConfig config = redisCurrentConfig("203.0.113.10", 6380);
        ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE, 1, 1, false);
        SuiteHarness.RunningServer server = new SuiteHarness.RunningServer("redis", scenario.id(), 6380, Path.of("target/redis.log"));

        harness.prepareScenario(server, scenario, config);

        Assert.assertEquals(List.of("127.0.0.1:6380:100:4:::0"), preparer.calls);
    }

    @Test
    public void redisDensePrefillRunsOnlyOncePerPass() throws Exception {
        RecordingDenseHllPreparer preparer = new RecordingDenseHllPreparer();
        BenchHarness harness = new BenchHarness(preparer, 1_000);
        SuiteConfig config = redisCurrentConfig("203.0.113.10", 6380);
        ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE, 1, 1, false);
        SuiteHarness.RunningServer server = new SuiteHarness.RunningServer("redis", scenario.id(), 6380, Path.of("target/redis.log"));

        harness.prepareScenario(server, scenario, config);
        harness.prepareScenario(server, scenario, config);

        Assert.assertEquals(List.of("127.0.0.1:6380:100:4:::0"), preparer.calls);
    }

    @Test
    public void stoppingRedisPassClearsEndpointClassificationForReusedPort() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness();
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
            SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());

            SuiteHarness.RunningServer running = harness.startServer(artifact, scenario, config, artifact.port(), Path.of("target/redis.log"));

            Assert.assertEquals(1, externalRedisEndpoints(harness).size());

            harness.stopServer(running);

            Assert.assertTrue(externalRedisEndpoints(harness).isEmpty());
        }
    }

    @Test
    public void stoppingRedisPassClearsPreparedStateForRepeatedHarnessReuse() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            RecordingDenseHllPreparer preparer = new RecordingDenseHllPreparer();
            BenchHarness harness = new BenchHarness(preparer, 1_000);
            SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);
            SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE, 1, 1, false);
            Path logFile = Path.of("target/redis.log");

            SuiteHarness.RunningServer firstRun = harness.startServer(artifact, scenario, config, artifact.port(), logFile);
            harness.prepareScenario(firstRun, scenario, config);
            harness.stopServer(firstRun);

            SuiteHarness.RunningServer secondRun = harness.startServer(artifact, scenario, config, artifact.port(), logFile);
            harness.prepareScenario(secondRun, scenario, config);

            Assert.assertEquals(List.of(
                    "127.0.0.1:" + server.port() + ":100:4:::0",
                    "127.0.0.1:" + server.port() + ":100:4:::0"
            ), preparer.calls);
        }
    }

    @Test
    public void setGetCountsNullGetReplyAsStrictFailure() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return nullBulk();
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET_GET,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    2,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertTrue("errors=" + result.errors(), result.errors() > 0);
        }
    }

    @Test
    public void extendedMutatorsCountWrongSuccessfulRepliesAsStrictFailures() throws Exception {
        assertWrongIntegerReplyIsStrictFailure(BenchWorkloadKind.LIST_LPUSH, 0);
        for (BenchWorkloadKind workload : List.of(BenchWorkloadKind.HASH_HSET, BenchWorkloadKind.SET_SADD,
                BenchWorkloadKind.ZSET_ZADD)) {
            assertWrongIntegerReplyIsStrictFailure(workload, 2);
        }
    }

    @Test
    public void hashAndZsetUpdatesAcceptRedisZeroMutationCounts() throws Exception {
        for (BenchWorkloadKind workload : List.of(BenchWorkloadKind.HASH_HSET, BenchWorkloadKind.ZSET_ZADD)) {
            try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> integer(0))) {
                Assert.assertTrue(server.awaitListening());
                BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
                BenchWorkloadRequest request = new BenchWorkloadRequest(
                        workload,
                        "127.0.0.1",
                        server.port(),
                        1,
                        1,
                        1,
                        1,
                        8,
                        false,
                        true
                );

                BenchWorkloadResult result = harness.runWorkload(request);

                Assert.assertEquals(workload.name(), 1, result.ops());
                Assert.assertEquals(workload.name(), 0, result.errors());
            }
        }
    }

    private static void assertGetWorkloadPrefillsKeys(BenchWorkloadKind workload) throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return bulk("xxxx");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    workload,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    3,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(workload.name(), 2, result.ops());
            Assert.assertEquals(workload.name(), 0, result.errors());
            Assert.assertEquals(workload.name(), List.of("SET", "SET", "SET", "GET", "GET"),
                    server.awaitCommands(5));
        }
    }

    private static void assertWrongIntegerReplyIsStrictFailure(BenchWorkloadKind workload, int replyValue) throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("SET".equals(command)) {
                return ok();
            }
            return integer(replyValue);
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    workload,
                    "127.0.0.1",
                    server.port(),
                    1,
                    1,
                    1,
                    8,
                    8,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(workload.name(), 1, result.ops());
            Assert.assertEquals(workload.name(), 1, result.errors());
        }
    }

    @Test
    public void mixedReadWriteStrictlyValidatesBulkStringLength() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("SET".equals(command)) {
                return ok();
            }
            if ("GET".equals(command)) {
                return bulk("x");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.MIXED_READ_WRITE,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    8,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertEquals(1, result.errors());
        }
    }

    @Test
    public void setGetStrictlyValidatesBulkStringLength() throws Exception {
        try (ScriptedRespServer server = ScriptedRespServer.start((command, index) -> {
            if ("GET".equals(command)) {
                return bulk("x");
            }
            return ok();
        })) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new NoopDenseHllPreparer(), 1_000);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.SET_GET,
                    "127.0.0.1",
                    server.port(),
                    2,
                    1,
                    1,
                    8,
                    4,
                    false,
                    true
            );

            BenchWorkloadResult result = harness.runWorkload(request);

            Assert.assertEquals(2, result.ops());
            Assert.assertEquals(1, result.errors());
        }
    }

    private static void assertFrameContains(
            BenchWorkloadKind workload,
            int keyIndex,
            int opIndex,
            byte[] value,
            String command
    ) {
        String frame = new String(
                BenchHarness.encodeExtendedCommandForTest(workload, keyIndex, opIndex, value),
                StandardCharsets.US_ASCII
        );
        Assert.assertTrue(frame, frame.contains("\r\n" + command + "\r\n"));
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] ok() {
        return "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] integer(long value) {
        return (":" + value + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] emptyArray() {
        return "*0\r\n".getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] bulk(String value) {
        return ("$" + value.length() + "\r\n" + value + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] nullBulk() {
        return "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] error(String value) {
        return ("-" + value + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static Map<String, Double> metricsByName(BenchWorkloadResult result) {
        return result.toMetrics().stream()
                .collect(java.util.stream.Collectors.toMap(metric -> metric.name(), metric -> metric.value()));
    }

    private static double metric(IterationResult result, String name) {
        return result.metrics().stream()
                .filter(metric -> metric.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing metric " + name))
                .value();
    }

    private static SuiteConfig redisCurrentConfig(String suiteHost, int redisPort) throws Exception {
        return redisCurrentConfig(suiteHost, redisPort, "", "", 0);
    }

    private static SuiteConfig redisCurrentConfig(String suiteHost, int redisPort, String redisUser, String redisAuth, int redisDb) throws Exception {
        yier.bubu.redis.app.bench.YierdisBenchServerArgs serverArgs = new yier.bubu.redis.app.bench.YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        SuiteArtifact redis = SuiteArtifact.externalRedis("redis", "127.0.0.1", redisPort, redisUser, redisAuth, redisDb);
        SuiteArtifact current = SuiteArtifact.yierdisJar("current", RedisSuiteTestSupport.redisCurrentOnlyConfig(
                Path.of("target/redis-suite-test"), 16378, redisPort).current().jarPath(), "head");
        return new SuiteConfig(
                SuiteProfileName.RELEASE,
                current,
                Optional.empty(),
                List.of(redis, current),
                Path.of("target/redis-suite-test"),
                suiteHost,
                16378,
                "java",
                "4g",
                "4g",
                "6g",
                serverArgs,
                true
        );
    }

    @SuppressWarnings("unchecked")
    private static List<String> externalRedisEndpoints(BenchHarness harness) throws Exception {
        Field field = BenchHarness.class.getDeclaredField("externalRedisEndpoints");
        field.setAccessible(true);
        java.util.Set<Object> endpoints = (java.util.Set<Object>) field.get(harness);
        return endpoints.stream().map(Object::toString).toList();
    }

    private static final class RecordingDenseHllPreparer implements BenchHarness.DenseHllPreparer {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void prefill(String host, int port, int keyspace, int pipeline, String redisUser, String redisAuth, int redisDb) {
            calls.add(host + ":" + port + ":" + keyspace + ":" + pipeline
                    + ":" + redisUser + ":" + redisAuth + ":" + redisDb);
        }
    }

    private static final class NoopDenseHllPreparer implements BenchHarness.DenseHllPreparer {
        @Override
        public void prefill(String host, int port, int keyspace, int pipeline, String redisUser, String redisAuth, int redisDb) {
        }
    }

    private static final class OkRespServer implements AutoCloseable {
        private static final byte[] OK = ok();
        private static final byte[] ONE = integer(1);
        private static final byte[] SCAN_REPLY = "*2\r\n$1\r\n0\r\n*0\r\n".getBytes(StandardCharsets.US_ASCII);

        private final ServerSocket serverSocket;
        private final CountDownLatch listening = new CountDownLatch(1);
        private final List<Socket> accepted = new ArrayList<>();
        private volatile boolean closed;
        private volatile int commands;
        private Thread thread;

        private OkRespServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        static OkRespServer start() throws IOException {
            OkRespServer server = new OkRespServer(new ServerSocket(0));
            server.thread = new Thread(server::acceptLoop, "bench-harness-ok-resp-server");
            server.thread.setDaemon(true);
            server.thread.start();
            return server;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        boolean awaitListening() throws InterruptedException {
            return listening.await(1, TimeUnit.SECONDS);
        }

        int awaitCommands(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (System.nanoTime() < deadline) {
                if (commands >= expected) {
                    return commands;
                }
                Thread.sleep(10);
            }
            return commands;
        }

        private void acceptLoop() {
            listening.countDown();
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    synchronized (accepted) {
                        accepted.add(socket);
                    }
                    handle(socket);
                } catch (IOException e) {
                    if (!closed && !isClientDisconnect(e)) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }

        private void handle(Socket socket) throws IOException {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            while (!closed && !socket.isClosed()) {
                RespClientCodec.RespReply command = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                if (command.kind() != RespClientCodec.RespReply.Kind.ARRAY || command.values().isEmpty()) {
                    out.write(OK);
                    out.flush();
                    continue;
                }
                commands++;
                String name = commandName(command);
                if ("SCAN".equals(name)) {
                    out.write(SCAN_REPLY);
                } else if ("GET".equals(name)) {
                    out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if ("EXPIRE".equals(name) || "LPUSH".equals(name) || "HSET".equals(name)
                        || "SADD".equals(name) || "ZADD".equals(name)) {
                    out.write(ONE);
                } else {
                    out.write(OK);
                }
                out.flush();
            }
        }

        private String commandName(RespClientCodec.RespReply command) {
            byte[] bytes = command.values().get(0).bytes();
            return bytes == null ? "" : new String(bytes, StandardCharsets.US_ASCII);
        }

        private boolean isClientDisconnect(IOException e) {
            return e.getMessage() != null && e.getMessage().contains("unexpected EOF");
        }

        @Override
        public void close() throws Exception {
            closed = true;
            serverSocket.close();
            synchronized (accepted) {
                for (Socket socket : accepted) {
                    socket.close();
                }
            }
            if (thread != null) {
                thread.join(1_000);
            }
        }
    }

    private static final class ScriptedRespServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final CountDownLatch listening = new CountDownLatch(1);
        private final List<Socket> accepted = new ArrayList<>();
        private final List<String> commands = Collections.synchronizedList(new ArrayList<>());
        private final BiFunction<String, Integer, byte[]> responseScript;
        private volatile boolean closed;
        private Thread thread;

        private ScriptedRespServer(ServerSocket serverSocket, BiFunction<String, Integer, byte[]> responseScript) {
            this.serverSocket = serverSocket;
            this.responseScript = responseScript;
        }

        static ScriptedRespServer start(BiFunction<String, Integer, byte[]> responseScript) throws IOException {
            ScriptedRespServer server = new ScriptedRespServer(new ServerSocket(0), responseScript);
            server.thread = new Thread(server::acceptLoop, "bench-harness-scripted-resp-server");
            server.thread.setDaemon(true);
            server.thread.start();
            return server;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        boolean awaitListening() throws InterruptedException {
            return listening.await(1, TimeUnit.SECONDS);
        }

        List<String> awaitCommands(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (System.nanoTime() < deadline) {
                synchronized (commands) {
                    if (commands.size() >= expected) {
                        return List.copyOf(commands);
                    }
                }
                Thread.sleep(10);
            }
            synchronized (commands) {
                return List.copyOf(commands);
            }
        }

        private void acceptLoop() {
            listening.countDown();
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    synchronized (accepted) {
                        accepted.add(socket);
                    }
                    handle(socket);
                } catch (IOException e) {
                    if (!closed && !isClientDisconnect(e)) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }

        private void handle(Socket socket) throws IOException {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            while (!closed && !socket.isClosed()) {
                RespClientCodec.RespReply command = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
                if (command.kind() != RespClientCodec.RespReply.Kind.ARRAY || command.values().isEmpty()) {
                    continue;
                }
                String name = commandName(command);
                int index;
                synchronized (commands) {
                    index = commands.size();
                    commands.add(name);
                }
                byte[] reply = responseScript.apply(name, index);
                out.write(reply == null ? ok() : reply);
                out.flush();
            }
        }

        private String commandName(RespClientCodec.RespReply command) {
            byte[] bytes = command.values().get(0).bytes();
            return bytes == null ? "" : new String(bytes, StandardCharsets.US_ASCII);
        }

        private boolean isClientDisconnect(IOException e) {
            return e.getMessage() != null && e.getMessage().contains("unexpected EOF");
        }

        @Override
        public void close() throws Exception {
            closed = true;
            serverSocket.close();
            synchronized (accepted) {
                for (Socket socket : accepted) {
                    socket.close();
                }
            }
            if (thread != null) {
                thread.join(1_000);
            }
        }
    }
}
