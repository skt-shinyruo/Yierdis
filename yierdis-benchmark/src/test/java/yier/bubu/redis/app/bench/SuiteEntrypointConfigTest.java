package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.suite.ScenarioDefinition;
import yier.bubu.redis.app.bench.suite.SuiteArtifact;
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteHarness;
import yier.bubu.redis.app.bench.suite.SuiteProfileName;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SuiteEntrypointConfigTest {
    @Test
    public void suiteConfigFromEntrypointArgsCarriesServerAndReportSettings() throws Exception {
        Path current = regularTempJar("current");
        Path reportDir = Files.createTempDirectory("suite-entrypoint-report-");

        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        new CommandLine(benchArgs).parseArgs(
                "--suite",
                "--currentServerJar", current.toString(),
                "--reportDir", reportDir.toString(),
                "--suiteProfile", "release",
                "--javaCmd", "/custom/java",
                "--xms", "512m",
                "--xmx", "2g",
                "--maxDirectMemory", "3g",
                "--host", "127.0.0.2",
                "--portBase", "17378",
                "--port", "6381",
                "--maxmemoryBytes", "1048576"
        );

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        new CommandLine(serverArgs).parseArgs(benchArgs.serverArgs.toArray(String[]::new));
        serverArgs.normalizeAndValidate();

        SuiteConfig config = SuiteConfig.from(benchArgs, serverArgs);

        Assert.assertEquals(SuiteProfileName.RELEASE, config.profile());
        Assert.assertEquals(current.toAbsolutePath().normalize(), config.current().jarPath());
        Assert.assertEquals(reportDir.toAbsolutePath().normalize(), config.reportDir());
        Assert.assertEquals("127.0.0.2", config.host());
        Assert.assertEquals(17378, config.portBase());
        Assert.assertEquals("/custom/java", config.javaCmd());
        Assert.assertEquals("512m", config.xms());
        Assert.assertEquals("2g", config.xmx());
        Assert.assertEquals("3g", config.maxDirectMemory());
        Assert.assertEquals(6381, config.baseServerArgs().port);
        Assert.assertEquals(1_048_576L, config.baseServerArgs().maxmemoryBytes);
        Assert.assertTrue(config.strictReplies());
    }

    @Test
    public void suiteConfigFromEntrypointArgsCarriesRedisSettings() throws Exception {
        Path current = regularTempJar("current");

        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        new CommandLine(benchArgs).parseArgs(
                "--suite",
                "--includeRedis",
                "--currentServerJar", current.toString(),
                "--redisHost", "127.0.0.9",
                "--redisPort", "6389",
                "--redisLabel", "redis",
                "--redisUser", "bench-user",
                "--redisAuth", "bench-secret",
                "--redisDb", "4"
        );

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();

        SuiteConfig config = SuiteConfig.from(benchArgs, serverArgs);

        Assert.assertEquals(List.of("redis", "current"), config.artifactLabels());
        Assert.assertEquals("redis", config.artifactsInRunOrder().get(0).label());
        Assert.assertEquals(SuiteArtifact.Kind.EXTERNAL_REDIS, config.artifactsInRunOrder().get(0).kind());
        Assert.assertEquals("127.0.0.9", config.artifactsInRunOrder().get(0).host());
        Assert.assertEquals(6389, config.artifactsInRunOrder().get(0).port());
        Assert.assertEquals("bench-user", config.artifactsInRunOrder().get(0).authUser());
        Assert.assertEquals("bench-secret", config.artifactsInRunOrder().get(0).authPassword());
        Assert.assertEquals(4, config.artifactsInRunOrder().get(0).db());
    }

    @Test
    public void workloadRequestRejectsInvalidValues() {
        assertRejects("workload", () -> new BenchWorkloadRequest(null, "127.0.0.1", 6379, 1, 1, 1, 1, 0, false, true));
        assertRejects("host", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, " ", 6379, 1, 1, 1, 1, 0, false, true));
        assertRejects("port", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 0, 1, 1, 1, 1, 0, false, true));
        assertRejects("requests", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 6379, 0, 1, 1, 1, 0, false, true));
        assertRejects("clients", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 6379, 1, 0, 1, 1, 0, false, true));
        assertRejects("pipeline", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 6379, 1, 1, 0, 1, 0, false, true));
        assertRejects("keyspace", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 6379, 1, 1, 1, 0, 0, false, true));
        assertRejects("dataSize", () -> new BenchWorkloadRequest(BenchWorkloadKind.PING, "127.0.0.1", 6379, 1, 1, 1, 1, -1, false, true));
    }

    @Test
    public void workloadResultExportsRequiredMetricsAndLatencyMetricsWhenPresent() {
        BenchWorkloadResult throughput = new BenchWorkloadResult(100, 2, 0.5, 200.0, Double.NaN, Double.NaN, Double.NaN);

        Assert.assertEquals(4, throughput.toMetrics().size());
        Assert.assertEquals("ops", throughput.toMetrics().get(0).name());
        Assert.assertEquals(100.0, throughput.toMetrics().get(0).value(), 0.0);
        Assert.assertEquals("errors", throughput.toMetrics().get(1).name());
        Assert.assertEquals("seconds", throughput.toMetrics().get(2).name());
        Assert.assertEquals("qps", throughput.toMetrics().get(3).name());

        BenchWorkloadResult latency = new BenchWorkloadResult(100, 0, 1.0, 100.0, 1.0, 2.0, 3.0);

        Assert.assertEquals(7, latency.toMetrics().size());
        Assert.assertEquals("p50_ms", latency.toMetrics().get(4).name());
        Assert.assertEquals("p95_ms", latency.toMetrics().get(5).name());
        Assert.assertEquals("p99_ms", latency.toMetrics().get(6).name());
    }

    @Test
    public void readinessProbeTimesOutWhenServerAcceptsButNeverReplies() throws Exception {
        try (HangingServer server = HangingServer.start()) {
            Assert.assertTrue(server.awaitListening());

            long startNs = System.nanoTime();
            boolean ready = BenchHarness.waitReady("127.0.0.1", server.port(), 250, 50);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

            Assert.assertFalse(ready);
            Assert.assertTrue("elapsedMillis=" + elapsedMillis, elapsedMillis < 1_000);
        }
    }

    @Test
    public void workloadReturnsWhenServerAcceptsButNeverReplies() throws Exception {
        try (HangingServer server = HangingServer.start()) {
            Assert.assertTrue(server.awaitListening());
            BenchHarness harness = new BenchHarness(new RecordingDenseHllPreparer(), 100);
            BenchWorkloadRequest request = new BenchWorkloadRequest(
                    BenchWorkloadKind.PING,
                    "127.0.0.1",
                    server.port(),
                    1,
                    1,
                    1,
                    1,
                    0,
                    false,
                    true
            );

            long startNs = System.nanoTime();
            BenchWorkloadResult result = harness.runWorkload(request);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

            Assert.assertEquals(0, result.ops());
            Assert.assertEquals(1, result.errors());
            Assert.assertTrue("elapsedMillis=" + elapsedMillis, elapsedMillis < 1_000);
        }
    }

    @Test
    public void denseHllPrefillRunsOncePerPassOnlyForDenseScenarios() throws Exception {
        Path current = regularTempJar("current");
        Path reportDir = Files.createTempDirectory("suite-prefill-report-");
        SuiteConfig config = suiteConfig(current, reportDir);
        RecordingDenseHllPreparer preparer = new RecordingDenseHllPreparer();
        BenchHarness harness = new BenchHarness(preparer);
        SuiteHarness.RunningServer dense = runningServer("dense-pass", 17379, reportDir);
        SuiteHarness.RunningServer pfcount = runningServer("pfcount-pass", 17380, reportDir);
        SuiteHarness.RunningServer ping = runningServer("ping-pass", 17381, reportDir);
        SuiteHarness.RunningServer setGet = runningServer("set-get-pass", 17382, reportDir);

        harness.prepareScenario(dense, scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE), config);
        harness.prepareScenario(dense, scenario("release-hll-dense-c64-p8", BenchWorkloadKind.HLL_DENSE), config);
        harness.prepareScenario(pfcount, scenario("release-hll-pfcount-c64-p8", BenchWorkloadKind.HLL_PFCOUNT), config);
        harness.prepareScenario(ping, scenario("release-ping-latency", BenchWorkloadKind.PING), config);
        harness.prepareScenario(setGet, scenario("release-set-get-128b-c32-p4", BenchWorkloadKind.SET_GET), config);

        Assert.assertEquals(List.of(
                "127.0.0.1:17379:4096:4",
                "127.0.0.1:17380:4096:4"
        ), preparer.calls);
    }

    private static void assertRejects(String messagePart, ThrowingRunnable runnable) {
        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class, runnable::run);
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(messagePart));
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    private static SuiteConfig suiteConfig(Path current, Path reportDir) {
        YierdisBenchArgs benchArgs = new YierdisBenchArgs();
        new CommandLine(benchArgs).parseArgs(
                "--suite",
                "--currentServerJar", current.toString(),
                "--reportDir", reportDir.toString()
        );
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return SuiteConfig.from(benchArgs, serverArgs);
    }

    private static ScenarioDefinition scenario(String id, BenchWorkloadKind workload) {
        return new ScenarioDefinition(id, id, workload, 4096, 0, 1, 1, 4, 1, 1,
                workload != BenchWorkloadKind.HLL_PFCOUNT);
    }

    private static SuiteHarness.RunningServer runningServer(String scenarioId, int port, Path reportDir) {
        return new SuiteHarness.RunningServer("current", scenarioId, port, reportDir.resolve(scenarioId + ".log"));
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    private static final class RecordingDenseHllPreparer implements BenchHarness.DenseHllPreparer {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void prefill(String host, int port, int keyspace, int pipeline) {
            calls.add(host + ":" + port + ":" + keyspace + ":" + pipeline);
        }
    }

    private static final class HangingServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final CountDownLatch listening = new CountDownLatch(1);
        private final List<Socket> accepted = new ArrayList<>();
        private volatile boolean closed;
        private Thread thread;

        private HangingServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        static HangingServer start() throws IOException {
            HangingServer server = new HangingServer(new ServerSocket(0));
            server.thread = new Thread(server::acceptLoop, "bench-harness-hanging-server");
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

        private void acceptLoop() {
            listening.countDown();
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    synchronized (accepted) {
                        accepted.add(socket);
                    }
                } catch (IOException e) {
                    if (!closed) {
                        throw new IllegalStateException(e);
                    }
                }
            }
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
