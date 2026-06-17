package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteProfileName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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
    public void harnessRejectsUnsupportedExtendedWorkloadsClearly() {
        BenchWorkloadRequest request = new BenchWorkloadRequest(
                BenchWorkloadKind.MAXMEMORY_EVICTION,
                "127.0.0.1",
                6379,
                1,
                1,
                1,
                1,
                0,
                false,
                true
        );

        IllegalArgumentException failure = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> new BenchHarness().runWorkload(request)
        );

        Assert.assertEquals("unsupported extended suite workload: MAXMEMORY_EVICTION", failure.getMessage());
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

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
