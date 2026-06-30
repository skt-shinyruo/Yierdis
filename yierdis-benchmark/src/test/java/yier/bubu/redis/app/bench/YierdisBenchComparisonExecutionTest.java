package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class YierdisBenchComparisonExecutionTest {
    @Test
    public void comparisonSideContextsUseSharedServerArgvWithSideSpecificJarAndPort() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");
        Path runDir = Files.createTempDirectory("bench-comparison-run-");

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        new CommandLine(serverArgs).parseArgs(
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--nativeDefragEnabled",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );
        serverArgs.normalizeAndValidate();

        YierdisBench.BenchConfig config = config(serverArgs,
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--javaCmd", "/usr/lib/jvm/java-25-openjdk-amd64/bin/java",
                "--xms", "512m",
                "--xmx", "512m",
                "--maxDirectMemory", "1g",
                "--portBase", "17378"
        );

        YierdisBench.ComparisonSideContext baseline = YierdisBench.comparisonSideContext(config, "baseline", baselineJar, 0, runDir);
        YierdisBench.ComparisonSideContext current = YierdisBench.comparisonSideContext(config, "current", currentJar, 1, runDir);

        Assert.assertEquals("baseline", baseline.label);
        Assert.assertEquals("current", current.label);
        Assert.assertEquals(17378, baseline.port);
        Assert.assertEquals(17379, current.port);
        Assert.assertEquals(baselineJar, baseline.jarPath);
        Assert.assertEquals(currentJar, current.jarPath);
        Assert.assertEquals(runDir.resolve("server-baseline.log"), baseline.logFile);
        Assert.assertEquals(runDir.resolve("server-current.log"), current.logFile);

        List<String> baselineArgv = baseline.serverArgs.toArgv();
        List<String> currentArgv = current.serverArgs.toArgv();
        Assert.assertNotEquals(baselineArgv, currentArgv);
        assertArgValue(baselineArgv, "--port", "17378");
        assertArgValue(currentArgv, "--port", "17379");
        Assert.assertTrue(baselineArgv.contains("--nativeDefragEnabled"));
        Assert.assertTrue(currentArgv.contains("--nativeDefragEnabled"));
        assertArgValue(baselineArgv, "--maxmemoryScope", "per-db");
        assertArgValue(currentArgv, "--maxmemoryScope", "per-db");

        Assert.assertTrue(baseline.commandLine.contains(baselineJar.toAbsolutePath().toString()));
        Assert.assertTrue(current.commandLine.contains(currentJar.toAbsolutePath().toString()));
        Assert.assertFalse(baseline.commandLine.contains(currentJar.toAbsolutePath().toString()));
        Assert.assertFalse(current.commandLine.contains(baselineJar.toAbsolutePath().toString()));
    }

    @Test
    public void comparisonSideContextPreservesExplicitNativeSlotCapacityInServerArgv() throws Exception {
        Path currentJar = regularTempJar("current");
        Path runDir = Files.createTempDirectory("bench-comparison-run-native-slots-");

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        new CommandLine(serverArgs).parseArgs(
                "--databases", "1",
                "--nativeSlotCapacity", "2097152"
        );
        serverArgs.normalizeAndValidate();

        YierdisBench.BenchConfig config = config(serverArgs,
                "--comparisonMode",
                "--baselineServerJar", currentJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--javaCmd", "/usr/lib/jvm/java-25-openjdk-amd64/bin/java",
                "--xms", "512m",
                "--xmx", "512m",
                "--maxDirectMemory", "1g",
                "--portBase", "17378"
        );

        YierdisBench.ComparisonSideContext current = YierdisBench.comparisonSideContext(config, "current", currentJar, 1, runDir);

        assertArgValue(current.serverArgs.toArgv(), "--databases", "1");
        assertArgValue(current.serverArgs.toArgv(), "--nativeSlotCapacity", "2097152");
    }

    @Test
    public void comparisonSideValidationDetectsMissingMeasurementsAndErrors() {
        YierdisBench.BackendResult empty = new YierdisBench.BackendResult("baseline", 16378);
        Assert.assertFalse(YierdisBench.comparisonSideHasAnyMeasurements(empty));
        Assert.assertFalse(YierdisBench.comparisonSideHasRequiredMeasurements(empty, false));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(empty, false));

        YierdisBench.BackendResult partial = new YierdisBench.BackendResult("baseline", 16378);
        partial.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 100, 0, 1.0, 100.0, java.time.Instant.parse("2026-05-16T00:00:00Z")
        );
        Assert.assertTrue(YierdisBench.comparisonSideHasAnyMeasurements(partial));
        Assert.assertFalse(YierdisBench.comparisonSideHasRequiredMeasurements(partial, true));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(partial, true));

        YierdisBench.BackendResult completeWithErrors = completeThroughputOnly("baseline", 16378, 1);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeWithErrors, true));
        Assert.assertTrue(YierdisBench.comparisonSideHasBenchmarkErrors(completeWithErrors, true));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(completeWithErrors, true));

        YierdisBench.BackendResult completeWithLatencyErrors = completeWithLatency("baseline", 16378, 2);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeWithLatencyErrors, false));
        Assert.assertTrue(YierdisBench.comparisonSideHasBenchmarkErrors(completeWithLatencyErrors, false));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(completeWithLatencyErrors, false));

        YierdisBench.BackendResult completeClean = completeThroughputOnly("baseline", 16378, 0);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeClean, true));
        Assert.assertFalse(YierdisBench.comparisonSideHasBenchmarkErrors(completeClean, true));
        Assert.assertTrue(YierdisBench.comparisonSideCanBeCompared(completeClean, true));
    }

    private static YierdisBench.BenchConfig config(YierdisBenchServerArgs serverArgs, String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        return YierdisBench.BenchConfig.from(args, serverArgs);
    }

    private static Path regularTempJar(String label) throws Exception {
        Path jar = Files.createTempFile("bench-" + label + "-", ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    private static YierdisBench.BackendResult completeThroughputOnly(String label, int port, long errors) {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult(label, port);
        result.setThroughput = throughput(YierdisBench.Workload.SET_RANDOM, errors);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, 0);
        return result;
    }

    private static YierdisBench.BackendResult completeWithLatency(String label, int port, long latencyErrors) {
        YierdisBench.BackendResult result = completeThroughputOnly(label, port, 0);
        result.pingLatency = latency(YierdisBench.Workload.PING, latencyErrors);
        result.setLatency = latency(YierdisBench.Workload.SET_RANDOM, 0);
        result.getLatency = latency(YierdisBench.Workload.GET_RANDOM, 0);
        result.appendLatency = latency(YierdisBench.Workload.APPEND, 0);
        result.pfaddSparseLatency = latency(YierdisBench.Workload.PFADD_SPARSE, 0);
        result.pfaddDenseLatency = latency(YierdisBench.Workload.PFADD_DENSE, 0);
        return result;
    }

    private static YierdisBench.ThroughputResult throughput(YierdisBench.Workload workload, long errors) {
        return new YierdisBench.ThroughputResult(
                workload, 100, errors, 1.0, 100.0, java.time.Instant.parse("2026-05-16T00:00:00Z")
        );
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors) {
        return new YierdisBench.LatencyResult(
                workload,
                100,
                errors,
                1.0,
                100.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );
    }

    private static void assertArgValue(List<String> argv, String flag, String expectedValue) {
        int index = argv.indexOf(flag);
        Assert.assertTrue("missing flag: " + flag, index >= 0);
        Assert.assertTrue("missing value after flag: " + flag, index + 1 < argv.size());
        Assert.assertEquals(expectedValue, argv.get(index + 1));
    }
}
