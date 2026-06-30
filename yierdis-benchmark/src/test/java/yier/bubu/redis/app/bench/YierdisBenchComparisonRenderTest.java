package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.suite.RedisSuiteTestSupport;
import yier.bubu.redis.app.bench.suite.SuiteCsvWriter;
import yier.bubu.redis.app.bench.suite.SuiteMarkdownWriter;
import yier.bubu.redis.app.bench.suite.SuiteRunResult;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class YierdisBenchComparisonRenderTest {
    @Test
    public void renderComparableComparisonShowsLabelsProvenanceAndDeltas() {
        YierdisBench.BackendResult baseline = fullResult("baseline", 16378, 1000.0, 2000.0, new long[]{1_000_000, 1_500_000, 2_000_000});
        YierdisBench.BackendResult current = fullResult("current", 16379, 1250.0, 1500.0, new long[]{1_000_000, 1_250_000, 1_500_000});

        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.success(
                        "baseline",
                        Path.of("/tmp/baseline.jar"),
                        List.of("java", "-jar", "/tmp/baseline.jar", "--port", "16378"),
                        "79228e3",
                        baseline
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current",
                        Path.of("/tmp/current.jar"),
                        List.of("java", "-jar", "/tmp/current.jar", "--port", "16379"),
                        "9b9f58c",
                        current
                ),
                false,
                "commit labels supplied by benchmark operator"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("[comparison]"));
        Assert.assertTrue(rendered.contains("status: comparable"));
        Assert.assertTrue(rendered.contains("baseline jar: /tmp/baseline.jar"));
        Assert.assertTrue(rendered.contains("current jar: /tmp/current.jar"));
        Assert.assertTrue(rendered.contains("baseline commit: 79228e3"));
        Assert.assertTrue(rendered.contains("current commit: 9b9f58c"));
        Assert.assertTrue(rendered.contains("baseline command: java -jar /tmp/baseline.jar --port 16378"));
        Assert.assertTrue(rendered.contains("current command: java -jar /tmp/current.jar --port 16379"));
        Assert.assertTrue(rendered.contains("side"));
        Assert.assertTrue(rendered.contains("status"));
        Assert.assertTrue(rendered.contains("SET_QPS"));
        Assert.assertTrue(rendered.contains("SET_delta_pct"));
        Assert.assertTrue(rendered.contains("GET_QPS"));
        Assert.assertTrue(rendered.contains("GET_delta_pct"));
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("PING_delta_pct"));
        Assert.assertTrue(rendered.contains("baseline"));
        Assert.assertTrue(rendered.contains("current"));
        Assert.assertTrue(rendered.contains("1000.000"));
        Assert.assertTrue(rendered.contains("1250.000"));
        Assert.assertTrue(rendered.contains("+25.000%"));
        Assert.assertTrue(rendered.contains("2.000"));
        Assert.assertTrue(rendered.contains("1.500"));
        Assert.assertTrue(rendered.contains("-25.000%"));
    }

    @Test
    public void renderFailureComparisonMarksPairNonComparableAndSuppressesDeltas() {
        YierdisBench.BackendResult partial = new YierdisBench.BackendResult("baseline", 16378);
        partial.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 100, 0, 1.0, 100.0, Instant.parse("2026-05-16T00:00:00Z")
        );
        YierdisBench.BackendResult current = fullResult("current", 16379, 1250.0, 1500.0, new long[]{1_000_000, 1_250_000, 1_500_000});

        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.failure(
                        "baseline",
                        Path.of("/tmp/baseline.jar"),
                        List.of("java", "-jar", "/tmp/baseline.jar", "--port", "16378"),
                        "unknown",
                        partial,
                        true,
                        "SET failed: -ERR internal error"
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current",
                        Path.of("/tmp/current.jar"),
                        List.of("java", "-jar", "/tmp/current.jar", "--port", "16379"),
                        "9b9f58c",
                        current
                ),
                false,
                "baseline artifact commit cannot be tied to this workspace; historical baseline failed in this environment"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("status: non-comparable"));
        Assert.assertTrue(rendered.contains("environment: baseline artifact commit cannot be tied to this workspace; historical baseline failed in this environment"));
        Assert.assertTrue(rendered.contains("baseline status: failed-partial"));
        Assert.assertTrue(rendered.contains("baseline failure: SET failed: -ERR internal error"));
        Assert.assertTrue(rendered.contains("current status: ok"));
        Assert.assertTrue(rendered.contains("n/a"));
        Assert.assertFalse(rendered.contains("+25.000%"));
    }

    @Test
    public void renderFailureComparisonIncludesRedisSpecificNonComparableReason() {
        YierdisBench.BackendResult baseline = fullResult("redis", 16378, 1000.0, 2000.0, new long[]{1_000_000, 1_500_000, 2_000_000});
        YierdisBench.BackendResult current = fullResult("current", 16379, 1250.0, 1500.0, new long[]{1_000_000, 1_250_000, 1_500_000});

        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.failure(
                        "redis",
                        Path.of("/tmp/redis"),
                        List.of("redis-server", "--port", "16378"),
                        "redis-7.2.0",
                        baseline,
                        true,
                        "comparison skipped: external Redis config required"
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current",
                        Path.of("/tmp/current.jar"),
                        List.of("java", "-jar", "/tmp/current.jar", "--port", "16379"),
                        "9b9f58c",
                        current
                ),
                false,
                "external Redis config required"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("status: non-comparable"));
        Assert.assertTrue(rendered.contains("environment: external Redis config required"));
        Assert.assertTrue(rendered.contains("baseline jar: /tmp/redis"));
        Assert.assertTrue(rendered.contains("baseline command: redis-server --port 16378"));
        Assert.assertTrue(rendered.contains("baseline commit: redis-7.2.0"));
        Assert.assertTrue(rendered.contains("baseline status: failed-partial"));
        Assert.assertTrue(rendered.contains("baseline failure: comparison skipped: external Redis config required"));
        Assert.assertTrue(rendered.contains("current status: ok"));
        Assert.assertTrue(rendered.contains("n/a"));
    }

    @Test
    public void renderSkipLatencyComparisonOmitsLatencyColumns() {
        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.success(
                        "baseline", Path.of("/tmp/baseline.jar"), List.of("java", "-jar", "/tmp/baseline.jar"), "unknown", throughputOnly("baseline", 16378, 1000.0)
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current", Path.of("/tmp/current.jar"), List.of("java", "-jar", "/tmp/current.jar"), "unknown", throughputOnly("current", 16379, 1200.0)
                ),
                true,
                "commit labels unavailable for supplied artifacts"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("status: comparable"));
        Assert.assertTrue(rendered.contains("SET_delta_pct"));
        Assert.assertFalse(rendered.contains("PING_p95(ms)"));
        Assert.assertFalse(rendered.contains("PING_delta_pct"));
        Assert.assertTrue(rendered.contains("commit labels unavailable for supplied artifacts"));
    }

    @Test
    public void comparisonsCsvIncludesRedisBaselineLabelAndReason() {
        SuiteRunResult result = RedisSuiteTestSupport.redisComparisonResult(false, "external Redis config required");

        String csv = SuiteCsvWriter.comparisonsCsv(result);

        Assert.assertTrue(csv.contains("baseline_artifact,current_artifact"));
        Assert.assertTrue(csv.contains("redis,current"));
        Assert.assertTrue(csv.contains("external Redis config required"));
    }

    @Test
    public void markdownReportIncludesRedisSummarySection() {
        SuiteRunResult result = RedisSuiteTestSupport.redisComparisonResult(true, "");

        String markdown = SuiteMarkdownWriter.write(result);

        Assert.assertTrue(markdown.contains("## Redis Comparison Summary"));
        Assert.assertTrue(markdown.contains("redis -> current"));
    }

    private static YierdisBench.BackendResult fullResult(String label, int port, double setQps, double getQps, long[] latencyNanos) {
        YierdisBench.BackendResult result = throughputOnly(label, port, setQps);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, getQps, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, setQps / 2.0, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, setQps / 4.0, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, setQps / 5.0, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, getQps / 2.0, 0);
        result.pingLatency = latency(YierdisBench.Workload.PING, 0, latencyNanos);
        result.setLatency = latency(YierdisBench.Workload.SET_RANDOM, 0, latencyNanos);
        result.getLatency = latency(YierdisBench.Workload.GET_RANDOM, 0, latencyNanos);
        result.appendLatency = latency(YierdisBench.Workload.APPEND, 0, latencyNanos);
        result.pfaddSparseLatency = latency(YierdisBench.Workload.PFADD_SPARSE, 0, latencyNanos);
        result.pfaddDenseLatency = latency(YierdisBench.Workload.PFADD_DENSE, 0, latencyNanos);
        return result;
    }

    private static YierdisBench.BackendResult throughputOnly(String label, int port, double setQps) {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult(label, port);
        result.setThroughput = throughput(YierdisBench.Workload.SET_RANDOM, setQps, 0);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, setQps * 2.0, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, setQps / 2.0, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, setQps / 4.0, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, setQps / 5.0, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, setQps, 0);
        return result;
    }

    private static YierdisBench.ThroughputResult throughput(YierdisBench.Workload workload, double qps, long errors) {
        return new YierdisBench.ThroughputResult(
                workload, 1000, errors, 1.0, qps, Instant.parse("2026-05-16T00:00:00Z")
        );
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors) {
        return latency(workload, errors, new long[]{1_000_000, 1_500_000, 2_000_000});
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors, long[] sortedNanos) {
        return new YierdisBench.LatencyResult(
                workload, 1000, errors, 1.0, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(sortedNanos)
        );
    }
}
