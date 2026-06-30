package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SuiteMetricSummaryTest {
    @Test
    public void summarizesOnlyRepeatIterations() {
        IterationResult warmup = IterationResult.warmup(0, List.of(new SuiteMetric("qps", 100.0), new SuiteMetric("errors", 0.0)));
        IterationResult repeat0 = IterationResult.repeat(0, List.of(new SuiteMetric("qps", 200.0), new SuiteMetric("errors", 0.0)));
        IterationResult repeat1 = IterationResult.repeat(1, List.of(new SuiteMetric("qps", 300.0), new SuiteMetric("errors", 1.0)));
        IterationResult repeat2 = IterationResult.repeat(2, List.of(new SuiteMetric("qps", 400.0), new SuiteMetric("errors", 0.0)));

        Map<String, MetricSummary> summaries = MetricSummary.summarizeRepeats(List.of(warmup, repeat0, repeat1, repeat2));

        MetricSummary qps = summaries.get("qps");
        Assert.assertEquals(3, qps.sampleCount());
        Assert.assertEquals(200.0, qps.min(), 0.001);
        Assert.assertEquals(300.0, qps.median(), 0.001);
        Assert.assertEquals(300.0, qps.mean(), 0.001);
        Assert.assertEquals(400.0, qps.max(), 0.001);

        MetricSummary errors = summaries.get("errors");
        Assert.assertEquals(1.0 / 3.0, errors.mean(), 0.001);
    }

    @Test
    public void passResultIsDirtyWhenFailedOrErrorsArePresent() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult clean = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 1000.0),
                        new SuiteMetric("errors", 0.0),
                        new SuiteMetric("p95_ms", 1.0),
                        new SuiteMetric("p99_ms", 2.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult dirty = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 1000.0),
                        new SuiteMetric("errors", 2.0),
                        new SuiteMetric("p95_ms", 1.0),
                        new SuiteMetric("p99_ms", 2.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult failed = ScenarioPassResult.failed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, "server did not become ready");

        Assert.assertTrue(clean.clean());
        Assert.assertFalse(dirty.clean());
        Assert.assertFalse(failed.clean());
    }

    @Test
    public void completedPassWithoutRepeatSummariesIsDirty() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);

        ScenarioPassResult empty = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult warmupOnly = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.warmup(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult repeatWithoutMetrics = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of())
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());

        Assert.assertFalse(empty.clean());
        Assert.assertFalse(warmupOnly.clean());
        Assert.assertFalse(repeatWithoutMetrics.clean());
    }

    @Test
    public void summarizesEvenCountMedianAndReturnsImmutableMap() {
        Map<String, MetricSummary> summaries = MetricSummary.summarizeRepeats(List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 100.0), new SuiteMetric("errors", 0.0))),
                IterationResult.repeat(1, List.of(new SuiteMetric("qps", 300.0), new SuiteMetric("errors", 1.0)))
        ));

        Assert.assertEquals(List.of("qps", "errors"), List.copyOf(summaries.keySet()));
        Assert.assertEquals(200.0, summaries.get("qps").median(), 0.001);
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> summaries.put("p95_ms", new MetricSummary("p95_ms", 1, 1.0, 1.0, 1.0, 1.0)));
    }

    @Test
    public void metricSummaryConstructorPreservesValueObjectInvariants() {
        Assert.assertEquals(new MetricSummary("p95_ms", 3, 1.0, 2.0, 2.5, 4.0),
                new MetricSummary("p95_ms", 3, 1.0, 2.0, 2.5, 4.0));

        assertInvalidSummary(null, 1, 0.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("QPS", 1, 0.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps_", 1, 0.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps", 0, 0.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps", -1, 0.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps", 1, Double.NaN, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps", 1, 0.0, Double.POSITIVE_INFINITY, 0.0, 0.0);
        assertInvalidSummary("qps", 1, 0.0, 0.0, Double.NEGATIVE_INFINITY, 0.0);
        assertInvalidSummary("qps", 1, 0.0, 0.0, 0.0, Double.NaN);
        assertInvalidSummary("qps", 1, -1.0, 0.0, 0.0, 0.0);
        assertInvalidSummary("qps", 1, 2.0, 1.0, 2.0, 3.0);
        assertInvalidSummary("qps", 1, 1.0, 4.0, 2.0, 3.0);
        assertInvalidSummary("qps", 1, 1.0, 2.0, 0.5, 3.0);
        assertInvalidSummary("qps", 1, 1.0, 2.0, 3.5, 3.0);
    }

    @Test
    public void passResultCopiesAutoComputedSummaries() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult pass = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());

        Assert.assertThrows(UnsupportedOperationException.class,
                () -> pass.summaries().put("p95_ms", new MetricSummary("p95_ms", 1, 1.0, 1.0, 1.0, 1.0)));
    }

    @Test
    public void suiteMetricRejectsNonFiniteValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteMetric("qps", Double.NaN));
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteMetric("qps", Double.POSITIVE_INFINITY));
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteMetric("qps", Double.NEGATIVE_INFINITY));
    }

    @Test
    public void suiteMetricAcceptsOnlyLowercaseSnakeCaseNames() {
        new SuiteMetric("qps", 1.0);
        new SuiteMetric("errors", 0.0);
        new SuiteMetric("p95_ms", 1.0);
        new SuiteMetric("p99_ms", 1.0);

        assertInvalidMetricName(null);
        assertInvalidMetricName("");
        assertInvalidMetricName("QPS");
        assertInvalidMetricName("qps-ms");
        assertInvalidMetricName("_qps");
        assertInvalidMetricName("qps_");
        assertInvalidMetricName("qps__avg");
        assertInvalidMetricName("_");
    }

    @Test
    public void completedPassRequiresCoreMetrics() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, false);

        ScenarioPassResult missingErrors = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult missingQps = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());

        Assert.assertFalse(missingErrors.clean());
        Assert.assertFalse(missingQps.clean());
    }

    @Test
    public void latencyPassRequiresLatencyMetrics() {
        ScenarioDefinition latency = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioDefinition noLatency = new ScenarioDefinition("release-ping-throughput", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, false);

        ScenarioPassResult missingP95 = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, latency, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0), new SuiteMetric("p99_ms", 2.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult missingP99 = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, latency, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0), new SuiteMetric("p95_ms", 1.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult cleanNoLatency = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, noLatency, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());

        Assert.assertFalse(missingP95.clean());
        Assert.assertFalse(missingP99.clean());
        Assert.assertTrue(cleanNoLatency.clean());
    }

    @Test
    public void suiteMetricRejectsNegativeValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteMetric("errors", -0.001));
    }

    @Test
    public void suppliedSummaryMapKeysMustMatchSummaryNames() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-throughput", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, false);
        Map<String, MetricSummary> summaries = Map.of(
                "qps", new MetricSummary("errors", 1, 0.0, 0.0, 0.0, 0.0),
                "errors", new MetricSummary("errors", 1, 0.0, 0.0, 0.0, 0.0)
        );

        Assert.assertThrows(IllegalArgumentException.class, () -> new ScenarioPassResult("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, false, "",
                List.of(), ObservationSnapshot.empty(), ObservationSnapshot.empty(), summaries));
    }

    @Test
    public void suppliedSummaryMapMustMatchRepeatSummaries() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-throughput", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, false);
        Map<String, MetricSummary> summaries = Map.of(
                "qps", new MetricSummary("qps", 1, 1000.0, 1000.0, 1000.0, 1000.0),
                "errors", new MetricSummary("errors", 1, 0.0, 0.0, 0.0, 0.0)
        );

        Assert.assertThrows(IllegalArgumentException.class, () -> new ScenarioPassResult("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, false, "",
                List.of(), ObservationSnapshot.empty(), ObservationSnapshot.empty(), summaries));
    }

    private static void assertInvalidMetricName(String name) {
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteMetric(name, 1.0));
    }

    private static void assertInvalidSummary(String name, int sampleCount, double min, double median, double mean, double max) {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new MetricSummary(name, sampleCount, min, median, mean, max));
    }
}
