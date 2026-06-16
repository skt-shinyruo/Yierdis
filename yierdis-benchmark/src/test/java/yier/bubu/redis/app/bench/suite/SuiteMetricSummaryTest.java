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
        ScenarioPassResult clean = ScenarioPassResult.completed("current", scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult dirty = ScenarioPassResult.completed("current", scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 2.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult failed = ScenarioPassResult.failed("current", scenario, "server did not become ready");

        Assert.assertTrue(clean.clean());
        Assert.assertFalse(dirty.clean());
        Assert.assertFalse(failed.clean());
    }

    @Test
    public void completedPassWithoutRepeatSummariesIsDirty() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);

        ScenarioPassResult empty = ScenarioPassResult.completed("current", scenario, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult warmupOnly = ScenarioPassResult.completed("current", scenario, List.of(
                IterationResult.warmup(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult repeatWithoutMetrics = ScenarioPassResult.completed("current", scenario, List.of(
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
    public void passResultCopiesAutoComputedSummaries() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult pass = ScenarioPassResult.completed("current", scenario, List.of(
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
}
