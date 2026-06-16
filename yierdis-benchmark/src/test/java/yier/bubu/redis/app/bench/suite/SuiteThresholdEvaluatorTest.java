package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.List;
import java.util.Map;

public class SuiteThresholdEvaluatorTest {
    @Test
    public void marksComparableRegressionWarnings() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult current = pass("current", scenario, 850.0, 12.0, 24.0, 0.0);

        ScenarioComparison comparison = ScenarioComparison.compare(scenario, baseline, current);
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        Assert.assertTrue(comparison.comparable());
        assertFinding(findings, "qps", ThresholdFinding.Level.WARNING);
        assertFinding(findings, "p95_ms", ThresholdFinding.Level.WARNING);
        assertFinding(findings, "p99_ms", ThresholdFinding.Level.WARNING);
    }

    @Test
    public void marksErrorsAndNonComparableAsCriticalObservations() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult currentWithErrors = pass("current", scenario, 1000.0, 10.0, 20.0, 1.0);
        ScenarioPassResult failed = ScenarioPassResult.failed("current", scenario, "workload failed");

        List<ThresholdFinding> dirtyFindings = ThresholdEvaluator.evaluate(
                ScenarioComparison.compare(scenario, baseline, currentWithErrors),
                ThresholdPolicy.defaults()
        );
        assertFinding(dirtyFindings, "errors", ThresholdFinding.Level.CRITICAL);
        assertFinding(dirtyFindings, "comparability", ThresholdFinding.Level.CRITICAL);

        List<ThresholdFinding> failedFindings = ThresholdEvaluator.evaluate(
                ScenarioComparison.compare(scenario, baseline, failed),
                ThresholdPolicy.defaults()
        );
        assertFinding(failedFindings, "comparability", ThresholdFinding.Level.CRITICAL);
    }

    @Test
    public void emptyCompletedPassIsNonComparable() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult current = ScenarioPassResult.completed("current", scenario, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty());

        ScenarioComparison comparison = ScenarioComparison.compare(scenario, baseline, current);
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        Assert.assertFalse(comparison.comparable());
        Assert.assertTrue(comparison.nonComparableReason().contains("current"));
        assertFinding(findings, "comparability", ThresholdFinding.Level.CRITICAL);
    }

    @Test
    public void mismatchedScenariosAreNonComparable() {
        ScenarioDefinition expected = scenario();
        ScenarioDefinition other = new ScenarioDefinition("release-ping-latency", "PING", BenchWorkloadKind.PING,
                100, 0, 1000, 8, 4, 0, 1, true);

        ScenarioComparison baselineMismatch = ScenarioComparison.compare(expected,
                pass("baseline", other, 1000.0, 10.0, 20.0, 0.0),
                pass("current", expected, 1000.0, 10.0, 20.0, 0.0));
        ScenarioComparison currentMismatch = ScenarioComparison.compare(expected,
                pass("baseline", expected, 1000.0, 10.0, 20.0, 0.0),
                pass("current", other, 1000.0, 10.0, 20.0, 0.0));

        Assert.assertFalse(baselineMismatch.comparable());
        Assert.assertTrue(baselineMismatch.nonComparableReason().contains("baseline scenario"));
        Assert.assertFalse(currentMismatch.comparable());
        Assert.assertTrue(currentMismatch.nonComparableReason().contains("current scenario"));
    }

    @Test
    public void recordsBothDirtySidesInNonComparableReason() {
        ScenarioDefinition scenario = scenario();
        ScenarioComparison comparison = ScenarioComparison.compare(scenario,
                pass("baseline", scenario, 1000.0, 10.0, 20.0, 1.0),
                pass("current", scenario, 1000.0, 10.0, 20.0, 1.0));

        Assert.assertFalse(comparison.comparable());
        Assert.assertTrue(comparison.nonComparableReason().contains("baseline"));
        Assert.assertTrue(comparison.nonComparableReason().contains("current"));
    }

    @Test
    public void deltasAreImmutableAndThresholdBoundariesAreStrict() {
        ScenarioDefinition scenario = scenario();
        ScenarioComparison comparison = ScenarioComparison.compare(scenario,
                pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0),
                pass("current", scenario, 900.0, 11.5, 23.0, 0.0));

        Assert.assertTrue(comparison.comparable());
        Assert.assertEquals(List.of("qps", "p95_ms", "p99_ms"), List.copyOf(comparison.deltaPercentByMetric().keySet()));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> comparison.deltaPercentByMetric().put("qps", -99.0));
        Assert.assertTrue(ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults()).isEmpty());
    }

    @Test
    public void thresholdPolicyRejectsInvalidValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new ThresholdPolicy(-0.001, 15.0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ThresholdPolicy(10.0, -0.001));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ThresholdPolicy(Double.NaN, 15.0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ThresholdPolicy(10.0, Double.POSITIVE_INFINITY));
    }

    @Test
    public void missingRequiredMetricsAreNonComparableWithFindings() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult missingQps = passWithMetrics("current", scenario,
                new SuiteMetric("errors", 0.0), new SuiteMetric("p95_ms", 10.0), new SuiteMetric("p99_ms", 20.0));
        ScenarioPassResult missingErrors = passWithMetrics("current", scenario,
                new SuiteMetric("qps", 1000.0), new SuiteMetric("p95_ms", 10.0), new SuiteMetric("p99_ms", 20.0));
        ScenarioPassResult missingLatency = passWithMetrics("current", scenario,
                new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0), new SuiteMetric("p95_ms", 10.0));

        assertNonComparableFindingMentions(ScenarioComparison.compare(scenario, baseline, missingQps), "qps");
        assertNonComparableFindingMentions(ScenarioComparison.compare(scenario, baseline, missingErrors), "errors");
        assertNonComparableFindingMentions(ScenarioComparison.compare(scenario, baseline, missingLatency), "p99_ms");
    }

    @Test
    public void zeroBaselineRequiredDenominatorIsNonComparable() {
        ScenarioDefinition scenario = scenario();
        ScenarioComparison zeroQps = ScenarioComparison.compare(scenario,
                pass("baseline", scenario, 0.0, 10.0, 20.0, 0.0),
                pass("current", scenario, 1000.0, 10.0, 20.0, 0.0));
        ScenarioComparison zeroLatency = ScenarioComparison.compare(scenario,
                pass("baseline", scenario, 1000.0, 0.0, 20.0, 0.0),
                pass("current", scenario, 1000.0, 10.0, 20.0, 0.0));

        Assert.assertFalse(zeroQps.comparable());
        Assert.assertTrue(zeroQps.nonComparableReason().contains("qps"));
        Assert.assertFalse(zeroLatency.comparable());
        Assert.assertTrue(zeroLatency.nonComparableReason().contains("p95_ms"));
    }

    @Test
    public void directScenarioComparisonConstructorRejectsInconsistentState() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult dirtyCurrent = pass("current", scenario, 1000.0, 10.0, 20.0, 1.0);

        Assert.assertThrows(IllegalArgumentException.class, () -> new ScenarioComparison(
                scenario, baseline, dirtyCurrent, true, "", Map.of("qps", 0.0)));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ScenarioComparison(
                scenario, baseline, baseline, false, "", Map.of()));
    }

    @Test
    public void scenarioExecutionShapeIgnoresDisplayNameButRejectsShapeDifferences() {
        ScenarioDefinition expected = scenario();
        ScenarioDefinition renamed = new ScenarioDefinition(expected.id(), "Renamed scenario", expected.workload(),
                expected.keyspace(), expected.dataSize(), expected.requests(), expected.clients(), expected.pipeline(),
                expected.warmupIterations(), expected.repeatIterations(), expected.latency());
        ScenarioDefinition differentShape = new ScenarioDefinition(expected.id(), expected.displayName(), expected.workload(),
                expected.keyspace(), expected.dataSize(), expected.requests(), expected.clients() + 1, expected.pipeline(),
                expected.warmupIterations(), expected.repeatIterations(), expected.latency());

        ScenarioComparison renamedComparison = ScenarioComparison.compare(expected,
                pass("baseline", renamed, 1000.0, 10.0, 20.0, 0.0),
                pass("current", expected, 1000.0, 10.0, 20.0, 0.0));
        ScenarioComparison shapeComparison = ScenarioComparison.compare(expected,
                pass("baseline", differentShape, 1000.0, 10.0, 20.0, 0.0),
                pass("current", expected, 1000.0, 10.0, 20.0, 0.0));

        Assert.assertTrue(renamedComparison.comparable());
        Assert.assertFalse(shapeComparison.comparable());
        Assert.assertTrue(shapeComparison.nonComparableReason().contains("clients"));
    }

    private static ScenarioDefinition scenario() {
        return new ScenarioDefinition("release-set-get-256b-c64-p8", "SET/GET", BenchWorkloadKind.SET_GET,
                100, 256, 1000, 8, 4, 0, 1, true);
    }

    private static ScenarioPassResult pass(String artifact, ScenarioDefinition scenario, double qps, double p95, double p99, double errors) {
        return ScenarioPassResult.completed(artifact, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", qps),
                        new SuiteMetric("p95_ms", p95),
                        new SuiteMetric("p99_ms", p99),
                        new SuiteMetric("errors", errors)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
    }

    private static ScenarioPassResult passWithMetrics(String artifact, ScenarioDefinition scenario, SuiteMetric... metrics) {
        return ScenarioPassResult.completed(artifact, scenario, List.of(
                IterationResult.repeat(0, List.of(metrics))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
    }

    private static void assertFinding(List<ThresholdFinding> findings, String metric, ThresholdFinding.Level level) {
        for (ThresholdFinding finding : findings) {
            if (finding.metric().equals(metric) && finding.level() == level) {
                return;
            }
        }
        Assert.fail("missing " + level + " finding for " + metric + " in " + findings);
    }

    private static void assertNonComparableFindingMentions(ScenarioComparison comparison, String text) {
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        Assert.assertFalse(comparison.comparable());
        Assert.assertTrue(comparison.nonComparableReason().contains(text));
        assertFinding(findings, "comparability", ThresholdFinding.Level.CRITICAL);
        Assert.assertTrue(findings.toString().contains(text));
    }
}
