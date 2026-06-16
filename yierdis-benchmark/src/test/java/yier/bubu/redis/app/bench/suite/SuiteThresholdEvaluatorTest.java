package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.List;

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

    private static void assertFinding(List<ThresholdFinding> findings, String metric, ThresholdFinding.Level level) {
        for (ThresholdFinding finding : findings) {
            if (finding.metric().equals(metric) && finding.level() == level) {
                return;
            }
        }
        Assert.fail("missing " + level + " finding for " + metric + " in " + findings);
    }
}
