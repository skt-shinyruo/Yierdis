package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.List;

public final class ThresholdEvaluator {
    private ThresholdEvaluator() {
    }

    public static List<ThresholdFinding> evaluate(ScenarioComparison comparison, ThresholdPolicy policy) {
        List<ThresholdFinding> findings = new ArrayList<>();
        String scenarioId = comparison.scenario().id();
        if (!comparison.comparable()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenarioId, "comparability",
                    comparison.baseline().artifactLabel() + " vs " + comparison.current().artifactLabel()
                            + ": " + comparison.nonComparableReason()));
        }
        addErrorFinding(findings, scenarioId, comparison.baseline());
        addErrorFinding(findings, scenarioId, comparison.current());
        if (!comparison.comparable()) {
            return List.copyOf(findings);
        }

        addQpsFinding(findings, comparison, policy);
        addLatencyFinding(findings, comparison, policy, "p95_ms");
        addLatencyFinding(findings, comparison, policy, "p99_ms");
        return List.copyOf(findings);
    }

    private static void addErrorFinding(List<ThresholdFinding> findings, String scenarioId, ScenarioPassResult pass) {
        MetricSummary errors = pass.summaries().get("errors");
        if (errors != null && errors.max() > 0.0) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenarioId, "errors",
                    pass.artifactLabel() + " recorded benchmark errors: max=" + format(errors.max())));
        }
    }

    private static void addQpsFinding(List<ThresholdFinding> findings, ScenarioComparison comparison, ThresholdPolicy policy) {
        Double delta = comparison.deltaPercentByMetric().get("qps");
        if (delta != null && delta < -policy.qpsDropPercent()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, comparison.scenario().id(), "qps",
                    "QPS decreased by " + format(delta) + "%"));
        }
    }

    private static void addLatencyFinding(List<ThresholdFinding> findings, ScenarioComparison comparison, ThresholdPolicy policy, String metric) {
        Double delta = comparison.deltaPercentByMetric().get(metric);
        if (delta != null && delta > policy.latencyIncreasePercent()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, comparison.scenario().id(), metric,
                    metric + " increased by " + format(delta) + "%"));
        }
    }

    private static String format(double value) {
        return String.format(java.util.Locale.ROOT, "%.3f", value);
    }
}
