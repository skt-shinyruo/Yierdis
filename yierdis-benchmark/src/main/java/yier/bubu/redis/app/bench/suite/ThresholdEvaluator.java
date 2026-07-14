package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.List;

public final class ThresholdEvaluator {
    private ThresholdEvaluator() {
    }

    public static List<ThresholdFinding> evaluate(ScenarioComparison comparison, ThresholdPolicy policy) {
        List<ThresholdFinding> findings = new ArrayList<>();
        String scenarioId = comparison.scenario().id();
        if (comparison.scenario().comparisonRole() == ScenarioDefinition.ComparisonRole.DIAGNOSTIC) {
            addDiagnosticFindings(findings, scenarioId, comparison);
            return List.copyOf(findings);
        }
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
        addProductionHardeningMedianQpsGateFinding(findings, comparison, policy);
        addLatencyFinding(findings, comparison, policy, "p95_ms");
        addLatencyFinding(findings, comparison, policy, "p99_ms");
        return List.copyOf(findings);
    }

    private static void addDiagnosticFindings(
            List<ThresholdFinding> findings,
            String scenarioId,
            ScenarioComparison comparison
    ) {
        if (!comparison.comparable()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, scenarioId, "diagnostic_comparability",
                    comparison.baseline().artifactLabel() + " vs " + comparison.current().artifactLabel()
                            + ": " + comparison.nonComparableReason()));
        }
        addDiagnosticErrorFinding(findings, scenarioId, comparison.baseline());
        addDiagnosticErrorFinding(findings, scenarioId, comparison.current());
    }

    private static void addErrorFinding(List<ThresholdFinding> findings, String scenarioId, ScenarioPassResult pass) {
        MetricSummary errors = pass.summaries().get("errors");
        if (errors != null && errors.max() > 0.0) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenarioId, "errors",
                    pass.artifactLabel() + " recorded benchmark errors: max=" + format(errors.max())));
        }
    }

    private static void addDiagnosticErrorFinding(List<ThresholdFinding> findings, String scenarioId, ScenarioPassResult pass) {
        MetricSummary errors = pass.summaries().get("errors");
        if (errors != null && errors.max() > 0.0) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, scenarioId, "diagnostic_errors",
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

    private static void addProductionHardeningMedianQpsGateFinding(
            List<ThresholdFinding> findings,
            ScenarioComparison comparison,
            ThresholdPolicy policy
    ) {
        if (comparison.scenario().comparisonRole()
                != ScenarioDefinition.ComparisonRole.PRODUCTION_HARDENING_MEDIAN_QPS_GATE) {
            return;
        }
        MetricSummary baseline = comparison.baseline().summaries().get("qps");
        MetricSummary current = comparison.current().summaries().get("qps");
        if (baseline == null || current == null || baseline.median() == 0.0) {
            return;
        }
        double ratio = current.median() / baseline.median();
        if (ratio < policy.productionHardeningMinimumMedianQpsRatio()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, comparison.scenario().id(), "median_qps_ratio",
                    "median QPS ratio " + formatRatio(ratio) + " is below required "
                            + formatRatio(policy.productionHardeningMinimumMedianQpsRatio())));
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

    private static String formatRatio(double value) {
        return String.format(java.util.Locale.ROOT, "%.4f", value);
    }
}
