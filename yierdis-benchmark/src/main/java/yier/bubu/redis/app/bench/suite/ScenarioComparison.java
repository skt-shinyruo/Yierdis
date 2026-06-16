package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ScenarioComparison(
        ScenarioDefinition scenario,
        ScenarioPassResult baseline,
        ScenarioPassResult current,
        boolean comparable,
        String nonComparableReason,
        Map<String, Double> deltaPercentByMetric
) {
    public ScenarioComparison {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(nonComparableReason, "nonComparableReason");
        Objects.requireNonNull(deltaPercentByMetric, "deltaPercentByMetric");
        String expectedReason = nonComparableReason(scenario, baseline, current);
        boolean expectedComparable = expectedReason.isEmpty();
        Map<String, Double> expectedDeltas = expectedComparable ? deltas(baseline, current) : Map.of();
        Map<String, Double> suppliedDeltas = Collections.unmodifiableMap(new LinkedHashMap<>(deltaPercentByMetric));
        if (comparable != expectedComparable) {
            throw new IllegalArgumentException("comparable state does not match scenario comparison inputs");
        }
        if (!nonComparableReason.equals(expectedReason)) {
            throw new IllegalArgumentException("nonComparableReason does not match scenario comparison inputs");
        }
        if (!suppliedDeltas.equals(expectedDeltas)) {
            throw new IllegalArgumentException("deltaPercentByMetric does not match scenario comparison inputs");
        }
        deltaPercentByMetric = suppliedDeltas;
    }

    public static ScenarioComparison compare(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current) {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        String reason = nonComparableReason(scenario, baseline, current);
        boolean comparable = reason.isEmpty();
        Map<String, Double> deltas = comparable ? deltas(baseline, current) : Map.of();
        return new ScenarioComparison(scenario, baseline, current, comparable, reason, deltas);
    }

    private static String nonComparableReason(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current) {
        List<String> reasons = new ArrayList<>();
        addScenarioMismatch(reasons, "baseline", scenario, baseline.scenario());
        addScenarioMismatch(reasons, "current", scenario, current.scenario());
        if (!baseline.clean()) {
            reasons.add(baseline.artifactLabel() + " is not clean" + dirtySuffix(baseline));
        }
        if (!current.clean()) {
            reasons.add(current.artifactLabel() + " is not clean" + dirtySuffix(current));
        }
        addZeroBaselineDenominator(reasons, scenario, baseline, "qps");
        if (scenario.latency()) {
            addZeroBaselineDenominator(reasons, scenario, baseline, "p95_ms");
            addZeroBaselineDenominator(reasons, scenario, baseline, "p99_ms");
        }
        return String.join("; ", reasons);
    }

    private static void addScenarioMismatch(List<String> reasons, String side, ScenarioDefinition expected, ScenarioDefinition actual) {
        if (!expected.id().equals(actual.id())) {
            reasons.add(side + " scenario id " + actual.id() + " does not match " + expected.id());
            return;
        }
        addFieldMismatch(reasons, side, "workload", expected.workload(), actual.workload());
        addFieldMismatch(reasons, side, "keyspace", expected.keyspace(), actual.keyspace());
        addFieldMismatch(reasons, side, "dataSize", expected.dataSize(), actual.dataSize());
        addFieldMismatch(reasons, side, "requests", expected.requests(), actual.requests());
        addFieldMismatch(reasons, side, "clients", expected.clients(), actual.clients());
        addFieldMismatch(reasons, side, "pipeline", expected.pipeline(), actual.pipeline());
        addFieldMismatch(reasons, side, "warmupIterations", expected.warmupIterations(), actual.warmupIterations());
        addFieldMismatch(reasons, side, "repeatIterations", expected.repeatIterations(), actual.repeatIterations());
        addFieldMismatch(reasons, side, "latency", expected.latency(), actual.latency());
    }

    private static void addFieldMismatch(List<String> reasons, String side, String field, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            reasons.add(side + " scenario " + field + " " + actual + " does not match " + expected);
        }
    }

    private static String dirtySuffix(ScenarioPassResult pass) {
        List<String> details = new ArrayList<>();
        if (!pass.failureMessage().isBlank()) {
            details.add(pass.failureMessage());
        }
        List<String> missing = pass.missingRequiredMetrics();
        if (!missing.isEmpty()) {
            details.add("missing required metrics " + missing);
        }
        return details.isEmpty() ? "" : ": " + String.join(", ", details);
    }

    private static void addZeroBaselineDenominator(List<String> reasons, ScenarioDefinition scenario, ScenarioPassResult baseline, String metric) {
        if (!sameExecutionShape(scenario, baseline.scenario()) || !baseline.missingRequiredMetrics().isEmpty()) {
            return;
        }
        MetricSummary summary = baseline.summaries().get(metric);
        if (summary != null && summary.mean() == 0.0) {
            reasons.add("baseline " + metric + " mean is zero");
        }
    }

    private static boolean sameExecutionShape(ScenarioDefinition expected, ScenarioDefinition actual) {
        return expected.id().equals(actual.id())
                && expected.workload() == actual.workload()
                && expected.keyspace() == actual.keyspace()
                && expected.dataSize() == actual.dataSize()
                && expected.requests() == actual.requests()
                && expected.clients() == actual.clients()
                && expected.pipeline() == actual.pipeline()
                && expected.warmupIterations() == actual.warmupIterations()
                && expected.repeatIterations() == actual.repeatIterations()
                && expected.latency() == actual.latency();
    }

    private static Map<String, Double> deltas(ScenarioPassResult baseline, ScenarioPassResult current) {
        Map<String, Double> out = new LinkedHashMap<>();
        for (Map.Entry<String, MetricSummary> entry : current.summaries().entrySet()) {
            MetricSummary base = baseline.summaries().get(entry.getKey());
            if (base == null) {
                continue;
            }
            double baseValue = base.mean();
            if (baseValue == 0.0) {
                continue;
            }
            out.put(entry.getKey(), ((entry.getValue().mean() - baseValue) * 100.0) / baseValue);
        }
        return Collections.unmodifiableMap(out);
    }
}
