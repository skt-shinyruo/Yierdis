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
        nonComparableReason = nonComparableReason == null ? "" : nonComparableReason;
        deltaPercentByMetric = deltaPercentByMetric == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(deltaPercentByMetric));
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
        if (!scenario.equals(baseline.scenario())) {
            reasons.add("baseline scenario " + baseline.scenario().id() + " does not match " + scenario.id());
        }
        if (!scenario.equals(current.scenario())) {
            reasons.add("current scenario " + current.scenario().id() + " does not match " + scenario.id());
        }
        if (!baseline.clean()) {
            reasons.add(baseline.artifactLabel() + " is not clean" + failureSuffix(baseline));
        }
        if (!current.clean()) {
            reasons.add(current.artifactLabel() + " is not clean" + failureSuffix(current));
        }
        return String.join("; ", reasons);
    }

    private static String failureSuffix(ScenarioPassResult pass) {
        return pass.failureMessage().isBlank() ? "" : ": " + pass.failureMessage();
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
