package yier.bubu.redis.app.bench.suite;

import java.util.LinkedHashMap;
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
    public static ScenarioComparison compare(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current) {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        boolean comparable = baseline.clean() && current.clean();
        String reason = "";
        if (!comparable) {
            reason = baseline.clean() ? "current is not clean" : "baseline is not clean";
        }
        Map<String, Double> deltas = comparable ? deltas(baseline, current) : Map.of();
        return new ScenarioComparison(scenario, baseline, current, comparable, reason, deltas);
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
        return out;
    }
}
