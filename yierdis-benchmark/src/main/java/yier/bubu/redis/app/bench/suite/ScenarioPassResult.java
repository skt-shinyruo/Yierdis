package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ScenarioPassResult(
        String artifactLabel,
        SuiteArtifact.Kind artifactKind,
        ScenarioDefinition scenario,
        boolean failed,
        String failureMessage,
        List<IterationResult> iterations,
        ObservationSnapshot before,
        ObservationSnapshot after,
        Map<String, MetricSummary> summaries
) {
    public ScenarioPassResult {
        Objects.requireNonNull(artifactLabel, "artifactLabel");
        Objects.requireNonNull(artifactKind, "artifactKind");
        Objects.requireNonNull(scenario, "scenario");
        failureMessage = failureMessage == null ? "" : failureMessage;
        iterations = iterations == null ? List.of() : List.copyOf(iterations);
        before = before == null ? ObservationSnapshot.empty() : before;
        after = after == null ? ObservationSnapshot.empty() : after;
        Map<String, MetricSummary> computedSummaries = MetricSummary.summarizeRepeats(iterations);
        summaries = summaries == null ? computedSummaries : copySummaries(summaries);
        if (!summaries.equals(computedSummaries)) {
            throw new IllegalArgumentException("supplied summaries must match repeat iteration summaries");
        }
    }

    public static ScenarioPassResult completed(
            String artifactLabel,
            SuiteArtifact.Kind artifactKind,
            ScenarioDefinition scenario,
            List<IterationResult> iterations,
            ObservationSnapshot before,
            ObservationSnapshot after
    ) {
        return new ScenarioPassResult(artifactLabel, artifactKind, scenario, false, "", iterations, before, after, null);
    }

    public static ScenarioPassResult failed(String artifactLabel, SuiteArtifact.Kind artifactKind,
                                            ScenarioDefinition scenario, String failureMessage) {
        return new ScenarioPassResult(artifactLabel, artifactKind, scenario, true, failureMessage, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty(), Map.of());
    }

    public boolean clean() {
        if (failed) {
            return false;
        }
        if (summaries.isEmpty()) {
            return false;
        }
        if (!missingRequiredMetrics().isEmpty()) {
            return false;
        }
        MetricSummary errors = summaries.get("errors");
        return errors == null || errors.max() == 0.0;
    }

    public List<String> missingRequiredMetrics() {
        if (failed) {
            return List.of();
        }
        List<String> missing = new ArrayList<>();
        requireMetric(missing, "qps");
        requireMetric(missing, "errors");
        if (scenario.latency()) {
            requireMetric(missing, "p95_ms");
            requireMetric(missing, "p99_ms");
        }
        return List.copyOf(missing);
    }

    private void requireMetric(List<String> missing, String metric) {
        if (!summaries.containsKey(metric)) {
            missing.add(metric);
        }
    }

    private static Map<String, MetricSummary> copySummaries(Map<String, MetricSummary> source) {
        LinkedHashMap<String, MetricSummary> copy = new LinkedHashMap<>();
        for (Map.Entry<String, MetricSummary> entry : source.entrySet()) {
            MetricSummary summary = Objects.requireNonNull(entry.getValue(), "summary");
            if (!entry.getKey().equals(summary.name())) {
                throw new IllegalArgumentException("summary map key must match summary name: " + entry.getKey());
            }
            copy.put(entry.getKey(), summary);
        }
        return Collections.unmodifiableMap(copy);
    }
}
