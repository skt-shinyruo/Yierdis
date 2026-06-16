package yier.bubu.redis.app.bench.suite;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ScenarioPassResult(
        String artifactLabel,
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
        Objects.requireNonNull(scenario, "scenario");
        failureMessage = failureMessage == null ? "" : failureMessage;
        iterations = iterations == null ? List.of() : List.copyOf(iterations);
        before = before == null ? ObservationSnapshot.empty() : before;
        after = after == null ? ObservationSnapshot.empty() : after;
        summaries = summaries == null
                ? MetricSummary.summarizeRepeats(iterations)
                : Collections.unmodifiableMap(new LinkedHashMap<>(summaries));
    }

    public static ScenarioPassResult completed(
            String artifactLabel,
            ScenarioDefinition scenario,
            List<IterationResult> iterations,
            ObservationSnapshot before,
            ObservationSnapshot after
    ) {
        return new ScenarioPassResult(artifactLabel, scenario, false, "", iterations, before, after, null);
    }

    public static ScenarioPassResult failed(String artifactLabel, ScenarioDefinition scenario, String failureMessage) {
        return new ScenarioPassResult(artifactLabel, scenario, true, failureMessage, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty(), Map.of());
    }

    public boolean clean() {
        if (failed) {
            return false;
        }
        if (summaries.isEmpty()) {
            return false;
        }
        MetricSummary errors = summaries.get("errors");
        return errors == null || errors.max() == 0.0;
    }
}
