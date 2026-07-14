package yier.bubu.redis.app.bench.suite;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record SuiteRunResult(
        String runId,
        SuiteProfileName profile,
        Instant startedAt,
        Instant finishedAt,
        SuiteEnvironment environment,
        List<SuiteArtifact> artifacts,
        List<ScenarioDefinition> scenarios,
        List<ScenarioPassResult> passes,
        List<ScenarioComparison> comparisons,
        List<ThresholdFinding> findings
) {
    public SuiteRunResult {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(finishedAt, "finishedAt");
        Objects.requireNonNull(environment, "environment");
        if (finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not be before startedAt");
        }
        artifacts = artifacts == null ? List.of() : List.copyOf(artifacts);
        scenarios = scenarios == null ? List.of() : List.copyOf(scenarios);
        passes = passes == null ? List.of() : List.copyOf(passes);
        comparisons = comparisons == null ? List.of() : List.copyOf(comparisons);
        findings = findings == null ? List.of() : List.copyOf(findings);
    }

    public boolean hasCriticalFindings() {
        return findings.stream().anyMatch(finding -> finding.level() == ThresholdFinding.Level.CRITICAL);
    }
}
