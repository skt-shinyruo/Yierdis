package yier.bubu.redis.app.bench.suite;

import java.util.Map;

public final class SuiteMarkdownWriter {
    private SuiteMarkdownWriter() {
    }

    public static String write(SuiteRunResult result) {
        StringBuilder out = new StringBuilder();
        out.append("# Yierdis Benchmark Suite Report\n\n");
        out.append("## Run\n\n");
        out.append("| Field | Value |\n");
        out.append("| --- | --- |\n");
        row(out, "Run ID", result.runId());
        row(out, "Profile", result.profile().cliName());
        row(out, "Started At", result.startedAt().toString());
        row(out, "Finished At", result.finishedAt().toString());
        out.append('\n');

        out.append("## Environment\n\n");
        out.append("| Key | Value |\n");
        out.append("| --- | --- |\n");
        Map<String, String> environment = result.environment().values();
        for (String key : environment.keySet().stream().sorted().toList()) {
            row(out, key, environment.get(key));
        }
        out.append('\n');

        out.append("## Scenarios\n\n");
        out.append("| Scenario | Display Name | Workload | Requests | Clients | Pipeline | Latency |\n");
        out.append("| --- | --- | --- | ---: | ---: | ---: | --- |\n");
        for (ScenarioDefinition scenario : result.scenarios()) {
            row(out, scenario.id(), scenario.displayName(), scenario.workload().name(),
                    Integer.toString(scenario.requests()), Integer.toString(scenario.clients()),
                    Integer.toString(scenario.pipeline()), Boolean.toString(scenario.latency()));
        }
        out.append('\n');

        out.append("## Findings\n\n");
        out.append("| Level | Scenario | Metric | Message |\n");
        out.append("| --- | --- | --- | --- |\n");
        if (result.findings().isEmpty()) {
            row(out, "OK", "", "", "No threshold findings");
        } else {
            for (ThresholdFinding finding : result.findings()) {
                row(out, finding.level().name(), finding.scenarioId(), finding.metric(), finding.message());
            }
        }
        out.append('\n');

        out.append("## Scenario Metrics\n\n");
        out.append("| Artifact | Scenario | Group | Metric | Samples | Min | Median | Mean | Max |\n");
        out.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
        for (ScenarioPassResult pass : result.passes()) {
            for (MetricSummary summary : pass.summaries().values()) {
                row(out, pass.artifactLabel(), pass.scenario().id(), "repeat", summary.name(),
                        Integer.toString(summary.sampleCount()), SuiteCsvWriter.number(summary.min()),
                        SuiteCsvWriter.number(summary.median()), SuiteCsvWriter.number(summary.mean()),
                        SuiteCsvWriter.number(summary.max()));
            }
        }
        out.append('\n');

        out.append("## Comparisons\n\n");
        out.append("| Scenario | Metric | Baseline | Current | Delta % | Status |\n");
        out.append("| --- | --- | ---: | ---: | ---: | --- |\n");
        for (ScenarioComparison comparison : result.comparisons()) {
            if (!comparison.comparable()) {
                row(out, comparison.scenario().id(), "comparability", "", "", "",
                        "non-comparable: " + comparison.nonComparableReason());
                continue;
            }
            for (Map.Entry<String, Double> entry : comparison.deltaPercentByMetric().entrySet()) {
                String metric = entry.getKey();
                MetricSummary baseline = comparison.baseline().summaries().get(metric);
                MetricSummary current = comparison.current().summaries().get(metric);
                if (baseline == null || current == null) {
                    continue;
                }
                row(out, comparison.scenario().id(), metric, SuiteCsvWriter.number(baseline.mean()),
                        SuiteCsvWriter.number(current.mean()), SuiteCsvWriter.number(entry.getValue()),
                        comparisonStatus(metric, entry.getValue()).toUpperCase(java.util.Locale.ROOT));
            }
        }
        out.append('\n');

        if (result.artifacts().stream().anyMatch(artifact -> artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS)) {
            out.append("## Redis Comparison Summary\n\n");
            for (ScenarioComparison comparison : result.comparisons()) {
                if (comparison.baseline().artifactKind() == SuiteArtifact.Kind.EXTERNAL_REDIS) {
                    out.append("- ")
                            .append(escape(comparison.baseline().artifactLabel()))
                            .append(" -> ")
                            .append(escape(comparison.current().artifactLabel()))
                            .append('\n');
                }
            }
            out.append("- External Redis configuration is operator-managed.\n");
        }
        return out.toString();
    }

    private static String comparisonStatus(String metric, double deltaPercent) {
        ThresholdPolicy defaults = ThresholdPolicy.defaults();
        if ("qps".equals(metric) && deltaPercent < -defaults.qpsDropPercent()) {
            return "warning";
        }
        if (("p95_ms".equals(metric) || "p99_ms".equals(metric)) && deltaPercent > defaults.latencyIncreasePercent()) {
            return "warning";
        }
        return "ok";
    }

    private static void row(StringBuilder out, String... cells) {
        out.append('|');
        for (String cell : cells) {
            out.append(' ').append(escape(cell)).append(" |");
        }
        out.append('\n');
    }

    static String escape(String value) {
        String text = value == null ? "" : value;
        return text
                .replace("\\", "\\\\")
                .replace("|", "\\|")
                .replace("\r\n", "\n")
                .replace("\r", "\n")
                .replace("\n", "<br>");
    }
}
