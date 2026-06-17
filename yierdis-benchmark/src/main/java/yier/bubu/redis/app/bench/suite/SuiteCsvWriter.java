package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class SuiteCsvWriter {
    private static final ThresholdPolicy DEFAULT_POLICY = ThresholdPolicy.defaults();

    private SuiteCsvWriter() {
    }

    public static String metricsCsv(SuiteRunResult result) {
        StringBuilder out = new StringBuilder();
        out.append("artifact,scenario,iteration_group,metric,sample_count,min,median,mean,max\n");
        for (ScenarioPassResult pass : result.passes()) {
            for (MetricSummary summary : pass.summaries().values()) {
                appendRow(out, List.of(
                        pass.artifactLabel(),
                        pass.scenario().id(),
                        "repeat",
                        summary.name(),
                        Integer.toString(summary.sampleCount()),
                        number(summary.min()),
                        number(summary.median()),
                        number(summary.mean()),
                        number(summary.max())
                ));
            }
        }
        return out.toString();
    }

    public static String comparisonsCsv(SuiteRunResult result) {
        StringBuilder out = new StringBuilder();
        out.append("scenario,metric,baseline,current,delta_percent,status\n");
        for (ScenarioComparison comparison : result.comparisons()) {
            if (!comparison.comparable()) {
                appendRow(out, List.of(comparison.scenario().id(), "comparability", "", "", "",
                        "non-comparable"));
                continue;
            }
            for (Map.Entry<String, Double> entry : comparison.deltaPercentByMetric().entrySet()) {
                String metric = entry.getKey();
                MetricSummary baseline = comparison.baseline().summaries().get(metric);
                MetricSummary current = comparison.current().summaries().get(metric);
                if (baseline == null || current == null) {
                    continue;
                }
                appendRow(out, List.of(
                        comparison.scenario().id(),
                        metric,
                        number(baseline.mean()),
                        number(current.mean()),
                        number(entry.getValue()),
                        status(metric, entry.getValue())
                ));
            }
        }
        return out.toString();
    }

    private static String status(String metric, double deltaPercent) {
        if ("qps".equals(metric) && deltaPercent < -DEFAULT_POLICY.qpsDropPercent()) {
            return "warning";
        }
        if (("p95_ms".equals(metric) || "p99_ms".equals(metric))
                && deltaPercent > DEFAULT_POLICY.latencyIncreasePercent()) {
            return "warning";
        }
        return "ok";
    }

    static String number(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static void appendRow(StringBuilder out, List<String> cells) {
        List<String> escaped = new ArrayList<>(cells.size());
        for (String cell : cells) {
            escaped.add(escape(cell));
        }
        out.append(String.join(",", escaped)).append('\n');
    }

    static String escape(String value) {
        String text = value == null ? "" : value;
        boolean quote = text.indexOf(',') >= 0
                || text.indexOf('"') >= 0
                || text.indexOf('\n') >= 0
                || text.indexOf('\r') >= 0;
        if (!quote) {
            return text;
        }
        return "\"" + text.replace("\"", "\"\"") + "\"";
    }
}
