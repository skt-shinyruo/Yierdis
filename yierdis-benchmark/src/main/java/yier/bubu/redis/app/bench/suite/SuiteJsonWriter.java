package yier.bubu.redis.app.bench.suite;

import java.util.List;
import java.util.Map;

public final class SuiteJsonWriter {
    private SuiteJsonWriter() {
    }

    public static String write(SuiteRunResult result) {
        Json out = new Json();
        out.objectStart();
        out.name("runId").string(result.runId()).comma();
        out.name("profile").string(result.profile().cliName()).comma();
        out.name("startedAt").string(result.startedAt().toString()).comma();
        out.name("finishedAt").string(result.finishedAt().toString()).comma();
        out.name("environment");
        writeEnvironment(out, result.environment());
        out.comma();
        out.name("artifacts");
        writeArtifacts(out, result);
        out.comma();
        out.name("scenarios");
        writeScenarios(out, result);
        out.comma();
        out.name("passes");
        writePasses(out, result);
        out.comma();
        out.name("comparisons");
        writeComparisons(out, result);
        out.comma();
        out.name("findings");
        writeFindings(out, result);
        out.objectEnd();
        out.newline();
        return out.toString();
    }

    private static void writeEnvironment(Json out, SuiteEnvironment environment) {
        writeStringMap(out, environment.values());
    }

    private static void writeArtifacts(Json out, SuiteRunResult result) {
        out.arrayStart();
        for (int i = 0; i < result.artifacts().size(); i++) {
            SuiteArtifact artifact = result.artifacts().get(i);
            out.objectStart();
            out.name("label").string(artifact.label()).comma();
            out.name("jarPath").string(artifact.jarPath().toString()).comma();
            out.name("commitLabel").string(artifact.commitLabel());
            out.objectEnd();
            if (i + 1 < result.artifacts().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeScenarios(Json out, SuiteRunResult result) {
        out.arrayStart();
        for (int i = 0; i < result.scenarios().size(); i++) {
            writeScenario(out, result.scenarios().get(i));
            if (i + 1 < result.scenarios().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writePasses(Json out, SuiteRunResult result) {
        out.arrayStart();
        for (int i = 0; i < result.passes().size(); i++) {
            ScenarioPassResult pass = result.passes().get(i);
            out.objectStart();
            out.name("artifact").string(pass.artifactLabel()).comma();
            out.name("scenario").string(pass.scenario().id()).comma();
            out.name("failed").bool(pass.failed()).comma();
            out.name("failureMessage").string(pass.failureMessage()).comma();
            out.name("before");
            writeStringMap(out, pass.before().values());
            out.comma();
            out.name("after");
            writeStringMap(out, pass.after().values());
            out.comma();
            out.name("summaries");
            writeSummaries(out, pass);
            out.comma();
            out.name("iterations");
            writeIterations(out, pass);
            out.objectEnd();
            if (i + 1 < result.passes().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeComparisons(Json out, SuiteRunResult result) {
        out.arrayStart();
        for (int i = 0; i < result.comparisons().size(); i++) {
            ScenarioComparison comparison = result.comparisons().get(i);
            out.objectStart();
            out.name("scenario").string(comparison.scenario().id()).comma();
            out.name("baseline").string(comparison.baseline().artifactLabel()).comma();
            out.name("current").string(comparison.current().artifactLabel()).comma();
            out.name("comparable").bool(comparison.comparable()).comma();
            out.name("nonComparableReason").string(comparison.nonComparableReason()).comma();
            out.name("deltas");
            out.objectStart();
            int index = 0;
            for (Map.Entry<String, Double> entry : comparison.deltaPercentByMetric().entrySet()) {
                out.name(entry.getKey()).number(entry.getValue());
                if (++index < comparison.deltaPercentByMetric().size()) {
                    out.comma();
                }
            }
            out.objectEnd();
            out.objectEnd();
            if (i + 1 < result.comparisons().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeFindings(Json out, SuiteRunResult result) {
        out.arrayStart();
        for (int i = 0; i < result.findings().size(); i++) {
            ThresholdFinding finding = result.findings().get(i);
            out.objectStart();
            out.name("level").string(finding.level().name()).comma();
            out.name("scenarioId").string(finding.scenarioId()).comma();
            out.name("metric").string(finding.metric()).comma();
            out.name("message").string(finding.message());
            out.objectEnd();
            if (i + 1 < result.findings().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeScenario(Json out, ScenarioDefinition scenario) {
        out.objectStart();
        out.name("id").string(scenario.id()).comma();
        out.name("displayName").string(scenario.displayName()).comma();
        out.name("workload").string(scenario.workload().name()).comma();
        out.name("keyspace").number(scenario.keyspace()).comma();
        out.name("dataSize").number(scenario.dataSize()).comma();
        out.name("requests").number(scenario.requests()).comma();
        out.name("clients").number(scenario.clients()).comma();
        out.name("pipeline").number(scenario.pipeline()).comma();
        out.name("warmupIterations").number(scenario.warmupIterations()).comma();
        out.name("repeatIterations").number(scenario.repeatIterations()).comma();
        out.name("latency").bool(scenario.latency());
        out.objectEnd();
    }

    private static void writeSummaries(Json out, ScenarioPassResult pass) {
        out.arrayStart();
        int index = 0;
        for (MetricSummary summary : pass.summaries().values()) {
            out.objectStart();
            out.name("metric").string(summary.name()).comma();
            out.name("sampleCount").number(summary.sampleCount()).comma();
            out.name("min").number(summary.min()).comma();
            out.name("median").number(summary.median()).comma();
            out.name("mean").number(summary.mean()).comma();
            out.name("max").number(summary.max());
            out.objectEnd();
            if (++index < pass.summaries().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeIterations(Json out, ScenarioPassResult pass) {
        out.arrayStart();
        for (int i = 0; i < pass.iterations().size(); i++) {
            IterationResult iteration = pass.iterations().get(i);
            out.objectStart();
            out.name("kind").string(iteration.kind().name()).comma();
            out.name("index").number(iteration.index()).comma();
            out.name("metrics");
            writeIterationMetrics(out, iteration.metrics());
            out.objectEnd();
            if (i + 1 < pass.iterations().size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeIterationMetrics(Json out, List<SuiteMetric> metrics) {
        out.arrayStart();
        for (int i = 0; i < metrics.size(); i++) {
            SuiteMetric metric = metrics.get(i);
            out.objectStart();
            out.name("metric").string(metric.name()).comma();
            out.name("value").number(metric.value());
            out.objectEnd();
            if (i + 1 < metrics.size()) {
                out.comma();
            }
        }
        out.arrayEnd();
    }

    private static void writeStringMap(Json out, Map<String, String> values) {
        out.objectStart();
        List<String> keys = values.keySet().stream().sorted().toList();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            out.name(key).string(values.get(key));
            if (i + 1 < keys.size()) {
                out.comma();
            }
        }
        out.objectEnd();
    }

    static String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }

    private static final class Json {
        private final StringBuilder out = new StringBuilder();

        void objectStart() {
            out.append('{');
        }

        void objectEnd() {
            out.append('}');
        }

        void arrayStart() {
            out.append('[');
        }

        void arrayEnd() {
            out.append(']');
        }

        void comma() {
            out.append(',');
        }

        void newline() {
            out.append('\n');
        }

        Json name(String name) {
            string(name);
            out.append(':');
            return this;
        }

        Json string(String value) {
            out.append('"').append(escape(value)).append('"');
            return this;
        }

        Json number(double value) {
            out.append(String.format(java.util.Locale.ROOT, "%.3f", value));
            return this;
        }

        Json number(int value) {
            out.append(value);
            return this;
        }

        Json bool(boolean value) {
            out.append(value);
            return this;
        }

        @Override
        public String toString() {
            return out.toString();
        }
    }
}
