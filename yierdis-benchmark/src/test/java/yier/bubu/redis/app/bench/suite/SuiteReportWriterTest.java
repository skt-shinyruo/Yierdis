package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SuiteReportWriterTest {
    @Test
    public void writesAllSuiteReportArtifacts() throws Exception {
        Path reportDir = Files.createTempDirectory("suite-report-writer-test");
        SuiteRunResult result = suiteRunResult();

        SuiteReportWriter.writeAll(result, reportDir);

        Path jsonPath = reportDir.resolve("suite-result.json");
        Path metricsPath = reportDir.resolve("metrics.csv");
        Path comparisonsPath = reportDir.resolve("comparisons.csv");
        Path markdownPath = reportDir.resolve("report.md");
        Assert.assertTrue(Files.exists(jsonPath));
        Assert.assertTrue(Files.exists(metricsPath));
        Assert.assertTrue(Files.exists(comparisonsPath));
        Assert.assertTrue(Files.exists(markdownPath));

        String json = Files.readString(jsonPath, StandardCharsets.UTF_8);
        Assert.assertTrue(json.contains("\"runId\":\"run-1\""));
        Assert.assertTrue(json.contains("\"profile\":\"release\""));
        Assert.assertTrue(json.contains("\"release-ping-latency\""));
        Assert.assertTrue(json.contains("\"artifacts\""));
        Assert.assertTrue(json.contains("\"environment\""));
        Assert.assertTrue(json.contains("\"summaries\""));
        Assert.assertTrue(json.contains("\"iterations\""));
        Assert.assertTrue(json.contains("\"kind\":\"WARMUP\""));
        Assert.assertTrue(json.contains("\"kind\":\"REPEAT\""));
        Assert.assertTrue(json.contains("\"findings\""));
        Assert.assertTrue(json.contains("\"comparisons\""));

        String metricsCsv = Files.readString(metricsPath, StandardCharsets.UTF_8);
        Assert.assertTrue(metricsCsv.startsWith("artifact,scenario,row_type,iteration_kind,iteration_index,metric,sample_count,min,median,mean,max,value\n"));
        Assert.assertTrue(metricsCsv.contains("current,release-ping-latency,summary,REPEAT,,qps,1,850.000,850.000,850.000,850.000,\n"));
        Assert.assertTrue(metricsCsv.contains("current,release-ping-latency,iteration,WARMUP,0,qps,,,,,,425.000\n"));
        Assert.assertTrue(metricsCsv.contains("current,release-ping-latency,iteration,REPEAT,0,qps,,,,,,850.000\n"));

        String comparisonsCsv = Files.readString(comparisonsPath, StandardCharsets.UTF_8);
        Assert.assertTrue(comparisonsCsv.startsWith("scenario_id,baseline_artifact,current_artifact,metric,baseline_value,current_value,delta_percent,ratio,comparable,reason,status\n"));
        Assert.assertTrue(comparisonsCsv.contains("release-ping-latency,baseline,current,qps,1000.000,850.000,-15.000,0.850,true,,warning"));

        String markdown = Files.readString(markdownPath, StandardCharsets.UTF_8);
        Assert.assertTrue(markdown.contains("# Yierdis Benchmark Suite Report"));
        Assert.assertTrue(markdown.contains("release-ping-latency"));
        Assert.assertTrue(markdown.contains("WARNING"));
    }

    @Test
    public void escapesCsvJsonAndMarkdownTableContent() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING, \"latency\"\ncase",
                BenchWorkloadKind.PING, 10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult current = pass("current", scenario, 850.0, 10.0, 20.0, 0.0);
        SuiteRunResult result = new SuiteRunResult(
                "run-\"1\n",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                new SuiteEnvironment(Map.of("quote,key", "value \"x\"\nnext")),
                List.of(new SuiteArtifact("current", Path.of("/tmp/current, \"jar\".jar"), "main\nabc")),
                List.of(scenario),
                List.of(current),
                List.of(),
                List.of(new ThresholdFinding(ThresholdFinding.Level.WARNING, scenario.id(), "qps", "pipe | newline\nmessage"))
        );

        String json = SuiteJsonWriter.write(result);
        Assert.assertTrue(json.contains("\"runId\":\"run-\\\"1\\n\""));
        Assert.assertTrue(json.contains("\"quote,key\":\"value \\\"x\\\"\\nnext\""));
        Assert.assertTrue(json.contains("\"commitLabel\":\"main\\nabc\""));

        String metricsCsv = SuiteCsvWriter.metricsCsv(result);
        Assert.assertTrue(metricsCsv.contains("current,release-ping-latency,summary,REPEAT,,qps,1,850.000,850.000,850.000,850.000,"));
        Assert.assertTrue(metricsCsv.contains("current,release-ping-latency,iteration,WARMUP,0,qps,,,,,,425.000"));

        String markdown = SuiteMarkdownWriter.write(result);
        Assert.assertTrue(markdown.contains("PING, \"latency\"<br>case"));
        Assert.assertTrue(markdown.contains("pipe \\| newline<br>message"));
    }

    @Test
    public void jsonWriterRendersExternalRedisArtifactWithoutSecrets() {
        ScenarioDefinition scenario = new ScenarioDefinition("redis-set", "Redis SET",
                BenchWorkloadKind.SET_GET, 10, 128, 100, 1, 1, 0, 1, false);
        ScenarioPassResult redis = ScenarioPassResult.completed("redis", SuiteArtifact.Kind.EXTERNAL_REDIS, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 1000.0),
                        new SuiteMetric("errors", 0.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        SuiteRunResult result = new SuiteRunResult(
                "run-redis",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                new SuiteEnvironment(Map.of("redis.info.server", "redis_version:8.0.0")),
                List.of(SuiteArtifact.externalRedis("redis", "127.0.0.1", 6379, "bench-user", "bench-secret", 2)),
                List.of(scenario),
                List.of(redis),
                List.of(),
                List.of()
        );

        String json = SuiteJsonWriter.write(result);

        Assert.assertTrue(json.contains("\"label\":\"redis\""));
        Assert.assertTrue(json.contains("\"kind\":\"EXTERNAL_REDIS\""));
        Assert.assertTrue(json.contains("\"host\":\"127.0.0.1\""));
        Assert.assertTrue(json.contains("\"port\":6379"));
        Assert.assertTrue(json.contains("\"db\":2"));
        Assert.assertTrue(json.contains("\"commitLabel\":\"\""));
        Assert.assertFalse(json.contains("\"jarPath\""));
        Assert.assertFalse(json.contains("bench-user"));
        Assert.assertFalse(json.contains("bench-secret"));
    }

    @Test
    public void csvEscapesEmittedFieldsWithCommasQuotesAndNewlines() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING latency",
                BenchWorkloadKind.PING, 10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult pass = pass("current,\"quoted\"\nartifact", scenario, 850.0, 10.0, 20.0, 0.0);
        SuiteRunResult result = new SuiteRunResult(
                "run-1",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                new SuiteEnvironment(null),
                List.of(),
                List.of(scenario),
                List.of(pass),
                List.of(),
                List.of()
        );

        String metricsCsv = SuiteCsvWriter.metricsCsv(result);

        Assert.assertTrue(metricsCsv.contains("\"current,\"\"quoted\"\"\nartifact\",release-ping-latency,summary,REPEAT,,qps"));
    }

    @Test
    public void jsonAndMarkdownSortMapKeysForDeterministicOutput() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING latency",
                BenchWorkloadKind.PING, 10, 0, 100, 1, 1, 0, 1, true);
        Map<String, String> environment = new LinkedHashMap<>();
        environment.put("z-key", "z");
        environment.put("a-key", "a");
        Map<String, String> before = new LinkedHashMap<>();
        before.put("z-before", "z");
        before.put("a-before", "a");
        Map<String, String> after = new LinkedHashMap<>();
        after.put("z-after", "z");
        after.put("a-after", "a");
        ScenarioPassResult pass = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 850.0),
                        new SuiteMetric("p95_ms", 10.0),
                        new SuiteMetric("p99_ms", 20.0),
                        new SuiteMetric("errors", 0.0)
                ))
        ), new ObservationSnapshot(before), new ObservationSnapshot(after));
        SuiteRunResult result = new SuiteRunResult(
                "run-1",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                new SuiteEnvironment(environment),
                List.of(),
                List.of(scenario),
                List.of(pass),
                List.of(),
                List.of()
        );

        String json = SuiteJsonWriter.write(result);
        String markdown = SuiteMarkdownWriter.write(result);

        Assert.assertTrue(json.indexOf("\"a-key\":\"a\"") < json.indexOf("\"z-key\":\"z\""));
        Assert.assertTrue(json.indexOf("\"a-before\":\"a\"") < json.indexOf("\"z-before\":\"z\""));
        Assert.assertTrue(json.indexOf("\"a-after\":\"a\"") < json.indexOf("\"z-after\":\"z\""));
        Assert.assertTrue(markdown.indexOf("| a-key | a |") < markdown.indexOf("| z-key | z |"));
    }

    @Test
    public void reportWriterWritesSuiteResultJsonForExternalRedisArtifact() throws Exception {
        Path reportDir = Files.createTempDirectory("suite-report-writer-redis");
        ScenarioDefinition scenario = new ScenarioDefinition("redis-get", "Redis GET",
                BenchWorkloadKind.SET_GET, 10, 128, 100, 1, 1, 0, 1, false);
        ScenarioPassResult redis = ScenarioPassResult.completed("redis", SuiteArtifact.Kind.EXTERNAL_REDIS, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 950.0),
                        new SuiteMetric("errors", 0.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult current = ScenarioPassResult.completed("current", SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", 900.0),
                        new SuiteMetric("errors", 0.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioComparison comparison = ScenarioComparison.compare(scenario, redis, current);
        SuiteRunResult result = new SuiteRunResult(
                "run-redis-report",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:02Z"),
                new SuiteEnvironment(Map.of("redis.info.server", "redis_version:8.0.0")),
                List.of(
                        SuiteArtifact.externalRedis("redis", "127.0.0.1", 6380, "", "hidden-secret", 0),
                        SuiteArtifact.yierdisJar("current", Path.of("/tmp/current.jar"), "head")
                ),
                List.of(scenario),
                List.of(redis, current),
                List.of(comparison),
                ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults())
        );

        SuiteReportWriter.writeAll(result, reportDir);

        String json = Files.readString(reportDir.resolve("suite-result.json"), StandardCharsets.UTF_8);
        Assert.assertTrue(json.contains("\"kind\":\"EXTERNAL_REDIS\""));
        Assert.assertTrue(json.contains("\"host\":\"127.0.0.1\""));
        Assert.assertTrue(json.contains("\"port\":6380"));
        Assert.assertTrue(json.contains("\"db\":0"));
        Assert.assertTrue(json.contains("\"kind\":\"YIERDIS_JAR\""));
        Assert.assertTrue(json.contains("\"jarPath\":\"/tmp/current.jar\""));
        Assert.assertFalse(json.contains("hidden-secret"));
    }

    @Test
    public void suiteRunResultValidatesAndCopiesInputs() {
        Instant startedAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant finishedAt = Instant.parse("2026-01-01T00:00:01Z");
        SuiteRunResult empty = new SuiteRunResult("run-1", SuiteProfileName.RELEASE, startedAt, finishedAt,
                new SuiteEnvironment(null), null, null, null, null, null);

        Assert.assertTrue(empty.artifacts().isEmpty());
        Assert.assertTrue(empty.scenarios().isEmpty());
        Assert.assertTrue(empty.environment().values().isEmpty());
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> empty.artifacts().add(new SuiteArtifact("current", Path.of("/tmp/current.jar"), "")));
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteRunResult(" ", SuiteProfileName.RELEASE,
                startedAt, finishedAt, new SuiteEnvironment(null), null, null, null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> new SuiteRunResult("run-1", SuiteProfileName.RELEASE,
                finishedAt, startedAt, new SuiteEnvironment(null), null, null, null, null, null));
    }

    @Test
    public void suiteEnvironmentCapturesStableKeysAndCopiesValues() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("first", "1");
        SuiteEnvironment environment = new SuiteEnvironment(values);
        values.put("second", "2");

        Assert.assertEquals(List.of("first"), List.copyOf(environment.values().keySet()));
        Assert.assertThrows(UnsupportedOperationException.class, () -> environment.values().put("third", "3"));

        SuiteEnvironment captured = SuiteEnvironment.capture();
        Assert.assertTrue(captured.values().containsKey("java.version"));
        Assert.assertTrue(captured.values().containsKey("java.vm.name"));
        Assert.assertTrue(captured.values().containsKey("os.name"));
        Assert.assertTrue(captured.values().containsKey("os.arch"));
        Assert.assertTrue(captured.values().containsKey("available.processors"));
    }

    private static SuiteRunResult suiteRunResult() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING latency",
                BenchWorkloadKind.PING, 10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult current = pass("current", scenario, 850.0, 12.0, 24.0, 0.0);
        ScenarioComparison comparison = ScenarioComparison.compare(scenario, baseline, current);
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        return new SuiteRunResult(
                "run-1",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:05Z"),
                new SuiteEnvironment(Map.of("java.version", "25", "os.name", "Linux")),
                List.of(
                        new SuiteArtifact("baseline", Path.of("/tmp/baseline.jar"), "base"),
                        new SuiteArtifact("current", Path.of("/tmp/current.jar"), "head")
                ),
                List.of(scenario),
                List.of(baseline, current),
                List.of(comparison),
                findings
        );
    }

    private static ScenarioPassResult pass(String artifact, ScenarioDefinition scenario, double qps, double p95, double p99, double errors) {
        return ScenarioPassResult.completed(artifact, SuiteArtifact.Kind.YIERDIS_JAR, scenario, List.of(
                IterationResult.warmup(0, List.of(
                        new SuiteMetric("qps", qps / 2.0),
                        new SuiteMetric("p95_ms", p95 * 2.0),
                        new SuiteMetric("p99_ms", p99 * 2.0),
                        new SuiteMetric("errors", errors)
                )),
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", qps),
                        new SuiteMetric("p95_ms", p95),
                        new SuiteMetric("p99_ms", p99),
                        new SuiteMetric("errors", errors)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
    }
}
