package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SuiteRunnerOrchestrationTest {
    @Test
    public void comparisonRunStartsFreshServersInScenarioArtifactOrder() throws Exception {
        ScenarioDefinition scenario1 = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 2, true);
        ScenarioDefinition scenario2 = scenario("release-set-get-256b-c64-p8", BenchWorkloadKind.SET_GET, 2, 3, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-report-"), 16378);
        FakeHarness harness = new FakeHarness();

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario1, scenario2)).run();

        Assert.assertEquals(List.of(
                "start baseline release-ping-latency", "stop baseline release-ping-latency",
                "start current release-ping-latency", "stop current release-ping-latency",
                "start baseline release-set-get-256b-c64-p8", "stop baseline release-set-get-256b-c64-p8",
                "start current release-set-get-256b-c64-p8", "stop current release-set-get-256b-c64-p8"
        ), harness.lifecycle);
        Assert.assertEquals(4, result.passes().size());
        Assert.assertEquals(2, result.comparisons().size());
        Assert.assertTrue(result.findings().isEmpty());
        Assert.assertEquals(List.of("baseline", "current"), result.artifacts().stream().map(SuiteArtifact::label).toList());
        Assert.assertEquals(List.of(scenario1, scenario2), result.scenarios());
        Assert.assertEquals(config.profile(), result.profile());
        Assert.assertFalse(result.runId().isBlank());
        Assert.assertFalse(result.finishedAt().isBefore(result.startedAt()));
        Assert.assertTrue(result.environment().values().containsKey("java.version"));

        for (ScenarioPassResult pass : result.passes()) {
            long warmups = pass.iterations().stream().filter(iteration -> iteration.kind() == IterationResult.Kind.WARMUP).count();
            long repeats = pass.iterations().stream().filter(iteration -> iteration.kind() == IterationResult.Kind.REPEAT).count();
            Assert.assertEquals(pass.scenario().warmupIterations(), warmups);
            Assert.assertEquals(pass.scenario().repeatIterations(), repeats);
            Assert.assertEquals(pass.artifactLabel() + ":" + pass.scenario().id() + ":before",
                    pass.before().values().get("phase"));
            Assert.assertEquals(pass.artifactLabel() + ":" + pass.scenario().id() + ":after",
                    pass.after().values().get("phase"));
            Assert.assertFalse(pass.failed());
            Assert.assertTrue(pass.clean());
        }
    }

    @Test
    public void currentOnlyIterationFailureProducesFailedPassFindingAndStopsServer() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 2, true);
        SuiteConfig config = TestSuiteConfigs.currentOnly(Files.createTempDirectory("suite-runner-failure-report-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.failIteration = true;

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(1, result.passes().size());
        ScenarioPassResult pass = result.passes().get(0);
        Assert.assertTrue(pass.failed());
        Assert.assertTrue(pass.failureMessage(), pass.failureMessage().contains("forced failure"));
        Assert.assertEquals(List.of("start current release-ping-latency", "stop current release-ping-latency"),
                harness.lifecycle);
        Assert.assertEquals(1, result.findings().size());
        ThresholdFinding finding = result.findings().get(0);
        Assert.assertEquals(ThresholdFinding.Level.CRITICAL, finding.level());
        Assert.assertEquals(scenario.id(), finding.scenarioId());
    }

    @Test
    public void comparisonFailureUsesEvaluatorFindingWithoutDuplicateComparabilityFinding() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 2, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-dirty-report-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.failCurrentIteration = true;

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(2, result.passes().size());
        Assert.assertEquals(1, result.comparisons().size());
        Assert.assertFalse(result.comparisons().get(0).comparable());
        List<ThresholdFinding> comparability = result.findings().stream()
                .filter(finding -> finding.metric().equals("comparability"))
                .toList();
        Assert.assertEquals(1, comparability.size());
        Assert.assertEquals(ThresholdFinding.Level.CRITICAL, comparability.get(0).level());
    }

    @Test
    public void highPortBaseStillAllocatesValidPorts() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-port-report-"), 65535);
        FakeHarness harness = new FakeHarness();

        new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(2, harness.ports.size());
        for (int port : harness.ports) {
            Assert.assertTrue("invalid port " + port, port > 0 && port <= 65535);
        }
    }

    private static ScenarioDefinition scenario(String id, BenchWorkloadKind workload, int warmups, int repeats, boolean latency) {
        return new ScenarioDefinition(id, id, workload, 100, workload == BenchWorkloadKind.PING ? 0 : 256,
                1000, 8, 4, warmups, repeats, latency);
    }

    private static final class FakeHarness implements SuiteHarness {
        private final List<String> lifecycle = new ArrayList<>();
        private final List<Integer> ports = new ArrayList<>();
        private SuiteHarness.RunningServer active;
        private boolean failIteration;
        private boolean failCurrentIteration;
        private int observationCount;

        @Override
        public SuiteHarness.RunningServer startServer(
                SuiteArtifact artifact,
                ScenarioDefinition scenario,
                SuiteConfig config,
                int port,
                Path logFile
        ) {
            Assert.assertTrue("invalid port " + port, port > 0 && port <= 65535);
            Assert.assertTrue(logFile.normalize().startsWith(config.reportDir()));
            Assert.assertTrue(logFile.getFileName().toString().contains(artifact.label()));
            Assert.assertTrue(logFile.getFileName().toString().contains(scenario.id()));
            lifecycle.add("start " + artifact.label() + " " + scenario.id());
            ports.add(port);
            active = new SuiteHarness.RunningServer(artifact.label(), scenario.id(), port, logFile);
            observationCount = 0;
            return active;
        }

        @Override
        public ObservationSnapshot captureObservation(String host, int port) {
            Assert.assertEquals("127.0.0.1", host);
            Assert.assertNotNull(active);
            observationCount++;
            String phase = observationCount == 1 ? "before" : "after";
            return new ObservationSnapshot(Map.of("phase",
                    active.artifactLabel() + ":" + active.scenarioId() + ":" + phase));
        }

        @Override
        public IterationResult runIteration(
                SuiteHarness.RunningServer server,
                ScenarioDefinition scenario,
                int index,
                IterationResult.Kind kind,
                SuiteConfig config
        ) throws Exception {
            if (failIteration || (failCurrentIteration && server.artifactLabel().equals("current"))) {
                throw new Exception("forced failure in " + server.artifactLabel() + " " + scenario.id());
            }
            List<SuiteMetric> metrics = List.of(
                    new SuiteMetric("qps", 1000.0),
                    new SuiteMetric("p95_ms", 10.0),
                    new SuiteMetric("p99_ms", 20.0),
                    new SuiteMetric("errors", 0.0)
            );
            return new IterationResult(kind, index, metrics);
        }

        @Override
        public void stopServer(SuiteHarness.RunningServer server) {
            lifecycle.add("stop " + server.artifactLabel() + " " + server.scenarioId());
            active = null;
        }
    }
}

final class TestSuiteConfigs {
    private TestSuiteConfigs() {
    }

    static SuiteConfig comparison(Path reportDir, int portBase) throws Exception {
        return config(reportDir, portBase, Optional.of(new SuiteArtifact("baseline", regularTempJar("baseline"), "base")));
    }

    static SuiteConfig currentOnly(Path reportDir, int portBase) throws Exception {
        return config(reportDir, portBase, Optional.empty());
    }

    private static SuiteConfig config(Path reportDir, int portBase, Optional<SuiteArtifact> baseline) throws Exception {
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return new SuiteConfig(
                SuiteProfileName.RELEASE,
                new SuiteArtifact("current", regularTempJar("current"), "head"),
                baseline,
                reportDir,
                "127.0.0.1",
                portBase,
                "java",
                "4g",
                "4g",
                "6g",
                serverArgs,
                true
        );
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
