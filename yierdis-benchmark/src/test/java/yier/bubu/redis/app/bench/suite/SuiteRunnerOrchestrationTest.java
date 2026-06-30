package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchHarness;
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
    public void stopFailureAfterIterationFailurePreservesBothFailuresAndDoesNotAbortRun() throws Exception {
        ScenarioDefinition scenario1 = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 2, true);
        ScenarioDefinition scenario2 = scenario("release-set-get-256b-c64-p8", BenchWorkloadKind.SET_GET, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.currentOnly(Files.createTempDirectory("suite-runner-stop-after-failure-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.failIteration = true;
        harness.failStopOnce = true;

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario1, scenario2)).run();

        Assert.assertEquals(2, result.passes().size());
        ScenarioPassResult first = result.passes().get(0);
        Assert.assertTrue(first.failed());
        Assert.assertTrue(first.failureMessage(), first.failureMessage().contains("forced failure"));
        Assert.assertTrue(first.failureMessage(), first.failureMessage().contains("forced stop failure"));
        Assert.assertEquals("current:" + scenario1.id() + ":before", first.before().values().get("phase"));
        Assert.assertEquals(List.of(
                "start current release-ping-latency", "stop current release-ping-latency",
                "start current release-set-get-256b-c64-p8", "stop current release-set-get-256b-c64-p8"
        ), harness.lifecycle);
    }

    @Test
    public void stopFailureAfterSuccessfulPassMarksFailedAndContinuesRemainingComparisonPasses() throws Exception {
        ScenarioDefinition scenario1 = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        ScenarioDefinition scenario2 = scenario("release-set-get-256b-c64-p8", BenchWorkloadKind.SET_GET, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-stop-success-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.failStopOnce = true;

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario1, scenario2)).run();

        Assert.assertEquals(4, result.passes().size());
        ScenarioPassResult first = result.passes().get(0);
        Assert.assertTrue(first.failed());
        Assert.assertTrue(first.failureMessage(), first.failureMessage().contains("forced stop failure"));
        Assert.assertEquals(List.of(
                "start baseline release-ping-latency", "stop baseline release-ping-latency",
                "start current release-ping-latency", "stop current release-ping-latency",
                "start baseline release-set-get-256b-c64-p8", "stop baseline release-set-get-256b-c64-p8",
                "start current release-set-get-256b-c64-p8", "stop current release-set-get-256b-c64-p8"
        ), harness.lifecycle);
        Assert.assertEquals(2, result.comparisons().size());
        Assert.assertFalse(result.comparisons().get(0).comparable());
        Assert.assertTrue(result.comparisons().get(1).comparable());
        List<ThresholdFinding> comparability = result.findings().stream()
                .filter(finding -> finding.metric().equals("comparability"))
                .toList();
        Assert.assertEquals(1, comparability.size());
        Assert.assertEquals(ThresholdFinding.Level.CRITICAL, comparability.get(0).level());
    }

    @Test
    public void checkedStopFailureIsPreservedAsPassFailure() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.currentOnly(Files.createTempDirectory("suite-runner-checked-stop-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.failStopCheckedOnce = true;

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(1, result.passes().size());
        ScenarioPassResult pass = result.passes().get(0);
        Assert.assertTrue(pass.failed());
        Assert.assertTrue(pass.failureMessage(), pass.failureMessage().contains("forced checked stop failure"));
        Assert.assertEquals(1, result.findings().size());
        Assert.assertTrue(result.findings().get(0).message(), result.findings().get(0).message().contains("forced checked stop failure"));
    }

    @Test
    public void currentOnlyDirtyFindingMentionsMissingMetricsAndErrorSummary() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.currentOnly(Files.createTempDirectory("suite-runner-dirty-current-"), 16378);
        FakeHarness harness = new FakeHarness();
        harness.repeatMetrics = List.of(
                new SuiteMetric("qps", 1000.0),
                new SuiteMetric("p95_ms", 10.0),
                new SuiteMetric("errors", 2.0)
        );

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(1, result.passes().size());
        Assert.assertFalse(result.passes().get(0).failed());
        Assert.assertFalse(result.passes().get(0).clean());
        Assert.assertEquals(1, result.findings().size());
        ThresholdFinding finding = result.findings().get(0);
        Assert.assertEquals(ThresholdFinding.Level.CRITICAL, finding.level());
        Assert.assertTrue(finding.message(), finding.message().contains("p99_ms"));
        Assert.assertTrue(finding.message(), finding.message().contains("errors"));
        Assert.assertTrue(finding.message(), finding.message().contains("max=2.000"));
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
    public void highButSufficientPortBaseStaysInContiguousSafeRange() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-port-report-"), 65534);
        FakeHarness harness = new FakeHarness();

        new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(List.of(65534, 65535), harness.ports);
    }

    @Test
    public void lowPortBaseFailsClearlyBeforeStartingServers() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.currentOnly(Files.createTempDirectory("suite-runner-low-port-"), 1023);
        FakeHarness harness = new FakeHarness();

        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                () -> new SuiteRunner(config, harness, List.of(scenario)).run());

        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("portBase"));
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("headroom"));
        Assert.assertTrue(harness.lifecycle.isEmpty());
    }

    @Test
    public void highPortBaseWithInsufficientHeadroomFailsClearlyBeforeStartingServers() throws Exception {
        ScenarioDefinition scenario = scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteConfig config = TestSuiteConfigs.comparison(Files.createTempDirectory("suite-runner-port-overflow-"), 65535);
        FakeHarness harness = new FakeHarness();

        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                () -> new SuiteRunner(config, harness, List.of(scenario)).run());

        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("portBase"));
        Assert.assertTrue(harness.lifecycle.isEmpty());
    }

    @Test
    public void runUsesConfiguredArtifactOrderAndArtifactEndpointsForObservations() throws Exception {
        ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteArtifact current = new SuiteArtifact("current", TestSuiteConfigs.regularTempJar("current"), "head");
        SuiteArtifact baseline = new SuiteArtifact("baseline", TestSuiteConfigs.regularTempJar("baseline"), "base");
        SuiteArtifact redis = SuiteArtifact.externalRedis("redis", "127.0.0.9", 6389, "", "", 0);
        SuiteConfig config = TestSuiteConfigs.config(
                Files.createTempDirectory("suite-runner-artifact-order-"),
                16378,
                Optional.of(baseline),
                current,
                List.of(current, redis, baseline)
        );
        FakeHarness harness = new FakeHarness();

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(List.of("current", "redis", "baseline"),
                result.artifacts().stream().map(SuiteArtifact::label).toList());
        Assert.assertEquals(List.of(
                "start current release-ping-latency", "stop current release-ping-latency",
                "start redis release-ping-latency", "stop redis release-ping-latency",
                "start baseline release-ping-latency", "stop baseline release-ping-latency"
        ), harness.lifecycle);
        Assert.assertEquals(List.of(
                "current@127.0.0.1:16378",
                "current@127.0.0.1:16378",
                "redis@127.0.0.9:6389",
                "redis@127.0.0.9:6389",
                "baseline@127.0.0.1:16380",
                "baseline@127.0.0.1:16380"
        ), harness.observations);
        Assert.assertEquals(3, result.passes().size());
    }

    @Test
    public void redisCurrentRunProducesComparisonBetweenRedisAndCurrent() throws Exception {
        ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-ping-latency", BenchWorkloadKind.PING, 1, 1, true);
        SuiteArtifact current = new SuiteArtifact("current", TestSuiteConfigs.regularTempJar("current"), "head");
        SuiteArtifact redis = SuiteArtifact.externalRedis("redis", "127.0.0.9", 6389, "", "", 0);
        SuiteConfig config = TestSuiteConfigs.config(
                Files.createTempDirectory("suite-runner-redis-current-"),
                16378,
                Optional.empty(),
                current,
                List.of(redis, current)
        );
        FakeHarness harness = new FakeHarness();

        SuiteRunResult result = new SuiteRunner(config, harness, List.of(scenario)).run();

        Assert.assertEquals(2, result.passes().size());
        Assert.assertEquals(1, result.comparisons().size());
        ScenarioComparison comparison = result.comparisons().get(0);
        Assert.assertTrue(comparison.comparable());
        Assert.assertEquals(scenario.id(), comparison.scenario().id());
        Assert.assertEquals(List.of("redis", "current"), result.artifacts().stream().map(SuiteArtifact::label).toList());
    }

    @Test
    public void maxmemoryScenarioAgainstRedisIsNotComparableWithoutExplicitSupport() {
        ScenarioDefinition scenario = SuiteProfileFactory.expand(SuiteProfileName.RELEASE).stream()
                .filter(item -> item.id().equals("release-maxmemory-eviction"))
                .findFirst()
                .orElseThrow();

        ScenarioPassResult redis = RedisSuiteTestSupport.cleanPass("redis", scenario);
        ScenarioPassResult current = RedisSuiteTestSupport.cleanPass("current", scenario);

        ScenarioComparison comparison = ScenarioComparison.compare(scenario, redis, current);

        Assert.assertFalse(comparison.comparable());
        Assert.assertTrue(comparison.nonComparableReason().contains("external Redis config required"));
    }

    @Test
    public void nativeDefragScenarioAgainstRedisIsNeverComparable() {
        ScenarioDefinition scenario = SuiteProfileFactory.expand(SuiteProfileName.RELEASE).stream()
                .filter(item -> item.id().equals("release-native-defrag-append"))
                .findFirst()
                .orElseThrow();

        Assert.assertEquals(ScenarioDefinition.RedisComparable.NO, scenario.redisComparable());
    }

    @Test
    public void realRedisPassUsesArtifactAwareObservationCapture() throws Exception {
        try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
            Assert.assertTrue(server.awaitListening());
            SuiteArtifact redis = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);
            SuiteConfig config = TestSuiteConfigs.config(
                    Files.createTempDirectory("suite-runner-real-redis-"),
                    16378,
                    Optional.empty(),
                    redis,
                    List.of(redis)
            );
            ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-ping-latency", BenchWorkloadKind.PING, 0, 1, true);

            SuiteRunResult result = new SuiteRunner(config, new BenchHarness(), List.of(scenario)).run();

            Assert.assertEquals(1, result.passes().size());
            ScenarioPassResult pass = result.passes().get(0);
            Assert.assertFalse(pass.before().values().containsKey("STATS"));
            Assert.assertFalse(pass.after().values().containsKey("STATS"));
            Assert.assertEquals(List.of("INFO", "MEMORY STATS"), List.copyOf(pass.before().values().keySet()));
            Assert.assertEquals(List.of("INFO", "MEMORY STATS"), List.copyOf(pass.after().values().keySet()));
            Assert.assertFalse(server.awaitCommands(5).contains("STATS"));
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
        private boolean failStopOnce;
        private boolean failStopCheckedOnce;
        private List<SuiteMetric> repeatMetrics;
        private int observationCount;
        private final List<String> observations = new ArrayList<>();

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
            Assert.assertNotNull(active);
            observations.add(active.artifactLabel() + "@" + host + ":" + port);
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
            List<SuiteMetric> metrics = repeatMetrics == null || kind == IterationResult.Kind.WARMUP ? List.of(
                    new SuiteMetric("qps", 1000.0),
                    new SuiteMetric("p95_ms", 10.0),
                    new SuiteMetric("p99_ms", 20.0),
                    new SuiteMetric("errors", 0.0)
            ) : repeatMetrics;
            return new IterationResult(kind, index, metrics);
        }

        @Override
        public void stopServer(SuiteHarness.RunningServer server) throws Exception {
            lifecycle.add("stop " + server.artifactLabel() + " " + server.scenarioId());
            active = null;
            if (failStopCheckedOnce) {
                failStopCheckedOnce = false;
                throw new Exception("forced checked stop failure in "
                        + server.artifactLabel() + " " + server.scenarioId());
            }
            if (failStopOnce) {
                failStopOnce = false;
                throw new IllegalStateException("forced stop failure in "
                        + server.artifactLabel() + " " + server.scenarioId());
            }
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

    static SuiteConfig config(Path reportDir, int portBase, Optional<SuiteArtifact> baseline) throws Exception {
        return config(reportDir, portBase, baseline, new SuiteArtifact("current", regularTempJar("current"), "head"), null);
    }

    static SuiteConfig config(
            Path reportDir,
            int portBase,
            Optional<SuiteArtifact> baseline,
            SuiteArtifact current,
            List<SuiteArtifact> artifactsInRunOrder
    ) throws Exception {
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return new SuiteConfig(
                SuiteProfileName.RELEASE,
                current,
                baseline,
                artifactsInRunOrder == null ? defaultArtifacts(current, baseline) : artifactsInRunOrder,
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

    private static List<SuiteArtifact> defaultArtifacts(SuiteArtifact current, Optional<SuiteArtifact> baseline) {
        List<SuiteArtifact> artifacts = new ArrayList<>();
        baseline.ifPresent(artifacts::add);
        artifacts.add(current);
        return artifacts;
    }

    static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
