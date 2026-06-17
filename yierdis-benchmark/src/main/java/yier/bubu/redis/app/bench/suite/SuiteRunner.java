package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class SuiteRunner {
    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final int MIN_SUITE_PORT = 1024;
    private static final int MAX_PORT = 65535;

    private final SuiteConfig config;
    private final SuiteHarness harness;
    private final List<ScenarioDefinition> scenarios;

    public SuiteRunner(SuiteConfig config, SuiteHarness harness, List<ScenarioDefinition> scenarios) {
        this.config = Objects.requireNonNull(config, "config");
        this.harness = Objects.requireNonNull(harness, "harness");
        this.scenarios = List.copyOf(Objects.requireNonNull(scenarios, "scenarios"));
    }

    public SuiteRunResult run() {
        Instant startedAt = Instant.now();
        String runId = RUN_ID_TIME.format(startedAt) + "-" + config.profile().cliName();
        List<SuiteArtifact> artifacts = artifactsInRunOrder();
        validatePortHeadroom(artifacts.size());
        List<ScenarioPassResult> passes = new ArrayList<>();

        for (int scenarioIndex = 0; scenarioIndex < scenarios.size(); scenarioIndex++) {
            ScenarioDefinition scenario = scenarios.get(scenarioIndex);
            for (int artifactIndex = 0; artifactIndex < artifacts.size(); artifactIndex++) {
                SuiteArtifact artifact = artifacts.get(artifactIndex);
                int port = portFor(scenarioIndex, artifactIndex, artifacts.size());
                Path logFile = logFileFor(artifact, scenario);
                passes.add(runPass(artifact, scenario, port, logFile));
            }
        }

        List<ScenarioComparison> comparisons = comparisons(passes);
        List<ThresholdFinding> findings = findings(passes, comparisons);
        Instant finishedAt = Instant.now();
        return new SuiteRunResult(
                runId,
                config.profile(),
                startedAt,
                finishedAt,
                SuiteEnvironment.capture(),
                artifacts,
                scenarios,
                passes,
                comparisons,
                findings
        );
    }

    private List<SuiteArtifact> artifactsInRunOrder() {
        List<SuiteArtifact> artifacts = new ArrayList<>();
        config.baseline().ifPresent(artifacts::add);
        artifacts.add(config.current());
        return List.copyOf(artifacts);
    }

    private ScenarioPassResult runPass(SuiteArtifact artifact, ScenarioDefinition scenario, int port, Path logFile) {
        SuiteHarness.RunningServer server = null;
        ObservationSnapshot before = ObservationSnapshot.empty();
        ScenarioPassResult pass;
        try {
            server = harness.startServer(artifact, scenario, config, port, logFile);
            before = requireObservation(harness.captureObservation(config.host(), port));
            List<IterationResult> iterations = new ArrayList<>();
            for (int i = 0; i < scenario.warmupIterations(); i++) {
                iterations.add(requireIteration(harness.runIteration(server, scenario, i, IterationResult.Kind.WARMUP, config)));
            }
            for (int i = 0; i < scenario.repeatIterations(); i++) {
                iterations.add(requireIteration(harness.runIteration(server, scenario, i, IterationResult.Kind.REPEAT, config)));
            }
            ObservationSnapshot after = requireObservation(harness.captureObservation(config.host(), port));
            pass = ScenarioPassResult.completed(artifact.label(), scenario, iterations, before, after);
        } catch (Exception e) {
            pass = failedPass(artifact.label(), scenario, e, before);
        }
        if (server != null) {
            pass = stopServer(server, pass);
        }
        return pass;
    }

    private ScenarioPassResult stopServer(SuiteHarness.RunningServer server, ScenarioPassResult pass) {
        try {
            harness.stopServer(server);
            return pass;
        } catch (Throwable e) {
            String message = pass.failed()
                    ? pass.failureMessage() + "; stop failed: " + conciseFailureMessage(e)
                    : "stop failed: " + conciseFailureMessage(e);
            return new ScenarioPassResult(pass.artifactLabel(), pass.scenario(), true, message,
                    pass.iterations(), pass.before(), pass.after(), null);
        }
    }

    private static ObservationSnapshot requireObservation(ObservationSnapshot observation) {
        return Objects.requireNonNull(observation, "observation");
    }

    private static IterationResult requireIteration(IterationResult iteration) {
        return Objects.requireNonNull(iteration, "iteration");
    }

    private static ScenarioPassResult failedPass(String artifactLabel, ScenarioDefinition scenario, Throwable failure, ObservationSnapshot before) {
        String message = conciseFailureMessage(failure);
        if (before.values().isEmpty()) {
            return ScenarioPassResult.failed(artifactLabel, scenario, message);
        }
        return new ScenarioPassResult(artifactLabel, scenario, true, message, List.of(), before,
                ObservationSnapshot.empty(), null);
    }

    private static String conciseFailureMessage(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            message = failure.getClass().getSimpleName();
        }
        return message;
    }

    private List<ScenarioComparison> comparisons(List<ScenarioPassResult> passes) {
        if (config.baseline().isEmpty()) {
            return List.of();
        }
        List<ScenarioComparison> comparisons = new ArrayList<>();
        for (ScenarioDefinition scenario : scenarios) {
            ScenarioPassResult baseline = findPass(passes, "baseline", scenario);
            ScenarioPassResult current = findPass(passes, "current", scenario);
            comparisons.add(ScenarioComparison.compare(scenario, baseline, current));
        }
        return List.copyOf(comparisons);
    }

    private static ScenarioPassResult findPass(List<ScenarioPassResult> passes, String artifactLabel, ScenarioDefinition scenario) {
        for (ScenarioPassResult pass : passes) {
            if (pass.artifactLabel().equals(artifactLabel) && pass.scenario().id().equals(scenario.id())) {
                return pass;
            }
        }
        throw new IllegalStateException("missing " + artifactLabel + " pass for scenario " + scenario.id());
    }

    private List<ThresholdFinding> findings(List<ScenarioPassResult> passes, List<ScenarioComparison> comparisons) {
        if (!comparisons.isEmpty()) {
            List<ThresholdFinding> out = new ArrayList<>();
            for (ScenarioComparison comparison : comparisons) {
                out.addAll(ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults()));
            }
            return List.copyOf(out);
        }

        List<ThresholdFinding> out = new ArrayList<>();
        for (ScenarioPassResult pass : passes) {
            if (!pass.clean()) {
                out.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, pass.scenario().id(),
                        "pass", pass.artifactLabel() + " is not clean" + dirtySuffix(pass)));
            }
        }
        return List.copyOf(out);
    }

    private static String dirtySuffix(ScenarioPassResult pass) {
        List<String> details = new ArrayList<>();
        if (!pass.failureMessage().isBlank()) {
            details.add(pass.failureMessage());
        }
        List<String> missing = pass.missingRequiredMetrics();
        if (!missing.isEmpty()) {
            details.add("missing required metrics " + missing);
        }
        MetricSummary errors = pass.summaries().get("errors");
        if (errors != null && errors.max() > 0.0) {
            details.add("errors max=" + format(errors.max()));
        }
        if (details.isEmpty()) {
            return "";
        }
        return ": " + String.join(", ", details);
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private void validatePortHeadroom(int artifactCount) {
        long requiredPorts = (long) scenarios.size() * artifactCount;
        if (requiredPorts == 0) {
            return;
        }
        long lastPort = (long) config.portBase() + requiredPorts - 1L;
        if (config.portBase() < MIN_SUITE_PORT || lastPort > MAX_PORT) {
            throw new IllegalArgumentException("portBase " + config.portBase()
                    + " does not have headroom for " + requiredPorts
                    + " contiguous suite ports within " + MIN_SUITE_PORT + ".." + MAX_PORT);
        }
    }

    private int portFor(int scenarioIndex, int artifactIndex, int artifactCount) {
        long offset = (long) scenarioIndex * artifactCount + artifactIndex;
        return (int) ((long) config.portBase() + offset);
    }

    private Path logFileFor(SuiteArtifact artifact, ScenarioDefinition scenario) {
        String fileName = sanitizePathPart(artifact.label()) + "-" + sanitizePathPart(scenario.id()) + ".log";
        return config.reportDir().resolve(fileName).normalize();
    }

    private static String sanitizePathPart(String value) {
        String sanitized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]+", "-");
        sanitized = sanitized.replaceAll("^[.-]+", "").replaceAll("[.-]+$", "");
        return sanitized.isBlank() ? "item" : sanitized;
    }
}
