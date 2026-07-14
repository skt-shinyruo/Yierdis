package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class SuiteRunner {
    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final int MIN_SUITE_PORT = 1024;
    private static final int MAX_PORT = 65535;

    private final SuiteConfig config;
    private final SuiteHarness harness;
    private final List<ScenarioDefinition> scenarios;
    private final ObservationClient observationClient;

    public SuiteRunner(SuiteConfig config, SuiteHarness harness, List<ScenarioDefinition> scenarios) {
        this(config, harness, scenarios, new ObservationClient());
    }

    SuiteRunner(SuiteConfig config, SuiteHarness harness, List<ScenarioDefinition> scenarios, ObservationClient observationClient) {
        this.config = Objects.requireNonNull(config, "config");
        this.harness = Objects.requireNonNull(harness, "harness");
        this.scenarios = List.copyOf(Objects.requireNonNull(scenarios, "scenarios"));
        this.observationClient = Objects.requireNonNull(observationClient, "observationClient");
    }

    public SuiteRunResult run() {
        Instant startedAt = Instant.now();
        String runId = RUN_ID_TIME.format(startedAt) + "-" + config.profile().cliName();
        List<SuiteArtifact> artifacts = artifactsInRunOrder();
        List<SuiteArtifact> generatedPortArtifacts = generatedPortArtifacts(artifacts);
        validatePortHeadroom(generatedPortArtifacts.size());
        List<ScenarioPassResult> passes = new ArrayList<>();

        for (int scenarioIndex = 0; scenarioIndex < scenarios.size(); scenarioIndex++) {
            int generatedPortIndex = 0;
            ScenarioDefinition scenario = scenarios.get(scenarioIndex);
            for (SuiteArtifact artifact : artifacts) {
                int port = portFor(scenarioIndex, generatedPortIndex, generatedPortArtifacts.size(), artifact.kind());
                if (artifact.kind() == SuiteArtifact.Kind.YIERDIS_JAR) {
                    generatedPortIndex++;
                }
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
                captureEnvironment(artifacts),
                artifacts,
                scenarios,
                passes,
                comparisons,
                findings
        );
    }

    private List<SuiteArtifact> artifactsInRunOrder() {
        return config.artifactsInRunOrder();
    }

    private static List<SuiteArtifact> generatedPortArtifacts(List<SuiteArtifact> artifacts) {
        List<SuiteArtifact> generatedPortArtifacts = new ArrayList<>();
        for (SuiteArtifact artifact : artifacts) {
            if (artifact.kind() == SuiteArtifact.Kind.YIERDIS_JAR) {
                generatedPortArtifacts.add(artifact);
            }
        }
        return List.copyOf(generatedPortArtifacts);
    }

    private ScenarioPassResult runPass(SuiteArtifact artifact, ScenarioDefinition scenario, int port, Path logFile) {
        SuiteHarness.RunningServer server = null;
        ObservationSnapshot before = ObservationSnapshot.empty();
        ScenarioPassResult pass;
        try {
            server = harness.startServer(artifact, scenario, config, port, logFile);
            before = requireObservation(captureObservation(artifact, port));
            List<IterationResult> iterations = new ArrayList<>();
            for (int i = 0; i < scenario.warmupIterations(); i++) {
                iterations.add(requireIteration(harness.runIteration(server, scenario, i, IterationResult.Kind.WARMUP, config)));
            }
            for (int i = 0; i < scenario.repeatIterations(); i++) {
                iterations.add(requireIteration(harness.runIteration(server, scenario, i, IterationResult.Kind.REPEAT, config)));
            }
            ObservationSnapshot after = requireObservation(captureObservation(artifact, port));
            pass = ScenarioPassResult.completed(artifact.label(), artifact.kind(), scenario, iterations, before, after);
        } catch (Exception e) {
            pass = failedPass(artifact.label(), artifact.kind(), scenario, e, before);
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
            return new ScenarioPassResult(pass.artifactLabel(), pass.artifactKind(), pass.scenario(), true, message,
                    pass.iterations(), pass.before(), pass.after(), null);
        }
    }

    private static ObservationSnapshot requireObservation(ObservationSnapshot observation) {
        return Objects.requireNonNull(observation, "observation");
    }

    private static IterationResult requireIteration(IterationResult iteration) {
        return Objects.requireNonNull(iteration, "iteration");
    }

    private static ScenarioPassResult failedPass(String artifactLabel, SuiteArtifact.Kind artifactKind,
                                                 ScenarioDefinition scenario, Throwable failure, ObservationSnapshot before) {
        String message = conciseFailureMessage(failure);
        if (before.values().isEmpty()) {
            return ScenarioPassResult.failed(artifactLabel, artifactKind, scenario, message);
        }
        return new ScenarioPassResult(artifactLabel, artifactKind, scenario, true, message, List.of(), before,
                ObservationSnapshot.empty(), null);
    }

    private ObservationSnapshot captureObservation(SuiteArtifact artifact, int allocatedPort) {
        if (artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS) {
            return harness.captureObservation(artifact.host(), artifact.port());
        }
        return harness.captureObservation(config.host(), allocatedPort);
    }

    private SuiteEnvironment captureEnvironment(List<SuiteArtifact> artifacts) {
        SuiteEnvironment environment = SuiteEnvironment.capture();
        Map<String, String> values = new LinkedHashMap<>(environment.values());
        artifacts.stream()
                .filter(artifact -> artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS)
                .findFirst()
                .ifPresent(artifact -> {
                    values.put("redis.host", artifact.host());
                    values.put("redis.port", Integer.toString(artifact.port()));
                    values.put("redis.db", Integer.toString(artifact.db()));
                    values.putAll(observationClient.captureEnvironmentMetadata(artifact));
                });
        return new SuiteEnvironment(values);
    }

    private static String conciseFailureMessage(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            message = failure.getClass().getSimpleName();
        }
        return message;
    }

    private List<ScenarioComparison> comparisons(List<ScenarioPassResult> passes) {
        ComparisonArtifacts artifacts = comparisonArtifacts();
        if (artifacts == null) {
            return List.of();
        }
        List<ScenarioComparison> comparisons = new ArrayList<>();
        for (ScenarioDefinition scenario : scenarios) {
            ScenarioPassResult baseline = findPass(passes, artifacts.baselineLabel(), scenario);
            ScenarioPassResult current = findPass(passes, artifacts.currentLabel(), scenario);
            comparisons.add(ScenarioComparison.compare(scenario, baseline, current));
        }
        return List.copyOf(comparisons);
    }

    private ComparisonArtifacts comparisonArtifacts() {
        List<SuiteArtifact> artifacts = config.artifactsInRunOrder();
        if (artifacts.size() < 2) {
            return null;
        }
        String currentLabel = config.current().label();
        SuiteArtifact baselineArtifact = null;
        for (SuiteArtifact artifact : artifacts) {
            if (!artifact.label().equals(currentLabel)) {
                baselineArtifact = artifact;
                break;
            }
        }
        if (baselineArtifact == null) {
            return null;
        }
        return new ComparisonArtifacts(baselineArtifact.label(), currentLabel);
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
                boolean diagnostic = pass.scenario().comparisonRole() == ScenarioDefinition.ComparisonRole.DIAGNOSTIC;
                out.add(new ThresholdFinding(diagnostic ? ThresholdFinding.Level.WARNING : ThresholdFinding.Level.CRITICAL,
                        pass.scenario().id(), diagnostic ? "diagnostic_pass" : "pass",
                        pass.artifactLabel() + " is not clean" + dirtySuffix(pass)));
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

    private void validatePortHeadroom(int generatedPortArtifactCount) {
        long requiredPorts = (long) scenarios.size() * generatedPortArtifactCount;
        if (requiredPorts == 0) {
            return;
        }
        long lastPort = (long) config.portBase() + requiredPorts - 1L;
        if (config.portBase() < MIN_SUITE_PORT || lastPort > MAX_PORT) {
            throw new IllegalArgumentException("portBase " + config.portBase()
                    + " does not have headroom for " + requiredPorts
                    + " contiguous jar-backed suite ports within " + MIN_SUITE_PORT + ".." + MAX_PORT);
        }
    }

    private int portFor(int scenarioIndex, int generatedPortIndex, int generatedPortArtifactCount, SuiteArtifact.Kind kind) {
        if (kind == SuiteArtifact.Kind.EXTERNAL_REDIS) {
            return config.portBase();
        }
        long offset = (long) scenarioIndex * generatedPortArtifactCount + generatedPortIndex;
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

    private record ComparisonArtifacts(String baselineLabel, String currentLabel) {
    }
}
