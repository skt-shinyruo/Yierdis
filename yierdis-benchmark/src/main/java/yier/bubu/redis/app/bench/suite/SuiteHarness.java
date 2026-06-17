package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.util.Objects;

public interface SuiteHarness {
    RunningServer startServer(
            SuiteArtifact artifact,
            ScenarioDefinition scenario,
            SuiteConfig config,
            int port,
            Path logFile
    ) throws Exception;

    ObservationSnapshot captureObservation(String host, int port);

    IterationResult runIteration(
            RunningServer server,
            ScenarioDefinition scenario,
            int index,
            IterationResult.Kind kind,
            SuiteConfig config
    ) throws Exception;

    void stopServer(RunningServer server);

    record RunningServer(String artifactLabel, String scenarioId, int port, Path logFile, Object handle) {
        public RunningServer(String artifactLabel, String scenarioId, int port, Path logFile) {
            this(artifactLabel, scenarioId, port, logFile, null);
        }

        public RunningServer {
            Objects.requireNonNull(artifactLabel, "artifactLabel");
            Objects.requireNonNull(scenarioId, "scenarioId");
            Objects.requireNonNull(logFile, "logFile");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("port must be in range 1..65535");
            }
        }
    }
}
