package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.YierdisBenchArgs;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record SuiteConfig(
        SuiteProfileName profile,
        SuiteArtifact current,
        Optional<SuiteArtifact> baseline,
        Path reportDir,
        String host,
        int portBase,
        String javaCmd,
        String xms,
        String xmx,
        String maxDirectMemory,
        YierdisBenchServerArgs baseServerArgs,
        boolean strictReplies
) {
    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

    public SuiteConfig {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(current, "current");
        baseline = baseline == null ? Optional.empty() : baseline;
        Objects.requireNonNull(reportDir, "reportDir");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(javaCmd, "javaCmd");
        Objects.requireNonNull(xms, "xms");
        Objects.requireNonNull(xmx, "xmx");
        Objects.requireNonNull(maxDirectMemory, "maxDirectMemory");
        Objects.requireNonNull(baseServerArgs, "baseServerArgs");
        baseServerArgs = baseServerArgs.copy();
        if (portBase < 0 || portBase > 65535) {
            throw new IllegalArgumentException("portBase must be in range 0..65535");
        }
    }

    public static SuiteConfig from(YierdisBenchArgs args, YierdisBenchServerArgs serverArgs) {
        Objects.requireNonNull(args, "args");
        Objects.requireNonNull(serverArgs, "serverArgs");
        if (!args.suite) {
            throw new IllegalArgumentException("suite mode is not enabled");
        }
        if (args.currentServerJar == null) {
            throw new IllegalArgumentException("suite requires currentServerJar");
        }
        if (args.serverJar != null) {
            throw new IllegalArgumentException("suite does not support serverJar");
        }
        if (args.noStartServer) {
            throw new IllegalArgumentException("suite does not support noStartServer");
        }
        if (args.comparisonMode) {
            throw new IllegalArgumentException("suite does not support comparisonMode");
        }
        if (args.nativeEval) {
            throw new IllegalArgumentException("suite does not support nativeEval");
        }

        SuiteProfileName profile = SuiteProfileName.parse(args.suiteProfile);
        SuiteArtifact current = new SuiteArtifact("current", requireRegularFile(args.currentServerJar, "currentServerJar"), "");
        Optional<SuiteArtifact> baseline = args.baselineServerJar == null
                ? Optional.empty()
                : Optional.of(new SuiteArtifact("baseline", requireRegularFile(args.baselineServerJar, "baselineServerJar"), ""));
        Path reportDir = normalizeReportDir(args.reportDir, profile);

        return new SuiteConfig(
                profile,
                current,
                baseline,
                reportDir,
                args.host,
                args.portBase,
                args.javaCmd,
                args.xms,
                args.xmx,
                args.maxDirectMemory,
                serverArgs,
                true
        );
    }

    public List<String> artifactLabels() {
        List<String> labels = new ArrayList<>();
        baseline.ifPresent(artifact -> labels.add(artifact.label()));
        labels.add(current.label());
        return labels;
    }

    private static Path normalizeReportDir(Path supplied, SuiteProfileName profile) {
        Path dir = supplied;
        if (dir == null) {
            String runId = RUN_ID_TIME.format(Instant.now()) + "-" + profile.cliName();
            dir = Path.of("target", "benchmark-reports", runId);
        }
        Path absolute = dir.toAbsolutePath().normalize();
        if (Files.exists(absolute) && !Files.isDirectory(absolute)) {
            throw new IllegalArgumentException("reportDir must be a directory: " + absolute);
        }
        return absolute;
    }

    private static Path requireRegularFile(Path path, String optionName) {
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized)) {
            throw new IllegalArgumentException(optionName + " does not exist or is not a regular file: " + normalized);
        }
        return path;
    }
}
