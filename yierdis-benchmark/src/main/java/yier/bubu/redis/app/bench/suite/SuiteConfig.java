package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.YierdisBenchArgs;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record SuiteConfig(
        SuiteProfileName profile,
        SuiteArtifact current,
        Optional<SuiteArtifact> baseline,
        List<SuiteArtifact> artifactsInRunOrder,
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
        Objects.requireNonNull(artifactsInRunOrder, "artifactsInRunOrder");
        artifactsInRunOrder = List.copyOf(artifactsInRunOrder);
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

    public SuiteConfig(
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
        this(
                profile,
                current,
                baseline,
                defaultArtifacts(current, baseline),
                reportDir,
                host,
                portBase,
                javaCmd,
                xms,
                xmx,
                maxDirectMemory,
                baseServerArgs,
                strictReplies
        );
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
        if (args.includeRedis && args.baselineServerJar != null) {
            throw new IllegalArgumentException("suite does not support baselineServerJar with includeRedis");
        }
        if (args.redisDb < 0) {
            throw new IllegalArgumentException("redisDb must be >= 0");
        }

        SuiteProfileName profile = SuiteProfileName.parse(args.suiteProfile);
        SuiteArtifact current = SuiteArtifact.yierdisJar("current", requireRegularFile(args.currentServerJar, "currentServerJar"), "");
        Optional<SuiteArtifact> baseline = args.baselineServerJar == null
                ? Optional.empty()
                : Optional.of(SuiteArtifact.yierdisJar("baseline", requireRegularFile(args.baselineServerJar, "baselineServerJar"), ""));
        validateRedisLabel(args, baseline);
        List<SuiteArtifact> artifacts = new ArrayList<>();
        if (args.includeRedis) {
            artifacts.add(SuiteArtifact.externalRedis(
                    args.redisLabel,
                    args.redisHost,
                    args.redisPort,
                    args.redisUser,
                    args.redisAuth,
                    args.redisDb
            ));
        } else {
            baseline.ifPresent(artifacts::add);
        }
        artifacts.add(current);
        Path reportDir = normalizeReportDir(args.reportDir, profile);

        return new SuiteConfig(
                profile,
                current,
                baseline,
                artifacts,
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

    private static void validateRedisLabel(YierdisBenchArgs args, Optional<SuiteArtifact> baseline) {
        String label = Objects.requireNonNull(args.redisLabel, "redisLabel");
        if ("current".equals(label) || "baseline".equals(label)) {
            throw new IllegalArgumentException("redisLabel must not collide with current or baseline");
        }
        if (baseline.isPresent() && baseline.get().label().equals(label)) {
            throw new IllegalArgumentException("redisLabel must not duplicate another artifact label");
        }
    }

    public List<String> artifactLabels() {
        List<String> labels = new ArrayList<>();
        for (SuiteArtifact artifact : artifactsInRunOrder) {
            labels.add(artifact.label());
        }
        return labels;
    }

    @Override
    public List<SuiteArtifact> artifactsInRunOrder() {
        return Collections.unmodifiableList(artifactsInRunOrder);
    }

    @Override
    public YierdisBenchServerArgs baseServerArgs() {
        return baseServerArgs.copy();
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
        return normalized;
    }

    private static List<SuiteArtifact> defaultArtifacts(SuiteArtifact current, Optional<SuiteArtifact> baseline) {
        List<SuiteArtifact> artifacts = new ArrayList<>();
        baseline.ifPresent(artifacts::add);
        artifacts.add(current);
        return artifacts;
    }
}
