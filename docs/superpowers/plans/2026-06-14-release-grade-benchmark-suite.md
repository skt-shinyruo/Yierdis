> **Superseded:** Replaced by `docs/superpowers/specs/2026-07-17-redis-benchmark-comparable-rewrite-design.md`. Retained only as historical context.

# Release-Grade Benchmark Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first-class release-grade benchmark suite mode that produces current-only and baseline/current JSON, CSV, and Markdown performance reports while preserving the existing single-run benchmark.

**Architecture:** Keep `YierdisBench` as the existing entrypoint, add suite-specific orchestration in `yier.bubu.redis.app.bench.suite`, and add a small root-package adapter layer that exposes existing benchmark internals without spreading nested `YierdisBench` types through the suite package. Build the feature through tests first: config, profile expansion, metric aggregation, thresholding, report writing, fake orchestration, then real entrypoint integration.

**Tech Stack:** Java 25, Maven, JUnit 4, picocli, existing RESP client codec, existing `yierdis-benchmark` workers and server launch arguments.

---

## Scope Check

The approved spec is one subsystem: a release-grade benchmark suite inside `yierdis-benchmark`. It touches CLI parsing, suite models, report generation, runner orchestration, a benchmark adapter, and docs, but all changes serve the same benchmark-suite feature and can be implemented as one plan with task commits.

The plan intentionally keeps raw logs, GC logs, JFR, hard release gating, and CI integration outside the first implementation because they are non-goals in the spec.

## File Structure

Create suite package files:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileName.java`: enum for `release` and `full`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`: validated suite CLI config built from `YierdisBenchArgs` and `YierdisBenchServerArgs`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java`: `current` and optional `baseline` artifact metadata.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java`: run-time environment metadata.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java`: stable scenario definition.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`: expands `release` and `full` profiles.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMetric.java`: metric name/value record.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/IterationResult.java`: one warmup or repeat result.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/MetricSummary.java`: min, median, mean, max, and sample count.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioPassResult.java`: one artifact running one scenario.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java`: baseline/current comparison status and deltas.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdPolicy.java`: soft threshold settings.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdFinding.java`: warning or critical observation.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdEvaluator.java`: threshold evaluation.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationSnapshot.java`: `STATS`, `MEMORY STATS`, and `INFO` snapshots.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`: RESP snapshot collector.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunResult.java`: complete suite result.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteJsonWriter.java`: deterministic JSON writer with no new dependency.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java`: `metrics.csv` and `comparisons.csv`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java`: `report.md`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteReportWriter.java`: writes all artifacts.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteHarness.java`: interface for server lifecycle, workload execution, and observation collection.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`: suite orchestration.

Create root benchmark adapter files:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`: real `SuiteHarness` implementation using existing benchmark internals.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadKind.java`: public workload enum for suite scenarios.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadRequest.java`: request shape passed to the real adapter.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadResult.java`: adapter result converted to suite metrics.

Modify existing files:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`: add `--suite`, `--suiteProfile`, and `--reportDir`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`: dispatch suite mode before existing comparison/single-run modes.
- `docs/project-docs/client-and-bench-internals.md`: document suite mode.
- `README.md`: add short suite example.

Create tests:

- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactoryTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteMetricSummaryTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteThresholdEvaluatorTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteReportWriterTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`

## Task 1: Suite CLI Config

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileName.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`

- [ ] **Step 1: Write the failing suite config test**

Create `SuiteConfigTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.YierdisBenchArgs;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SuiteConfigTest {
    @Test
    public void suiteRequiresCurrentJarAndAcceptsOptionalBaselineJar() throws Exception {
        Path current = regularTempJar("current");
        Path baseline = regularTempJar("baseline");

        SuiteConfig currentOnly = config(
                "--suite",
                "--currentServerJar", current.toString(),
                "--suiteProfile", "release"
        );

        Assert.assertEquals(SuiteProfileName.RELEASE, currentOnly.profile());
        Assert.assertEquals(current, currentOnly.current().jarPath());
        Assert.assertFalse(currentOnly.baseline().isPresent());
        Assert.assertTrue(currentOnly.reportDir().toString().contains("target/benchmark-reports"));
        Assert.assertEquals("current", currentOnly.current().label());

        SuiteConfig comparison = config(
                "--suite",
                "--currentServerJar", current.toString(),
                "--baselineServerJar", baseline.toString(),
                "--suiteProfile", "full"
        );

        Assert.assertEquals(SuiteProfileName.FULL, comparison.profile());
        Assert.assertEquals(List.of("baseline", "current"), comparison.artifactLabels());
        Assert.assertEquals(baseline, comparison.baseline().orElseThrow().jarPath());
    }

    @Test
    public void suiteRejectsInvalidModeCombinations() throws Exception {
        Path current = regularTempJar("current");
        Path single = regularTempJar("single");

        assertRejects("suite requires currentServerJar", "--suite");
        assertRejects("suite does not support serverJar",
                "--suite", "--currentServerJar", current.toString(), "--serverJar", single.toString());
        assertRejects("suite does not support noStartServer",
                "--suite", "--currentServerJar", current.toString(), "--noStartServer");
        assertRejects("suite does not support comparisonMode",
                "--suite", "--currentServerJar", current.toString(), "--comparisonMode");
        assertRejects("suite does not support nativeEval",
                "--suite", "--currentServerJar", current.toString(), "--nativeEval");
    }

    @Test
    public void suiteRejectsMissingJarAndBadReportDir() throws Exception {
        Path missing = Files.createTempDirectory("suite-missing-").resolve("server.jar");
        IllegalArgumentException missingJar = assertRejects(
                "currentServerJar",
                "--suite",
                "--currentServerJar", missing.toString()
        );
        Assert.assertTrue(missingJar.getMessage().contains(missing.toAbsolutePath().toString()));

        Path current = regularTempJar("current");
        Path reportFile = Files.createTempFile("suite-report", ".txt");
        assertRejects("reportDir must be a directory",
                "--suite", "--currentServerJar", current.toString(), "--reportDir", reportFile.toString());
    }

    @Test
    public void profileParsingIsCaseInsensitiveAndRejectsUnknownNames() throws Exception {
        Path current = regularTempJar("current");

        Assert.assertEquals(
                SuiteProfileName.RELEASE,
                config("--suite", "--currentServerJar", current.toString(), "--suiteProfile", "ReLeAsE").profile()
        );
        Assert.assertEquals(
                SuiteProfileName.FULL,
                config("--suite", "--currentServerJar", current.toString(), "--suiteProfile", "FULL").profile()
        );

        assertRejects("suiteProfile must be one of",
                "--suite", "--currentServerJar", current.toString(), "--suiteProfile", "nightly");
    }

    private static SuiteConfig config(String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return SuiteConfig.from(args, serverArgs);
    }

    private static IllegalArgumentException assertRejects(String messagePart, String... argv) {
        try {
            config(argv);
            Assert.fail("expected rejection containing " + messagePart);
            return null;
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
            return e;
        }
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
```

- [ ] **Step 2: Run the failing suite config test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `SuiteConfig`, `SuiteProfileName`, `SuiteArtifact`, and suite CLI args do not exist.

- [ ] **Step 3: Add suite CLI args**

Modify `YierdisBenchArgs.java` by adding these fields after `currentServerJar`:

```java
    @Option(names = "--suite", description = "Run the release-grade benchmark suite.")
    public boolean suite;

    @Option(names = "--suiteProfile", defaultValue = "release", description = "Suite profile: release|full.")
    public String suiteProfile = "release";

    @Option(names = "--reportDir", description = "Directory for suite JSON, CSV, and Markdown report artifacts.")
    public Path reportDir;
```

- [ ] **Step 4: Add suite profile and artifact classes**

Create `SuiteProfileName.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Locale;

public enum SuiteProfileName {
    RELEASE("release"),
    FULL("full");

    private final String cliName;

    SuiteProfileName(String cliName) {
        this.cliName = cliName;
    }

    public String cliName() {
        return cliName;
    }

    public static SuiteProfileName parse(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (SuiteProfileName candidate : values()) {
            if (candidate.cliName.equals(normalized)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("suiteProfile must be one of: release, full");
    }
}
```

Create `SuiteArtifact.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.util.Objects;

public record SuiteArtifact(String label, Path jarPath, String commitLabel) {
    public SuiteArtifact {
        if (!"current".equals(label) && !"baseline".equals(label)) {
            throw new IllegalArgumentException("artifact label must be current or baseline");
        }
        Objects.requireNonNull(jarPath, "jarPath");
        commitLabel = commitLabel == null ? "" : commitLabel;
    }
}
```

- [ ] **Step 5: Add suite config validation**

Create `SuiteConfig.java`:

```java
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
                serverArgs.copy(),
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
```

- [ ] **Step 6: Run suite config test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 7: Commit suite config**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileName.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java
git commit -m "feat: add benchmark suite config"
```

## Task 2: Suite Profiles And Scenario Model

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadKind.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactoryTest.java`

- [ ] **Step 1: Write the failing profile expansion test**

Create `SuiteProfileFactoryTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SuiteProfileFactoryTest {
    @Test
    public void releaseProfileHasStableCoreAndRiskScenarios() {
        List<ScenarioDefinition> scenarios = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);

        assertHasScenario(scenarios, "release-ping-latency");
        assertHasScenario(scenarios, "release-set-get-256b-c64-p8");
        assertHasScenario(scenarios, "release-append-256b-c64-p8");
        assertHasScenario(scenarios, "release-hll-sparse-c64-p8");
        assertHasScenario(scenarios, "release-hll-dense-c64-p8");
        assertHasScenario(scenarios, "release-native-defrag-append");
        assertHasScenario(scenarios, "release-maxmemory-eviction");
        assertHasScenario(scenarios, "release-ttl-expiration");

        ScenarioDefinition setGet = scenario(scenarios, "release-set-get-256b-c64-p8");
        Assert.assertEquals(BenchWorkloadKind.SET_GET, setGet.workload());
        Assert.assertEquals(256, setGet.dataSize());
        Assert.assertEquals(64, setGet.clients());
        Assert.assertEquals(8, setGet.pipeline());
        Assert.assertTrue(setGet.repeatIterations() >= 3);
        Assert.assertTrue(setGet.warmupIterations() >= 1);
    }

    @Test
    public void fullProfileIncludesReleaseScenariosAndExtendedFamilies() {
        List<ScenarioDefinition> release = SuiteProfileFactory.expand(SuiteProfileName.RELEASE);
        List<ScenarioDefinition> full = SuiteProfileFactory.expand(SuiteProfileName.FULL);

        Assert.assertTrue(full.size() > release.size());
        for (ScenarioDefinition releaseScenario : release) {
            assertHasScenario(full, releaseScenario.id());
        }

        assertHasScenario(full, "full-list-lpush");
        assertHasScenario(full, "full-hash-hset");
        assertHasScenario(full, "full-set-sadd");
        assertHasScenario(full, "full-zset-zadd");
        assertHasScenario(full, "full-scan-count-100");
        assertHasScenario(full, "full-mixed-read-write-hot");
    }

    @Test
    public void scenarioIdsAreUniqueAndUseStableLowercaseNames() {
        for (SuiteProfileName profile : SuiteProfileName.values()) {
            Set<String> ids = new HashSet<>();
            for (ScenarioDefinition scenario : SuiteProfileFactory.expand(profile)) {
                Assert.assertTrue("duplicate id " + scenario.id(), ids.add(scenario.id()));
                Assert.assertTrue("id must be lowercase kebab: " + scenario.id(), scenario.id().matches("[a-z0-9-]+"));
                Assert.assertTrue("requests must be positive", scenario.requests() > 0);
                Assert.assertTrue("keyspace must be positive", scenario.keyspace() > 0);
                Assert.assertTrue("clients must be positive", scenario.clients() > 0);
                Assert.assertTrue("pipeline must be positive", scenario.pipeline() > 0);
            }
        }
    }

    private static ScenarioDefinition scenario(List<ScenarioDefinition> scenarios, String id) {
        for (ScenarioDefinition scenario : scenarios) {
            if (scenario.id().equals(id)) {
                return scenario;
            }
        }
        Assert.fail("missing scenario " + id);
        return null;
    }

    private static void assertHasScenario(List<ScenarioDefinition> scenarios, String id) {
        scenario(scenarios, id);
    }
}
```

- [ ] **Step 2: Run the failing profile test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteProfileFactoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because profile and scenario classes do not exist.

- [ ] **Step 3: Add public workload kind enum**

Create `BenchWorkloadKind.java`:

```java
package yier.bubu.redis.app.bench;

public enum BenchWorkloadKind {
    PING,
    SET_GET,
    APPEND,
    HLL_SPARSE,
    HLL_DENSE,
    HLL_PFCOUNT,
    NATIVE_DEFRAG_APPEND,
    MAXMEMORY_EVICTION,
    TTL_EXPIRATION,
    LIST_LPUSH,
    HASH_HSET,
    SET_SADD,
    ZSET_ZADD,
    SCAN,
    MIXED_READ_WRITE
}
```

- [ ] **Step 4: Add scenario definition record**

Create `ScenarioDefinition.java`:

```java
package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.Objects;

public record ScenarioDefinition(
        String id,
        String displayName,
        BenchWorkloadKind workload,
        int keyspace,
        int dataSize,
        int requests,
        int clients,
        int pipeline,
        int warmupIterations,
        int repeatIterations,
        boolean latency
) {
    public ScenarioDefinition {
        requireStableId(id);
        displayName = Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(workload, "workload");
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (dataSize < 0) {
            throw new IllegalArgumentException("dataSize must be >= 0");
        }
        if (requests <= 0) {
            throw new IllegalArgumentException("requests must be > 0");
        }
        if (clients <= 0) {
            throw new IllegalArgumentException("clients must be > 0");
        }
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        if (warmupIterations < 0) {
            throw new IllegalArgumentException("warmupIterations must be >= 0");
        }
        if (repeatIterations <= 0) {
            throw new IllegalArgumentException("repeatIterations must be > 0");
        }
    }

    private static void requireStableId(String id) {
        if (id == null || !id.matches("[a-z0-9-]+")) {
            throw new IllegalArgumentException("scenario id must be lowercase kebab-case");
        }
    }
}
```

- [ ] **Step 5: Add profile factory**

Create `SuiteProfileFactory.java`:

```java
package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.ArrayList;
import java.util.List;

public final class SuiteProfileFactory {
    private SuiteProfileFactory() {
    }

    public static List<ScenarioDefinition> expand(SuiteProfileName profile) {
        List<ScenarioDefinition> scenarios = new ArrayList<>(releaseScenarios());
        if (profile == SuiteProfileName.FULL) {
            scenarios.addAll(fullOnlyScenarios());
        }
        return List.copyOf(scenarios);
    }

    private static List<ScenarioDefinition> releaseScenarios() {
        return List.of(
                scenario("release-ping-latency", "PING latency baseline", BenchWorkloadKind.PING, 10_000, 0, 50_000, 16, 1, 1, 5, true),
                scenario("release-set-get-128b-c32-p4", "SET/GET 128B c32 p4", BenchWorkloadKind.SET_GET, 200_000, 128, 200_000, 32, 4, 1, 5, true),
                scenario("release-set-get-256b-c64-p8", "SET/GET 256B c64 p8", BenchWorkloadKind.SET_GET, 500_000, 256, 500_000, 64, 8, 1, 5, true),
                scenario("release-set-get-1024b-c64-p8", "SET/GET 1024B c64 p8", BenchWorkloadKind.SET_GET, 200_000, 1024, 200_000, 64, 8, 1, 5, true),
                scenario("release-append-256b-c64-p8", "APPEND 256B c64 p8", BenchWorkloadKind.APPEND, 200_000, 256, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-sparse-c64-p8", "HLL sparse PFADD c64 p8", BenchWorkloadKind.HLL_SPARSE, 200_000, 0, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-dense-c64-p8", "HLL dense PFADD c64 p8", BenchWorkloadKind.HLL_DENSE, 4096, 0, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-pfcount-c64-p8", "HLL PFCOUNT c64 p8", BenchWorkloadKind.HLL_PFCOUNT, 4096, 0, 300_000, 64, 8, 1, 5, false),
                scenario("release-native-defrag-append", "Native defrag APPEND p99", BenchWorkloadKind.NATIVE_DEFRAG_APPEND, 4096, 256, 50_000, 8, 4, 1, 5, true),
                scenario("release-maxmemory-eviction", "Maxmemory eviction pressure", BenchWorkloadKind.MAXMEMORY_EVICTION, 50_000, 512, 100_000, 32, 4, 1, 5, false),
                scenario("release-ttl-expiration", "TTL expiration pressure", BenchWorkloadKind.TTL_EXPIRATION, 50_000, 128, 100_000, 32, 4, 1, 5, false)
        );
    }

    private static List<ScenarioDefinition> fullOnlyScenarios() {
        return List.of(
                scenario("full-list-lpush", "List LPUSH", BenchWorkloadKind.LIST_LPUSH, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-hash-hset", "Hash HSET", BenchWorkloadKind.HASH_HSET, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-set-sadd", "Set SADD", BenchWorkloadKind.SET_SADD, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-zset-zadd", "ZSet ZADD", BenchWorkloadKind.ZSET_ZADD, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-scan-count-100", "SCAN COUNT 100", BenchWorkloadKind.SCAN, 200_000, 128, 50_000, 16, 1, 1, 7, true),
                scenario("full-mixed-read-write-hot", "Mixed read/write hot keys", BenchWorkloadKind.MIXED_READ_WRITE, 10_000, 256, 500_000, 128, 8, 1, 7, true)
        );
    }

    private static ScenarioDefinition scenario(
            String id,
            String displayName,
            BenchWorkloadKind workload,
            int keyspace,
            int dataSize,
            int requests,
            int clients,
            int pipeline,
            int warmupIterations,
            int repeatIterations,
            boolean latency
    ) {
        return new ScenarioDefinition(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency);
    }
}
```

- [ ] **Step 6: Run profile tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteProfileFactoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 7: Commit profile expansion**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadKind.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactoryTest.java
git commit -m "feat: define benchmark suite profiles"
```

## Task 3: Metric Aggregation, Comparability, And Thresholds

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMetric.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/IterationResult.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/MetricSummary.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioPassResult.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdPolicy.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdFinding.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdEvaluator.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteMetricSummaryTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteThresholdEvaluatorTest.java`

- [ ] **Step 1: Write failing metric summary test**

Create `SuiteMetricSummaryTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SuiteMetricSummaryTest {
    @Test
    public void summarizesOnlyRepeatIterations() {
        IterationResult warmup = IterationResult.warmup(0, List.of(new SuiteMetric("qps", 100.0), new SuiteMetric("errors", 0.0)));
        IterationResult repeat0 = IterationResult.repeat(0, List.of(new SuiteMetric("qps", 200.0), new SuiteMetric("errors", 0.0)));
        IterationResult repeat1 = IterationResult.repeat(1, List.of(new SuiteMetric("qps", 300.0), new SuiteMetric("errors", 1.0)));
        IterationResult repeat2 = IterationResult.repeat(2, List.of(new SuiteMetric("qps", 400.0), new SuiteMetric("errors", 0.0)));

        Map<String, MetricSummary> summaries = MetricSummary.summarizeRepeats(List.of(warmup, repeat0, repeat1, repeat2));

        MetricSummary qps = summaries.get("qps");
        Assert.assertEquals(3, qps.sampleCount());
        Assert.assertEquals(200.0, qps.min(), 0.001);
        Assert.assertEquals(300.0, qps.median(), 0.001);
        Assert.assertEquals(300.0, qps.mean(), 0.001);
        Assert.assertEquals(400.0, qps.max(), 0.001);

        MetricSummary errors = summaries.get("errors");
        Assert.assertEquals(1.0 / 3.0, errors.mean(), 0.001);
    }

    @Test
    public void passResultIsDirtyWhenFailedOrErrorsArePresent() {
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING", yier.bubu.redis.app.bench.BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult clean = ScenarioPassResult.completed("current", scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 0.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult dirty = ScenarioPassResult.completed("current", scenario, List.of(
                IterationResult.repeat(0, List.of(new SuiteMetric("qps", 1000.0), new SuiteMetric("errors", 2.0)))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
        ScenarioPassResult failed = ScenarioPassResult.failed("current", scenario, "server did not become ready");

        Assert.assertTrue(clean.clean());
        Assert.assertFalse(dirty.clean());
        Assert.assertFalse(failed.clean());
    }
}
```

- [ ] **Step 2: Write failing threshold evaluator test**

Create `SuiteThresholdEvaluatorTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.List;

public class SuiteThresholdEvaluatorTest {
    @Test
    public void marksComparableRegressionWarnings() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult current = pass("current", scenario, 850.0, 12.0, 24.0, 0.0);

        ScenarioComparison comparison = ScenarioComparison.compare(scenario, baseline, current);
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        Assert.assertTrue(comparison.comparable());
        assertFinding(findings, "qps", ThresholdFinding.Level.WARNING);
        assertFinding(findings, "p95_ms", ThresholdFinding.Level.WARNING);
        assertFinding(findings, "p99_ms", ThresholdFinding.Level.WARNING);
    }

    @Test
    public void marksErrorsAndNonComparableAsCriticalObservations() {
        ScenarioDefinition scenario = scenario();
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 10.0, 20.0, 0.0);
        ScenarioPassResult currentWithErrors = pass("current", scenario, 1000.0, 10.0, 20.0, 1.0);
        ScenarioPassResult failed = ScenarioPassResult.failed("current", scenario, "workload failed");

        List<ThresholdFinding> dirtyFindings = ThresholdEvaluator.evaluate(
                ScenarioComparison.compare(scenario, baseline, currentWithErrors),
                ThresholdPolicy.defaults()
        );
        assertFinding(dirtyFindings, "errors", ThresholdFinding.Level.CRITICAL);
        assertFinding(dirtyFindings, "comparability", ThresholdFinding.Level.CRITICAL);

        List<ThresholdFinding> failedFindings = ThresholdEvaluator.evaluate(
                ScenarioComparison.compare(scenario, baseline, failed),
                ThresholdPolicy.defaults()
        );
        assertFinding(failedFindings, "comparability", ThresholdFinding.Level.CRITICAL);
    }

    private static ScenarioDefinition scenario() {
        return new ScenarioDefinition("release-set-get-256b-c64-p8", "SET/GET", BenchWorkloadKind.SET_GET,
                100, 256, 1000, 8, 4, 0, 1, true);
    }

    private static ScenarioPassResult pass(String artifact, ScenarioDefinition scenario, double qps, double p95, double p99, double errors) {
        return ScenarioPassResult.completed(artifact, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", qps),
                        new SuiteMetric("p95_ms", p95),
                        new SuiteMetric("p99_ms", p99),
                        new SuiteMetric("errors", errors)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
    }

    private static void assertFinding(List<ThresholdFinding> findings, String metric, ThresholdFinding.Level level) {
        for (ThresholdFinding finding : findings) {
            if (finding.metric().equals(metric) && finding.level() == level) {
                return;
            }
        }
        Assert.fail("missing " + level + " finding for " + metric + " in " + findings);
    }
}
```

- [ ] **Step 3: Run failing metric and threshold tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteMetricSummaryTest,SuiteThresholdEvaluatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because metric, pass result, comparison, threshold, and observation classes do not exist.

- [ ] **Step 4: Add metric and iteration model**

Create `SuiteMetric.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Objects;

public record SuiteMetric(String name, double value) {
    public SuiteMetric {
        if (name == null || !name.matches("[a-z0-9_]+")) {
            throw new IllegalArgumentException("metric name must be lowercase snake_case");
        }
    }
}
```

Create `IterationResult.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.List;
import java.util.Objects;

public record IterationResult(Kind kind, int index, List<SuiteMetric> metrics) {
    public enum Kind {
        WARMUP,
        REPEAT
    }

    public IterationResult {
        Objects.requireNonNull(kind, "kind");
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        metrics = metrics == null ? List.of() : List.copyOf(metrics);
    }

    public static IterationResult warmup(int index, List<SuiteMetric> metrics) {
        return new IterationResult(Kind.WARMUP, index, metrics);
    }

    public static IterationResult repeat(int index, List<SuiteMetric> metrics) {
        return new IterationResult(Kind.REPEAT, index, metrics);
    }
}
```

- [ ] **Step 5: Add observation snapshot value object**

Create `ObservationSnapshot.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Map;

public record ObservationSnapshot(Map<String, String> values) {
    public ObservationSnapshot {
        values = values == null ? Map.of() : Map.copyOf(values);
    }

    public static ObservationSnapshot empty() {
        return new ObservationSnapshot(Map.of());
    }
}
```

- [ ] **Step 6: Add metric summary and pass result**

Create `MetricSummary.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record MetricSummary(String name, int sampleCount, double min, double median, double mean, double max) {
    public static Map<String, MetricSummary> summarizeRepeats(List<IterationResult> iterations) {
        Map<String, List<Double>> valuesByName = new LinkedHashMap<>();
        for (IterationResult iteration : iterations) {
            if (iteration.kind() != IterationResult.Kind.REPEAT) {
                continue;
            }
            for (SuiteMetric metric : iteration.metrics()) {
                valuesByName.computeIfAbsent(metric.name(), ignored -> new ArrayList<>()).add(metric.value());
            }
        }

        Map<String, MetricSummary> summaries = new LinkedHashMap<>();
        for (Map.Entry<String, List<Double>> entry : valuesByName.entrySet()) {
            summaries.put(entry.getKey(), of(entry.getKey(), entry.getValue()));
        }
        return summaries;
    }

    private static MetricSummary of(String name, List<Double> source) {
        List<Double> values = new ArrayList<>(source);
        values.sort(Comparator.naturalOrder());
        int n = values.size();
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        double median = n % 2 == 1
                ? values.get(n / 2)
                : (values.get(n / 2 - 1) + values.get(n / 2)) / 2.0;
        return new MetricSummary(name, n, values.get(0), median, sum / n, values.get(n - 1));
    }
}
```

Create `ScenarioPassResult.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ScenarioPassResult(
        String artifactLabel,
        ScenarioDefinition scenario,
        boolean failed,
        String failureMessage,
        List<IterationResult> iterations,
        ObservationSnapshot before,
        ObservationSnapshot after,
        Map<String, MetricSummary> summaries
) {
    public ScenarioPassResult {
        Objects.requireNonNull(artifactLabel, "artifactLabel");
        Objects.requireNonNull(scenario, "scenario");
        failureMessage = failureMessage == null ? "" : failureMessage;
        iterations = iterations == null ? List.of() : List.copyOf(iterations);
        before = before == null ? ObservationSnapshot.empty() : before;
        after = after == null ? ObservationSnapshot.empty() : after;
        summaries = summaries == null ? MetricSummary.summarizeRepeats(iterations) : Map.copyOf(summaries);
    }

    public static ScenarioPassResult completed(
            String artifactLabel,
            ScenarioDefinition scenario,
            List<IterationResult> iterations,
            ObservationSnapshot before,
            ObservationSnapshot after
    ) {
        return new ScenarioPassResult(artifactLabel, scenario, false, "", iterations, before, after, null);
    }

    public static ScenarioPassResult failed(String artifactLabel, ScenarioDefinition scenario, String failureMessage) {
        return new ScenarioPassResult(artifactLabel, scenario, true, failureMessage, List.of(),
                ObservationSnapshot.empty(), ObservationSnapshot.empty(), Map.of());
    }

    public boolean clean() {
        if (failed) {
            return false;
        }
        MetricSummary errors = summaries.get("errors");
        return errors == null || errors.max() == 0.0;
    }
}
```

- [ ] **Step 7: Add comparison and threshold evaluator**

Create `ScenarioComparison.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record ScenarioComparison(
        ScenarioDefinition scenario,
        ScenarioPassResult baseline,
        ScenarioPassResult current,
        boolean comparable,
        String nonComparableReason,
        Map<String, Double> deltaPercentByMetric
) {
    public static ScenarioComparison compare(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current) {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");
        boolean comparable = baseline.clean() && current.clean();
        String reason = "";
        if (!comparable) {
            reason = baseline.clean() ? "current is not clean" : "baseline is not clean";
        }
        Map<String, Double> deltas = comparable ? deltas(baseline, current) : Map.of();
        return new ScenarioComparison(scenario, baseline, current, comparable, reason, deltas);
    }

    private static Map<String, Double> deltas(ScenarioPassResult baseline, ScenarioPassResult current) {
        Map<String, Double> out = new LinkedHashMap<>();
        for (Map.Entry<String, MetricSummary> entry : current.summaries().entrySet()) {
            MetricSummary base = baseline.summaries().get(entry.getKey());
            if (base == null) {
                continue;
            }
            double baseValue = base.mean();
            if (baseValue == 0.0) {
                continue;
            }
            out.put(entry.getKey(), ((entry.getValue().mean() - baseValue) * 100.0) / baseValue);
        }
        return out;
    }
}
```

Create `ThresholdPolicy.java`:

```java
package yier.bubu.redis.app.bench.suite;

public record ThresholdPolicy(double qpsDropPercent, double latencyIncreasePercent) {
    public static ThresholdPolicy defaults() {
        return new ThresholdPolicy(10.0, 15.0);
    }
}
```

Create `ThresholdFinding.java`:

```java
package yier.bubu.redis.app.bench.suite;

public record ThresholdFinding(Level level, String scenarioId, String metric, String message) {
    public enum Level {
        WARNING,
        CRITICAL
    }
}
```

Create `ThresholdEvaluator.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.ArrayList;
import java.util.List;

public final class ThresholdEvaluator {
    private ThresholdEvaluator() {
    }

    public static List<ThresholdFinding> evaluate(ScenarioComparison comparison, ThresholdPolicy policy) {
        List<ThresholdFinding> findings = new ArrayList<>();
        String scenarioId = comparison.scenario().id();
        if (!comparison.comparable()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenarioId, "comparability", comparison.nonComparableReason()));
        }
        addErrorFinding(findings, scenarioId, comparison.baseline());
        addErrorFinding(findings, scenarioId, comparison.current());
        if (!comparison.comparable()) {
            return List.copyOf(findings);
        }

        addQpsFinding(findings, comparison, policy);
        addLatencyFinding(findings, comparison, policy, "p95_ms");
        addLatencyFinding(findings, comparison, policy, "p99_ms");
        return List.copyOf(findings);
    }

    private static void addErrorFinding(List<ThresholdFinding> findings, String scenarioId, ScenarioPassResult pass) {
        MetricSummary errors = pass.summaries().get("errors");
        if (errors != null && errors.max() > 0.0) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenarioId, "errors",
                    pass.artifactLabel() + " recorded benchmark errors"));
        }
    }

    private static void addQpsFinding(List<ThresholdFinding> findings, ScenarioComparison comparison, ThresholdPolicy policy) {
        Double delta = comparison.deltaPercentByMetric().get("qps");
        if (delta != null && delta < -policy.qpsDropPercent()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, comparison.scenario().id(), "qps",
                    "QPS decreased by " + format(delta) + "%"));
        }
    }

    private static void addLatencyFinding(List<ThresholdFinding> findings, ScenarioComparison comparison, ThresholdPolicy policy, String metric) {
        Double delta = comparison.deltaPercentByMetric().get(metric);
        if (delta != null && delta > policy.latencyIncreasePercent()) {
            findings.add(new ThresholdFinding(ThresholdFinding.Level.WARNING, comparison.scenario().id(), metric,
                    metric + " increased by " + format(delta) + "%"));
        }
    }

    private static String format(double value) {
        return String.format(java.util.Locale.ROOT, "%.3f", value);
    }
}
```

- [ ] **Step 8: Run metric and threshold tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteMetricSummaryTest,SuiteThresholdEvaluatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 9: Commit metrics and thresholds**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMetric.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/IterationResult.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/MetricSummary.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioPassResult.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdPolicy.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdFinding.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ThresholdEvaluator.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteMetricSummaryTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteThresholdEvaluatorTest.java
git commit -m "feat: summarize suite benchmark metrics"
```

## Task 4: Suite Report Writers

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunResult.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteJsonWriter.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteReportWriter.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteReportWriterTest.java`

- [ ] **Step 1: Write failing report writer test**

Create `SuiteReportWriterTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class SuiteReportWriterTest {
    @Test
    public void writesJsonCsvAndMarkdownArtifacts() throws Exception {
        Path dir = Files.createTempDirectory("suite-report-");
        ScenarioDefinition scenario = new ScenarioDefinition("release-ping-latency", "PING latency", BenchWorkloadKind.PING,
                10, 0, 100, 1, 1, 0, 1, true);
        ScenarioPassResult baseline = pass("baseline", scenario, 1000.0, 1.0, 2.0);
        ScenarioPassResult current = pass("current", scenario, 900.0, 1.4, 2.5);
        ScenarioComparison comparison = ScenarioComparison.compare(scenario, baseline, current);
        List<ThresholdFinding> findings = ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults());

        SuiteRunResult result = new SuiteRunResult(
                "run-1",
                SuiteProfileName.RELEASE,
                Instant.parse("2026-06-14T00:00:00Z"),
                Instant.parse("2026-06-14T01:00:00Z"),
                new SuiteEnvironment(Map.of("java.version", "25")),
                List.of(new SuiteArtifact("baseline", Path.of("baseline.jar"), "base"),
                        new SuiteArtifact("current", Path.of("current.jar"), "curr")),
                List.of(scenario),
                List.of(baseline, current),
                List.of(comparison),
                findings
        );

        SuiteReportWriter.writeAll(result, dir);

        String json = Files.readString(dir.resolve("suite-result.json"));
        String metrics = Files.readString(dir.resolve("metrics.csv"));
        String comparisons = Files.readString(dir.resolve("comparisons.csv"));
        String markdown = Files.readString(dir.resolve("report.md"));

        Assert.assertTrue(json.contains("\"runId\":\"run-1\""));
        Assert.assertTrue(json.contains("\"profile\":\"release\""));
        Assert.assertTrue(json.contains("\"scenarioId\":\"release-ping-latency\""));
        Assert.assertTrue(metrics.startsWith("artifact,scenario,iteration_group,metric,sample_count,min,median,mean,max"));
        Assert.assertTrue(metrics.contains("current,release-ping-latency,repeat,qps"));
        Assert.assertTrue(comparisons.startsWith("scenario,metric,baseline,current,delta_percent,status"));
        Assert.assertTrue(comparisons.contains("release-ping-latency,qps,1000.000,900.000,-10.000,warning"));
        Assert.assertTrue(markdown.contains("# Yierdis Benchmark Suite Report"));
        Assert.assertTrue(markdown.contains("release-ping-latency"));
        Assert.assertTrue(markdown.contains("WARNING"));
    }

    private static ScenarioPassResult pass(String artifact, ScenarioDefinition scenario, double qps, double p95, double p99) {
        return ScenarioPassResult.completed(artifact, scenario, List.of(
                IterationResult.repeat(0, List.of(
                        new SuiteMetric("qps", qps),
                        new SuiteMetric("p95_ms", p95),
                        new SuiteMetric("p99_ms", p99),
                        new SuiteMetric("errors", 0.0)
                ))
        ), ObservationSnapshot.empty(), ObservationSnapshot.empty());
    }
}
```

- [ ] **Step 2: Run failing report test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteReportWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because report model and writers do not exist.

- [ ] **Step 3: Add suite run and environment records**

Create `SuiteEnvironment.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Map;

public record SuiteEnvironment(Map<String, String> values) {
    public SuiteEnvironment {
        values = values == null ? Map.of() : Map.copyOf(values);
    }

    public static SuiteEnvironment capture() {
        return new SuiteEnvironment(Map.of(
                "java.version", System.getProperty("java.version", ""),
                "java.vm.name", System.getProperty("java.vm.name", ""),
                "os.name", System.getProperty("os.name", ""),
                "os.arch", System.getProperty("os.arch", ""),
                "available.processors", Integer.toString(Runtime.getRuntime().availableProcessors())
        ));
    }
}
```

Create `SuiteRunResult.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record SuiteRunResult(
        String runId,
        SuiteProfileName profile,
        Instant startedAt,
        Instant finishedAt,
        SuiteEnvironment environment,
        List<SuiteArtifact> artifacts,
        List<ScenarioDefinition> scenarios,
        List<ScenarioPassResult> passes,
        List<ScenarioComparison> comparisons,
        List<ThresholdFinding> findings
) {
    public SuiteRunResult {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(finishedAt, "finishedAt");
        Objects.requireNonNull(environment, "environment");
        artifacts = artifacts == null ? List.of() : List.copyOf(artifacts);
        scenarios = scenarios == null ? List.of() : List.copyOf(scenarios);
        passes = passes == null ? List.of() : List.copyOf(passes);
        comparisons = comparisons == null ? List.of() : List.copyOf(comparisons);
        findings = findings == null ? List.of() : List.copyOf(findings);
    }
}
```

- [ ] **Step 4: Add deterministic JSON writer**

Create `SuiteJsonWriter.java` with a small writer that escapes strings and emits stable fields. Use this public entrypoint:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Map;

public final class SuiteJsonWriter {
    private SuiteJsonWriter() {
    }

    public static String write(SuiteRunResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        field(sb, "runId", result.runId()).append(',');
        field(sb, "profile", result.profile().cliName()).append(',');
        field(sb, "startedAt", result.startedAt().toString()).append(',');
        field(sb, "finishedAt", result.finishedAt().toString()).append(',');
        sb.append("\"environment\":");
        stringMap(sb, result.environment().values()).append(',');
        sb.append("\"scenarios\":[");
        for (int i = 0; i < result.scenarios().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            ScenarioDefinition scenario = result.scenarios().get(i);
            sb.append('{');
            field(sb, "scenarioId", scenario.id()).append(',');
            field(sb, "displayName", scenario.displayName()).append(',');
            field(sb, "workload", scenario.workload().name()).append(',');
            numericField(sb, "requests", scenario.requests());
            sb.append('}');
        }
        sb.append("],\"passes\":[");
        for (int i = 0; i < result.passes().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            ScenarioPassResult pass = result.passes().get(i);
            sb.append('{');
            field(sb, "artifact", pass.artifactLabel()).append(',');
            field(sb, "scenarioId", pass.scenario().id()).append(',');
            sb.append("\"failed\":").append(pass.failed()).append(',');
            field(sb, "failureMessage", pass.failureMessage()).append(',');
            sb.append("\"summaries\":");
            summaries(sb, pass);
            sb.append('}');
        }
        sb.append("],\"findings\":[");
        for (int i = 0; i < result.findings().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            ThresholdFinding finding = result.findings().get(i);
            sb.append('{');
            field(sb, "level", finding.level().name()).append(',');
            field(sb, "scenarioId", finding.scenarioId()).append(',');
            field(sb, "metric", finding.metric()).append(',');
            field(sb, "message", finding.message());
            sb.append('}');
        }
        sb.append("]}");
        return sb.toString();
    }

    private static StringBuilder summaries(StringBuilder sb, ScenarioPassResult pass) {
        sb.append('{');
        int i = 0;
        for (Map.Entry<String, MetricSummary> entry : pass.summaries().entrySet()) {
            if (i++ > 0) {
                sb.append(',');
            }
            sb.append('"').append(escape(entry.getKey())).append("\":{");
            numericField(sb, "sampleCount", entry.getValue().sampleCount()).append(',');
            numericField(sb, "min", entry.getValue().min()).append(',');
            numericField(sb, "median", entry.getValue().median()).append(',');
            numericField(sb, "mean", entry.getValue().mean()).append(',');
            numericField(sb, "max", entry.getValue().max());
            sb.append('}');
        }
        return sb.append('}');
    }

    private static StringBuilder stringMap(StringBuilder sb, Map<String, String> values) {
        sb.append('{');
        int i = 0;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (i++ > 0) {
                sb.append(',');
            }
            field(sb, entry.getKey(), entry.getValue());
        }
        return sb.append('}');
    }

    private static StringBuilder field(StringBuilder sb, String name, String value) {
        return sb.append('"').append(escape(name)).append("\":\"").append(escape(value)).append('"');
    }

    private static StringBuilder numericField(StringBuilder sb, String name, double value) {
        return sb.append('"').append(escape(name)).append("\":")
                .append(String.format(java.util.Locale.ROOT, "%.3f", value));
    }

    private static String escape(String value) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> out.append(c);
            }
        }
        return out.toString();
    }
}
```

- [ ] **Step 5: Add CSV, Markdown, and file writer**

Create `SuiteCsvWriter.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Locale;
import java.util.Map;

public final class SuiteCsvWriter {
    private SuiteCsvWriter() {
    }

    public static String metricsCsv(SuiteRunResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("artifact,scenario,iteration_group,metric,sample_count,min,median,mean,max\n");
        for (ScenarioPassResult pass : result.passes()) {
            for (Map.Entry<String, MetricSummary> entry : pass.summaries().entrySet()) {
                MetricSummary summary = entry.getValue();
                sb.append(csv(pass.artifactLabel())).append(',')
                        .append(csv(pass.scenario().id())).append(',')
                        .append("repeat").append(',')
                        .append(csv(entry.getKey())).append(',')
                        .append(summary.sampleCount()).append(',')
                        .append(format(summary.min())).append(',')
                        .append(format(summary.median())).append(',')
                        .append(format(summary.mean())).append(',')
                        .append(format(summary.max())).append('\n');
            }
        }
        return sb.toString();
    }

    public static String comparisonsCsv(SuiteRunResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("scenario,metric,baseline,current,delta_percent,status\n");
        for (ScenarioComparison comparison : result.comparisons()) {
            for (Map.Entry<String, Double> delta : comparison.deltaPercentByMetric().entrySet()) {
                MetricSummary baseline = comparison.baseline().summaries().get(delta.getKey());
                MetricSummary current = comparison.current().summaries().get(delta.getKey());
                if (baseline == null || current == null) {
                    continue;
                }
                sb.append(csv(comparison.scenario().id())).append(',')
                        .append(csv(delta.getKey())).append(',')
                        .append(format(baseline.mean())).append(',')
                        .append(format(current.mean())).append(',')
                        .append(format(delta.getValue())).append(',')
                        .append(statusFor(comparison, delta.getKey())).append('\n');
            }
        }
        return sb.toString();
    }

    private static String statusFor(ScenarioComparison comparison, String metric) {
        if (!comparison.comparable()) {
            return "non-comparable";
        }
        Double delta = comparison.deltaPercentByMetric().get(metric);
        if ("qps".equals(metric) && delta != null && delta <= -10.0) {
            return "warning";
        }
        if (("p95_ms".equals(metric) || "p99_ms".equals(metric)) && delta != null && delta >= 15.0) {
            return "warning";
        }
        return "ok";
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String csv(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0 && value.indexOf('\r') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
```

Create `SuiteMarkdownWriter.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.util.Locale;
import java.util.Map;

public final class SuiteMarkdownWriter {
    private SuiteMarkdownWriter() {
    }

    public static String write(SuiteRunResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Yierdis Benchmark Suite Report\n\n");
        sb.append("- Run: `").append(result.runId()).append("`\n");
        sb.append("- Profile: `").append(result.profile().cliName()).append("`\n");
        sb.append("- Started: `").append(result.startedAt()).append("`\n");
        sb.append("- Finished: `").append(result.finishedAt()).append("`\n\n");

        sb.append("## Findings\n\n");
        if (result.findings().isEmpty()) {
            sb.append("No soft-threshold findings.\n\n");
        } else {
            for (ThresholdFinding finding : result.findings()) {
                sb.append("- **").append(finding.level()).append("** `")
                        .append(finding.scenarioId()).append("` `")
                        .append(finding.metric()).append("`: ")
                        .append(finding.message()).append('\n');
            }
            sb.append('\n');
        }

        sb.append("## Scenario Metrics\n\n");
        sb.append("| artifact | scenario | metric | mean | min | median | max | samples |\n");
        sb.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
        for (ScenarioPassResult pass : result.passes()) {
            for (Map.Entry<String, MetricSummary> entry : pass.summaries().entrySet()) {
                MetricSummary summary = entry.getValue();
                sb.append("| ").append(pass.artifactLabel())
                        .append(" | ").append(pass.scenario().id())
                        .append(" | ").append(entry.getKey())
                        .append(" | ").append(format(summary.mean()))
                        .append(" | ").append(format(summary.min()))
                        .append(" | ").append(format(summary.median()))
                        .append(" | ").append(format(summary.max()))
                        .append(" | ").append(summary.sampleCount())
                        .append(" |\n");
            }
        }
        sb.append('\n');

        if (!result.comparisons().isEmpty()) {
            sb.append("## Comparisons\n\n");
            sb.append("| scenario | metric | delta % | status |\n");
            sb.append("| --- | --- | ---: | --- |\n");
            for (ScenarioComparison comparison : result.comparisons()) {
                if (!comparison.comparable()) {
                    sb.append("| ").append(comparison.scenario().id())
                            .append(" | comparability |  | non-comparable: ")
                            .append(comparison.nonComparableReason()).append(" |\n");
                    continue;
                }
                for (Map.Entry<String, Double> delta : comparison.deltaPercentByMetric().entrySet()) {
                    sb.append("| ").append(comparison.scenario().id())
                            .append(" | ").append(delta.getKey())
                            .append(" | ").append(format(delta.getValue()))
                            .append(" | ").append(statusFor(delta.getKey(), delta.getValue()))
                            .append(" |\n");
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String statusFor(String metric, double delta) {
        if ("qps".equals(metric) && delta <= -10.0) {
            return "WARNING";
        }
        if (("p95_ms".equals(metric) || "p99_ms".equals(metric)) && delta >= 15.0) {
            return "WARNING";
        }
        return "OK";
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }
}
```

Create `SuiteReportWriter.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class SuiteReportWriter {
    private SuiteReportWriter() {
    }

    public static void writeAll(SuiteRunResult result, Path reportDir) throws IOException {
        Files.createDirectories(reportDir);
        Files.writeString(reportDir.resolve("suite-result.json"), SuiteJsonWriter.write(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("metrics.csv"), SuiteCsvWriter.metricsCsv(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("comparisons.csv"), SuiteCsvWriter.comparisonsCsv(result), StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("report.md"), SuiteMarkdownWriter.write(result), StandardCharsets.UTF_8);
    }
}
```

Ensure `SuiteCsvWriter.comparisonsCsv(...)` formats the example qps row as:

```text
release-ping-latency,qps,1000.000,900.000,-10.000,warning
```

- [ ] **Step 6: Run report writer test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteReportWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 7: Commit report writers**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunResult.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteJsonWriter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteReportWriter.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteReportWriterTest.java
git commit -m "feat: write benchmark suite reports"
```

## Task 5: Observation Snapshot Client

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationSnapshot.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`

- [ ] **Step 1: Write failing observation client test**

Create `ObservationClientTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ObservationClientTest {
    @Test
    public void formatsRespRepliesForSnapshotStorage() throws Exception {
        RespClientCodec.RespReply stats = RespClientCodec.readReply(
                new ByteArrayInputStream("*4\r\n$12\r\nqueued_tasks\r\n:3\r\n$13\r\nqueued_bytes\r\n:9\r\n".getBytes(StandardCharsets.US_ASCII)),
                1024
        );
        RespClientCodec.RespReply info = RespClientCodec.readReply(
                new ByteArrayInputStream("$20\r\n# Server\r\nversion:1\r\n".getBytes(StandardCharsets.US_ASCII)),
                1024
        );

        Assert.assertEquals("queued_tasks=3; queued_bytes=9", ObservationClient.formatReply(stats));
        Assert.assertEquals("# Server\nversion:1\n", ObservationClient.formatReply(info));
    }

    @Test
    public void snapshotStoresNamedObservationValues() {
        ObservationSnapshot snapshot = new ObservationSnapshot(Map.of(
                "STATS", "queued_tasks=0",
                "MEMORY STATS", "used_bytes=10",
                "INFO", "# Server"
        ));

        Assert.assertEquals("queued_tasks=0", snapshot.values().get("STATS"));
        Assert.assertEquals("used_bytes=10", snapshot.values().get("MEMORY STATS"));
        Assert.assertEquals("# Server", snapshot.values().get("INFO"));
    }
}
```

- [ ] **Step 2: Run failing observation test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `ObservationClient` does not exist.

- [ ] **Step 3: Implement observation client**

Create `ObservationClient.java`:

```java
package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ObservationClient {
    private static final int CONNECT_TIMEOUT_MILLIS = 1000;

    public ObservationSnapshot capture(String host, int port) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("STATS", execute(host, port, List.of("STATS")));
        values.put("MEMORY STATS", execute(host, port, List.of("MEMORY", "STATS")));
        values.put("INFO", execute(host, port, List.of("INFO")));
        return new ObservationSnapshot(values);
    }

    static String formatReply(RespClientCodec.RespReply reply) {
        if (reply == null) {
            return "";
        }
        return switch (reply.kind()) {
            case SIMPLE_STRING, ERROR -> reply.text();
            case INTEGER -> Long.toString(reply.integer());
            case BULK_STRING -> new String(reply.bytes(), StandardCharsets.UTF_8);
            case NULL -> "null";
            case ARRAY -> formatArray(reply.values());
        };
    }

    private static String formatArray(List<RespClientCodec.RespReply> values) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < values.size(); i += 2) {
            RespClientCodec.RespReply key = values.get(i);
            RespClientCodec.RespReply value = i + 1 < values.size() ? values.get(i + 1) : null;
            parts.add(formatReply(key) + "=" + formatReply(value));
        }
        return String.join("; ", parts);
    }

    private static String execute(String host, int port, List<String> args) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            try (BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
                 BufferedInputStream in = new BufferedInputStream(socket.getInputStream())) {
                List<byte[]> encoded = new ArrayList<>(args.size());
                for (String arg : args) {
                    encoded.add(arg.getBytes(StandardCharsets.UTF_8));
                }
                RespClientCodec.writeCommand(out, encoded);
                out.flush();
                return formatReply(RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES));
            }
        } catch (IOException e) {
            return "ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }
}
```

- [ ] **Step 4: Run observation tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit observation client**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationSnapshot.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java
git commit -m "feat: collect benchmark suite observations"
```

## Task 6: Suite Runner With Fake Harness

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteHarness.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`

- [ ] **Step 1: Write failing runner orchestration test**

Create `SuiteRunnerOrchestrationTest.java`:

```java
package yier.bubu.redis.app.bench.suite;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SuiteRunnerOrchestrationTest {
    @Test
    public void runnerStartsFreshServerForEachScenarioAndArtifact() throws Exception {
        Path current = regularTempJar("current");
        Path baseline = regularTempJar("baseline");
        SuiteConfig config = TestSuiteConfigs.comparison(current, baseline);
        FakeHarness harness = new FakeHarness();
        SuiteRunner runner = new SuiteRunner(config, harness, List.of(
                scenario("release-ping-latency", BenchWorkloadKind.PING),
                scenario("release-set-get-256b-c64-p8", BenchWorkloadKind.SET_GET)
        ));

        SuiteRunResult result = runner.run();

        Assert.assertEquals(List.of(
                "start:baseline:release-ping-latency",
                "stop:baseline:release-ping-latency",
                "start:current:release-ping-latency",
                "stop:current:release-ping-latency",
                "start:baseline:release-set-get-256b-c64-p8",
                "stop:baseline:release-set-get-256b-c64-p8",
                "start:current:release-set-get-256b-c64-p8",
                "stop:current:release-set-get-256b-c64-p8"
        ), harness.lifecycle);
        Assert.assertEquals(4, result.passes().size());
        Assert.assertEquals(2, result.comparisons().size());
        Assert.assertTrue(result.findings().isEmpty());
        for (ScenarioPassResult pass : result.passes()) {
            Assert.assertEquals(2, pass.iterations().size());
            Assert.assertEquals(1, pass.iterations().stream().filter(i -> i.kind() == IterationResult.Kind.WARMUP).count());
            Assert.assertEquals(1, pass.iterations().stream().filter(i -> i.kind() == IterationResult.Kind.REPEAT).count());
            Assert.assertTrue(pass.before().values().containsKey("STATS"));
            Assert.assertTrue(pass.after().values().containsKey("INFO"));
        }
    }

    @Test
    public void runnerPreservesFailedScenarioAndStillWritesResultModel() throws Exception {
        Path current = regularTempJar("current");
        SuiteConfig config = TestSuiteConfigs.currentOnly(current);
        FakeHarness harness = new FakeHarness();
        harness.failScenarioId = "release-ping-latency";
        SuiteRunner runner = new SuiteRunner(config, harness, List.of(scenario("release-ping-latency", BenchWorkloadKind.PING)));

        SuiteRunResult result = runner.run();

        Assert.assertEquals(1, result.passes().size());
        Assert.assertTrue(result.passes().get(0).failed());
        Assert.assertTrue(result.passes().get(0).failureMessage().contains("forced failure"));
        Assert.assertEquals(1, result.findings().size());
        Assert.assertEquals(ThresholdFinding.Level.CRITICAL, result.findings().get(0).level());
    }

    private static ScenarioDefinition scenario(String id, BenchWorkloadKind kind) {
        return new ScenarioDefinition(id, id, kind, 10, 16, 100, 2, 1, 1, 1, true);
    }

    private static Path regularTempJar(String prefix) throws Exception {
        Path jar = Files.createTempFile(prefix, ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    private static final class FakeHarness implements SuiteHarness {
        final List<String> lifecycle = new ArrayList<>();
        String failScenarioId = "";

        @Override
        public SuiteHarness.RunningServer startServer(SuiteArtifact artifact, ScenarioDefinition scenario, SuiteConfig config, int port, Path logFile) {
            lifecycle.add("start:" + artifact.label() + ":" + scenario.id());
            return new SuiteHarness.RunningServer(artifact.label(), scenario.id(), port, logFile);
        }

        @Override
        public ObservationSnapshot captureObservation(String host, int port) {
            return new ObservationSnapshot(java.util.Map.of("STATS", "queued_tasks=0", "MEMORY STATS", "used=1", "INFO", "ok"));
        }

        @Override
        public IterationResult runIteration(SuiteHarness.RunningServer server, ScenarioDefinition scenario, int index, IterationResult.Kind kind, SuiteConfig config) {
            if (scenario.id().equals(failScenarioId)) {
                throw new IllegalStateException("forced failure for " + scenario.id());
            }
            return new IterationResult(kind, index, List.of(
                    new SuiteMetric("qps", 1000.0),
                    new SuiteMetric("p95_ms", 1.0),
                    new SuiteMetric("p99_ms", 2.0),
                    new SuiteMetric("errors", 0.0)
            ));
        }

        @Override
        public void stopServer(SuiteHarness.RunningServer server) {
            lifecycle.add("stop:" + server.artifactLabel() + ":" + server.scenarioId());
        }
    }
}
```

Also add this package-private `TestSuiteConfigs` helper at the bottom of `SuiteRunnerOrchestrationTest.java` after the test class:

```java
final class TestSuiteConfigs {
    private TestSuiteConfigs() {
    }

    static SuiteConfig currentOnly(Path current) {
        return new SuiteConfig(SuiteProfileName.RELEASE, new SuiteArtifact("current", current, ""),
                java.util.Optional.empty(), Path.of("target/benchmark-reports/test"), "127.0.0.1", 16378,
                "java", "128m", "128m", "256m", new yier.bubu.redis.app.bench.YierdisBenchServerArgs(), true);
    }

    static SuiteConfig comparison(Path current, Path baseline) {
        return new SuiteConfig(SuiteProfileName.RELEASE, new SuiteArtifact("current", current, ""),
                java.util.Optional.of(new SuiteArtifact("baseline", baseline, "")), Path.of("target/benchmark-reports/test"),
                "127.0.0.1", 16378, "java", "128m", "128m", "256m",
                new yier.bubu.redis.app.bench.YierdisBenchServerArgs(), true);
    }
}
```

- [ ] **Step 2: Run failing runner test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `SuiteHarness` and `SuiteRunner` do not exist.

- [ ] **Step 3: Add suite harness interface**

Create `SuiteHarness.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;

public interface SuiteHarness {
    RunningServer startServer(SuiteArtifact artifact, ScenarioDefinition scenario, SuiteConfig config, int port, Path logFile) throws Exception;

    ObservationSnapshot captureObservation(String host, int port);

    IterationResult runIteration(RunningServer server, ScenarioDefinition scenario, int index, IterationResult.Kind kind, SuiteConfig config) throws Exception;

    void stopServer(RunningServer server);

    record RunningServer(String artifactLabel, String scenarioId, int port, Path logFile, Object handle) {
        public RunningServer(String artifactLabel, String scenarioId, int port, Path logFile) {
            this(artifactLabel, scenarioId, port, logFile, null);
        }
    }
}
```

- [ ] **Step 4: Add suite runner**

Create `SuiteRunner.java`:

```java
package yier.bubu.redis.app.bench.suite;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class SuiteRunner {
    private final SuiteConfig config;
    private final SuiteHarness harness;
    private final List<ScenarioDefinition> scenarios;

    public SuiteRunner(SuiteConfig config, SuiteHarness harness, List<ScenarioDefinition> scenarios) {
        this.config = config;
        this.harness = harness;
        this.scenarios = List.copyOf(scenarios);
    }

    public SuiteRunResult run() {
        Instant started = Instant.now();
        List<SuiteArtifact> artifacts = artifacts();
        List<ScenarioPassResult> passes = new ArrayList<>();
        List<ScenarioComparison> comparisons = new ArrayList<>();
        List<ThresholdFinding> findings = new ArrayList<>();

        for (ScenarioDefinition scenario : scenarios) {
            List<ScenarioPassResult> scenarioPasses = new ArrayList<>();
            for (int i = 0; i < artifacts.size(); i++) {
                SuiteArtifact artifact = artifacts.get(i);
                ScenarioPassResult pass = runPass(artifact, scenario, config.portBase() + scenarioPortOffset(scenario, i));
                passes.add(pass);
                scenarioPasses.add(pass);
                if (!pass.clean()) {
                    findings.add(new ThresholdFinding(ThresholdFinding.Level.CRITICAL, scenario.id(), "comparability",
                            artifact.label() + " is not clean"));
                }
            }
            if (scenarioPasses.size() == 2) {
                ScenarioComparison comparison = ScenarioComparison.compare(scenario, scenarioPasses.get(0), scenarioPasses.get(1));
                comparisons.add(comparison);
                findings.addAll(ThresholdEvaluator.evaluate(comparison, ThresholdPolicy.defaults()));
            }
        }

        return new SuiteRunResult(runId(started), config.profile(), started, Instant.now(), SuiteEnvironment.capture(),
                artifacts, scenarios, passes, comparisons, findings);
    }

    private ScenarioPassResult runPass(SuiteArtifact artifact, ScenarioDefinition scenario, int port) {
        Path logFile = config.reportDir().resolve("server-" + artifact.label() + "-" + scenario.id() + ".log");
        SuiteHarness.RunningServer server = null;
        try {
            server = harness.startServer(artifact, scenario, config, port, logFile);
            ObservationSnapshot before = harness.captureObservation(config.host(), port);
            List<IterationResult> iterations = new ArrayList<>();
            for (int i = 0; i < scenario.warmupIterations(); i++) {
                iterations.add(harness.runIteration(server, scenario, i, IterationResult.Kind.WARMUP, config));
            }
            for (int i = 0; i < scenario.repeatIterations(); i++) {
                iterations.add(harness.runIteration(server, scenario, i, IterationResult.Kind.REPEAT, config));
            }
            ObservationSnapshot after = harness.captureObservation(config.host(), port);
            return ScenarioPassResult.completed(artifact.label(), scenario, iterations, before, after);
        } catch (Exception e) {
            return ScenarioPassResult.failed(artifact.label(), scenario, failureSummary(e));
        } finally {
            if (server != null) {
                harness.stopServer(server);
            }
        }
    }

    private List<SuiteArtifact> artifacts() {
        List<SuiteArtifact> out = new ArrayList<>();
        config.baseline().ifPresent(out::add);
        out.add(config.current());
        return out;
    }

    private int scenarioPortOffset(ScenarioDefinition scenario, int artifactIndex) {
        return Math.abs(scenario.id().hashCode() % 10_000) * 2 + artifactIndex;
    }

    private static String runId(Instant started) {
        return started.toString().replace(":", "").replace(".", "-");
    }

    private static String failureSummary(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null || message.isBlank() ? root.getClass().getSimpleName() : message;
    }
}
```

- [ ] **Step 5: Run runner orchestration test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit fake-runner orchestration**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteHarness.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java
git commit -m "feat: orchestrate benchmark suite runs"
```

## Task 7: Real Benchmark Harness Adapter And Entrypoint

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadRequest.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadResult.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`

- [ ] **Step 1: Write failing entrypoint config test**

Create `SuiteEntrypointConfigTest.java`:

```java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.suite.SuiteConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class SuiteEntrypointConfigTest {
    @Test
    public void suiteArgsParseThroughBenchmarkEntrypointArgs() throws Exception {
        Path current = Files.createTempFile("current", ".jar");
        Files.writeString(current, "stub", StandardCharsets.US_ASCII);
        Path reportDir = Files.createTempDirectory("suite-report-");

        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(
                "--suite",
                "--suiteProfile", "release",
                "--currentServerJar", current.toString(),
                "--reportDir", reportDir.toString(),
                "--xms", "256m",
                "--xmx", "256m",
                "--maxDirectMemory", "512m",
                "--strictReplies"
        );
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        SuiteConfig config = SuiteConfig.from(args, serverArgs);

        Assert.assertEquals(current, config.current().jarPath());
        Assert.assertEquals(reportDir.toAbsolutePath().normalize(), config.reportDir());
        Assert.assertEquals("256m", config.xms());
        Assert.assertEquals("512m", config.maxDirectMemory());
        Assert.assertTrue(config.strictReplies());
    }
}
```

- [ ] **Step 2: Run entrypoint config test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteEntrypointConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS if previous config task is complete. If it fails, fix `SuiteConfig.from(...)` before adding the real harness.

- [ ] **Step 3: Add adapter request and result records**

Create `BenchWorkloadRequest.java`:

```java
package yier.bubu.redis.app.bench;

public record BenchWorkloadRequest(
        BenchWorkloadKind workload,
        String host,
        int port,
        int requests,
        int clients,
        int pipeline,
        int keyspace,
        int dataSize,
        boolean latency,
        boolean strictReplies
) {
}
```

Create `BenchWorkloadResult.java`:

```java
package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.suite.SuiteMetric;

import java.util.ArrayList;
import java.util.List;

public record BenchWorkloadResult(long ops, long errors, double seconds, double qps, double p50Millis, double p95Millis, double p99Millis) {
    public List<SuiteMetric> toMetrics() {
        List<SuiteMetric> metrics = new ArrayList<>();
        metrics.add(new SuiteMetric("ops", ops));
        metrics.add(new SuiteMetric("errors", errors));
        metrics.add(new SuiteMetric("seconds", seconds));
        metrics.add(new SuiteMetric("qps", qps));
        if (!Double.isNaN(p50Millis)) {
            metrics.add(new SuiteMetric("p50_ms", p50Millis));
            metrics.add(new SuiteMetric("p95_ms", p95Millis));
            metrics.add(new SuiteMetric("p99_ms", p99Millis));
        }
        return List.copyOf(metrics);
    }
}
```

- [ ] **Step 4: Add real harness adapter**

Create `BenchHarness.java` in the root `bench` package. This class can use package-private nested types in `YierdisBench`:

```java
package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.suite.IterationResult;
import yier.bubu.redis.app.bench.suite.ObservationClient;
import yier.bubu.redis.app.bench.suite.ObservationSnapshot;
import yier.bubu.redis.app.bench.suite.ScenarioDefinition;
import yier.bubu.redis.app.bench.suite.SuiteArtifact;
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteHarness;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class BenchHarness implements SuiteHarness {
    private final ObservationClient observationClient = new ObservationClient();

    @Override
    public SuiteHarness.RunningServer startServer(SuiteArtifact artifact, ScenarioDefinition scenario, SuiteConfig config, int port, Path logFile) throws Exception {
        YierdisBenchServerArgs serverArgs = config.baseServerArgs().copy();
        serverArgs.port = port;
        serverArgs.normalizeAndValidate();
        YierdisBench.ServerProcess server = new YierdisBench.ServerProcess(
                config.javaCmd(),
                artifact.jarPath(),
                config.xms(),
                config.xmx(),
                config.maxDirectMemory(),
                serverArgs,
                logFile
        );
        server.start();
        if (!waitReady(config.host(), port)) {
            throw new IllegalStateException("server did not become ready: " + logFile);
        }
        return new SuiteHarness.RunningServer(artifact.label(), scenario.id(), port, logFile, server);
    }

    @Override
    public ObservationSnapshot captureObservation(String host, int port) {
        return observationClient.capture(host, port);
    }

    @Override
    public IterationResult runIteration(SuiteHarness.RunningServer server, ScenarioDefinition scenario, int index, IterationResult.Kind kind, SuiteConfig config) throws Exception {
        BenchWorkloadResult result = runWorkload(new BenchWorkloadRequest(
                scenario.workload(),
                config.host(),
                server.port(),
                scenario.requests(),
                scenario.clients(),
                scenario.pipeline(),
                scenario.keyspace(),
                scenario.dataSize(),
                scenario.latency(),
                config.strictReplies()
        ));
        return new IterationResult(kind, index, result.toMetrics());
    }

    @Override
    public void stopServer(SuiteHarness.RunningServer server) {
        Object handle = server.handle();
        if (handle instanceof YierdisBench.ServerProcess process) {
            process.stop();
        }
    }

    private static boolean waitReady(String host, int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            try {
                BenchWorkloadResult ping = runWorkload(new BenchWorkloadRequest(
                        BenchWorkloadKind.PING, host, port, 1, 1, 1, 1, 0, true, true));
                return ping.errors() == 0;
            } catch (Exception ignored) {
                Thread.sleep(100);
            }
        }
        return false;
    }

    private static BenchWorkloadResult runWorkload(BenchWorkloadRequest request) throws Exception {
        Objects.requireNonNull(request, "request");
        YierdisBench.Workload workload = mapWorkload(request.workload());
        if (request.latency()) {
            return runLatency(request, workload);
        }
        return runThroughput(request, workload);
    }

    private static BenchWorkloadResult runThroughput(BenchWorkloadRequest request, YierdisBench.Workload workload) throws Exception {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');
        ExecutorService executor = Executors.newFixedThreadPool(request.clients());
        List<Future<YierdisBench.WorkerCounter>> futures = new ArrayList<>();
        int perClient = request.requests() / request.clients();
        int remainder = request.requests() % request.clients();
        Instant startedAt = Instant.now();
        long start = System.nanoTime();
        for (int i = 0; i < request.clients(); i++) {
            int n = perClient + (i < remainder ? 1 : 0);
            futures.add(executor.submit(new YierdisBench.ThroughputWorker(
                    request.host(), request.port(), workload, n, request.pipeline(), request.keyspace(), value, 0, request.strictReplies())));
        }
        long ops = 0;
        long errors = 0;
        for (Future<YierdisBench.WorkerCounter> future : futures) {
            YierdisBench.WorkerCounter counter = future.get();
            ops += counter.ops;
            errors += counter.errors;
        }
        executor.shutdownNow();
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        double qps = seconds <= 0.0 ? 0.0 : ops / seconds;
        return new BenchWorkloadResult(ops, errors, seconds, qps, Double.NaN, Double.NaN, Double.NaN);
    }

    private static BenchWorkloadResult runLatency(BenchWorkloadRequest request, YierdisBench.Workload workload) throws Exception {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');
        ExecutorService executor = Executors.newFixedThreadPool(request.clients());
        List<Future<YierdisBench.LatencySamples>> futures = new ArrayList<>();
        int perClient = request.requests() / request.clients();
        int remainder = request.requests() % request.clients();
        long start = System.nanoTime();
        for (int i = 0; i < request.clients(); i++) {
            int n = perClient + (i < remainder ? 1 : 0);
            futures.add(executor.submit(new YierdisBench.LatencyWorker(
                    request.host(), request.port(), workload, n, request.keyspace(), value, request.strictReplies())));
        }
        List<Long> samples = new ArrayList<>();
        long errors = 0;
        for (Future<YierdisBench.LatencySamples> future : futures) {
            YierdisBench.LatencySamples result = future.get();
            errors += result.errors;
            for (long sample : result.samples) {
                samples.add(sample);
            }
        }
        executor.shutdownNow();
        long[] sorted = new long[samples.size()];
        for (int i = 0; i < samples.size(); i++) {
            sorted[i] = samples.get(i);
        }
        Arrays.sort(sorted);
        YierdisBench.LatencyStats stats = YierdisBench.LatencyStats.ofSortedNanos(sorted);
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        double qps = seconds <= 0.0 ? 0.0 : sorted.length / seconds;
        return new BenchWorkloadResult(sorted.length, errors, seconds, qps,
                stats.p50Millis(), stats.p95Millis(), stats.p99Millis());
    }

    private static YierdisBench.Workload mapWorkload(BenchWorkloadKind workload) {
        return switch (workload) {
            case PING -> YierdisBench.Workload.PING;
            case SET_GET -> YierdisBench.Workload.SET_RANDOM;
            case APPEND, NATIVE_DEFRAG_APPEND -> YierdisBench.Workload.APPEND;
            case HLL_SPARSE -> YierdisBench.Workload.PFADD_SPARSE;
            case HLL_DENSE -> YierdisBench.Workload.PFADD_DENSE;
            case HLL_PFCOUNT -> YierdisBench.Workload.PFCOUNT;
            case MAXMEMORY_EVICTION, TTL_EXPIRATION, LIST_LPUSH, HASH_HSET, SET_SADD, ZSET_ZADD, SCAN, MIXED_READ_WRITE ->
                    throw new IllegalArgumentException("unsupported extended suite workload: " + workload);
        };
    }
}
```

After adding this class, compilation may fail because `LatencyStats.p50Millis()`, `p95Millis()`, and `p99Millis()` are package-private instance methods inside `YierdisBench`. Since `BenchHarness` is in the same package, that access is valid.

This task deliberately rejects extended suite workloads instead of silently mapping them to `SET_RANDOM`. Task 8 wires those workloads before the full profile is considered complete.

- [ ] **Step 5: Wire suite mode into the entrypoint**

Modify `YierdisBench.java` imports:

```java
import yier.bubu.redis.app.bench.suite.SuiteConfig;
import yier.bubu.redis.app.bench.suite.SuiteProfileFactory;
import yier.bubu.redis.app.bench.suite.SuiteReportWriter;
import yier.bubu.redis.app.bench.suite.SuiteRunResult;
import yier.bubu.redis.app.bench.suite.SuiteRunner;
```

After `baseServerArgs.normalizeAndValidate();`, before `BenchConfig config;`, add:

```java
        if (benchArgs.suite) {
            SuiteConfig suiteConfig;
            try {
                suiteConfig = SuiteConfig.from(benchArgs, baseServerArgs);
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
                cmd.usage(System.err);
                return;
            }
            println("YierdisBench（release suite）");
            println("profile: " + suiteConfig.profile().cliName());
            println("reportDir: " + suiteConfig.reportDir());
            println("");
            SuiteRunner runner = new SuiteRunner(
                    suiteConfig,
                    new BenchHarness(),
                    SuiteProfileFactory.expand(suiteConfig.profile())
            );
            SuiteRunResult result = runner.run();
            SuiteReportWriter.writeAll(result, suiteConfig.reportDir());
            println("suite-result.json: " + suiteConfig.reportDir().resolve("suite-result.json"));
            println("metrics.csv       : " + suiteConfig.reportDir().resolve("metrics.csv"));
            println("comparisons.csv   : " + suiteConfig.reportDir().resolve("comparisons.csv"));
            println("report.md         : " + suiteConfig.reportDir().resolve("report.md"));
            println("");
            println("完成。");
            return;
        }
```

- [ ] **Step 6: Run focused suite compilation tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteEntrypointConfigTest,SuiteRunnerOrchestrationTest,SuiteConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 7: Commit real harness and entrypoint**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadRequest.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadResult.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java
git commit -m "feat: run benchmark suite mode"
```

## Task 8: Extended Suite Workloads

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`

- [ ] **Step 1: Write failing extended workload command test**

Create `BenchHarnessExtendedWorkloadTest.java`:

```java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class BenchHarnessExtendedWorkloadTest {
    @Test
    public void commandForEachExtendedWorkloadUsesTheExpectedRedisCommand() {
        assertCommand(BenchWorkloadKind.MAXMEMORY_EVICTION, "SET");
        assertCommand(BenchWorkloadKind.TTL_EXPIRATION, "EXPIRE");
        assertCommand(BenchWorkloadKind.LIST_LPUSH, "LPUSH");
        assertCommand(BenchWorkloadKind.HASH_HSET, "HSET");
        assertCommand(BenchWorkloadKind.SET_SADD, "SADD");
        assertCommand(BenchWorkloadKind.ZSET_ZADD, "ZADD");
        assertCommand(BenchWorkloadKind.SCAN, "SCAN");
        assertCommand(BenchWorkloadKind.MIXED_READ_WRITE, "GET");
    }

    @Test
    public void extendedWorkloadsAreRecognizedByHarness() {
        for (BenchWorkloadKind workload : List.of(
                BenchWorkloadKind.MAXMEMORY_EVICTION,
                BenchWorkloadKind.TTL_EXPIRATION,
                BenchWorkloadKind.LIST_LPUSH,
                BenchWorkloadKind.HASH_HSET,
                BenchWorkloadKind.SET_SADD,
                BenchWorkloadKind.ZSET_ZADD,
                BenchWorkloadKind.SCAN,
                BenchWorkloadKind.MIXED_READ_WRITE
        )) {
            Assert.assertTrue("expected extended workload " + workload, BenchHarness.isExtendedWorkload(workload));
        }
    }

    private static void assertCommand(BenchWorkloadKind workload, String expectedCommand) {
        byte[] frame = BenchHarness.encodeExtendedCommandForTest(workload, 7, 11, new byte[]{'x', 'x'});
        String rendered = new String(frame, StandardCharsets.US_ASCII);
        Assert.assertTrue(rendered, rendered.contains("\r\n" + expectedCommand + "\r\n"));
    }
}
```

- [ ] **Step 2: Run failing extended workload test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `BenchHarness.isExtendedWorkload(...)` and `encodeExtendedCommandForTest(...)` do not exist, and extended workloads are still rejected.

- [ ] **Step 3: Add extended workload command encoding helpers**

Modify `BenchHarness.java` to import `RespClientCodec`, `OutputStream`, and `StandardCharsets`, then add these methods:

```java
    static boolean isExtendedWorkload(BenchWorkloadKind workload) {
        return switch (workload) {
            case MAXMEMORY_EVICTION, TTL_EXPIRATION, LIST_LPUSH, HASH_HSET, SET_SADD, ZSET_ZADD, SCAN, MIXED_READ_WRITE -> true;
            default -> false;
        };
    }

    static byte[] encodeExtendedCommandForTest(BenchWorkloadKind workload, int keyIndex, int opIndex, byte[] value) {
        return RespClientCodec.encodeCommand(extendedCommand(workload, keyIndex, opIndex, value));
    }

    private static List<byte[]> extendedCommand(BenchWorkloadKind workload, int keyIndex, int opIndex, byte[] value) {
        byte[] key = ascii("suite:" + workload.name().toLowerCase(java.util.Locale.ROOT) + ":" + keyIndex);
        byte[] field = ascii("f" + opIndex);
        byte[] member = ascii("m" + opIndex);
        return switch (workload) {
            case MAXMEMORY_EVICTION -> List.of(ascii("SET"), key, value);
            case TTL_EXPIRATION -> List.of(ascii("EXPIRE"), key, ascii("60"));
            case LIST_LPUSH -> List.of(ascii("LPUSH"), key, value);
            case HASH_HSET -> List.of(ascii("HSET"), key, field, value);
            case SET_SADD -> List.of(ascii("SADD"), key, member);
            case ZSET_ZADD -> List.of(ascii("ZADD"), key, ascii(Integer.toString(opIndex)), member);
            case SCAN -> List.of(ascii("SCAN"), ascii("0"), ascii("COUNT"), ascii("100"));
            case MIXED_READ_WRITE -> (opIndex % 5 == 0)
                    ? List.of(ascii("SET"), key, value)
                    : List.of(ascii("GET"), key);
            default -> throw new IllegalArgumentException("not an extended workload: " + workload);
        };
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
```

- [ ] **Step 4: Implement extended workload execution path**

Modify `runWorkload(...)` in `BenchHarness.java` so extended workloads run through a new command-based path before `mapWorkload(...)`:

```java
    private static BenchWorkloadResult runWorkload(BenchWorkloadRequest request) throws Exception {
        Objects.requireNonNull(request, "request");
        if (isExtendedWorkload(request.workload())) {
            return runExtended(request);
        }
        YierdisBench.Workload workload = mapWorkload(request.workload());
        if (request.latency()) {
            return runLatency(request, workload);
        }
        return runThroughput(request, workload);
    }
```

Add `runExtended(...)` as a simple synchronous RESP loop. It is intentionally less optimized than the core worker path because these workloads are for profile coverage, not the hottest SET/GET/HLL micro path:

```java
    private static BenchWorkloadResult runExtended(BenchWorkloadRequest request) throws Exception {
        byte[] value = new byte[request.dataSize()];
        Arrays.fill(value, (byte) 'x');
        long errors = 0;
        long start = System.nanoTime();
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new java.net.InetSocketAddress(request.host(), request.port()), 1000);
            try (java.io.BufferedOutputStream out = new java.io.BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                 java.io.BufferedInputStream in = new java.io.BufferedInputStream(socket.getInputStream(), 64 * 1024)) {
                int remaining = request.requests();
                int opIndex = 0;
                while (remaining > 0) {
                    int batch = Math.min(request.pipeline(), remaining);
                    for (int i = 0; i < batch; i++) {
                        int keyIndex = Math.floorMod(opIndex, request.keyspace());
                        RespClientCodec.writeCommand(out, extendedCommand(request.workload(), keyIndex, opIndex, value));
                        opIndex++;
                    }
                    out.flush();
                    for (int i = 0; i < batch; i++) {
                        if (YierdisBench.isErrorReply(RespClientCodec.readReply(in, yier.bubu.redis.protocol.resp.RespProtocolLimits.DEFAULT_MAX_BULK_BYTES))) {
                            errors++;
                        }
                    }
                    remaining -= batch;
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        double qps = seconds <= 0.0 ? 0.0 : request.requests() / seconds;
        return new BenchWorkloadResult(request.requests(), errors, seconds, qps, Double.NaN, Double.NaN, Double.NaN);
    }
```

If `YierdisBench.isErrorReply(...)` is private, change it to package-private static in `YierdisBench.java`:

```java
    static boolean isErrorReply(RespClientCodec.RespReply reply) {
```

- [ ] **Step 5: Run extended workload tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchHarnessExtendedWorkloadTest,SuiteProfileFactoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit extended workload coverage**

Run:

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java
git commit -m "feat: cover extended benchmark suite workloads"
```

## Task 9: Documentation And Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`
- Test: existing benchmark tests

- [ ] **Step 1: Update README benchmark section**

Modify `README.md` in the Benchmark 和 Smoke section to include:

```markdown
正式性能报告：

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/manual-release
```

baseline/current 对比报告：

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --baselineServerJar artifacts/baseline/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/release-comparison
```
```

Keep the existing `./scripts/bench.sh` examples unchanged.

- [ ] **Step 2: Update benchmark internals docs**

In `docs/project-docs/client-and-bench-internals.md`, add a section after `## yierdis-benchmark`:

```markdown
### Suite mode

`--suite` runs the release-grade benchmark suite. It is separate from the single-run benchmark path: single-run mode remains the fast local benchmark, while suite mode expands a stable profile into scenarios, starts a fresh server for each scenario and artifact, records warmup and repeat iterations, captures before/after `STATS`, `MEMORY STATS`, and `INFO`, and writes `suite-result.json`, `metrics.csv`, `comparisons.csv`, and `report.md`.

`--currentServerJar` is required. `--baselineServerJar` is optional. When both jars are present, suite mode compares only clean baseline/current scenario pairs. A scenario with startup failure, protocol/reply errors, benchmark errors, or missing measurements is marked `non-comparable`.

The first version uses soft thresholds: QPS drops, p95/p99 latency increases, errors, and non-comparable scenarios are reported as warnings or critical observations but do not fail the process by default.
```

- [ ] **Step 3: Run focused benchmark suite tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteProfileFactoryTest,SuiteMetricSummaryTest,SuiteThresholdEvaluatorTest,SuiteReportWriterTest,SuiteRunnerOrchestrationTest,ObservationClientTest,SuiteEntrypointConfigTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 4: Run the full benchmark module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

Expected: PASS.

- [ ] **Step 5: Package benchmark and server jars**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark,yierdis-server/yierdis-server-main -am -DskipTests package
```

Expected: PASS and both shaded jars exist:

```text
yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar
yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar
```

- [ ] **Step 6: Run a tiny suite smoke command**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/suite-smoke \
  --xms 256m --xmx 256m --maxDirectMemory 512m \
  --portBase 18378
```

Expected: command exits 0 and writes:

```text
target/benchmark-reports/suite-smoke/suite-result.json
target/benchmark-reports/suite-smoke/metrics.csv
target/benchmark-reports/suite-smoke/comparisons.csv
target/benchmark-reports/suite-smoke/report.md
```

If this smoke run is too long for the current environment, stop it and record the reason in the final handoff; do not change the release profile just to make local smoke faster unless the user approves a separate `smoke` profile.

- [ ] **Step 7: Check report artifacts contain expected markers**

Run:

```bash
rg -n "Yierdis Benchmark Suite Report|release-ping-latency|suite-result|qps|comparability" target/benchmark-reports/suite-smoke
```

Expected: matches in `report.md`, `suite-result.json`, `metrics.csv`, or `comparisons.csv`.

- [ ] **Step 8: Commit docs and verification updates**

Run:

```bash
git add README.md docs/project-docs/client-and-bench-internals.md
git commit -m "docs: document benchmark suite mode"
```

## Task 10: Final Review

**Files:**
- Review all changed files from Tasks 1-9.

- [ ] **Step 1: Inspect final diff**

Run:

```bash
git status --short
git log --oneline -8
```

Expected: working tree is clean after the task commits, and recent commits show the suite config, profiles, metrics, reports, runner, entrypoint, and docs.

- [ ] **Step 2: Run final verification command**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

Expected: PASS.

- [ ] **Step 3: Confirm no unresolved implementation markers in suite code or docs**

Run:

```bash
rg -n "TB[D]|TO[D]O|FIXM[E]|implement[ ]later" yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench README.md docs/project-docs/client-and-bench-internals.md
```

Expected: no matches from the new suite implementation. Existing unrelated matches, if any, must be listed in the final handoff.

- [ ] **Step 4: Prepare implementation summary**

Write the final handoff with:

```text
Implemented suite mode:
- CLI: --suite, --suiteProfile, --currentServerJar, --baselineServerJar, --reportDir
- Profiles: release and full
- Reports: suite-result.json, metrics.csv, comparisons.csv, report.md
- Soft thresholds and non-comparable handling
- Before/after STATS, MEMORY STATS, INFO snapshots

Verification:
- <commands run>
- <suite smoke result or reason it was not run>
```
