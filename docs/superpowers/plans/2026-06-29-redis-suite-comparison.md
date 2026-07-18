> **Superseded:** Replaced by `docs/superpowers/specs/2026-07-17-redis-benchmark-comparable-rewrite-design.md`. Retained only as historical context.

# Redis Suite Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Redis as a first-class comparison target to the existing release-grade benchmark suite so one suite run can generate Redis vs current-Yierdis comparison reports.

**Architecture:** Extend suite configuration and artifact modeling so the suite can run both jar-backed Yierdis artifacts and an external Redis endpoint through one harness. Keep the existing RESP/TCP workload engine, but make artifact lifecycle, observation capture, scenario comparability, and report rendering target-aware.

**Tech Stack:** Java 25, Maven, existing `yierdis-benchmark` suite/reporting classes, `RespClientCodec`, picocli, JUnit 4.

## Global Constraints

- Add Redis as an explicit suite comparison target.
- Keep the existing suite report artifacts: `suite-result.json`, `metrics.csv`, `comparisons.csv`, and `report.md`.
- Run identical scenario shapes for Redis and Yierdis: requests, clients, pipeline, keyspace, data size, warmup count, repeat count, and latency mode.
- Mark each Redis/Yierdis scenario pair as comparable only when both sides complete clean workload measurements with no reply/protocol errors.
- Record clear non-comparable reasons when Redis does not support a command, a setup step fails, a measurement is missing, or either side records errors.
- Treat Yierdis-specific observation commands as optional for Redis so observability differences do not invalidate otherwise clean workload results.
- Preserve the existing Yierdis baseline/current suite behavior.
- Document recommended Redis runtime settings so results are interpretable.
- Keep Redis as an externally managed endpoint in the first version.
- Do not make Redis the default suite target.
- Do not auto-start or configure Redis in the first version.
- Do not emulate Redis-only features in Yierdis or Yierdis-only observability in Redis.
- Do not replace the separate `redis-benchmark` compatibility design.
- Do not claim Redis drop-in compatibility from benchmark success.
- Do not add hidden warmup behavior outside the existing scenario warmup model.
- `--baselineServerJar` may not be combined with `--includeRedis` in this design. Three-way Redis/baseline/current comparison is out of scope for this spec.
- Scenarios classified as `EXTERNAL_CONFIG_REQUIRED` are non-comparable in this version unless a future spec adds an explicit operator acknowledgement option.
- The first version does not require `scripts/bench.sh` support.
- Use JDK 25 for every Maven, Java, and script command: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`.

---

## File Structure

- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
  Add Redis suite CLI options without changing non-suite behavior.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java`
  Replace the jar-only model with a target-aware artifact model.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`
  Parse Redis suite configuration, validate option combinations, and build artifact lists.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`
  Attach Redis comparability metadata to scenarios.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java`
  Add Redis compatibility classification to scenario metadata.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
  Add external Redis start/prepare/stop behavior while preserving jar-backed Yierdis runs.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`
  Add target-aware observation capture.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`
  Run Redis and current Yierdis in one suite pass order and capture environment metadata.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java`
  Compute Redis-specific non-comparable reasons from scenario metadata and dirty passes.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java`
  Capture Redis endpoint/runtime metadata, including host, port, db, and Redis INFO-derived fields, when Redis is part of the run.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java`
  Render Redis/current ratios, explicit artifact labels, and non-comparable reasons into `comparisons.csv`.
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java`
  Add Redis comparison summary sections, direction labels, and environment notes.
- Modify: `README.md`
  Document the Redis comparison suite command and Redis config guidance.
- Modify: `docs/project-docs/client-and-bench-internals.md`
  Document Redis suite mode boundaries and comparability rules.
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`

### Shared Test Helpers For Tasks 2-4

Tasks 2-4 rely on one shared test-support file so later tasks do not invent
private helpers ad hoc.

- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
  with these exact helpers:
  - `static ScenarioDefinition scenario(String id, BenchWorkloadKind workload, int warmups, int repeats, boolean latency)`
  - `static SuiteConfig redisCurrentOnlyConfig(Path reportDir, int portBase, int redisPort) throws Exception`
  - `static ScenarioPassResult cleanPass(String artifactLabel, ScenarioDefinition scenario)`
  - `static SuiteRunResult redisComparisonResult(boolean comparable, String reason)`
  - nested `static final class RedisLikeObservationServer implements AutoCloseable`

The support class copies the same scenario defaults already used in
`SuiteRunnerOrchestrationTest`: keyspace `100`, data size `256` except `PING`
uses `0`, requests `1000`, clients `8`, pipeline `4`.

### Task 1: Extend Suite Config And Artifact Modeling For Redis

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`

**Interfaces:**
- Consumes: `public static SuiteConfig SuiteConfig.from(YierdisBenchArgs args, YierdisBenchServerArgs serverArgs)`
- Produces: `public record SuiteArtifact(String label, Kind kind, Path jarPath, String host, int port, String commitLabel, String authUser, String authPassword, int db)`
- Produces: `public enum SuiteArtifact.Kind { YIERDIS_JAR, EXTERNAL_REDIS }`
- Produces: `public List<SuiteArtifact> SuiteConfig.artifactsInRunOrder()`

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void suiteIncludeRedisBuildsExternalRedisArtifact() throws Exception {
    Path current = regularTempJar("current");

    SuiteConfig config = config(
            "--suite",
            "--includeRedis",
            "--redisHost", "127.0.0.1",
            "--redisPort", "6379",
            "--currentServerJar", current.toString()
    );

    Assert.assertEquals(List.of("redis", "current"), config.artifactLabels());
    Assert.assertEquals(SuiteArtifact.Kind.EXTERNAL_REDIS, config.artifactsInRunOrder().get(0).kind());
    Assert.assertEquals("127.0.0.1", config.artifactsInRunOrder().get(0).host());
    Assert.assertEquals(6379, config.artifactsInRunOrder().get(0).port());
}

@Test
public void suiteIncludeRedisRejectsBaselineJarCombination() throws Exception {
    Path current = regularTempJar("current");
    Path baseline = regularTempJar("baseline");

    IllegalArgumentException rejected = assertRejects(
            "--suite",
            "--includeRedis",
            "--baselineServerJar", baseline.toString(),
            "--currentServerJar", current.toString()
    );

    Assert.assertTrue(rejected.getMessage().contains("baselineServerJar"));
    Assert.assertTrue(rejected.getMessage().contains("includeRedis"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because `YierdisBenchArgs` has no `includeRedis`/`redisHost`/`redisPort` options and `SuiteArtifact` is still jar-only.

- [ ] **Step 3: Write minimal implementation**

```java
// YierdisBenchArgs.java
@Option(names = "--includeRedis", description = "Include an external Redis target in suite mode.")
public boolean includeRedis;

@Option(names = "--redisHost", defaultValue = "127.0.0.1", description = "External Redis host for suite comparison.")
public String redisHost = "127.0.0.1";

@Option(names = "--redisPort", defaultValue = "6379", description = "External Redis port for suite comparison.")
public int redisPort = 6379;

@Option(names = "--redisLabel", defaultValue = "redis", description = "Artifact label used for external Redis in reports.")
public String redisLabel = "redis";

@Option(names = "--redisUser", description = "Optional ACL username for external Redis in suite mode.")
public String redisUser;

@Option(names = "--redisAuth", description = "Optional password for external Redis in suite mode.")
public String redisAuth;

@Option(names = "--redisDb", defaultValue = "0", description = "Logical DB selected on external Redis before each suite pass.")
public int redisDb = 0;
```

```java
// SuiteArtifact.java
public record SuiteArtifact(
        String label,
        Kind kind,
        Path jarPath,
        String host,
        int port,
        String commitLabel,
        String authUser,
        String authPassword,
        int db
) {
    public enum Kind {
        YIERDIS_JAR,
        EXTERNAL_REDIS
    }

    public SuiteArtifact {
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(kind, "kind");
        commitLabel = commitLabel == null ? "" : commitLabel;
        authUser = authUser == null ? "" : authUser;
        authPassword = authPassword == null ? "" : authPassword;
        host = host == null ? "" : host;
        if (kind == Kind.YIERDIS_JAR && jarPath == null) {
            throw new IllegalArgumentException("jarPath is required for YIERDIS_JAR artifacts");
        }
        if (kind == Kind.EXTERNAL_REDIS) {
            if (host.isBlank()) {
                throw new IllegalArgumentException("host is required for EXTERNAL_REDIS artifacts");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port must be in 1..65535 for EXTERNAL_REDIS artifacts");
            }
        }
    }

    public static SuiteArtifact yierdisJar(String label, Path jarPath, String commitLabel) {
        return new SuiteArtifact(label, Kind.YIERDIS_JAR, jarPath, "", 0, commitLabel, "", "", 0);
    }

    public static SuiteArtifact externalRedis(String label, String host, int port, String authUser, String authPassword, int db) {
        return new SuiteArtifact(label, Kind.EXTERNAL_REDIS, null, host, port, "", authUser, authPassword, db);
    }
}
```

```java
// SuiteConfig.java
if (args.includeRedis && args.baselineServerJar != null) {
    throw new IllegalArgumentException("suite does not support baselineServerJar with includeRedis");
}

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
}
artifacts.add(SuiteArtifact.yierdisJar("current", requireRegularFile(args.currentServerJar, "currentServerJar"), ""));
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS for Redis suite config parsing and no regressions in existing suite config tests.

- [ ] **Step 5: Commit**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java
git commit -m "feat: add redis suite artifact config"
```

### Task 2: Add Redis-Aware Harness Lifecycle And Observation Capture

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`

**Interfaces:**
- Consumes: `public List<SuiteArtifact> SuiteConfig.artifactsInRunOrder()`
- Produces: `public ObservationSnapshot ObservationClient.capture(SuiteArtifact artifact)`
- Produces: `public SuiteHarness.RunningServer BenchHarness.startServer(SuiteArtifact artifact, ScenarioDefinition scenario, SuiteConfig config, int port, Path logFile) throws Exception`
- Produces: `public void BenchHarness.stopServer(SuiteHarness.RunningServer server)`

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void captureRedisObservationDoesNotRequireStats() {
    try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
        SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);
        ObservationSnapshot snapshot = new ObservationClient().capture(artifact);

        Assert.assertTrue(snapshot.values().containsKey("INFO"));
        Assert.assertTrue(snapshot.values().containsKey("MEMORY STATS"));
        Assert.assertFalse(snapshot.values().containsKey("STATS"));
    }
}

@Test
public void redisArtifactStartServerFlushesDbAndDoesNotSpawnProcess() throws Exception {
    try (RedisSuiteTestSupport.RedisLikeObservationServer server = RedisSuiteTestSupport.RedisLikeObservationServer.start()) {
        SuiteArtifact artifact = SuiteArtifact.externalRedis("redis", "127.0.0.1", server.port(), "", "", 0);
        BenchHarness harness = new BenchHarness();
        ScenarioDefinition scenario = RedisSuiteTestSupport.scenario("release-set-get-128b-c32-p4", BenchWorkloadKind.SET_GET, 1, 1, true);
        SuiteConfig config = RedisSuiteTestSupport.redisCurrentOnlyConfig(Path.of("target/redis-suite-test"), 16378, server.port());
        SuiteHarness.RunningServer running = harness.startServer(artifact, scenario, config, artifact.port(), Path.of("target/redis.log"));

        Assert.assertNull(running.handle());
        Assert.assertTrue(server.commands().contains("FLUSHDB"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest,SuiteRunnerOrchestrationTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because `ObservationClient` only accepts host/port and always requests `STATS`, and `BenchHarness.startServer(...)` only knows how to launch jar-backed Yierdis processes.

- [ ] **Step 3: Write minimal implementation**

```java
// ObservationClient.java
public ObservationSnapshot capture(SuiteArtifact artifact) {
    Objects.requireNonNull(artifact, "artifact");
    return artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS
            ? captureRedis(artifact.host(), artifact.port())
            : captureYierdis(artifact.host(), artifact.port());
}

private ObservationSnapshot captureRedis(String host, int port) {
    Map<String, String> values = new LinkedHashMap<>();
    values.put("INFO", captureCommand(host, port, "INFO"));
    values.put("MEMORY STATS", captureCommand(host, port, "MEMORY", "STATS"));
    return new ObservationSnapshot(values);
}
```

```java
// BenchHarness.java
if (artifact.kind() == SuiteArtifact.Kind.EXTERNAL_REDIS) {
    waitExternalRedisReady(artifact);
    prepareExternalRedisPass(artifact);
    return new SuiteHarness.RunningServer(artifact.label(), scenario.id(), artifact.port(), logFile, null);
}

private void prepareExternalRedisPass(SuiteArtifact artifact) {
    runAdminCommand(artifact, List.of(bytes("FLUSHDB")));
}
```

```java
// SuiteRunner.java
ObservationSnapshot before = requireObservation(harness.captureObservation(artifact));
ObservationSnapshot after = requireObservation(harness.captureObservation(artifact));
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest,SuiteRunnerOrchestrationTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS for Redis observation and external lifecycle behavior, with existing Yierdis harness tests still green.

- [ ] **Step 5: Commit**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java
git commit -m "feat: add redis suite lifecycle and observations"
```

### Task 3: Encode Redis Scenario Compatibility And Comparison Reasons

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`

**Interfaces:**
- Consumes: `public record ScenarioDefinition(String id, String displayName, BenchWorkloadKind workload, int keyspace, int dataSize, int requests, int clients, int pipeline, int warmupIterations, int repeatIterations, boolean latency, ServerOverrides serverOverrides)`
- Produces: `public enum ScenarioDefinition.RedisComparable { YES, EXTERNAL_CONFIG_REQUIRED, NO }`
- Produces: `public String ScenarioDefinition.redisNonComparableReason()`
- Produces: `public static ScenarioComparison ScenarioComparison.compare(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current)`

- [ ] **Step 1: Write the failing tests**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because `ScenarioDefinition` has no Redis compatibility metadata and `ScenarioComparison` cannot derive Redis-specific non-comparable reasons.

- [ ] **Step 3: Write minimal implementation**

```java
// ScenarioDefinition.java
public enum RedisComparable {
    YES,
    EXTERNAL_CONFIG_REQUIRED,
    NO
}

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
        boolean latency,
        ServerOverrides serverOverrides,
        RedisComparable redisComparable,
        String redisNonComparableReason
) {
    public ScenarioDefinition {
        requireStableId(id);
        displayName = Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(workload, "workload");
        serverOverrides = serverOverrides == null ? ServerOverrides.none() : serverOverrides;
        redisComparable = redisComparable == null ? RedisComparable.YES : redisComparable;
        redisNonComparableReason = redisNonComparableReason == null ? "" : redisNonComparableReason;
    }
}
```

```java
// ScenarioDefinition.java convenience constructor retained for existing tests
public ScenarioDefinition(
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
    this(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
            warmupIterations, repeatIterations, latency, ServerOverrides.none(), RedisComparable.YES, "");
}
```

```java
// SuiteProfileFactory.java
scenario("release-native-defrag-append", "Native defrag APPEND p99", BenchWorkloadKind.NATIVE_DEFRAG_APPEND,
        4096, 256, 50_000, 8, 4, 1, 5, true,
        ScenarioDefinition.ServerOverrides.nativeDefrag(256L * 1024L, 256L, 5L),
        ScenarioDefinition.RedisComparable.NO, "yierdis-only native defrag scenario")
scenario("release-maxmemory-eviction", "Maxmemory eviction pressure", BenchWorkloadKind.MAXMEMORY_EVICTION,
        50_000, 512, 100_000, 32, 4, 1, 5, false,
        ScenarioDefinition.ServerOverrides.maxmemory(16L * 1024L * 1024L, "allkeys-lru", 16, 20L),
        ScenarioDefinition.RedisComparable.EXTERNAL_CONFIG_REQUIRED, "external Redis config required")
scenario("release-set-get-256b-c64-p8", "SET/GET 256B c64 p8", BenchWorkloadKind.SET_GET,
        500_000, 256, 500_000, 64, 8, 1, 5, true,
        ScenarioDefinition.ServerOverrides.none(),
        ScenarioDefinition.RedisComparable.YES, "")
```

```java
// ScenarioComparison.java
if (baseline.artifactLabel().equals("redis") && scenario.redisComparable() != ScenarioDefinition.RedisComparable.YES) {
    reasons.add(scenario.redisNonComparableReason());
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS with stable Redis-specific non-comparable reasons.

- [ ] **Step 5: Commit**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java
git commit -m "feat: add redis scenario comparability rules"
```

### Task 4: Render Redis Comparison Reports And Environment Metadata

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java`
- Modify: `README.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`

**Interfaces:**
- Consumes: `public record ScenarioComparison(ScenarioDefinition scenario, ScenarioPassResult baseline, ScenarioPassResult current, boolean comparable, String nonComparableReason, Map<String, Double> deltaPercentByMetric)`
- Produces: `public static SuiteEnvironment SuiteEnvironment.capture()` plus Redis-aware metadata capture used by `SuiteRunner`
- Produces: `public static String SuiteCsvWriter.comparisonsCsv(SuiteRunResult result)`
- Produces: `public static String SuiteMarkdownWriter.write(SuiteRunResult result)`

- [ ] **Step 1: Write the failing tests**

```java
@Test
public void comparisonsCsvIncludesRedisBaselineLabelAndReason() {
    SuiteRunResult result = RedisSuiteTestSupport.redisComparisonResult(false, "external Redis config required");

    String csv = SuiteCsvWriter.comparisonsCsv(result);

    Assert.assertTrue(csv.contains("baseline_artifact,current_artifact"));
    Assert.assertTrue(csv.contains("redis,current"));
    Assert.assertTrue(csv.contains("external Redis config required"));
}

@Test
public void markdownReportIncludesRedisSummarySection() {
    SuiteRunResult result = RedisSuiteTestSupport.redisComparisonResult(true, "");

    String markdown = SuiteMarkdownWriter.write(result);

    Assert.assertTrue(markdown.contains("## Redis Comparison Summary"));
    Assert.assertTrue(markdown.contains("redis -> current"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because current report writers assume Yierdis baseline/current only and do not emit Redis-specific summary text or environment notes.

- [ ] **Step 3: Write minimal implementation**

```java
// SuiteRunner.java
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
environment = new SuiteEnvironment(values);
```

```java
// SuiteCsvWriter.java
out.append("scenario_id,baseline_artifact,current_artifact,metric,baseline_value,current_value,delta_percent,ratio,comparable,reason\n");
for (ScenarioComparison comparison : result.comparisons()) {
    if (!comparison.comparable()) {
        out.append(comparison.scenario().id()).append(',')
                .append(comparison.baseline().artifactLabel()).append(',')
                .append(comparison.current().artifactLabel()).append(',')
                .append("comparability").append(",,,,")
                .append(Boolean.toString(false)).append(',')
                .append(csv(comparison.nonComparableReason()))
                .append('\n');
        continue;
    }
    out.append(comparison.scenario().id()).append(',')
            .append(comparison.baseline().artifactLabel()).append(',')
            .append(comparison.current().artifactLabel()).append(',')
            .append("qps").append(',')
            .append(formatMetric(comparison.baseline(), "qps")).append(',')
            .append(formatMetric(comparison.current(), "qps")).append(',')
            .append(formatDelta(comparison, "qps")).append(',')
            .append(formatRatio(comparison, "qps")).append(',')
            .append(Boolean.toString(comparison.comparable())).append(',')
            .append(csv(comparison.nonComparableReason()))
            .append('\n');
}
```

```java
// SuiteMarkdownWriter.java
if (result.artifacts().stream().anyMatch(artifact -> artifact.label().equals("redis"))) {
    out.append("## Redis Comparison Summary\n\n");
    out.append("- redis -> current\n");
    out.append("- External Redis configuration is operator-managed.\n");
}
```

```markdown
<!-- README.md -->
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --includeRedis \
  --redisHost 127.0.0.1 \
  --redisPort 6379 \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/redis-comparison

Recommended Redis settings:

save ""
appendonly no
maxmemory-policy noeviction
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS for Redis report rendering and Redis-specific doc coverage.

- [ ] **Step 5: Commit**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteEnvironment.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteCsvWriter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteMarkdownWriter.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java \
  README.md \
  docs/project-docs/client-and-bench-internals.md \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java
git commit -m "feat: render redis suite comparison reports"
```

### Task 5: Verify Full Benchmark Module Behavior And Manual Redis Flow

**Files:**
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`
- Modify: `docs/project-docs/client-and-bench-internals.md`

**Interfaces:**
- Consumes: all interfaces produced by Tasks 1-4
- Produces: a verified Redis suite comparison path with documented manual smoke steps

- [ ] **Step 1: Run focused benchmark tests**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,SuiteRunnerOrchestrationTest,ObservationClientTest,BenchHarnessExtendedWorkloadTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS for all Redis suite comparison focused tests.

- [ ] **Step 2: Run the full benchmark module tests**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test`

Expected: PASS for the full benchmark module.

- [ ] **Step 3: Package benchmark and server jars**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark,yierdis-server/yierdis-server-main -am -DskipTests package`

Expected: PASS and produce `yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar` plus `yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar`.

- [ ] **Step 4: Run manual Redis suite smoke**

Run:

```bash
redis-server --save '' --appendonly no --port 6379
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --includeRedis \
  --redisHost 127.0.0.1 \
  --redisPort 6379 \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/redis-comparison-smoke
```

Expected: PASS and produce `suite-result.json`, `metrics.csv`, `comparisons.csv`, and `report.md` with `redis` and `current` artifacts plus Redis environment metadata.

- [ ] **Step 5: Commit**

```bash
git add docs/project-docs/client-and-bench-internals.md
git commit -m "test: verify redis suite comparison flow"
```

## Self-Review

- Spec coverage: Task 1 covers CLI/config/artifact modeling. Task 2 covers Redis lifecycle and observation capture. Task 3 covers scenario compatibility and comparability reasons. Task 4 covers reporting, environment metadata, and docs. Task 5 covers verification and manual smoke.
- Placeholder scan: checked for `TBD`, `TODO`, and vague “handle appropriately” language; each task names exact files, test commands, and produced interfaces.
- Type consistency: the plan consistently uses `SuiteArtifact.Kind`, `SuiteConfig.artifactsInRunOrder()`, `ScenarioDefinition.RedisComparable`, explicit Redis metadata capture, and target-aware observation capture across later tasks.
