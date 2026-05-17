# DB Native Allocator Benchmark Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a narrow jar-only baseline/current comparison mode to `yierdis-benchmark` so Track 1 benchmark output is explicit, provenance-rich, and non-comparable when either side fails.

**Architecture:** Keep the existing single-run path intact and add a separate comparison path behind `--comparisonMode`. Reuse the existing RESP workload execution and `BackendResult` measurement fields, but render comparison output through dedicated comparison-side/result models that include labels, jar provenance, command lines, deltas, and failure status. Capture post-launch startup, protocol, workload, and partial-measurement failures into the comparison result instead of throwing them through as valid numeric deltas.

**Tech Stack:** Java 25, Maven, JUnit 4, picocli, existing pure Java RESP benchmark helpers in `yierdis-benchmark`.

---

## Scope

Track 1 only:

- Add jar-only comparison mode to `yierdis-benchmark`.
- Add CLI flags `--comparisonMode`, `--baselineServerJar <path>`, and `--currentServerJar <path>`.
- Keep existing single-run `--serverJar` and `--noStartServer` behavior unchanged.
- In comparison mode, reject `--serverJar`, reject `--noStartServer`, require both comparison jar flags, and fail pre-launch if either path is missing or not a regular file.
- Force the existing focused DB native defrag comparison off in comparison mode, equivalent to `--skipNativeDefragCompare`.
- Cover only the main RESP summary fields already produced by the benchmark: SET, GET, APPEND, PFADD sparse, PFADD dense, PFCOUNT, and latency metrics when latency is enabled.
- Capture post-launch failures as non-comparable comparison side results.
- Update the existing benchmark report artifact so raw historical baseline numbers are not presented as a trustworthy before/after pair when the baseline remains broken.

Out of scope:

- Do not modify allocator internals, collection internals, server runtime behavior, CI infrastructure, or production hardening.
- Do not change DB native defrag behavior except skipping the existing focused defrag compare in comparison mode.
- Do not rewrite `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`.
- Do not modify or commit untracked `yierdis.md`.

## File Map

- Modify `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
  - Owns new picocli options: `comparisonMode`, `baselineServerJar`, `currentServerJar`.
- Modify `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
  - Extend `BenchConfig` validation and state.
  - Add `ServerProcess.commandLine()` so launch command construction has one source of truth.
  - Add comparison context/model/renderer nested classes.
  - Extract the existing main RESP workload body into a helper shared by single-run and comparison mode.
  - Add comparison-mode main branch and failure capture.
- Add `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java`
  - CLI/config validation, forced defrag skip, jar regular-file validation.
- Modify `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchServerArgsReuseTest.java`
  - Verify `ServerProcess.commandLine()` and shared argv shape.
- Add `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`
  - Stable comparison rendering, deltas, non-comparable failure rendering, provenance, known and unknown commit labels.
- Add `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java`
  - Side context construction, side labels, port offsets, shared server argv, command provenance without launching a process.
- Modify `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`
  - Record Track 1 comparison-mode behavior and caveated baseline state.

## Task 1: CLI Validation And Command Provenance Helper

**Files:**

- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchServerArgsReuseTest.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java`

- [ ] **Step 1: Write failing config validation tests**

Create `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java`:

```java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class BenchComparisonConfigTest {
    @Test
    public void comparisonModeRequiresBothJarFlags() throws Exception {
        Path baselineJar = regularTempJar("baseline");

        IllegalArgumentException missingCurrent = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString()
        );
        Assert.assertTrue(missingCurrent.getMessage().contains("currentServerJar"));

        Path currentJar = regularTempJar("current");
        YierdisBench.BenchConfig config = config(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString()
        );

        Assert.assertTrue(config.comparisonMode);
        Assert.assertFalse(config.noStartServer);
        Assert.assertEquals(baselineJar, config.baselineServerJar);
        Assert.assertEquals(currentJar, config.currentServerJar);
        Assert.assertEquals(List.of("baseline", "current"), config.backends);
        Assert.assertTrue(config.skipNativeDefragCompare);
    }

    @Test
    public void comparisonModeRejectsSingleRunServerJar() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");
        Path singleRunJar = regularTempJar("single");

        IllegalArgumentException rejected = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--serverJar", singleRunJar.toString()
        );

        Assert.assertTrue(rejected.getMessage().contains("serverJar"));
    }

    @Test
    public void comparisonModeRejectsNoStartServer() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");

        IllegalArgumentException rejected = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--noStartServer"
        );

        Assert.assertTrue(rejected.getMessage().contains("noStartServer"));
    }

    @Test
    public void comparisonModeRejectsMissingOrNonRegularJarPathsBeforeLaunch() throws Exception {
        Path currentJar = regularTempJar("current");
        Path missingBaseline = Files.createTempDirectory("missing-baseline-").resolve("server.jar");
        IllegalArgumentException missing = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", missingBaseline.toString(),
                "--currentServerJar", currentJar.toString()
        );
        Assert.assertTrue(missing.getMessage().contains("baselineServerJar"));
        Assert.assertTrue(missing.getMessage().contains(missingBaseline.toAbsolutePath().toString()));

        Path baselineJar = regularTempJar("baseline");
        Path currentDirectory = Files.createTempDirectory("current-dir-");
        IllegalArgumentException nonRegular = assertRejects(
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentDirectory.toString()
        );
        Assert.assertTrue(nonRegular.getMessage().contains("currentServerJar"));
        Assert.assertTrue(nonRegular.getMessage().contains(currentDirectory.toAbsolutePath().toString()));
    }

    @Test
    public void singleRunServerJarAndNoStartServerBehaviorRemainUnchanged() throws Exception {
        Path serverJar = regularTempJar("single");

        YierdisBench.BenchConfig jarConfig = config("--serverJar", serverJar.toString());
        Assert.assertFalse(jarConfig.comparisonMode);
        Assert.assertEquals(serverJar, jarConfig.serverJar);
        Assert.assertEquals(List.of("foreign"), jarConfig.backends);

        YierdisBench.BenchConfig externalConfig = config("--noStartServer");
        Assert.assertFalse(externalConfig.comparisonMode);
        Assert.assertTrue(externalConfig.noStartServer);
        Assert.assertEquals(List.of("external"), externalConfig.backends);
    }

    private static YierdisBench.BenchConfig config(String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        return YierdisBench.BenchConfig.from(args, serverArgs);
    }

    private static IllegalArgumentException assertRejects(String... argv) {
        try {
            config(argv);
            Assert.fail("expected comparison config validation failure");
            return null;
        } catch (IllegalArgumentException expected) {
            return expected;
        }
    }

    private static Path regularTempJar(String label) throws Exception {
        Path jar = Files.createTempFile("bench-" + label + "-", ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }
}
```

- [ ] **Step 2: Extend the existing server argv test with commandLine provenance**

In `BenchServerArgsReuseTest.serverProcessUsesNormalizedArgvFromLaunchCopy`, move the `expected` command construction before `process.start()`, then assert `process.commandLine()` before the fake process is launched. The portion after `YierdisBench.ServerProcess process = ...` should become:

```java
List<String> expected = new ArrayList<>();
expected.add("-Xms4g");
expected.add("-Xmx4g");
expected.add("-XX:MaxDirectMemorySize=6g");
expected.add("-jar");
expected.add(fakeJar.toAbsolutePath().toString());
expected.addAll(serverArgsForRun.toArgv());

List<String> expectedCommandLine = new ArrayList<>();
expectedCommandLine.add(script.toAbsolutePath().toString());
expectedCommandLine.addAll(expected);
Assert.assertEquals(expectedCommandLine, process.commandLine());

try {
    process.start();

    List<String> actual = waitForLines(logFile, expected.size());
    Assert.assertEquals(expected, actual);
    assertArgValue(actual, "--executorSchedulingPolicy", "global");
    assertArgValue(actual, "--maxmemoryScope", "per-db");
    assertArgValue(actual, "--maxmemoryPolicy", "allkeys-lru");
    assertArgValue(actual, "--nativeDefragMaxMoveBytes", "1024");
    assertArgValue(actual, "--nativeDefragMaxObjects", "7");
    assertArgValue(actual, "--nativeDefragTimeLimitMillis", "3");
    Assert.assertTrue(actual.contains("--nativeDefragEnabled"));
    Assert.assertTrue(actual.contains("--noCleanup"));
    Assert.assertFalse(actual.contains("--offheapBackend"));
    Assert.assertFalse(actual.contains("--offheapMaxBytes"));
} finally {
    process.stop();
}
```

- [ ] **Step 3: Run tests and verify they fail for missing CLI/config symbols**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchComparisonConfigTest,BenchServerArgsReuseTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL. The failure should include compile errors such as `cannot find symbol` for `comparisonMode`, `baselineServerJar`, `currentServerJar`, or `commandLine()`.

- [ ] **Step 4: Add comparison options**

In `YierdisBenchArgs.java`, insert these options after `serverJar`:

```java
@Option(names = "--comparisonMode", description = "Run jar-only baseline/current comparison mode.")
public boolean comparisonMode;

@Option(names = "--baselineServerJar", description = "Path to baseline yierdis server jar for comparison mode.")
public Path baselineServerJar;

@Option(names = "--currentServerJar", description = "Path to current yierdis server jar for comparison mode.")
public Path currentServerJar;
```

- [ ] **Step 5: Extend `BenchConfig` fields, constructor, and validation**

In `YierdisBench.BenchConfig`, add fields after `serverJar`:

```java
final boolean comparisonMode;
final Path baselineServerJar;
final Path currentServerJar;
```

Update the private constructor signature so the first arguments are:

```java
private BenchConfig(
        boolean noStartServer,
        Path serverJar,
        boolean comparisonMode,
        Path baselineServerJar,
        Path currentServerJar,
        List<String> backends,
        String host,
        int portBase,
        String javaCmd,
        String serverXms,
        String serverXmx,
        String serverMaxDirectMemory,
        YierdisBenchServerArgs baseServerArgs,
        int keyspace,
        int dataSize,
        int requests,
        int clients,
        int pipeline,
        int latencyRequests,
        int latencyClients,
        boolean skipPrefill,
        boolean skipLatency,
        boolean strictReplies,
        boolean skipNativeDefragCompare,
        boolean nativeEval,
        int nativeEvalIterations
)
```

Assign the new fields immediately after `this.serverJar = serverJar;`:

```java
this.comparisonMode = comparisonMode;
this.baselineServerJar = baselineServerJar;
this.currentServerJar = currentServerJar;
```

Replace `BenchConfig.from` with:

```java
static BenchConfig from(YierdisBenchArgs args, YierdisBenchServerArgs baseServerArgs) {
    Objects.requireNonNull(args, "args");
    Objects.requireNonNull(baseServerArgs, "baseServerArgs");

    if (args.comparisonMode) {
        if (args.serverJar != null) {
            throw new IllegalArgumentException("comparisonMode rejects serverJar; use baselineServerJar/currentServerJar");
        }
        if (args.noStartServer) {
            throw new IllegalArgumentException("comparisonMode rejects noStartServer; comparison mode starts both jars");
        }
        if (args.baselineServerJar == null) {
            throw new IllegalArgumentException("comparisonMode requires baselineServerJar");
        }
        if (args.currentServerJar == null) {
            throw new IllegalArgumentException("comparisonMode requires currentServerJar");
        }
        requireRegularFile("baselineServerJar", args.baselineServerJar);
        requireRegularFile("currentServerJar", args.currentServerJar);
    } else if (args.serverJar != null) {
        requireRegularFile("serverJar", args.serverJar);
    }
    if (args.nativeEval) {
        effectiveNativeEvalIterations(args.nativeEvalIterations);
    }

    List<String> backends = args.comparisonMode
            ? List.of("baseline", "current")
            : (args.noStartServer ? List.of("external") : DEFAULT_BACKENDS);

    return new BenchConfig(
            args.noStartServer,
            args.serverJar,
            args.comparisonMode,
            args.baselineServerJar,
            args.currentServerJar,
            backends,
            args.host,
            args.portBase,
            args.javaCmd,
            args.xms,
            args.xmx,
            args.maxDirectMemory,
            baseServerArgs,
            args.keyspace,
            args.dataSize,
            args.requests,
            args.clients,
            args.pipeline,
            args.latencyRequests,
            args.latencyClients,
            args.skipPrefill,
            args.skipLatency,
            args.strictReplies,
            args.comparisonMode || args.skipNativeDefragCompare,
            args.nativeEval,
            args.nativeEvalIterations
    );
}
```

Add this helper inside `YierdisBench`, near `findServerJar()`:

```java
private static void requireRegularFile(String optionName, Path path) {
    if (!Files.isRegularFile(path)) {
        throw new IllegalArgumentException(optionName + " 不存在或不是普通文件: " + path.toAbsolutePath());
    }
}
```

- [ ] **Step 6: Add `ServerProcess.commandLine()` and reuse it in `start()`**

In `YierdisBench.ServerProcess`, insert before `start()`:

```java
List<String> commandLine() {
    List<String> cmd = new ArrayList<>();
    cmd.add(javaCmd);
    cmd.add("-Xms" + xms);
    cmd.add("-Xmx" + xmx);
    cmd.add("-XX:MaxDirectMemorySize=" + maxDirectMemory);
    cmd.add("-jar");
    cmd.add(serverJar.toAbsolutePath().toString());
    cmd.addAll(serverArgs.toArgv());
    return cmd;
}
```

Then replace command construction in `start()` with:

```java
List<String> cmd = commandLine();
```

- [ ] **Step 7: Run tests and verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchComparisonConfigTest,BenchServerArgsReuseTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 8: Commit Task 1**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchServerArgsReuseTest.java yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java
git commit -m "feat: validate benchmark comparison jar mode"
```

## Task 2: Comparison Model And Renderer

**Files:**

- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`

- [ ] **Step 1: Write failing renderer tests**

Create `YierdisBenchComparisonRenderTest.java`:

```java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class YierdisBenchComparisonRenderTest {
    @Test
    public void renderComparableComparisonShowsLabelsProvenanceAndDeltas() {
        YierdisBench.BackendResult baseline = fullResult("baseline", 16378, 1000.0, 2000.0, new long[]{1_000_000, 1_500_000, 2_000_000});
        YierdisBench.BackendResult current = fullResult("current", 16379, 1250.0, 1500.0, new long[]{1_000_000, 1_250_000, 1_500_000});

        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.success(
                        "baseline",
                        Path.of("/tmp/baseline.jar"),
                        List.of("java", "-jar", "/tmp/baseline.jar", "--port", "16378"),
                        "79228e3",
                        baseline
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current",
                        Path.of("/tmp/current.jar"),
                        List.of("java", "-jar", "/tmp/current.jar", "--port", "16379"),
                        "9b9f58c",
                        current
                ),
                false,
                "commit labels supplied by benchmark operator"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("[comparison]"));
        Assert.assertTrue(rendered.contains("status: comparable"));
        Assert.assertTrue(rendered.contains("baseline jar: /tmp/baseline.jar"));
        Assert.assertTrue(rendered.contains("current jar: /tmp/current.jar"));
        Assert.assertTrue(rendered.contains("baseline commit: 79228e3"));
        Assert.assertTrue(rendered.contains("current commit: 9b9f58c"));
        Assert.assertTrue(rendered.contains("baseline command: java -jar /tmp/baseline.jar --port 16378"));
        Assert.assertTrue(rendered.contains("current command: java -jar /tmp/current.jar --port 16379"));
        Assert.assertTrue(rendered.contains("side"));
        Assert.assertTrue(rendered.contains("status"));
        Assert.assertTrue(rendered.contains("SET_QPS"));
        Assert.assertTrue(rendered.contains("SET_delta_pct"));
        Assert.assertTrue(rendered.contains("GET_QPS"));
        Assert.assertTrue(rendered.contains("GET_delta_pct"));
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("PING_delta_pct"));
        Assert.assertTrue(rendered.contains("baseline"));
        Assert.assertTrue(rendered.contains("current"));
        Assert.assertTrue(rendered.contains("1000.000"));
        Assert.assertTrue(rendered.contains("1250.000"));
        Assert.assertTrue(rendered.contains("+25.000%"));
        Assert.assertTrue(rendered.contains("2.000"));
        Assert.assertTrue(rendered.contains("1.500"));
        Assert.assertTrue(rendered.contains("-25.000%"));
    }

    @Test
    public void renderFailureComparisonMarksPairNonComparableAndSuppressesDeltas() {
        YierdisBench.BackendResult partial = new YierdisBench.BackendResult("baseline", 16378);
        partial.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 100, 0, 1.0, 100.0, Instant.parse("2026-05-16T00:00:00Z")
        );
        YierdisBench.BackendResult current = fullResult("current", 16379, 1250.0, 1500.0, new long[]{1_000_000, 1_250_000, 1_500_000});

        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.failure(
                        "baseline",
                        Path.of("/tmp/baseline.jar"),
                        List.of("java", "-jar", "/tmp/baseline.jar", "--port", "16378"),
                        "unknown",
                        partial,
                        true,
                        "SET failed: -ERR internal error"
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current",
                        Path.of("/tmp/current.jar"),
                        List.of("java", "-jar", "/tmp/current.jar", "--port", "16379"),
                        "9b9f58c",
                        current
                ),
                false,
                "baseline artifact commit cannot be tied to this workspace; historical baseline failed in this environment"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("status: non-comparable"));
        Assert.assertTrue(rendered.contains("environment: baseline artifact commit cannot be tied to this workspace; historical baseline failed in this environment"));
        Assert.assertTrue(rendered.contains("baseline status: failed-partial"));
        Assert.assertTrue(rendered.contains("baseline failure: SET failed: -ERR internal error"));
        Assert.assertTrue(rendered.contains("current status: ok"));
        Assert.assertTrue(rendered.contains("n/a"));
        Assert.assertFalse(rendered.contains("+25.000%"));
    }

    @Test
    public void renderSkipLatencyComparisonOmitsLatencyColumns() {
        YierdisBench.ComparisonResult result = new YierdisBench.ComparisonResult(
                YierdisBench.ComparisonSideResult.success(
                        "baseline", Path.of("/tmp/baseline.jar"), List.of("java", "-jar", "/tmp/baseline.jar"), "unknown", throughputOnly("baseline", 16378, 1000.0)
                ),
                YierdisBench.ComparisonSideResult.success(
                        "current", Path.of("/tmp/current.jar"), List.of("java", "-jar", "/tmp/current.jar"), "unknown", throughputOnly("current", 16379, 1200.0)
                ),
                true,
                "commit labels unavailable for supplied artifacts"
        );

        String rendered = YierdisBench.renderComparison(result);

        Assert.assertTrue(rendered.contains("status: comparable"));
        Assert.assertTrue(rendered.contains("SET_delta_pct"));
        Assert.assertFalse(rendered.contains("PING_p95(ms)"));
        Assert.assertFalse(rendered.contains("PING_delta_pct"));
        Assert.assertTrue(rendered.contains("commit labels unavailable for supplied artifacts"));
    }

    private static YierdisBench.BackendResult fullResult(String label, int port, double setQps, double getQps, long[] latencyNanos) {
        YierdisBench.BackendResult result = throughputOnly(label, port, setQps);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, getQps, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, setQps / 2.0, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, setQps / 4.0, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, setQps / 5.0, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, getQps / 2.0, 0);
        result.pingLatency = latency(YierdisBench.Workload.PING, 0, latencyNanos);
        result.setLatency = latency(YierdisBench.Workload.SET_RANDOM, 0, latencyNanos);
        result.getLatency = latency(YierdisBench.Workload.GET_RANDOM, 0, latencyNanos);
        result.appendLatency = latency(YierdisBench.Workload.APPEND, 0, latencyNanos);
        result.pfaddSparseLatency = latency(YierdisBench.Workload.PFADD_SPARSE, 0, latencyNanos);
        result.pfaddDenseLatency = latency(YierdisBench.Workload.PFADD_DENSE, 0, latencyNanos);
        return result;
    }

    private static YierdisBench.BackendResult throughputOnly(String label, int port, double setQps) {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult(label, port);
        result.setThroughput = throughput(YierdisBench.Workload.SET_RANDOM, setQps, 0);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, setQps * 2.0, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, setQps / 2.0, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, setQps / 4.0, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, setQps / 5.0, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, setQps, 0);
        return result;
    }

    private static YierdisBench.ThroughputResult throughput(YierdisBench.Workload workload, double qps, long errors) {
        return new YierdisBench.ThroughputResult(
                workload, 1000, errors, 1.0, qps, Instant.parse("2026-05-16T00:00:00Z")
        );
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors) {
        return latency(workload, errors, new long[]{1_000_000, 1_500_000, 2_000_000});
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors, long[] sortedNanos) {
        return new YierdisBench.LatencyResult(
                workload, 1000, errors, 1.0, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(sortedNanos)
        );
    }
}
```

- [ ] **Step 2: Run tests and verify they fail for missing model and renderer**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL with compile errors for `ComparisonResult`, `ComparisonSideResult`, or `renderComparison`.

- [ ] **Step 3: Add comparison result models**

Inside `YierdisBench`, place these nested classes after `BenchConfig` and before `ServerProcess`:

```java
static final class ComparisonResult {
    final ComparisonSideResult baseline;
    final ComparisonSideResult current;
    final boolean skipLatency;
    final String environmentCaveat;

    ComparisonResult(
            ComparisonSideResult baseline,
            ComparisonSideResult current,
            boolean skipLatency,
            String environmentCaveat
    ) {
        this.baseline = Objects.requireNonNull(baseline, "baseline");
        this.current = Objects.requireNonNull(current, "current");
        this.skipLatency = skipLatency;
        this.environmentCaveat = Objects.requireNonNull(environmentCaveat, "environmentCaveat");
    }

    boolean comparable() {
        return baseline.comparable() && current.comparable();
    }
}

static final class ComparisonSideResult {
    final String label;
    final Path jarPath;
    final List<String> commandLine;
    final String commitLabel;
    final BackendResult result;
    final boolean failed;
    final boolean partial;
    final String failureMessage;

    private ComparisonSideResult(
            String label,
            Path jarPath,
            List<String> commandLine,
            String commitLabel,
            BackendResult result,
            boolean failed,
            boolean partial,
            String failureMessage
    ) {
        this.label = Objects.requireNonNull(label, "label");
        this.jarPath = Objects.requireNonNull(jarPath, "jarPath");
        this.commandLine = List.copyOf(commandLine);
        this.commitLabel = Objects.requireNonNull(commitLabel, "commitLabel");
        this.result = Objects.requireNonNull(result, "result");
        this.failed = failed;
        this.partial = partial;
        this.failureMessage = failureMessage == null ? "" : failureMessage;
    }

    static ComparisonSideResult success(
            String label,
            Path jarPath,
            List<String> commandLine,
            String commitLabel,
            BackendResult result
    ) {
        return new ComparisonSideResult(label, jarPath, commandLine, commitLabel, result, false, false, "");
    }

    static ComparisonSideResult failure(
            String label,
            Path jarPath,
            List<String> commandLine,
            String commitLabel,
            BackendResult result,
            boolean partial,
            String failureMessage
    ) {
        return new ComparisonSideResult(label, jarPath, commandLine, commitLabel, result, true, partial, failureMessage);
    }

    boolean comparable() {
        return !failed && !partial;
    }

    String statusLabel() {
        if (failed && partial) {
            return "failed-partial";
        }
        if (failed) {
            return "failed";
        }
        if (partial) {
            return "partial";
        }
        return "ok";
    }
}
```

- [ ] **Step 4: Add comparison renderer helpers**

Inside `YierdisBench`, place this renderer near `renderSummary`:

```java
static String renderComparison(ComparisonResult result) {
    Objects.requireNonNull(result, "result");
    StringBuilder sb = new StringBuilder();

    sb.append("[comparison]\n");
    sb.append("status: ").append(result.comparable() ? "comparable" : "non-comparable").append('\n');
    sb.append("environment: ").append(result.environmentCaveat).append('\n');
    appendComparisonProvenance(sb, "baseline", result.baseline);
    appendComparisonProvenance(sb, "current", result.current);
    sb.append('\n');

    String header = result.skipLatency
            ? String.format(
            "%-8s | %-14s | %12s | %13s | %12s | %13s | %12s | %13s | %16s | %17s | %16s | %17s | %12s | %13s",
            "side", "status", "SET_QPS", "SET_delta_pct", "GET_QPS", "GET_delta_pct",
            "APPEND_QPS", "APPEND_delta", "PFADD_S_QPS", "PFADD_S_delta",
            "PFADD_D_QPS", "PFADD_D_delta", "PFCOUNT_QPS", "PFCOUNT_delta"
    )
            : String.format(
            "%-8s | %-14s | %12s | %13s | %12s | %13s | %14s | %14s | %14s | %14s | %14s | %14s | %12s | %13s | %14s | %14s | %16s | %17s | %16s | %17s | %12s | %13s | %18s | %18s | %18s | %18s",
            "side", "status", "SET_QPS", "SET_delta_pct", "GET_QPS", "GET_delta_pct",
            "PING_p95(ms)", "PING_delta_pct", "SET_p95(ms)", "SET_lat_delta", "GET_p95(ms)", "GET_lat_delta",
            "APPEND_QPS", "APPEND_delta", "APPEND_p95(ms)", "APPEND_lat_delta",
            "PFADD_S_QPS", "PFADD_S_delta", "PFADD_D_QPS", "PFADD_D_delta", "PFCOUNT_QPS", "PFCOUNT_delta",
            "PFADD_S_p95(ms)", "PFADD_S_lat_delta", "PFADD_D_p95(ms)", "PFADD_D_lat_delta"
    );
    appendTableHeader(sb, header);
    appendComparisonRow(sb, result.baseline, result.baseline, result.comparable(), result.skipLatency);
    appendComparisonRow(sb, result.current, result.baseline, result.comparable(), result.skipLatency);

    if (!result.baseline.comparable()) {
        sb.append("baseline status: ").append(result.baseline.statusLabel()).append('\n');
        sb.append("baseline failure: ").append(result.baseline.failureMessage).append('\n');
    }
    if (!result.current.comparable()) {
        sb.append("current status: ").append(result.current.statusLabel()).append('\n');
        sb.append("current failure: ").append(result.current.failureMessage).append('\n');
    }
    return sb.toString();
}

private static void appendComparisonProvenance(StringBuilder sb, String prefix, ComparisonSideResult side) {
    sb.append(prefix).append(" jar: ").append(side.jarPath.toAbsolutePath()).append('\n');
    sb.append(prefix).append(" command: ").append(String.join(" ", side.commandLine)).append('\n');
    sb.append(prefix).append(" commit: ").append(side.commitLabel).append('\n');
}

private static void appendComparisonRow(
        StringBuilder sb,
        ComparisonSideResult side,
        ComparisonSideResult baseline,
        boolean comparable,
        boolean skipLatency
) {
    BackendResult r = side.result;
    BackendResult b = baseline.result;
    String setQps = throughputQps(r.setThroughput);
    String getQps = throughputQps(r.getThroughput);
    String appendQps = throughputQps(r.appendThroughput);
    String pfaddSparseQps = throughputQps(r.pfaddSparseThroughput);
    String pfaddDenseQps = throughputQps(r.pfaddDenseThroughput);
    String pfcountQps = throughputQps(r.pfcountThroughput);

    boolean showDelta = comparable && side != baseline;
    String setDelta = showDelta ? deltaPct(b.setThroughput == null ? null : b.setThroughput.qps, r.setThroughput == null ? null : r.setThroughput.qps) : "-";
    String getDelta = showDelta ? deltaPct(b.getThroughput == null ? null : b.getThroughput.qps, r.getThroughput == null ? null : r.getThroughput.qps) : "-";
    String appendDelta = showDelta ? deltaPct(b.appendThroughput == null ? null : b.appendThroughput.qps, r.appendThroughput == null ? null : r.appendThroughput.qps) : "-";
    String pfaddSparseDelta = showDelta ? deltaPct(b.pfaddSparseThroughput == null ? null : b.pfaddSparseThroughput.qps, r.pfaddSparseThroughput == null ? null : r.pfaddSparseThroughput.qps) : "-";
    String pfaddDenseDelta = showDelta ? deltaPct(b.pfaddDenseThroughput == null ? null : b.pfaddDenseThroughput.qps, r.pfaddDenseThroughput == null ? null : r.pfaddDenseThroughput.qps) : "-";
    String pfcountDelta = showDelta ? deltaPct(b.pfcountThroughput == null ? null : b.pfcountThroughput.qps, r.pfcountThroughput == null ? null : r.pfcountThroughput.qps) : "-";

    if (skipLatency) {
        sb.append(String.format(
                "%-8s | %-14s | %12s | %13s | %12s | %13s | %12s | %13s | %16s | %17s | %16s | %17s | %12s | %13s",
                side.label, side.statusLabel(), setQps, comparisonDelta(setDelta, comparable, showDelta), getQps, comparisonDelta(getDelta, comparable, showDelta),
                appendQps, comparisonDelta(appendDelta, comparable, showDelta), pfaddSparseQps, comparisonDelta(pfaddSparseDelta, comparable, showDelta),
                pfaddDenseQps, comparisonDelta(pfaddDenseDelta, comparable, showDelta), pfcountQps, comparisonDelta(pfcountDelta, comparable, showDelta)
        )).append('\n');
        return;
    }

    String pingP95 = latencyP95(r.pingLatency);
    String setP95 = latencyP95(r.setLatency);
    String getP95 = latencyP95(r.getLatency);
    String appendP95 = latencyP95(r.appendLatency);
    String pfaddSparseP95 = latencyP95(r.pfaddSparseLatency);
    String pfaddDenseP95 = latencyP95(r.pfaddDenseLatency);
    String pingDelta = showDelta ? deltaPct(b.pingLatency == null ? null : b.pingLatency.stats.p95Millis(), r.pingLatency == null ? null : r.pingLatency.stats.p95Millis()) : "-";
    String setLatDelta = showDelta ? deltaPct(b.setLatency == null ? null : b.setLatency.stats.p95Millis(), r.setLatency == null ? null : r.setLatency.stats.p95Millis()) : "-";
    String getLatDelta = showDelta ? deltaPct(b.getLatency == null ? null : b.getLatency.stats.p95Millis(), r.getLatency == null ? null : r.getLatency.stats.p95Millis()) : "-";
    String appendLatDelta = showDelta ? deltaPct(b.appendLatency == null ? null : b.appendLatency.stats.p95Millis(), r.appendLatency == null ? null : r.appendLatency.stats.p95Millis()) : "-";
    String pfaddSparseLatDelta = showDelta ? deltaPct(b.pfaddSparseLatency == null ? null : b.pfaddSparseLatency.stats.p95Millis(), r.pfaddSparseLatency == null ? null : r.pfaddSparseLatency.stats.p95Millis()) : "-";
    String pfaddDenseLatDelta = showDelta ? deltaPct(b.pfaddDenseLatency == null ? null : b.pfaddDenseLatency.stats.p95Millis(), r.pfaddDenseLatency == null ? null : r.pfaddDenseLatency.stats.p95Millis()) : "-";

    sb.append(String.format(
            "%-8s | %-14s | %12s | %13s | %12s | %13s | %14s | %14s | %14s | %14s | %14s | %14s | %12s | %13s | %14s | %14s | %16s | %17s | %16s | %17s | %12s | %13s | %18s | %18s | %18s | %18s",
            side.label, side.statusLabel(), setQps, comparisonDelta(setDelta, comparable, showDelta), getQps, comparisonDelta(getDelta, comparable, showDelta),
            pingP95, comparisonDelta(pingDelta, comparable, showDelta), setP95, comparisonDelta(setLatDelta, comparable, showDelta), getP95, comparisonDelta(getLatDelta, comparable, showDelta),
            appendQps, comparisonDelta(appendDelta, comparable, showDelta), appendP95, comparisonDelta(appendLatDelta, comparable, showDelta),
            pfaddSparseQps, comparisonDelta(pfaddSparseDelta, comparable, showDelta), pfaddDenseQps, comparisonDelta(pfaddDenseDelta, comparable, showDelta), pfcountQps, comparisonDelta(pfcountDelta, comparable, showDelta),
            pfaddSparseP95, comparisonDelta(pfaddSparseLatDelta, comparable, showDelta), pfaddDenseP95, comparisonDelta(pfaddDenseLatDelta, comparable, showDelta)
    )).append('\n');
}

private static String throughputQps(ThroughputResult result) {
    return result == null ? "-" : DF.format(result.qps);
}

private static String latencyP95(LatencyResult result) {
    return result == null ? "-" : DF.format(result.stats.p95Millis());
}

private static String comparisonDelta(String delta, boolean comparable, boolean showDelta) {
    if (!comparable) {
        return "n/a";
    }
    return showDelta ? delta : "-";
}

private static String deltaPct(Double baseline, Double current) {
    if (baseline == null || current == null || baseline == 0.0) {
        return "n/a";
    }
    double pct = ((current - baseline) * 100.0) / baseline;
    return (pct >= 0.0 ? "+" : "") + DF.format(pct) + "%";
}
```

- [ ] **Step 5: Run renderer tests and verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java
git commit -m "feat: render benchmark baseline comparisons"
```

## Task 3: Comparison Execution Path And Failure Capture

**Files:**

- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java`

- [ ] **Step 1: Write failing side-context tests without launching processes**

Create `YierdisBenchComparisonExecutionTest.java`:

```java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class YierdisBenchComparisonExecutionTest {
    @Test
    public void comparisonSideContextsUseSharedServerArgvWithSideSpecificJarAndPort() throws Exception {
        Path baselineJar = regularTempJar("baseline");
        Path currentJar = regularTempJar("current");
        Path runDir = Files.createTempDirectory("bench-comparison-run-");

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        new CommandLine(serverArgs).parseArgs(
                "--maxmemoryScope", "Per_Db",
                "--maxmemoryPolicy", "ALLKEYS-LRU",
                "--nativeDefragEnabled",
                "--keysTimeBudgetMillis", "0",
                "--keysMaxResults", "0"
        );
        serverArgs.normalizeAndValidate();

        YierdisBench.BenchConfig config = config(serverArgs,
                "--comparisonMode",
                "--baselineServerJar", baselineJar.toString(),
                "--currentServerJar", currentJar.toString(),
                "--javaCmd", "/usr/lib/jvm/java-25-openjdk-amd64/bin/java",
                "--xms", "512m",
                "--xmx", "512m",
                "--maxDirectMemory", "1g",
                "--portBase", "17378"
        );

        YierdisBench.ComparisonSideContext baseline = YierdisBench.comparisonSideContext(config, "baseline", baselineJar, 0, runDir);
        YierdisBench.ComparisonSideContext current = YierdisBench.comparisonSideContext(config, "current", currentJar, 1, runDir);

        Assert.assertEquals("baseline", baseline.label);
        Assert.assertEquals("current", current.label);
        Assert.assertEquals(17378, baseline.port);
        Assert.assertEquals(17379, current.port);
        Assert.assertEquals(baselineJar, baseline.jarPath);
        Assert.assertEquals(currentJar, current.jarPath);
        Assert.assertEquals(runDir.resolve("server-baseline.log"), baseline.logFile);
        Assert.assertEquals(runDir.resolve("server-current.log"), current.logFile);

        List<String> baselineArgv = baseline.serverArgs.toArgv();
        List<String> currentArgv = current.serverArgs.toArgv();
        Assert.assertNotEquals(baselineArgv, currentArgv);
        assertArgValue(baselineArgv, "--port", "17378");
        assertArgValue(currentArgv, "--port", "17379");
        Assert.assertTrue(baselineArgv.contains("--nativeDefragEnabled"));
        Assert.assertTrue(currentArgv.contains("--nativeDefragEnabled"));
        assertArgValue(baselineArgv, "--maxmemoryScope", "per-db");
        assertArgValue(currentArgv, "--maxmemoryScope", "per-db");

        Assert.assertTrue(baseline.commandLine.contains(baselineJar.toAbsolutePath().toString()));
        Assert.assertTrue(current.commandLine.contains(currentJar.toAbsolutePath().toString()));
        Assert.assertFalse(baseline.commandLine.contains(currentJar.toAbsolutePath().toString()));
        Assert.assertFalse(current.commandLine.contains(baselineJar.toAbsolutePath().toString()));
    }

    @Test
    public void comparisonSideValidationDetectsMissingMeasurementsAndErrors() {
        YierdisBench.BackendResult empty = new YierdisBench.BackendResult("baseline", 16378);
        Assert.assertFalse(YierdisBench.comparisonSideHasAnyMeasurements(empty));
        Assert.assertFalse(YierdisBench.comparisonSideHasRequiredMeasurements(empty, false));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(empty, false));

        YierdisBench.BackendResult partial = new YierdisBench.BackendResult("baseline", 16378);
        partial.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 100, 0, 1.0, 100.0, java.time.Instant.parse("2026-05-16T00:00:00Z")
        );
        Assert.assertTrue(YierdisBench.comparisonSideHasAnyMeasurements(partial));
        Assert.assertFalse(YierdisBench.comparisonSideHasRequiredMeasurements(partial, true));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(partial, true));

        YierdisBench.BackendResult completeWithErrors = completeThroughputOnly("baseline", 16378, 1);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeWithErrors, true));
        Assert.assertTrue(YierdisBench.comparisonSideHasBenchmarkErrors(completeWithErrors, true));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(completeWithErrors, true));

        YierdisBench.BackendResult completeWithLatencyErrors = completeWithLatency("baseline", 16378, 2);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeWithLatencyErrors, false));
        Assert.assertTrue(YierdisBench.comparisonSideHasBenchmarkErrors(completeWithLatencyErrors, false));
        Assert.assertFalse(YierdisBench.comparisonSideCanBeCompared(completeWithLatencyErrors, false));

        YierdisBench.BackendResult completeClean = completeThroughputOnly("baseline", 16378, 0);
        Assert.assertTrue(YierdisBench.comparisonSideHasRequiredMeasurements(completeClean, true));
        Assert.assertFalse(YierdisBench.comparisonSideHasBenchmarkErrors(completeClean, true));
        Assert.assertTrue(YierdisBench.comparisonSideCanBeCompared(completeClean, true));
    }

    private static YierdisBench.BenchConfig config(YierdisBenchServerArgs serverArgs, String... argv) {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs(argv);
        return YierdisBench.BenchConfig.from(args, serverArgs);
    }

    private static Path regularTempJar(String label) throws Exception {
        Path jar = Files.createTempFile("bench-" + label + "-", ".jar");
        Files.writeString(jar, "stub", StandardCharsets.US_ASCII);
        return jar;
    }

    private static YierdisBench.BackendResult completeThroughputOnly(String label, int port, long errors) {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult(label, port);
        result.setThroughput = throughput(YierdisBench.Workload.SET_RANDOM, errors);
        result.getThroughput = throughput(YierdisBench.Workload.GET_RANDOM, 0);
        result.appendThroughput = throughput(YierdisBench.Workload.APPEND, 0);
        result.pfaddSparseThroughput = throughput(YierdisBench.Workload.PFADD_SPARSE, 0);
        result.pfaddDenseThroughput = throughput(YierdisBench.Workload.PFADD_DENSE, 0);
        result.pfcountThroughput = throughput(YierdisBench.Workload.PFCOUNT, 0);
        return result;
    }

    private static YierdisBench.BackendResult completeWithLatency(String label, int port, long latencyErrors) {
        YierdisBench.BackendResult result = completeThroughputOnly(label, port, 0);
        result.pingLatency = latency(YierdisBench.Workload.PING, latencyErrors);
        result.setLatency = latency(YierdisBench.Workload.SET_RANDOM, 0);
        result.getLatency = latency(YierdisBench.Workload.GET_RANDOM, 0);
        result.appendLatency = latency(YierdisBench.Workload.APPEND, 0);
        result.pfaddSparseLatency = latency(YierdisBench.Workload.PFADD_SPARSE, 0);
        result.pfaddDenseLatency = latency(YierdisBench.Workload.PFADD_DENSE, 0);
        return result;
    }

    private static YierdisBench.ThroughputResult throughput(YierdisBench.Workload workload, long errors) {
        return new YierdisBench.ThroughputResult(
                workload, 100, errors, 1.0, 100.0, java.time.Instant.parse("2026-05-16T00:00:00Z")
        );
    }

    private static YierdisBench.LatencyResult latency(YierdisBench.Workload workload, long errors) {
        return new YierdisBench.LatencyResult(
                workload,
                100,
                errors,
                1.0,
                100.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );
    }

    private static void assertArgValue(List<String> argv, String flag, String expectedValue) {
        int index = argv.indexOf(flag);
        Assert.assertTrue("missing flag: " + flag, index >= 0);
        Assert.assertTrue("missing value after flag: " + flag, index + 1 < argv.size());
        Assert.assertEquals(expectedValue, argv.get(index + 1));
    }
}
```

- [ ] **Step 2: Run tests and verify they fail for missing side-context and side-validation helpers**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonExecutionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL with compile errors for `ComparisonSideContext`, `comparisonSideContext`, `comparisonSideHasAnyMeasurements`, `comparisonSideHasRequiredMeasurements`, `comparisonSideHasBenchmarkErrors`, or `comparisonSideCanBeCompared`.

- [ ] **Step 3: Add comparison side context helper**

Inside `YierdisBench`, near `ComparisonSideResult`, add:

```java
static final class ComparisonSideContext {
    final String label;
    final Path jarPath;
    final int port;
    final Path logFile;
    final YierdisBenchServerArgs serverArgs;
    final List<String> commandLine;

    ComparisonSideContext(
            String label,
            Path jarPath,
            int port,
            Path logFile,
            YierdisBenchServerArgs serverArgs,
            List<String> commandLine
    ) {
        this.label = Objects.requireNonNull(label, "label");
        this.jarPath = Objects.requireNonNull(jarPath, "jarPath");
        this.port = port;
        this.logFile = Objects.requireNonNull(logFile, "logFile");
        this.serverArgs = Objects.requireNonNull(serverArgs, "serverArgs");
        this.commandLine = List.copyOf(commandLine);
    }
}
```

Add this static helper:

```java
static ComparisonSideContext comparisonSideContext(
        BenchConfig config,
        String label,
        Path jarPath,
        int sideIndex,
        Path runDir
) {
    YierdisBenchServerArgs serverArgsForRun = config.baseServerArgs.copy();
    serverArgsForRun.port = config.portBase + sideIndex;
    serverArgsForRun.normalizeAndValidate();
    Path logFile = runDir.resolve("server-" + label + ".log");
    ServerProcess server = new ServerProcess(
            config.javaCmd,
            jarPath,
            config.serverXms,
            config.serverXmx,
            config.serverMaxDirectMemory,
            serverArgsForRun,
            logFile
    );
    return new ComparisonSideContext(
            label,
            jarPath,
            serverArgsForRun.port,
            logFile,
            serverArgsForRun,
            server.commandLine()
    );
}
```

- [ ] **Step 4: Extract main RESP workload execution into a reusable helper**

Move the existing workload body from the single-run loop into this method. Keep the same order and assignments:

```java
private static void runMainRespWorkloads(BenchConfig config, int port, BackendResult backendResult) throws Exception {
    if (!config.skipPrefill) {
        println("");
        println("[1/3] 预置数据（SET keyspace=" + config.keyspace + "，dataSize=" + config.dataSize + "，pipeline=" + config.pipeline + "）");
        ThroughputResult prefill = runThroughput(
                config.host,
                port,
                Workload.SET_SEQUENTIAL,
                config.keyspace,
                config.clients,
                config.pipeline,
                config.keyspace,
                config.dataSize,
                config.strictReplies
        );
        println("预置完成: " + prefill);
    }

    println("");
    println("[2/3] 吞吐压测（requests=" + config.requests + "，clients=" + config.clients + "，pipeline=" + config.pipeline + "）");
    ThroughputResult setQps = runThroughput(
            config.host, port, Workload.SET_RANDOM, config.requests, config.clients, config.pipeline,
            config.keyspace, config.dataSize, config.strictReplies
    );
    ThroughputResult getQps = runThroughput(
            config.host, port, Workload.GET_RANDOM, config.requests, config.clients, config.pipeline,
            config.keyspace, config.dataSize, config.strictReplies
    );
    backendResult.setThroughput = setQps;
    backendResult.getThroughput = getQps;
    println("SET: " + setQps);
    println("GET: " + getQps);

    if (!config.skipLatency) {
        println("");
        println("[3/3] 延迟压测（pipeline=1，requests=" + config.latencyRequests + "，clients=" + config.latencyClients + "）");
        LatencyResult pingLat = runLatency(config.host, port, Workload.PING, config.latencyRequests, config.latencyClients, config.keyspace, config.dataSize, config.strictReplies);
        LatencyResult setLat = runLatency(config.host, port, Workload.SET_RANDOM, config.latencyRequests, config.latencyClients, config.keyspace, config.dataSize, config.strictReplies);
        LatencyResult getLat = runLatency(config.host, port, Workload.GET_RANDOM, config.latencyRequests, config.latencyClients, config.keyspace, config.dataSize, config.strictReplies);
        backendResult.pingLatency = pingLat;
        backendResult.setLatency = setLat;
        backendResult.getLatency = getLat;
        println("PING: " + pingLat);
        println("SET : " + setLat);
        println("GET : " + getLat);
    }

    println("");
    println("[APPEND] 追加写入压测");
    if (!config.skipLatency) {
        LatencyResult appendLat = runLatency(config.host, port, Workload.APPEND, config.latencyRequests, config.latencyClients, config.keyspace, config.dataSize, config.strictReplies);
        backendResult.appendLatency = appendLat;
        println("APPEND: " + appendLat);
    }
    ThroughputResult appendQps = runThroughput(
            config.host, port, Workload.APPEND, config.requests, config.clients, config.pipeline,
            config.keyspace, config.dataSize, config.strictReplies
    );
    backendResult.appendThroughput = appendQps;
    println("APPEND throughput: " + appendQps);

    println("");
    println("[HLL] PFADD/PFCOUNT sparse/dense 压测");
    int hllDenseKeyspace = Math.max(1, Math.min(config.keyspace, Math.min(config.requests, 4096)));
    ThroughputResult pfaddSparseQps = runThroughput(
            config.host, port, Workload.PFADD_SPARSE, config.requests, config.clients, config.pipeline,
            config.keyspace, config.dataSize, config.strictReplies
    );
    prefillDenseHll(config.host, port, hllDenseKeyspace, config.pipeline);
    ThroughputResult pfaddDenseQps = runThroughput(
            config.host, port, Workload.PFADD_DENSE, config.requests, config.clients, config.pipeline,
            hllDenseKeyspace, config.dataSize, config.strictReplies
    );
    ThroughputResult pfcountQps = runThroughput(
            config.host, port, Workload.PFCOUNT, config.requests, config.clients, config.pipeline,
            hllDenseKeyspace, config.dataSize, config.strictReplies
    );
    backendResult.pfaddSparseThroughput = pfaddSparseQps;
    backendResult.pfaddDenseThroughput = pfaddDenseQps;
    backendResult.pfcountThroughput = pfcountQps;
    println("PFADD sparse throughput: " + pfaddSparseQps);
    println("PFADD dense throughput : " + pfaddDenseQps);
    println("PFCOUNT throughput     : " + pfcountQps);
    if (!config.skipLatency) {
        LatencyResult pfaddSparseLat = runLatency(config.host, port, Workload.PFADD_SPARSE, config.latencyRequests, config.latencyClients, config.keyspace, config.dataSize, config.strictReplies);
        LatencyResult pfaddDenseLat = runLatency(config.host, port, Workload.PFADD_DENSE, config.latencyRequests, config.latencyClients, hllDenseKeyspace, config.dataSize, config.strictReplies);
        backendResult.pfaddSparseLatency = pfaddSparseLat;
        backendResult.pfaddDenseLatency = pfaddDenseLat;
        println("PFADD sparse latency   : " + pfaddSparseLat);
        println("PFADD dense latency    : " + pfaddDenseLat);
    }
}
```

In the existing single-run loop, replace only the moved workload body with:

```java
runMainRespWorkloads(config, port, backendResult);
```

- [ ] **Step 5: Add measurement, error, and failure message helpers**

Add:

```java
static boolean comparisonSideHasRequiredMeasurements(BackendResult result, boolean skipLatency) {
    if (result == null) {
        return false;
    }
    if (result.setThroughput == null
            || result.getThroughput == null
            || result.appendThroughput == null
            || result.pfaddSparseThroughput == null
            || result.pfaddDenseThroughput == null
            || result.pfcountThroughput == null) {
        return false;
    }
    if (skipLatency) {
        return true;
    }
    return result.pingLatency != null
            && result.setLatency != null
            && result.getLatency != null
            && result.appendLatency != null
            && result.pfaddSparseLatency != null
            && result.pfaddDenseLatency != null;
}

static boolean comparisonSideHasBenchmarkErrors(BackendResult result, boolean skipLatency) {
    if (result == null) {
        return true;
    }
    return (result.setThroughput != null && result.setThroughput.errors > 0)
            || (result.getThroughput != null && result.getThroughput.errors > 0)
            || (result.appendThroughput != null && result.appendThroughput.errors > 0)
            || (result.pfaddSparseThroughput != null && result.pfaddSparseThroughput.errors > 0)
            || (result.pfaddDenseThroughput != null && result.pfaddDenseThroughput.errors > 0)
            || (result.pfcountThroughput != null && result.pfcountThroughput.errors > 0)
            || (!skipLatency && result.pingLatency != null && result.pingLatency.errors > 0)
            || (!skipLatency && result.setLatency != null && result.setLatency.errors > 0)
            || (!skipLatency && result.getLatency != null && result.getLatency.errors > 0)
            || (!skipLatency && result.appendLatency != null && result.appendLatency.errors > 0)
            || (!skipLatency && result.pfaddSparseLatency != null && result.pfaddSparseLatency.errors > 0)
            || (!skipLatency && result.pfaddDenseLatency != null && result.pfaddDenseLatency.errors > 0);
}

static boolean comparisonSideCanBeCompared(BackendResult result, boolean skipLatency) {
    return comparisonSideHasRequiredMeasurements(result, skipLatency)
            && !comparisonSideHasBenchmarkErrors(result, skipLatency);
}

static boolean comparisonSideHasAnyMeasurements(BackendResult result) {
    return result.setThroughput != null
            || result.getThroughput != null
            || result.appendThroughput != null
            || result.pfaddSparseThroughput != null
            || result.pfaddDenseThroughput != null
            || result.pfcountThroughput != null
            || result.pingLatency != null
            || result.setLatency != null
            || result.getLatency != null
            || result.appendLatency != null
            || result.pfaddSparseLatency != null
            || result.pfaddDenseLatency != null;
}

private static String failureSummary(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) {
        root = root.getCause();
    }
    String message = root.getMessage();
    if (message == null || message.isBlank()) {
        message = root.getClass().getSimpleName();
    }
    return message;
}
```

- [ ] **Step 6: Add comparison execution helpers**

Add:

```java
private static ComparisonResult runComparison(BenchConfig config, Path runDir) {
    ComparisonSideResult baseline = runComparisonSide(
            config,
            comparisonSideContext(config, "baseline", config.baselineServerJar, 0, runDir),
            "unknown"
    );
    ComparisonSideResult current = runComparisonSide(
            config,
            comparisonSideContext(config, "current", config.currentServerJar, 1, runDir),
            "unknown"
    );
    String caveat = "commit labels unavailable for supplied artifacts unless the operator ties the jar paths to commits externally";
    if (!baseline.comparable() || !current.comparable()) {
        caveat = "comparison is environment-limited because at least one side failed or was only partially measured";
    }
    return new ComparisonResult(baseline, current, config.skipLatency, caveat);
}

private static ComparisonSideResult runComparisonSide(
        BenchConfig config,
        ComparisonSideContext context,
        String commitLabel
) {
    BackendResult backendResult = new BackendResult(context.label, context.port);
    ServerProcess server = new ServerProcess(
            config.javaCmd,
            context.jarPath,
            config.serverXms,
            config.serverXmx,
            config.serverMaxDirectMemory,
            context.serverArgs,
            context.logFile
    );
    Instant startedAt = Instant.now();
    try {
        println("============================================================");
        println("comparison side: " + context.label + "  port=" + context.port);
        println("jar: " + context.jarPath);
        println("日志: " + context.logFile);
        server.start();
        if (!waitReady(config.host, context.port, READY_TIMEOUT_MILLIS)) {
            throw new IllegalStateException("服务未就绪，请检查日志: " + context.logFile);
        }
        println("服务就绪，启动耗时: " + Duration.between(startedAt, Instant.now()).toMillis() + " ms");
        runMainRespWorkloads(config, context.port, backendResult);
        if (!comparisonSideCanBeCompared(backendResult, config.skipLatency)) {
            return ComparisonSideResult.failure(
                    context.label,
                    context.jarPath,
                    context.commandLine,
                    commitLabel,
                    backendResult,
                    !comparisonSideHasRequiredMeasurements(backendResult, config.skipLatency),
                    comparisonSideHasBenchmarkErrors(backendResult, config.skipLatency)
                            ? "comparison side recorded protocol/reply errors"
                            : "comparison side is missing required measurements"
            );
        }
        return ComparisonSideResult.success(
                context.label,
                context.jarPath,
                context.commandLine,
                commitLabel,
                backendResult
        );
    } catch (Throwable failure) {
        return ComparisonSideResult.failure(
                context.label,
                context.jarPath,
                context.commandLine,
                commitLabel,
                backendResult,
                comparisonSideHasAnyMeasurements(backendResult),
                failureSummary(failure)
        );
    } finally {
        server.stop();
    }
}
```

- [ ] **Step 7: Add main comparison branch before the single-run path**

In `main`, after the native-eval branch and before `Path serverJar = null;`, insert:

```java
if (config.comparisonMode) {
    Path runDir = Files.createTempDirectory(Path.of(".").toAbsolutePath().normalize(), ".bench-java.");
    println("YierdisBench（baseline/current comparison）");
    println("运行目录: " + runDir);
    println("baselineServerJar: " + config.baselineServerJar);
    println("currentServerJar : " + config.currentServerJar);
    println("");
    printBudgetHint(config);
    println("");
    ComparisonResult comparison = runComparison(config, runDir);
    println("");
    println("============================================================");
    println("基线对比汇总（吞吐越大越好；延迟越小越好）");
    printComparison(comparison);
    println("");
    println("完成。");
    return;
}
```

Add print helper near `printSummary`:

```java
private static void printComparison(ComparisonResult result) {
    for (String line : renderComparison(result).split("\n", -1)) {
        if (!line.isEmpty()) {
            println(line);
        }
    }
}
```

- [ ] **Step 8: Run comparison execution tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=YierdisBenchComparisonExecutionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 9: Run the focused benchmark test set**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=BenchComparisonConfigTest,BenchServerArgsReuseTest,YierdisBenchComparisonRenderTest,YierdisBenchComparisonExecutionTest,YierdisBenchSummaryFormatTest,NativeEvalFormatTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 10: Commit Task 3**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java
git commit -m "feat: run benchmark jar comparisons"
```

## Task 4: Benchmark Report Update

**Files:**

- Modify: `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`

- [ ] **Step 1: Update the baseline note into explicit Track 1 comparison status**

Replace the existing `## Baseline note` section with:

````markdown
## Baseline/current comparison status

Track 1 adds an explicit jar-only comparison mode to `yierdis-benchmark`:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --comparisonMode \
  --baselineServerJar artifacts/baseline-79228e3/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --javaCmd /usr/lib/jvm/java-25-openjdk-amd64/bin/java \
  --xms 512m --xmx 512m --maxDirectMemory 1g \
  --portBase 17378 --keyspace 2000 --requests 4000 --clients 16 --pipeline 8 \
  --latencyRequests 1000 --latencyClients 8 --dataSize 128 --strictReplies \
  -- --maxmemoryBytes 134217728
```

The comparison report records:

- baseline/current jar paths
- attempted baseline/current launch commands
- side labels
- comparable deltas only when both sides completed cleanly
- `non-comparable` status when either side fails or is only partially measured
- an environment caveat when jar paths cannot be tied to exact commits

Current caveat: the pre-migration baseline server previously probed from commit `79228e3` returned `-ERR internal error` for minimal `SET`, `PFADD`, and `APPEND` probes in this environment. Until that baseline jar completes the same RESP workload shape cleanly, raw current-branch benchmark numbers in this report are not a trustworthy before/after comparison.
````

- [ ] **Step 2: Verify the report contains the required caveat language**

Run:

```bash
rg -n "comparisonMode|non-comparable|79228e3|not a trustworthy before/after comparison" docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md
```

Expected: PASS. The command prints lines for all four patterns.

- [ ] **Step 3: Commit Task 4**

```bash
git add docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md
git commit -m "docs: caveat benchmark baseline comparison"
```

## Task 5: Full Track 1 Verification

**Files:**

- Verify all changed Track 1 files.
- Do not modify `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`.
- Do not modify or add `yierdis.md`.

- [ ] **Step 1: Run full benchmark module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

Expected: PASS.

- [ ] **Step 2: Confirm source scope stayed inside Track 1**

Run:

```bash
git diff --stat HEAD~4..HEAD
```

Expected: output includes only:

```text
docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md
yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java
yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java
yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java
yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchServerArgsReuseTest.java
yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java
yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java
```

- [ ] **Step 3: Confirm protected files were not changed**

Run:

```bash
git status --short docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md yierdis.md
```

Expected: no output for the roadmap spec. If `yierdis.md` is untracked before execution, it may still show as untracked; do not stage, modify, or commit it.

- [ ] **Step 4: Confirm every Maven command in this plan uses the required JDK 25 prefix**

Run:

```bash
rg -n "^JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:\\$PATH mvn " docs/superpowers/plans/2026-05-16-db-native-allocator-benchmark-baseline.md
```

Expected: output includes every Maven command in the plan. There must be no Maven command unless it starts with:

```text
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn
```

- [ ] **Step 5: Final implementation handoff summary**

Report:

```text
Status: DONE
Tests: JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
Summary: comparison mode validates jar-only baseline/current inputs, renders provenance-rich comparable/non-comparable output, captures post-launch failures, and updates the benchmark report caveat.
Concerns: historical baseline may remain non-comparable if it still returns protocol/workload errors.
```

## Self-Review Checklist

- CLI/config validation covers required comparison jar flags, rejected `--serverJar`, rejected `--noStartServer`, missing paths, non-regular paths, and unchanged single-run behavior.
- Comparison mode forces `skipNativeDefragCompare` through `BenchConfig`.
- `ServerProcess.commandLine()` is the only command construction source for both process launch and provenance.
- Renderer output includes side labels, jar paths, attempted commands, commit labels, environment caveat, comparable deltas, and non-comparable failure status.
- Execution captures startup, readiness, protocol, workload, and partial-measurement failures into `ComparisonSideResult`.
- Report update states the historical baseline caveat and avoids presenting current raw numbers as a trustworthy before/after pair.
- Scope remains inside benchmark baseline repair; no allocator internals, collection internals, production hardening, or roadmap rewrite.
