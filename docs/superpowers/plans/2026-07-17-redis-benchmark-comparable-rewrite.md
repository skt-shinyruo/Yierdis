# Redis-Benchmark-Comparable Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `yierdis-benchmark` with a connect-only Java NIO benchmark that reproduces the official built-in `redis-benchmark` workloads and exposes directly comparable throughput and latency results.

**Architecture:** Build the replacement beside the old implementation in a focused `bench.redis` package, prove catalog, wire, parser, statistics, runner, and output semantics independently, then switch the jar entrypoint and delete every old benchmark mode. A single-selector NIO runner prepares one reusable pipeline buffer per client, records official batch latency semantics in HdrHistogram, and lets a catalog orchestrator classify unsupported and dependent cases.

**Tech Stack:** Java 25, Maven, picocli 4.7.6, Java NIO `Selector`/`SocketChannel`, existing `RespClientCodec`, HdrHistogram 2.2.2, JUnit 4.13.2, Bash.

---

## Source Contract And Global Constraints

- Approved design: `docs/superpowers/specs/2026-07-17-redis-benchmark-comparable-rewrite-design.md`.
- Official behavior reference: Redis `src/redis-benchmark.c` at commit `af293cf75bf88773f9c04e20276cff57cffa730a`.
- Use JDK 25 for every Maven, Java, packaging, and acceptance command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH
```

- Do not dispatch requests to Redis from production code.
- Do not invoke `redis-benchmark` from production code or required Maven tests.
- Do not preserve suite, baseline/current comparison, external Redis comparison, native allocator evaluation, child-server launch, or old CLI compatibility.
- Do not add hidden warmup, `FLUSHDB`, GET prefill, or unreported setup traffic.
- Keep the existing `yier.bubu.redis.app.bench.YierdisBench` main-class name for the shaded jar manifest.
- Preserve user changes outside the files listed in this plan.
- Implement every behavior test-first.

## Final File Structure

### Production files retained or created

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
  Thin main-class launcher only.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkFormat.java`
  Human, quiet, and CSV output modes.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkConfig.java`
  Validated immutable run configuration.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptions.java`
  Picocli-facing option model and config conversion.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCase.java`
  One official output case, support declaration, dependency, and reply policy.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkReplyExpectation.java`
  Minimum reply-shape validation for measured commands.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalog.java`
  Canonical 21-case order and selection semantics.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplate.java`
  Inline/RESP argument model and prepared pipeline creation.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/PreparedPipeline.java`
  Per-client reusable encoded pipeline and random digit offsets.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRandom.java`
  Seeded per-placeholder random source and 12-digit rendering.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkPayload.java`
  Official deterministic payload generator.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRespReply.java`
  Allocation-light reply shape used by the NIO path.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoder.java`
  Bounded incremental RESP2 decoder over `ByteBuffer`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkClock.java`
  Injectable monotonic time source.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkLatencyRecorder.java`
  HdrHistogram adapter with official bounds and percentile extraction.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkStatistics.java`
  Successful case metrics.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkStatus.java`
  `SUCCESS`, `UNSUPPORTED`, `SKIPPED`, and `FAILED`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseResult.java`
  Status-safe result for one official case.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRunResult.java`
  Ordered complete run and exit-status policy.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseExecutor.java`
  Testable boundary between catalog orchestration and network execution.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkExecutionException.java`
  Active-case failure carrying the validated measured-reply count.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkClient.java`
  Per-connection state, buffers, pending replies, and batch timestamp.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunner.java`
  Single-selector connection, scheduling, timing, and failure engine.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmark.java`
  Public catalog orchestration facade used by CLI and integration tests.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRenderer.java`
  Pure human, quiet, and CSV rendering.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommand.java`
  Picocli command that runs the facade and renders results.

### Test files created

- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptionsTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalogTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplateTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoderTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkLatencyRecorderTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseResultTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/ScriptedRespServer.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunnerTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRendererTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchScriptContractTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/benchmark/RedisBenchmarkCatalogCoverageGuardTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/benchmark/RedisBenchmarkRealServerTest.java`

### Existing files modified

- `pom.xml`
- `yierdis-benchmark/pom.xml`
- `yierdis-tests/yierdis-integration-tests/pom.xml`
- `scripts/bench.sh`
- `README.md`
- `docs/project-docs/client-and-bench-internals.md`
- `docs/project-docs/core-logic-index.md`
- `docs/project-docs/production-hardening-operations.md`
- `docs/project-docs/testing-and-debugging.md`
- the six superseded benchmark design/plan documents named by the approved design

### Existing benchmark code deleted during the entrypoint cutover

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadKind.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadRequest.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadResult.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchComparisonConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchScriptContractTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchServerArgsReuseTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/NativeEvalFormatTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/RespCommandWriterTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonRenderTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchServerArgsTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSuiteEntrypointTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSummaryFormatTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/`

---

### Task 1: Add The Replacement Configuration Boundary

**Files:**
- Modify: `pom.xml`
- Modify: `yierdis-benchmark/pom.xml`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkFormat.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkConfig.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptions.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkConfigTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptionsTest.java`

- [ ] **Step 1: Write failing config and option tests**

```java
@Test
public void defaultsMatchOfficialWorkloadAndYierdisEndpoint() {
    RedisBenchmarkOptions options = new RedisBenchmarkOptions();
    new CommandLine(options).parseArgs();

    BenchmarkConfig config = options.toConfig(() -> 123L);
    Assert.assertEquals("127.0.0.1", config.host());
    Assert.assertEquals(16378, config.port());
    Assert.assertEquals(100_000, config.requests());
    Assert.assertEquals(50, config.clients());
    Assert.assertEquals(3, config.dataSize());
    Assert.assertEquals(1, config.pipeline());
    Assert.assertTrue(config.keyspace().isEmpty());
    Assert.assertTrue(config.keepAlive());
    Assert.assertEquals(3, config.precision());
    Assert.assertEquals(BenchmarkFormat.HUMAN, config.format());
    Assert.assertEquals(123L, config.seed());
}

@Test
public void explicitZeroKeyspaceIsDifferentFromOmittedKeyspace() {
    RedisBenchmarkOptions options = new RedisBenchmarkOptions();
    new CommandLine(options).parseArgs("--keyspace", "0", "--tests", "SET,get", "--format", "csv");

    BenchmarkConfig config = options.toConfig(() -> 999L);
    Assert.assertEquals(0L, config.keyspace().orElseThrow());
    Assert.assertEquals(Set.of("set", "get"), config.tests());
    Assert.assertEquals(BenchmarkFormat.CSV, config.format());
}

@Test
public void invalidWorkloadBoundsAreRejected() {
    Assert.assertThrows(IllegalArgumentException.class, () ->
            new BenchmarkConfig("", 0, 0, 0, 0, 0, OptionalLong.empty(), true,
                    Set.of(), 5, 1L, BenchmarkFormat.HUMAN, "", "", -1));
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=BenchmarkConfigTest,RedisBenchmarkOptionsTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `BenchmarkConfig`, `BenchmarkFormat`, and `RedisBenchmarkOptions` do not exist.

- [ ] **Step 3: Add HdrHistogram dependency management and implement the option model**

Add to root properties and dependency management:

```xml
<hdrhistogram.version>2.2.2</hdrhistogram.version>

<dependency>
    <groupId>org.hdrhistogram</groupId>
    <artifactId>HdrHistogram</artifactId>
    <version>${hdrhistogram.version}</version>
</dependency>
```

Add this dependency to `yierdis-benchmark/pom.xml` without removing old dependencies yet:

```xml
<dependency>
    <groupId>org.hdrhistogram</groupId>
    <artifactId>HdrHistogram</artifactId>
</dependency>
```

Implement the config contract with these exact fields and validation:

```java
public record BenchmarkConfig(
        String host,
        int port,
        int requests,
        int clients,
        int dataSize,
        int pipeline,
        OptionalLong keyspace,
        boolean keepAlive,
        Set<String> tests,
        int precision,
        long seed,
        BenchmarkFormat format,
        String username,
        String password,
        int database
) {
    public BenchmarkConfig {
        host = Objects.requireNonNull(host, "host").trim();
        keyspace = Objects.requireNonNull(keyspace, "keyspace");
        format = Objects.requireNonNull(format, "format");
        username = username == null ? "" : username;
        password = password == null ? "" : password;
        tests = tests == null ? Set.of() : tests.stream()
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        if (host.isEmpty()) throw new IllegalArgumentException("host must not be blank");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port must be in range 1..65535");
        if (requests <= 0) throw new IllegalArgumentException("requests must be > 0");
        if (clients <= 0) throw new IllegalArgumentException("clients must be > 0");
        if (dataSize < 1 || dataSize > 1024 * 1024 * 1024) throw new IllegalArgumentException("dataSize out of range");
        if (pipeline <= 0) throw new IllegalArgumentException("pipeline must be > 0");
        if (keyspace.isPresent() && keyspace.getAsLong() < 0) throw new IllegalArgumentException("keyspace must be >= 0");
        if (precision < 0 || precision > 4) throw new IllegalArgumentException("precision must be in range 0..4");
        if (database < 0) throw new IllegalArgumentException("database must be >= 0");
        if (!username.isEmpty() && password.isEmpty()) throw new IllegalArgumentException("username requires password");
    }
}
```

`RedisBenchmarkOptions` must expose the approved long option names, keep `Long keyspace` nullable so omission is distinguishable from explicit zero, normalize comma-separated tests, and use a package-visible `toConfig(LongSupplier seedSupplier)` for deterministic tests. `BenchmarkFormat.parse(String)` must accept case-insensitive `human`, `quiet`, and `csv` and reject anything else.

- [ ] **Step 4: Run the focused tests and verify they pass**

Run the Step 2 command again.

Expected: PASS.

- [ ] **Step 5: Commit the configuration boundary**

```bash
git add pom.xml yierdis-benchmark/pom.xml \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkConfigTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkOptionsTest.java
git commit -m "feat: define redis-comparable benchmark configuration"
```

---

### Task 2: Define The Complete Official Case Catalog

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkReplyExpectation.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCase.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplate.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalog.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalogTest.java`

- [ ] **Step 1: Write the failing 21-case catalog golden test**

```java
@Test
public void catalogMatchesOfficialBuiltInOrderAndSupport() {
    List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().allCases();
    Assert.assertEquals(List.of(
            "PING_INLINE", "PING_MBULK", "SET", "GET", "INCR", "LPUSH", "RPUSH",
            "LPOP", "RPOP", "SADD", "HSET", "SPOP", "ZADD", "ZPOPMIN",
            "LPUSH (needed to benchmark LRANGE)",
            "LRANGE_100 (first 100 elements)",
            "LRANGE_300 (first 300 elements)",
            "LRANGE_500 (first 500 elements)",
            "LRANGE_600 (first 600 elements)",
            "MSET (10 keys)", "XADD"
    ), cases.stream().map(RedisBenchmarkCase::title).toList());

    Assert.assertEquals(List.of("spop", "zpopmin", "mset", "xadd"), cases.stream()
            .filter(testCase -> !testCase.support().supported())
            .map(RedisBenchmarkCase::id)
            .toList());
    Assert.assertEquals(17, cases.stream().filter(testCase -> testCase.support().supported()).count());
}

@Test
public void selectionAliasesMatchOfficialBehavior() {
    RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
    Assert.assertEquals(List.of("PING_INLINE", "PING_MBULK"), catalog.select(Set.of("ping")).stream()
            .map(RedisBenchmarkCase::title).toList());
    Assert.assertEquals(List.of(
            "LPUSH (needed to benchmark LRANGE)",
            "LRANGE_300 (first 300 elements)"
    ), catalog.select(Set.of("lrange_300")).stream().map(RedisBenchmarkCase::title).toList());
    Assert.assertEquals(List.of(
            "LPUSH (needed to benchmark LRANGE)",
            "LRANGE_100 (first 100 elements)",
            "LRANGE_300 (first 300 elements)",
            "LRANGE_500 (first 500 elements)",
            "LRANGE_600 (first 600 elements)"
    ), catalog.select(Set.of("lrange")).stream().map(RedisBenchmarkCase::title).toList());
    Assert.assertThrows(IllegalArgumentException.class, () -> catalog.select(Set.of("no_such_test")));
}
```

- [ ] **Step 2: Run the catalog test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=RedisBenchmarkCatalogTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the catalog and case model do not exist.

- [ ] **Step 3: Implement the immutable case and template declaration model**

Use this case contract:

```java
public record RedisBenchmarkCase(
        String id,
        String title,
        Set<String> selectionTriggers,
        RedisBenchmarkCommandTemplate template,
        Set<String> requiredCommands,
        BenchmarkReplyExpectation replyExpectation,
        Support support,
        String dependencyId
) {
    public record Support(boolean supported, String reason) {
        public static Support supported() { return new Support(true, ""); }
        public static Support unsupported(String reason) { return new Support(false, reason); }
    }
}
```

`RedisBenchmarkCommandTemplate` must initially model declarations only:

```java
public final class RedisBenchmarkCommandTemplate {
    public enum WireMode { INLINE, RESP }
    public enum ArgumentKind { LITERAL, PAYLOAD, RANDOM_SCORE }

    public record Argument(ArgumentKind kind, byte[] literal) {
        public Argument {
            kind = Objects.requireNonNull(kind, "kind");
            literal = Objects.requireNonNull(literal, "literal").clone();
        }

        @Override
        public byte[] literal() {
            return literal.clone();
        }

        public static Argument literal(String value) {
            return new Argument(ArgumentKind.LITERAL,
                    value.getBytes(StandardCharsets.US_ASCII));
        }

        public static Argument payload() {
            return new Argument(ArgumentKind.PAYLOAD, new byte[0]);
        }

        public static Argument randomScore() {
            return new Argument(ArgumentKind.RANDOM_SCORE, new byte[0]);
        }
    }

    private final WireMode wireMode;
    private final byte[] inlineFrame;
    private final List<Argument> arguments;

    private RedisBenchmarkCommandTemplate(
            WireMode wireMode,
            byte[] inlineFrame,
            List<Argument> arguments
    ) {
        this.wireMode = wireMode;
        this.inlineFrame = inlineFrame.clone();
        this.arguments = List.copyOf(arguments);
    }

    public static RedisBenchmarkCommandTemplate inline(String frame) {
        byte[] bytes = frame.getBytes(StandardCharsets.US_ASCII);
        if (!frame.endsWith("\r\n")) {
            throw new IllegalArgumentException("inline frame must end with CRLF");
        }
        return new RedisBenchmarkCommandTemplate(WireMode.INLINE, bytes, List.of());
    }

    public static RedisBenchmarkCommandTemplate resp(Argument... arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("RESP template requires a command");
        }
        return new RedisBenchmarkCommandTemplate(
                WireMode.RESP,
                new byte[0],
                List.of(arguments.clone())
        );
    }
}
```

Populate `RedisBenchmarkCatalog` with all 21 cases. Use these exact unsupported reasons:

```text
Yierdis does not support SPOP
Yierdis does not support ZPOPMIN
Yierdis does not support MSET
Yierdis does not support XADD
```

Use `lrange_setup` as the dependency id for all four LRANGE result cases. The setup trigger set is `lrange,lrange_100,lrange_300,lrange_500,lrange_600`; each result case has `{lrange, lrange_N}` as its trigger set. Ordinary non-LRANGE cases use their official lower-case selector, while ping cases also accept `ping`.

- [ ] **Step 4: Run the catalog test and verify it passes**

Run the Step 2 command again.

Expected: PASS with exactly 21 ordered cases, 17 supported and 4 unsupported.

- [ ] **Step 5: Commit the catalog**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCatalogTest.java
git commit -m "feat: add official redis benchmark case catalog"
```

---

### Task 3: Encode Reusable Official Pipeline Templates

**Files:**
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplate.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/PreparedPipeline.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRandom.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkPayload.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplateTest.java`

- [ ] **Step 1: Write failing exact-wire and randomization tests**

```java
@Test
public void inlinePingUsesOfficialBytes() {
    RedisBenchmarkCase ping = catalog.caseById("ping_inline");
    PreparedPipeline pipeline = ping.template().prepare(1, new byte[]{'x'}, OptionalLong.empty());
    Assert.assertArrayEquals("PING\r\n".getBytes(StandardCharsets.US_ASCII), pipeline.bytesForWrite(new BenchmarkRandom(1L)));
}

@Test
public void omittedKeyspaceKeepsLiteralPlaceholder() {
    RedisBenchmarkCase set = catalog.caseById("set");
    PreparedPipeline pipeline = set.template().prepare(1, new byte[]{'a', 'b', 'c'}, OptionalLong.empty());
    String wire = new String(pipeline.bytesForWrite(new BenchmarkRandom(1L)), StandardCharsets.US_ASCII);
    Assert.assertTrue(wire.contains("key:__rand_int__"));
}

@Test
public void explicitZeroAndPositiveKeyspaceRenderTwelveDigitsPerOccurrence() {
    RedisBenchmarkCommandTemplate mset = catalog.caseById("mset").template();
    PreparedPipeline zero = mset.prepare(1, new byte[]{'x'}, OptionalLong.of(0));
    String zeroWire = new String(zero.bytesForWrite(new BenchmarkRandom(1L)), StandardCharsets.US_ASCII);
    Assert.assertEquals(10, occurrences(zeroWire, "key:000000000000"));

    PreparedPipeline random = mset.prepare(2, new byte[]{'x'}, OptionalLong.of(10_000));
    String first = new String(random.bytesForWrite(new BenchmarkRandom(7L)), StandardCharsets.US_ASCII);
    Assert.assertFalse(first.contains("__rand_int__"));
    Assert.assertEquals(20, occurrences(first, "key:"));
    Matcher matcher = Pattern.compile("key:(\\d{12})").matcher(first);
    Set<String> renderedKeys = new HashSet<>();
    while (matcher.find()) renderedKeys.add(matcher.group(1));
    Assert.assertTrue("each marker must consume the random stream", renderedKeys.size() > 1);
}

@Test
public void randomizedZaddUsesIndependentTwelveDigitScoreAndMember() {
    PreparedPipeline zadd = catalog.caseById("zadd").template()
            .prepare(1, new byte[]{'x'}, OptionalLong.of(1_000_000));
    String wire = new String(zadd.bytesForWrite(new BenchmarkRandom(11L)), StandardCharsets.US_ASCII);
    Matcher matcher = Pattern.compile("(?:\\r\\n|element:)(\\d{12})\\r\\n").matcher(wire);
    List<String> randomArguments = new ArrayList<>();
    while (matcher.find()) randomArguments.add(matcher.group(1));
    Assert.assertEquals(2, randomArguments.size());
    Assert.assertNotEquals(randomArguments.get(0), randomArguments.get(1));
}

@Test
public void officialPayloadGeneratorIsDeterministic() {
    Assert.assertArrayEquals(BenchmarkPayload.generate(32), BenchmarkPayload.generate(32));
    Assert.assertEquals(3, BenchmarkPayload.generate(3).length);
}

@Test
public void fixedModeWireMatchesEveryOfficialRespTemplate() {
    assertFixedResp("ping_mbulk", "PING");
    assertFixedResp("set", "SET", "key:__rand_int__", "abc");
    assertFixedResp("get", "GET", "key:__rand_int__");
    assertFixedResp("incr", "INCR", "counter:__rand_int__");
    assertFixedResp("lpush", "LPUSH", "mylist", "abc");
    assertFixedResp("rpush", "RPUSH", "mylist", "abc");
    assertFixedResp("lpop", "LPOP", "mylist");
    assertFixedResp("rpop", "RPOP", "mylist");
    assertFixedResp("sadd", "SADD", "myset", "element:__rand_int__");
    assertFixedResp("hset", "HSET", "myhash", "element:__rand_int__", "abc");
    assertFixedResp("spop", "SPOP", "myset");
    assertFixedResp("zadd", "ZADD", "myzset", "0", "element:__rand_int__");
    assertFixedResp("zpopmin", "ZPOPMIN", "myzset");
    assertFixedResp("lrange_setup", "LPUSH", "mylist", "abc");
    assertFixedResp("lrange_100", "LRANGE", "mylist", "0", "99");
    assertFixedResp("lrange_300", "LRANGE", "mylist", "0", "299");
    assertFixedResp("lrange_500", "LRANGE", "mylist", "0", "499");
    assertFixedResp("lrange_600", "LRANGE", "mylist", "0", "599");
    assertFixedResp("mset", msetArguments());
    assertFixedResp("xadd", "XADD", "mystream", "*", "myfield", "abc");
}

private void assertFixedResp(String id, String... arguments) {
    byte[] expected = RespClientCodec.encodeCommand(Arrays.stream(arguments)
            .map(value -> value.getBytes(StandardCharsets.US_ASCII))
            .toList());
    PreparedPipeline prepared = catalog.caseById(id).template()
            .prepare(1, "abc".getBytes(StandardCharsets.US_ASCII), OptionalLong.empty());
    Assert.assertArrayEquals(id, expected, prepared.bytesForWrite(new BenchmarkRandom(1L)));
}

private static String[] msetArguments() {
    String[] arguments = new String[21];
    arguments[0] = "MSET";
    for (int pair = 0; pair < 10; pair++) {
        arguments[pair * 2 + 1] = "key:__rand_int__";
        arguments[pair * 2 + 2] = "abc";
    }
    return arguments;
}
```

- [ ] **Step 2: Run the template test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=RedisBenchmarkCommandTemplateTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because preparation, randomization, and payload generation are absent.

- [ ] **Step 3: Implement prepared pipelines without timed-path frame allocation**

`prepare(...)` must encode `pipeline` complete copies up front with `RespClientCodec.encodeCommand(...)`. In random mode it replaces every 12-byte `__rand_int__` marker in the prepared byte array with zero digits and records each marker offset. In fixed mode it records no offsets and leaves marker text unchanged. `RANDOM_SCORE` encodes ASCII `0` in fixed mode and a random marker in random mode.

`PreparedPipeline` is package-private. It owns a mutable byte array and immutable offset array; its mutable write bytes are visible only to the runner and same-package tests:

```java
final class PreparedPipeline {
    private final byte[] bytes;
    private final int[] randomOffsets;
    private final OptionalLong keyspace;

    byte[] bytesForWrite(BenchmarkRandom random) {
        for (int offset : randomOffsets) {
            long bound = keyspace.orElseThrow();
            long value = bound == 0 ? 0 : random.nextLong(bound);
            BenchmarkRandom.writeTwelveDigits(bytes, offset, value);
        }
        return bytes;
    }

    PreparedPipeline copyForClient() {
        return new PreparedPipeline(bytes.clone(), randomOffsets.clone(), keyspace);
    }
}
```

Use the official payload recurrence, restarting at `1234` for each catalog pass:

```java
int state = 1234;
for (int index = 0; index < size; index++) {
    state = state * 1103515245 + 12345;
    data[index] = (byte) ('0' + ((state >>> 16) & 63));
}
```

Do not expose the internal mutable pipeline array through a public API. The runner wraps each client's owned copy in a writable `ByteBuffer`; tests stay in the same package and may call `bytesForWrite` directly.

- [ ] **Step 4: Run catalog and template tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkCommandTemplateTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit template encoding**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTemplateTest.java
git commit -m "feat: encode reusable redis benchmark pipelines"
```

---

### Task 4: Add A Bounded Incremental RESP Reply Decoder

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRespReply.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoder.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoderTest.java`

- [ ] **Step 1: Write failing fragmentation, coalescing, and limit tests**

```java
@Test
public void fragmentedBulkReplyDoesNotAdvanceUntilComplete() throws Exception {
    IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(1024, 1024, 1024, 32);
    ByteBuffer input = ByteBuffer.allocate(64);
    input.put("$3\r\nab".getBytes(StandardCharsets.US_ASCII)).flip();
    int start = input.position();
    Assert.assertNull(decoder.tryDecode(input));
    Assert.assertEquals(start, input.position());

    input.compact().put("c\r\n".getBytes(StandardCharsets.US_ASCII)).flip();
    BenchmarkRespReply reply = decoder.tryDecode(input);
    Assert.assertEquals(BenchmarkRespReply.Kind.BULK_STRING, reply.kind());
    Assert.assertEquals(3, reply.bulkLength());
}

@Test
public void coalescedRepliesAreDecodedOneAtATime() throws Exception {
    ByteBuffer input = ByteBuffer.wrap("+PONG\r\n:1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n".getBytes(StandardCharsets.US_ASCII));
    IncrementalRespReplyDecoder decoder = defaults();
    Assert.assertEquals("PONG", decoder.tryDecode(input).text());
    Assert.assertEquals(1L, decoder.tryDecode(input).integer().longValue());
    Assert.assertEquals(2, decoder.tryDecode(input).arrayLength());
    Assert.assertFalse(input.hasRemaining());
}

@Test
public void oversizedBulkAndMalformedCrlfFail() {
    IncrementalRespReplyDecoder decoder = new IncrementalRespReplyDecoder(3, 10, 10, 4);
    Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("$4\r\ntest\r\n")));
    Assert.assertThrows(IOException.class, () -> decoder.tryDecode(ascii("+OK\n")));
}

@Test
public void resp2NullBulkAndNullArrayRemainDistinct() throws Exception {
    IncrementalRespReplyDecoder decoder = defaults();
    Assert.assertEquals(BenchmarkRespReply.Kind.NULL_BULK,
            decoder.tryDecode(ascii("$-1\r\n")).kind());
    Assert.assertEquals(BenchmarkRespReply.Kind.NULL_ARRAY,
            decoder.tryDecode(ascii("*-1\r\n")).kind());
}
```

- [ ] **Step 2: Run the decoder test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=IncrementalRespReplyDecoderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the incremental decoder does not exist.

- [ ] **Step 3: Implement transactional ByteBuffer parsing**

Use a recursive parser with a private `NeedMoreData` sentinel. `tryDecode` must restore the starting position only for incomplete input and must throw `IOException` for invalid input:

```java
public BenchmarkRespReply tryDecode(ByteBuffer input) throws IOException {
    int start = input.position();
    try {
        return decode(input, 0);
    } catch (NeedMoreData incomplete) {
        input.position(start);
        return null;
    }
}

private BenchmarkRespReply decode(ByteBuffer input, int depth) throws IOException, NeedMoreData {
    if (depth > maxDepth) throw new IOException("RESP nesting exceeds limit");
    byte marker = requireByte(input);
    return switch (marker) {
        case '+' -> BenchmarkRespReply.simple(readLine(input));
        case '-' -> BenchmarkRespReply.error(readLine(input));
        case ':' -> BenchmarkRespReply.integer(parseLong(readLine(input), "integer"));
        case '$' -> decodeBulk(input);
        case '*' -> decodeArray(input, depth + 1);
        default -> throw new IOException("unexpected RESP reply type: " + (char) marker);
    };
}
```

The constructor arguments are `(maxBulkBytes, maxLineBytes, maxArrayLength, maxDepth)`. `BenchmarkRespReply.Kind` keeps `NULL_BULK` and `NULL_ARRAY` distinct. For bulk replies, map `$-1` to `NULL_BULK`, validate any non-negative length before skipping payload and CRLF, and reject other negative lengths. For arrays, map `*-1` to `NULL_ARRAY`, validate any non-negative count, recursively consume every child, and retain only the top-level element count. Preserve simple/error text because PONG, OK, and error reasons require it. Reject RESP3 markers, including `_`, because this replacement is RESP2-only. Use `RespProtocolLimits.DEFAULT_MAX_BULK_BYTES`, `DEFAULT_MAX_ARGS`, and `DEFAULT_MAX_INLINE_BYTES` in production; grow the compacting client read buffer only up to the validated reply bound when a fragmented bulk or array cannot yet fit.

- [ ] **Step 4: Run the decoder test and verify it passes**

Run the Step 2 command again.

Expected: PASS.

- [ ] **Step 5: Commit the decoder**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRespReply.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoder.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/IncrementalRespReplyDecoderTest.java
git commit -m "feat: decode benchmark replies incrementally"
```

---

### Task 5: Implement Official-Style Statistics And Result Invariants

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkClock.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkLatencyRecorder.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkStatistics.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkStatus.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseResult.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkRunResult.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkLatencyRecorderTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseResultTest.java`

- [ ] **Step 1: Write failing histogram and non-success invariant tests**

```java
@Test
public void histogramClampsAtThreeSecondsAndProducesSummary() {
    BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(3);
    recorder.recordMicros(100);
    recorder.recordMicros(200);
    recorder.recordMicros(4_000_000);

    BenchmarkLatencyRecorder.Summary summary = recorder.summary();
    Assert.assertEquals(3, summary.count());
    Assert.assertEquals(3_000_319L, summary.maxMicros());
    Assert.assertTrue(summary.p50Micros() >= 100);
    Assert.assertTrue(summary.p99Micros() >= summary.p50Micros());
}

@Test
public void successfulStatisticsUseStopBoundaryCompletedRequestsForRps() {
    BenchmarkStatistics statistics = BenchmarkStatistics.from(100, 102, 104, 100, 40L,
            new BenchmarkLatencyRecorder.Summary(100, 200.0, 100, 200, 300, 400, 500));
    Assert.assertEquals(2550.0, statistics.requestsPerSecond(), 0.001);
    Assert.assertEquals(102, statistics.completedRequests());
    Assert.assertEquals(104, statistics.wireRequests());
}

@Test
public void unsupportedSkippedAndFailedResultsCannotCarryMetrics() {
    Assert.assertNull(BenchmarkCaseResult.unsupported(testCase, "missing").statistics());
    Assert.assertNull(BenchmarkCaseResult.skipped(testCase, "dependency").statistics());
    Assert.assertNull(BenchmarkCaseResult.failed(testCase, 7, "disconnect").statistics());
    Assert.assertThrows(IllegalArgumentException.class, () ->
            new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, null, "", 0));
}
```

- [ ] **Step 2: Run the statistics tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=BenchmarkLatencyRecorderTest,BenchmarkCaseResultTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the statistics and result types do not exist.

- [ ] **Step 3: Implement histogram bounds, clock, and result factories**

Use `org.HdrHistogram.Histogram` with lowest discernible value `10`, highest trackable value `3_000_000`, and configured significant digits. Clamp only recorded inputs above 3,000,000 microseconds and reject negative latency. Report the histogram's equivalent-value min/max directly; with three significant digits, a clamped 3,000,000 input has the same official upper equivalent value, 3,000,319 microseconds.

```java
public interface BenchmarkClock {
    long nanoTime();
    static BenchmarkClock system() { return System::nanoTime; }
}

public record BenchmarkStatistics(
        int requestedRequests,
        long completedRequests,
        long wireRequests,
        long histogramSamples,
        long elapsedMillis,
        double requestsPerSecond,
        BenchmarkLatencyRecorder.Summary latency
) {
    public static BenchmarkStatistics from(int requested, long completed,
                                           long wire, long samples,
                                           long elapsedMillis,
                                           BenchmarkLatencyRecorder.Summary latency) {
        if (elapsedMillis < 0) throw new IllegalArgumentException("elapsedMillis must be >= 0");
        if (completed < requested || completed > wire) {
            throw new IllegalArgumentException("completedRequests must be in requested..wire");
        }
        if (samples != requested || latency.count() != samples) {
            throw new IllegalArgumentException("successful histogram must contain requested samples");
        }
        double rps = elapsedMillis == 0 ? 0.0 : completed / (elapsedMillis / 1000.0);
        return new BenchmarkStatistics(
                requested, completed, wire, samples, elapsedMillis, rps, latency);
    }
}
```

`BenchmarkCaseResult` must require metrics only for `SUCCESS`, forbid metrics for other states, require a non-blank reason for non-success states, and retain completed replies for `FAILED`. `BenchmarkRunResult.exitCode()` returns `1` when any case failed and `0` otherwise.

- [ ] **Step 4: Run the statistics tests and verify they pass**

Run the Step 2 command again.

Expected: PASS.

- [ ] **Step 5: Commit statistics and result types**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkLatencyRecorderTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseResultTest.java
git commit -m "feat: record redis-style benchmark statistics"
```

---

### Task 6: Build The Single-Selector Happy-Path Runner

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkCaseExecutor.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkClient.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunner.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/ScriptedRespServer.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing request-budget and batch-latency tests**

```java
@Test
public void pipelineOneCompletesConfiguredRequestsAcrossClients() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.respondingWith("+PONG\r\n")) {
        BenchmarkStatistics statistics = runPing(server, 11, 3, 1, true);
        Assert.assertEquals(11, statistics.requestedRequests());
        Assert.assertEquals(11, statistics.completedRequests());
        Assert.assertEquals(11, statistics.wireRequests());
        Assert.assertEquals(11, statistics.histogramSamples());
        Assert.assertEquals(11, server.awaitCommands(11));
    }
}

@Test
public void finalPipelineIsFullButOnlyConfiguredSamplesAreRecorded() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.respondingWith("+PONG\r\n")) {
        BenchmarkStatistics statistics = runPing(server, 10, 1, 4, true);
        Assert.assertEquals(10, statistics.requestedRequests());
        Assert.assertEquals(12, statistics.completedRequests());
        Assert.assertEquals(12, statistics.wireRequests());
        Assert.assertEquals(10, statistics.histogramSamples());
        Assert.assertEquals(12, server.awaitCommands(12));
    }
}

@Test
public void everyReplyInOnePipelineUsesTheSameFirstReadLatency() throws Exception {
    ManualClock clock = new ManualClock();
    try (ScriptedRespServer server = ScriptedRespServer.respondingInOneWrite(
            "+PONG\r\n", 3, () -> clock.advanceMicros(250))) {
        BenchmarkStatistics statistics = runPing(server, 3, 1, 3, true, clock);
        Assert.assertEquals(250L, statistics.latency().minMicros());
        Assert.assertEquals(250L, statistics.latency().maxMicros());
    }
}

@Test
public void stopDrainsThresholdClientButDoesNotWaitForAnotherOutstandingClient() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.thresholdBoundaryScenario()) {
        BenchmarkStatistics statistics = runPing(server, 7, 3, 3, true);
        Assert.assertEquals(7, statistics.requestedRequests());
        Assert.assertEquals(7, statistics.completedRequests());
        Assert.assertEquals(9, statistics.wireRequests());
        Assert.assertEquals(7, server.awaitRepliesSent(7));
        Assert.assertFalse(server.stalledConnectionWasReleased());
    }
}
```

- [ ] **Step 2: Run the runner test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=NioBenchmarkRunnerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the runner and scripted server do not exist.

- [ ] **Step 3: Implement the scripted server and runner state machine**

`ScriptedRespServer` must bind port `0`, accept the configured number of clients, parse inline and RESP array requests, count complete commands, and support immediate, batched, fragmented, error, close, and coordinated threshold-boundary responses. `thresholdBoundaryScenario()` sends one full three-reply batch, then one reply on a connection it keeps stalled, then a full three-reply batch on a third connection; it exposes sent-reply and stall-release probes. `awaitCommandCountsPerConnection` returns counts sorted ascending after the expected total and connection count are reached. Bound all waits to five seconds so a failing test cannot hang.

Use this executor boundary:

```java
@FunctionalInterface
public interface BenchmarkCaseExecutor {
    BenchmarkStatistics execute(
            RedisBenchmarkCase testCase,
            BenchmarkConfig config,
            byte[] payload,
            BenchmarkRandom random
    ) throws Exception;
}
```

`NioBenchmarkClient` must hold one `SocketChannel`, one `SelectionKey`, one copied `PreparedPipeline`, a first-write prefix `ByteBuffer`, a preallocated two-element gathering-write array, a pipeline write `ByteBuffer`, a compacting read `ByteBuffer`, one decoder, `prefixPending`, `pendingReplies`, `batchStartNanos`, `batchLatencyMicros`, and phases `CONNECTING`, `READY`, `WRITING`, `READING`, `DONE`.

The runner loop must enforce these exact counters:

```java
long issued = 0;
long completedReplies = 0;
long histogramSamples = 0;
NioBenchmarkClient thresholdClient = null;
boolean stopping = false;

boolean grantBatch(NioBenchmarkClient client) {
    if (issued >= config.requests()) return false;
    issued += config.pipeline();
    client.beginBatch(random, clock.nanoTime(), config.pipeline());
    return true;
}

void onReply(NioBenchmarkClient client, BenchmarkRespReply reply) {
    testCase.replyExpectation().validate(reply);
    completedReplies++;
    if (histogramSamples < config.requests()) {
        histogramSamples++;
        latency.recordMicros(client.batchLatencyMicros());
        if (histogramSamples == config.requests()) thresholdClient = client;
    }
    client.decrementPendingReplies();
    if (client == thresholdClient && client.pendingReplies() == 0) stopping = true;
}
```

Create and register all non-blocking client channels before capturing the measured start time, but do not wait for connection completion. Enter the selector immediately after that timestamp. Start batch latency immediately before the first write attempt; on a new connection, a gathering write emits the prebuilt AUTH/SELECT prefix immediately followed by the measured pipeline without timed-path concatenation. Preserve both buffer positions across partial gathering writes. Set batch latency once, on the first readable selector event, before reading or parsing bytes, even when that event yields only a prefix reply. Prefix replies decrement `prefixPending` but never `completedReplies`, `histogramSamples`, or `pendingReplies`.

`histogramSamples` never exceeds the configured request count; later measured replies increment `completedReplies` and are validated but not sampled. Stop elapsed time only when `thresholdClient` has drained the rest of its full batch, even if another already-issued client becomes idle first; then close other channels without waiting for their replies. Build statistics with `completedReplies` as the RPS numerator and `histogramSamples == requests`. Compute wire requests from `issued`, which advances only for granted full batches, not from server replies or failed grant attempts.

- [ ] **Step 4: Run the happy-path runner tests and verify they pass**

Run the Step 2 command again.

Expected: PASS for pipeline 1, full tail pipeline, and shared batch latency.

- [ ] **Step 5: Commit the runner core**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/ScriptedRespServer.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunnerTest.java
git commit -m "feat: run benchmark cases on one nio selector"
```

---

### Task 7: Harden Connection, Reply, And Failure Semantics

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkExecutionException.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkClient.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunner.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkReplyExpectation.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/ScriptedRespServer.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/NioBenchmarkRunnerTest.java`

- [ ] **Step 1: Add failing reconnect, prefix, fragmentation, and error tests**

```java
@Test
public void keepAliveFalseReconnectsAfterEveryPipeline() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.respondingWith("+PONG\r\n")) {
        runPing(server, 6, 2, 2, false);
        Assert.assertEquals(4, server.awaitAcceptedConnections(4));
        Assert.assertEquals(List.of(0, 2, 2, 2), server.awaitCommandCountsPerConnection(6));
    }
}

@Test
public void authAndSelectPrefixMeasuredCommandsWithoutEnteringCounts() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.authAndSelectAware()) {
        BenchmarkStatistics result = runAuthenticatedPing(server, 5, "default", "secret", 2, false);
        Assert.assertEquals(5, result.histogramSamples());
        Assert.assertEquals(5, result.completedRequests());
        Assert.assertEquals(List.of("AUTH", "SELECT", "PING"), server.firstConnectionCommandPrefix());
        Assert.assertTrue(server.everyConnectionStartsWith(List.of("AUTH", "SELECT")));
        Assert.assertEquals(5, result.requestedRequests());
    }
}

@Test
public void fragmentedRepliesAndPartialWritesStillComplete() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.fragmentingEveryByte("+PONG\r\n")) {
        Assert.assertEquals(20, runPing(server, 20, 4, 3, true).histogramSamples());
    }
}

@Test
public void errorReplyDisconnectAndWrongShapeFailTheCase() throws Exception {
    assertExecutionFails(ScriptedRespServer.respondingWith("-ERR boom\r\n"), "ERR boom");
    assertExecutionFails(ScriptedRespServer.closingAfterCommands(1), "disconnect");
    assertExecutionFails(ScriptedRespServer.respondingWith(":1\r\n"), "expected PONG");
}

@Test
public void rejectedPrefixFailsWithoutCountingItAsBenchmarkTraffic() throws Exception {
    try (ScriptedRespServer server = ScriptedRespServer.rejectingAuth("-WRONGPASS invalid password\r\n")) {
        BenchmarkExecutionException failure = Assert.assertThrows(
                BenchmarkExecutionException.class,
                () -> runAuthenticatedPing(server, 5, "default", "bad", 0, true));
        Assert.assertEquals(0, failure.completedReplies());
        Assert.assertTrue(failure.getMessage().contains("WRONGPASS"));
        Assert.assertEquals(0, server.measuredCommandReplies());
    }
}
```

- [ ] **Step 2: Run the runner test and verify the new cases fail**

Run the Task 6 Step 2 command.

Expected: FAIL in reconnect, prefix, fragmented I/O, or structured failure behavior.

- [ ] **Step 3: Implement the hardening behavior**

- AUTH uses `AUTH <password>` or `AUTH <username> <password>` and requires `OK` on every newly connected socket that is granted a measured batch.
- Non-zero DB uses `SELECT <database>` and requires `OK` on every newly connected socket that is granted a measured batch.
- Initial and reconnect prefixes are encoded before the measured pipeline in the same first-write byte sequence. Their replies never increment completed, wire, or histogram counts, but the first readable event for a prefix still captures the shared first-batch latency, matching official behavior.
- With `keepAlive=false`, a client whose batch completes before the global reply threshold is replaced before the old socket is closed, matching `createMissingClients`. The replacement then checks the issuance budget; it may remain connected without sending a prefix or measured batch when all full batches were already granted.
- Every write preserves `ByteBuffer.position()` after partial writes; every read compacts unread bytes after draining all complete replies.
- A readable event sets batch latency once before the first `SocketChannel.read` call.
- RESP error, decoder failure, EOF with pending replies, wrong reply shape, connect failure, and 30 seconds without connection/read/write progress throw an execution exception containing title, completed count, and root cause.

Use this typed failure so orchestration never parses an exception message to recover the completed count:

```java
public final class BenchmarkExecutionException extends Exception {
    private final long completedReplies;
    private final String detail;

    public BenchmarkExecutionException(
            String title,
            long completedReplies,
            int requestedReplies,
            String detail,
            Throwable cause
    ) {
        super(title + " failed after " + completedReplies + "/" + requestedReplies
                + " replies: " + detail, cause);
        this.completedReplies = completedReplies;
        this.detail = Objects.requireNonNull(detail, "detail");
    }

    public long completedReplies() {
        return completedReplies;
    }

    public String detail() {
        return detail;
    }
}
```

Implement reply expectations with exact accepted shapes:

```java
PONG          -> SIMPLE_STRING text "PONG"
OK            -> SIMPLE_STRING text "OK"
INTEGER       -> INTEGER
BULK_OR_NULL  -> BULK_STRING or NULL_BULK
ARRAY         -> ARRAY
```

- [ ] **Step 4: Run all decoder and runner tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=IncrementalRespReplyDecoderTest,NioBenchmarkRunnerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit runner hardening**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis
git commit -m "fix: harden redis-comparable benchmark execution"
```

---

### Task 8: Orchestrate Catalog Status And Dependencies

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmark.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkTest.java`

- [ ] **Step 1: Write failing orchestration tests**

```java
@Test
public void unsupportedCasesProduceRowsWithoutCallingExecutor() {
    List<String> executed = new ArrayList<>();
    RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
        executed.add(testCase.id());
        return success(config);
    });

    BenchmarkRunResult result = benchmark.run(configWithTests("spop,zpopmin,mset,xadd"));
    Assert.assertTrue(executed.isEmpty());
    Assert.assertEquals(List.of(UNSUPPORTED, UNSUPPORTED, UNSUPPORTED, UNSUPPORTED),
            result.cases().stream().map(BenchmarkCaseResult::status).toList());
    Assert.assertEquals(0, result.exitCode());
}

@Test
public void failedLrangeSetupSkipsAllSelectedDependentsAndRunContinues() {
    RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
        if (testCase.id().equals("lrange_setup")) throw new IOException("setup failed");
        return success(config);
    });
    BenchmarkRunResult result = benchmark.run(configWithTests("lrange,set"));

    Assert.assertEquals(FAILED, result.caseById("lrange_setup").status());
    Assert.assertEquals(SKIPPED, result.caseById("lrange_100").status());
    Assert.assertEquals(SUCCESS, result.caseById("set").status());
    Assert.assertEquals(1, result.exitCode());
}

@Test
public void payloadAndRandomSourceAreSharedAcrossOneCatalogPass() {
    BenchmarkConfig config = new BenchmarkConfig(
            "127.0.0.1", 16378, 2, 1, 3, 1,
            OptionalLong.of(10_000), true, Set.of("set", "get"),
            3, 73L, BenchmarkFormat.HUMAN, "", "", 0
    );
    List<byte[]> payloads = new ArrayList<>();
    List<BenchmarkRandom> randoms = new ArrayList<>();
    List<Long> observed = new ArrayList<>();
    RedisBenchmark benchmark = benchmark((testCase, ignored, payload, random) -> {
        payloads.add(payload);
        randoms.add(random);
        observed.add(random.nextLong(10_000));
        return success(config);
    });

    BenchmarkRunResult result = benchmark.run(config);
    Assert.assertEquals(List.of(SUCCESS, SUCCESS),
            result.cases().stream().map(BenchmarkCaseResult::status).toList());
    Assert.assertSame(payloads.get(0), payloads.get(1));
    Assert.assertSame(randoms.get(0), randoms.get(1));

    BenchmarkRandom expected = new BenchmarkRandom(73L);
    Assert.assertEquals(List.of(expected.nextLong(10_000), expected.nextLong(10_000)), observed);
}

@Test
public void unknownSelectorFailsBeforeExecutorTraffic() {
    AtomicBoolean executed = new AtomicBoolean();
    RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
        executed.set(true);
        return success(config);
    });

    Assert.assertThrows(IllegalArgumentException.class,
            () -> benchmark.run(configWithTests("no_such_test")));
    Assert.assertFalse(executed.get());
}
```

- [ ] **Step 2: Run the orchestration test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=RedisBenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the public facade does not exist.

- [ ] **Step 3: Implement ordered orchestration**

`RedisBenchmark` must have a production constructor using `RedisBenchmarkCatalog` and `NioBenchmarkRunner`, plus a package-visible constructor accepting a fake executor.

```java
public BenchmarkRunResult run(BenchmarkConfig config) {
    List<RedisBenchmarkCase> selected = catalog.select(config.tests());
    byte[] payload = BenchmarkPayload.generate(config.dataSize());
    BenchmarkRandom random = new BenchmarkRandom(config.seed());
    Map<String, BenchmarkCaseResult> completed = new HashMap<>();
    List<BenchmarkCaseResult> results = new ArrayList<>();

    for (RedisBenchmarkCase testCase : selected) {
        BenchmarkCaseResult result;
        if (!testCase.support().supported()) {
            result = BenchmarkCaseResult.unsupported(testCase, testCase.support().reason());
        } else if (!testCase.dependencyId().isEmpty()
                && completed.get(testCase.dependencyId()).status() != BenchmarkStatus.SUCCESS) {
            result = BenchmarkCaseResult.skipped(testCase,
                    "dependency " + testCase.dependencyId() + " did not succeed");
        } else {
            try {
                result = BenchmarkCaseResult.success(testCase,
                        executor.execute(testCase, config, payload, random));
            } catch (BenchmarkExecutionException failure) {
                result = BenchmarkCaseResult.failed(testCase,
                        failure.completedReplies(), failure.detail());
            } catch (Exception failure) {
                result = BenchmarkCaseResult.failed(testCase, 0, conciseMessage(failure));
            }
        }
        completed.put(testCase.id(), result);
        results.add(result);
    }
    return new BenchmarkRunResult(results);
}
```

Do not reset the server, payload generator, or random source between cases. Do not invoke the executor for unsupported or dependency-skipped cases.

- [ ] **Step 4: Run orchestration plus catalog tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit orchestration**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmark.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkTest.java
git commit -m "feat: orchestrate official benchmark case results"
```

---

### Task 9: Render Comparable Human, Quiet, And CSV Results

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRenderer.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRendererTest.java`

- [ ] **Step 1: Write failing renderer golden tests**

```java
@Test
public void csvKeepsOfficialColumnsFirstAndLeavesUnsupportedMetricsEmpty() {
    String csv = new BenchmarkOutputRenderer().render(config(BenchmarkFormat.CSV), run(
            success("PING_INLINE", statistics()),
            unsupported("SPOP", "Yierdis does not support SPOP")
    ));
    String[] lines = csv.lines().toArray(String[]::new);
    Assert.assertEquals("\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\",\"status\",\"reason\"", lines[0]);
    Assert.assertTrue(lines[1].startsWith("\"PING_INLINE\",\"2500.00\""));
    Assert.assertEquals("\"SPOP\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"UNSUPPORTED\",\"Yierdis does not support SPOP\"", lines[2]);
}

@Test
public void quietAndHumanOutputNeverRenderFakeMetricsForFailure() {
    String quiet = renderer.render(config(BenchmarkFormat.QUIET), run(failed("SET", 7, "disconnect")));
    Assert.assertEquals("SET: FAILED after 7 replies (disconnect)\n", quiet);
    Assert.assertFalse(quiet.contains("0.00 requests per second"));

    String human = renderer.render(config(BenchmarkFormat.HUMAN), run(success("PING_MBULK", statistics())));
    Assert.assertTrue(human.contains("====== PING_MBULK ======"));
    Assert.assertTrue(human.contains("throughput summary: 2500.00 requests per second"));
    Assert.assertTrue(human.contains("latency summary (msec)"));
}
```

- [ ] **Step 2: Run renderer tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=BenchmarkOutputRendererTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the renderer does not exist.

- [ ] **Step 3: Implement pure deterministic rendering**

- Human success blocks include actual completed requests at the stop boundary, elapsed seconds, parallel clients, payload bytes, keepalive, throughput, and `avg/min/p50/p95/p99/max` milliseconds.
- Quiet success lines use `TITLE: %.2f requests per second, p50=%.3f msec`.
- CSV uses RFC 4180 quoting for every field, the exact ten-column header in the test, and the official fixed three decimal places for latency fields. `config.precision()` controls only HdrHistogram significant digits.
- Non-success human and quiet output includes canonical title, status, reason, and completed count for failures.
- Non-success CSV leaves all seven numeric metric fields empty and emits status/reason.
- Every non-empty rendered report ends with one newline. The renderer writes no progress display and has no access to network classes.

- [ ] **Step 4: Run renderer and result tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=BenchmarkCaseResultTest,BenchmarkOutputRendererTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit rendering**

```bash
git add yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRenderer.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchmarkOutputRendererTest.java
git commit -m "feat: render comparable benchmark results"
```

---

### Task 10: Switch The Entrypoint And Delete The Old Benchmark

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommand.java`
- Replace: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Modify: `yierdis-benchmark/pom.xml`
- Delete: old production and test paths listed in Final File Structure
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/RedisBenchmarkCommandTest.java`

- [ ] **Step 1: Write failing command and entrypoint tests**

```java
@Test
public void helpUsesTheReplacementCommandWithoutConnecting() {
    CommandLine commandLine = YierdisBench.commandLine();
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    Assert.assertEquals(0, commandLine.execute("--help"));
    Assert.assertTrue(out.toString().contains("--requests"));
    Assert.assertFalse(out.toString().contains("--suite"));
    Assert.assertFalse(out.toString().contains("--serverJar"));
}

@Test
public void commandRendersInjectedRunAndReturnsRunExitCode() {
    Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> new BenchmarkRunResult(List.of(
            BenchmarkCaseResult.unsupported(catalog.caseById("spop"), "missing")
    ));
    RedisBenchmarkCommand command = new RedisBenchmarkCommand(fake, new BenchmarkOutputRenderer());
    CommandLine cli = new CommandLine(command);
    StringWriter out = new StringWriter();
    cli.setOut(new PrintWriter(out));
    Assert.assertEquals(0, cli.execute("--tests", "spop", "--format", "quiet"));
    Assert.assertTrue(out.toString().contains("SPOP: UNSUPPORTED"));
}

@Test
public void unknownSelectorIsUsageErrorWithoutNetworkTraffic() {
    RedisBenchmarkCommand command = new RedisBenchmarkCommand(
            new RedisBenchmark()::run, new BenchmarkOutputRenderer());
    Assert.assertEquals(2, new CommandLine(command).execute("--tests", "no_such_test"));
}

@Test
public void failedRunRendersCompleteResultAndExitsOne() {
    Function<BenchmarkConfig, BenchmarkRunResult> fake = config -> new BenchmarkRunResult(List.of(
            BenchmarkCaseResult.failed(catalog.caseById("set"), 7, "disconnect")
    ));
    RedisBenchmarkCommand command = new RedisBenchmarkCommand(fake, new BenchmarkOutputRenderer());
    CommandLine cli = new CommandLine(command);
    StringWriter out = new StringWriter();
    cli.setOut(new PrintWriter(out));

    Assert.assertEquals(1, cli.execute("--tests", "set", "--format", "quiet"));
    Assert.assertTrue(out.toString().contains("SET: FAILED after 7 replies (disconnect)"));
}
```

- [ ] **Step 2: Run entrypoint tests and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=YierdisBenchEntrypointTest,RedisBenchmarkCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the replacement command is not wired.

- [ ] **Step 3: Implement the command, replace the launcher, and delete old code**

`RedisBenchmarkCommand` is a picocli `Callable<Integer>` with a `@Mixin RedisBenchmarkOptions`, a production constructor that injects `new RedisBenchmark()::run`, and a package-visible constructor accepting `Function<BenchmarkConfig, BenchmarkRunResult>` plus the renderer. It converts options, invokes the function, renders once to the command's output writer, and returns `BenchmarkRunResult.exitCode()`. Convert `IllegalArgumentException` from config conversion or catalog selection into a picocli `ParameterException` so invalid values and unknown selectors exit `2`; execution failures already represented in the run return `1`.

Replace `YierdisBench` with:

```java
public final class YierdisBench {
    public static void main(String[] args) {
        int exitCode = commandLine().execute(args);
        if (exitCode != 0) System.exit(exitCode);
    }

    public static CommandLine commandLine() {
        return new CommandLine(new RedisBenchmarkCommand())
                .setCaseInsensitiveEnumValuesAllowed(true);
    }

    private YierdisBench() {
    }
}
```

Delete every old benchmark source/test listed in Final File Structure. Then remove `yierdis-db-api` and `yierdis-memory-ffm` from `yierdis-benchmark/pom.xml`. Keep only networking RESP, picocli, HdrHistogram, and JUnit dependencies. Do not retain legacy routing tokens or classes.

- [ ] **Step 4: Run the full rewritten benchmark module tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am test
```

Expected: PASS and no old suite/comparison/native-eval test classes remain.

- [ ] **Step 5: Prove the old implementation is absent**

Run:

```bash
rg -n "BenchHarness|YierdisBenchArgs|YierdisBenchServerArgs|SuiteRunner|nativeEval|comparisonMode|includeRedis|serverJar" \
  yierdis-benchmark/src/main

rg -n "FLUSHDB|warmup|prefill" yierdis-benchmark/src/main
```

Expected: neither search returns matches.

- [ ] **Step 6: Commit the cutover and deletions**

```bash
git add -A yierdis-benchmark
git commit -m "feat: replace legacy benchmark entrypoint"
```

---

### Task 11: Add Command Coverage And Real-Server Guards

**Files:**
- Modify: `yierdis-tests/yierdis-integration-tests/pom.xml`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java`
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/benchmark/RedisBenchmarkCatalogCoverageGuardTest.java`
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/benchmark/RedisBenchmarkRealServerTest.java`

- [ ] **Step 1: Write failing integration guards**

Add a public immutable accessor in `DefaultCommandRegistrationTest`; the coverage guard is in a sibling integration-test package:

```java
public static Set<String> defaultCommandNames() {
    return DEFAULT_COMMANDS;
}
```

Then write:

```java
@Test
public void catalogSupportMatchesRegisteredYierdisCommands() {
    Set<String> commands = DefaultCommandRegistrationTest.defaultCommandNames();
    for (RedisBenchmarkCase testCase : new RedisBenchmarkCatalog().allCases()) {
        boolean allPresent = commands.containsAll(testCase.requiredCommands());
        Assert.assertEquals(testCase.title() + " support classification", allPresent,
                testCase.support().supported());
    }
}

@Test
public void allOfficialCasesRunOrReportUnsupportedAgainstRealYierdis() throws Exception {
    try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--noCleanup")) {
        BenchmarkConfig config = new BenchmarkConfig(
                "127.0.0.1", server.port(), 40, 4, 3, 3,
                OptionalLong.empty(), true, Set.of(), 3, 1L,
                BenchmarkFormat.CSV, "", "", 0
        );
        BenchmarkRunResult result = new RedisBenchmark().run(config);

        Assert.assertEquals(21, result.cases().size());
        Assert.assertEquals(17, result.cases().stream().filter(r -> r.status() == SUCCESS).count());
        Assert.assertEquals(List.of("spop", "zpopmin", "mset", "xadd"), result.cases().stream()
                .filter(r -> r.status() == UNSUPPORTED)
                .map(r -> r.testCase().id()).toList());
        Assert.assertEquals(0, result.exitCode());
    }
}
```

- [ ] **Step 2: Run the guards and verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=RedisBenchmarkCatalogCoverageGuardTest,RedisBenchmarkRealServerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the integration module cannot yet see benchmark classes and the guards do not exist.

- [ ] **Step 3: Add the test dependency and complete the guards**

Add to integration-test dependencies:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-benchmark</artifactId>
    <version>${project.version}</version>
    <scope>test</scope>
</dependency>
```

Expose only the public catalog/config/facade/result APIs needed by integration tests. Do not make NIO client internals public. Ensure the coverage guard treats inline PING as requiring registered `PING`, and verifies the four unsupported cases each have at least one absent required command.

- [ ] **Step 4: Run the integration guards and benchmark tests**

Run the Step 2 command, then:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am test
```

Expected: both commands PASS.

- [ ] **Step 5: Commit integration coverage**

```bash
git add yierdis-tests/yierdis-integration-tests/pom.xml \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/DefaultCommandRegistrationTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/benchmark
git commit -m "test: guard redis benchmark catalog coverage"
```

---

### Task 12: Rewrite The Benchmark Script And Documentation

**Files:**
- Modify: `scripts/bench.sh`
- Modify: `README.md`
- Modify: `docs/project-docs/client-and-bench-internals.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/production-hardening-operations.md`
- Modify: `docs/project-docs/testing-and-debugging.md`
- Modify: `docs/superpowers/specs/2026-06-14-release-grade-benchmark-suite-design.md`
- Modify: `docs/superpowers/specs/2026-06-28-redis-benchmark-compatible-benchmark-design.md`
- Modify: `docs/superpowers/specs/2026-06-28-redis-suite-comparison-design.md`
- Modify: `docs/superpowers/plans/2026-06-14-release-grade-benchmark-suite.md`
- Modify: `docs/superpowers/plans/2026-06-28-redis-benchmark-compatible-benchmark.md`
- Modify: `docs/superpowers/plans/2026-06-29-redis-suite-comparison.md`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchScriptContractTest.java`

- [ ] **Step 1: Write the failing script contract test**

```java
@Test
public void benchScriptBuildsOnlyTheClientAndTargetsAnExistingServer() throws IOException {
    String script = Files.readString(repoRoot().resolve("scripts/bench.sh"));
    Assert.assertTrue(script.contains("-pl yierdis-benchmark -am"));
    Assert.assertTrue(script.contains("--host"));
    Assert.assertTrue(script.contains("--port"));
    Assert.assertTrue(script.contains("--requests"));
    Assert.assertTrue(script.contains("--clients"));
    Assert.assertTrue(script.contains("--data-size"));
    Assert.assertTrue(script.contains("--pipeline"));
    Assert.assertFalse(script.contains("--serverJar"));
    Assert.assertFalse(script.contains("currentServerJar"));
    Assert.assertFalse(script.contains("baselineServerJar"));
    Assert.assertFalse(script.contains("--suite"));
}

private static Path repoRoot() {
    return Path.of(System.getProperty("maven.multiModuleProjectDirectory"));
}
```

- [ ] **Step 2: Run the script contract test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=BenchScriptContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `scripts/bench.sh` still starts or configures the old benchmark.

- [ ] **Step 3: Rewrite `scripts/bench.sh` for connect-only execution**

The script must:

- build `yierdis-benchmark` only unless `SKIP_BUILD=1`;
- discover the shaded benchmark jar;
- default `HOST=127.0.0.1`, `PORT=16378`, `REQUESTS=100000`, `CLIENTS=50`, `DATA_SIZE=3`, `PIPELINE=1`, `FORMAT=human`;
- pass `KEYSPACE` only when the environment variable is non-empty, preserving omitted-vs-zero semantics;
- optionally pass `TESTS`, `KEEP_ALIVE`, `PRECISION`, `SEED`, `USERNAME`, `PASSWORD`, and `DATABASE`; encode keepalive as `--keep-alive=<true|false>` so false is not mistaken for an omitted boolean flag;
- never discover, start, or stop a server jar.

The final `exec` shape is:

```bash
exec java $BENCH_JVM_OPTS -jar "$bench_jar" \
  --host "$HOST" --port "$PORT" \
  --requests "$REQUESTS" --clients "$CLIENTS" \
  --data-size "$DATA_SIZE" --pipeline "$PIPELINE" \
  --format "$FORMAT" "${optional_args[@]}"
```

- [ ] **Step 4: Rewrite current documentation and mark historical docs**

README must show:

1. start Yierdis separately;
2. run `./scripts/bench.sh`;
3. run official `redis-benchmark` separately against Redis with equivalent values;
4. compare corresponding canonical titles and shared CSV fields;
5. understand that SPOP, ZPOPMIN, MSET, and XADD are currently unsupported.

Replace the old benchmark internals section with this flow:

```text
RedisBenchmarkOptions
  -> BenchmarkConfig
  -> RedisBenchmarkCatalog.select()
  -> RedisBenchmark
  -> NioBenchmarkRunner (one Selector)
  -> BenchmarkLatencyRecorder
  -> BenchmarkCaseResult / BenchmarkRunResult
  -> BenchmarkOutputRenderer
```

Update `core-logic-index.md` to point at `RedisBenchmarkOptions`, `RedisBenchmarkCatalog`, `NioBenchmarkRunner`, and `BenchmarkOutputRenderer` instead of the deleted argument model. Replace the deleted focused-test list in `testing-and-debugging.md` with the new catalog, template, runner, renderer, command, and script-contract tests. In `production-hardening-operations.md`, remove the deleted automatic baseline/current suite command and state that performance evidence now consists of separately managed Yierdis and official Redis runs with equivalent built-in workload options; this benchmark does not compute release thresholds or artifact ratios.

At the top of each of the six historical documents add:

```markdown
> **Superseded:** Replaced by `docs/superpowers/specs/2026-07-17-redis-benchmark-comparable-rewrite-design.md`. Retained only as historical context.
```

- [ ] **Step 5: Run script and documentation contract checks**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am -Dtest=BenchScriptContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Run:

```bash
rg -n -- "--suite|--serverJar|baseline/current comparison|native allocator eval|--includeRedis" \
  README.md scripts/bench.sh docs/project-docs/client-and-bench-internals.md \
  docs/project-docs/core-logic-index.md \
  docs/project-docs/production-hardening-operations.md \
  docs/project-docs/testing-and-debugging.md
```

Expected: test PASS; search returns no current-documentation references to removed benchmark modes.

- [ ] **Step 6: Commit scripts and docs**

```bash
git add scripts/bench.sh README.md docs/project-docs/client-and-bench-internals.md \
  docs/project-docs/core-logic-index.md \
  docs/project-docs/production-hardening-operations.md \
  docs/project-docs/testing-and-debugging.md \
  docs/superpowers/specs/2026-06-14-release-grade-benchmark-suite-design.md \
  docs/superpowers/specs/2026-06-28-redis-benchmark-compatible-benchmark-design.md \
  docs/superpowers/specs/2026-06-28-redis-suite-comparison-design.md \
  docs/superpowers/plans/2026-06-14-release-grade-benchmark-suite.md \
  docs/superpowers/plans/2026-06-28-redis-benchmark-compatible-benchmark.md \
  docs/superpowers/plans/2026-06-29-redis-suite-comparison.md \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/redis/BenchScriptContractTest.java
git commit -m "docs: document redis-comparable benchmark"
```

---

### Task 13: Full Verification And Packaged Acceptance

**Files:**
- Verify all files changed by Tasks 1-12
- No new production behavior in this task

- [ ] **Step 1: Run the complete benchmark module test suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am test
```

Expected: BUILD SUCCESS.

- [ ] **Step 2: Run benchmark integration guards**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=RedisBenchmarkCatalogCoverageGuardTest,RedisBenchmarkRealServerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: BUILD SUCCESS with 17 success cases and four unsupported cases in the real-server test.

- [ ] **Step 3: Package benchmark, server, and CLI jars**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark,yierdis-server/yierdis-server-main,yierdis-cli \
  -am -DskipTests package
```

Expected: shaded jars exist under all three target directories.

- [ ] **Step 4: Run a packaged connect-only acceptance**

Confirm port `16379` is unused, then start the packaged Yierdis server in a managed terminal session:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --port 16379 --noCleanup
```

From a second terminal, poll readiness with the packaged Java CLI, run the benchmark, and retain the CSV for exact checks:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
timeout 30 bash -c 'until java -jar yierdis-cli/target/yierdis-cli-0.1.0-SNAPSHOT.jar --host 127.0.0.1 --port 16379 PING | rg -q PONG; do sleep 0.1; done'

JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --host 127.0.0.1 --port 16379 \
  --requests 40 --clients 4 --pipeline 3 --format csv \
  > /tmp/yierdis-benchmark-acceptance.csv

wc -l /tmp/yierdis-benchmark-acceptance.csv
rg -c '"SUCCESS"' /tmp/yierdis-benchmark-acceptance.csv
rg -c '"UNSUPPORTED"' /tmp/yierdis-benchmark-acceptance.csv
```

Expected:

- CSV header begins with the eight official shared columns;
- 21 result rows are present;
- 17 rows have `SUCCESS`;
- SPOP, ZPOPMIN, MSET, and XADD have `UNSUPPORTED` with empty numeric fields;
- process exits zero;
- the three verification commands print `22`, `17`, and `4` respectively;
- server process is stopped after the run.

- [ ] **Step 5: Verify deletion and repository cleanliness**

```bash
rg -n -- "BenchHarness|YierdisBenchArgs|YierdisBenchServerArgs|SuiteRunner|nativeEval|comparisonMode|includeRedis|serverJar" \
  yierdis-benchmark/src/main README.md scripts/bench.sh \
  docs/project-docs/client-and-bench-internals.md \
  docs/project-docs/core-logic-index.md \
  docs/project-docs/production-hardening-operations.md \
  docs/project-docs/testing-and-debugging.md
```

Expected: no current implementation or documentation references.

```bash
git status --short
```

Expected: clean worktree.

- [ ] **Step 6: Resolve acceptance failures through the owning task**

If Step 4 exposes a defect, reopen the Task 1-12 component that owns the behavior, add the focused regression test named by that task, make the minimum fix, rerun that task's focused command and Steps 1-5 here, then use that task's exact `git add` list for a `fix: complete redis benchmark acceptance` commit. If no adjustment is needed, do not create an empty commit.
