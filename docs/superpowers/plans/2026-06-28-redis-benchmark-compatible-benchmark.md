> **Superseded:** Replaced by `docs/superpowers/specs/2026-07-17-redis-benchmark-comparable-rewrite-design.md`. Retained only as historical context.

# Redis-Benchmark-Compatible Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the default `yierdis-benchmark` entrypoint with a connect-only `redis-benchmark`-style runner while preserving suite, comparison, and native-eval behavior behind explicit non-default modes.

**Architecture:** Keep `YierdisBench` as the jar main class, but reduce it to a thin delegator that routes either to the new compatibility path or to preserved legacy advanced modes. Build the compatibility path in a new `compat` package with four focused units: parser, case/catalog model, runner, and renderers. Use benchmark-module unit tests for parser/runner/output behavior and one integration-test-module guard that compares the benchmark catalog against live server command metadata.

**Tech Stack:** Java 25, Maven, existing `RespClientCodec`, existing benchmark/socket helpers, picocli for explicit advanced modes only, JUnit 4, shell scripts.

## Global Constraints

- The default compatibility path is connect-only. It targets an already running server and does not auto-start a Yierdis child process.
- Existing Yierdis-specific suite/comparison/native-eval behavior should remain available behind explicit non-default entrypoints.
- The first version must explicitly reject unsupported flags instead of silently ignoring them.
- Unsupported first-version options include: `-s <socket>`, `-3`, `--threads`, `--cluster`, `--enable-tracking`, TLS-related options, and any feature that depends on Redis server capabilities Yierdis does not expose.
- The default compatibility path must not add an implicit warmup stage.
- The first version must not add a default continue-on-error mode.
- Raw command-template mode performs no benchmark-specific data preparation by default.
- Only measured benchmark commands contribute to throughput and latency statistics.
- `PING_INLINE` must send inline protocol bytes rather than a RESP array request.
- Use JDK 25 for every Maven, Java, and script command:
  `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`.

---

## File Structure

### New production files

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java`
  Routes `main(String[])` into compatibility mode or explicit advanced modes.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/LegacyBenchModes.java`
  Owns preserved suite/comparison/native-eval entrypoints after they are extracted from `YierdisBench`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatCommand.java`
  Normalized compatibility-mode CLI model.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParser.java`
  Parses Redis-style argv into `RedisBenchmarkCompatCommand`.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java`
  Built-in or raw command benchmark case model.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplate.java`
  Request-template model for RESP-array and inline request encoding.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkPreparationPlan.java`
  Fixture setup model for built-in cases that need existing data.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java`
  Compatibility default set, optional built-ins, coverage classification export.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCaseClassification.java`
  Per-command catalog classification used by the coverage guard.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java`
  Connection setup, pipelining, keepalive/reconnect behavior, fail-fast reply handling, latency collection.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRenderer.java`
  Human, quiet, and CSV output.

### New benchmark-module tests

- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointRoutingTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParserTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkScriptedServer.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplateTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunnerTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRendererTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java`

### New integration guard

- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/BenchmarkCatalogCoverageGuardTest.java`

### Existing files to modify

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- `yierdis-benchmark/pom.xml`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSuiteEntrypointTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchScriptContractTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java`
- `yierdis-tests/yierdis-integration-tests/pom.xml`
- `scripts/bench.sh`
- `scripts/smoke.sh`
- `docs/project-docs/client-and-bench-internals.md`

## Task 1: Route The Entrypoint And Parse Redis-Style CLI

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatCommand.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParser.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointRoutingTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParserTest.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`

**Interfaces:**
- Consumes: `public static void YierdisBench.main(String[] args) throws Exception`
- Produces: `public int BenchmarkEntrypointRouter.run(String[] args) throws Exception`
- Produces: `public RedisBenchmarkCompatCommand RedisBenchmarkCompatParser.parse(String[] argv)`
- Produces: `public boolean RedisBenchmarkCompatCommand.rawMode()`

- [ ] **Step 1: Write the failing tests**

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParserTest.java
package yier.bubu.redis.app.bench.compat;

import org.junit.Assert;
import org.junit.Test;

public class RedisBenchmarkCompatParserTest {
    @Test
    public void hostFlagIsNotHelpAndCompatModeIsConnectOnly() {
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(
                new String[]{"-h", "127.0.0.1", "-p", "6380", "-n", "1000", "-c", "20"}
        );

        Assert.assertEquals("127.0.0.1", command.host());
        Assert.assertEquals(6380, command.port());
        Assert.assertEquals(1000, command.requests());
        Assert.assertEquals(20, command.clients());
        Assert.assertFalse(command.help());
        Assert.assertFalse(command.rawMode());
    }

    @Test
    public void positionalCommandTurnsOnRawModeAndIgnoresSelectedTests() {
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(
                new String[]{"-t", "set,get", "lpush", "mylist", "__rand_int__"}
        );

        Assert.assertTrue(command.rawMode());
        Assert.assertEquals(java.util.List.of("lpush", "mylist", "__rand_int__"), command.rawCommandArgs());
        Assert.assertEquals(java.util.List.of("set", "get"), command.selectedTests());
    }

    @Test
    public void unsupportedThreadsOptionIsRejected() {
        try {
            new RedisBenchmarkCompatParser().parse(new String[]{"--threads", "2"});
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("--threads"));
        }
    }
}
```

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointRoutingTest.java
package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

public class YierdisBenchEntrypointRoutingTest {
    @Test
    public void suiteTokenRoutesToLegacyMode() throws Exception {
        BenchmarkEntrypointRouter router = new BenchmarkEntrypointRouter();

        try {
            router.run(new String[]{"suite"});
            Assert.fail("expected validation error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("suite requires currentServerJar"));
        }
    }

    @Test
    public void defaultPathUsesCompatParser() throws Exception {
        BenchmarkEntrypointRouter router = new BenchmarkEntrypointRouter();

        int exitCode = router.run(new String[]{"--help"});

        Assert.assertEquals(0, exitCode);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=YierdisBenchEntrypointRoutingTest,RedisBenchmarkCompatParserTest \
  test
```

Expected: FAIL with class-not-found or symbol-not-found errors for
`BenchmarkEntrypointRouter`, `RedisBenchmarkCompatParser`, or
`RedisBenchmarkCompatCommand`.

- [ ] **Step 3: Write the minimal implementation**

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatCommand.java
package yier.bubu.redis.app.bench.compat;

import java.util.List;

public record RedisBenchmarkCompatCommand(
        String host,
        int port,
        int requests,
        int clients,
        int dataSize,
        int dbNum,
        boolean keepAlive,
        int randomKeyspaceLength,
        int pipeline,
        boolean quiet,
        boolean csv,
        int precision,
        boolean loop,
        boolean idleMode,
        boolean stdinLastArg,
        boolean help,
        boolean version,
        List<String> selectedTests,
        List<String> rawCommandArgs
) {
    public RedisBenchmarkCompatCommand {
        selectedTests = List.copyOf(selectedTests);
        rawCommandArgs = List.copyOf(rawCommandArgs);
    }

    public boolean rawMode() {
        return !rawCommandArgs.isEmpty();
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParser.java
package yier.bubu.redis.app.bench.compat;

import java.util.ArrayList;
import java.util.List;

public final class RedisBenchmarkCompatParser {
    public RedisBenchmarkCompatCommand parse(String[] argv) {
        String host = "127.0.0.1";
        int port = 6379;
        int requests = 100_000;
        int clients = 50;
        int dataSize = 3;
        int dbNum = 0;
        boolean keepAlive = true;
        int randomKeyspaceLength = 0;
        int pipeline = 1;
        boolean quiet = false;
        boolean csv = false;
        int precision = 0;
        boolean loop = false;
        boolean idleMode = false;
        boolean stdinLastArg = false;
        boolean help = false;
        boolean version = false;
        List<String> selectedTests = new ArrayList<>();
        List<String> raw = new ArrayList<>();

        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];
            if (!raw.isEmpty()) {
                raw.add(arg);
                continue;
            }
            switch (arg) {
                case "--help" -> help = true;
                case "--version" -> version = true;
                case "-h" -> host = argv[++i];
                case "-p" -> port = Integer.parseInt(argv[++i]);
                case "-n" -> requests = Integer.parseInt(argv[++i]);
                case "-c" -> clients = Integer.parseInt(argv[++i]);
                case "-d" -> dataSize = Integer.parseInt(argv[++i]);
                case "--dbnum" -> dbNum = Integer.parseInt(argv[++i]);
                case "-k" -> keepAlive = !"0".equals(argv[++i]);
                case "-r" -> randomKeyspaceLength = Integer.parseInt(argv[++i]);
                case "-P" -> pipeline = Integer.parseInt(argv[++i]);
                case "-q" -> quiet = true;
                case "--csv" -> csv = true;
                case "--precision" -> precision = Integer.parseInt(argv[++i]);
                case "-l" -> loop = true;
                case "-I" -> idleMode = true;
                case "-x" -> stdinLastArg = true;
                case "-t" -> selectedTests = List.of(argv[++i].split(","));
                case "-s", "-3", "--threads", "--cluster", "--enable-tracking" ->
                        throw new IllegalArgumentException("unsupported benchmark option: " + arg);
                default -> {
                    if (arg.startsWith("-")) {
                        throw new IllegalArgumentException("unsupported benchmark option: " + arg);
                    }
                    raw.add(arg);
                }
            }
        }

        return new RedisBenchmarkCompatCommand(
                host, port, requests, clients, dataSize, dbNum, keepAlive, randomKeyspaceLength,
                pipeline, quiet, csv, precision, loop, idleMode, stdinLastArg, help, version,
                selectedTests, raw
        );
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java
package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.compat.RedisBenchmarkCompatCommand;
import yier.bubu.redis.app.bench.compat.RedisBenchmarkCompatParser;

import java.util.Arrays;

public final class BenchmarkEntrypointRouter {
    public int run(String[] args) throws Exception {
        if (args.length > 0) {
            String mode = args[0];
            String[] tail = Arrays.copyOfRange(args, 1, args.length);
            if ("suite".equals(mode)) {
                YierdisBench.runLegacySuiteMode(tail);
                return 0;
            }
            if ("compare".equals(mode)) {
                YierdisBench.runLegacyComparisonMode(tail);
                return 0;
            }
            if ("native-eval".equals(mode)) {
                YierdisBench.runLegacyNativeEvalMode(tail);
                return 0;
            }
        }
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(args);
        return (command.help() || command.version()) ? 0 : 0;
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java
public static void main(String[] args) throws Exception {
    int exitCode = new BenchmarkEntrypointRouter().run(args);
    if (exitCode != 0) {
        System.exit(exitCode);
    }
}

static void runLegacySuiteMode(String[] args) {
    throw new IllegalArgumentException("suite requires currentServerJar");
}

static void runLegacyComparisonMode(String[] args) {
    throw new IllegalArgumentException("comparisonMode requires baselineServerJar and currentServerJar");
}

static void runLegacyNativeEvalMode(String[] args) {
    throw new IllegalArgumentException("native-eval mode requires explicit invocation parameters");
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=YierdisBenchEntrypointRoutingTest,RedisBenchmarkCompatParserTest \
  test
```

Expected: PASS with both test classes green.

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatCommand.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParser.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchEntrypointRoutingTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCompatParserTest.java
git commit -m "refactor: route benchmark entrypoint by mode"
```

## Task 2: Build The Compatibility Runner Core

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplate.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRenderer.java`
- Create: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkScriptedServer.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplateTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunnerTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRendererTest.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java`

**Interfaces:**
- Consumes: `RedisBenchmarkCompatCommand`
- Produces: `public int RedisBenchmarkRunner.run(RedisBenchmarkCompatCommand command, java.io.PrintStream out, java.io.PrintStream err) throws Exception`
- Produces: `public static RedisBenchmarkCommandTemplate inline(byte[] inlineBytes)`
- Produces: `public static RedisBenchmarkCommandTemplate respArray(List<byte[]> argv)`
- Produces: `public void RedisBenchmarkOutputRenderer.renderHuman(...)`
- Produces: `public void RedisBenchmarkOutputRenderer.renderQuiet(...)`
- Produces: `public void RedisBenchmarkOutputRenderer.renderCsv(...)`

- [ ] **Step 1: Write the failing tests**

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplateTest.java
package yier.bubu.redis.app.bench.compat;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.SplittableRandom;

public class RedisBenchmarkCommandTemplateTest {
    @Test
    public void pingInlineUsesInlineBytesInsteadOfRespArray() throws Exception {
        RedisBenchmarkCommandTemplate template = RedisBenchmarkCommandTemplate.inline(
                "PING\r\n".getBytes(StandardCharsets.US_ASCII)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        template.writeTo(out, new SplittableRandom(1L), 0);

        Assert.assertEquals("PING\r\n", out.toString(StandardCharsets.US_ASCII));
    }

    @Test
    public void randIntExpandsPerExecution() throws Exception {
        RedisBenchmarkCommandTemplate template = RedisBenchmarkCommandTemplate.respArray(
                java.util.List.of(
                        "SET".getBytes(StandardCharsets.US_ASCII),
                        "key:__rand_int__".getBytes(StandardCharsets.US_ASCII),
                        "v".getBytes(StandardCharsets.US_ASCII)
                )
        );
        ByteArrayOutputStream first = new ByteArrayOutputStream();
        ByteArrayOutputStream second = new ByteArrayOutputStream();
        SplittableRandom random = new SplittableRandom(7L);

        template.writeTo(first, random, 100);
        template.writeTo(second, random, 100);

        Assert.assertNotEquals(first.toString(StandardCharsets.US_ASCII), second.toString(StandardCharsets.US_ASCII));
        Assert.assertTrue(first.toString(StandardCharsets.US_ASCII).contains("key:"));
    }
}
```

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunnerTest.java
package yier.bubu.redis.app.bench.compat;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class RedisBenchmarkRunnerTest {
    @Test
    public void respErrorAbortsWithNonZeroExitCode() throws Exception {
        try (RedisBenchmarkScriptedServer server = RedisBenchmarkScriptedServer.errorServer("-ERR boom\r\n")) {
            RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatCommand(
                    "127.0.0.1", server.port(), 1, 1, 3, 0, true, 0, 1,
                    false, false, 0, false, false, false, false, false,
                    java.util.List.of(), java.util.List.of("PING")
            );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();

            int exitCode = new RedisBenchmarkRunner().run(command,
                    new java.io.PrintStream(out), new java.io.PrintStream(err));

            Assert.assertNotEquals(0, exitCode);
            Assert.assertTrue(err.toString().contains("ERR boom"));
        }
    }
}
```

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkScriptedServer.java
package yier.bubu.redis.app.bench.compat;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public final class RedisBenchmarkScriptedServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final BiFunction<String, Integer, byte[]> script;
    private final CountDownLatch listening = new CountDownLatch(1);
    private final List<String> commands = new ArrayList<>();
    private volatile boolean closed;
    private Thread thread;

    private RedisBenchmarkScriptedServer(ServerSocket serverSocket, BiFunction<String, Integer, byte[]> script) {
        this.serverSocket = serverSocket;
        this.script = script;
    }

    public static RedisBenchmarkScriptedServer errorServer(String errorReply) throws IOException {
        return start((command, index) -> errorReply.getBytes(StandardCharsets.US_ASCII));
    }

    public static RedisBenchmarkScriptedServer captureCommandsServer() throws IOException {
        return start((command, index) -> {
            if ("GET".equals(command)) {
                return "$1\r\nx\r\n".getBytes(StandardCharsets.US_ASCII);
            }
            return "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
        });
    }

    public static RedisBenchmarkScriptedServer start(BiFunction<String, Integer, byte[]> script) throws IOException {
        RedisBenchmarkScriptedServer server = new RedisBenchmarkScriptedServer(new ServerSocket(0), script);
        server.thread = new Thread(server::acceptLoop, "redis-benchmark-scripted-server");
        server.thread.setDaemon(true);
        server.thread.start();
        return server;
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public List<String> commands() {
        synchronized (commands) {
            return List.copyOf(commands);
        }
    }

    public boolean awaitListening() throws InterruptedException {
        return listening.await(1, TimeUnit.SECONDS);
    }

    private void acceptLoop() {
        listening.countDown();
        while (!closed) {
            try (Socket socket = serverSocket.accept()) {
                handle(socket);
            } catch (IOException e) {
                if (!closed) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private void handle(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        int index = 0;
        while (!closed && !socket.isClosed()) {
            RespClientCodec.RespReply reply = RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            if (reply.kind() != RespClientCodec.RespReply.Kind.ARRAY || reply.values().isEmpty()) {
                throw new IllegalStateException("expected RESP array request");
            }
            RespClientCodec.RespReply commandReply = reply.values().get(0);
            if (commandReply.kind() != RespClientCodec.RespReply.Kind.BULK_STRING) {
                throw new IllegalStateException("expected command bulk string");
            }
            String command = new String(commandReply.bytes(), StandardCharsets.US_ASCII).toUpperCase();
            synchronized (commands) {
                commands.add(command);
            }
            out.write(script.apply(command, index++));
            out.flush();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        serverSocket.close();
        if (thread != null) {
            thread.join(1_000L);
        }
    }
}
```

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRendererTest.java
package yier.bubu.redis.app.bench.compat;

import org.junit.Assert;
import org.junit.Test;

public class RedisBenchmarkOutputRendererTest {
    @Test
    public void quietOutputMatchesRedisStyleSummaryLine() {
        String line = new RedisBenchmarkOutputRenderer().quietLine("PING_INLINE", 1234.5, 0.321);
        Assert.assertEquals("PING_INLINE: 1234.50 requests per second, p50=0.321 msec", line);
    }

    @Test
    public void csvOutputUsesStableHeader() {
        Assert.assertEquals(
                "\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\"",
                new RedisBenchmarkOutputRenderer().csvHeader()
        );
    }

    @Test
    public void humanOutputUsesRedisStyleHeadingAndSummary() {
        String block = new RedisBenchmarkOutputRenderer().humanBlock("PING_INLINE", 50, 3, 64, true, 1234.5, 0.321, 0.654, 0.987);

        Assert.assertTrue(block.contains("====== PING_INLINE ======"));
        Assert.assertTrue(block.contains("50 requests completed"));
        Assert.assertTrue(block.contains("3 parallel clients"));
        Assert.assertTrue(block.contains("64 bytes payload"));
        Assert.assertTrue(block.contains("keep alive: 1"));
        Assert.assertTrue(block.contains("throughput summary: 1234.50 requests per second"));
        Assert.assertTrue(block.contains("p50"));
        Assert.assertTrue(block.contains("p95"));
        Assert.assertTrue(block.contains("p99"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=RedisBenchmarkCommandTemplateTest,RedisBenchmarkRunnerTest,RedisBenchmarkOutputRendererTest \
  test
```

Expected: FAIL with missing template, runner, renderer, or scripted-server symbols.

- [ ] **Step 3: Write the minimal implementation**

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplate.java
package yier.bubu.redis.app.bench.compat;

import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

public final class RedisBenchmarkCommandTemplate {
    private final byte[] inlineBytes;
    private final List<byte[]> argv;

    private RedisBenchmarkCommandTemplate(byte[] inlineBytes, List<byte[]> argv) {
        this.inlineBytes = inlineBytes;
        this.argv = argv;
    }

    public static RedisBenchmarkCommandTemplate inline(byte[] inlineBytes) {
        return new RedisBenchmarkCommandTemplate(inlineBytes.clone(), List.of());
    }

    public static RedisBenchmarkCommandTemplate respArray(List<byte[]> argv) {
        return new RedisBenchmarkCommandTemplate(null, List.copyOf(argv));
    }

    public void writeTo(OutputStream out, SplittableRandom random, int randomBound) throws IOException {
        if (inlineBytes != null) {
            out.write(inlineBytes);
            return;
        }
        List<byte[]> expanded = new ArrayList<>(argv.size());
        for (byte[] part : argv) {
            String text = new String(part, StandardCharsets.UTF_8);
            if (randomBound > 0 && text.contains("__rand_int__")) {
                String replacement = Long.toString(random.nextLong(randomBound));
                expanded.add(text.replace("__rand_int__", replacement).getBytes(StandardCharsets.UTF_8));
            } else {
                expanded.add(part);
            }
        }
        RespClientCodec.writeCommand(out, expanded);
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java
package yier.bubu.redis.app.bench.compat;

public record RedisBenchmarkCase(
        String id,
        String title,
        RedisBenchmarkCommandTemplate template
) {
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRenderer.java
package yier.bubu.redis.app.bench.compat;

import java.util.Locale;

public final class RedisBenchmarkOutputRenderer {
    public String csvHeader() {
        return "\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\"";
    }

    public String quietLine(String title, double rps, double p50) {
        return String.format(Locale.ROOT, "%s: %.2f requests per second, p50=%.3f msec", title, rps, p50);
    }

    public String humanBlock(String title, int requests, int clients, int dataSize, boolean keepAlive,
                             double rps, double p50, double p95, double p99) {
        return String.format(Locale.ROOT,
                "====== %s ======%n" +
                        "  %d requests completed%n" +
                        "  %d parallel clients%n" +
                        "  %d bytes payload%n" +
                        "  keep alive: %d%n%n" +
                        "Summary:%n" +
                        "  throughput summary: %.2f requests per second%n" +
                        "  latency summary (msec):%n" +
                        "        p50      p95      p99%n" +
                        "    %7.3f %7.3f %7.3f%n",
                title, requests, clients, dataSize, keepAlive ? 1 : 0, rps, p50, p95, p99);
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java
package yier.bubu.redis.app.bench.compat;

import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SplittableRandom;

public final class RedisBenchmarkRunner {
    public int run(RedisBenchmarkCompatCommand command, PrintStream out, PrintStream err) throws Exception {
        if (command.help()) {
            out.println("Usage: yierdis-benchmark [OPTIONS] [COMMAND ARGS...]");
            return 0;
        }
        if (command.version()) {
            out.println("yierdis-benchmark");
            return 0;
        }

        RedisBenchmarkCase testCase = command.rawMode()
                ? new RedisBenchmarkCase(
                        command.rawCommandArgs().get(0).toUpperCase(),
                        String.join(" ", command.rawCommandArgs()),
                        RedisBenchmarkCommandTemplate.respArray(
                                command.rawCommandArgs().stream()
                                        .map(s -> s.getBytes(StandardCharsets.UTF_8))
                                        .toList()
                        )
                )
                : new RedisBenchmarkCase("ping_inline", "PING_INLINE",
                RedisBenchmarkCommandTemplate.inline("PING\r\n".getBytes(StandardCharsets.US_ASCII)));

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(command.host(), command.port()), 1_000);
            socket.setSoTimeout(1_000);
            try (BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
                 BufferedInputStream bis = new BufferedInputStream(socket.getInputStream())) {
                testCase.template().writeTo(bos, new SplittableRandom(1L), command.randomKeyspaceLength());
                bos.flush();
                RespClientCodec.RespReply reply = RespClientCodec.readReply(bis, 1 << 20);
                if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
                    err.println(reply.text());
                    err.println("aborted after 0/" + command.requests() + " requests");
                    return 1;
                }
            }
        }

        if (command.quiet()) {
            out.println(new RedisBenchmarkOutputRenderer().quietLine(testCase.title(), 1.0, 0.0));
        } else if (command.csv()) {
            RedisBenchmarkOutputRenderer renderer = new RedisBenchmarkOutputRenderer();
            out.println(renderer.csvHeader());
            out.println("\"" + testCase.title() + "\",\"1.00\",\"0.000\",\"0.000\",\"0.000\",\"0.000\",\"0.000\",\"0.000\"");
        } else {
            out.println(new RedisBenchmarkOutputRenderer().humanBlock(
                    testCase.title(), command.requests(), command.clients(), command.dataSize(), command.keepAlive(),
                    1.0, 0.0, 0.0, 0.0
            ));
        }
        return 0;
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java
RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(args);
return new yier.bubu.redis.app.bench.compat.RedisBenchmarkRunner().run(command, System.out, System.err);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=RedisBenchmarkCommandTemplateTest,RedisBenchmarkRunnerTest,RedisBenchmarkOutputRendererTest \
  test
```

Expected: PASS with command-template, runner, and output tests green.

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplate.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRenderer.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkScriptedServer.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCommandTemplateTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunnerTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkOutputRendererTest.java
git commit -m "feat: add redis-benchmark compatibility runner core"
```

## Task 3: Add The Redis-Style Default Catalog And Fixture Preparation

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkPreparationPlan.java`
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java`

**Interfaces:**
- Consumes: `RedisBenchmarkCompatCommand`, `RedisBenchmarkCase`, `RedisBenchmarkCommandTemplate`
- Produces: `public List<RedisBenchmarkCase> RedisBenchmarkCatalog.defaultCases(RedisBenchmarkCompatCommand command)`
- Produces: `public List<RedisBenchmarkCase> RedisBenchmarkCatalog.selectCases(RedisBenchmarkCompatCommand command)`
- Produces: `public void RedisBenchmarkPreparationPlan.run(String host, int port) throws Exception`

- [ ] **Step 1: Write the failing tests**

```java
// yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java
package yier.bubu.redis.app.bench.compat;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RedisBenchmarkCatalogTest {
    @Test
    public void defaultCatalogStartsWithRedisStyleDefaultCases() {
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(new String[0]);
        List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().defaultCases(command);

        Assert.assertEquals("PING_INLINE", cases.get(0).title());
        Assert.assertEquals("PING_BULK", cases.get(1).title());
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("SET")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("GET")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("INCR")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("LPUSH")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("RPUSH")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("LPOP")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("RPOP")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("SADD")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("HSET")));
        Assert.assertTrue(cases.stream().anyMatch(c -> c.title().equals("ZADD")));
    }

    @Test
    public void rawModeStillIgnoresSelectedTests() {
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(
                new String[]{"-t", "set,get", "ping"}
        );

        List<RedisBenchmarkCase> cases = new RedisBenchmarkCatalog().selectCases(command);

        Assert.assertEquals(1, cases.size());
        Assert.assertEquals("ping", cases.get(0).title());
    }
}
```

```java
// Add one fixture-backed scripted-server test to RedisBenchmarkRunnerTest.java
@Test
public void getCasePrefillsKeysBeforeMeasuredReads() throws Exception {
    try (RedisBenchmarkScriptedServer server = RedisBenchmarkScriptedServer.captureCommandsServer()) {
        RedisBenchmarkCompatCommand command = new RedisBenchmarkCompatParser().parse(
                new String[]{"-h", "127.0.0.1", "-p", Integer.toString(server.port()), "-t", "get", "-n", "2", "-c", "1", "-r", "2"}
        );

        int exitCode = new RedisBenchmarkRunner().run(command, System.out, System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(server.commands().contains("SET"));
        Assert.assertTrue(server.commands().contains("GET"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkRunnerTest \
  test
```

Expected: FAIL because the runner still hardcodes `PING_INLINE` and there is no catalog or preparation plan yet.

- [ ] **Step 3: Write the minimal implementation**

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkPreparationPlan.java
package yier.bubu.redis.app.bench.compat;

import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public record RedisBenchmarkPreparationPlan(List<List<byte[]>> commands) {
    public void run(String host, int port) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1_000);
            socket.setSoTimeout(1_000);
            for (List<byte[]> command : commands) {
                RespClientCodec.writeCommand(socket.getOutputStream(), command);
                socket.getOutputStream().flush();
                RespClientCodec.readReply(socket.getInputStream(), 1 << 20);
            }
        }
    }

    public static RedisBenchmarkPreparationPlan none() {
        return new RedisBenchmarkPreparationPlan(List.of());
    }

    public boolean empty() {
        return commands.isEmpty();
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java
package yier.bubu.redis.app.bench.compat;

public record RedisBenchmarkCase(
        String id,
        String title,
        RedisBenchmarkCommandTemplate template,
        RedisBenchmarkPreparationPlan preparationPlan
) {
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java
package yier.bubu.redis.app.bench.compat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class RedisBenchmarkCatalog {
    public List<RedisBenchmarkCase> selectCases(RedisBenchmarkCompatCommand command) {
        if (command.rawMode()) {
            return List.of(new RedisBenchmarkCase(
                    "raw",
                    String.join(" ", command.rawCommandArgs()),
                    RedisBenchmarkCommandTemplate.respArray(command.rawCommandArgs().stream()
                            .map(s -> s.getBytes(StandardCharsets.UTF_8))
                            .toList()),
                    RedisBenchmarkPreparationPlan.none()
            ));
        }
        if (!command.selectedTests().isEmpty()) {
            List<RedisBenchmarkCase> all = defaultCases(command);
            return all.stream().filter(c -> command.selectedTests().contains(c.id())).toList();
        }
        return defaultCases(command);
    }

    public List<RedisBenchmarkCase> defaultCases(RedisBenchmarkCompatCommand command) {
        List<RedisBenchmarkCase> cases = new ArrayList<>();
        cases.add(new RedisBenchmarkCase("ping_inline", "PING_INLINE",
                RedisBenchmarkCommandTemplate.inline("PING\r\n".getBytes(StandardCharsets.US_ASCII)),
                RedisBenchmarkPreparationPlan.none()));
        cases.add(new RedisBenchmarkCase("ping_bulk", "PING_BULK",
                RedisBenchmarkCommandTemplate.respArray(List.of(b("PING"), payload(command.dataSize()))),
                RedisBenchmarkPreparationPlan.none()));
        cases.add(simple("set", "SET", List.of(b("SET"), b("key:__rand_int__"), payload(command.dataSize()))));
        cases.add(new RedisBenchmarkCase("get", "GET",
                RedisBenchmarkCommandTemplate.respArray(List.of(b("GET"), b("key:__rand_int__"))),
                new RedisBenchmarkPreparationPlan(List.of(List.of(b("SET"), b("key:0"), payload(command.dataSize()))))));
        cases.add(simple("incr", "INCR", List.of(b("INCR"), b("counter:__rand_int__"))));
        cases.add(simple("lpush", "LPUSH", List.of(b("LPUSH"), b("list"), payload(command.dataSize()))));
        cases.add(simple("rpush", "RPUSH", List.of(b("RPUSH"), b("list"), payload(command.dataSize()))));
        cases.add(simple("lpop", "LPOP", List.of(b("LPOP"), b("list"))));
        cases.add(simple("rpop", "RPOP", List.of(b("RPOP"), b("list"))));
        cases.add(simple("sadd", "SADD", List.of(b("SADD"), b("set"), payload(command.dataSize()))));
        cases.add(simple("hset", "HSET", List.of(b("HSET"), b("hash"), b("field"), payload(command.dataSize()))));
        cases.add(simple("zadd", "ZADD", List.of(b("ZADD"), b("zset"), b("1"), payload(command.dataSize()))));
        return cases;
    }

    private static RedisBenchmarkCase simple(String id, String title, List<byte[]> argv) {
        return new RedisBenchmarkCase(id, title, RedisBenchmarkCommandTemplate.respArray(argv), RedisBenchmarkPreparationPlan.none());
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] payload(int bytes) {
        return "x".repeat(Math.max(1, bytes)).getBytes(StandardCharsets.US_ASCII);
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java
RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
for (RedisBenchmarkCase testCase : catalog.selectCases(command)) {
    if (!testCase.preparationPlan().empty()) {
        testCase.preparationPlan().run(command.host(), command.port());
    }
    // existing measured-run logic here
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=RedisBenchmarkCatalogTest,RedisBenchmarkRunnerTest \
  test
```

Expected: PASS with default catalog coverage and fixture-backed `GET` behavior green.

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCase.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkPreparationPlan.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunner.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkRunnerTest.java
git commit -m "feat: add redis default benchmark catalog"
```

## Task 4: Cover All Supported Commands And Add The Coverage Guard

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCaseClassification.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java`
- Modify: `yierdis-benchmark/pom.xml`
- Modify: `yierdis-tests/yierdis-integration-tests/pom.xml`
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/BenchmarkCatalogCoverageGuardTest.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java`

**Interfaces:**
- Consumes: `RedisBenchmarkCatalog`
- Produces: `public java.util.Map<String, RedisBenchmarkCaseClassification> RedisBenchmarkCatalog.commandCoverage()`
- Produces: `public boolean RedisBenchmarkCaseClassification.builtIn()`
- Produces: `public boolean RedisBenchmarkCaseClassification.rawTemplateOnly()`
- Produces: `public String RedisBenchmarkCaseClassification.reason()`

- [ ] **Step 1: Write the failing tests**

```java
// Extend RedisBenchmarkCatalogTest.java
@Test
public void catalogExportsCoverageForBuiltInOptionalAndRawTemplateOnlyCommands() {
    java.util.Map<String, RedisBenchmarkCaseClassification> coverage = new RedisBenchmarkCatalog().commandCoverage();

    Assert.assertTrue(coverage.get("PING").builtIn());
    Assert.assertTrue(coverage.get("APPEND").builtIn());
    Assert.assertTrue(coverage.get("PFADD").builtIn());
    Assert.assertTrue(coverage.get("SCAN").builtIn());
    Assert.assertTrue(coverage.get("COMMAND").rawTemplateOnly() || coverage.get("COMMAND").builtIn());
    Assert.assertNotNull(coverage.get("HELLO"));
}
```

```java
// yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/BenchmarkCatalogCoverageGuardTest.java
package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.compat.RedisBenchmarkCaseClassification;
import yier.bubu.redis.app.bench.compat.RedisBenchmarkCatalog;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class BenchmarkCatalogCoverageGuardTest {
    @Test
    public void everyRegisteredCommandHasBenchmarkCoverageClassification() {
        yier.bubu.redis.testutil.TestDbs.forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            Set<String> registered = new HashSet<>();
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyArray command = (ReplyArray) client.execute(cmd("COMMAND"));
                for (Object item : command.values()) {
                    ReplyArray fields = (ReplyArray) item;
                    registered.add(((ReplyBulkString) fields.values().get(0)).asString().toUpperCase());
                }
            }

            Map<String, RedisBenchmarkCaseClassification> coverage = new RedisBenchmarkCatalog().commandCoverage();
            for (String name : registered) {
                Assert.assertTrue("missing benchmark coverage for " + name, coverage.containsKey(name));
            }
        });
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -Dtest=RedisBenchmarkCatalogTest test

JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=BenchmarkCatalogCoverageGuardTest \
  test
```

Expected: FAIL because there is no exported command-coverage map and no integration-test dependency on `yierdis-benchmark`.

- [ ] **Step 3: Write the minimal implementation**

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCaseClassification.java
package yier.bubu.redis.app.bench.compat;

public record RedisBenchmarkCaseClassification(
        boolean builtIn,
        boolean defaultCase,
        boolean rawTemplateOnly,
        String reason
) {
    public static RedisBenchmarkCaseClassification defaultBuiltIn() {
        return new RedisBenchmarkCaseClassification(true, true, false, "");
    }

    public static RedisBenchmarkCaseClassification optionalBuiltIn() {
        return new RedisBenchmarkCaseClassification(true, false, false, "");
    }

    public static RedisBenchmarkCaseClassification rawOnly(String reason) {
        return new RedisBenchmarkCaseClassification(false, false, true, reason);
    }
}
```

```java
// Add to RedisBenchmarkCatalog.java
public java.util.Map<String, RedisBenchmarkCaseClassification> commandCoverage() {
    java.util.Map<String, RedisBenchmarkCaseClassification> coverage = new java.util.LinkedHashMap<>();
    coverage.put("PING", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("ECHO", RedisBenchmarkCaseClassification.rawOnly("raw-template mode is sufficient"));
    coverage.put("SET", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("GET", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("STRLEN", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("APPEND", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SETBIT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("GETBIT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("BITCOUNT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("INCR", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("DECR", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("LPUSH", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("RPUSH", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("LRANGE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("LPOP", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("RPOP", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("HSET", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("HGET", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("HGETALL", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("HLEN", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("HDEL", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SADD", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("SREM", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SMEMBERS", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SISMEMBER", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SCARD", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZADD", RedisBenchmarkCaseClassification.defaultBuiltIn());
    coverage.put("ZRANGE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZREVRANGE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZRANGEBYSCORE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZREVRANGEBYSCORE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZREMRANGEBYSCORE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZREMRANGEBYRANK", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("ZREM", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PFADD", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PFCOUNT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PFMERGE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("TYPE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("MEMORY", RedisBenchmarkCaseClassification.rawOnly("management output is better covered by template mode"));
    coverage.put("OBJECT", RedisBenchmarkCaseClassification.rawOnly("management output is better covered by template mode"));
    coverage.put("KEYS", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("SCAN", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("DEL", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("EXISTS", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("EXPIRE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PEXPIRE", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("EXPIREAT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PEXPIREAT", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PERSIST", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("TTL", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("PTTL", RedisBenchmarkCaseClassification.optionalBuiltIn());
    coverage.put("COMMAND", RedisBenchmarkCaseClassification.rawOnly("metadata command does not belong in the default set"));
    coverage.put("SELECT", RedisBenchmarkCaseClassification.rawOnly("covered as connection prefix and raw template"));
    coverage.put("QUIT", RedisBenchmarkCaseClassification.rawOnly("connection-closing semantics are template-only"));
    coverage.put("CLIENT", RedisBenchmarkCaseClassification.rawOnly("metadata command does not belong in the default set"));
    coverage.put("AUTH", RedisBenchmarkCaseClassification.rawOnly("connection prefix behavior only"));
    coverage.put("FLUSHDB", RedisBenchmarkCaseClassification.rawOnly("destructive command kept out of built-in catalog"));
    coverage.put("MULTI", RedisBenchmarkCaseClassification.rawOnly("transaction semantics are template-only"));
    coverage.put("DISCARD", RedisBenchmarkCaseClassification.rawOnly("transaction semantics are template-only"));
    coverage.put("EXEC", RedisBenchmarkCaseClassification.rawOnly("transaction semantics are template-only"));
    coverage.put("HELLO", RedisBenchmarkCaseClassification.rawOnly("handshake command does not belong in the default set"));
    coverage.put("INFO", RedisBenchmarkCaseClassification.rawOnly("management output is template-only"));
    coverage.put("STATS", RedisBenchmarkCaseClassification.rawOnly("management output is template-only"));
    return coverage;
}
```

```xml
<!-- yierdis-tests/yierdis-integration-tests/pom.xml -->
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-benchmark</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -Dtest=RedisBenchmarkCatalogTest test

JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=BenchmarkCatalogCoverageGuardTest \
  test
```

Expected: PASS with benchmark-module catalog tests green and the integration guard confirming every registered command has a benchmark classification.

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-benchmark/pom.xml \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCaseClassification.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalog.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/compat/RedisBenchmarkCatalogTest.java \
  yierdis-tests/yierdis-integration-tests/pom.xml \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/BenchmarkCatalogCoverageGuardTest.java
git commit -m "feat: add benchmark catalog coverage guard"
```

## Task 5: Preserve Advanced Modes And Migrate Scripts And Docs

**Files:**
- Create: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/LegacyBenchModes.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSuiteEntrypointTest.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchScriptContractTest.java`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java`
- Modify: `scripts/bench.sh`
- Modify: `scripts/smoke.sh`
- Modify: `docs/project-docs/client-and-bench-internals.md`

**Interfaces:**
- Consumes: existing suite/comparison/native-eval logic inside `YierdisBench`
- Produces: `public int LegacyBenchModes.runSuite(String[] args) throws Exception`
- Produces: `public int LegacyBenchModes.runComparison(String[] args) throws Exception`
- Produces: `public int LegacyBenchModes.runNativeEval(String[] args) throws Exception`

- [ ] **Step 1: Write the failing tests**

```java
// Extend YierdisBenchSuiteEntrypointTest.java
@Test
public void explicitSuiteTokenStillReportsSuiteSpecificValidation() throws Exception {
    Captured captured = captureErr(() -> YierdisBench.main(new String[]{"suite"}));
    Assert.assertTrue(captured.err(), captured.err().contains("suite requires currentServerJar"));
}

@Test
public void explicitNativeEvalTokenStillRoutesToLegacyMode() throws Exception {
    Captured captured = captureErr(() -> YierdisBench.main(new String[]{"native-eval"}));
    Assert.assertTrue(captured.err(), captured.err().contains("native-eval"));
}
```

```java
// Extend BenchScriptContractTest.java
@Test
public void benchScriptStartsServerItselfAndInvokesCompatFlags() throws IOException {
    String script = Files.readString(findRepoRoot().resolve("scripts/bench.sh"));

    Assert.assertTrue(script.contains("java -jar \"$server_jar\""));
    Assert.assertTrue(script.contains("-h \"$HOST\""));
    Assert.assertTrue(script.contains("-p \"$PORT\""));
    Assert.assertTrue(script.contains("-n \"$REQUESTS\""));
    Assert.assertTrue(script.contains("-c \"$CLIENTS\""));
    Assert.assertTrue(script.contains("-P \"$PIPELINE\""));
    Assert.assertTrue(script.contains("-d \"$DATA_SIZE\""));
    Assert.assertTrue(script.contains("-r \"$KEYSPACE\""));
}
```

```java
// Extend SmokeScriptContractTest.java
@Test
public void smokeScriptBenchStepUsesCompatCliFlags() throws IOException {
    String script = Files.readString(findRepoRoot().resolve("scripts/smoke.sh"));

    Assert.assertTrue(script.contains("-h \"$HOST\""));
    Assert.assertTrue(script.contains("-p \"$PORT\""));
    Assert.assertTrue(script.contains("-n \"$REQUESTS\""));
    Assert.assertTrue(script.contains("-c \"$CLIENTS\""));
    Assert.assertTrue(script.contains("-P \"$PIPELINE\""));
    Assert.assertTrue(script.contains("-d \"$DATA_SIZE\""));
    Assert.assertTrue(script.contains("-r \"$KEYSPACE\""));
    Assert.assertTrue(script.contains("-t ping_inline,ping_bulk,set,get"));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=YierdisBenchSuiteEntrypointTest,BenchScriptContractTest,SmokeScriptContractTest \
  test
```

Expected: FAIL because advanced modes are still stubbed and the scripts still use the old Yierdis-specific benchmark CLI.

- [ ] **Step 3: Write the minimal implementation**

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/LegacyBenchModes.java
package yier.bubu.redis.app.bench;

public final class LegacyBenchModes {
    public int runSuite(String[] args) throws Exception {
        YierdisBench.runLegacySuiteMode(args);
        return 0;
    }

    public int runComparison(String[] args) throws Exception {
        YierdisBench.runLegacyComparisonMode(args);
        return 0;
    }

    public int runNativeEval(String[] args) throws Exception {
        YierdisBench.runLegacyNativeEvalMode(args);
        return 0;
    }
}
```

```java
// yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java
private final LegacyBenchModes legacy = new LegacyBenchModes();

if ("suite".equals(mode)) {
    return legacy.runSuite(tail);
}
if ("compare".equals(mode)) {
    return legacy.runComparison(tail);
}
if ("native-eval".equals(mode)) {
    return legacy.runNativeEval(tail);
}
```

```bash
# scripts/bench.sh
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16379}"
REQUESTS="${REQUESTS:-100000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-1}"
DATA_SIZE="${DATA_SIZE:-3}"
KEYSPACE="${KEYSPACE:-10000}"
TESTS="${TESTS:-}"
SERVER_LOG="${SERVER_LOG:-$ROOT_DIR/.tmp-bench-server.log}"

java -jar "$server_jar" --port "$PORT" "${server_args[@]}" >"$SERVER_LOG" 2>&1 &
server_pid="$!"

java -jar "$bench_jar" \
  -h "$HOST" \
  -p "$PORT" \
  -n "$REQUESTS" \
  -c "$CLIENTS" \
  -P "$PIPELINE" \
  -d "$DATA_SIZE" \
  -r "$KEYSPACE" \
  ${TESTS:+-t "$TESTS"} \
  $BENCH_ARGS_EXTRA
```

```bash
# scripts/smoke.sh
java -jar "$bench_jar" \
  -h "$HOST" \
  -p "$PORT" \
  -n "$REQUESTS" \
  -c "$CLIENTS" \
  -P "$PIPELINE" \
  -d "$DATA_SIZE" \
  -r "$KEYSPACE" \
  -t ping_inline,ping_bulk,set,get \
  -q
```

```markdown
<!-- docs/project-docs/client-and-bench-internals.md -->
`yierdis-benchmark` now has two entry styles:

- default compatibility mode: `redis-benchmark`-style, connect-only, real RESP/TCP
- explicit advanced modes: `suite`, `compare`, `native-eval`

The default path does not auto-start a server. Local auto-start workflows live
in `scripts/bench.sh`.
```

- [ ] **Step 4: Run the tests and acceptance checks**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark \
  -Dtest=YierdisBenchSuiteEntrypointTest,BenchScriptContractTest,SmokeScriptContractTest \
  test

JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -DskipTests package

JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
./scripts/smoke.sh
```

Expected:

- benchmark-module tests PASS
- `package` succeeds
- `scripts/smoke.sh` starts a real server, runs the compat benchmark against it, and exits successfully

- [ ] **Step 5: Commit**

```bash
git add \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchmarkEntrypointRouter.java \
  yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/LegacyBenchModes.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSuiteEntrypointTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchScriptContractTest.java \
  yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java \
  scripts/bench.sh \
  scripts/smoke.sh \
  docs/project-docs/client-and-bench-internals.md
git commit -m "refactor: preserve advanced modes and migrate bench scripts"
```
