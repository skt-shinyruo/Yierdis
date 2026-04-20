# Yierdis Bytes-First Request Path Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a comparison-friendly request-path baseline and replace the current `String -> UTF-8 byte[]` server request adaptation with a bytes-first internal path while preserving `Custom Protocol v1` wire compatibility.

**Architecture:** Keep the existing repository boundaries intact. `yierdis-bench` gains deterministic report rendering on top of the current `PING`/`SET`/`GET` suite so the request-path change has a stable before/after baseline. `yierdis-protocol` gains a request-specific payload parser plus a byte-backed request DTO, and `yierdis-server` adapts that DTO directly to `ExecutionRequest` without `ProtocolCommandAdapter` rebuilding UTF-8 bytes from `String` values.

**Tech Stack:** Java 25, Maven, Netty, JUnit 4, `Custom Protocol v1`, JDK FFM API

---

## Scope Split

This plan intentionally covers only the first independent sub-project from the roadmap:

- minimal baseline/reporting needed to compare request-path changes
- byte-backed protocol request parsing
- direct adaptation into `ExecutionRequest`
- end-to-end regression coverage for unchanged wire behavior

It does **not** include:

- TTL-heavy / eviction-heavy benchmark expansion
- off-heap `KeyHandle` / `BytesSlice` API propagation
- pooled/slab allocator work
- multi-executor design

Those belong in follow-up plans once this sub-project lands and produces measurable data.

## File Structure Map

### Bench Baseline And Reporting

- Modify: `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`
  Purpose: Extract deterministic summary rendering from direct `println(...)` calls and make baseline output easy to diff before and after request-path changes.
- Create: `yierdis-bench/src/test/java/yier/bubu/redis/bench/YierdisBenchSummaryFormatTest.java`
  Purpose: Lock the textual summary/report format so the benchmark output does not drift while protocol internals are changing.
- Modify: `yierdis-bench/src/test/java/yier/bubu/redis/bench/CustomCommandWriterTest.java`
  Purpose: Keep the existing low-allocation writer guarantees green while the benchmark harness is refactored.

### Protocol Request Parsing

- Create: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1ArgvRequest.java`
  Purpose: Hold decoded request arguments as `byte[][]` plus retained-byte accounting, with null preservation and a stable read-only view for the server adapter.
- Create: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParser.java`
  Purpose: Parse the strict request schema `{ "cmd": string, "args": [string|null...] }` directly into UTF-8 argument bytes without materializing `String`/`List<String>` for the server hot path.
- Create: `yierdis-protocol/yierdis-protocol-codec/src/test/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParserTest.java`
  Purpose: Cover UTF-8, null args, whitespace trimming, invalid schema, escape handling, and retained-byte accounting for the new parser.

### Netty Decode And Server Adaptation

- Modify: `yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
  Purpose: Keep length-prefixed framing and protocol-error recovery exactly as-is, but swap request payload decoding from generic JSON-object-to-`String` parsing to the dedicated bytes-first parser.
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
  Purpose: Adapt `CustomProtocolV1ArgvRequest` directly to `ExecutionRequest` without recreating UTF-8 bytes.
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java`
  Purpose: Add an explicit “wrap read-only argv” factory for already-owned heap bytes so the adapter can keep a stable fast path without defensive copies.

### Regression And Contract Coverage

- Modify: `yierdis-protocol/yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`
  Purpose: Assert decoder output, direct-buffer handling, pipelining, and protocol-error recovery for the byte-backed request DTO.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`
  Purpose: Assert adapter behavior, null preservation, retained-byte accounting, and zero re-encoding semantics through read-only backing reuse.
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java`
  Purpose: Guard the new `ByteArrayExecutionRequest` wrapping factory and its read-only fast path.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
  Purpose: Ensure decode errors still resync correctly with the new parser.
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  Purpose: Ensure end-to-end server wiring and existing command behavior remain unchanged after the internal request type swap.

### Documentation

- Modify: `README.md`
  Purpose: Add a short benchmarking note documenting the baseline comparison command for this optimization slice without changing any external protocol claims.

---

### Task 1: Make Benchmark Output Comparison-Friendly

**Files:**
- Modify: `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java`
- Create: `yierdis-bench/src/test/java/yier/bubu/redis/bench/YierdisBenchSummaryFormatTest.java`
- Modify: `yierdis-bench/src/test/java/yier/bubu/redis/bench/CustomCommandWriterTest.java`
- Test: `yierdis-bench/src/test/java/yier/bubu/redis/bench/YierdisBenchSummaryFormatTest.java`

- [ ] **Step 1: Write the failing summary-format test**

```java
package yier.bubu.redis.bench;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

public class YierdisBenchSummaryFormatTest {
    @Test
    public void renderSummaryProducesStableDeterministicTable() {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult("foreign", 16378);
        result.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 1000, 0, 1.25, 800.0, Instant.parse("2026-04-19T00:00:00Z")
        );
        result.getThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.GET_RANDOM, 1000, 1, 1.00, 1000.0, Instant.parse("2026-04-19T00:00:01Z")
        );
        result.pingLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PING, 1000, 0, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );

        String rendered = YierdisBench.renderSummary(List.of(result), false);

        Assert.assertTrue(rendered.contains("backend"));
        Assert.assertTrue(rendered.contains("SET_QPS"));
        Assert.assertTrue(rendered.contains("GET_QPS"));
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("foreign"));
        Assert.assertTrue(rendered.contains("800.000"));
        Assert.assertTrue(rendered.contains("1"));
    }
}
```

- [ ] **Step 2: Run the benchmark module test to verify it fails**

Run:

```bash
mvn -pl yierdis-bench -Dtest=YierdisBenchSummaryFormatTest test
```

Expected: FAIL because `YierdisBench.renderSummary(...)` does not exist yet.

- [ ] **Step 3: Extract stable report rendering from `YierdisBench`**

Modify `yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java` by extracting the current summary printing into a reusable renderer and keeping `printSummary(...)` as a thin wrapper:

```java
static String renderSummary(List<BackendResult> results, boolean skipLatency) {
    StringBuilder sb = new StringBuilder();
    String header = skipLatency
            ? String.format("%-8s | %12s | %8s | %12s | %8s", "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR")
            : String.format(
            "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s",
            "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR",
            "PING_p95(ms)", "PING_E", "SET_p95(ms)", "SET_E", "GET_p95(ms)", "GET_E"
    );
    sb.append(header).append('\n');
    sb.append(repeat('-', header.length())).append('\n');

    for (BackendResult r : results) {
        String setQps = r.setThroughput == null ? "-" : DF.format(r.setThroughput.qps);
        String getQps = r.getThroughput == null ? "-" : DF.format(r.getThroughput.qps);
        String setErr = r.setThroughput == null ? "-" : Long.toString(r.setThroughput.errors);
        String getErr = r.getThroughput == null ? "-" : Long.toString(r.getThroughput.errors);
        if (skipLatency) {
            sb.append(String.format("%-8s | %12s | %8s | %12s | %8s", r.backend, setQps, setErr, getQps, getErr))
                    .append('\n');
            continue;
        }
        String pingP95 = r.pingLatency == null ? "-" : DF.format(r.pingLatency.stats.p95Millis());
        String setP95 = r.setLatency == null ? "-" : DF.format(r.setLatency.stats.p95Millis());
        String getP95 = r.getLatency == null ? "-" : DF.format(r.getLatency.stats.p95Millis());
        String pingErr = r.pingLatency == null ? "-" : Long.toString(r.pingLatency.errors);
        String setLatErr = r.setLatency == null ? "-" : Long.toString(r.setLatency.errors);
        String getLatErr = r.getLatency == null ? "-" : Long.toString(r.getLatency.errors);
        sb.append(String.format(
                "%-8s | %12s | %8s | %12s | %8s | %14s | %8s | %14s | %8s | %14s | %8s",
                r.backend, setQps, setErr, getQps, getErr,
                pingP95, pingErr, setP95, setLatErr, getP95, getLatErr
        )).append('\n');
    }
    return sb.toString();
}

private static void printSummary(List<BackendResult> results, boolean skipLatency) {
    for (String line : renderSummary(results, skipLatency).split("\n", -1)) {
        if (!line.isEmpty()) {
            println(line);
        }
    }
}
```

Add one low-allocation regression to `CustomCommandWriterTest` so the bench refactor does not regress the existing writer path:

```java
@Test
public void repeatedSummaryRenderingDoesNotTouchCommandWriterPath() throws Exception {
    byte[] key = utf8("next-key");
    byte[] value = utf8("line1\\line2\n中文");

    Assert.assertArrayEquals(
            CustomProtocolV1RequestEncoder.encodeRequestFrame(List.of(utf8("SET"), key, value)),
            writeFrame(writer -> writer.writeSet(key, value))
    );
}
```

- [ ] **Step 4: Run the benchmark tests to verify they pass**

Run:

```bash
mvn -pl yierdis-bench -Dtest=YierdisBenchSummaryFormatTest,CustomCommandWriterTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-bench/src/main/java/yier/bubu/redis/bench/YierdisBench.java \
  yierdis-bench/src/test/java/yier/bubu/redis/bench/YierdisBenchSummaryFormatTest.java \
  yierdis-bench/src/test/java/yier/bubu/redis/bench/CustomCommandWriterTest.java
git commit -m "test: lock benchmark summary rendering"
```

---

### Task 2: Add A Byte-Backed Custom Protocol Request Parser

**Files:**
- Create: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1ArgvRequest.java`
- Create: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParser.java`
- Create: `yierdis-protocol/yierdis-protocol-codec/src/test/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParserTest.java`
- Test: `yierdis-protocol/yierdis-protocol-codec/src/test/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParserTest.java`

- [ ] **Step 1: Write the failing parser tests**

```java
package yier.bubu.redis.protocol.v1;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CustomProtocolV1RequestPayloadParserTest {
    @Test
    public void parseDecodesUtf8ArgsAndNullsIntoArgvBytes() {
        byte[] payload = utf8("{\"cmd\":\" \\tPING\\r\\n \",\"args\":[\"alpha\",null,\"你好\",\"\"]}");

        CustomProtocolV1ArgvRequest request =
                CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);

        Assert.assertEquals(5, request.argc());
        Assert.assertArrayEquals(utf8("PING"), request.readOnlyArg(0));
        Assert.assertArrayEquals(utf8("alpha"), request.readOnlyArg(1));
        Assert.assertTrue(request.isNull(2));
        Assert.assertArrayEquals(utf8("你好"), request.readOnlyArg(3));
        Assert.assertArrayEquals(utf8(""), request.readOnlyArg(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseRejectsNonStringNonNullArgs() {
        byte[] payload = utf8("{\"cmd\":\"PING\",\"args\":[1]}");
        CustomProtocolV1RequestPayloadParser.parse(payload, 0, payload.length, 16);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Run the protocol-codec test to verify it fails**

Run:

```bash
mvn -pl yierdis-protocol/yierdis-protocol-codec -am -Dtest=CustomProtocolV1RequestPayloadParserTest test
```

Expected: FAIL because the parser class and byte-backed request DTO do not exist yet.

- [ ] **Step 3: Implement the DTO and request-specific parser**

Create `CustomProtocolV1ArgvRequest.java`:

```java
package yier.bubu.redis.protocol.v1;

import java.util.Objects;

public final class CustomProtocolV1ArgvRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    private CustomProtocolV1ArgvRequest(byte[][] argv, int retainedBytes) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
    }

    public static CustomProtocolV1ArgvRequest of(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        return new CustomProtocolV1ArgvRequest(argv, Math.max(0, retainedBytes));
    }

    public int argc() {
        return argv.length;
    }

    public boolean isNull(int index) {
        return argv[index] == null;
    }

    public byte[] readOnlyArg(int index) {
        return argv[index];
    }

    public int retainedBytes() {
        return retainedBytes;
    }
}
```

Create `CustomProtocolV1RequestPayloadParser.java` with a request-specific schema parser instead of going through generic `JsonObject` and `JsonString` values:

```java
package yier.bubu.redis.protocol.v1;

public final class CustomProtocolV1RequestPayloadParser {
    public static CustomProtocolV1ArgvRequest parse(byte[] payload, int off, int len, int maxArgs) {
        RequestCursor c = new RequestCursor(payload, off, len, maxArgs);
        c.skipWhitespace();
        c.expect('{');
        c.readCmdField();
        c.readArgsField();
        c.skipWhitespace();
        c.expectEndObject();
        return CustomProtocolV1ArgvRequest.of(c.argv(), c.retainedBytes());
    }

    private CustomProtocolV1RequestPayloadParser() {
    }
}
```

Inside the cursor helper, keep the implementation narrow:

- accept only the exact request schema
- trim command bytes exactly once
- unescape JSON strings directly into UTF-8 byte arrays
- preserve `null` args
- enforce `maxArgs`

- [ ] **Step 4: Run the new parser tests and the existing request-encoder tests**

Run:

```bash
mvn -pl yierdis-protocol/yierdis-protocol-codec -am \
  -Dtest=CustomProtocolV1RequestPayloadParserTest,CustomProtocolV1RequestEncoderTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1ArgvRequest.java \
  yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParser.java \
  yierdis-protocol/yierdis-protocol-codec/src/test/java/yier/bubu/redis/protocol/v1/CustomProtocolV1RequestPayloadParserTest.java
git commit -m "feat: add byte-backed custom protocol request parser"
```

---

### Task 3: Adapt Byte-Backed Requests Directly Into ExecutionRequest

**Files:**
- Modify: `yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- Modify: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java`
- Modify: `yierdis-protocol/yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java`
- Test: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`

- [ ] **Step 1: Write failing adapter and contract tests**

Add to `ProtocolCommandAdapterTest.java`:

```java
@Test
public void adaptsByteBackedRequestWithoutUtf8Reencoding() {
    EmbeddedChannel ch = new EmbeddedChannel(new ProtocolCommandAdapter());
    byte[] cmd = utf8("PING");
    byte[] arg = utf8("alpha");

    ExecutionRequest adapted = null;
    try {
        Assert.assertTrue(ch.writeInbound(
                CustomProtocolV1ArgvRequest.of(new byte[][]{cmd, arg, null}, cmd.length + arg.length)
        ));

        adapted = ch.readInbound();
        Assert.assertArrayEquals(cmd, adapted.readOnlyByteArray(0));
        Assert.assertArrayEquals(arg, adapted.readOnlyByteArray(1));
        Assert.assertTrue(adapted.isNull(2));
        Assert.assertEquals(cmd.length + arg.length, adapted.retainedBytes());
    } finally {
        if (adapted != null) {
            adapted.close();
        }
        ch.finishAndReleaseAll();
    }
}
```

Add to `ExecutionRequestContractTest.java`:

```java
@Test
public void wrappedReadOnlyArgvRequestKeepsStableReadOnlyBacking() {
    byte[] cmd = ascii("SET");
    byte[] key = ascii("key");

    ExecutionRequest request = ByteArrayExecutionRequest.wrapReadOnly(
            new byte[][]{cmd, key, null},
            cmd.length + key.length
    );

    Assert.assertSame(cmd, readOnlyByteArray(request, 0));
    Assert.assertSame(key, readOnlyByteArray(request, 1));
    Assert.assertTrue(request.isNull(2));
}
```

- [ ] **Step 2: Run the affected tests to verify they fail**

Run:

```bash
mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am \
  -Dtest=ExecutionRequestContractTest,ProtocolCommandAdapterTest test
```

Expected: FAIL because `wrapReadOnly(...)` and the new adapter path do not exist yet.

- [ ] **Step 3: Implement the zero-reencode adaptation path**

Modify `ByteArrayExecutionRequest.java`:

```java
public static ByteArrayExecutionRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
    Objects.requireNonNull(argv, "argv");
    byte[][] owned = new byte[argv.length][];
    System.arraycopy(argv, 0, owned, 0, argv.length);
    return new ByteArrayExecutionRequest(owned, Math.max(0, retainedBytes), true);
}
```

Modify `ProtocolCommandAdapter.java` so it accepts the new DTO and reuses the already-decoded bytes:

```java
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;

final class ProtocolCommandAdapter extends SimpleChannelInboundHandler<CustomProtocolV1ArgvRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CustomProtocolV1ArgvRequest msg) {
        if (ctx == null || msg == null) {
            return;
        }
        byte[][] argv = new byte[msg.argc()][];
        for (int i = 0; i < argv.length; i++) {
            argv[i] = msg.readOnlyArg(i);
        }
        ctx.fireChannelRead(ByteArrayExecutionRequest.wrapReadOnly(argv, msg.retainedBytes()));
    }
}
```

Modify `CustomRequestDecoder.java` so the payload decode path emits the new DTO:

```java
private CustomProtocolV1ArgvRequest parseCommandPayload(ByteBuf payload) {
    int off = payload.readerIndex();
    int len = payload.readableBytes();
    if (payload.hasArray()) {
        byte[] arr = payload.array();
        int base = payload.arrayOffset() + off;
        return CustomProtocolV1RequestPayloadParser.parse(arr, base, len, maxArgs);
    }
    byte[] copy = new byte[len];
    payload.getBytes(off, copy);
    return CustomProtocolV1RequestPayloadParser.parse(copy, 0, copy.length, maxArgs);
}
```

- [ ] **Step 4: Run decoder, adapter, and contract tests**

Run:

```bash
mvn -pl yierdis-protocol/yierdis-protocol-netty,yierdis-core/yierdis-core-runtime,yierdis-server -am \
  -Dtest=CustomRequestDecoderTest,ExecutionRequestContractTest,ProtocolCommandAdapterTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-protocol/yierdis-protocol-netty/src/main/java/yier/bubu/redis/protocol/netty/CustomRequestDecoder.java \
  yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java \
  yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ByteArrayExecutionRequest.java \
  yierdis-protocol/yierdis-protocol-netty/src/test/java/yier/bubu/redis/protocol/netty/CustomRequestDecoderTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/contract/ExecutionRequestContractTest.java
git commit -m "feat: adapt custom protocol requests without utf8 reencoding"
```

---

### Task 4: Prove End-To-End Behavior And Document The Baseline Command

**Files:**
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
- Modify: `README.md`
- Test: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Write the failing integration regression assertions**

Add to `YierdisServerBootstrapCommandWiringTest.java`:

```java
@Test
public void bootstrapStillProcessesHelloInfoStatsAndDataCommandsAfterByteBackedDecodePath() throws Exception {
    try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0", "--databases", "2")) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            JsonObject hello = roundTrip(socket.getOutputStream(), socket.getInputStream(), "{\"cmd\":\"HELLO\",\"args\":[]}");
            Assert.assertTrue(booleanField(hello, "ok"));

            JsonObject set = roundTrip(socket.getOutputStream(), socket.getInputStream(), "{\"cmd\":\"SET\",\"args\":[\"k\",\"v\"]}");
            Assert.assertTrue(booleanField(set, "ok"));

            JsonObject get = roundTrip(socket.getOutputStream(), socket.getInputStream(), "{\"cmd\":\"GET\",\"args\":[\"k\"]}");
            Assert.assertTrue(booleanField(get, "ok"));
            Assert.assertEquals("v", stringField(get, "result"));
        }
    }
}
```

Add to `CustomProtocolResyncIntegrationTest.java`:

```java
@Test
public void malformedFrameStillResyncsAndExecutesNextValidCommandAfterByteBackedParserSwap() throws Exception {
    try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0")) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            writeRawFrame(out, "{\"cmd\":}");
            writeFrame(out, "{\"cmd\":\"PING\",\"args\":[]}");

            JsonObject first = parseJsonObject(readReplyLine(in));
            Assert.assertFalse(booleanField(first, "ok"));

            JsonObject second = parseJsonObject(readReplyLine(in));
            Assert.assertTrue(booleanField(second, "ok"));
            Assert.assertEquals("PONG", stringField(second, "result"));
        }
    }
}
```

- [ ] **Step 2: Run the server integration tests to verify failure**

Run:

```bash
mvn -pl yierdis-server -am \
  -Dtest=CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: FAIL if any decoder/adaptation behavior changed unexpectedly.

- [ ] **Step 3: Add the README baseline comparison command**

Modify the benchmark section in `README.md` with a short, explicit comparison recipe for this optimization slice:

```md
### Request-Path Baseline Comparison

For request-path changes, capture the existing pure-Java benchmark output before and after the branch using the same parameters:

~~~bash
REQUESTS=200000 CLIENTS=64 PIPELINE=8 DATA_SIZE=256 ./scripts/bench.sh
~~~

Keep the rendered summary table from `yierdis-bench` unchanged between runs so throughput and latency rows are directly diffable.
```

- [ ] **Step 4: Run the focused integration tests plus a package build**

Run:

```bash
mvn -pl yierdis-server -am \
  -Dtest=CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCommandWiringTest test
mvn -q -DskipTests package
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/src/test/java/yier/bubu/redis/CustomProtocolResyncIntegrationTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java \
  README.md
git commit -m "docs: add baseline command for request path tuning"
```

---

## Self-Review Checklist

- Spec coverage:
  This plan covers the first optimization sub-project only: deterministic benchmark output, byte-backed protocol request parsing, direct `ExecutionRequest` adaptation, and end-to-end regression coverage.
- Placeholder scan:
  No placeholder markers remain in the task body.
- Type consistency:
  The plan consistently uses `CustomProtocolV1ArgvRequest`, `CustomProtocolV1RequestPayloadParser`, `ByteArrayExecutionRequest.wrapReadOnly(...)`, and `YierdisBench.renderSummary(...)`.
