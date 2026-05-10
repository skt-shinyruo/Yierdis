# Redis Protocol Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Custom Protocol v1 with Redis RESP as the only public server protocol, with RESP2 as the primary compatibility target and negotiated RESP3 basics through `HELLO 3`.

**Architecture:** Add a Netty-free RESP module that owns request frames, reply encoding, and client-side parser helpers. Keep command execution protocol-agnostic by continuing to route through `ExecutionRequest` and `ReplyWriter`, while extending `ReplyWriterFactory` so writers can read per-connection `ServerSession.respVersion()` as an integer and map it inside the RESP module. Replace the server pipeline with RESP handlers, then rewrite CLI/benchmark clients and remove Custom Protocol v1 modules.

**Tech Stack:** Java 25, Maven, Netty 4.1, JUnit 4, existing `BytesSink`, `ExecutionRequest`, `ReplyWriter`, `CommandExecutor`, and Netty `EmbeddedChannel` tests.

---

## File Structure

Create:

- `yierdis-networking/yierdis-networking-resp/pom.xml`: new Netty-free RESP module.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolVersion.java`: RESP2/RESP3 enum.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolLimits.java`: default request/reply limits replacing `ProtocolLimits`.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`: binary-safe argv request DTO.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`: RESP request to `ExecutionRequest`.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`: `ReplyWriter` implementation for RESP2 and RESP3.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java`: session-aware writer factory.
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespClientCodec.java`: small blocking-client encoder/parser helpers for CLI and benchmark.
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplyWriterTest.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespClientCodecTest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespPipelineIntegrationTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/resp/RespBoundaryGuardTest.java`

Modify:

- `pom.xml`: replace custom networking dependencies in dependency management with `yierdis-networking-resp`.
- `yierdis-networking/pom.xml`: replace custom modules with `yierdis-networking-resp`.
- `yierdis-networking/yierdis-networking-netty/pom.xml`: depend on `yierdis-networking-resp` and remove custom dependencies.
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java`: add RESP protocol accessors.
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java`: add session-aware default method.
- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`: store RESP protocol version as integer `2` or `3`.
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`: create writers with session.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`: use `RespReplyWriterFactory`.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`: replace custom handlers with RESP handlers and idle/slow-client handlers.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`: use session-aware writer for direct reject/error replies.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`: make `HELLO` negotiate RESP2/RESP3 and support `SETNAME`.
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`: add `CLIENT` and `AUTH`.
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-cli/pom.xml`: depend on RESP module, not custom module.
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java`: rewrite as RESP2 client.
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`: print RESP replies instead of JSON envelopes.
- `yierdis-benchmark/pom.xml`: depend on RESP module, not custom module.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`: encode/inspect RESP replies.
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`: use `RespProtocolLimits`.
- `yierdis-tests/yierdis-architecture-tests/pom.xml`
- `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- README and docs that mention Custom Protocol v1.

Delete after replacement:

- `yierdis-networking/yierdis-networking-custom-v1`
- `yierdis-networking/yierdis-networking-custom-v1-execution`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/custom/v1`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/custom/v1`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/custom/v1`
- `yierdis-cli/src/main/java/yier/bubu/redis/app/client/CustomProtocolV1Replies.java`

---

### Task 1: Add RESP Module Skeleton And Limits

**Files:**

- Modify: `pom.xml`
- Modify: `yierdis-networking/pom.xml`
- Create: `yierdis-networking/yierdis-networking-resp/pom.xml`
- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolVersion.java`
- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespProtocolLimits.java`
- Test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespProtocolLimitsTest.java`

- [ ] **Step 1: Write the failing limits test**

```java
package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

public class RespProtocolLimitsTest {
    @Test
    public void defaultsArePositiveAndRedisProtocolOriented() {
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_BULK_BYTES > 0);
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_ARGS > 0);
        Assert.assertTrue(RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES > 0);
        Assert.assertEquals(512 * 1024 * 1024, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
        Assert.assertEquals(1024 * 1024, RespProtocolLimits.DEFAULT_MAX_INLINE_BYTES);
    }
}
```

- [ ] **Step 2: Run the new module test and verify it fails**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespProtocolLimitsTest`

Expected: FAIL because the module and classes do not exist.

- [ ] **Step 3: Add the module POM**

Create `yierdis-networking/yierdis-networking-resp/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-networking</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>yierdis-networking-resp</artifactId>
    <packaging>jar</packaging>

    <name>yierdis-networking-resp</name>
    <description>RESP wire protocol model, codecs, and reply writer.</description>

    <dependencies>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-common-bytes</artifactId>
        </dependency>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-server-api</artifactId>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Update Maven aggregators**

In `yierdis-networking/pom.xml`, add `yierdis-networking-resp` before `yierdis-networking-netty` while keeping the custom modules until Task 12 removes their modules and consumers:

```xml
<modules>
    <module>yierdis-networking-custom-v1</module>
    <module>yierdis-networking-custom-v1-execution</module>
    <module>yierdis-networking-resp</module>
    <module>yierdis-networking-netty</module>
</modules>
```

In root `pom.xml`, add dependency management for `yierdis-networking-resp` next to networking modules:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-networking-resp</artifactId>
    <version>${project.version}</version>
</dependency>
```

Leave the custom dependency management entries in place until Task 12 removes their modules and consumers.

- [ ] **Step 5: Add version and limit classes**

Create `RespProtocolVersion.java`:

```java
package yier.bubu.redis.protocol.resp;

public enum RespProtocolVersion {
    RESP2(2),
    RESP3(3);

    private final int wireValue;

    RespProtocolVersion(int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RespProtocolVersion fromWireValue(int value) {
        return switch (value) {
            case 2 -> RESP2;
            case 3 -> RESP3;
            default -> throw new IllegalArgumentException("NOPROTO unsupported protocol version");
        };
    }
}
```

Create `RespProtocolLimits.java`:

```java
package yier.bubu.redis.protocol.resp;

public final class RespProtocolLimits {
    public static final int DEFAULT_MAX_BULK_BYTES = 512 * 1024 * 1024;
    public static final int DEFAULT_MAX_ARGS = 1024 * 1024;
    public static final int DEFAULT_MAX_INLINE_BYTES = 1024 * 1024;

    private RespProtocolLimits() {
    }
}
```

- [ ] **Step 6: Run tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespProtocolLimitsTest`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pom.xml yierdis-networking/pom.xml yierdis-networking/yierdis-networking-resp
git commit -m "feat: add resp networking module"
```

---

### Task 2: Add RESP Request DTO And Execution Adapter

**Files:**

- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`
- Test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`

- [ ] **Step 1: Write failing adapter tests**

```java
package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespExecutionAdapterTest {
    @Test
    public void convertsBinarySafeArgvToExecutionRequest() {
        RespCommandRequest request = RespCommandRequest.copyOf(List.of(
                bytes("SET"),
                bytes("k"),
                new byte[]{0, 1, 2}
        ));

        ExecutionRequest out = RespExecutionAdapter.DEFAULT.toExecutionRequest(request);

        Assert.assertEquals(3, out.argc());
        Assert.assertArrayEquals(bytes("SET"), out.readOnlyByteArray(0));
        Assert.assertArrayEquals(bytes("k"), out.readOnlyByteArray(1));
        Assert.assertArrayEquals(new byte[]{0, 1, 2}, out.readOnlyByteArray(2));
        Assert.assertEquals(6, out.retainedBytes());
    }

    @Test
    public void rejectsNullArgvElement() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                RespCommandRequest.copyOf(java.util.Arrays.asList(bytes("GET"), null))
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Run the failing test**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespExecutionAdapterTest`

Expected: FAIL because `RespCommandRequest` and `RespExecutionAdapter` do not exist.

- [ ] **Step 3: Implement request DTO**

Create `RespCommandRequest.java`:

```java
package yier.bubu.redis.protocol.resp;

import java.util.List;
import java.util.Objects;

public final class RespCommandRequest {
    private final byte[][] argv;
    private final int retainedBytes;

    private RespCommandRequest(byte[][] argv, int retainedBytes) {
        this.argv = argv;
        this.retainedBytes = retainedBytes;
    }

    public static RespCommandRequest copyOf(List<byte[]> args) {
        Objects.requireNonNull(args, "args");
        byte[][] argv = new byte[args.size()][];
        int retainedBytes = 0;
        for (int i = 0; i < args.size(); i++) {
            byte[] arg = args.get(i);
            if (arg == null) {
                throw new IllegalArgumentException("RESP command argv must not contain null bulk strings");
            }
            argv[i] = arg.clone();
            retainedBytes += arg.length;
        }
        return new RespCommandRequest(argv, retainedBytes);
    }

    public static RespCommandRequest wrapReadOnly(byte[][] argv, int retainedBytes) {
        Objects.requireNonNull(argv, "argv");
        byte[][] owned = new byte[argv.length][];
        for (int i = 0; i < argv.length; i++) {
            if (argv[i] == null) {
                throw new IllegalArgumentException("RESP command argv must not contain null bulk strings");
            }
            owned[i] = argv[i];
        }
        return new RespCommandRequest(owned, Math.max(0, retainedBytes));
    }

    public int argc() {
        return argv.length;
    }

    public byte[] readOnlyArg(int index) {
        return argv[index];
    }

    public int retainedBytes() {
        return retainedBytes;
    }
}
```

- [ ] **Step 4: Implement execution adapter**

Create `RespExecutionAdapter.java`:

```java
package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

public final class RespExecutionAdapter {
    public static final RespExecutionAdapter DEFAULT = new RespExecutionAdapter();

    public ExecutionRequest toExecutionRequest(RespCommandRequest request) {
        Objects.requireNonNull(request, "request");
        byte[][] argv = new byte[request.argc()][];
        for (int i = 0; i < argv.length; i++) {
            argv[i] = request.readOnlyArg(i);
        }
        return ByteArrayExecutionRequest.wrapReadOnly(argv, request.retainedBytes());
    }
}
```

- [ ] **Step 5: Run tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespExecutionAdapterTest`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-networking/yierdis-networking-resp
git commit -m "feat: adapt resp commands to execution requests"
```

---

### Task 3: Implement RESP Reply Writer

**Files:**

- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`
- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java`
- Test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplyWriterTest.java`

- [ ] **Step 1: Write failing RESP2/RESP3 writer tests**

```java
package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;

import java.nio.charset.StandardCharsets;

public class RespReplyWriterTest {
    @Test
    public void resp2ScalarsAndErrorsUseRedisWireFormat() {
        Assert.assertEquals("+OK\r\n", write2(w -> w.simpleString("OK")));
        Assert.assertEquals("-ERR wrong\r\n", write2(w -> w.error("wrong")));
        Assert.assertEquals("-WRONGTYPE bad\r\n", write2(w -> w.error("WRONGTYPE bad")));
        Assert.assertEquals(":42\r\n", write2(w -> w.integer(42)));
        Assert.assertEquals("$3\r\nabc\r\n", write2(w -> w.bulkString(bytes("abc"))));
        Assert.assertEquals("$-1\r\n", write2(w -> w.nullValue()));
    }

    @Test
    public void resp2MapsAndBooleansDowngradeToCompatibleTypes() {
        String out = write2(w -> {
            w.mapHeader(2);
            w.bulkString(bytes("server"));
            w.bulkString(bytes("yierdis"));
            w.bulkString(bytes("proto"));
            w.integer(2);
        });
        Assert.assertEquals("*4\r\n$6\r\nserver\r\n$7\r\nyierdis\r\n$5\r\nproto\r\n:2\r\n", out);
        Assert.assertEquals(":1\r\n", write2(w -> w.booleanValue(true)));
        Assert.assertEquals(":0\r\n", write2(w -> w.booleanValue(false)));
    }

    @Test
    public void resp3UsesNativeTypes() {
        Assert.assertEquals("_\r\n", write3(RespReplyWriter::nullValue));
        Assert.assertEquals("#t\r\n", write3(w -> w.booleanValue(true)));
        Assert.assertEquals(",1.5\r\n", write3(w -> w.doubleValue(1.5)));
        String out = write3(w -> {
            w.mapHeader(1);
            w.bulkString(bytes("proto"));
            w.integer(3);
        });
        Assert.assertEquals("%1\r\n$5\r\nproto\r\n:3\r\n", out);
    }

    private static String write2(WriterAction action) {
        return write(RespProtocolVersion.RESP2, action);
    }

    private static String write3(WriterAction action) {
        return write(RespProtocolVersion.RESP3, action);
    }

    private static String write(RespProtocolVersion version, WriterAction action) {
        ByteArraySink sink = new ByteArraySink();
        RespReplyWriter writer = new RespReplyWriter(sink, version);
        action.write(writer);
        return sink.utf8();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private interface WriterAction {
        void write(RespReplyWriter writer);
    }

    private static final class ByteArraySink implements BytesSink {
        private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

        @Override
        public void writeByte(int value) {
            out.write(value);
        }

        @Override
        public void writeBytes(byte[] src, int off, int len) {
            out.write(src, off, len);
        }

        String utf8() {
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
```

- [ ] **Step 2: Run the failing tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespReplyWriterTest`

Expected: FAIL because `RespReplyWriter` does not exist.

- [ ] **Step 3: Implement the writer**

Implement `RespReplyWriter` with these concrete rules:

```java
public final class RespReplyWriter implements ReplyWriter {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private final BytesSink out;
    private final RespProtocolVersion version;
    private boolean closeAfterReplyRequested;

    public RespReplyWriter(BytesSink out, RespProtocolVersion version) {
        this.out = Objects.requireNonNull(out, "out");
        this.version = version == null ? RespProtocolVersion.RESP2 : version;
    }

    @Override
    public void simpleString(String value) {
        writeAsciiLine('+', sanitizeSimple(value));
    }

    @Override
    public void error(String message) {
        writeAsciiLine('-', normalizeError(message));
    }

    @Override
    public void protocolError(String message) {
        error(message == null ? "ERR Protocol error" : message);
        requestCloseAfterReply();
    }

    @Override
    public void integer(long value) {
        writeAsciiLine(':', Long.toString(value));
    }

    @Override
    public void nullValue() {
        if (version == RespProtocolVersion.RESP3) {
            writeAscii("_\r\n");
        } else {
            writeAscii("$-1\r\n");
        }
    }

    @Override
    public void mapHeader(int pairs) {
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('%', Integer.toString(Math.max(0, pairs)));
        } else {
            writeAsciiLine('*', Integer.toString(Math.max(0, pairs) * 2));
        }
    }
}
```

Implement every `ReplyWriter` and `ReplySink` method using the encoding table in the approved design. `ReplySink` requires `bulkString(byte[] data)`, `bulkString(byte[] data, int off, int len)`, `bulkString(BytesSlice slice)`, and `bulkStringLongAscii(long value)`; implement each directly. For `bulkString(BytesSlice slice)`, write `$<slice.length()>\r\n`, call `slice.writeTo(out)`, then write CRLF. Keep error sanitization local: replace `\r` and `\n` with spaces and cap messages at 512 bytes.

- [ ] **Step 4: Implement the factory**

Create `RespReplyWriterFactory.java`:

```java
package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.api.ServerSession;

import java.util.Objects;

public final class RespReplyWriterFactory implements ReplyWriterFactory {
    @Override
    public ReplyWriter newWriter(BytesSink out) {
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), RespProtocolVersion.RESP2);
    }

    @Override
    public ReplyWriter newWriter(ServerSession session, BytesSink out) {
        RespProtocolVersion version = session == null
                ? RespProtocolVersion.RESP2
                : RespProtocolVersion.fromWireValue(session.respVersion());
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), version);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespReplyWriterTest`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-networking/yierdis-networking-resp
git commit -m "feat: encode replies as resp"
```

---

### Task 4: Add Session Protocol State And Writer Factory Plumbing

**Files:**

- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Test: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
- Test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`

- [ ] **Step 1: Add failing session test**

Add to `EngineSessionTest`:

```java
@Test
public void respVersionDefaultsToResp2AndCanSwitchToResp3() {
    EngineSession session = new EngineSession(16, 1024);
    Assert.assertEquals(2, session.respVersion());

    session.setRespVersion(3);
    Assert.assertEquals(3, session.respVersion());

    Assert.assertThrows(IllegalArgumentException.class, () -> session.setRespVersion(4));
}
```

- [ ] **Step 2: Run the failing session test**

Run: `mvn -pl yierdis-server/yierdis-server-core test -Dtest=EngineSessionTest#respVersionDefaultsToResp2AndCanSwitchToResp3`

Expected: FAIL because `ServerSession.respVersion()` does not exist.

- [ ] **Step 3: Extend `ServerSession`**

Add methods:

```java
int respVersion();

void setRespVersion(int version);
```

- [ ] **Step 4: Implement state in `EngineSession`**

Add field:

```java
private int respVersion = 2;
```

Add methods:

```java
@Override
public int respVersion() {
    return respVersion;
}

@Override
public void setRespVersion(int version) {
    if (version != 2 && version != 3) {
        throw new IllegalArgumentException("respVersion must be 2 or 3");
    }
    this.respVersion = version;
}
```

- [ ] **Step 5: Extend `ReplyWriterFactory`**

Change `ReplyWriterFactory` to:

```java
@FunctionalInterface
public interface ReplyWriterFactory {
    ReplyWriter newWriter(BytesSink out);

    default ReplyWriter newWriter(ServerSession session, BytesSink out) {
        return newWriter(out);
    }
}
```

- [ ] **Step 6: Use session-aware writer creation in executor**

In `CommandExecutorExecutionSupport.execute`, replace:

```java
ReplyWriter writer = replyWriterFactory.newWriter(ioAdapter.newReplySink(connection));
```

with:

```java
ReplyWriter writer = replyWriterFactory.newWriter(connection.session(), ioAdapter.newReplySink(connection));
```

In `handleExecutionFailure`, make the same replacement.

- [ ] **Step 7: Run focused tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-core test -Dtest=EngineSessionTest
mvn -pl yierdis-server/yierdis-server-executor test -Dtest=CommandExecutorTest
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-server/yierdis-server-api yierdis-server/yierdis-server-core yierdis-server/yierdis-server-executor
git commit -m "feat: track resp protocol version per session"
```

---

### Task 5: Implement Netty RESP Decoder And Adapter

**Files:**

- Modify: `yierdis-networking/yierdis-networking-netty/pom.xml`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`
- Test: `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`

- [ ] **Step 1: Update Netty POM**

Remove custom dependencies and add:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-networking-resp</artifactId>
</dependency>
```

- [ ] **Step 2: Write failing decoder tests**

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespCommandRequest;

import java.nio.charset.StandardCharsets;

public class RespRequestDecoderTest {
    @Test
    public void decodesArrayCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        ch.writeInbound(Unpooled.copiedBuffer("*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n", StandardCharsets.US_ASCII));

        RespCommandRequest req = ch.readInbound();
        Assert.assertEquals(2, req.argc());
        Assert.assertArrayEquals(bytes("PING"), req.readOnlyArg(0));
        Assert.assertArrayEquals(bytes("hey"), req.readOnlyArg(1));
        Assert.assertNull(ch.readInbound());
    }

    @Test
    public void decodesPipelinedCommandsInOrder() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        ch.writeInbound(Unpooled.copiedBuffer("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n", StandardCharsets.US_ASCII));
        Assert.assertEquals("PING", new String(((RespCommandRequest) ch.readInbound()).readOnlyArg(0), StandardCharsets.US_ASCII));
        Assert.assertEquals("PING", new String(((RespCommandRequest) ch.readInbound()).readOnlyArg(0), StandardCharsets.US_ASCII));
    }

    @Test
    public void decodesInlineCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(1024, 16, 1024));
        ch.writeInbound(Unpooled.copiedBuffer("SET a 1\r\n", StandardCharsets.US_ASCII));
        RespCommandRequest req = ch.readInbound();
        Assert.assertEquals(3, req.argc());
        Assert.assertArrayEquals(bytes("SET"), req.readOnlyArg(0));
        Assert.assertArrayEquals(bytes("a"), req.readOnlyArg(1));
        Assert.assertArrayEquals(bytes("1"), req.readOnlyArg(2));
    }

    @Test
    public void emitsProtocolErrorForOversizedBulk() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespRequestDecoder(2, 16, 1024));
        ch.writeInbound(Unpooled.copiedBuffer("*1\r\n$3\r\nabc\r\n", StandardCharsets.US_ASCII));
        Object msg = ch.readInbound();
        Assert.assertTrue(msg instanceof RespProtocolError);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
```

- [ ] **Step 3: Run failing decoder tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-netty test -Dtest=RespRequestDecoderTest`

Expected: FAIL because RESP Netty classes do not exist.

- [ ] **Step 4: Implement protocol error DTO**

```java
package yier.bubu.redis.protocol.resp.netty;

public record RespProtocolError(String message, boolean closeAfterReply) {
    public RespProtocolError {
        if (message == null || message.isBlank()) {
            message = "ERR Protocol error";
        }
    }
}
```

- [ ] **Step 5: Implement decoder state machine**

Implement `RespRequestDecoder extends ByteToMessageDecoder` with:

- Constructor: `RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes)`.
- Parse `*<argc>\r\n` array headers.
- Parse `$<len>\r\n<payload>\r\n` bulk strings.
- Reject negative array lengths, null bulk strings, oversized bulk strings, oversized inline lines, too many args, and malformed CRLF.
- Parse inline requests by reading to CRLF and splitting ASCII whitespace with quote handling matching `InlineCommandParser` behavior where practical.
- Emit `RespCommandRequest.wrapReadOnly(argv, retainedBytes)` for valid commands.
- Emit `new RespProtocolError("ERR Protocol error: invalid bulk length", true)` for protocol errors and consume enough bytes to avoid repeated emission.

Use `ByteBuf.bytesBefore((byte) '\n')` to avoid copying incomplete lines. Use heap byte arrays for retained argv because existing `ByteArrayExecutionRequest` is heap-backed and safe across Netty buffer release.

- [ ] **Step 6: Implement command adapter**

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.protocol.resp.RespCommandRequest;
import yier.bubu.redis.protocol.resp.RespExecutionAdapter;

public final class RespCommandAdapter extends ChannelInboundHandlerAdapter {
    private final RespExecutionAdapter adapter;

    public RespCommandAdapter() {
        this(RespExecutionAdapter.DEFAULT);
    }

    public RespCommandAdapter(RespExecutionAdapter adapter) {
        this.adapter = adapter == null ? RespExecutionAdapter.DEFAULT : adapter;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RespCommandRequest request) {
            ctx.fireChannelRead(adapter.toExecutionRequest(request));
            return;
        }
        super.channelRead(ctx, msg);
    }
}
```

- [ ] **Step 7: Run decoder tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-netty test -Dtest=RespRequestDecoderTest`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-networking/yierdis-networking-netty
git commit -m "feat: decode resp requests with netty"
```

---

### Task 6: Replace Server Pipeline With RESP

**Files:**

- Modify: `yierdis-server/yierdis-server-main/pom.xml`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- Create: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespProtocolIntegrationTest.java`

- [ ] **Step 1: Write failing server integration test**

Create `RespProtocolIntegrationTest.java`:

```java
package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespProtocolIntegrationTest {
    @Test
    public void serverAcceptsRedisCliStyleResp2Commands() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));

            out.write("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*2\r\n$3\r\nGET\r\n$1\r\na\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$1\r\n1\r\n", readAscii(in, 7));
        }
    }

    private static String readAscii(InputStream in, int len) throws Exception {
        byte[] bytes = in.readNBytes(len);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
```

- [ ] **Step 2: Run failing integration test**

Run: `mvn -pl yierdis-server/yierdis-server-main test -Dtest=RespProtocolIntegrationTest`

Expected: FAIL because server still wires Custom Protocol v1.

- [ ] **Step 3: Update server main POM**

Remove:

```xml
<artifactId>yierdis-networking-custom-v1-execution</artifactId>
```

Add:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-networking-resp</artifactId>
</dependency>
```

- [ ] **Step 4: Use `RespReplyWriterFactory` in bootstrap**

In `YierdisServerBootstrap`, replace `JsonLineReplyWriterFactory` construction and imports with:

```java
RespReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
```

Keep the variable typed as `ReplyWriterFactory` if that reduces imports in adjacent constructors.

- [ ] **Step 5: Implement protocol error reply handler**

Create `RespProtocolErrorReplyHandler.java`:

```java
package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;

import java.util.Objects;

public final class RespProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final ReplyWriterFactory replyWriterFactory;

    public RespProtocolErrorReplyHandler(ReplyWriterFactory replyWriterFactory) {
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RespProtocolError error)) {
            super.channelRead(ctx, msg);
            return;
        }
        ByteBuf out = ctx.alloc().buffer();
        try {
            ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
            writer.protocolError(error.message());
            if (error.closeAfterReply()) {
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.writeAndFlush(out);
            }
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }
}
```

- [ ] **Step 6: Rewrite channel initializer pipeline**

Replace imports and pipeline entries in `YierdisServerChannelInitializer` with:

```java
.addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(executor))
.addLast("respRequestDecoder", new RespRequestDecoder(
        config.protocolMaxBulkBytes(),
        config.protocolMaxArgs(),
        config.protocolMaxLineBytes()
))
.addLast("respCommandAdapter", new RespCommandAdapter())
.addLast("respProtocolErrorReply", new RespProtocolErrorReplyHandler(replyWriterFactory))
.addLast("commandHandler", new YierdisFastCommandHandler(executor, replyWriterFactory));
```

Change the field and constructor parameter type from `JsonLineReplyWriterFactory` to `ReplyWriterFactory`.

- [ ] **Step 7: Make direct reject replies session-aware**

In `YierdisFastCommandHandler.channelRead0`, replace:

```java
ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
```

with:

```java
ReplyWriter writer = replyWriterFactory.newWriter(connection.session(), new NettyByteBufSink(out));
```

In `newReplyWriter(ByteBuf out, ChannelHandlerContext ctx)`, also pass the session if a `NettyExecutionConnection` exists.

- [ ] **Step 8: Run integration tests**

Run:

```bash
mvn -pl yierdis-networking/yierdis-networking-netty test -Dtest=RespRequestDecoderTest
mvn -pl yierdis-server/yierdis-server-main test -Dtest=RespProtocolIntegrationTest,YierdisServerBootstrapCommandWiringTest
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add yierdis-networking/yierdis-networking-netty yierdis-server/yierdis-server-main
git commit -m "feat: serve redis resp protocol"
```

---

### Task 7: Implement HELLO, CLIENT, And AUTH Compatibility

**Files:**

- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespHandshakeIntegrationTest.java`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandProcessorTest.java`

- [ ] **Step 1: Write failing handshake test**

```java
package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespHandshakeIntegrationTest {
    @Test
    public void hello3SwitchesConnectionToResp3() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            String hello = readSome(in);
            Assert.assertTrue(hello.startsWith("%5\r\n"));
            Assert.assertTrue(hello.contains("$5\r\nproto\r\n:3\r\n"));

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));
        }
    }

    @Test
    public void clientSetinfoSetnameAndGetnameAreAccepted() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$7\r\ngo-redis\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$4\r\ntest\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$4\r\ntest\r\n", readAscii(in, 10));
        }
    }

    private static String readAscii(InputStream in, int len) throws Exception {
        return new String(in.readNBytes(len), StandardCharsets.US_ASCII);
    }

    private static String readSome(InputStream in) throws Exception {
        byte[] buf = new byte[256];
        int n = in.read(buf);
        return new String(buf, 0, n, StandardCharsets.US_ASCII);
    }
}
```

- [ ] **Step 2: Run failing handshake tests**

Run: `mvn -pl yierdis-server/yierdis-server-main test -Dtest=RespHandshakeIntegrationTest`

Expected: FAIL because `HELLO` does not set protocol state and `CLIENT` is unknown.

- [ ] **Step 3: Update `HELLO` parser registration**

In `ServerCommandModule.register`, replace `CommandParsers.oneOfRequest("hello", 1, 2)` with `CommandParsers.minRequest(1, "hello")` so `HELLO 3 SETNAME x` can parse.

- [ ] **Step 4: Implement HELLO option handling**

In `hello`, handle:

```java
int requested = ctx.session().respVersion();
int i = 1;
if (request.argc() >= 2) {
    String version = CommandSupport.utf8(request, 1);
    if ("2".equals(version)) {
        requested = 2;
        i = 2;
    } else if ("3".equals(version)) {
        requested = 3;
        i = 2;
    } else {
        out.error("NOPROTO unsupported protocol version");
        return;
    }
}
while (i < request.argc()) {
    if (CommandSupport.asciiEqualsIgnoreCase(request, i, "SETNAME") && i + 1 < request.argc()) {
        ctx.session().setClientName(CommandSupport.utf8(request, i + 1));
        i += 2;
        continue;
    }
    if (CommandSupport.asciiEqualsIgnoreCase(request, i, "AUTH")) {
        out.error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
        return;
    }
    out.error("ERR syntax error");
    return;
}
ctx.session().setRespVersion(requested);
```

Then write the existing 5-pair map with `proto` equal to `ctx.session().respVersion()`.

- [ ] **Step 5: Register CLIENT and AUTH**

In `CoreConnectionCommands.register`, add:

```java
registration.register("CLIENT", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(2, "client"), this::client);
registration.register("AUTH", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(1, "auth"), this::auth);
```

Implement:

```java
private void client(ExecutionRequest request, CommandContext ctx) {
    ReplyWriter out = ctx.out();
    if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "SETINFO")) {
        out.simpleString("OK");
        return;
    }
    if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "SETNAME")) {
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "client|setname");
            return;
        }
        ctx.session().setClientName(CommandSupport.utf8(request, 2));
        out.simpleString("OK");
        return;
    }
    if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "GETNAME")) {
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "client|getname");
            return;
        }
        String name = ctx.session().clientName();
        if (name == null) {
            out.nullValue();
        } else {
            out.bulkString(name.getBytes(StandardCharsets.UTF_8));
        }
        return;
    }
    out.error("ERR unknown subcommand '" + CommandSupport.utf8(request, 1) + "'. Try CLIENT HELP.");
}

private void auth(ExecutionRequest request, CommandContext ctx) {
    ctx.out().error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
}
```

Add `java.nio.charset.StandardCharsets` import.

- [ ] **Step 6: Run tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-main test -Dtest=RespHandshakeIntegrationTest
mvn -pl yierdis-tests/yierdis-integration-tests test -Dtest=CommandProcessorTest
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-server/yierdis-server-main yierdis-command/yierdis-command-builtin yierdis-tests/yierdis-integration-tests
git commit -m "feat: support redis client handshake commands"
```

---

### Task 8: Add Idle Timeout And Slow Client Protection

**Files:**

- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`

- [ ] **Step 1: Add failing args test**

In `YierdisServerArgsTest`, add:

```java
@Test
public void clientTimeoutAndOutputBufferArgsAreParsed() {
    YierdisServerArgs args = parse(
            "--client-idle-timeout-millis", "1000",
            "--client-output-buffer-limit-bytes", "2048",
            "--client-output-buffer-over-limit-millis", "3000"
    );
    YierdisServerRuntimeConfig config = args.toRuntimeConfig();
    Assert.assertEquals(1000, config.clientIdleTimeoutMillis());
    Assert.assertEquals(2048, config.clientOutputBufferLimitBytes());
    Assert.assertEquals(3000, config.clientOutputBufferOverLimitMillis());
}
```

- [ ] **Step 2: Run failing args test**

Run: `mvn -pl yierdis-server/yierdis-server-main test -Dtest=YierdisServerArgsTest#clientTimeoutAndOutputBufferArgsAreParsed`

Expected: FAIL because new flags do not exist.

- [ ] **Step 3: Add arg names**

Add constants:

```java
public static final String CLIENT_IDLE_TIMEOUT_MILLIS = "--client-idle-timeout-millis";
public static final String CLIENT_OUTPUT_BUFFER_LIMIT_BYTES = "--client-output-buffer-limit-bytes";
public static final String CLIENT_OUTPUT_BUFFER_OVER_LIMIT_MILLIS = "--client-output-buffer-over-limit-millis";
```

- [ ] **Step 4: Add fields and validation**

In `YierdisServerArgs`, add defaults:

```java
@Option(names = YierdisServerArgNames.CLIENT_IDLE_TIMEOUT_MILLIS, defaultValue = "300000", description = "Close clients idle for this many milliseconds (0 disables).")
public long clientIdleTimeoutMillis = 300000;

@Option(names = YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_LIMIT_BYTES, defaultValue = "67108864", description = "Close slow clients above this outbound buffer size (0 disables).")
public long clientOutputBufferLimitBytes = 67108864;

@Option(names = YierdisServerArgNames.CLIENT_OUTPUT_BUFFER_OVER_LIMIT_MILLIS, defaultValue = "10000", description = "Slow-client grace period above output buffer limit in milliseconds.")
public long clientOutputBufferOverLimitMillis = 10000;
```

Validate all three as `>= 0`, and require `clientOutputBufferOverLimitMillis > 0` when `clientOutputBufferLimitBytes > 0`.

Add fields to `copy()`, `toRuntimeConfig()`, and `toArgv()`.

- [ ] **Step 5: Extend runtime config record**

Add fields after protocol limit fields:

```java
long clientIdleTimeoutMillis,
long clientOutputBufferLimitBytes,
long clientOutputBufferOverLimitMillis,
```

- [ ] **Step 6: Wire idle timeout**

In `YierdisServerChannelInitializer.initChannel`, before decoder:

```java
if (config.clientIdleTimeoutMillis() > 0) {
    ch.pipeline().addLast("idleTimeout", new IdleStateHandler(
            config.clientIdleTimeoutMillis(), 0, 0, TimeUnit.MILLISECONDS
    ));
}
```

Add an inbound handler that closes on `IdleStateEvent.READER_IDLE`.

- [ ] **Step 7: Wire output buffer watermarks**

Set write buffer watermark in `initChannel`:

```java
if (config.clientOutputBufferLimitBytes() > 0) {
    int high = (int) Math.min(Integer.MAX_VALUE, config.clientOutputBufferLimitBytes());
    int low = Math.max(1, high / 2);
    ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(low, high));
}
```

Keep the existing `WriteBufferBackpressureHandler` as the first pipeline handler.

- [ ] **Step 8: Run tests**

Run: `mvn -pl yierdis-server/yierdis-server-main test -Dtest=YierdisServerArgsTest`

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add yierdis-server/yierdis-server-main
git commit -m "feat: add redis client timeout protections"
```

---

### Task 9: Add RESP Client Codec Helpers

**Files:**

- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespClientCodec.java`
- Test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespClientCodecTest.java`

- [ ] **Step 1: Write failing codec tests**

```java
package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespClientCodecTest {
    @Test
    public void encodesCommand() throws Exception {
        Assert.assertEquals("*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
                ascii(RespClientCodec.encodeCommand(List.of(bytes("PING"), bytes("hey")))));
    }

    @Test
    public void readsSimpleBulkIntegerNullAndArrayReplies() throws Exception {
        Assert.assertTrue(RespClientCodec.readReply(in("+OK\r\n"), 1024).isSimpleString("OK"));
        Assert.assertArrayEquals(bytes("abc"), RespClientCodec.readReply(in("$3\r\nabc\r\n"), 1024).bytes());
        Assert.assertEquals(Long.valueOf(7), RespClientCodec.readReply(in(":7\r\n"), 1024).integer());
        Assert.assertTrue(RespClientCodec.readReply(in("$-1\r\n"), 1024).isNull());
        Assert.assertEquals(2, RespClientCodec.readReply(in("*2\r\n+OK\r\n:1\r\n"), 1024).values().size());
    }

    private static ByteArrayInputStream in(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static String ascii(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
```

- [ ] **Step 2: Run failing codec tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespClientCodecTest`

Expected: FAIL because `RespClientCodec` does not exist.

- [ ] **Step 3: Implement command encoding**

Expose:

```java
public static byte[] encodeCommand(List<byte[]> args)
public static void writeCommand(OutputStream out, List<byte[]> args) throws IOException
```

Use exact RESP2 array/bulk encoding:

```java
out.write(('*' + Integer.toString(args.size()) + "\r\n").getBytes(StandardCharsets.US_ASCII));
for (byte[] arg : args) {
    byte[] value = arg == null ? new byte[0] : arg;
    out.write(('$' + Integer.toString(value.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
    out.write(value);
    out.write(new byte[]{'\r', '\n'});
}
```

- [ ] **Step 4: Implement minimal reply parser**

Expose a nested immutable reply:

```java
public record RespReply(Kind kind, String text, byte[] bytes, Long integer, List<RespReply> values) {
    public enum Kind { SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, NULL, ARRAY }
    public boolean isNull() { return kind == Kind.NULL; }
    public boolean isSimpleString(String expected) { return kind == Kind.SIMPLE_STRING && java.util.Objects.equals(text, expected); }
    public byte[] bytes() { return bytes == null ? null : bytes.clone(); }
}
```

Parser rules:

- `+`: read simple string line.
- `-`: read error line.
- `:`: read integer line.
- `$`: read length, return null for `-1`, enforce `maxBulkBytes`, read exact bytes and CRLF.
- `*`: read count, recursively read elements, return null for `-1`.

- [ ] **Step 5: Run codec tests**

Run: `mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=RespClientCodecTest`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-networking/yierdis-networking-resp
git commit -m "feat: add resp client codec helpers"
```

---

### Task 10: Rewrite CLI To Speak RESP2

**Files:**

- Modify: `yierdis-cli/pom.xml`
- Modify: `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisClient.java`
- Delete: `yierdis-cli/src/main/java/yier/bubu/redis/app/client/CustomProtocolV1Replies.java`
- Modify: `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`
- Test: `yierdis-cli/src/test/java/yier/bubu/redis/app/client/YierdisClientTest.java`

- [ ] **Step 1: Add failing client test expectation**

Update `YierdisClientTest` so `PING` expects Redis simple string shape from the client API:

```java
@Test
public void pingReturnsSimpleStringPongOverResp() throws Exception {
    try (TestServer server = TestServer.start();
         YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
        YierdisClient.RespReply reply = client.executeUtf8(List.of("PING"), 3000);
        Assert.assertEquals(YierdisClient.RespReply.Kind.SIMPLE_STRING, reply.kind());
        Assert.assertEquals("PONG", reply.text());
    }
}
```

- [ ] **Step 2: Run failing CLI test**

Run: `mvn -pl yierdis-cli test -Dtest=YierdisClientTest`

Expected: FAIL because the client still returns JSON envelopes.

- [ ] **Step 3: Update CLI POM**

Remove dependencies on `yierdis-networking-custom-v1` and custom Netty JSON helpers. Add:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-networking-resp</artifactId>
</dependency>
```

Keep Netty dependency only if `YierdisClient` remains Netty-based. If simplifying to blocking sockets, remove Netty from the CLI module after tests pass.

- [ ] **Step 4: Rewrite client API**

Replace `JsonReply` with:

```java
public record RespReply(Kind kind, String text, byte[] bytes, Long integer, List<RespReply> values) {
    public enum Kind {
        SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, NULL, ARRAY
    }

    public byte[] bytes() {
        return bytes == null ? null : bytes.clone();
    }
}
```

Use `RespClientCodec.writeCommand(OutputStream out, List<byte[]> args)` and `RespClientCodec.readReply(InputStream in, int maxBulkBytes)` from Task 9.

- [ ] **Step 5: Preserve one-at-a-time request pairing**

Keep `execute(List<byte[]> args, long timeoutMillis)` synchronized on `requestLock`. On timeout, close the socket/channel because reply pairing is FIFO and cannot be safely recovered.

- [ ] **Step 6: Update CLI output**

In `YierdisCli`, replace JSON envelope printing with Redis-like display:

- simple string: print text
- bulk string: print UTF-8 text when valid, otherwise print hex in `--hex` mode
- integer: print number
- null: print `(nil)`
- error: print `(error) <message>`
- array: print one element per line with `1)`, `2)`, matching basic `redis-cli` readability

- [ ] **Step 7: Run CLI tests**

Run: `mvn -pl yierdis-cli test`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-cli
git commit -m "feat: rewrite cli for resp"
```

---

### Task 11: Rewrite Benchmark RESP Framing And Reply Inspection

**Files:**

- Modify: `yierdis-benchmark/pom.xml`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- Modify: `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/CustomCommandWriterTest.java`
- Test: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSummaryFormatTest.java`

- [ ] **Step 1: Rename and update command writer test**

Rename `CustomCommandWriterTest` to `RespCommandWriterTest`. The core assertion should be:

```java
@Test
public void writesRespArrayBulkCommand() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RespClientCodec.writeCommand(out, List.of(bytes("SET"), bytes("a"), bytes("1")));
    Assert.assertEquals("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n", out.toString(StandardCharsets.US_ASCII));
}
```

- [ ] **Step 2: Run failing benchmark tests**

Run: `mvn -pl yierdis-benchmark test -Dtest=RespCommandWriterTest`

Expected: FAIL because benchmark still uses `CustomProtocolV1RequestEncoder`.

- [ ] **Step 3: Update benchmark POM**

Remove:

```xml
<artifactId>yierdis-networking-custom-v1</artifactId>
```

Add:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-networking-resp</artifactId>
</dependency>
```

- [ ] **Step 4: Replace frame writer**

In `YierdisBench`, replace `CustomProtocolV1RequestEncoder.writeRequestFrame(...)` with:

```java
RespClientCodec.writeCommand(out, args);
```

For precomputed ping frame:

```java
private static final byte[] FRAME_PING = RespClientCodec.encodeCommand(List.of(CMD_PING));
```

- [ ] **Step 5: Replace strict reply inspection**

Replace `CustomProtocolV1ReplyInspector.matchesOkAsciiStringResult(line, 0, lineLen, OK)` with a RESP check:

```java
RespClientCodec.RespReply reply = RespClientCodec.readReply(in, maxBulkBytes);
return reply.isSimpleString("OK");
```

For GET, accept bulk string length equal to `dataSize` or null when the workload permits missing keys.

- [ ] **Step 6: Update server args limits import**

In `YierdisBenchServerArgs`, replace `ProtocolLimits` with `RespProtocolLimits`.

- [ ] **Step 7: Run benchmark tests**

Run: `mvn -pl yierdis-benchmark test`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-benchmark
git commit -m "feat: benchmark with resp protocol"
```

---

### Task 12: Remove Custom Protocol Modules And References

**Files:**

- Modify: `pom.xml`
- Modify: `yierdis-networking/pom.xml`
- Modify: `yierdis-tests/yierdis-architecture-tests/pom.xml`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Create: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/resp/RespBoundaryGuardTest.java`
- Delete: `yierdis-networking/yierdis-networking-custom-v1`
- Delete: `yierdis-networking/yierdis-networking-custom-v1-execution`
- Delete: custom Netty package under `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/custom/v1`
- Delete: custom tests under `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/custom/v1`
- Delete: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/custom/v1`

- [ ] **Step 1: Add architecture guard against custom protocol**

Create `RespBoundaryGuardTest.java`:

```java
package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class RespBoundaryGuardTest {
    @Test
    public void productionSourcesMustNotReferenceCustomProtocolV1() throws IOException {
        Path root = Path.of("").toAbsolutePath().normalize();
        try (Stream<Path> files = Files.walk(root)) {
            Path offender = files
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().contains("/src/main/java/") || p.getFileName().toString().equals("pom.xml"))
                    .filter(p -> !p.toString().contains("/target/"))
                    .filter(p -> contains(p, "protocol.custom.v1")
                            || contains(p, "CustomProtocolV1")
                            || contains(p, "JsonLineReplyWriter"))
                    .findFirst()
                    .orElse(null);
            Assert.assertNull("custom protocol reference remains: " + offender, offender);
        }
    }

    private static boolean contains(Path path, String needle) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8).contains(needle);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
```

- [ ] **Step 2: Run failing architecture guard**

Run: `mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=RespBoundaryGuardTest`

Expected: FAIL because custom modules and dependencies still exist.

- [ ] **Step 3: Remove custom modules from Maven**

In root `pom.xml`, remove dependency management entries for:

- `yierdis-networking-custom-v1`
- `yierdis-networking-custom-v1-execution`

In `yierdis-networking/pom.xml`, ensure only:

```xml
<modules>
    <module>yierdis-networking-resp</module>
    <module>yierdis-networking-netty</module>
</modules>
```

In architecture test POM, remove dependency entries for custom modules and add `yierdis-networking-resp` if needed.

- [ ] **Step 4: Delete custom directories**

Use normal git deletion:

```bash
git rm -r yierdis-networking/yierdis-networking-custom-v1
git rm -r yierdis-networking/yierdis-networking-custom-v1-execution
git rm -r yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/custom/v1
git rm -r yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/custom/v1
git rm -r yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/custom/v1
```

- [ ] **Step 5: Update architecture policy**

Replace custom module/package entries in `architecture-policy.yml` with:

```yaml
yierdis-networking-resp:
  allowed_dependencies:
    - yierdis-common-bytes
    - yierdis-server-api
  owned_packages:
    - yier.bubu.redis.protocol.resp
```

Keep `yierdis-networking-netty` depending on `yierdis-networking-resp`.

- [ ] **Step 6: Run architecture tests**

Run: `mvn -pl yierdis-tests/yierdis-architecture-tests test`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pom.xml yierdis-networking yierdis-tests/yierdis-architecture-tests
git commit -m "refactor: remove custom protocol modules"
```

---

### Task 13: Update Documentation And Scripts

**Files:**

- Modify: `README.md`
- Modify: `docs/protocol-reference.md`
- Modify: `docs/project-introduction.md`
- Modify: `docs/project-overview.md`
- Modify: `docs/module-architecture.md`
- Modify: `docs/development-navigation.md`
- Modify: `docs/client-and-bench-internals.md`
- Modify: `docs/glossary.md`
- Modify: `scripts/smoke.sh`
- Modify: `scripts/bench.sh`

- [ ] **Step 1: Find remaining custom protocol references**

Run:

```bash
rg -n "Custom Protocol|custom protocol|custom-v1|custom\\.v1|CustomProtocolV1|NDJSON|JsonLine|<len>:<json>|protocol.custom.v1" README.md docs scripts yierdis-cli yierdis-benchmark yierdis-networking yierdis-server yierdis-tests
```

Expected: output lists documentation references and no production Java references after Task 12.

- [ ] **Step 2: Rewrite protocol reference**

Change `docs/protocol-reference.md` from Custom Protocol v1 to RESP:

- Describe RESP2 as the public default.
- Show request example: `*2\r\n$3\r\nGET\r\n$1\r\na\r\n`.
- Show reply examples: `+OK`, `$-1`, arrays, errors.
- Document `HELLO 3` as negotiated RESP3.
- State malformed RESP closes connection after an error reply.

- [ ] **Step 3: Update project docs**

Replace wording that says Yierdis is not RESP-compatible with wording that says:

```text
Yierdis exposes Redis RESP as its public TCP protocol. RESP2 is the default compatibility target for redis-cli, Jedis, Lettuce, and go-redis. RESP3 is available for basic negotiated replies through HELLO 3.
```

- [ ] **Step 4: Update scripts**

Make `scripts/smoke.sh` use `redis-cli` when available:

```bash
redis-cli -p "$PORT" PING
redis-cli -p "$PORT" SET smoke:key smoke:value
redis-cli -p "$PORT" GET smoke:key
```

Keep a Java CLI fallback if the script already supports local jars.

- [ ] **Step 5: Verify no stale references remain**

Run the same `rg` command from Step 1.

Expected: only historical design docs under `docs/superpowers/specs` may mention Custom Protocol v1 as current-state context.

- [ ] **Step 6: Commit**

```bash
git add README.md docs scripts
git commit -m "docs: document redis protocol compatibility"
```

---

### Task 14: Client Compatibility Smoke Tests

**Files:**

- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RedisCliCompatibilityTest.java`
- Create: `docs/testing-and-debugging.md` additions for optional Jedis/Lettuce/go-redis checks

- [ ] **Step 1: Add redis-cli smoke test that skips when redis-cli is absent**

```java
package yier.bubu.redis.app.server;

import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class RedisCliCompatibilityTest {
    @Test
    public void redisCliCanPingSetAndGet() throws Exception {
        Assume.assumeTrue(commandExists("redis-cli"));
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
            Assert.assertEquals("PONG", run("redis-cli", "-p", Integer.toString(server.port()), "PING"));
            Assert.assertEquals("OK", run("redis-cli", "-p", Integer.toString(server.port()), "SET", "a", "1"));
            Assert.assertEquals("1", run("redis-cli", "-p", Integer.toString(server.port()), "GET", "a"));
        }
    }

    private static boolean commandExists(String command) throws Exception {
        Process p = new ProcessBuilder("sh", "-lc", "command -v " + command).start();
        return p.waitFor(2, TimeUnit.SECONDS) && p.exitValue() == 0;
    }

    private static String run(String... command) throws Exception {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String out;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            out = reader.readLine();
        }
        Assert.assertTrue("command timed out", p.waitFor(5, TimeUnit.SECONDS));
        Assert.assertEquals("command exit", 0, p.exitValue());
        return out;
    }
}
```

- [ ] **Step 2: Run the smoke test**

Run: `mvn -pl yierdis-server/yierdis-server-main test -Dtest=RedisCliCompatibilityTest`

Expected: PASS if `redis-cli` is installed; SKIPPED otherwise.

- [ ] **Step 3: Add manual client compatibility docs**

Add to `docs/testing-and-debugging.md`:

```text
Redis client smoke checks:

- redis-cli: redis-cli -p 6378 PING
- RESP3: redis-cli -3 -p 6378 HELLO 3
- Jedis: connect, ping, set, get, pipeline with default protocol settings.
- Lettuce: connect, ping, set, get, pipeline with RESP2 default and RESP3 when configured.
- go-redis: use Protocol: 2 for primary smoke, then Protocol: 3 for HELLO 3 smoke.
```

- [ ] **Step 4: Run final focused suite**

Run:

```bash
mvn -pl yierdis-networking/yierdis-networking-resp,yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main,yierdis-cli,yierdis-benchmark,yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/yierdis-server-main docs/testing-and-debugging.md
git commit -m "test: add redis client compatibility smoke"
```

---

## Final Verification

- [ ] Run the full Maven suite:

```bash
mvn test
```

Expected: PASS.

- [ ] Confirm Custom Protocol v1 is absent from production code:

```bash
rg -n "protocol\\.custom\\.v1|CustomProtocolV1|JsonLineReplyWriter|yierdis-networking-custom-v1" pom.xml yierdis-* docs README.md scripts
```

Expected: only approved historical mentions in `docs/superpowers/specs/2026-05-10-redis-protocol-compatibility-design.md` and this implementation plan, or no output if those docs are excluded.

- [ ] Start the shaded server jar and verify Redis CLI compatibility:

```bash
mvn -pl yierdis-server/yierdis-server-main -am package -DskipTests
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 6378
redis-cli -p 6378 PING
redis-cli -p 6378 SET final:key ok
redis-cli -p 6378 GET final:key
redis-cli -3 -p 6378 HELLO 3
```

Expected:

```text
PONG
OK
ok
```

`HELLO 3` should print a map-like response containing `server`, `version`, `proto`, `mode`, and `role`, with `proto` equal to `3`.
