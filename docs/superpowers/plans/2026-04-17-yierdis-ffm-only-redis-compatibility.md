# Yierdis FFM-Only Redis Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Converge Yierdis to an FFM-only storage kernel while driving the supported command surface toward Redis semantics without changing the existing `Custom Protocol v1`.

**Architecture:** Keep the current module boundaries and Netty execution model, but make `yierdis-core-db` the single FFM-backed storage path, add a compatibility ledger plus Redis differential tests, and then tighten command semantics family-by-family until the ledger and regression suites agree. The implementation proceeds in small slices so that semantics, leak safety, and observability stay verifiable throughout the migration.

**Tech Stack:** Java 25, Maven, Netty, JUnit 4, Testcontainers (test scope), `Custom Protocol v1`, JDK FFM API

---

## File Structure Map

### Docs and compatibility truth

- Create: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
  Purpose: Single source of truth for command-family compatibility state (`Compatible`, `Known incompatibility to fix`, `Explicitly out of scope`).
- Modify: `README.md`
  Purpose: Remove teaching-oriented framing and restate project scope as a single-node FFM-backed Redis-semantics implementation over `Custom Protocol v1`.

### Differential test harness

- Modify: `yierdis-client/pom.xml`
  Purpose: Add test-scope dependencies for the Redis differential harness.
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisRespTestClient.java`
  Purpose: Minimal RESP client used only by tests to talk to real Redis.
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
  Purpose: Compare logical outcomes between Redis and Yierdis across the supported command surface.

### Command semantics

- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HashCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ListCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/SetCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HllCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
  Purpose: Align parsing, reply shapes, TTL edge cases, and transaction queueing with Redis behavior.

### FFM-only DB kernel

- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/HeapKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHeapExpireIndex.java`
  Purpose: Remove heap-backed authority paths and make all supported data structures FFM-backed only.

### Observability and guard tests

- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyStorageBoundaryGuardTest.java`
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyValuePathGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/TransactionQueueLimitTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
  Purpose: Enforce the FFM-only architecture and keep leak, semantics, and reporting coverage green during migration.

---

### Task 1: Establish The Compatibility Ledger And Redis Differential Harness

**Files:**
- Create: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Modify: `README.md`
- Modify: `yierdis-client/pom.xml`
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisRespTestClient.java`
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Test: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`

- [ ] **Step 1: Write the failing differential test**

```java
@Test
public void pingSetGetAndExpireMatchRedis() throws Exception {
    try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)) {
        redis.start();
        try (TestServer yierdis = TestServer.start();
             YierdisClient yClient = YierdisClient.connect("127.0.0.1", yierdis.port());
             RedisRespTestClient rClient = RedisRespTestClient.connect(redis.getHost(), redis.getMappedPort(6379))) {

            Assert.assertEquals("PONG", stringResult(
                    yClient.execute(Arrays.asList(b("PING")), 2000).envelope()));
            Assert.assertEquals("PONG", rClient.simpleString("PING"));

            Assert.assertEquals("OK", stringResult(
                    yClient.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("5")), 2000).envelope()));
            Assert.assertEquals("OK", rClient.simpleString("SET", "k", "v", "EX", "5"));

            Assert.assertEquals("v", stringResult(
                    yClient.execute(Arrays.asList(b("GET"), b("k")), 2000).envelope()));
            Assert.assertEquals("v", rClient.bulkString("GET", "k"));
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#pingSetGetAndExpireMatchRedis test`
Expected: FAIL with compilation errors for missing `org.testcontainers.*` and missing `RedisRespTestClient`.

- [ ] **Step 3: Write minimal implementation**

Add to `yierdis-client/pom.xml`:

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.20.1</version>
    <scope>test</scope>
</dependency>
```

Create `RedisRespTestClient.java`:

```java
final class RedisRespTestClient implements Closeable {
    private final Socket socket;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;

    private RedisRespTestClient(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    static RedisRespTestClient connect(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        socket.setSoTimeout(2_000);
        return new RedisRespTestClient(socket);
    }

    String simpleString(String... argv) throws IOException {
        write(argv);
        byte prefix = readPrefix();
        if (prefix != '+') {
            throw new IOException("expected simple string, got " + (char) prefix);
        }
        return readLine();
    }

    String bulkString(String... argv) throws IOException {
        write(argv);
        byte prefix = readPrefix();
        if (prefix != '$') {
            throw new IOException("expected bulk string, got " + (char) prefix);
        }
        int len = Integer.parseInt(readLine());
        if (len < 0) {
            return null;
        }
        byte[] bytes = in.readNBytes(len);
        expectCrLf();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    long integer(String... argv) throws IOException {
        write(argv);
        byte prefix = readPrefix();
        if (prefix != ':') {
            throw new IOException("expected integer, got " + (char) prefix);
        }
        return Long.parseLong(readLine());
    }

    private void write(String... argv) throws IOException {
        out.write(("*" + argv.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String arg : argv) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(bytes);
            out.write('\r');
            out.write('\n');
        }
        out.flush();
    }

    private byte readPrefix() throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new IOException("unexpected EOF");
        }
        return (byte) b;
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (;;) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF");
            }
            if (b == '\r') {
                if (in.read() != '\n') {
                    throw new IOException("expected LF");
                }
                return sb.toString();
            }
            sb.append((char) b);
        }
    }

    private void expectCrLf() throws IOException {
        if (in.read() != '\r' || in.read() != '\n') {
            throw new IOException("expected CRLF");
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
```

Create `RedisSemanticDifferentialTest.java` with local helpers instead of reaching into other test classes:

```java
private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
}

private static String stringResult(JsonValue envelope) {
    JsonValue v = CustomProtocolV1Replies.resultValue(envelope);
    Assert.assertTrue(v instanceof JsonString);
    return ((JsonString) v).value();
}

private static long longResult(JsonValue envelope) {
    JsonValue v = CustomProtocolV1Replies.resultValue(envelope);
    Assert.assertTrue(v instanceof JsonLong);
    return ((JsonLong) v).value();
}

private static final class TestServer implements AutoCloseable {
    private final YierdisServerBootstrap server;

    private TestServer(YierdisServerBootstrap server) {
        this.server = server;
    }

    static TestServer start() throws Exception {
        return new TestServer(YierdisServerBootstrap.start("--port", "0", "--ioThreads", "1", "--noCleanup"));
    }

    int port() {
        return server.port();
    }

    @Override
    public void close() {
        server.close();
    }
}
```

Create `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`:

```md
# Custom Protocol V1 Redis Semantics Ledger

## Rules

- State must be one of: `Compatible`, `Known incompatibility to fix`, `Explicitly out of scope`
- `Simplified for teaching` is forbidden
- Any new supported command behavior must update this ledger in the same change

## Current State

| Command family | State | Notes |
| --- | --- | --- |
| PING/ECHO | Compatible | Protocol representation differs, logical result should match Redis |
| String/Bitmap | Known incompatibility to fix | Differential tests drive exact edge cases |
| TTL/Expire | Known incompatibility to fix | Differential tests drive sentinel values and persistence behavior |
| Hash/List/Set/ZSet | Known incompatibility to fix | Existing support must be tightened |
| HLL | Known incompatibility to fix | Keep logical parity, protocol differs |
| MULTI/EXEC/DISCARD | Known incompatibility to fix | Queueing and EXECABORT need parity tests |
| WATCH | Explicitly out of scope | Not part of this phase |
```

Update the top of `README.md`:

```md
# yierdis (Java 25 + Netty)

一个基于 Java 25、Netty 与 FFM API 的单机内存 KV 服务端。

对外协议使用自定义协议 **Custom Protocol v1**。目标是在保留该协议的前提下，使已支持命令的逻辑语义尽量贴近 Redis，并使用堆外内存作为唯一主存储路径。
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#pingSetGetAndExpireMatchRedis test`
Expected: PASS with `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add README.md \
  docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md \
  yierdis-client/pom.xml \
  yierdis-client/src/test/java/yier/bubu/redis/client/RedisRespTestClient.java \
  yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java
git commit -m "test: add redis semantic differential harness"
```

### Task 2: Tighten String, Bitmap, And TTL Semantics

**Files:**
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Test: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`

- [ ] **Step 1: Write the failing semantic cases**

```java
@Test
public void setOptionsAndTtlFamilyMatchRedis() throws Exception {
    try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)) {
        redis.start();
        try (TestServer yierdis = TestServer.start();
             YierdisClient yClient = YierdisClient.connect("127.0.0.1", yierdis.port());
             RedisRespTestClient rClient = RedisRespTestClient.connect(redis.getHost(), redis.getMappedPort(6379))) {

            Assert.assertEquals("OK", stringResult(
                    yClient.execute(Arrays.asList(b("SET"), b("a"), b("1"), b("NX")), 2000).envelope()));
            Assert.assertEquals("OK", rClient.simpleString("SET", "a", "1", "NX"));

            Assert.assertNull(CustomProtocolV1Replies.resultValue(
                    yClient.execute(Arrays.asList(b("SET"), b("a"), b("2"), b("NX")), 2000).envelope()));
            Assert.assertNull(rClient.bulkString("SET", "a", "2", "NX"));

            Assert.assertEquals(1L, longResult(
                    yClient.execute(Arrays.asList(b("EXPIRE"), b("a"), b("5")), 2000).envelope()));
            Assert.assertEquals(1L, rClient.integer("EXPIRE", "a", "5"));
        }
    }
}

@Test
public void bitmapOperationsMatchRedis() throws Exception {
    try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)) {
        redis.start();
        try (TestServer yierdis = TestServer.start();
             YierdisClient yClient = YierdisClient.connect("127.0.0.1", yierdis.port());
             RedisRespTestClient rClient = RedisRespTestClient.connect(redis.getHost(), redis.getMappedPort(6379))) {

            Assert.assertEquals(0L, longResult(
                    yClient.execute(Arrays.asList(b("SETBIT"), b("bits"), b("7"), b("1")), 2000).envelope()));
            Assert.assertEquals(0L, rClient.integer("SETBIT", "bits", "7", "1"));

            Assert.assertEquals(1L, longResult(
                    yClient.execute(Arrays.asList(b("GETBIT"), b("bits"), b("7")), 2000).envelope()));
            Assert.assertEquals(1L, rClient.integer("GETBIT", "bits", "7"));
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#setOptionsAndTtlFamilyMatchRedis,RedisSemanticDifferentialTest#bitmapOperationsMatchRedis test`
Expected: FAIL with at least one assertion mismatch in TTL, nil mapping, or bitmap edge handling.

- [ ] **Step 3: Write minimal implementation**

In `StringCommands.set(...)` keep Redis-style nil behavior for non-applied `NX` and `XX`:

```java
if (!result.applied()) {
    out.bulkString((byte[]) null);
    return;
}
if (getOld) {
    out.bulkString(result.oldValue());
    return;
}
out.simpleString("OK");
```

In `KeyCommands`, keep Redis sentinel classes for `TTL` and `PTTL`:

```java
private void ttl(ExecutionRequest request, CommandContext ctx) {
    out.integer(support.dbReads(ctx).ttl().ttlSeconds(support.argView(request, 1)));
}

private void pttl(ExecutionRequest request, CommandContext ctx) {
    out.integer(support.dbReads(ctx).ttl().ttlMillis(support.argView(request, 1)));
}
```

In `YierdisTtlOps`, keep `-2` for missing keys and `-1` for keys without expire:

```java
if (expireAtMillis == null) {
    return keyLifecycle.exists(key) ? -1L : -2L;
}
```

Update the ledger rows for `String/Bitmap` and `TTL/Expire` as exact cases move to `Compatible`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#setOptionsAndTtlFamilyMatchRedis,RedisSemanticDifferentialTest#bitmapOperationsMatchRedis test`
Expected: PASS with `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md \
  yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java
git commit -m "fix: align string bitmap and ttl semantics"
```

### Task 3: Enforce FFM-Only Storage Boundaries

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyStorageBoundaryGuardTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/HeapKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHeapExpireIndex.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java`
- Test: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyStorageBoundaryGuardTest.java`

- [ ] **Step 1: Write the failing storage-boundary test**

```java
public class FfmOnlyStorageBoundaryGuardTest {
    @Test
    public void heapSpecificStorageFilesAreGone() {
        Path repo = Path.of("").toAbsolutePath();
        Assert.assertFalse(Files.exists(repo.resolve(
                "yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/HeapKeyHandle.java")));
        Assert.assertFalse(Files.exists(repo.resolve(
                "yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java")));
        Assert.assertFalse(Files.exists(repo.resolve(
                "yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHeapExpireIndex.java")));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-db -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyStorageBoundaryGuardTest test`
Expected: FAIL because the heap-specific files still exist.

- [ ] **Step 3: Write minimal implementation**

In `YierdisDb`, keep only the FFM-backed setup:

```java
this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
this.offHeapAllocator = new YierdisForeignOffHeapAllocator(this.memoryRuntime, 0);
this.resources = new YierdisDbOwnedResources(this.memoryRuntime, this.offHeapAllocator, ownsMemoryRuntime, true);
YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(this.memoryRuntime, "ffm-key");
this.store = new YierdisFfmKeyspace<>(blobStore);
this.expires = new YierdisFfmExpireIndex(blobStore);
this.keysStoredOffHeap = true;
```

In `KeyHandle.java`, delete heap factories and keep only:

```java
public static KeyHandle forFfm(YierdisFfmBytesRef ref, int hash) {
    return new FfmKeyHandle(ref, hash);
}
```

Delete the three heap-only files and update `OffHeapKeysToggleTest` so it only asserts the surviving FFM path:

```java
Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyStorageBoundaryGuardTest,OffHeapKeysToggleTest test`
Expected: PASS with `Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyStorageBoundaryGuardTest.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java
git add -u yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db
git commit -m "refactor: remove heap storage foundations"
```

### Task 4: Converge Value Encodings To FFM-Only

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyValuePathGuardTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java`
- Test: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyValuePathGuardTest.java`

- [ ] **Step 1: Write the failing value-path guard and leak test**

```java
public class FfmOnlyValuePathGuardTest {
    @Test
    public void valueFilesDoNotKeepHeapFallbackBranches() throws Exception {
        Path root = Path.of("yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db");
        String object = Files.readString(root.resolve("YierdisObject.java"));
        String hash = Files.readString(root.resolve("HashValue.java"));
        String list = Files.readString(root.resolve("ListValue.java"));
        String set = Files.readString(root.resolve("SetValue.java"));
        String zset = Files.readString(root.resolve("ZSetValue.java"));

        Assert.assertFalse(object.contains("payload = valueBytes"));
        Assert.assertFalse(hash.contains("if (memoryRuntime != null)"));
        Assert.assertFalse(list.contains("if (memoryRuntime != null)"));
        Assert.assertFalse(set.contains("if (memoryRuntime != null)"));
        Assert.assertFalse(zset.contains("if (memoryRuntime != null)"));
    }
}
```

Add to `OffHeapStringStorageTest.java`:

```java
@Test
public void stringDeleteAndOverwriteReleaseFfmBackedPayloads() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
        YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            Assert.assertTrue(db.writes().strings().setString(b("k"), b("hello"), SetMode.NORMAL, null));
            long usedAfterSet = runtime.usedBytes();
            Assert.assertTrue(usedAfterSet > 0L);
            Assert.assertTrue(db.writes().strings().setString(b("k"), b("world"), SetMode.NORMAL, null));
            Assert.assertEquals(1L, db.writes().keyspace().del(java.util.List.of(b("k"))));
            Assert.assertEquals(0L, runtime.usedBytes());
        } finally {
            db.shutdown();
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyValuePathGuardTest,OffHeapStringStorageTest test`
Expected: FAIL because the source files still contain heap fallback branches or because the new release assertion does not hold.

- [ ] **Step 3: Write minimal implementation**

In `YierdisObject`, remove heap `byte[]` payload fallback:

```java
static YierdisObject newString(OffHeapAllocator offHeapAllocator, byte[] valueBytes) {
    if (valueBytes == null || valueBytes.length == 0) {
        YierdisObject o = new YierdisObject(ValueType.STRING, ValueEncoding.STRING_EMBSTR, null);
        o.rawLen = 0;
        return o;
    }
    Long parsed = tryParseLongForIntEncoding(valueBytes, valueBytes.length);
    if (parsed != null) {
        return newStringInt(parsed);
    }
    OffHeapBuf buf = offHeapAllocator.allocate(valueBytes.length);
    buf.setBytes(0, valueBytes, 0, valueBytes.length);
    YierdisObject o = new YierdisObject(ValueType.STRING,
            valueBytes.length <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW,
            buf);
    o.rawLen = valueBytes.length;
    return o;
}
```

In each composite value file, delete the heap branch and keep the FFM path as the single body. Example for `HashValue`:

```java
int hset(byte[] field, byte[] value) {
    if (mapFfm != null) {
        YierdisFfmBytesRef nextValue = value == null ? null : ffmBlobStore.store(value);
        YierdisFfmBytesRef old = mapFfm.put(field, nextValue);
        if (old == null) {
            rawBytes += (long) field.length + (value == null ? 0 : value.length);
            return 1;
        }
        rawBytes += (value == null ? 0 : value.length) - old.length();
        ffmBlobStore.release(old);
        return 0;
    }
    int pairIndex = indexOfFieldPairFfm(field);
    if (pairIndex >= 0) {
        packedFfm.set(pairIndex + 1, value);
        return 0;
    }
    packedFfm.addLast(field);
    packedFfm.addLast(value);
    return 1;
}
```

In `ListValue`, keep only the FFM-backed packed/quicklist path:

```java
void rpushAll(List<byte[]> values) {
    if (quicklistFfm != null) {
        for (byte[] v : values) {
            qlAddLastFfm(v);
        }
        return;
    }
    for (byte[] v : values) {
        listpackFfm.addLast(v);
    }
    totalSize += values.size();
    if (listpackFfm.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
        convertToQuickListFfm();
    }
}
```

In `SetValue`, keep only the FFM intset/hashset path:

```java
private boolean addOne(byte[] member) {
    long parsed = parseCanonicalLongOrSentinel(member);
    boolean isInt = parsed != Long.MIN_VALUE || isLongMinValueBytes(member);
    if (!isInt) {
        convertToHashSetFfm();
        return hashsetFfm.put(member, PRESENT) == null;
    }
    boolean added = intsetFfm.add(parsed);
    if (added && intsetFfm.size() > YierdisEncodingThresholds.SET_MAX_INTSET_ENTRIES) {
        convertToHashSetFfm();
    }
    return added;
}
```

In `ZSetValue`, keep only the FFM backing store:

```java
int zaddMany(List<byte[]> scoreMemberPairs, boolean[] changedRef) {
    return ffm.zaddMany(scoreMemberPairs, changedRef);
}
```

In `YierdisHllOps` and `YierdisHyperLogLog`, keep HLL payload access on the FFM-backed string path and remove any heap-only rewrite branch that bypasses the object-owned FFM buffer.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyValuePathGuardTest,OffHeapStringStorageTest,UnsafeOffHeapDbSmokeTest,OffHeapLeakRegressionTest test`
Expected: PASS with no assertion failures and no native-memory-leak failures.

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyValuePathGuardTest.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java
git commit -m "refactor: converge value encodings to ffm only"
```

### Task 5: Reconcile Memory Accounting And Observability

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Test: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`

- [ ] **Step 1: Write the failing observability test**

```java
@Test
public void infoAndMemoryStatsDescribeFfmOnlyKernel() throws Exception {
    try (TestServer server = TestServer.start()) {
        try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            client.execute(Arrays.asList(b("SET"), b("k"), b("v")), 1000);

            YierdisClient.JsonReply info = client.execute(Arrays.asList(b("INFO"), b("YIERDIS")), 1000);
            Assert.assertTrue(ok(info.envelope()));
            JsonObject infoObj = objectResult(info.envelope());
            Assert.assertEquals("yierdis", stringField(infoObj, "server"));
            Assert.assertTrue(longField(infoObj, "executor_queue_capacity") > 0L);

            YierdisClient.JsonReply mem = client.execute(Arrays.asList(b("MEMORY"), b("STATS")), 1000);
            Assert.assertTrue(ok(mem.envelope()));
            JsonObject memObj = objectResult(mem.envelope());
            Assert.assertEquals(1L, longField(memObj, "keys_stored_offheap"));
            Assert.assertTrue(longField(memObj, "offheap_used_bytes") > 0L);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=YierdisClientTest#infoAndMemoryStatsDescribeFfmOnlyKernel test`
Expected: FAIL if memory fields or structured `INFO YIERDIS` values still reflect mixed heap/off-heap assumptions.

- [ ] **Step 3: Write minimal implementation**

In `YierdisDbMemoryReporter`, compute stats from FFM-backed truth:

```java
public YierdisMemoryStats memoryStats() {
    checkThread.run();
    long offheapUsed = keyLifecycle.offHeapUsedBytes();
    long total = offheapUsed
            + store.tableOverheadBytesEstimate()
            + expires.tableOverheadBytesEstimate()
            + expires.entryOverheadBytesEstimate();
    return new YierdisMemoryStats(
            maxmemoryBytes,
            usedBytesForMaxmemory(),
            effectiveUsedBytesForMaxmemory(),
            0L,
            ledger.reservedBytes(),
            offheapUsed,
            true,
            store.tableOverheadBytesEstimate(),
            expires.tableOverheadBytesEstimate(),
            expires.entryOverheadBytesEstimate(),
            total,
            true,
            store.size(),
            expires.size(),
            store.rehashing(),
            store.table0Capacity(),
            store.table1Capacity(),
            expires.rehashing(),
            expires.table0Capacity(),
            expires.table1Capacity()
    );
}
```

In `NettyServerInfoProvider`, keep FFM-oriented memory reporting:

```java
sb.append("yierdis_offheap_included_in_maxmemory:").append(memStats.offHeapIncludedInMaxmemory() ? 1 : 0).append("\r\n");
sb.append("yierdis_offheap_used_bytes:").append(memStats.offHeapUsedBytes()).append("\r\n");
sb.append("yierdis_ledger_used_bytes:").append(memStats.heapDataBytesEstimate()).append("\r\n");
```

Update the ledger if any memory-reporting field names remain intentionally different under `Custom Protocol v1`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=YierdisClientTest#infoAndMemoryStatsDescribeFfmOnlyKernel test`
Expected: PASS with `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java \
  yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java \
  yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java
git commit -m "fix: align memory stats and observability with ffm kernel"
```

### Task 6: Tighten Transaction Semantics And Close The Remaining Compatibility Gaps

**Files:**
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/TransactionQueueLimitTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Modify: `README.md`
- Test: `yierdis-client/src/test/java/yier/bubu/redis/client/TransactionQueueLimitTest.java`

- [ ] **Step 1: Write the failing transaction-compatibility tests**

```java
@Test
public void discardAfterAbortRestoresUsableTransactionState() throws Exception {
    try (TestServer server = TestServer.startWithArgs(
            "--transactionQueueMaxCommands", "1",
            "--transactionQueueMaxBytes", "0"
    )) {
        try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
            Assert.assertEquals("QUEUED", stringResult(execute(client, b("SET"), b("k"), b("v"))));
            Assert.assertEquals("ERR Transaction queue is full",
                    stringField(errorObject(execute(client, b("GET"), b("k"))), "message"));
            Assert.assertEquals("OK", stringResult(execute(client, b("DISCARD"))));
            Assert.assertEquals("OK", stringResult(execute(client, b("MULTI"))));
        }
    }
}
```

Add to `RedisSemanticDifferentialTest.java`:

```java
@Test
public void multiExecDiscardMatchRedisForSupportedSubset() throws Exception {
    try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379)) {
        redis.start();
        try (TestServer yierdis = TestServer.start();
             YierdisClient yClient = YierdisClient.connect("127.0.0.1", yierdis.port());
             RedisRespTestClient rClient = RedisRespTestClient.connect(redis.getHost(), redis.getMappedPort(6379))) {

            Assert.assertEquals("OK", stringResult(
                    yClient.execute(Arrays.asList(b("MULTI")), 2000).envelope()));
            Assert.assertEquals("OK", rClient.simpleString("MULTI"));
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#multiExecDiscardMatchRedisForSupportedSubset,TransactionQueueLimitTest#discardAfterAbortRestoresUsableTransactionState test`
Expected: FAIL if queue abort, replay, or reset semantics still differ.

- [ ] **Step 3: Write minimal implementation**

In `TransactionCommands.exec(...)`, keep Redis-style `EXECABORT` handling and replay ordering:

```java
if (tx.aborted()) {
    tx.discard();
    out.error("EXECABORT Transaction discarded because of previous errors.");
    return;
}

List<ExecutionRequest> queued = tx.drain();
out.arrayHeader(queued.size());
for (ExecutionRequest queuedRequest : queued) {
    try (ExecutionRequest replay = queuedRequest) {
        processor.execute(replay, ctx);
    }
}
```

In `ServerSessionState`, ensure `discard()` resets both queue contents and byte accounting:

```java
public synchronized void discard() {
    for (ExecutionRequest request : queue) {
        request.close();
    }
    queue.clear();
    queuedBytes = 0L;
    active = false;
    aborted = false;
}
```

In `YierdisFastCommandProcessor`, keep MULTI-mode queueing centralized and deterministic:

```java
if (tx != null && tx.active()) {
    boolean isMulti = CommandSupport.asciiEqualsIgnoreCase(request, 0, "MULTI");
    boolean isExec = CommandSupport.asciiEqualsIgnoreCase(request, 0, "EXEC");
    boolean isDiscard = CommandSupport.asciiEqualsIgnoreCase(request, 0, "DISCARD");
    if (!isMulti && !isExec && !isDiscard) {
        String enqueueErr = tx.tryEnqueue(request);
        if (enqueueErr != null) {
            out.error(enqueueErr);
            return;
        }
        out.simpleString("QUEUED");
        return;
    }
}
```

Update the ledger rows for `MULTI/EXEC/DISCARD`, and remove any remaining teaching-oriented wording from `README.md`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#multiExecDiscardMatchRedisForSupportedSubset,TransactionQueueLimitTest#discardAfterAbortRestoresUsableTransactionState test`
Expected: PASS with `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add README.md \
  docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md \
  yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java \
  yierdis-client/src/test/java/yier/bubu/redis/client/TransactionQueueLimitTest.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
  yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java
git commit -m "fix: align transaction subset with redis semantics"
```

## Self-Review Checklist

- Spec coverage:
  This plan covers ledger/docs posture, Redis differential testing, FFM-only storage boundary enforcement, FFM-only value-path convergence, observability, and transaction parity.
- Placeholder scan:
  No `TBD`, `TODO`, or deferred “figure this out later” items remain in the plan body.
- Type consistency:
  The plan consistently refers to `Custom Protocol v1`, `RedisRespTestClient`, `RedisSemanticDifferentialTest`, `FfmOnlyStorageBoundaryGuardTest`, and `FfmOnlyValuePathGuardTest`.
