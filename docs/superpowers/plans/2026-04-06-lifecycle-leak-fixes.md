# Lifecycle Leak Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two failure-path resource leaks in instance startup and client connection setup, with regression coverage.

**Architecture:** Add targeted rollback/cleanup logic without changing public APIs. Drive both fixes with regression tests first: one runtime startup rollback test and one client connect cleanup test.

**Tech Stack:** Java 25, Maven, JUnit 4, Netty 4.1

---

### Task 1: Guard Partial Instance Startup

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

- [ ] **Step 1: Write the failing test**

Add a regression test to `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java` that creates one engine successfully and throws on the second create call, then asserts the first engine was shut down:

```java
    @Test
    public void createCleansUpAlreadyCreatedEnginesWhenFactoryFailsMidStartup() {
        List<String> closeOrder = new ArrayList<>();
        DbEngineFactory factory = new DbEngineFactory() {
            private int calls;

            @Override
            public RuntimeDbEngine create(
                    int dbIndex,
                    long maxmemoryBytes,
                    String maxmemoryPolicy,
                    int maxmemorySamples,
                    long evictionTimeLimitMillis,
                    long expireCleanupTimeLimitMillis
            ) {
                if (calls++ == 0) {
                    return new FailingRuntimeDbEngine("db-" + dbIndex, closeOrder);
                }
                throw new IllegalStateException("boom-create-" + dbIndex);
            }
        };

        try {
            YierdisInstance.create(YierdisInstanceConfig.builder()
                    .databases(2)
                    .engineFactory(factory)
                    .build());
            Assert.fail("expected startup failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("boom-create-1", e.getMessage());
            Assert.assertEquals(Arrays.asList("db-0"), closeOrder);
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest#createCleansUpAlreadyCreatedEnginesWhenFactoryFailsMidStartup -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because `closeOrder` is empty.

- [ ] **Step 3: Write minimal implementation**

Update `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java` so `create(...)` cleans up partially-created engines and the shared runtime on failure:

```java
        RuntimeDbEngine[] dbs = new RuntimeDbEngine[databases];
        try {
            for (int i = 0; i < databases; i++) {
                long dbMax = config.maxmemoryBytes();
                if (perDbScope) {
                    dbMax = perDbMaxmemory;
                    if (remainder > 0) {
                        dbMax++;
                        remainder--;
                    }
                }
                dbs[i] = engineFactory.create(
                        i,
                        dbMax,
                        config.maxmemoryPolicy(),
                        config.maxmemorySamples(),
                        config.evictionTimeLimitMillis(),
                        config.expireCleanupTimeLimitMillis()
                );
            }

            // existing global maxmemory wiring

            return new YierdisInstance(config, dbs, memoryRuntime, true);
        } catch (Throwable t) {
            throw startupFailure(t, dbs, memoryRuntime);
        }
```

Add private helpers that shut down non-null engines, close the memory runtime, suppress cleanup failures onto the original throwable, and rethrow as the original runtime/error or wrapped `IllegalStateException`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest#createCleansUpAlreadyCreatedEnginesWhenFactoryFailsMidStartup -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

### Task 2: Guard Client Connect Failure Cleanup

**Files:**
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
- Modify: `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java`

- [ ] **Step 1: Write the failing test**

Add a regression test to `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java` that fails a localhost connect and verifies no new event loop thread remains:

```java
    @Test
    public void failedConnectDoesNotLeakEventLoopThreads() throws Exception {
        java.util.Set<String> before = threadNames();

        try {
            YierdisClient.connect("127.0.0.1", unusedPort());
            Assert.fail("Expected connection failure");
        } catch (java.net.ConnectException expected) {
            // expected
        }

        waitForNoExtraEventLoopThreads(before, 3000);
    }
```

Add helper methods in the test file to:

```java
    private static java.util.Set<String> threadNames() { ... }
    private static int unusedPort() throws java.io.IOException { ... }
    private static void waitForNoExtraEventLoopThreads(java.util.Set<String> before, long timeoutMillis) throws Exception { ... }
```

The wait helper should repeatedly compare current thread names against `before` and fail only if extra names containing `"nioEventLoopGroup"` remain after the timeout.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-client -am -Dtest=YierdisClientTest#failedConnectDoesNotLeakEventLoopThreads -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because a new `nioEventLoopGroup` thread remains alive.

- [ ] **Step 3: Write minimal implementation**

Wrap `bootstrap.connect(...)` in `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisClient.java` with cleanup on failure:

```java
        try {
            Channel channel = bootstrap.connect(host, port).sync().channel();
            return new YierdisClient(group, channel, responses, terminalError);
        } catch (Throwable t) {
            try {
                group.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
            throw t;
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl yierdis-client -am -Dtest=YierdisClientTest#failedConnectDoesNotLeakEventLoopThreads -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

### Task 3: Run Focused Verification

**Files:**
- No code changes

- [ ] **Step 1: Run targeted suite**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-client -am -Dtest=YierdisInstanceTest,YierdisClientTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 2: Run repository verification**

Run: `mvn test`
Expected: PASS
