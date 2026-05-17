# DB Native Allocator Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded production-hardening validation for DB native allocator churn, cleanup, metric stability, maxmemory pressure, and lightweight smoke execution.

**Architecture:** Keep Track 4 validation-only: extend existing DB/allocator regression tests and reuse the current smoke/bench scripts instead of introducing new allocator policies, storage behavior, or benchmark framework code. Assertions must be deterministic but not brittle: prefer zero-after-shutdown, relative cleanup, bounded profiles, and per-run consistency over exact global counter snapshots.

**Tech Stack:** Java 25, Maven, JUnit 4, Yierdis DB memory module, Yierdis FFM memory module, Bash smoke/bench scripts.

---

## Scope

This plan implements only the child spec at `docs/superpowers/specs/2026-05-16-db-native-allocator-production-hardening-design.md`.

In scope:

- deterministic repeated native DB churn/soak coverage
- repeated native leak cleanup assertions over DB shutdown cycles
- allocator and DB defrag metric stability assertions
- narrow maxmemory/native allocator regression coverage
- lightweight CI-style smoke entry point that reuses existing scripts and tiny workloads

Explicitly out of scope:

- parent roadmap rewrites
- allocator policy redesign
- scan/snapshot semantic changes
- key-byte migration work
- collection nativeization beyond existing root validation
- new benchmark framework
- broad CI infrastructure changes

Protected files:

- Do not modify `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`.
- Do not modify, stage, or commit `yierdis.md`.

## Files

- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
  - Add repeated DB churn/soak helpers and tests.
  - Add repeated shutdown cleanup assertions for key bytes, string bytes, collection roots, scan/snapshot quarantine, and runtime cleanup.
  - Add narrow maxmemory/native allocator regression test.
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbDefragMaintenanceTest.java`
  - Add bounded defrag metric stability coverage over repeated identical cycles.
- Modify: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
  - Add repeated allocator cleanup/profile stability coverage only if DB-level tests expose an allocator-only gap.
- Modify: `scripts/smoke.sh`
  - Add a tiny allocator-regression smoke mode or environment override that reuses existing startup, command, and bench behavior.
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java`
  - Add a script contract test for the opt-in allocator smoke path.
- Do not modify: `scripts/bench.sh` unless smoke needs an existing pass-through override fixed; do not turn it into a new benchmark framework.

## Maven Command Rule

All Maven commands in this plan use this exact Java 25 prefix style:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

For scripts that run Java, use the same `JAVA_HOME` and `PATH` assignment before the script command.

---

### Task 1: Repeated DB Native Churn Soak

**Goal:** Convert the existing single mixed native DB churn coverage into a bounded repeated soak that proves each iteration returns allocator and DB memory state to baseline.

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`

- [ ] **Step 1: Write the failing test**

Add a new test named `repeatedDeterministicMixedNativeDbChurnReleasesRuntimeEveryCycle`.

Use the existing `deterministicMixedNativeDbChurnPreservesResultsAndReleasesRuntime` body as the operation source, but extract it into a helper so the new test can run multiple independent DB/runtime cycles:

```java
@Test
public void repeatedDeterministicMixedNativeDbChurnReleasesRuntimeEveryCycle() {
    for (int cycle = 0; cycle < 5; cycle++) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-db-mixed-churn-repeat-" + cycle)) {
            YierdisDb db = createNativeRegressionDb(runtime, 2_000_000L, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            try {
                runDeterministicMixedNativeDbChurn(db, 0x5EED_7A11L + cycle, 128);
                assertNativeDbEmpty(db);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals("cycle " + cycle + " leaked runtime bytes", 0L, runtime.usedBytes());
        }
    }
}
```

Expected initial failure: compile failure for missing helpers such as `createNativeRegressionDb`, `runDeterministicMixedNativeDbChurn`, or `assertNativeDbEmpty`.

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#repeatedDeterministicMixedNativeDbChurnReleasesRuntimeEveryCycle -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before implementation, either because helpers do not exist or because the single-cycle code has not yet been extracted.

- [ ] **Step 3: Implement the helper extraction**

Extract the mixed churn body into:

```java
private static YierdisDb createNativeRegressionDb(
        YierdisFfmMemoryRuntime runtime,
        long maxmemoryBytes,
        MaxmemoryPolicy maxmemoryPolicy
) {
    return YierdisDb.createWithSharedFfmRuntime(
            runtime,
            maxmemoryBytes,
            maxmemoryPolicy,
            5,
            5,
            5,
            new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE)
    );
}

private static void runDeterministicMixedNativeDbChurn(YierdisDb db, long seed, int operationCount) {
    Random random = new Random(seed);
    Map<String, String> strings = new HashMap<>();
    Set<String> trackedKeys = new LinkedHashSet<>();
    String[] stringKeys = {"mix:s:0", "mix:s:1", "mix:s:2", "mix:s:3", "mix:s:4"};
    String[] collectionKeys = {"mix:list", "mix:hash", "mix:set", "mix:zset", "mix:hll"};
    boolean expiredTtlCleanup = false;
    boolean scannedAndSnapshotted = false;
    boolean ranDefragMaintenance = false;

    for (int op = 0; op < operationCount; op++) {
        // Move the existing switch from deterministicMixedNativeDbChurnPreservesResultsAndReleasesRuntime here.
        // Keep the same operations and assertions, replacing the hard-coded 128 loop limit with operationCount.
        // Do not add random, time-based, or large workload behavior.
    }

    // Move the existing final model validation, tracked key deletion, and cleanup assertions here.
}

private static void assertNativeDbEmpty(YierdisDb db) {
    YierdisMemoryStats empty = db.memory().memoryStats();
    NativeAllocatorStats allocator = db.keyLifecycle().nativeAllocator().stats();
    Assert.assertEquals(0, db.size());
    Assert.assertEquals(0L, db.usedBytesForMaxmemory());
    Assert.assertEquals(0L, empty.keyCount());
    Assert.assertEquals(0L, empty.usedBytesForMaxmemory());
    Assert.assertEquals(0L, empty.heapDataBytesEstimate());
    Assert.assertEquals(0L, empty.offHeapUsedBytes());
    Assert.assertEquals(0L, empty.totalEstimatedBytes());
    Assert.assertEquals(0L, empty.nativeDefragQuarantinedObjects());
    Assert.assertEquals(0L, empty.nativeDefragQuarantineBytes());
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.STRING_BYTES));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.ENTRY_RECORD));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.KEY_BYTES));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.LIST_NODE));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.HASH_NODE));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.SET_NODE));
    Assert.assertEquals(0L, allocator.objectCount(NativeObjectKind.ZSET_NODE));
    Assert.assertEquals(0L, allocator.logicalUsedBytes());
    Assert.assertEquals(0L, allocator.quarantinedObjects());
}
```

Keep the original `deterministicMixedNativeDbChurnPreservesResultsAndReleasesRuntime` test by changing it to call the helper once. This preserves focused single-cycle coverage while adding repeated soak coverage.

- [ ] **Step 4: Review assertions for boundedness**

Check that the new repeated test:

- uses fixed seeds
- runs exactly 5 cycles and 128 operations per cycle
- asserts cleanup after each cycle, not only after the loop
- does not assert exact global counters such as total defrag moved bytes across all cycles
- does not sleep or depend on wall-clock timing

- [ ] **Step 5: Run verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#deterministicMixedNativeDbChurnPreservesResultsAndReleasesRuntime,repeatedDeterministicMixedNativeDbChurnReleasesRuntimeEveryCycle -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

---

### Task 2: Repeated Shutdown Leak Cleanup

**Goal:** Add repeated execution coverage for shutdown cleanup of key bytes, string bytes, collection roots, scan/snapshot quarantine, and runtime bytes.

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`

- [ ] **Step 1: Write the failing test**

Add a new test named `repeatedNativeShutdownCleanupReleasesKeysStringsCollectionsAndQuarantine`.

Use a short, deterministic loop:

```java
@Test
public void repeatedNativeShutdownCleanupReleasesKeysStringsCollectionsAndQuarantine() {
    for (int cycle = 0; cycle < 6; cycle++) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-shutdown-cleanup-repeat-" + cycle)) {
            YierdisDb db = createNativeRegressionDb(runtime, 0, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            try {
                Assert.assertTrue(db.writes().strings().setString(b("cleanup:string:" + cycle), b("value"), SetMode.NORMAL, null).value());
                writeOneOfEachCollection(db);
                assertCollectionRootCounts(db, 1L);

                try (NativeEpochScope epoch = db.keyLifecycle().nativeAllocator().beginEpoch(NativeEpochKind.SNAPSHOT)) {
                    Assert.assertEquals(Long.valueOf(1L), db.writes().keyspace().del(List.of(b("cleanup:string:" + cycle))).value());
                    YierdisMemoryStats during = db.memory().memoryStats();
                    Assert.assertTrue(during.nativeDefragQuarantinedObjects() > 0L);
                    Assert.assertTrue(during.nativeDefragQuarantineBytes() > 0L);
                }

                YierdisMemoryStats afterEpoch = db.memory().memoryStats();
                Assert.assertEquals(0L, afterEpoch.nativeDefragQuarantinedObjects());
                Assert.assertEquals(0L, afterEpoch.nativeDefragQuarantineBytes());

                Assert.assertEquals(Long.valueOf(4L), db.writes().keyspace().del(List.of(
                        b("list"),
                        b("hash"),
                        b("set"),
                        b("zset")
                )).value());
                assertCollectionRootCounts(db, 0L);
                assertNativeDbEmpty(db);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals("cycle " + cycle + " leaked runtime bytes", 0L, runtime.usedBytes());
        }
    }
}
```

Expected initial failure: either missing extracted helper from Task 1 if this task is attempted independently, or a real cleanup failure if repeated shutdown leaks exist.

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#repeatedNativeShutdownCleanupReleasesKeysStringsCollectionsAndQuarantine -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before implementation is complete or PASS only if the behavior is already correctly covered by extracted helpers and existing code.

- [ ] **Step 3: Implement minimal support**

If Task 1 helpers are not present, add only the minimal `createNativeRegressionDb` and `assertNativeDbEmpty` helpers shown in Task 1. Do not duplicate helper logic. Do not change storage semantics unless the failing assertion identifies a genuine leak.

If a cleanup assertion fails, inspect the specific nonzero counter and fix the narrow release path responsible for it. Keep fixes within native key/value/root cleanup paths; do not mask leaks by weakening assertions.

- [ ] **Step 4: Review cleanup boundaries**

Verify that the test covers both explicit delete cleanup and `db.shutdown()` cleanup. Ensure `NativeEpochScope` is closed before zero-after-epoch assertions and that `runtime.usedBytes()` is asserted after shutdown for every cycle.

- [ ] **Step 5: Run verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#productionCollectionRootsUseSharedNativeAllocatorAndReleaseAfterDeleteAndShutdown,repeatedNativeShutdownCleanupReleasesKeysStringsCollectionsAndQuarantine,snapshotEpochDelaysStringReleaseUntilClosed -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

---

### Task 3: Defrag Metric Stability

**Goal:** Prove DB defrag maintenance metrics remain interpretable across repeated identical runs without freezing exact global totals that could drift.

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbDefragMaintenanceTest.java`

- [ ] **Step 1: Write the failing test**

Add a new test named `repeatedDefragMaintenanceReportsStableBoundedMetrics`.

Use fresh runtimes per cycle and compare per-cycle profiles:

```java
@Test
public void repeatedDefragMaintenanceReportsStableBoundedMetrics() {
    Long expectedMovedObjects = null;
    Long expectedKeyCount = null;
    Long expectedStringCount = null;

    for (int cycle = 0; cycle < 4; cycle++) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db-defrag-stable-repeat-" + cycle)) {
            YierdisDb db = createDefragEnabledDb(runtime, new NativeDefragOptions(1_000_000L, 1_000L, Long.MAX_VALUE));
            db.bindToCurrentThread();
            try {
                populateDefragMetricFixture(db, cycle);
                NativeAllocatorStats before = db.keyLifecycle().nativeAllocator().stats();

                db.defragMaintenance();

                YierdisMemoryStats afterMemory = db.memory().memoryStats();
                NativeAllocatorStats afterAllocator = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertTrue(afterMemory.nativeDefragLastMovedObjects() > 0L);
                Assert.assertTrue(afterMemory.nativeDefragLastMovedBytes() > 0L);
                Assert.assertTrue(afterMemory.nativeDefragMovedBytes() >= afterMemory.nativeDefragLastMovedBytes());
                Assert.assertEquals(afterAllocator.quarantinedObjects(), afterMemory.nativeDefragQuarantinedObjects());
                Assert.assertEquals(afterAllocator.quarantineBytes(), afterMemory.nativeDefragQuarantineBytes());
                Assert.assertEquals(before.objectCount(NativeObjectKind.KEY_BYTES), afterAllocator.objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertEquals(before.objectCount(NativeObjectKind.STRING_BYTES), afterAllocator.objectCount(NativeObjectKind.STRING_BYTES));

                if (expectedMovedObjects == null) {
                    expectedMovedObjects = afterMemory.nativeDefragLastMovedObjects();
                    expectedKeyCount = afterAllocator.objectCount(NativeObjectKind.KEY_BYTES);
                    expectedStringCount = afterAllocator.objectCount(NativeObjectKind.STRING_BYTES);
                } else {
                    Assert.assertEquals(expectedMovedObjects.longValue(), afterMemory.nativeDefragLastMovedObjects());
                    Assert.assertEquals(expectedKeyCount.longValue(), afterAllocator.objectCount(NativeObjectKind.KEY_BYTES));
                    Assert.assertEquals(expectedStringCount.longValue(), afterAllocator.objectCount(NativeObjectKind.STRING_BYTES));
                }
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
```

Expected initial failure: compile failure for missing `populateDefragMetricFixture`.

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisDbDefragMaintenanceTest#repeatedDefragMaintenanceReportsStableBoundedMetrics -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before fixture helper is implemented.

- [ ] **Step 3: Implement the fixture helper**

Add:

```java
private static void populateDefragMetricFixture(YierdisDb db, int cycle) {
    Assert.assertTrue(db.writes().strings().setString(b("stable:string:0"), b("hello-" + cycle), SetMode.NORMAL, null).value());
    Assert.assertTrue(db.writes().strings().setString(b("stable:string:1"), b("world-" + cycle), SetMode.NORMAL, null).value());
    Assert.assertEquals(Long.valueOf(1L), db.writes().lists().rpush(b("stable:list"), List.of(b("a"))).value());
    Assert.assertEquals(Long.valueOf(1L), db.writes().hashes().hset(b("stable:hash"), List.of(b("f"), b("v"))).value());
    Assert.assertEquals(Long.valueOf(1L), db.writes().sets().sadd(b("stable:set"), List.of(b("m"))).value());
    Assert.assertEquals(Long.valueOf(1L), db.writes().zsets().zadd(b("stable:zset"), List.of(b("1"), b("m"))).value());
}
```

Do not assert exact moved bytes across cycles. It is acceptable to assert exact per-kind object counts because this fixture owns the objects and uses fresh runtimes.

- [ ] **Step 4: Review metric stability**

Confirm the test compares:

- positive per-cycle `lastMoved*` metrics
- cumulative moved bytes only as `>= lastMovedBytes`
- stats-to-memory metric consistency
- object counts before and after defrag
- fresh runtime cleanup to zero after every cycle

- [ ] **Step 5: Run verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisDbDefragMaintenanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

---

### Task 4: Narrow Maxmemory Native Allocator Regression

**Goal:** Catch cleanup/accounting regressions when native allocator-backed DB writes run under a small maxmemory limit.

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`

- [ ] **Step 1: Write the failing test**

Add a new test named `nativeAllocatorCleanupRemainsStableUnderNarrowMaxmemory`.

Use a small deterministic workload and assert cleanup, not a new eviction policy:

```java
@Test
public void nativeAllocatorCleanupRemainsStableUnderNarrowMaxmemory() {
    for (int cycle = 0; cycle < 4; cycle++) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-maxmemory-repeat-" + cycle)) {
            YierdisDb db = createNativeRegressionDb(runtime, 4096L, MaxmemoryPolicy.NOEVICTION);
            db.bindToCurrentThread();
            List<byte[]> written = new ArrayList<>();
            try {
                for (int i = 0; i < 16; i++) {
                    byte[] key = b("maxmemory:" + cycle + ":" + i);
                    byte[] value = b("value-" + i + "-native-maxmemory");
                    boolean accepted = db.writes().strings().setString(key, value, SetMode.NORMAL, null).value();
                    if (!accepted) {
                        break;
                    }
                    written.add(key);
                    assertMemoryStatsCoherent(db);
                    Assert.assertTrue(db.usedBytesForMaxmemory() <= 4096L);
                }

                Assert.assertTrue("expected at least one accepted write", written.size() > 0);
                NativeAllocatorStats populated = db.keyLifecycle().nativeAllocator().stats();
                Assert.assertEquals(db.size(), populated.objectCount(NativeObjectKind.KEY_BYTES));
                Assert.assertTrue(populated.logicalUsedBytes() > 0L);

                Assert.assertEquals(Long.valueOf(written.size()), db.writes().keyspace().del(written).value());
                assertNativeDbEmpty(db);
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
```

Expected initial failure: possible behavior failure if maxmemory accounting rejects too early, leaks after delete, or helper extraction is missing.

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#nativeAllocatorCleanupRemainsStableUnderNarrowMaxmemory -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before helpers exist or PASS if current behavior already satisfies the regression.

- [ ] **Step 3: Implement minimal code changes**

Prefer test-only changes. If production code must change, only fix native cleanup/accounting defects directly exposed by this test. Do not change maxmemory admission semantics unless the current behavior violates existing maxmemory contracts.

If `setString` throws instead of returning `false` under `NOEVICTION`, catch the existing project-specific memory exception type used by maxmemory code and break the loop only after asserting at least one write was accepted.

- [ ] **Step 4: Review maxmemory assertions**

Ensure the test:

- uses `NOEVICTION` and a narrow byte limit
- accepts either bounded successful writes or deterministic rejection after some writes
- asserts cleanup after delete and after shutdown
- avoids exact total byte values other than the configured upper bound

- [ ] **Step 5: Run verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest#nativeAllocatorCleanupRemainsStableUnderNarrowMaxmemory,nativeDbChurnKeepsReporterAndRuntimeAccountingConsistent -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

---

### Task 5: Lightweight Allocator Smoke Entry

**Goal:** Add a CI-style smoke path that reuses existing `scripts/smoke.sh`, keeps the workload tiny, and exercises allocator-sensitive commands without introducing a new benchmark framework.

**Files:**
- Modify: `scripts/smoke.sh`
- Modify: `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SmokeScriptContractTest.java`
- Modify only if required: `scripts/bench.sh`

- [ ] **Step 1: Write the failing script contract test**

Extend `SmokeScriptContractTest` with a contract that reads `scripts/smoke.sh` and asserts the allocator smoke mode exists and is opt-in:

```java
@Test
public void smokeScriptHasOptInAllocatorSensitivePath() throws IOException {
    String script = Files.readString(findRepoRoot().resolve("scripts/smoke.sh"));

    Assert.assertTrue(script.contains("ALLOCATOR_SMOKE=\"${ALLOCATOR_SMOKE:-0}\""));
    Assert.assertTrue(script.contains("[[ \"$ALLOCATOR_SMOKE\" == \"1\" ]]"));
    Assert.assertTrue(script.contains("allocator-sensitive command path"));
    Assert.assertTrue(script.contains("APPEND smoke:native:string -tail"));
    Assert.assertTrue(script.contains("LPUSH smoke:native:list a"));
    Assert.assertTrue(script.contains("HSET smoke:native:hash f v"));
    Assert.assertTrue(script.contains("SADD smoke:native:set m"));
    Assert.assertTrue(script.contains("ZADD smoke:native:zset 1 m"));
    Assert.assertTrue(script.contains("DEL smoke:native:string smoke:native:list smoke:native:hash smoke:native:set smoke:native:zset"));
}
```

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SmokeScriptContractTest#smokeScriptHasOptInAllocatorSensitivePath -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before `scripts/smoke.sh` has the opt-in allocator path.

- [ ] **Step 2: Implement the smoke mode**

In `scripts/smoke.sh`, add an opt-in variable near the smoke config:

```bash
ALLOCATOR_SMOKE="${ALLOCATOR_SMOKE:-0}"
```

After the existing PING/SET/GET block and before the bench invocation, add allocator-sensitive commands that remain tiny:

```bash
  if [[ "$ALLOCATOR_SMOKE" == "1" ]]; then
    printf "[smoke] allocator-sensitive command path\n"
    if redis_cli_available; then
      timeout 10s redis-cli -h "$HOST" -p "$PORT" SET smoke:native:string smoke-value
      timeout 10s redis-cli -h "$HOST" -p "$PORT" APPEND smoke:native:string -tail
      local native_value
      native_value="$(timeout 10s redis-cli -h "$HOST" -p "$PORT" GET smoke:native:string)"
      [[ "$native_value" == "smoke-value-tail" ]] || die "allocator GET smoke:native:string 返回异常：$native_value"
      timeout 10s redis-cli -h "$HOST" -p "$PORT" LPUSH smoke:native:list a
      timeout 10s redis-cli -h "$HOST" -p "$PORT" HSET smoke:native:hash f v
      timeout 10s redis-cli -h "$HOST" -p "$PORT" SADD smoke:native:set m
      timeout 10s redis-cli -h "$HOST" -p "$PORT" ZADD smoke:native:zset 1 m
      timeout 10s redis-cli -h "$HOST" -p "$PORT" DEL smoke:native:string smoke:native:list smoke:native:hash smoke:native:set smoke:native:zset
    else
      printf "[smoke] allocator path skipped: redis-cli unavailable and Java CLI fallback only covers scalar commands\n"
    fi
  fi
```

Keep the existing tiny bench invocation. Do not add a new script unless the implementation review proves `scripts/smoke.sh` cannot reasonably host the mode.

- [ ] **Step 3: Review script behavior**

Check that:

- default `./scripts/smoke.sh` behavior remains unchanged when `ALLOCATOR_SMOKE` is unset
- `ALLOCATOR_SMOKE=1` adds only a few command checks
- no huge request/client/keyspace defaults are introduced
- no new benchmark launcher or framework is introduced
- `scripts/bench.sh` remains unchanged unless a pass-through bug blocks this mode

- [ ] **Step 4: Run script verification**

Build jars first if needed:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -DskipTests package
```

Run the script contract test:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SmokeScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Then run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH ALLOCATOR_SMOKE=1 KEYSPACE=10 DATA_SIZE=8 REQUESTS=50 CLIENTS=1 PIPELINE=1 ./scripts/smoke.sh
```

Expected: PASS with server startup, PING/SET/GET, allocator-sensitive command path, tiny bench, and `done`.

---

### Task 6: Focused Track Verification And Guardrails

**Goal:** Run the smallest complete verification set for Track 4 and confirm protected files were not modified.

**Files:**
- Verify only; no edits expected.

- [ ] **Step 1: Run focused DB memory tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=NativeStorageRegressionTest,YierdisDbDefragMaintenanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 2: Run focused allocator tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 3: Run smoke mode**

Run the smoke script contract test:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SmokeScriptContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Then run the live smoke mode:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH ALLOCATOR_SMOKE=1 KEYSPACE=10 DATA_SIZE=8 REQUESTS=50 CLIENTS=1 PIPELINE=1 ./scripts/smoke.sh
```

Expected: PASS.

- [ ] **Step 4: Run formatting and protected-file checks**

Run:

```bash
git diff --check
git diff --name-only -- docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md yierdis.md
git status --short
```

Expected:

- `git diff --check` has no output.
- protected-file diff command has no output.
- `git status --short` lists only intentional Track 4 files.
- `yierdis.md` is not staged and not modified by this work.

---

## Final Review Checklist

- [ ] Repeated native churn runs are deterministic and bounded.
- [ ] Native runtime bytes return to zero after every DB shutdown cycle.
- [ ] Key bytes, string bytes, entry records, and collection root native object counts return to zero after cleanup.
- [ ] Scan/snapshot epoch quarantine is asserted during the epoch and released after scope close.
- [ ] Defrag metrics are checked with positive/relative/bounded assertions rather than brittle global totals.
- [ ] Maxmemory coverage is narrow and validates cleanup/accounting without changing policy semantics.
- [ ] Smoke mode reuses `scripts/smoke.sh` and existing tiny bench behavior.
- [ ] `scripts/bench.sh` is not turned into a new framework.
- [ ] Parent roadmap spec is unchanged.
- [ ] `yierdis.md` is unchanged, unstaged, and uncommitted.
