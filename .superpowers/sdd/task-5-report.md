# Task 5 Report: Verify Full Benchmark Module Behavior And Manual Redis Flow

Date: 2026-06-30
Worktree: `/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison`
Baseline HEAD: `07973922`

## Scope

Task 5 required:

1. Focused benchmark verification tests
2. Full benchmark module tests
3. Packaging benchmark and server jars
4. Manual Redis suite smoke
5. Minimal doc correction only if verification proved one was needed

No documentation change was required from the observed results.

## Environment Notes

- All Maven commands used JDK 25 via:
  `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`
- In the managed sandbox, socket-binding tests failed with `java.net.SocketException: Operation not permitted`.
- Because Task 5 explicitly requires local socket/process verification, the relevant Maven verification commands were rerun unsandboxed.

## Step 1: Focused Benchmark Verification Tests

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am \
  -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,SuiteRunnerOrchestrationTest,ObservationClientTest,BenchHarnessExtendedWorkloadTest,YierdisBenchComparisonRenderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- Initial sandboxed attempt failed because local socket-binding was blocked by the environment, not because of assertion failures.
- Unsandboxed rerun succeeded.
- Final result: `BUILD SUCCESS`
- Test summary: `Tests run: 64, Failures: 0, Errors: 0, Skipped: 0`

## Step 2: Full Benchmark Module Tests

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark -am test
```

Result:

- Initial sandboxed attempt failed for the same socket-binding restriction.
- Unsandboxed rerun succeeded.
- Final result: `BUILD SUCCESS`
- Test summary: `Tests run: 146, Failures: 0, Errors: 0, Skipped: 0`

## Step 3: Package Benchmark And Server Jars

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-benchmark,yierdis-server/yierdis-server-main -am -DskipTests package
```

Result:

- `BUILD SUCCESS`
- Produced:
  - `yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar`
  - `yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar`

Verified artifact paths:

```text
/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison/yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar
/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison/yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar
```

## Step 4: Manual Redis Suite Smoke

Required command sequence from brief:

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

Result:

- Could not execute because `redis-server` is not installed in this environment.
- Exact evidence:

```text
$ which redis-server || type redis-server || ls /usr/bin/redis-server /usr/local/bin/redis-server
redis-server not found
redis-server not found
ls: cannot access '/usr/bin/redis-server': No such file or directory
ls: cannot access '/usr/local/bin/redis-server': No such file or directory
```

- Because the binary is absent, the manual Redis smoke was blocked before launch.
- No Redis smoke output directory or Redis-generated report artifacts were created.

## Documentation Review

- Reviewed the task outcome against the allowed doc touch target:
  `docs/project-docs/client-and-bench-internals.md`
- Verification did not reveal a content mismatch that required correction.
- No documentation file was modified.

## Git / Commit Outcome

- No task-scoped file changes were needed.
- No commit was created.
- Unrelated pre-existing untracked files remained untouched:
  - `docs/superpowers/plans/2026-06-29-redis-suite-comparison.md`
  - `docs/superpowers/specs/2026-06-28-redis-suite-comparison-design.md`

## Final Assessment

- Focused benchmark verification: passed
- Full benchmark module tests: passed
- Benchmark/server packaging: passed
- Manual Redis suite smoke: blocked by missing `redis-server` binary
- Documentation correction: not needed

Overall task status: `DONE_WITH_CONCERNS`

Concern summary:

- The required manual Redis smoke could not be completed on 2026-06-30 because `redis-server` is not installed in the execution environment.
- Socket-binding test verification also required unsandboxed execution because the managed sandbox rejects local listening sockets with `Operation not permitted`.

## Follow-Up: Global Shared Off-Heap Maxmemory Runtime Performance Fix

Date: 2026-06-30
Baseline HEAD for follow-up: `07973922`

### What Changed

- Added a focused runtime regression in `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java` that attaches a global maxmemory coordinator to a stub engine and proves shared off-heap accounting must not call `memoryStats()`.
- Added a cheap `MemoryOps.offHeapUsedBytes()` seam in `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MemoryOps.java`.
- Implemented the cheap path in `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java` and `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryOps.java` by reusing the existing direct native byte accounting path instead of building full `YierdisMemoryStats`.
- Switched `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java` shared global accounting from `engine.memory().memoryStats().offHeapUsedBytes()` to `engine.memory().offHeapUsedBytes()`.
- Extended `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java` so the shared-runtime native accounting boundary is asserted through the new cheap API as well.

The accounting boundary remains unchanged:

- Per-DB `usedBytesForMaxmemory()` still excludes shared FFM off-heap when a global coordinator is attached.
- Shared off-heap is still counted once at the instance/global layer.

### TDD Evidence

RED command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime -am \
  -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

RED result:

- `BUILD FAILURE`
- Failure was the expected pre-fix compile break proving the seam did not exist yet:

```text
/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java:[334,9]
method does not override or implement a method from a supertype
```

GREEN command 1:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime -am \
  -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

GREEN result 1:

- `BUILD SUCCESS`
- Runtime regression summary: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`

GREEN command 2:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime,yierdis-db/yierdis-db-memory -am \
  -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath,YierdisDbMemoryReporterTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

GREEN result 2:

- `BUILD SUCCESS`
- `YierdisDbMemoryReporterTest`: `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0`
- `DbEngineFactoryInjectionTest`: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`

### Files Changed

- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MemoryOps.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryOps.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java`
- `.superpowers/sdd/task-5-report.md`

### Test Commands And Results

- `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime -am -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath -Dsurefire.failIfNoSpecifiedTests=false test`
  Result: RED before implementation (`BUILD FAILURE`), then GREEN after implementation (`BUILD SUCCESS`, `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`)
- `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime,yierdis-db/yierdis-db-memory -am -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath,YierdisDbMemoryReporterTest -Dsurefire.failIfNoSpecifiedTests=false test`
  Result: GREEN (`BUILD SUCCESS`; db-memory `Tests run: 3, Failures: 0, Errors: 0, Skipped: 0`; runtime `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`)

### Self-Review Concerns

- The new `MemoryOps.offHeapUsedBytes()` default implementation still falls back to `memoryStats()` for engines that do not override it. That preserves compatibility while leaving older implementations on the expensive path until they opt in.
- I did not rerun the external Docker Redis smoke here; this fix is covered by focused runtime/db tests and the change is intentionally minimal.

## Follow-Up Refinement: Remove Allocator `stats()` From The Global Hot Path

Date: 2026-06-30

### Review Finding Addressed

- The first follow-up patch removed `memoryStats()` from the global path, but `YierdisDbMemoryReporter.offHeapUsedBytes()` still reached `nativeAllocator.stats()`.
- That meant the hot path still traversed the expensive allocator stats/object-scan path.

### What Changed

- Removed the earlier `MemoryOps.offHeapUsedBytes()` hot-path seam entirely.
- Added a runtime-only shared-off-heap seam to `RuntimeDbEngine`:
  - `globalSharedOffHeapUsageIdentity()`
  - `globalSharedOffHeapUsedBytes()`
- Implemented the seam in `YierdisDb` by exposing the shared `YierdisFfmMemoryRuntime` identity and its cheap `usedBytes()` counter, but only when a global maxmemory coordinator is attached.
- Updated `YierdisInstance.sharedOffHeapUsedBytes(...)` to:
  - dedupe DBs by shared-runtime identity with `IdentityHashMap`
  - count each shared FFM runtime once
  - avoid calling `engine.memory()` or allocator stats on the hot path
- Strengthened the runtime regression so it proves:
  - the global hot path does not touch `memory()`
  - shared off-heap is sampled once even when multiple DBs share the same runtime
- Updated db-memory tests to assert the runtime seam under an attached coordinator, preserving the accounting boundary.

### TDD Evidence For The Refinement

RED command 1:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime,yierdis-db/yierdis-db-memory -am \
  -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath,YierdisDbMemoryReporterTest#globalSharedOffHeapUsedBytesUsesSharedRuntimeCounter \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

RED result 1:

- `BUILD FAILURE`
- Expected missing-seam compile failures before production changes:

```text
cannot find symbol: method globalSharedOffHeapUsageIdentity()
cannot find symbol: method globalSharedOffHeapUsedBytes()
```

RED result 2 during refinement:

- Additional transient failures were resolved while tightening the new test shape:
  - stale test reference to removed `MemoryOps.offHeapUsedBytes()`
  - db-memory assertions that needed an attached coordinator to match the preserved boundary
  - a stale runtime-test `@Override` after removing the `MemoryOps` helper

GREEN command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-runtime,yierdis-db/yierdis-db-memory -am \
  -Dtest=DbEngineFactoryInjectionTest#globalMaxmemoryUsesCheapSharedOffHeapUsagePath,YierdisDbMemoryReporterTest#globalSharedOffHeapUsedBytesUsesSharedRuntimeCounter,YierdisDbMemoryReporterTest#memoryStatsCountsSharedNativeAllocatorLogicalBytesOnce \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

GREEN result:

- `BUILD SUCCESS`
- `YierdisDbMemoryReporterTest`: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`
- `DbEngineFactoryInjectionTest`: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`

### Files Changed In This Refinement

- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/RuntimeDbEngine.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java`

### Self-Review Concerns For The Refinement

- The runtime seam intentionally reports shared off-heap bytes only when a global coordinator is attached, so standalone/per-DB uses still rely on existing `memoryStats()` observability rather than this hot-path helper.
- I kept the behavior minimal and local to runtime admission; no broader observability refactor was done.
