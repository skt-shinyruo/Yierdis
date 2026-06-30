## Task 2 Report

### Scope

Implemented Redis-aware harness lifecycle and observation capture in:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`

### TDD Evidence

#### RED

Added failing tests first for:

- Redis observation capture omitting `STATS`
- External Redis lifecycle using `FLUSHDB` and no spawned process handle
- `SuiteRunner` consuming `SuiteConfig.artifactsInRunOrder()` and using artifact-specific observation endpoints

Focused command from brief:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest,SuiteRunnerOrchestrationTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Observed RED progression:

- First RED run failed at test compile due to newly introduced helper/test visibility issues.
- After tightening the tests, RED failed on missing production behavior/interfaces, including:
  - `ObservationClient.capture(SuiteArtifact)` missing
  - external Redis lifecycle not supported by `BenchHarness`

#### GREEN

Implemented the minimal production changes:

- `ObservationClient.capture(SuiteArtifact)` dispatches by artifact kind
- Redis observation capture collects `INFO` and `MEMORY STATS` only
- `BenchHarness.startServer(...)` treats `EXTERNAL_REDIS` as an already-running server, waits for readiness, flushes DB, and returns a null handle
- `SuiteRunner` now consumes `config.artifactsInRunOrder()` and uses artifact host/port for observations

Re-ran the same focused command. In the default sandbox it failed because local socket binding is blocked (`ServerSocket` -> `Operation not permitted`) for existing and new socket-based tests. Re-ran the exact same command outside the sandbox with approval.

Escalated GREEN command result:

- `BUILD SUCCESS`
- `Tests run: 33, Failures: 0, Errors: 0, Skipped: 0`

### Notes

- Existing Yierdis-only behavior remains intact.
- `SuiteRunner` now performs the Task 1 required integration by using `SuiteConfig.artifactsInRunOrder()` for actual run order.
- JaCoCo emitted a non-fatal warning about execution data mismatch for `BenchHarness$PreparedPass` during report generation; Maven test execution still succeeded.

### Review Rework

#### High Finding 1: Redis workloads used `config.host()`

RED:

- Added Redis-specific coverage in `BenchHarnessExtendedWorkloadTest` for:
  - `redisWorkloadHostUsesArtifactHost`
  - `redisDensePrefillUsesArtifactHost`
- Initial focused run in the sandbox confirmed the new suite comparison test was green but could not finish socket-backed workload coverage because `ServerSocket` creation is blocked there (`Operation not permitted`).
- An escalated run exposed that the first workload-path test approach was too broad and failed for protocol reasons, so the test was narrowed to the actual host-selection seam and dense-prefill path.

GREEN:

- `BenchHarness.runIteration(...)` now routes workload requests through `workloadHost(server, config)` instead of unconditionally using `config.host()`.
- Dense HLL prefill in `prepareScenario(...)` now also routes through `workloadHost(server, config)`.
- `BenchHarness.workloadHost(...)` resolves the pass label against `config.artifactsInRunOrder()` and uses the external Redis artifact host for Redis passes while preserving existing Yierdis host behavior.

#### High Finding 2: Redis/current produced no comparisons

RED:

- Added `SuiteRunnerOrchestrationTest.redisCurrentRunProducesComparisonBetweenRedisAndCurrent`.
- This failed before the fix with `expected:<1> but was:<0>` comparisons because `SuiteRunner.comparisons(...)` bailed out when `config.baseline()` was empty and assumed `"baseline"`/`"current"` labels.

GREEN:

- `SuiteRunner.comparisons(...)` now derives the comparison pair from `config.artifactsInRunOrder()` rather than `config.baseline()`.
- Redis/current runs now emit a comparison between the first non-current artifact and the configured current artifact, while baseline/current behavior remains unchanged.

#### Rework Verification

Focused/required command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest,SuiteRunnerOrchestrationTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Final GREEN result after rework:

- `BUILD SUCCESS`
- `Tests run: 37, Failures: 0, Errors: 0, Skipped: 0`

### Final Task 2 Fix

#### Remaining High Finding: real suite observation path still bypassed Redis-safe capture

RED:

- Added `SuiteRunnerOrchestrationTest.realRedisPassUsesArtifactAwareObservationCapture`.
- This uses the real orchestration path:
  - `SuiteRunner`
  - `BenchHarness`
  - `RedisSuiteTestSupport.RedisLikeObservationServer`
- The test failed before the fix on the actual suite path because Redis pass observations still included `STATS`, proving the system was bypassing the Redis-safe capture semantics during real orchestration.

Focused RED command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest#realRedisPassUsesArtifactAwareObservationCapture -Dsurefire.failIfNoSpecifiedTests=false test
```

Escalated RED evidence:

- The new orchestration test failed with an assertion on `STATS` still being present for the Redis pass.

GREEN:

- `SuiteRunner` now routes actual observation capture by artifact kind:
  - external Redis passes use artifact host/port
  - Yierdis passes preserve the existing `config.host()` plus allocated port behavior
- `BenchHarness.captureObservation(String host, int port)` now recognizes endpoints started as external Redis passes and dispatches them through `ObservationClient.capture(SuiteArtifact.externalRedis(...))`, which avoids `STATS`.
- Existing Yierdis observation behavior remains unchanged.

Focused GREEN evidence:

- The same single-test command passed after the fix.

Required covering command after the fix:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=ObservationClientTest,SuiteRunnerOrchestrationTest,BenchHarnessExtendedWorkloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Final GREEN result:

- `BUILD SUCCESS`
- `Tests run: 38, Failures: 0, Errors: 0, Skipped: 0`
