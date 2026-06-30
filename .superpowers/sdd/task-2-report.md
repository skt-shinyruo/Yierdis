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
