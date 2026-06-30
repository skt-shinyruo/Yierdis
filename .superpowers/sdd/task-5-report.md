# Task 5 Report: Native Slot Capacity Override, Verification, And Redis Smoke

Date: 2026-07-01
Worktree: `/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison`
Baseline HEAD: `6e1c02fc`

## Summary

Task 5 was completed by replacing the rejected "raise default native slot constants" direction with an explicit `nativeSlotCapacity` override path.

Final outcome:

- Default `YierdisDbStorageComponents.sharedNativeSlotCapacity()` remains `256 * 1024` (`262144`)
- The explicit benchmark/smoke override value is `2_097_152`
- Release suite current-side server overrides also set `databases = 1`
- The override is only applied through explicit argv/override wiring in benchmark suite release scenarios
- The old direct-constant-raise direction was revoked
- Fresh Redis smoke completed and the 5 required release scenarios are now comparable instead of failing with `current is not clean`

Post-review follow-up fixes completed after the smoke acceptance work:

- external Redis `AUTH` / `SELECT` are now honored consistently in readiness checks, per-pass admin setup, observation capture, and benchmark workload connections
- non-suite entrypoints now reject Redis-specific CLI options, including explicit uses that match the default values
- suite validation now rejects negative `redisDb` values and Redis label collisions with `current` / `baseline`

## Adopted Fix

The accepted solution was:

1. Keep production defaults unchanged
2. Add explicit `nativeSlotCapacity` override plumbing from benchmark args to server args/runtime config to bootstrap to engine factory to db/storage construction
3. Apply the higher slot capacity only where benchmark comparison smoke needs it
4. Keep the override explicit in argv/override paths rather than hidden code paths

Chosen release smoke override:

- `nativeSlotCapacity = 2_097_152`
- `databases = 1`

Where it applies:

- Release-suite scenario overrides for suite-started current-side Yierdis runs in benchmark comparison flow
- Specifically for:
  - `release-set-get-128b-c32-p4`
  - `release-set-get-256b-c64-p8`
  - `release-set-get-1024b-c64-p8`
  - `release-append-256b-c64-p8`
  - `release-hll-sparse-c64-p8`

What was explicitly not done:

- No production default slot increase
- No hidden bootstrap-only special case
- No spec/plan file modifications

## Files Changed

Production:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioDefinition.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`

Tests:

- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchComparisonExecutionTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchServerArgsTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteProfileFactoryTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbConstructionTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`

## RED Test Evidence

The old in-flight direction had incorrectly changed the default slot constants and related assertions. Before the override fix:

1. `YierdisDbConstructionTest` failed because default shared capacity had been changed from the intended `262144` to `524288`
2. `NativeStorageRegressionTest.defaultSharedNativeSlotCapacityStillOverflowsAroundNinetyThousandStringKeys` failed because the direct constant raise made the "default still overflows" regression unexpectedly pass

This established the required RED proof for the rejected default-raise direction.

## GREEN Test Evidence

After implementing the explicit override path and restoring defaults:

- `YierdisDbStorageComponents.sharedNativeSlotCapacity()` again returns `256 * 1024`
- Default behavior still overflows around the prior ceiling
- Explicit override behavior supports the larger smoke workload without the prior slot-limit failure
- Benchmark/server arg round-trip and scenario override wiring are preserved

Key GREEN-tested behaviors:

- default shared slot capacity unchanged
- `nativeSlotCapacity = 0` means "use default"
- negative `nativeSlotCapacity` is rejected
- explicit `nativeSlotCapacity` survives benchmark args -> server argv -> runtime config -> bootstrap -> engine factory -> db creation
- release smoke scenarios carry explicit `databases = 1` and `nativeSlotCapacity = 2_097_152`

## Verification Commands And Results

All Java/Maven commands used JDK 25 as required.

### Targeted RED/GREEN-Related Verification

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-benchmark,yierdis-server/yierdis-server-main -am -Dtest=NativeStorageRegressionTest#defaultSharedNativeSlotCapacityStillOverflowsAroundNinetyThousandStringKeys+explicitNativeSlotCapacitySupportsNinetyThousandStringKeysWithoutLeaks,YierdisDbConstructionTest#storageComponentsReserveNativeSlotsForEntriesStringsKeysAndCollectionRoots+sharedRuntimeEngineFactoryPreservesDbIndexWhenNativeSlotCapacityOverridesDefault,YierdisBenchServerArgsTest,YierdisServerArgsTest#normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields+nativeSlotCapacityParsesCopiesAndRoundTrips+nativeSlotCapacityAllowsZeroAsDefaultSentinelAndRejectsNegativeValues,SuiteProfileFactoryTest#releaseSmokeStringAndSparseHllScenariosCarryExplicitNativeSlotOverride,YierdisBenchComparisonExecutionTest#comparisonSideContextPreservesExplicitNativeSlotCapacityInServerArgv -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- `BUILD SUCCESS`

### Related Test Classes

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-benchmark,yierdis-server/yierdis-server-main -am -Dtest=NativeStorageRegressionTest,YierdisDbConstructionTest,YierdisServerArgsTest,YierdisBenchServerArgsTest,SuiteProfileFactoryTest,YierdisBenchComparisonExecutionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- `BUILD SUCCESS`

### Focused Benchmark Tests

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,SuiteRunnerOrchestrationTest,ObservationClientTest,BenchHarnessExtendedWorkloadTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- `BUILD SUCCESS`
- `Tests run: 64, Failures: 0, Errors: 0, Skipped: 0`

### Whole-Branch Follow-Up Verification

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,ObservationClientTest,BenchHarnessExtendedWorkloadTest,YierdisBenchSuiteEntrypointTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- `BUILD SUCCESS`
- `Tests run: 53, Failures: 0, Errors: 0, Skipped: 0`
- this rerun had to execute unsandboxed because the benchmark test fixtures bind local `ServerSocket` ports

### Full Benchmark Module Tests

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

Result:

- `BUILD SUCCESS`
- `Tests run: 150, Failures: 0, Errors: 0, Skipped: 0`

### Package

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark,yierdis-server/yierdis-server-main -am -DskipTests package
```

Result:

- `BUILD SUCCESS`
- Produced:
  - `/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison/yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar`
  - `/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison/yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar`

## Fresh Redis Smoke

`redis-server` was not installed locally, so the smoke used the externally
managed Redis endpoint already prepared for this task.

Deviation from the task brief:

- The briefed local command was `redis-server --save '' --appendonly no --port 6379`
- That exact flow was not possible in this environment because `redis-server`
  is not installed locally
- The design and implementation both treat Redis as an externally managed
  endpoint, so the acceptance smoke used a clean externally managed Redis
  instance at `127.0.0.1:6380` instead

Redis endpoint used for the final clean run:

- host: `127.0.0.1`
- port: `6380`

Acceptance smoke command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar --suite --suiteProfile release --includeRedis --redisHost 127.0.0.1 --redisPort 6380 --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --reportDir target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628
```

Result:

- Completed successfully
- Produced:
  - `target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628/suite-result.json`
  - `target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628/metrics.csv`
  - `target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628/comparisons.csv`
  - `target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628/report.md`

Smoke artifact directory:

- `/home/feng/code/project/Yierdis/.worktrees/codex-redis-suite-comparison/target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628`

Discarded prior smoke attempts:

- `target/benchmark-reports/redis-comparison-smoke-rerun-20260701-0044`
- `target/benchmark-reports/redis-comparison-smoke-rerun-2`

These were discarded because overlapping host-side benchmark parent processes
caused `Address already in use` contamination.

Discarded partial reruns:

- `target/benchmark-reports/redis-comparison-smoke-clean-20260701-0104`

## Follow-Up Fix: External Redis Auth/DB Suite Wiring

Summary:

- External Redis suite passes now authenticate and select the configured DB before readiness probes, `FLUSHDB`, observation capture, and benchmark traffic.
- The same auth/select bootstrap now covers both extended `BenchHarness` workloads and core `YierdisBench` worker workloads when suite mode targets external Redis.
- Redis suite options are rejected outside `--suite`, `redisDb` must be non-negative, and `redisLabel` collisions are rejected.

Files changed for this follow-up:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchHarness.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/BenchWorkloadRequest.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ObservationClient.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/BenchHarnessExtendedWorkloadTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/YierdisBenchSuiteEntrypointTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/ObservationClientTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- `docs/project-docs/client-and-bench-internals.md`

RED evidence:

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,BenchHarnessExtendedWorkloadTest,ObservationClientTest test
```

Result:

- `BUILD FAILURE`
- Initial RED point was compile-time failure because `BenchWorkloadRequest` could not yet carry Redis auth/db settings into workload connections.
- After adding the failing tests, subsequent RED runs also showed missing auth/select ordering and suite-only validation gaps until the production wiring was added.

GREEN evidence:

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest,BenchHarnessExtendedWorkloadTest,ObservationClientTest test
```

Result:

- `BUILD SUCCESS`
- `Tests run: 49, Failures: 0, Errors: 0, Skipped: 0`

Notable verification detail:

- The loopback socket tests require unsandboxed execution in this environment because sandboxed runs failed with `ServerSocket` `Operation not permitted`.

Behavior verified by GREEN tests:

- External Redis readiness/auth path sends `AUTH`/`SELECT` before `PING`.
- External Redis per-pass reset sends `AUTH`/`SELECT` before `FLUSHDB`.
- Observation capture and Redis environment metadata capture send `AUTH`/`SELECT` before `INFO` and `MEMORY STATS`.
- Extended workloads authenticate/select before both prefill and timed commands.
- Core workloads routed through `YierdisBench` workers authenticate/select only for external Redis requests.
- Explicit Redis-specific CLI options are rejected outside `--suite`, even when a user explicitly passes default-valued flags such as `--redisPort 6379`.
- `target/benchmark-reports/redis-comparison-smoke-rerun-20260701-0111`

These runs did not reach final report artifact emission and were not used as
acceptance evidence.

## Required Comparability Checks

The task required confirming that the following scenarios no longer fail because `current is not clean`:

- `release-set-get-128b-c32-p4`
- `release-set-get-256b-c64-p8`
- `release-set-get-1024b-c64-p8`
- `release-append-256b-c64-p8`
- `release-hll-sparse-c64-p8`

Final clean smoke result from `comparisons.csv` and `suite-result.json`:

- `release-set-get-128b-c32-p4`: `comparable=true`
- `release-set-get-256b-c64-p8`: `comparable=true`
- `release-set-get-1024b-c64-p8`: `comparable=true`
- `release-append-256b-c64-p8`: `comparable=true`
- `release-hll-sparse-c64-p8`: `comparable=true`

These scenarios also recorded `errors = 0` for current-side repeat runs in `report.md`.

Important scope note:

- `release-maxmemory-eviction` remains non-comparable with reason `external Redis config required; current is not clean`
- That scenario was already expected to need special external Redis config and is not one of the 5 required release smoke scenarios called out by the task

## Final Task Status

- Task 5 status: `DONE`
- Acceptance evidence directory: `target/benchmark-reports/redis-comparison-smoke-clean-20260701-012628`
- The 5 required release scenarios are now comparable on the current-side path
- Default shared native slot capacity remains unchanged at `262144`

## Default And Override Values

Required final state:

- Default shared slot capacity: `256 * 1024` (`262144`)
- Override slot capacity used for release smoke current-side path: `2_097_152`
- Release smoke current-side explicit DB count override: `1`

Where default remains in force:

- Regular production/default db construction
- Any path not explicitly passing a positive `nativeSlotCapacity`
- Any path using `nativeSlotCapacity = 0`

Where override applies:

- Benchmark suite release scenarios with explicit `ServerOverrides.databasesAndNativeSlots(1, 2_097_152)`

## Old Direction Revoked

The previously observed in-flight direct-constant-raise direction was removed from the accepted solution:

- shared slot default is no longer raised above `256 * 1024`
- tests no longer assert the inflated default
- the accepted implementation uses only explicit override wiring

## Commit Status

- No commit created

## Final Assessment

- RED evidence: captured from the rejected direct-default-raise direction
- GREEN evidence: captured for restored default behavior and explicit override behavior
- Focused benchmark tests: passed
- Full benchmark module tests: passed
- Package: passed
- Fresh Redis smoke: passed
- Required 5 release scenarios: comparable again, no longer failing due to `current is not clean`

Overall task status: `DONE`
