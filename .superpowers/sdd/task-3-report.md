Task 3 report: Encode Redis Scenario Compatibility And Comparison Reasons

Scope
- Implemented Redis comparability metadata on `ScenarioDefinition`.
- Tagged Redis-incompatible release scenarios in `SuiteProfileFactory`.
- Derived Redis-specific non-comparable reasons in `ScenarioComparison`.
- Added focused tests using `RedisSuiteTestSupport.cleanPass(...)`.

TDD evidence

RED
- Added failing tests:
  - `SuiteRunnerOrchestrationTest.maxmemoryScenarioAgainstRedisIsNotComparableWithoutExplicitSupport`
  - `SuiteRunnerOrchestrationTest.nativeDefragScenarioAgainstRedisIsNeverComparable`
  - `YierdisBenchComparisonRenderTest.renderFailureComparisonIncludesRedisSpecificNonComparableReason`
- Ran exact brief command:
  - `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Observed expected RED:
  - test compile failed because `ScenarioDefinition.RedisComparable` and `scenario.redisComparable()` did not exist yet
  - first RED finished at `2026-06-30T17:28:27+08:00`

Implementation
- Extended `ScenarioDefinition` with:
  - `RedisComparable { YES, EXTERNAL_CONFIG_REQUIRED, NO }`
  - `redisComparable`
  - `redisNonComparableReason`
- Preserved existing constructors by defaulting existing scenarios to `YES` and blank reason.
- Marked exact release scenarios:
  - `release-native-defrag-append` -> `NO`, `yierdis-only native defrag scenario`
  - `release-maxmemory-eviction` -> `EXTERNAL_CONFIG_REQUIRED`, `external Redis config required`
- Updated `ScenarioComparison.compare(...)` path to append the scenario Redis non-comparable reason when the baseline artifact label is `redis` and the scenario is not Redis-comparable.

GREEN
- Re-ran the same exact brief command after implementation.
- Needed unsandboxed execution because `SuiteRunnerOrchestrationTest.realRedisPassUsesArtifactAwareObservationCapture` opens a local `ServerSocket`; sandboxed execution failed with `java.net.SocketException: Operation not permitted`.
- Final GREEN command:
  - `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Final result:
  - `Tests run: 19, Failures: 0, Errors: 0, Skipped: 0`
  - `BUILD SUCCESS`
  - finished at `2026-06-30T17:32:18+08:00`

Notes
- The new render test models the Redis non-comparable reason through the existing `YierdisBench.ComparisonResult` contract by using a failed-partial baseline side plus the reason in `environmentCaveat`. No production rendering code was changed in this task.
