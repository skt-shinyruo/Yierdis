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

---

Task 3 fix report: Redis comparability must not depend on artifact display label

What changed
- Replaced the Redis comparability gate in `ScenarioComparison.addRedisScenarioCompatibility(...)` to use `ScenarioPassResult.artifactKind()` instead of `artifactLabel().equals("redis")`.
- Extended `ScenarioPassResult` to retain stable artifact identity with `SuiteArtifact.Kind`.
- Threaded artifact kind through `SuiteRunner` pass creation and stop/failure paths so comparison logic has the real artifact type available.
- Added a focused regression test for a custom Redis label:
  - `SuiteRunnerOrchestrationTest.maxmemoryScenarioAgainstCustomLabeledRedisIsStillNotComparable`
- Updated Redis pass test support to construct passes with explicit artifact kind where needed.
- Updated directly affected tests and helper call sites to provide `SuiteArtifact.Kind.YIERDIS_JAR` explicitly after the `ScenarioPassResult` shape change.

Why this fixes the review finding
- `--redisLabel` is presentation-only and can be customized, so it is not a stable indicator of Redis identity.
- `SuiteArtifact.Kind.EXTERNAL_REDIS` is stable and already represents Redis identity in suite configuration.
- Comparison gating now follows that stable identity all the way from artifact configuration into the comparison layer.

Files changed
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioComparison.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/ScenarioPassResult.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteRunner.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/RedisSuiteTestSupport.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteRunnerOrchestrationTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteMetricSummaryTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteReportWriterTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteThresholdEvaluatorTest.java`

TDD evidence
- RED:
  - Added the custom-label Redis regression test first.
  - Ran:
    - `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteRunnerOrchestrationTest,YierdisBenchComparisonRenderTest -Dsurefire.failIfNoSpecifiedTests=false test`
  - Observed expected failure before production was updated:
    - test compile failed because `ScenarioPassResult` did not yet carry stable artifact identity needed by the new test support path
- GREEN:
  - Re-ran the same focused command after the fix.
  - Sandbox run showed one environment limitation:
    - `SuiteRunnerOrchestrationTest.realRedisPassUsesArtifactAwareObservationCapture` failed with `java.net.SocketException: Operation not permitted` because local socket binding is blocked in the sandbox
  - Re-ran unsandboxed with the same Maven command.
  - Final result:
    - `Tests run: 20, Failures: 0, Errors: 0, Skipped: 0`
    - `BUILD SUCCESS`
    - finished at `2026-06-30T19:53:09+08:00`

Self-review
- Scope stayed within Task 3 and the review finding only; no Task 4 behavior was added.
- The change uses an existing stable domain identity (`SuiteArtifact.Kind`) rather than inventing new label conventions.
- The constructor/record shape change was kept minimal and only propagated to directly affected call sites.
- The new regression test would fail if Redis identity were inferred from a custom label again.
- No unrelated workspace changes were reverted; the existing untracked design/plan docs were left untouched.
