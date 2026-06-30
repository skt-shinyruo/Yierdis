# Task 1 Report

## Scope

Implemented Task 1 in the assigned worktree:

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchArgs.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteArtifact.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/suite/SuiteConfig.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/suite/SuiteConfigTest.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/SuiteEntrypointConfigTest.java`

Preserved existing Yierdis-only suite behavior while adding Redis-first-class suite artifact/config modeling.

## TDD Evidence

### RED

Added failing tests first:

- `SuiteConfigTest.suiteIncludeRedisBuildsExternalRedisArtifact`
- `SuiteConfigTest.suiteIncludeRedisRejectsBaselineJarCombination`
- `SuiteEntrypointConfigTest.suiteConfigFromEntrypointArgsCarriesRedisSettings`

Ran the exact focused command from the brief:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am -Dtest=SuiteConfigTest,SuiteEntrypointConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Observed failure before implementation. The build failed at test compilation because the required Redis suite API was missing:

- `SuiteConfig` had no `artifactsInRunOrder()`
- `SuiteArtifact` had no `Kind`

Representative failure lines:

```text
cannot find symbol: method artifactsInRunOrder()
cannot find symbol: variable Kind
```

### GREEN

Implemented the minimal production changes:

- Added Redis suite CLI options to `YierdisBenchArgs`
- Expanded `SuiteArtifact` to model `YIERDIS_JAR` and `EXTERNAL_REDIS`
- Added `SuiteConfig.artifactsInRunOrder()`
- Added Redis artifact creation and invalid combination rejection in `SuiteConfig.from(...)`
- Kept jar-only constructor compatibility for existing call sites

Re-ran the exact focused command.

The sandboxed run still failed due to pre-existing socket-using tests in `SuiteEntrypointConfigTest`:

```text
java.net.SocketException: Operation not permitted
```

Re-ran the same command outside the sandbox so those existing tests could bind a local port. Result:

```text
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Behavior Implemented

- `--includeRedis` adds a Redis artifact ahead of `current` in suite run order
- Redis suite defaults:
  - `redisHost=127.0.0.1`
  - `redisPort=6379`
  - `redisLabel=redis`
  - `redisDb=0`
- Optional Redis auth fields:
  - `redisUser`
  - `redisAuth`
- `--includeRedis` rejects the `--baselineServerJar` combination
- Baseline/current Yierdis-only suite behavior remains unchanged

## Commit

Committed after the GREEN verification step with subject:

- `feat: add redis suite artifact config`
