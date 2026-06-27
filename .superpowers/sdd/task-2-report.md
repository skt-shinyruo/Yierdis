# Task 2 Report: Make Processor and Engine Execution-Only

## Outcome

Implemented the Task 2 constructor refactor in the owned files and verified the required focused Maven test command under JDK 25.

## TDD Record

1. Updated the owned tests first:
   - `YierdisFastCommandProcessorModuleTest`
   - `YierdisFastCommandProcessorRegistrationTest`
   - `DefaultYierdisEngineTest`
2. Ran the required focused command and captured the red state:
   - `BUILD FAILURE`
   - Cause: missing explicit-construction APIs, specifically `YierdisFastCommandProcessor(YierdisCommandProcessorOptions, CommandRegistry)` during `DefaultYierdisEngineTest` compilation.
3. Implemented the minimal production changes in:
   - `YierdisFastCommandProcessor`
   - `DefaultYierdisEngine`
4. Re-ran the same focused command and captured the green state:
   - `BUILD SUCCESS`

## Code Changes

### `YierdisFastCommandProcessor`

- Added explicit-construction public APIs:
  - `new YierdisFastCommandProcessor(CommandRegistry registry)`
  - `new YierdisFastCommandProcessor(YierdisCommandProcessorOptions options, CommandRegistry registry)`
- Removed the self-array bootstrap assembly path and deleted:
  - `registerExtraModules(...)`
  - `toArray(...)`
- Kept execute-path behavior unchanged.
- Kept package-private module-based compatibility constructors only so same-package existing tests in `yierdis-command-core` still compile under Maven's module-wide `testCompile` step. These constructors delegate through `CommandRegistries.from(...)` and do not recreate the removed self-bootstrap path.

### `DefaultYierdisEngine`

- Reduced the public constructor surface to:
  - `new DefaultYierdisEngine(YierdisFastCommandProcessor commandProcessor, Runnable maintenanceTick)`
- Kept `execute(...)` and `maintenanceTick()` behavior unchanged.

### Tests

- `YierdisFastCommandProcessorModuleTest`
  - Replaced processor-owned assembly setup with explicit `CommandRegistry` construction.
- `YierdisFastCommandProcessorRegistrationTest`
  - Migrated affected setups to explicit `CommandRegistries.from(...)` plus `new YierdisFastCommandProcessor(registry)`.
- `DefaultYierdisEngineTest`
  - Migrated all affected setups to explicit `CommandRegistry -> YierdisFastCommandProcessor -> DefaultYierdisEngine` construction.

## Verification

Ran exactly:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-command/yierdis-command-core,yierdis-server/yierdis-server-core -am -Dtest=YierdisFastCommandProcessorModuleTest,YierdisFastCommandProcessorRegistrationTest,DefaultYierdisEngineTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

- Red run: `BUILD FAILURE`
- Green run: `BUILD SUCCESS`

## Self-Review

Checked the final diff and searched for remaining old constructor call sites.

Remaining non-owned usages found:

- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`

These files are outside Task 2 ownership, so they were not edited here. This means the task is complete for the owned files and the required focused verification command, but a broader repository-wide build will still need those call sites migrated to the execution-only engine API.
