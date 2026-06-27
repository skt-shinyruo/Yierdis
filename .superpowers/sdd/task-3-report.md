Task 3 Report: Add Explicit Server Command Composition

Initial blocker:
- `TransactionCommands` is package-private in `yierdis-command-core`, so the original brief shape (`new TransactionCommands(processor::execute)` from `server-main`) could not compile from `server-main`.

Resolution applied:
- Expanded write scope by one file: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Added a public neutral helper: `CommandRegistries.registerTransactionSupport(CommandRegistry registry, QueuedCommandReplayer replayer)`
- Kept `TransactionCommands` package-private and internal to command-core

TDD sequence:
1. Added the new Task 3 server composition test to `YierdisServerBootstrapCommandWiringTest`
2. Ran red:
   - `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test`
   - Observed expected red state from missing `ServerCommandComposition` and stale `TestYierdisEngines` wiring
   - Also found the brief’s exact `FastTestClient` snippet was not compileable in `server-main` on this branch because that module does not expose `yier.bubu.redis.testutil` as a test dependency
3. Implemented the minimal feasible production change set within the allowed files plus `CommandRegistries.java`
4. Kept the Task 3 behavior assertion in `YierdisServerBootstrapCommandWiringTest` by driving the composed processor through a local `DefaultYierdisEngine` + narrow session/reply-writer harness inside the test file
5. Ran green:
   - `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerBootstrapCommandWiringTest,ClosingSkipSideEffectsIntegrationTest,NettyExecutionAdapterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
   - Result: `BUILD SUCCESS`

Implementation summary:
- Added `ServerCommandComposition.createProcessor(...)` in `server-main`
- `ServerCommandComposition` now:
  - creates a `CommandRegistry`
  - creates `YierdisFastCommandProcessor(options, registry)`
  - registers transaction support via `CommandRegistries.registerTransactionSupport(registry, processor::execute)`
  - registers default modules plus `ServerCommandModule`
  - returns the processor
- Migrated `YierdisServerBootstrap` to construct `DefaultYierdisEngine` from the explicit processor
- Migrated `TestYierdisEngines` to the same execution-only processor/engine composition shape

Files changed:
- `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`

Self-review:
- Verified write set stayed inside the permitted files plus approved `CommandRegistries.java`
- Verified `TransactionCommands` remained package-private
- Verified server-only commands (`HELLO`/`INFO`/`STATS`) stayed in `server-main`
- Verified focused Task 3 suite passed with JDK 25 prefix

Concern:
- The brief’s exact sample assertion path using `yier.bubu.redis.testutil.FastTestClient` is not directly compileable from `server-main` in this branch, so the final test uses an in-file narrow engine/session harness to assert the same composed behavior (`PING` from default commands, `HELLO` from server commands) without expanding dependencies or touching the POM.
