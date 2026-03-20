# Architecture Boundary Repair Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the intended module boundaries so protocol/build-info/observability command assembly lives in `yierdis-server`, runtime only owns DB lifecycle/routing, and boundary regressions fail fast in tests/build rules.

**Architecture:** Introduce a small command-registration extension point in `yierdis-core-command`, move protocol/build-info/observability commands behind a server-local registrar, and remove command processor construction from `YierdisInstance`. Keep wire behavior unchanged. Defer the deeper session/runtime state redesign, including final ownership of `SELECT/QUIT/COMMAND`, to a follow-up plan after these boundaries are stable.

**Tech Stack:** Java 17, Maven multi-module build, Netty 4, JUnit 4

---

### Task 1: Make Core Command Registration Extensible

**Files:**
- Create: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/...` (add/update focused processor registration tests as needed)

- [ ] **Step 1: Write the failing test**

Add a focused test that constructs a processor with only the default core modules and verifies server-only commands are not implicitly registered by hard-coded constructor logic.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-command -Dtest=*CommandProcessor* test`
Expected: FAIL because `YierdisFastCommandProcessor` still hard-codes all modules in its constructor.

- [ ] **Step 3: Introduce the module extension point**

Create `CommandModule` (or equivalent small registrar interface) that accepts `CommandRegistry` and registers a cohesive command set. Refactor `YierdisFastCommandProcessor` so:
- default core modules are registered through that abstraction
- constructor callers can supply extra modules
- existing command execution behavior is unchanged

- [ ] **Step 4: Run focused tests**

Run: `mvn -pl yierdis-core/yierdis-core-command -Dtest=*CommandProcessor* test`
Expected: PASS

- [ ] **Step 5: Run module tests**

Run: `mvn -pl yierdis-core/yierdis-core-command test`
Expected: PASS

### Task 2: Remove Command Assembly from `YierdisInstance`

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/pom.xml`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: tests that currently call `instance.newCommandProcessor(...)`

- [ ] **Step 1: Write/update failing runtime/bootstrap tests**

Add or update tests that assert runtime can still create/bind DB engines and the server can still build a processor after removing `YierdisInstance.newCommandProcessor(...)`.

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisServerBootstrapCloseTest,CustomProtocolResyncIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because runtime/server code still depends on `newCommandProcessor(...)` and command-side router leakage in `YierdisInstance`.

- [ ] **Step 3: Implement the decoupling**

Remove processor factory helpers from `YierdisInstance`. Keep only DB lifecycle, routing-neutral engine access, and resource ownership there. Update the server bootstrap and affected tests to instantiate `YierdisFastCommandProcessor` directly from a server/test-local router adapter plus server modules/policies.

- [ ] **Step 4: Remove stale runtime dependencies**

Drop no-longer-needed runtime main-scope dependencies on `yierdis-core-command` and `yierdis-protocol-model`. Add explicit server-side dependencies where composition code now uses them directly.

- [ ] **Step 5: Run focused tests**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceBoundaryTest,YierdisServerBootstrapCloseTest,CustomProtocolResyncIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

### Task 3: Move Protocol/Observability Command Registration to `yierdis-server`

**Files:**
- Create: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Create or Move: server-local command implementation file(s) for `HELLO/INFO/STATS`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ServerCommands.java` (remove or trim once server owns the server-facing subset)
- Modify: `yierdis-core/yierdis-core-command/pom.xml`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Test: `yierdis-server/src/test/java/...`

- [ ] **Step 1: Write/update failing server-side tests**

Add or update server tests to verify `HELLO`, `INFO`, and `STATS` still work through the Netty server after command ownership moves. Keep `PING/ECHO/COMMAND/SELECT/QUIT/FLUSHDB` in core for Wave 1 because they remain transport-agnostic or DB-lifecycle behavior; revisit their final ownership in the later connection-state refactor.

- [ ] **Step 2: Run focused server tests to verify they fail**

Run: `mvn -pl yierdis-server -am -Dtest=CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCloseTest,NettyCommandExecutorTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL once Task 1 removes hard-coded server command registration from core defaults and before the server-local registrar is wired in.

- [ ] **Step 3: Implement server-local registrar**

Move protocol/build-info/observability command registration and protocol/build-info usage into `yierdis-server`. Wire the registrar from the server composition root instead of from core defaults. Remove the `protocol-model` dependency from `core-command` if nothing else needs it afterward.

- [ ] **Step 4: Run focused server tests**

Run: `mvn -pl yierdis-server -am -Dtest=CustomProtocolResyncIntegrationTest,YierdisServerBootstrapCloseTest,NettyCommandExecutorTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 5: Run cross-module tests**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-server -am test`
Expected: PASS

### Task 4: Add Boundary Guardrails

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: any module POMs that need new banned-dependency rules
- Modify: `README.md` if the documented boundaries need small wording updates after the refactor

- [ ] **Step 1: Extend the failing guardrail**

Update architecture tests/build rules so they fail when:
- `core-command` imports `yierdis.protocol.*`
- `core-runtime` depends on command assembly concerns again
- `HELLO/INFO/STATS` or their transaction policy drift back into core defaults

- [ ] **Step 2: Run guardrail tests to verify they fail before implementation**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest test`
Expected: FAIL before the boundary fixes are complete.

- [ ] **Step 3: Implement the guardrails**

Strengthen the architecture test and, where build-level enforcement makes sense, add Maven enforcer rules so regressions fail earlier than runtime code review.

- [ ] **Step 4: Run focused guardrail tests**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ArchitectureBoundaryTest test`
Expected: PASS

### Task 5: Final Verification

**Files:**
- No new files expected

- [ ] **Step 1: Run the targeted multi-module suite**

Run: `mvn -pl yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-server -am test`
Expected: PASS

- [ ] **Step 2: Run a repository-level build smoke test**

Run: `mvn test`
Expected: PASS

- [ ] **Step 3: Review diff for boundary regressions**

Run: `git diff --stat` and inspect touched files for accidental cross-module coupling.
Expected: Only planned modules/files changed.

- [ ] **Step 4: Commit in logical slices**

Commit 1: extension-point and server command move
Commit 2: runtime decoupling
Commit 3: boundary guardrails/docs
