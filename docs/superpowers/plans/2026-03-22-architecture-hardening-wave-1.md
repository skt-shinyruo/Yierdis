# Architecture Hardening Wave 1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the highest-risk architectural debt identified in the code review by making runtime thread/lifecycle rules explicit, shrinking the server executor blast radius, and extracting real collaborators out of `YierdisDb`.

**Architecture:** Execute three disjoint refactor tracks in parallel. First, remove runtime self-contradiction where public `DbEngine` views are immediately cast back to `RuntimeDbEngine`, and centralize owner-thread maintenance/close rules. Second, split `NettyCommandExecutor` into smaller server-side collaborators so queueing/backpressure policy and command execution/reply handling stop living in one file. Third, extract expiration and maxmemory support from `YierdisDb` into dedicated package-local collaborators, reducing the size and responsibility density of the DB core without changing wire semantics.

**Tech Stack:** Java 17, Maven multi-module reactor, Netty 4, JUnit 4

**Status:** Completed in worktree `architecture-remediation`.

**Verification completed on 2026-04-01:**
- `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceTest,YierdisServerBootstrapCloseTest,NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest,ExpireIndexTest,ExpireSemanticsTest,MaxmemoryDoubleReplyRegressionTest,GlobalMaxmemoryLruAcrossDbsTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
- `mvn test`
- `./scripts/smoke.sh`

---

### Task 1: Runtime Lifecycle Boundary Cleanup

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java`
- Create: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`

- [x] **Step 1: Add focused regression coverage**

Cover:
- maintenance should not require `DbEngine -> RuntimeDbEngine` cast at call sites
- owner-thread close still runs on the executor thread
- server bootstrap still schedules maintenance and shutdown correctly after the refactor

- [x] **Step 2: Introduce explicit runtime-only access**

Add a package-local runtime access helper in `yierdis-core-runtime` that exposes runtime engines and owner-thread-only operations without reusing the public `DbEngine` API surface.

- [x] **Step 3: Refactor maintenance and close flows**

Update `YierdisInstanceMaintenance` and `YierdisServerBootstrap` so they use the new runtime helper instead of:
- reading public `DbEngine` views
- casting them back to `RuntimeDbEngine`
- manually scattering owner-thread rules across bootstrap code

- [x] **Step 4: Run focused runtime/server tests**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am -Dtest=YierdisInstanceTest,YierdisServerBootstrapCloseTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS


### Task 2: Split `NettyCommandExecutor` Responsibilities

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandSubmitter.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandDrainLoop.java`
- Create: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutionSupport.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorBackpressureTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/NettyCommandExecutorFairSchedulingTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ClosingSkipSideEffectsIntegrationTest.java`

- [x] **Step 1: Preserve current behavior with focused tests**

Use the existing executor suites as the guardrail for:
- reject reasons and busy replies
- drain fairness/time-budget behavior
- close-after-reply and skip-on-closing semantics
- backpressure enter/exit behavior

- [x] **Step 2: Extract submission/backpressure orchestration**

Move the `trySubmitWithReason(...)` path plus the associated budget/accounting transitions into `NettyCommandSubmitter`, keeping `NettyCommandExecutor` as the façade.

- [x] **Step 3: Extract drain/execution orchestration**

Move `scheduleDrain`, `drainLoop`, `executeOne`, and the related reply/finish bookkeeping into a dedicated drain-loop collaborator. Keep public behavior and constructor signatures stable unless tests require a minimal adjustment.

- [x] **Step 4: Re-run focused executor/server tests**

Run: `mvn -pl yierdis-server -am -Dtest=NettyCommandExecutorTest,NettyCommandExecutorBackpressureTest,NettyCommandExecutorFairSchedulingTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS


### Task 3: Extract Expiration and Maxmemory Support From `YierdisDb`

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationManager.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/ExpireIndexTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MaxmemoryDoubleReplyRegressionTest.java`

- [x] **Step 1: Lock behavior with focused regression tests**

Cover:
- expiration cleanup still respects time budgets and lazy-delete semantics
- maxmemory candidate sampling/eviction behavior is unchanged
- global maxmemory integration still works across DBs

- [x] **Step 2: Extract expiration support**

Move expiration-cleanup-specific logic out of `YierdisDb` into a package-local collaborator that owns the cleanup loop and related helper methods, while keeping the DB as the state owner.

- [x] **Step 3: Extract maxmemory support**

Move maxmemory candidate selection, eviction, and related helper logic out of `YierdisDb` into a package-local collaborator. The DB should delegate to it rather than inline the full algorithm.

- [x] **Step 4: Re-run focused DB/runtime tests**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ExpireIndexTest,ExpireSemanticsTest,MaxmemoryDoubleReplyRegressionTest,GlobalMaxmemoryLruAcrossDbsTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS


### Task 4: Strengthen Boundary Guardrails and Docs

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-19-architecture-boundary-repair-design.md` (only if boundary wording is now stale)

- [x] **Step 1: Extend guardrails for the new seams**

Add checks that fail when:
- runtime maintenance code casts public `DbEngine` views back to `RuntimeDbEngine`
- `YierdisServerBootstrap` regains inline owner-thread lifecycle logic that belongs in runtime helpers
- the extracted DB collaborators drift back into `YierdisDb`

- [x] **Step 2: Update the documented architecture**

Document the new runtime helper seam and the extracted DB collaborators so future changes follow the same direction instead of re-inlining logic.

- [x] **Step 3: Run focused guardrail tests**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS


### Task 5: Integrated Verification

**Files:**
- No new files expected

- [x] **Step 1: Run the targeted multi-module suites**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-server -am test`
Expected: PASS

- [x] **Step 2: Review diff shape**

Run: `git diff --stat`
Expected: runtime/server/core-db changes align with the three planned tracks and guardrail/docs updates.

- [x] **Step 3: Run repository smoke verification if targeted suites pass cleanly**

Run: `mvn test`
Expected: PASS
