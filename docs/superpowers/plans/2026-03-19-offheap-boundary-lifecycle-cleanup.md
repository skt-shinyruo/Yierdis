# Off-Heap Boundary And Lifecycle Cleanup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make off-heap API usage, allocator ownership, and shutdown error reporting consistent so incremental builds stay stable and off-heap leaks are surfaced instead of silently swallowed.

**Architecture:** Keep `yier.bubu.redis.offheap.api.*` as the core/runtime SSOT, preserve compatibility only where needed for memory backend modules and tests, and make allocator ownership explicit instead of hidden in constructor overloads. Propagate close-time failures through runtime/server lifecycle boundaries so leak detection remains actionable.

**Tech Stack:** Java 17, Maven multi-module build, Netty, JUnit 4

---

### Task 1: Compatibility Boundary Cleanup

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/TestDbs.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/KeyHandleContractTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapBytesViewTtlRegressionTest.java`
- Modify: other runtime tests that still rely on stale constructor or stale `KeyHandle.forOffHeap(...)` call signatures
- Test: `yierdis-core/yierdis-core-runtime`

- [ ] **Step 1: Identify all stale call sites**

Run: `rg -n "new YierdisDb\\(|offHeapAllocator\\(|forOffHeap\\(" yierdis-core/yierdis-core-runtime/src/test/java`
Expected: find tests still depending on constructor/typing assumptions that predate the current off-heap API boundary.

- [ ] **Step 2: Update test utilities to use explicit ownership semantics**

Change `TestDbs` so helper constructors express whether the DB owns the allocator, instead of relying on overloaded constructors with hidden ownership defaults.

- [ ] **Step 3: Update runtime tests to the current API surface**

Replace stale uses of:
- allocator types passed through mismatched imports
- constructor overloads whose semantics changed
- `KeyHandle.forOffHeap(...)` call sites that need the new `OffHeapAddressAllocator` SSOT

- [ ] **Step 4: Run focused runtime test compilation**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime -am test-compile`
Expected: PASS with no stale signature or indirect type-reference failures.

- [ ] **Step 5: Run focused runtime tests that exercise updated call sites**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest,ContractsIntegrationSmokeTest,KeyHandleContractTest,OffHeapBytesViewTtlRegressionTest test`
Expected: PASS


### Task 2: DB Allocator Ownership Cleanup

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: any impacted DB/runtime tests asserting shutdown and ownership behavior
- Test: `yierdis-core/yierdis-core-db`, `yierdis-core/yierdis-core-runtime`

- [ ] **Step 1: Document current constructor/ownership matrix**

Confirm which `YierdisDb` overloads currently imply `ownsOffHeapAllocator=true` and which imply `false`, and map those to actual callers.

- [ ] **Step 2: Make ownership explicit in the API**

Refactor constructor delegation or add named/static construction helpers so `offHeapKeysEnabled` no longer implicitly changes allocator ownership.

- [ ] **Step 3: Preserve compatibility deliberately**

If legacy overloads must remain, make them delegate to the explicit ownership form with behavior that matches the project’s intended contract, and add comments/tests so this does not regress again.

- [ ] **Step 4: Add or update tests for single-DB vs shared-allocator cases**

Cover:
- DB-owned allocator closes exactly once
- shared allocator is not closed by a DB that does not own it
- enabling off-heap keys does not silently flip ownership

- [ ] **Step 5: Run focused DB/runtime tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime -am -Dtest=UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest,OffHeapKeysToggleTest,OffHeapLeakRegressionTest,YierdisInstanceTest test`
Expected: PASS


### Task 3: Leak Signal Propagation

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: existing lifecycle/leak regression tests or add targeted tests in runtime/server modules
- Test: `yierdis-server`, `yierdis-core/yierdis-core-runtime`

- [ ] **Step 1: Inspect where close/shutdown exceptions are swallowed**

Verify all `catch (Throwable ignored)` or `catch (Exception ignored)` paths in runtime/server close flows that hide allocator leak exceptions.

- [ ] **Step 2: Replace blanket swallowing with aggregated failure propagation**

Implement best-effort close that still attempts all cleanup, but rethrows one primary failure and attaches additional close failures as suppressed exceptions.

- [ ] **Step 3: Keep shutdown ordering intact**

Do not regress:
- DB shutdown on owner thread
- executor shutdown before instance close
- allocator close after DB shutdown

- [ ] **Step 4: Add regression coverage**

Add tests that simulate allocator close failure / leak-reporting failure and assert the lifecycle API surfaces it instead of silently ignoring it.

- [ ] **Step 5: Run focused lifecycle tests**

Run: `mvn -q -pl yierdis-server -am -Dtest=ClosingSkipSideEffectsIntegrationTest,TransactionQueueCleanupTest,NettyCommandExecutorTest test`
Run: `mvn -q -pl yierdis-core/yierdis-core-runtime -am -Dtest=OffHeapLeakRegressionTest,UnsafeOffHeapKeyspaceTest test`
Expected: PASS


### Task 4: Full Verification And Build Stability Check

**Files:**
- No planned source changes

- [ ] **Step 1: Run clean build**

Run: `mvn -q clean test`
Expected: PASS

- [ ] **Step 2: Run non-clean build immediately after**

Run: `mvn -q test`
Expected: PASS, confirming the incremental-build instability is resolved.

- [ ] **Step 3: Review remaining off-heap boundary duplication**

Check whether any remaining old/new API wrappers are intentional compatibility seams or cleanup debt that should be filed separately.
