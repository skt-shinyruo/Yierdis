# Global Maxmemory SPI & Governor Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor global maxmemory into a runtime governor that depends only on a small core-api SPI, so that storage engines/keyspace implementations can evolve or be replaced without changing maxmemory policy code.

**Architecture:** Introduce a `yierdis-core-api` maxmemory SPI (`MaxmemoryParticipant`, `MaxmemoryCoordinator`, etc.), implement the global governor in `yierdis-core-runtime`, adapt `YierdisDb` to implement the SPI, then wire `YierdisInstance` to assemble the governor and attach it to engines. Follow up by introducing an engine factory SPI so runtime no longer `new`s `YierdisDb`.

**Tech Stack:** Java 17, Maven multi-module build, JUnit4 tests.

---

### Task 1: Add maxmemory SPI types to `yierdis-core-api`

**Files:**
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryErrors.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCandidate.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryParticipant.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCoordinator.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCoordinatorAware.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryUsageSource.java`
- Test: `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/MaxmemoryPolicyTest.java`

**Step 1: Write failing test for policy parsing**

Create `MaxmemoryPolicyTest` that asserts:
- `"noeviction"` → `NOEVICTION`
- `"allkeys-random"` → `ALLKEYS_RANDOM`
- `"allkeys-lru"` → `ALLKEYS_LRU`
- unknown string throws `IllegalArgumentException`

Expected: compile fails (classes missing).

**Step 2: Implement `MaxmemoryPolicy`**

Implement:
- enum values: `NOEVICTION`, `ALLKEYS_RANDOM`, `ALLKEYS_LRU`
- `static MaxmemoryPolicy parse(String s)` which normalizes:
  - trim, lower-case, replace `_` with `-`

**Step 3: Implement stable error constants**

Add `MaxmemoryErrors.OOM_ERR` with the exact message currently used in tests:
`"OOM command not allowed when used memory > 'maxmemory'."`

**Step 4: Add SPI interfaces**

Implement:
- `MaxmemoryCoordinator`:
  - `void prepareWrite(long estimatedExtraBytes);`
  - `long nextLruClock();`
- `MaxmemoryCoordinatorAware`:
  - `void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator);`
- `MaxmemoryUsageSource`:
  - `long usedBytes();`
- `MaxmemoryCandidate` as a `record` (or final class) with:
  - `MaxmemoryParticipant owner`
  - `byte[] key`
  - `long lruClock`
- `MaxmemoryParticipant` with methods described in the design doc.

**Step 5: Run core-api tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-api test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 6: Commit**

Run:
`git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/MaxmemoryPolicyTest.java`  
`git commit -m "feat(core-api): add maxmemory SPI contracts"`

---

### Task 2: Implement a runtime global maxmemory governor (SPI-driven)

**Files:**
- Create: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernorTest.java`

**Step 1: Write failing governor test (noeviction)**

Create a stub `MaxmemoryParticipant` in the test that:
- reports `usedBytesForMaxmemory() = maxmemoryBytes`
- has no candidates to evict

Test: `prepareWrite(1)` throws an exception whose message equals `MaxmemoryErrors.OOM_ERR` when policy is `NOEVICTION`.

Expected: compile fails (governor missing).

**Step 2: Implement the governor skeleton**

Implement constructor args:
- `MaxmemoryParticipant[] participants`
- `MaxmemoryUsageSource[] sharedUsage`
- `long maxmemoryBytes`
- `MaxmemoryPolicy policy`
- `int samples`
- `long evictionTimeLimitNanos`

Implement `MaxmemoryCoordinator`:
- `nextLruClock()` backed by `AtomicLong`
- `prepareWrite(long estimatedExtraBytes)` with Redis-like semantics (see design doc)

**Step 3: Add eviction behavior test**

Write a test with a participant that:
- starts above budget
- returns a deterministic candidate key when sampled
- decrements `usedBytesForMaxmemory()` when `evict(...)` is called

Assert:
- `prepareWrite(extra)` evicts until under limit
- does not throw when eviction succeeds

**Step 4: Run runtime tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 5: Commit**

Run:
`git add yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernor.java yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisGlobalMaxmemoryGovernorTest.java`  
`git commit -m "feat(core-runtime): add SPI-driven global maxmemory governor"`

---

### Task 3: Adapt `YierdisDb` to consume `MaxmemoryCoordinator` (no direct coordinator class dependency)

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEvictionCoordinator.java`

**Step 1: Write a failing test that compiles against the new constant**

Update any tests that hardcode `YierdisDb.OOM_ERR` usage to instead reference `MaxmemoryErrors.OOM_ERR` (or keep message checks stable).

Expected: compile may fail until `YierdisDb` is updated.

**Step 2: Replace `YierdisDb.OOM_ERR` usage**

In `YierdisDb`:
- delete or deprecate the internal `OOM_ERR` constant
- throw `new YierdisCommandException(MaxmemoryErrors.OOM_ERR)` consistently

**Step 3: Replace `YierdisGlobalMaxmemoryCoordinator` field**

In `YierdisDb`:
- replace `private volatile YierdisGlobalMaxmemoryCoordinator globalMaxmemory;`
  with `private volatile MaxmemoryCoordinator maxmemoryCoordinator;`
- implement `MaxmemoryCoordinatorAware.attachMaxmemoryCoordinator(...)`
  to set/unset it

**Step 4: Wire coordinator into reservation and LRU clock**

Update:
- `DbMemoryLedger.reserve(...)` to call `maxmemoryCoordinator.prepareWrite(estimatedExtraBytes)` when coordinator != null
- `touch(...)` to use `maxmemoryCoordinator.nextLruClock()` for global LRU mode

**Step 5: Run db tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-db test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 6: Commit**

Run:
`git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEvictionCoordinator.java`  
`git commit -m "refactor(core-db): consume maxmemory coordinator SPI"`

---

### Task 4: Make `YierdisDb` implement `MaxmemoryParticipant` (encapsulate store internals)

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

**Step 1: Add a failing integration test for global LRU across DBs**

Add test (recommended location):
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`

Test outline:
1. Create an instance with 2 DBs, `maxmemoryScope=GLOBAL`, policy `"allkeys-lru"`, `maxmemorySamples >= totalGlobalKeys`.
2. In DB0: `SET a ...`, `SET b ...`
3. In DB1: `SET c ...`
4. Touch one key (e.g. `GET a`) to make it more recent.
5. Write another key that triggers eviction under the global budget.
6. Assert the globally least-recently-used key is evicted (e.g. `b`), even if it lives in a different DB than the triggering write.

Expected: FAIL until participant sampling + eviction works.

**Step 2: Implement `MaxmemoryParticipant` on `YierdisDb`**

Implement methods by delegating to existing internals:
- `usedBytesForMaxmemory()` → existing `usedBytesForMaxmemory()` logic *excluding* shared usage sources
- `keyCountEstimate()` → `store.size()`
- `cleanupExpired(nowMillis)` → existing `cleanupExpired()`
- `sampleCandidate(...)`:
  - RANDOM: `store.randomKey()` + return candidate with lruClock=0
  - LRU: pick candidate from random samples or deterministic full scan when requested by governor
- `scanBestCandidate(...)`:
  - LRU deterministic scan: iterate store and pick min `e.lruClock`
- `evict(candidate, nowMillis)`:
  - ensure not expired (or treat expired as already-evicted)
  - remove expire index entry
  - remove from store
  - release payload
  - adjust ledger used bytes

**Step 3: Run runtime tests**

Run: `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS (including the new cross-db LRU test).

**Step 4: Commit**

Run:
`git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`  
`git commit -m "feat(core-db): implement maxmemory participant for YierdisDb"`

---

### Task 5: Wire `YierdisInstance` GLOBAL maxmemory scope to the new governor

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java` (only if needed)

**Step 1: Write failing test for coordinator attachment**

Extend `YierdisInstanceTest.globalMaxmemoryCountsSharedOffheapOnceAcrossDbs()` (or add a new test) to assert:
- instance created with GLOBAL scope does not throw
- operations across both DBs respect the shared off-heap single-count invariant

Expected: FAIL until instance uses the new governor and shared usage source.

**Step 2: Create governor and attach to engines**

In `YierdisInstance.create(...)` when `maxmemoryScope == GLOBAL`:
- collect participants from each engine (cast or via a new engine interface if already introduced)
- create `YierdisGlobalMaxmemoryGovernor` with:
  - participants
  - shared usage source for off-heap allocator when shared across DBs
- call `attachMaxmemoryCoordinator(governor)` on each engine

**Step 3: Remove `YierdisDb.enableGlobalMaxmemory(...)` call path**

Delete the call in runtime assembly and rely solely on the SPI governor.

**Step 4: Run runtime + server tests**

Run:
- `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q -pl yierdis-server test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

**Step 5: Commit**

Run:
`git add yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`  
`git commit -m "refactor(core-runtime): assemble global maxmemory via SPI governor"`

---

### Task 6: Introduce an engine factory SPI so runtime no longer constructs `YierdisDb`

**Files:**
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

**Step 1: Write a failing test for factory injection**

Add a test that:
- supplies a custom `DbEngineFactory` that returns a stub engine
- asserts `YierdisInstance.create(config)` uses the factory (e.g. by setting a flag)

Expected: FAIL until config and instance support it.

**Step 2: Add factory to config**

In `YierdisInstanceConfig.Builder`:
- add field `DbEngineFactory engineFactory`
- default to null (meaning “use default YierdisDb engine factory”)

**Step 3: Implement default `YierdisDbEngineFactory`**

In core-db, implement factory that creates `YierdisDb` with existing constructor arguments.

**Step 4: Update instance assembly**

Replace the hard-coded `new YierdisDb(...)` with:
- `factory.create(dbIndex, config-derived params)`

Ensure the global governor wiring still works by requiring that created engines also implement:
- `MaxmemoryCoordinatorAware`
- `MaxmemoryParticipant` (GLOBAL scope)
- lifecycle hooks as needed by `YierdisInstance.bindToCurrentThread()` / `close()`

If necessary, introduce a new “runtime engine” interface in core-api for lifecycle methods.

**Step 5: Run full test suite**

Run: `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 6: Commit**

Run:
`git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`  
`git commit -m "feat(core-runtime): assemble engines via factory SPI"`

---

### Task 7: Delete the old internal global coordinator to prevent drift

**Files:**
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobalMaxmemoryCoordinator.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java` (remove unused helpers)

**Step 1: Delete file and fix compilation**

Remove the old class and any remaining references.

**Step 2: Run full test suite**

Run: `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 3: Commit**

Run:
`git add -A`  
`git commit -m "chore(core-db): remove legacy global maxmemory coordinator"`

---

### Task 8: Final verification and guardrails

**Files:**
- (Optional) Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Step 1: Run full test suite**

Run: `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`  
Expected: PASS.

**Step 2: (Optional) Extend boundary tests**

If needed, add a simple guard:
- policy governor must not import `yierdis-core-db` internals
- command layer still must not depend on `yierdis-core-db` (existing Maven enforcer already covers this)

**Step 3: Commit (if guard added)**

Run:
`git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`  
`git commit -m "test: extend boundary guards for maxmemory SPI"`

