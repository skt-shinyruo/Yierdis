# Write Reservation Redesign Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current command-managed write reservation protocol with DB-owned semantic read/write contracts, remove the legacy `values()/eviction()/keyspace()/ttl()` API path, and make maxmemory reservation lifecycle strictly internal to `YierdisDb`.

**Architecture:** Implement this in stages so the tree stays buildable: first add the new `DbReads` / `DbWrites` / runtime-maintenance contracts and transitional adapters, then migrate mutation logic into a DB-internal mutation executor plus semantic write APIs, then move all command handlers to `reads()/writes()`, and finally delete the legacy API and strengthen boundary guards. Temporary overlap between old and new API surfaces is acceptable only until the “remove legacy API” task is complete.

**Tech Stack:** Java 17, Maven multi-module reactor, JUnit 4

---

### Task 1: Introduce the New Public Contracts and Runtime Hook

**Files:**
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbReads.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbWrites.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/StringReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/StringWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HashReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HashWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ListReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ListWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/SetReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/SetWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ZSetReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ZSetWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HllReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HllWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/KeyspaceReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/KeyspaceWriteOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/TtlReadOps.java`
- Create: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/TtlWriteOps.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngine.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/RuntimeDbEngine.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`

- [ ] **Step 1: Write the failing contract test**

Add `DbEngineReadWriteBoundaryTest` to assert that:
- `DbEngine` exposes `reads()` and `writes()`
- `RuntimeDbEngine` exposes a runtime-only maxmemory maintenance hook
- fake engine implementations used in tests no longer compile until they implement the new methods

- [ ] **Step 2: Run the focused test command and verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-core/yierdis-core-command,yierdis-server -am -Dtest=DbEngineReadWriteBoundaryTest,DbEngineFactoryInjectionTest,YierdisInstanceTest,YierdisServerBootstrapCloseTest,YierdisFastCommandProcessorModuleTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the new contracts and fake-engine implementations do not exist yet.

- [ ] **Step 3: Add the new API surface and transitional adapters**

Implement the new read/write interfaces in `yierdis-core-api`, update `DbEngine` / `RuntimeDbEngine`, and add transitional `YierdisDbReads` / `YierdisDbWrites` adapters in `yierdis-core-db`. At this stage, the old `values()/eviction()/keyspace()/ttl()` API may remain temporarily so later tasks can migrate incrementally.

- [ ] **Step 4: Move runtime maintenance to the runtime-facing hook**

Update `YierdisInstanceMaintenance` to call the new `RuntimeDbEngine` maxmemory maintenance hook instead of `engine.eviction().enforceMaxmemory()`.

- [ ] **Step 5: Update all fake engine implementations**

Make every `DbEngine` / `RuntimeDbEngine` test double implement the new methods so the focused suites compile and run again.

- [ ] **Step 6: Re-run the focused test command**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-core/yierdis-core-command,yierdis-server -am -Dtest=DbEngineReadWriteBoundaryTest,DbEngineFactoryInjectionTest,YierdisInstanceTest,YierdisServerBootstrapCloseTest,YierdisFastCommandProcessorModuleTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 7: Commit the contract slice**

Run:
```bash
git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
  yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/DbEngineReadWriteBoundaryTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java
git commit -m "refactor: add db read write contracts"
```

### Task 2: Add the Internal Mutation Executor and Migrate String / Keyspace / TTL / Lifecycle

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MutationExecutorReservationTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbLifecycleOps.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/BitmapCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MaxmemoryDoubleReplyRegressionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ScanCursorContractTest.java`

- [ ] **Step 1: Write the failing mutation-executor regression test**

Add `MutationExecutorReservationTest` covering at least:
- a failed mutation rolls back its reservation
- a subsequent write does not inherit a leaked reservation
- `noeviction` still fails before mutating observable state

- [ ] **Step 2: Run the focused DB / command regression command and verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=MutationExecutorReservationTest,SetCommandTest,BitmapCommandTest,ExpireSemanticsTest,TtlMaxmemoryTest,MaxmemoryDoubleReplyRegressionTest,MemoryStatsCommandTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because string/keyspace/ttl/lifecycle writes still rely on `prepareWrite(...)` and direct legacy APIs.

- [ ] **Step 3: Introduce `YierdisDbMutationExecutor` and route string writes through it**

Refactor string mutations so `SET`, `APPEND`, `SETBIT`, `INCR`, and `DECR` go through DB-owned two-phase mutation planning and commit/rollback, not command-managed reservation.

- [ ] **Step 4: Move keyspace / TTL writes onto `writes()`**

Migrate `DEL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, and `PERSIST` so command handlers call `engine.writes()` and DB-internal mutators handle any lazy cleanup / TTL side effects.

- [ ] **Step 5: Move the remaining string/key read commands onto `reads()`**

Migrate `GET`, `STRLEN`, `GETBIT`, `BITCOUNT`, `TYPE`, `KEYS`, `SCAN`, `EXISTS`, `TTL`, and `PTTL` so command handlers stop depending on legacy `values()/keyspace()/ttl()` entry points and instead use the new `reads()` boundary while preserving lazy expiration and read-touch semantics.

- [ ] **Step 6: Keep lifecycle management separate**

Ensure `FLUSHDB` continues to use `lifecycle()`, and the runtime maxmemory maintenance hook stays off the command-facing `DbEngine` contract.

- [ ] **Step 7: Update `CommandSupport` helpers**

Add `dbReads(ctx)` / `dbWrites(ctx)` helpers or equivalent convenience methods so command handlers stop reaching for legacy `db(ctx).values()/keyspace()/ttl()/eviction()` directly.

- [ ] **Step 8: Re-run the focused DB / command regression command**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=MutationExecutorReservationTest,SetCommandTest,BitmapCommandTest,ExpireSemanticsTest,TtlMaxmemoryTest,MaxmemoryDoubleReplyRegressionTest,MemoryStatsCommandTest,CommandProcessorTest,ScanCursorContractTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 9: Commit the string/keyspace/ttl slice**

Run:
```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbLifecycleOps.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/MutationExecutorReservationTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/BitmapCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ExpireSemanticsTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/TtlMaxmemoryTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MaxmemoryDoubleReplyRegressionTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/MemoryStatsCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ScanCursorContractTest.java
git commit -m "refactor: move string and ttl writes behind db mutations"
```

### Task 3: Migrate List / Hash / Set / ZSet / HLL to `reads()` and `writes()`

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ListCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HashCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/SetCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HllCommands.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ListCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HashCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ZSetCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java`

- [ ] **Step 1: Write the failing collection-mutation regression coverage**

Add or extend tests so they fail unless:
- `LPOP/RPOP` are treated as writes
- `HDEL/SREM/ZREM/ZREMRANGEBY*` mutate through `writes()`
- `PFMERGE` and `PFADD` go through DB-owned mutation execution

- [ ] **Step 2: Run the focused collection test command and verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest,CommandProcessorTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because collection commands still depend on legacy mixed APIs or direct `prepareWrite(...)`.

- [ ] **Step 3: Move list and hash commands to the new DB read/write contracts**

Migrate `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `HSET`, `HGET`, `HGETALL`, `HLEN`, and `HDEL` to use `reads()` / `writes()`.

- [ ] **Step 4: Move set, zset, and HLL commands to the new DB read/write contracts**

Migrate `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `ZADD`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZREM`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `PFADD`, `PFCOUNT`, and `PFMERGE`.

- [ ] **Step 5: Re-run the focused collection test command**

Run: `mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest,CommandProcessorTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 6: Commit the collection slice**

Run:
```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbReads.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbWrites.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ListCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HashCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/SetCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/HllCommands.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ListCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HashCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/SetCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/ZSetCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/HllCommandTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/command/CommandProcessorTest.java
git commit -m "refactor: migrate collection commands to db reads writes"
```

### Task 4: Remove the Legacy API Surface and Strengthen Guardrails

**Files:**
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ValueOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/EvictionCoordinator.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/StringOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HashOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ListOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/SetOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/ZSetOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/HllOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/KeyspaceOps.java`
- Delete: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/TtlOps.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbValueOps.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEvictionCoordinator.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbKeyspaceOps.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbTtlOps.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngine.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-03-20-write-reservation-redesign-design.md` only if implementation forced a spec correction

- [ ] **Step 1: Extend the failing guardrails**

Update boundary tests so they fail when:
- `DbEngine.values()` or `DbEngine.eviction()` still exist
- `core-command` still references `prepareWrite`, `rollbackWriteReservationIfAny`, or `DbMemoryConstants` for budgeting
- `YierdisFastCommandProcessor` still contains a reservation cleanup `finally`
- DB-level smoke tests still depend on `db.values()` or other deleted legacy mixed APIs

- [ ] **Step 2: Run the guardrail command and verify it fails**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-core/yierdis-core-command,yierdis-server -am -Dtest=ArchitectureBoundaryTest,CoreCommandBoundaryGuardTest,DbEngineReadWriteBoundaryTest,DbEngineFactoryInjectionTest,YierdisInstanceTest,YierdisFastCommandProcessorModuleTest,YierdisServerBootstrapCloseTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL before the old interfaces and fallback logic are removed.

- [ ] **Step 3: Delete the legacy API and adapter files**

Remove the old mixed interfaces and DB adapter classes, then remove the temporary compatibility methods from `DbEngine` and `YierdisDb`.

- [ ] **Step 4: Migrate remaining DB-level smoke tests off the legacy API**

Update direct `YierdisDb` tests such as `UnsafeOffHeapDbSmokeTest` and `OffHeapStringStorageTest` to use the new read/write contracts instead of `db.values()`.

- [ ] **Step 5: Remove transitional fake-engine compatibility methods**

Update fake-engine tests that were expanded in Task 1 so they stop carrying deleted legacy methods such as `values()` / `eviction()` after the old interfaces are removed.

- [ ] **Step 6: Remove the processor-level reservation fallback**

Delete the `rollbackWriteReservationIfAny()` cleanup path from `YierdisFastCommandProcessor`; after Tasks 2 and 3 the DB mutation executor must own this invariant.

- [ ] **Step 7: Re-run the guardrail command**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-core/yierdis-core-command,yierdis-server -am -Dtest=ArchitectureBoundaryTest,CoreCommandBoundaryGuardTest,DbEngineReadWriteBoundaryTest,DbEngineFactoryInjectionTest,YierdisInstanceTest,YierdisFastCommandProcessorModuleTest,YierdisServerBootstrapCloseTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

- [ ] **Step 8: Commit the cleanup and guardrail slice**

Run:
```bash
git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db \
  yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/command/YierdisFastCommandProcessorModuleTest.java \
  yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCloseTest.java \
  yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java \
  README.md
git commit -m "refactor: remove legacy write reservation api"
```

### Task 5: Full Verification and Diff Review

**Files:**
- No new files expected

- [ ] **Step 1: Run the focused multi-module suite**

Run: `mvn -pl yierdis-core/yierdis-core-runtime,yierdis-core/yierdis-core-command,yierdis-server,yierdis-client -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS

- [ ] **Step 2: Run the full repository verification**

Run: `mvn test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS

- [ ] **Step 3: Review the diff for unwanted fallback code**

Run:
```bash
git diff --stat
rg -n "prepareWrite\\(|rollbackWriteReservationIfAny\\(|\\.values\\(\\)|\\.eviction\\(" yierdis-core yierdis-server yierdis-client
```
Expected: legacy `ValueOps` / `EvictionCoordinator` path removed, new read/write contracts added, no command-level `prepareWrite(...)` logic or legacy DB entry points left.

- [ ] **Step 4: Request final code review**

Dispatch a final reviewer over the completed implementation before any merge / branch-finishing workflow.
