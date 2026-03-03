# Contracts Boundary Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move execution contracts out of `yierdis-protocol-model` into a dedicated `yierdis-core-contract` module, make off-heap API pure (ServiceLoader-only), introduce an explicit instance-level maintenance component, and tighten `YierdisInstance` public API — without changing protocol behavior or command semantics.

**Architecture:** Introduce `yier.bubu.redis.contract.*` as the stable execution-contract surface. Keep `yierdis-protocol-model` for protocol limits, build info, JSON model, and reply IR model only. Wire server periodic maintenance via a runtime component instead of embedding logic in Netty bootstrap.

**Tech Stack:** Java 17, Maven multi-module, Netty 4.1, JUnit4.

---

### Task 1: Create `yierdis-core-contract` module skeleton

**Files:**
- Create: `yierdis-core/yierdis-core-contract/pom.xml`
- Modify: `yierdis-core/pom.xml`
- Modify: `pom.xml`

**Step 1: Add the Maven module**

Add `yierdis-core-contract` under `yierdis-core/` and include it in `yierdis-core/pom.xml` modules list.

**Step 2: Add dependencyManagement entry (parent)**

Add `yier.bubu.redis:yierdis-core-contract` into root `pom.xml` dependencyManagement so other modules can reference it without repeating version.

**Step 3: Verify module builds empty**

Run:
- `mvn -q -pl yierdis-core/yierdis-core-contract test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS (no tests).

---

### Task 2: Introduce new contract types in `yier.bubu.redis.contract`

**Files:**
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/Command.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ReplySink.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ReplyWriter.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ReplyWriterFactory.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/CommandContext.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/Session.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/DbIndexProvider.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/ServerSession.java`
- Create: `yierdis-core/yierdis-core-contract/src/main/java/yier/bubu/redis/contract/TransactionState.java`

**Step 1: Copy semantics exactly**

Implement these by moving the current code from:
- `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/*`

into the new package, keeping method signatures and Javadoc semantics the same.

Notes:
- `ReplyWriter` must remain protocol-agnostic (methods for scalar/aggregate shapes, plus `protocolError/internalError` defaults).
- `Command` retained-bytes semantics must stay the same.
- `CommandContext` reset/reuse semantics must stay the same.

**Step 2: Add a tiny compilation-only test**

Create a JUnit test in `yierdis-core-contract` that instantiates a dummy `CommandContext` and calls a couple of `ReplyWriter` methods through a minimal stub implementation, just to ensure the module compiles and the types fit together.

**Step 3: Run contract module tests**

Run:
- `mvn -q -pl yierdis-core/yierdis-core-contract test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 3: Migrate protocol-codec and protocol-netty to the new contract module

**Files:**
- Modify: `yierdis-protocol/yierdis-protocol-codec/pom.xml`
- Modify: `yierdis-protocol/yierdis-protocol-netty/pom.xml`
- Modify: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/CustomCommand.java`
- Modify: `yierdis-protocol/yierdis-protocol-codec/src/main/java/yier/bubu/redis/protocol/v1/JsonLineReplyWriter.java`
- Modify: `yierdis-protocol/yierdis-protocol-model/pom.xml` (remove now-moved types if needed)
- Modify: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/ProtocolLimits.java` (update imports if needed)

**Step 1: Add dependency on `yierdis-core-contract`**

Add `yierdis-core-contract` as a dependency for codec/netty modules.

**Step 2: Update imports**

Update `CustomCommand` to implement `yier.bubu.redis.contract.Command` and adjust any dependent imports.

Update `JsonLineReplyWriter*` to implement `yier.bubu.redis.contract.ReplyWriter`.

**Step 3: Run protocol module tests**

Run:
- `mvn -q -pl yierdis-protocol/yierdis-protocol-codec test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q -pl yierdis-protocol/yierdis-protocol-netty test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 4: Migrate core-command to the new contract module (and keep it Netty-free)

**Files:**
- Modify: `yierdis-core/yierdis-core-command/pom.xml`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/*` (imports)
- Modify: `yierdis-core/yierdis-core-command/src/test/java/yier/bubu/redis/corecommand/CoreCommandBoundaryGuardTest.java` (if needed)

**Step 1: Add dependency**

Add `yierdis-core-contract` dependency to `yierdis-core-command`.

**Step 2: Update imports**

Replace imports from `yier.bubu.redis.protocol.*` to `yier.bubu.redis.contract.*` for:
- `Command`, `CommandContext`, `ReplyWriter`
- `ServerSession`, `TransactionState`
- `DbIndexProvider`

Keep `YierdisBuildInfo` in protocol-model (still imported from `yier.bubu.redis.protocol.YierdisBuildInfo`).

**Step 3: Run core-command tests**

Run:
- `mvn -q -pl yierdis-core/yierdis-core-command test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS (including `CoreCommandBoundaryGuardTest`).

---

### Task 5: Move `InlineCommandParser` back into `yierdis-client`

**Files:**
- Move: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/InlineCommandParser.java`
  to `yierdis-client/src/main/java/yier/bubu/redis/client/InlineCommandParser.java`
- Modify: `yierdis-client/src/main/java/yier/bubu/redis/client/YierdisCli.java`
- Modify: `yierdis-client/pom.xml` (dependencies may change)

**Step 1: Move file and keep behavior identical**

Keep the class API (`parse`, `splitUtf8`) unchanged.

**Step 2: Update CLI imports**

`YierdisCli` should import `yier.bubu.redis.client.InlineCommandParser`.

**Step 3: Run client tests**

Run:
- `mvn -q -pl yierdis-client test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 6: Migrate server + executor integration to the new contract module

**Files:**
- Modify: `yierdis-server/pom.xml`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyCommandExecutor.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolErrorReplyHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/*` (imports)

**Step 1: Update dependencies**

Server should depend on `yierdis-core-contract`.

**Step 2: Update imports**

Replace usages of protocol contracts to contract package:
- `Command`, `CommandContext`, `ReplyWriter`, `ReplyWriterFactory`, `ServerSession`, `TransactionState`

Keep:
- protocol-netty decoder types still in protocol-netty module
- JSON reply writer factory still in protocol-codec

**Step 3: Run server tests**

Run:
- `mvn -q -pl yierdis-server test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 7: Make `YierdisOffHeapAllocators` ServiceLoader-only (pure API)

**Files:**
- Modify: `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapAllocators.java`
- Modify: `yierdis-offheap/api/src/main/java/yier/bubu/redis/db/offheap/api/YierdisOffHeapBackendUnavailableException.java` (if message needs)
- Test: update/add tests in `yierdis-offheap/api/src/test/java` as needed

**Step 1: Remove implementation-class knowledge**

Delete all:
- hard-coded backend class-name constants
- reflection fallback paths
- backend-specific “module presence” checks

Keep:
- `availableProviders()` and `availableProvidersSummary()`
- provider selection by `backend()` via `ServiceLoader`

On error:
- if no provider found: throw `YierdisOffHeapBackendUnavailableException` listing `availableProvidersSummary()`
- if provider throws `LinkageError`: wrap into `YierdisOffHeapBackendUnavailableException` with actionable message

**Step 2: Run off-heap API tests**

Run:
- `mvn -q -pl yierdis-offheap/api test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 8: Introduce explicit instance maintenance component

**Files:**
- Create: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Test: add/update tests in `yierdis-core/yierdis-core-runtime/src/test/java` and/or `yierdis-server/src/test/java`

**Step 1: Add maintenance component**

Implement a `maintenanceTick()` method that:
- iterates all DB engines and calls `expiration().cleanupExpired()`
- applies maxmemory enforcement based on instance config scope

Important: remove the server-side “`firstDb`” implicit convention by centralizing the “enforce once for GLOBAL” rule here.

**Step 2: Wire server bootstrap**

Replace inline logic in `YierdisServerBootstrap` scheduled cleanup block with a call to the maintenance component, executed via `executor.executeMaintenance(...)`.

**Step 3: Add an integration test**

Add a test that:
- runs with multiple DBs and global maxmemory enabled
- verifies that the maintenance tick does not double-enforce or drift semantics

**Step 4: Run runtime + server tests**

Run:
- `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q -pl yierdis-server test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 9: Tighten `YierdisInstance` public API surface

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: callers in `yierdis-server` and tests

**Step 1: Remove `YierdisDb` exposure**

Change the public API so that external consumers only see `DbEngine` (or `List<DbEngine>`):
- avoid returning `YierdisDb[]` as `DbEngine[]` (covariant array hazard)
- avoid exposing `YierdisDb` in public methods (`dbs()` / `db(int)`)

If tests need access to concrete DBs, provide package-private helpers in the same module test scope rather than public API.

**Step 2: Update server bootstrap**

Update `YierdisServerBootstrap` to bind engines using the new `YierdisInstance` accessor(s).

**Step 3: Run full test suite**

Run:
- `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 10: Remove old contract types from `yierdis-protocol-model`

**Files:**
- Delete: moved files under `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/` (contracts only)
- Modify: any remaining imports
- Update: architecture guard tests if they reference deleted types

**Step 1: Delete old classes**

Remove the old contract interfaces/classes from `protocol-model` after all modules compile against `core-contract`.

**Step 2: Run full test suite**

Run:
- `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

---

### Task 11: Clean up & verify repo boundaries

**Files:**
- Modify: boundary guard tests if needed:
  - `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Step 1: Update architecture boundary scans**

Adjust the tests so that the intended rule remains true:
- DB/ops must not depend on protocol model (and should not depend on execution contracts either unless explicitly intended).

**Step 2: Final verification**

Run:
- `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`

Expected: PASS.

