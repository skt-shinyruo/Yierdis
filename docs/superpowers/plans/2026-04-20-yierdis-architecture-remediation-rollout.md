# Yierdis Architecture Remediation Rollout Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the current architecture review findings into a three-week execution order that lands FFM-only convergence, compatibility governance, legacy seam cleanup, and trustworthy observability.

**Architecture:** Reuse the existing detailed implementation plan in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md` as the code-level SSOT for FFM-only convergence and Redis-semantic tightening. This rollout plan adds delivery sequencing, issue boundaries, and two missing workstreams from the review: bytes-first legacy seam cleanup and stronger build-time boundary enforcement.

**Tech Stack:** Java 25, Maven, Netty, JUnit 4, Testcontainers, `Custom Protocol v1`, JDK FFM API

---

## Detailed Plan Reuse Map

- Task 1 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for compatibility ledger and Redis differential harness.
- Task 2 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for `String`/`Bitmap`/`TTL` semantic tightening.
- Task 3 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for heap storage foundation removal.
- Task 4 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for FFM-only value path convergence.
- Task 5 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for memory accounting and observability reconciliation.
- Task 6 in `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`: reuse for transaction semantics and README cleanup.
- This rollout plan adds two missing tracks:
  - `ARCH-05`: remove remaining bytes-first legacy seams.
  - `ARCH-08`: move more boundary enforcement into Maven build rules instead of relying mostly on source-scanning tests.

## Week 1: Governance And Storage Foundations

### Issue ARCH-01: Compatibility Ledger And Differential Harness

**Why this week:** The review found that the project posture changed in design docs, but the compatibility ledger and Redis differential harness are still missing. That gap prevents the rest of the remediation from having a single semantic truth source.

**Files:**
- Create: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Modify: `README.md`
- Modify: `yierdis-client/pom.xml`
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisRespTestClient.java`
- Create: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 1 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Verify with `jdk25 mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest test`.
- [x] Mark done only when the ledger exists on disk, the README no longer frames supported behavior as teaching-oriented simplification, and at least one Redis differential test passes.

### Issue ARCH-03: Remove Heap Storage Foundations

**Why this week:** The architecture review found that heap keyspace and heap expire index files still exist, even though the target direction is FFM-only. Removing the storage foundations first reduces the number of code paths every later fix has to reason about.

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyStorageBoundaryGuardTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandle.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/KeyHandleAccess.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/FfmKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/key/HeapKeyHandle.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java`
- Delete: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHeapExpireIndex.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapKeysToggleTest.java`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 3 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Verify with `jdk25 mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyStorageBoundaryGuardTest,OffHeapKeysToggleTest test`.
- [x] Mark done only when heap-specific keyspace and expire index files are deleted and the new guard test passes.

### Week 1 Exit Criteria

- The repository has a compatibility ledger under `docs/compatibility/`.
- Redis differential testing exists and is runnable on demand.
- Heap storage foundation files are gone.
- The Java 25 guard test suite still passes:
  - `jdk25 mvn -pl yierdis-core/yierdis-core-api,yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime -am -Dtest=CoreApiBoundaryGuardTest,CoreCommandBoundaryGuardTest,YierdisDbArchitectureGuardTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`

## Week 2: Value Path Convergence And Legacy Seam Removal

### Issue ARCH-02: Tighten String, Bitmap, And TTL Semantics

**Why this week:** Once the ledger and differential harness exist, the cheapest semantic drift to fix is the `String`/`Bitmap`/`TTL` family. This gives quick compatibility wins while the larger value-path refactor is in flight.

**Files:**
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 2 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Verify with `jdk25 mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#setOptionsAndTtlFamilyMatchRedis,RedisSemanticDifferentialTest#bitmapOperationsMatchRedis test`.
- [x] Mark done only when the ledger rows for `String/Bitmap` and `TTL/Expire` are updated and the differential tests pass.

### Issue ARCH-04: Converge Value Encodings To FFM-Only

**Why this week:** The review found that `YierdisObject`, `ListValue`, `SetValue`, and `ZSetValue` still carry heap fallback branches. Removing those branches is the biggest architectural cleanup remaining in `core-db`.

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/test/java/yier/bubu/redis/db/FfmOnlyValuePathGuardTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisObject.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/HashValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ListValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/SetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/ZSetValue.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapStringStorageTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/UnsafeOffHeapDbSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/OffHeapLeakRegressionTest.java`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 4 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Verify with `jdk25 mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FfmOnlyValuePathGuardTest,OffHeapStringStorageTest,UnsafeOffHeapDbSmokeTest,OffHeapLeakRegressionTest test`.
- [x] Mark done only when `YierdisObject` and all supported composite value families no longer keep heap fallback branches.

### Issue ARCH-05: Close The Remaining Bytes-First Legacy Seams

**Why this week:** The review found that the repository still keeps compatibility-only seams in `ProtocolCommandAdapter`, `CommandSupport`, and `YierdisFastCommandProcessor`. Those seams are not the hot path anymore and should stop shaping production code.

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`
- Modify: `yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] Add a failing guard in `yierdis-server/src/test/java/yier/bubu/redis/ProtocolCommandAdapterTest.java` that rejects production fallback through `CustomProtocolV1Request`.
- [x] Remove the `CustomProtocolV1Request -> ByteArrayExecutionRequest.fromUtf8(...)` branch from `yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java`.
- [x] Remove the deprecated `execute(Command, CommandContext)` overload from `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java` if there are no production call sites left.
- [x] Remove the temporary frame-backed `Command` seam from `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java` if the producer search shows only `ExecutionRequest` remains in production code.
- [x] Tighten `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java` so regressions back to legacy request adaptation fail fast.
- [x] Verify producer cleanup with `rg -n "new CustomProtocolV1Request\\(|instanceof yier\\.bubu\\.redis\\.contract\\.Command|execute\\(Command cmd" /home/feng/code/project/Yierdis`.
- [x] Verify tests with `jdk25 mvn -pl yierdis-server,yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=ProtocolCommandAdapterTest,ArchitectureBoundaryTest test`.
- [x] Mark done only when production code no longer depends on legacy request adaptation and the temporary compatibility seam comment is gone.

### Week 2 Exit Criteria

- Supported value families use only the FFM-backed path.
- Differential tests cover the `String`/`Bitmap`/`TTL` family.
- Production command ingress is bytes-first without the legacy `CustomProtocolV1Request` adaptation path.

## Week 3: Observability Truth, Transaction Parity, And Build Hardening

### Issue ARCH-06: Reconcile Memory Accounting And Observability

**Why this week:** After the kernel is FFM-only, `INFO`, `STATS`, and `MEMORY STATS` need to stop reporting mixed-path assumptions or placeholder values that weaken operational trust.

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryReporter.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryOps.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/YierdisClientTest.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 5 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Extend the task to replace hard-coded Redis-style placeholder counters in `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java` with either real values or explicit Yierdis-specific fields.
- [x] Verify with `jdk25 mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=YierdisClientTest#infoAndMemoryStatsDescribeFfmOnlyKernel test`.
- [x] Mark done only when `INFO`/`STATS` no longer publish misleading `0` values for metrics the server does not actually track.

### Issue ARCH-07: Tighten Transaction Semantics And README Posture

**Why this week:** Transaction semantics and project posture are the last user-visible compatibility gaps still called out in the architecture review.

**Files:**
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/RedisSemanticDifferentialTest.java`
- Modify: `yierdis-client/src/test/java/yier/bubu/redis/client/TransactionQueueLimitTest.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerSessionState.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `docs/compatibility/custom-protocol-v1-redis-semantics-ledger.md`
- Modify: `README.md`
- Reference: `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`

- [x] Execute Task 6 from `docs/superpowers/plans/2026-04-17-yierdis-ffm-only-redis-compatibility.md`.
- [x] Verify with `jdk25 mvn -pl yierdis-client -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RedisSemanticDifferentialTest#multiExecDiscardMatchRedisForSupportedSubset,TransactionQueueLimitTest#discardAfterAbortRestoresUsableTransactionState test`.
- [x] Mark done only when the ledger row for `MULTI/EXEC/DISCARD` is updated and README wording matches the new project posture.

### Issue ARCH-08: Strengthen Build-Time Boundary Enforcement

**Why this week:** The review found that many module boundaries are held by source-scanning tests rather than Maven-level dependency rules. Those tests are still useful, but more forbidden dependencies should fail at build wiring time.

**Files:**
- Modify: `yierdis-core/yierdis-core-api/pom.xml`
- Modify: `yierdis-core/yierdis-core-runtime/pom.xml`
- Modify: `yierdis-protocol/yierdis-protocol-codec/pom.xml`
- Modify: `yierdis-server/pom.xml`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/coreapi/CoreApiBoundaryGuardTest.java`

- [x] Mirror the existing `maven-enforcer-plugin` pattern from `yierdis-core/yierdis-core-command/pom.xml` into the modules that should forbid reverse dependencies.
- [x] Add banned dependency rules so `core-api` cannot take `yierdis-core-db` or Netty transitively, `core-runtime` cannot re-own server assembly, and `protocol-codec` cannot depend on `yierdis-core-contract`.
- [x] Keep the existing source-scanning tests, but trim them so they focus on implementation-level invariants that Maven cannot express.
- [x] Verify with `jdk25 mvn -pl yierdis-core/yierdis-core-api,yierdis-core/yierdis-core-command,yierdis-core/yierdis-core-runtime,yierdis-protocol/yierdis-protocol-codec,yierdis-server -am test`.
- [x] Mark done only when the forbidden dependency cases fail during Maven resolution, not only during test execution.

### Week 3 Exit Criteria

- Memory reporting and observability reflect the FFM-only kernel.
- Transaction semantics are covered by differential tests for the supported subset.
- README and compatibility ledger no longer conflict.
- More boundary regressions fail at Maven build time.

## Issue Tracker View

| ID | Week | Summary | Depends On | Done When |
| --- | --- | --- | --- | --- |
| ARCH-01 | 1 | Add compatibility ledger and Redis differential harness | None | Ledger exists and at least one Redis differential test passes |
| ARCH-03 | 1 | Remove heap storage foundations | None | Heap keyspace and heap expire index files are deleted |
| ARCH-02 | 2 | Tighten `String`/`Bitmap`/`TTL` semantics | ARCH-01 | Differential tests for the family pass and ledger rows are updated |
| ARCH-04 | 2 | Remove heap fallback branches from supported value families | ARCH-03 | Value path guard and leak tests pass |
| ARCH-05 | 2 | Remove legacy bytes-first compatibility seams | ARCH-03 | Production code no longer adapts `CustomProtocolV1Request` |
| ARCH-06 | 3 | Reconcile memory accounting and observability | ARCH-04 | `INFO` and `MEMORY STATS` reflect FFM-only truth |
| ARCH-07 | 3 | Align transaction semantics and README posture | ARCH-01 | Transaction tests pass and docs are updated |
| ARCH-08 | 3 | Move more boundary enforcement into Maven rules | ARCH-03 | Illegal dependency regressions fail during Maven build |

## Risk Notes

- `ARCH-04` is the highest-risk change because it removes long-lived fallback branches inside `core-db`. Do not combine it with observability rewrites in the same commit.
- `ARCH-05` should only remove deprecated request/command seams after a producer search shows production code is fully on `ExecutionRequest`.
- `ARCH-08` should not replace architecture tests entirely. Keep tests for invariants that Maven Enforcer cannot express.

## Recommended Commit Sequence

- `test: add redis semantic differential harness`
- `refactor: remove heap storage foundations`
- `fix: align string bitmap and ttl semantics`
- `refactor: converge value encodings to ffm only`
- `refactor: remove legacy request compatibility seams`
- `fix: align memory stats and observability with ffm kernel`
- `fix: align transaction subset with redis semantics`
- `build: enforce architecture boundaries in maven`
