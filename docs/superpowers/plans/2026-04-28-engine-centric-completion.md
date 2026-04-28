# Engine-Centric Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the remaining Engine-Centric Architecture phases so command execution flows through engine-owned session state, typed command specs, a single request model, and handle-based storage pressure paths.

**Architecture:** Keep `YierdisEngine` as the command execution center introduced in Phase 1. Move business session state into `yierdis-core-engine`, keep `yierdis-executor-core` limited to queueing/backpressure/closing bookkeeping, remove legacy command registration and `Command` compatibility, then harden storage and architecture guard tests around the new ownership rules.

**Tech Stack:** Java 17 source level, Maven multi-module build, JUnit 4 tests, source-scanning architecture guards in `yierdis-core-runtime`.

---

## File Structure

- Create `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/EngineSession.java`: engine-owned `ServerSession` implementation with selected DB, client metadata, and transaction state.
- Modify `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnection.java`: expose `Session session()` and `boolean markClosing()` as connection capabilities.
- Modify `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnectionContext.java`: keep only queue state, pending counts, retained bytes, closing flag, input recovery, and executor stats.
- Modify `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`: build command contexts from `connection.session()` and close through `connection.markClosing()`.
- Modify `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionConnection.java`: own one `EngineSession`, one executor scheduling context, and transaction discard on close.
- Modify `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`: stop casting sessions to executor-owned state.
- Delete `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/DefaultExecutionSession.java`: remove business session state from executor-core.
- Modify `CommandModule`, `CommandSpec`, `CommandRegistry`, command modules, `YierdisFastCommandProcessor`, and `CommandSupport` to finish typed spec migration and remove `Command` compatibility.
- Modify `MaxmemoryCandidate`, DB maxmemory, and expiration paths so hot pressure paths use `KeyHandle` where handle APIs exist.
- Update `ArchitectureBoundaryTest`, `docs/module-architecture.md`, and `docs/request-execution-flow.md`.

## Task 1: Move Session Ownership To Engine

**Files:**
- Create: `yierdis-core/yierdis-core-engine/src/main/java/yier/bubu/redis/engine/EngineSession.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnection.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/ExecutionConnectionContext.java`
- Modify: `yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyExecutionConnection.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Test: `yierdis-core/yierdis-core-engine/src/test/java/yier/bubu/redis/engine/EngineSessionTest.java`
- Test: `yierdis-executor-core/src/test/java/yier/bubu/redis/executor/ExecutionConnectionContextTest.java`

- [x] **Step 1: Run the new red tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-engine -am -Dtest=EngineSessionTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-executor-core -am -Dtest=ExecutionConnectionContextTest#connectionContextTracksPendingBytesAndClosing -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `EngineSession` is missing and `ExecutionConnectionContext()` is missing.

- [x] **Step 2: Implement engine-owned session**

Move `DefaultExecutionSession` behavior into:

```java
public final class EngineSession implements ServerSession {
    public EngineSession(int maxQueuedCommands, long maxQueuedBytes) { ... }
    public void discardTransaction() { transaction.discard(); }
    @Override public int dbIndex() { ... }
    @Override public void setDbIndex(int dbIndex) { ... }
    @Override public long clientId() { ... }
    @Override public String clientName() { ... }
    @Override public void setClientName(String clientName) { ... }
    @Override public boolean authenticated() { ... }
    @Override public void setAuthenticated(boolean authenticated) { ... }
    @Override public TransactionState transaction() { ... }
}
```

- [x] **Step 3: Remove session from executor context**

Change `ExecutionConnection` to:

```java
public interface ExecutionConnection {
    String connectionId();
    Session session();
    ExecutionConnectionContext context();
    boolean markClosing();
}
```

Change `ExecutionConnectionContext` to a no-arg class with no session field, no `session()` method, and `markClosing()` only toggling executor-local closing state.

- [x] **Step 4: Update executor and Netty wiring**

`CommandExecutorExecutionSupport` must call `connection.session()` for `CommandContext` and `connection.markClosing()` for close-after-reply or internal failure. `NettyExecutionConnection` must create `new EngineSession(txMaxCommands, txMaxBytes)`, return it from `session()`, and discard its transaction only after `context.markClosing()` transitions to closed.

- [x] **Step 5: Update tests and run Phase 2 suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-engine,yierdis-executor-core,yierdis-server -am -Dtest=EngineSessionTest,ExecutionConnectionContextTest,CommandExecutorTest,TransactionQueueCleanupTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: all listed tests pass.

## Task 2: Complete Typed CommandSpec Migration

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandParsers.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandModule.java`
- Modify: command module classes under `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ServerCommandModule.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Add a failing guard**

```java
@Test
public void productionCommandsMustRegisterTypedCommandSpecs() throws IOException {
    List<Path> offenders = productionSources().stream()
            .filter(path -> source(path).contains("CommandModule.Handler")
                    || source(path).contains("new CommandSpec(")
                    || source(path).contains("registerDisallowedInMulti(")
                    || source(path).matches("(?s).*registration\\.register\\(\"[A-Z0-9_]+\",\\s*this::.*"))
            .toList();
    Assert.assertTrue("legacy command registration remains: " + offenders, offenders.isEmpty());
}
```

- [x] **Step 2: Add request parser helper**

```java
public static CommandParser<ExecutionRequest> request(CommandArity arity) {
    Objects.requireNonNull(arity, "arity");
    return reader -> {
        String error = arity.validate(reader.request().argc());
        if (error != null) {
            return CommandParseResult.error(error);
        }
        return CommandParseResult.ok(reader.request());
    };
}
```

- [x] **Step 3: Convert legacy registrations**

Replace `registration.register("PING", this::ping, descriptor)` with:

```java
registration.register("PING", descriptor, CommandParsers.request(CommandArity.oneOf("PING", 1, 2)), this::ping);
```

Replace `registerDisallowedInMulti` with explicit `CommandSpec.of(...).disallowedInMulti(...)`.

- [x] **Step 4: Remove legacy surfaces and verify**

Remove `CommandModule.Handler`, legacy `register` overloads, legacy `CommandSpec` constructor, `legacyParser()`, `legacyHandler()`, and `CommandRegistry.find(...)`.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-command,yierdis-server,yierdis-core/yierdis-core-runtime -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## Task 3: Remove Legacy `Command` Request Compatibility

**Files:**
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-core/yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify tests importing `yier.bubu.redis.contract.Command`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Add failing guard**

```java
@Test
public void productionCodeMustNotUseDeprecatedCommandRequestCompatibility() throws IOException {
    List<Path> offenders = productionSources().stream()
            .filter(path -> source(path).contains("contract.Command")
                    || source(path).contains("instanceof Command")
                    || source(path).contains("execute(Command"))
            .toList();
    Assert.assertTrue("deprecated Command compatibility remains: " + offenders, offenders.isEmpty());
}
```

- [x] **Step 2: Remove compatibility**

Delete `YierdisFastCommandProcessor.execute(Command, ...)` and replace `CommandSupport` compatibility checks with pure `ExecutionRequest` operations.

- [x] **Step 3: Migrate tests and verify**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-command,yierdis-server,yierdis-core/yierdis-core-runtime -am -Dtest=ArchitectureBoundaryTest,ProtocolCommandAdapterTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## Task 4: Normalize Storage Pressure Key Flow

**Files:**
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCandidate.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryParticipant.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- Test: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Add failing storage guard**

```java
@Test
public void storagePressurePathsMustUseKeyHandles() throws IOException {
    Assert.assertFalse(source("yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCandidate.java")
            .contains("byte[] key"));
    Assert.assertFalse(source("yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java")
            .contains(".randomKey()"));
}
```

- [x] **Step 2: Change candidates and TTL cleanup**

Change `MaxmemoryCandidate` to:

```java
public record MaxmemoryCandidate(MaxmemoryParticipant owner, KeyHandle keyHandle, long lruClock) {
}
```

Update participant eviction and expiration cleanup to use `KeyHandle` and `randomKeyHandle()`.

- [x] **Step 3: Verify storage suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-db,yierdis-core/yierdis-core-runtime -am -Dtest=ArchitectureBoundaryTest,*Expiration*,*Maxmemory* -Dsurefire.failIfNoSpecifiedTests=false test
```

## Task 5: Strengthen Docs And Full Verification

**Files:**
- Modify: `docs/module-architecture.md`
- Modify: `docs/request-execution-flow.md`
- Modify: `docs/superpowers/specs/2026-04-28-engine-centric-architecture-design.md` if API shape changed
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Update docs**

Document:

```text
ProtocolCommandAdapter -> ExecutionRequest -> CommandExecutor scheduling -> YierdisEngine -> CommandSpec parser -> typed handler -> DB/storage -> ReplyWriter
```

Document that executor-core owns pending/backpressure/closing state only, while `EngineSession` owns selected DB and transaction state.

- [x] **Step 2: Run final verification**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-core/yierdis-core-runtime -am -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn test
```

Expected: architecture guards and full multi-module build pass.

## Self-Review Checklist

- [x] Phase 2 acceptance: executor-core no longer contains `DefaultExecutionSession`; `ExecutionConnectionContext` can be constructed without session state; server connection close discards `EngineSession` transactions.
- [x] Phase 3 acceptance: production command registration no longer uses `CommandModule.Handler` or legacy `CommandSpec` constructors.
- [x] Phase 4 acceptance: production command execution accepts only `ExecutionRequest`; no `instanceof Command` compatibility remains.
- [x] Phase 5 acceptance: `MaxmemoryCandidate` stores `KeyHandle`; TTL cleanup samples handles when handle APIs exist.
- [x] Phase 6 acceptance: docs and architecture guards describe and enforce the engine-centered flow.
