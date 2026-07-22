# Backend Command and Runtime Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the weak session, command registration, transaction, reply-preflight, server-observability, and flat runtime-configuration contracts with one complete command/session model whose mutations execute only after a protocol-owned reply plan is reserved.

**Architecture:** `yierdis-server-api` owns the complete `CommandSession` and framework execution/reply contracts; `yierdis-command-api` and `yierdis-command-core` own syntax, parsing, preparation, transaction policy, and prepared-command construction. RESP owns wire sizing, the executor owns prepared-command lifecycle and one-time execution, command-builtin owns HELLO/INFO/STATS rendering, and `server-main` only converts CLI values and wires immutable snapshot/configuration ports.

**Tech Stack:** Java 25, Maven reactor, JUnit 4, Netty 4.1, RESP2/RESP3, existing `ExecutionRequest`, `MutationContext`, ordered reply slots, and explicit `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` for every Java or Maven command.

## Global Constraints

- This is a coordinated breaking rewrite. Do not retain deprecated adapters, overloads, aliases, or compatibility wrappers for deleted APIs.
- Preserve one owner thread for the entire server instance. Do not introduce per-DB, per-key, or multi-owner command execution.
- Preserve existing Redis behavior except the explicitly incorrect bare `AUTH` arity behavior inside and outside `MULTI`.
- Preserve RESP2 and RESP3. Command and storage modules must not calculate protocol headers, decimal digit counts, CRLF bytes, or encoded RESP byte counts.
- Every admitted request must produce exactly one ordered reply or explicitly close its connection.
- A mutation may start only after its complete reply envelope is reserved, and a mutation must never be automatically executed twice.
- Parse, preparation, cancellation, stale re-preparation, connection close, shutdown, and rejection must each leave one explicit owner for retained requests, prepared commands, result sources, and transaction entries.
- `server-main` may contain CLI conversion, concrete adapter construction, and final wiring only; it must contain no command handler or reply renderer.
- Native defrag configuration has one path: `StorageConfig -> DbEngineConfig`. Do not add a bootstrap side channel.
- All Java and Maven commands use JDK 25. No ignored or disabled regression test is an acceptable completion condition.
- This plan consumes the storage-plan contracts `PreparedMutation<R>`, `ByteSequenceSource`, and `ByteMapSource`; it must not make `yierdis-db-api` depend on command or server implementation modules.
- This plan consumes the executor/network plan's typed capacity waiter and ordered-slot lifecycle; it defines the shared `PreparedCommand`, `ReplyShape`, `ReplyPlan`, and `ReplySizer` contracts that executor work must use verbatim.

---

## File Structure And Migration Map

### Shared execution contracts

- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSession.java`: the complete session type required by every execution entry point.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandPreparationContext.java`: read-only session context used before reservation.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandExecutionContext.java`: mutation-capable context created only after reservation.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java`: framework-owned prepared work and retained-resource lifecycle.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ValidationResult.java`: `VALID`/`STALE` pre-execution decision.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyShape.java`: protocol-neutral scalar, aggregate, sequence, and map shape algebra.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyShapes.java`: validated shape factories and retained-byte aggregation.
- Create `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplySizer.java`: active protocol sizing port.
- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Session.java`.
- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java`.
- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyPlans.java` after RESP sizing is migrated.

### Command definition and execution orchestration

- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSyntax.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandKeySpec.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/TransactionPolicy.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandDefinition.java` in Task 3 with its final preparer-based surface.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandPreparer.java` in Task 3, when prepared execution is introduced atomically.
- Delete `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandDescriptor.java`.
- Delete `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java` in Task 3 when the final `CommandDefinition` replaces it.
- Delete `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandReplyPlanner.java` in Task 3 after the executor no longer consumes request-only reply plans.
- Delete `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandHandler.java` once every registration uses a preparer.
- Modify every command registration and handler file listed explicitly in Task 3.

### Server command and snapshot lane

- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerIdentity.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerSnapshotProvider.java`.
- Create `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerSnapshot.java` and the eight snapshot records listed in Task 4.
- Create `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerCommandModule.java`.
- Create `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerInfoRenderer.java`.
- Create `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerStatsRenderer.java`.
- Delete `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java`.
- Delete `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`.
- Replace `NettyServerInfoProvider.java` with `NettyServerSnapshotProvider.java` in `server-main`.

### Grouped standalone-server configuration

- Create `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/YierdisServerConfig.java`.
- Create `NetworkConfig.java`, `ExecutorConfig.java`, `ReplyConfig.java`, `StorageConfig.java`, and `MaintenanceConfig.java` in the same package.
- Delete `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`.
- Modify `YierdisServerArgs`, `ServerConfig`, bootstrap, executor conversion, channel initialization, snapshot composition, and their named tests as listed in Task 5.

### Cross-plan contracts consumed unchanged

The storage plan must provide these exact `yier.bubu.redis.storage.api` and `yier.bubu.redis.storage.api.result` contracts before Task 3 migrates result-dependent commands:

```java
public interface PreparedMutation<R> extends AutoCloseable {
    R preview();
    boolean isCurrent();
    MutationOutcome commit(MutationContext context);
    @Override
    void close();
}

@FunctionalInterface
public interface PayloadLengthSink {
    void payloadLength(int length); // -1 is semantic null
}

public interface ByteSequenceSource extends AutoCloseable {
    int elementCount();
    long retainedMemoryBytes();
    void visitElementLengths(PayloadLengthSink out);
    void emitTo(ByteValueSink out);
    @Override
    void close();
}

public interface ByteMapSource extends AutoCloseable {
    int pairCount();
    long retainedMemoryBytes();
    void visitPairLengths(PayloadLengthSink out);
    void emitPairsTo(ByteValueSink out);
    @Override
    void close();
}
```

## Task 1: Replace Marker Session And Optional Transaction Defaults

**Files:**

- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSession.java`
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Session.java`
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/DbIndexSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ClientMetadataSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ConnectionStatsSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ProtocolNegotiationSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ConnectionStatsView.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriterFactory.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnection.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionEngine.java`
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java`
- Modify: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java`
- Test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/CommandSessionContractTest.java`
- Test: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
- Test: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`
- Test: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Interfaces:**

- Consumes: existing `ExecutionRequest`, `ConnectionStatsView`, and session capability method bodies.
- Produces: `CommandSession`; required transaction cleanup/inspection; compile-time complete session entry points for Tasks 2-5.

- [ ] **Step 1: Write the failing complete-session and transaction-contract tests**

Create `CommandSessionContractTest` with direct reflection assertions so the failure identifies every old weak contract:

```java
package yier.bubu.redis.execution.api;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class CommandSessionContractTest {
    @Test
    public void commandSessionRequiresEveryConnectionCapability() {
        Assert.assertEquals(
                Set.of(
                        DbIndexSession.class,
                        ClientMetadataSession.class,
                        TransactionSession.class,
                        ConnectionStatsSession.class,
                        ProtocolNegotiationSession.class
                ),
                Set.of(CommandSession.class.getInterfaces())
        );
    }

    @Test
    public void transactionStateHasNoDefaultOwnershipOrAbortMethods() throws Exception {
        for (String name : new String[]{
                "aborted", "markAborted", "tryEnqueue", "forEachQueued",
                "drain", "discard", "close"
        }) {
            Method method = java.util.Arrays.stream(TransactionState.class.getMethods())
                    .filter(candidate -> candidate.getName().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing method: " + name));
            Assert.assertFalse("default method remains: " + name, method.isDefault());
            Assert.assertTrue(Modifier.isPublic(method.getModifiers()));
        }
    }
}
```

Add these methods to `EngineSessionTest`; they use the existing reference-counted request fixture in that class:

```java
@Test
public void forEachQueuedPlansWithoutTransferringOwnership() {
    EngineSession session = new EngineSession(4, 64);
    TransactionState tx = session.transaction();
    tx.begin();
    ExecutionRequest request = request("SET", "k", "v");
    Assert.assertNull(tx.tryEnqueue(request));

    java.util.List<ExecutionRequest> seen = new java.util.ArrayList<>();
    tx.forEachQueued(seen::add);

    Assert.assertEquals(1, seen.size());
    Assert.assertEquals(1, tx.size());
    tx.discard();
    request.close();
}

@Test
public void closeReleasesQueuedRequestsExactlyOnce() {
    EngineSession session = new EngineSession(4, 64);
    TransactionState tx = session.transaction();
    tx.begin();
    CountingExecutionRequest request = new CountingExecutionRequest("SET", "k", "v");
    Assert.assertNull(tx.tryEnqueue(request));

    tx.close();
    tx.close();

    Assert.assertEquals(1, request.retainedCloseCount());
    Assert.assertFalse(tx.active());
    Assert.assertEquals(0, tx.size());
}
```

- [ ] **Step 2: Run the Task 1 RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core \
  -am \
  -Dtest=CommandSessionContractTest,EngineSessionTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because `CommandSession` and `TransactionState.forEachQueued(...)` do not exist and abort/cleanup methods still have defaults.

- [ ] **Step 3: Add the complete session and required transaction interfaces**

Create `CommandSession.java` exactly as follows:

```java
package yier.bubu.redis.execution.api;

public interface CommandSession extends
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
}
```

Remove `extends Session` from all five capability interfaces. Replace `ProtocolNegotiationSession` with required methods:

```java
package yier.bubu.redis.execution.api;

public interface ProtocolNegotiationSession {
    int respVersion();

    void setRespVersion(int respVersion);
}
```

Replace `TransactionState` with this ownership-complete surface:

```java
package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public interface TransactionState extends AutoCloseable {
    boolean active();

    boolean aborted();

    void begin();

    void markAborted();

    String tryEnqueue(ExecutionRequest request);

    int size();

    void forEachQueued(Consumer<? super ExecutionRequest> visitor);

    List<ExecutionRequest> drain();

    void discard();

    @Override
    void close();
}
```

`EngineSession` must declare `implements CommandSession`. Its `DefaultTransactionState.forEachQueued` invokes the visitor while the synchronized queue remains owned, and `close()` delegates to the same idempotent cleanup path as `discard()`:

```java
@Override
public synchronized void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
    Objects.requireNonNull(visitor, "visitor");
    for (ExecutionRequest request : queue) {
        visitor.accept(request);
    }
}

@Override
public synchronized void close() {
    discard();
}
```

Delete `enqueue(...)` and `planExecReply(...)`; Task 3 replaces wire-plan inspection with prepared child shapes.

- [ ] **Step 4: Migrate every production session entry point**

Apply this exact signature set; do not keep the old overloads:

```java
// ExecutionConnection
CommandSession session();

// RedisReplyWriterFactory
RedisReplyWriter newWriter(CommandSession session, BytesSink out);

// YierdisEngine and CommandExecutionEngine in this task.
void execute(CommandSession session, ExecutionRequest request, RedisReplyWriter reply);
```

`DefaultYierdisEngine.execute(...)` constructs the existing `CommandContext` directly from the complete session and `MutationContext.of(request)`; remove every `CommandSessionCapabilities.from(...)` call rather than wrapping `CommandSession`. Update `NettyExecutionConnection.session()` to return `EngineSession`, which is covariant with `CommandSession`. Task 3 replaces both `execute(...)` entry points with `prepare(...)` only after `PreparedCommand` exists.

Update `ArchitectureBoundaryTest.engineAndExecutorMustExposeSessionRequestReplyBoundary` to require the exact source text `void execute(CommandSession session, ExecutionRequest request, RedisReplyWriter` in both engine interfaces and to reject imports/references to `execution.api.Session`.

Migrate these test fixtures to `implements CommandSession` and implement all required capability methods directly:

- `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/TestCommandContexts.java`
- `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/ContractsIntegrationSmokeTest.java`

- [ ] **Step 5: Run the Task 1 GREEN tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-api,yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-executor \
  -am \
  -Dtest=CommandSessionContractTest,EngineSessionTest,DefaultYierdisEngineTest,ExecutorCoreTestSupport \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; no test can construct a marker-only session and queued transaction cleanup is idempotent.

- [ ] **Step 6: Enforce zero residue for the deleted session model**

Run:

```bash
rg -n 'CommandSessionCapabilities|execution\.api\.Session|\bSession session\(\)|extends Session' \
  --glob '*.java' \
  yierdis-command yierdis-networking yierdis-server yierdis-tests
```

Expected: no output.

- [ ] **Step 7: Commit Task 1**

```bash
git add \
  yierdis-server/yierdis-server-api \
  yierdis-server/yierdis-server-core \
  yierdis-server/yierdis-server-executor \
  yierdis-networking/yierdis-networking-resp \
  yierdis-networking/yierdis-networking-netty \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionConnection.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: require complete command sessions"
```

## Task 2: Make CommandSyntax The Only Arity, Metadata, And MULTI Policy Source

**Files:**

- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSyntax.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandKeySpec.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/TransactionPolicy.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandArity.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandParsers.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandModule.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandDescriptor.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionQueuePolicy.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`
- Modify test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandSpecTest.java`
- Test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandSyntaxTest.java`
- Test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandMetadataRegressionTest.java`
- Rename test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandDescriptorRegistryTest.java` to `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandSyntaxRegistryTest.java`
- Modify test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/CommandRegistryTest.java`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify test: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify test: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`

**Interfaces:**

- Consumes: `CommandSession` from Task 1 plus the existing `CommandSpec<T>`, `CommandParser<T>`, `CommandHandler<T>`, `CommandReplyPlanner`, and `CommandContext` execution path.
- Produces: final `CommandSyntax`, `CommandArity`, `CommandKeySpec`, and `TransactionPolicy` contracts. The existing `CommandSpec<T>` temporarily owns one syntax object, parser, handler, and planner so Task 2 compiles; Task 3 deletes that old type once and introduces the final preparer-based `CommandDefinition<T>`.

- [ ] **Step 1: Write RED tests for syntax-derived metadata and bare AUTH**

Create `CommandSyntaxTest` with these methods:

```java
package yier.bubu.redis.command.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;

public class CommandSyntaxTest {
    @Test
    public void arityValidatesAndEmitsMetadataFromOneObject() {
        CommandArity exact = CommandArity.exact(2);
        Assert.assertEquals(2, exact.redisMetadataArity());
        Assert.assertNull(exact.validate("get", ArgReader.of(request("GET", "k"))));
        Assert.assertNotNull(exact.validate("get", ArgReader.of(request("GET"))));

        CommandArity oneOf = CommandArity.oneOf(2, 4);
        Assert.assertEquals(-2, oneOf.redisMetadataArity());
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k"))));
        Assert.assertNull(oneOf.validate("bitcount", ArgReader.of(request("BITCOUNT", "k", "0", "1"))));
    }

    @Test
    public void registrationTakesOneSpecWithoutASeparateName() throws Exception {
        Assert.assertNotNull(CommandModule.Registration.class.getMethod(
                "register", CommandSpec.class));
        for (java.lang.reflect.Method method : CommandModule.Registration.class.getMethods()) {
            if (method.getName().equals("register")) {
                Assert.assertArrayEquals(new Class<?>[]{CommandSpec.class}, method.getParameterTypes());
            }
        }
    }

    private static ByteArrayExecutionRequest request(String... args) {
        return ByteArrayExecutionRequest.fromUtf8(args[0], java.util.List.of(args).subList(1, args.length));
    }
}
```

Replace descriptor assertions in `CommandSpecTest` with this syntax-first case:

```java
@Test
public void specValidatesItsSyntaxBeforeInvokingTheCustomParser() {
    java.util.concurrent.atomic.AtomicInteger parserCalls =
            new java.util.concurrent.atomic.AtomicInteger();
    CommandSyntax syntax = new CommandSyntax(
            "AUTH", CommandArity.min(2), CommandKeySpec.NONE,
            TransactionPolicy.QUEUEABLE);
    CommandSpec<ArgReader> spec = CommandSpec.of(
            syntax,
            args -> {
                parserCalls.incrementAndGet();
                return CommandParseResult.ok(args);
            },
            (args, context) -> { }
    );

    CommandParseResult<ArgReader> invalid = spec.parse(request("AUTH"));
    Assert.assertFalse(invalid.ok());
    Assert.assertEquals(0, parserCalls.get());
    Assert.assertSame(syntax, spec.syntax());
}
```

Add these regression methods to `TransactionCommandTest`:

```java
@Test
public void bareAuthOutsideMultiUsesTheSyntaxArity() {
    forEachDb(db -> {
        try (FastTestClient client = new FastTestClient(TestCommandComposition.createProcessor(db))) {
            ReplyError error = (ReplyError) client.execute(java.util.List.of(b("AUTH")));
            Assert.assertEquals("ERR wrong number of arguments for 'auth' command", error.message());
        }
    });
}

@Test
public void bareAuthInsideMultiMarksDirtyAndExecDoesNotApplyQueuedWrites() {
    forEachDb(db -> {
        try (FastTestClient client = new FastTestClient(TestCommandComposition.createProcessor(db))) {
            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(cmd("MULTI"))).value());
            Assert.assertEquals("QUEUED", ((ReplySimpleString) client.execute(cmd("SET", "k", "v"))).value());
            ReplyError arity = (ReplyError) client.execute(cmd("AUTH"));
            Assert.assertEquals("ERR wrong number of arguments for 'auth' command", arity.message());
            ReplyError abort = (ReplyError) client.execute(cmd("EXEC"));
            Assert.assertEquals("EXECABORT Transaction discarded because of previous errors.", abort.message());
            Assert.assertTrue(client.execute(cmd("GET", "k")) instanceof ReplyNull);
        }
    });
}
```

- [ ] **Step 2: Run the Task 2 RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-api,yierdis-command/yierdis-command-core,yierdis-tests/yierdis-integration-tests \
  -am \
  -Dtest=CommandSyntaxTest,CommandSpecTest,CommandMetadataRegressionTest,TransactionCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because `CommandSyntax`, syntax-only metadata, and name-free spec registration do not exist, and bare `AUTH` is currently accepted by `CommandParsers.minRequest(1, "auth")`.

- [ ] **Step 3: Implement the unified syntax types**

Create the following exact records and enum:

```java
package yier.bubu.redis.command.api;

public record CommandKeySpec(int firstKeyIndex, int lastKeyIndex, int keyStep) {
    public static final CommandKeySpec NONE = new CommandKeySpec(0, 0, 0);

    public CommandKeySpec {
        if (firstKeyIndex < 0 || lastKeyIndex < -1 || keyStep < 0) {
            throw new IllegalArgumentException("invalid key index or step");
        }
        if (firstKeyIndex == 0 && (lastKeyIndex != 0 || keyStep != 0)) {
            throw new IllegalArgumentException("keyless commands must use 0, 0, 0");
        }
        if (firstKeyIndex > 0 && keyStep == 0) {
            throw new IllegalArgumentException("keyStep must be positive for keyed commands");
        }
        if (lastKeyIndex == -1 && firstKeyIndex == 0) {
            throw new IllegalArgumentException("variable-tail keys require firstKeyIndex > 0");
        }
        if (lastKeyIndex != -1 && firstKeyIndex > lastKeyIndex) {
            throw new IllegalArgumentException("lastKeyIndex must be -1 or >= firstKeyIndex");
        }
    }
}
```

```java
package yier.bubu.redis.command.api;

public enum TransactionPolicy {
    QUEUEABLE,
    TRANSACTION_CONTROL,
    DISALLOWED_IN_MULTI
}
```

```java
package yier.bubu.redis.command.api;

import java.util.Locale;
import java.util.Objects;

public record CommandSyntax(
        String nameUpper,
        CommandArity arity,
        CommandKeySpec keys,
        TransactionPolicy transactionPolicy
) {
    public CommandSyntax {
        Objects.requireNonNull(nameUpper, "nameUpper");
        Objects.requireNonNull(arity, "arity");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(transactionPolicy, "transactionPolicy");
        nameUpper = nameUpper.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty() || !nameUpper.chars().allMatch(ch -> ch <= 0x7f)) {
            throw new IllegalArgumentException("command name must be non-empty ASCII");
        }
    }

    public String nameLower() {
        return nameUpper.toLowerCase(Locale.ROOT);
    }
}
```

Replace `CommandArity` with one object that owns both acceptance and Redis metadata:

```java
package yier.bubu.redis.command.api;

import java.util.Arrays;

public final class CommandArity {
    private enum Kind { EXACT, MIN, RANGE, ONE_OF, PAIR_TAIL }

    private final Kind kind;
    private final int first;
    private final int second;
    private final int[] allowed;

    private CommandArity(Kind kind, int first, int second, int[] allowed) {
        this.kind = kind;
        this.first = first;
        this.second = second;
        this.allowed = allowed;
    }

    public static CommandArity exact(int argc) {
        requirePositive(argc, "argc");
        return new CommandArity(Kind.EXACT, argc, 0, null);
    }

    public static CommandArity min(int minArgc) {
        requirePositive(minArgc, "minArgc");
        return new CommandArity(Kind.MIN, minArgc, 0, null);
    }

    public static CommandArity range(int minArgc, int maxArgc) {
        requirePositive(minArgc, "minArgc");
        if (maxArgc < minArgc) {
            throw new IllegalArgumentException("maxArgc must be >= minArgc");
        }
        return new CommandArity(Kind.RANGE, minArgc, maxArgc, null);
    }

    public static CommandArity oneOf(int... allowedArgc) {
        if (allowedArgc == null || allowedArgc.length == 0) {
            throw new IllegalArgumentException("allowedArgc must not be empty");
        }
        int[] copy = Arrays.copyOf(allowedArgc, allowedArgc.length);
        Arrays.sort(copy);
        requirePositive(copy[0], "allowedArgc");
        for (int i = 1; i < copy.length; i++) {
            requirePositive(copy[i], "allowedArgc");
            if (copy[i] == copy[i - 1]) {
                throw new IllegalArgumentException("allowedArgc must not contain duplicates");
            }
        }
        return new CommandArity(Kind.ONE_OF, copy[0], 0, copy);
    }

    public static CommandArity pairTail(int minArgc, int tailStartIndex) {
        requirePositive(minArgc, "minArgc");
        if (tailStartIndex < 0 || tailStartIndex > minArgc) {
            throw new IllegalArgumentException("tailStartIndex is out of range");
        }
        return new CommandArity(Kind.PAIR_TAIL, minArgc, tailStartIndex, null);
    }

    public CommandParseError validate(String commandLower, ArgReader args) {
        int argc = args.argc();
        boolean accepted = switch (kind) {
            case EXACT -> argc == first;
            case MIN -> argc >= first;
            case RANGE -> argc >= first && argc <= second;
            case ONE_OF -> Arrays.binarySearch(allowed, argc) >= 0;
            case PAIR_TAIL -> argc >= first && ((argc - second) & 1) == 0;
        };
        return accepted ? null : CommandParseError.wrongArity(commandLower);
    }

    public int redisMetadataArity() {
        return kind == Kind.EXACT ? first : -first;
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
```

- [ ] **Step 4: Make the existing CommandSpec own the one syntax object**

Replace `CommandSpec` in place. It keeps the current execution mechanism for this task, but deletes `CommandDescriptor`, separate registration names, and parser-owned arity:

```java
package yier.bubu.redis.command.api;

import java.util.Objects;
import yier.bubu.redis.execution.api.ExecutionRequest;

public final class CommandSpec<T> {
    private final CommandSyntax syntax;
    private final CommandParser<T> parser;
    private final CommandHandler<T> handler;
    private final CommandReplyPlanner replyPlanner;

    private CommandSpec(
            CommandSyntax syntax,
            CommandParser<T> parser,
            CommandHandler<T> handler,
            CommandReplyPlanner replyPlanner
    ) {
        this.syntax = Objects.requireNonNull(syntax, "syntax");
        this.parser = Objects.requireNonNull(parser, "parser");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.replyPlanner = replyPlanner;
    }

    public static <T> CommandSpec<T> of(
            CommandSyntax syntax,
            CommandParser<T> parser,
            CommandHandler<T> handler
    ) {
        return new CommandSpec<>(syntax, parser, handler, null);
    }

    public CommandSpec<T> withReplyPlanner(CommandReplyPlanner planner) {
        return new CommandSpec<>(syntax, parser, handler,
                Objects.requireNonNull(planner, "planner"));
    }

    public CommandParseResult<T> parse(ExecutionRequest request) {
        ArgReader args = ArgReader.of(Objects.requireNonNull(request, "request"));
        CommandParseError arityError = syntax.arity().validate(syntax.nameLower(), args);
        return arityError == null
                ? parser.parse(args)
                : CommandParseResult.error(arityError);
    }

    public void executeParsed(Object parsed, yier.bubu.redis.execution.api.CommandContext context) {
        @SuppressWarnings("unchecked")
        T typed = (T) parsed;
        handler.execute(typed, context);
    }

    public yier.bubu.redis.execution.api.ReplyPlan planReply(ExecutionRequest request) {
        if (replyPlanner == null) {
            return yier.bubu.redis.execution.api.ReplyPlan.maximum();
        }
        yier.bubu.redis.execution.api.ReplyPlan plan = replyPlanner.plan(request);
        return plan == null ? yier.bubu.redis.execution.api.ReplyPlan.maximum() : plan;
    }

    public CommandSyntax syntax() {
        return syntax;
    }
}
```

Replace `CommandModule.Registration` with:

```java
interface Registration {
    void register(CommandSpec<?> spec);

    int commandCount();

    boolean containsUpperName(String nameUpper);

    CommandSpec<?> specByUpperName(String nameUpper);

    String[] upperNamesSorted();
}
```

Reduce `CommandParsers` to non-arity helpers only:

```java
public static CommandParser<ArgReader> args() {
    return CommandParseResult::ok;
}

public static CommandParser<ExecutionRequest> request() {
    return args -> CommandParseResult.ok(args.request());
}
```

`CommandRegistry.register(...)` derives the hash-table key from `spec.syntax().nameUpper()`. Keep `spec(ExecutionRequest)`, `specByUpperName(String)`, and `replyPlan(ExecutionRequest)` for the existing executor. Remove `descriptor(...)`, `descriptorByUpperName(...)`, and `disallowedInMultiError(...)`. Metadata reads `spec.syntax()` directly. Task 3 deletes `CommandSpec`, `replyPlan(...)`, and `CommandReplyPlanner` together.

Migrate every production registration in this same step so name-free spec registration compiles. `K` is `new CommandKeySpec(1, 1, 1)`, `KM` is `new CommandKeySpec(1, -1, 1)`, and `N` is `CommandKeySpec.NONE`; all unmarked rows use `QUEUEABLE`:

| File | Commands and exact syntax |
|---|---|
| `CoreConnectionCommands.java` | `PING oneOf(1,2) N`; `ECHO exact(2) N`; `COMMAND min(1) N`; `SELECT exact(2) N`; `QUIT exact(1) N`; `CLIENT min(2) N`; `AUTH min(2) N`; `FLUSHDB oneOf(1,2) N` |
| `KeyCommands.java` | `TYPE exact(2) K`; `MEMORY min(2) N`; `OBJECT min(2) N`; `KEYS exact(2) N`; `SCAN min(2) N`; `DEL min(2) KM`; `EXISTS min(2) KM`; `EXPIRE exact(3) K`; `PEXPIRE exact(3) K`; `EXPIREAT exact(3) K`; `PEXPIREAT exact(3) K`; `PERSIST exact(2) K`; `TTL exact(2) K`; `PTTL exact(2) K` |
| `StringCommands.java` | `SET min(3) K`; `GET exact(2) K`; `STRLEN exact(2) K`; `APPEND exact(3) K`; `SETBIT exact(4) K`; `GETBIT exact(3) K`; `BITCOUNT oneOf(2,4) K`; `INCR exact(2) K`; `DECR exact(2) K` |
| `ListCommands.java` | `LPUSH min(3) K`; `RPUSH min(3) K`; `LRANGE exact(4) K`; `LPOP oneOf(2,3) K`; `RPOP oneOf(2,3) K` |
| `HashCommands.java` | `HSET pairTail(4,2) K`; `HGET exact(3) K`; `HGETALL exact(2) K`; `HLEN exact(2) K`; `HDEL min(3) K`; `HSCAN min(3) K` |
| `SetCommands.java` | `SADD min(3) K`; `SREM min(3) K`; `SMEMBERS exact(2) K`; `SISMEMBER exact(3) K`; `SCARD exact(2) K`; `SSCAN min(3) K` |
| `ZSetCommands.java` | `ZADD pairTail(4,2) K`; `ZRANGE range(4,6) K`; `ZREVRANGE oneOf(4,5) K`; `ZRANGEBYSCORE min(4) K`; `ZREVRANGEBYSCORE min(4) K`; `ZREMRANGEBYSCORE exact(4) K`; `ZREMRANGEBYRANK exact(4) K`; `ZREM min(3) K`; `ZSCAN min(3) K` |
| `HllCommands.java` | `PFADD min(3) K`; `PFCOUNT min(2) KM`; `PFMERGE min(3) KM` |
| `TransactionCommands.java` | `MULTI exact(1) N TRANSACTION_CONTROL`; `DISCARD exact(1) N TRANSACTION_CONTROL`; `EXEC exact(1) N TRANSACTION_CONTROL` |
| `ServerCommandModule.java` | `HELLO min(1) N DISALLOWED_IN_MULTI`; `INFO oneOf(1,2) N`; `STATS exact(1) N` |

For example, the complete registration form is:

```java
registration.register(CommandSpec.of(
        new CommandSyntax("GET", CommandArity.exact(2),
                new CommandKeySpec(1, 1, 1), TransactionPolicy.QUEUEABLE),
        CommandParsers.request(),
        this::get
));
```

Preserve each existing `withReplyPlanner(...)` attachment on its migrated `CommandSpec` until Task 3 replaces the spec with a semantic preparer.

- [ ] **Step 5: Replace hard-coded transaction command-name checks with policy lookup**

Use this decision order in `TransactionQueuePolicy.queueIfNeeded(...)`:

```java
CommandSpec<?> spec = registry.spec(request);
if (spec == null) {
    tx.markAborted();
    out.error(CommandRequestSupport.unknownCommandMessage(request));
    return true;
}

if (spec.syntax().transactionPolicy() == TransactionPolicy.TRANSACTION_CONTROL) {
    return false;
}
if (spec.syntax().transactionPolicy() == TransactionPolicy.DISALLOWED_IN_MULTI) {
    tx.markAborted();
    out.error("ERR " + spec.syntax().nameUpper() + " is not allowed in MULTI");
    return true;
}

CommandParseResult<?> parsed = spec.parse(request);
if (!parsed.ok()) {
    tx.markAborted();
    out.error(parsed.error().toReplyMessage());
    return true;
}

String enqueueError = tx.tryEnqueue(request);
if (enqueueError != null) {
    out.error(enqueueError);
    return true;
}
out.simpleString("QUEUED");
return true;
```

Delete `isTransactionControl(...)`. Register `MULTI`, `EXEC`, and `DISCARD` with `TRANSACTION_CONTROL`; register `HELLO` with `DISALLOWED_IN_MULTI`; every other command is `QUEUEABLE`.

- [ ] **Step 6: Run Task 2 GREEN tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-api,yierdis-command/yierdis-command-core,yierdis-tests/yierdis-integration-tests \
  -am \
  -Dtest=CommandSyntaxTest,CommandSpecTest,YierdisFastCommandProcessorPolicyTest,CommandMetadataRegressionTest,TransactionCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; `AUTH` reports metadata `-2`, bare `AUTH` aborts a transaction before enqueue, and transaction controls are recognized from `TransactionPolicy` only.

- [ ] **Step 7: Enforce zero residue for descriptor/parser arity duplication**

Run:

```bash
rg -n 'CommandDescriptor|registerDisallowedInMulti|isTransactionControl|CommandParsers\.(exact|min|range|oneOf|pairTail)' \
  --glob '*.java' \
  yierdis-command yierdis-server yierdis-tests
```

Expected: no output.

- [ ] **Step 8: Commit Task 2**

```bash
git add yierdis-command/yierdis-command-api yierdis-command/yierdis-command-core \
  yierdis-command/yierdis-command-builtin \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java \
  yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: unify command syntax and transaction policy"
```

## Task 3: Prepare Semantic Replies Before Reserving Capacity And Execute Mutations Once

**Files:**

- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandPreparationContext.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandExecutionContext.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/PreparedCommand.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ValidationResult.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandDefinition.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandPreparer.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/SlowCommandGovernor.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyShape.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyShapes.java`
- Create: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplySizer.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyPlan.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyReservationSink.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyPlans.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandHandler.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandReplyPlanner.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSpec.java`
- Create: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/PreparedCommands.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandExceptionTranslator.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionQueuePolicy.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java`
- Modify: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistries.java`
- Delete: `yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/QueuedCommandReplayer.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java`
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionEngine.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionAttempt.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorTask.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- Create: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplySizer.java`
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`
- Modify: `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/BulkStringReplyAdapter.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CollectionScanCommandSupport.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandDb.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ReplyShapeTest.java`
- Modify test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/CoreContractSmokeTest.java`
- Delete test: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ReplyPlansTest.java`
- Rename test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandSpecTest.java` to `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/CommandDefinitionTest.java`
- Test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/PreparedCommandProcessorTest.java`
- Rename test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/TestCommandContexts.java` to `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/PreparedCommandTestSupport.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- Modify test: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java`
- Modify test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/CommandBuiltinDbAccessBoundaryTest.java`
- Modify test: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`
- Test: `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplySizerTest.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorAdmissionTest.java`
- Modify test fixture: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorBackpressureTest.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorFairSchedulingTest.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- Modify test: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/NettyExecutionAdapterIntegrationTest.java`
- Modify test fixture: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespIngressLifecycleIntegrationTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCloseTest.java`
- Modify test fixture: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java`
- Modify test: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Interfaces:**

- Consumes: Task 2's final `CommandSyntax` contracts and syntax-owning legacy `CommandSpec<T>` registrations; storage-plan `ByteSequenceSource`, `ByteMapSource`, and `PreparedMutation<R>`; and network/executor Task 1's exact `ExecutionReply`, `ReplyReservationResult`, and `CapacityRegistration` contracts. Network/executor Task 1 must be complete before this task starts.
- Produces: `PreparedCommand`, `ReplyShape`, `ReplyPlan`, and `ReplySizer` in `yierdis-server-api`; `CommandExecutionEngine.prepare(...)` for the executor; RESP-owned exact wire sizing.

- [ ] **Step 1: Write RED tests for semantic shapes and protocol-owned sizing**

Rename `CommandSpecTest` to `CommandDefinitionTest` and replace its type-shape case with:

```java
@Test
public void definitionHasOnlyFinalSyntaxParserPreparerComponents() {
    Assert.assertTrue(CommandDefinition.class.isRecord());
    Assert.assertArrayEquals(
            new Class<?>[]{CommandSyntax.class, CommandParser.class, CommandPreparer.class},
            java.util.Arrays.stream(CommandDefinition.class.getRecordComponents())
                    .map(java.lang.reflect.RecordComponent::getType)
                    .toArray(Class<?>[]::new)
    );
    Assert.assertThrows(NoSuchMethodException.class,
            () -> CommandDefinition.class.getMethod("handler"));
    Assert.assertThrows(NoSuchMethodException.class,
            () -> CommandDefinition.class.getMethod("replyPlanner"));
}
```

Create `ReplyShapeTest`:

```java
package yier.bubu.redis.execution.api;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ReplyShapeTest {
    @Test
    public void aggregateRetainedBytesComeOnlyFromSemanticChildren() {
        ReplyShape shape = ReplyShapes.array(List.of(
                ReplyShapes.bulkString(3, 11),
                ReplyShapes.integer(7),
                ReplyShapes.sequence(2, 13, consumer -> {
                    consumer.accept(1);
                    consumer.accept(-1);
                })
        ));
        Assert.assertEquals(24L, shape.retainedSourceBytes());
    }

    @Test
    public void semanticLengthViewIsRepeatableAndKeepsNullSemantic() {
        ReplyShape.ByteSequence sequence = (ReplyShape.ByteSequence) ReplyShapes.sequence(
                3, 9, consumer -> {
                    consumer.accept(2);
                    consumer.accept(-1);
                    consumer.accept(5);
                });
        List<Integer> first = new ArrayList<>();
        List<Integer> second = new ArrayList<>();
        sequence.payloadLengths().visit(first::add);
        sequence.payloadLengths().visit(second::add);
        Assert.assertEquals(List.of(2, -1, 5), first);
        Assert.assertEquals(first, second);
    }

    @Test
    public void publicRecordConstructorsCannotBypassShapeValidation() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ReplyShape.SimpleString(-1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ReplyShape.BulkString(1, -1));
        Assert.assertThrows(NullPointerException.class,
                () -> new ReplyShape.ByteSequence(1, null, 0));

        List<ReplyShape> mutable = new ArrayList<>();
        mutable.add(new ReplyShape.IntegerValue(1));
        ReplyShape.Aggregate aggregate = new ReplyShape.Aggregate(
                ReplyShape.AggregateKind.ARRAY, mutable, 0);
        mutable.clear();
        Assert.assertEquals(1, aggregate.elements().size());
    }
}
```

Create `RespReplySizerTest` with both session versions:

```java
@Test
public void oneSemanticMapProducesExactResp2AndResp3Plans() {
    ReplyShape shape = ReplyShapes.byteMap(1, 17L, consumer -> {
        consumer.accept(1);
        consumer.accept(2);
    });
    TestCommandSession session = new TestCommandSession();
    RespReplySizer sizer = new RespReplySizer();

    session.setRespVersion(2);
    ReplyPlan resp2 = sizer.plan(session, shape);
    session.setRespVersion(3);
    ReplyPlan resp3 = sizer.plan(session, shape);

    Assert.assertEquals(19L, resp2.encodedUpperBoundBytes());
    Assert.assertEquals(19L, resp3.encodedUpperBoundBytes());
    Assert.assertEquals(17L, resp2.retainedSourceBytes());
    Assert.assertEquals(17L, resp3.retainedSourceBytes());
}

@Test
public void semanticLengthCallbacksRejectInvalidValuesAndCardinality() {
    TestCommandSession session = new TestCommandSession();
    RespReplySizer sizer = new RespReplySizer();

    Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
            session,
            new ReplyShape.ByteSequence(1, consumer -> consumer.accept(-2), 0)
    ));
    Assert.assertThrows(IllegalArgumentException.class, () -> sizer.plan(
            session,
            new ReplyShape.ByteMap(1, consumer -> consumer.accept(1), 0)
    ));
}
```

The expected 19 bytes are `$1\r\na\r\n$2\r\nbb\r\n` plus the RESP2 `*2\r\n` or RESP3 `%1\r\n` aggregate header. Keep additional existing `RespReplyWriterTest` cases for null, number, map, set, and nested aggregate encodings.

- [ ] **Step 2: Write RED tests for retained preparation, stale re-preparation, and one execution**

Add these counter assertions to `ReplyCapacityBlockedSchedulingTest`; use its existing fake reply slot and deterministic executor:

```java
@Test
public void capacityWaitRetainsOnePreparedCommandAndExecutesItOnce() {
    AtomicInteger prepares = new AtomicInteger();
    AtomicInteger executes = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    Harness harness = harnessWithEngine((session, request) -> {
        prepares.incrementAndGet();
        return countingPrepared(ReplyShapes.bulkString(4096, 0), ValidationResult.VALID,
                executes, closes);
    });

    harness.blockReplyExpansion();
    harness.submit(command("GET", "key"));
    harness.drainOwner();
    Assert.assertEquals(1, prepares.get());
    Assert.assertEquals(0, executes.get());

    harness.releaseReplyExpansion();
    harness.drainOwner();
    Assert.assertEquals(1, prepares.get());
    Assert.assertEquals(1, executes.get());
    Assert.assertEquals(1, closes.get());
}

@Test
public void stalePreparedCommandIsClosedAndPreparedAgainBeforeMutation() {
    AtomicInteger prepares = new AtomicInteger();
    AtomicInteger executes = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    Harness harness = harnessWithEngine((session, request) -> countingPrepared(
            ReplyShapes.integer(1),
            prepares.getAndIncrement() == 0 ? ValidationResult.STALE : ValidationResult.VALID,
            executes,
            closes));

    harness.submit(command("INCR", "key"));
    harness.drainOwner();

    Assert.assertEquals(2, prepares.get());
    Assert.assertEquals(1, executes.get());
    Assert.assertEquals(2, closes.get());
}
```

Add `TransactionCommandTest#execReservesCombinedEnvelopeBeforeAnyQueuedMutation`, using a reply sink that rejects the `EXEC` plan, and assert both queued keys remain absent and the transaction remains populated for retry.

- [ ] **Step 3: Run the Task 3 RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-api,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-executor,yierdis-tests/yierdis-integration-tests \
  -am \
  -Dtest=CommandDefinitionTest,ReplyShapeTest,RespReplySizerTest,ReplyCapacityBlockedSchedulingTest,ReplyPreflightCommandTest,TransactionCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because semantic shapes, `ReplySizer`, and prepared task state do not exist and current capacity retry reruns the command processor.

- [ ] **Step 4: Replace CommandSpec once with the final CommandDefinition and add shared preparation contexts**

Create `CommandPreparer` and replace Task 2's existing `CommandSpec` atomically with the final `CommandDefinition`; `CommandDefinition` never exposes a handler or reply planner:

```java
package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.PreparedCommand;

@FunctionalInterface
public interface CommandPreparer<T> {
    PreparedCommand prepare(T parsed, CommandPreparationContext context);
}
```

```java
package yier.bubu.redis.command.api;

import java.util.Objects;
import yier.bubu.redis.execution.api.ExecutionRequest;

public record CommandDefinition<T>(
        CommandSyntax syntax,
        CommandParser<T> parser,
        CommandPreparer<T> preparer
) {
    public CommandDefinition {
        Objects.requireNonNull(syntax, "syntax");
        Objects.requireNonNull(parser, "parser");
        Objects.requireNonNull(preparer, "preparer");
    }

    public CommandParseResult<T> parse(ExecutionRequest request) {
        ArgReader args = ArgReader.of(Objects.requireNonNull(request, "request"));
        CommandParseError arityError = syntax.arity().validate(syntax.nameLower(), args);
        return arityError == null
                ? parser.parse(args)
                : CommandParseResult.error(arityError);
    }
}
```

In the same compilation step, replace `CommandModule.Registration` with its final surface:

```java
interface Registration {
    void register(CommandDefinition<?> definition);

    int commandCount();

    boolean containsUpperName(String nameUpper);

    CommandDefinition<?> definitionByUpperName(String nameUpper);

    String[] upperNamesSorted();
}
```

`CommandRegistry` stores `CommandDefinition<?>`, derives each table key only from `definition.syntax().nameUpper()`, and exposes `definition(ExecutionRequest)` plus `definitionByUpperName(String)`. Delete its `spec(...)`, `specByUpperName(...)`, and `replyPlan(...)` methods; metadata and transaction code now read the final definition.

Delete `CommandSpec`, `CommandHandler`, and `CommandReplyPlanner` only after every registration compiles against `CommandDefinition`/`CommandPreparer` in this same step. Then create these exact shared types in `yier.bubu.redis.execution.api`:

```java
package yier.bubu.redis.execution.api;

import java.util.Objects;

public record CommandPreparationContext(CommandSession session) {
    public CommandPreparationContext {
        Objects.requireNonNull(session, "session");
    }
}
```

```java
package yier.bubu.redis.execution.api;

public enum ValidationResult {
    VALID,
    STALE
}
```

```java
package yier.bubu.redis.execution.api;

public interface PreparedCommand extends AutoCloseable {
    ReplyShape replyShape();

    ValidationResult validateBeforeExecute();

    void execute(CommandExecutionContext context);

    @Override
    void close();
}
```

```java
package yier.bubu.redis.execution.api;

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

public final class CommandExecutionContext implements AutoCloseable {
    private final CommandSession session;
    private final RedisReplyWriter reply;
    private MutationContext mutationContext;

    private CommandExecutionContext(
            CommandSession session,
            RedisReplyWriter reply,
            MutationContext mutationContext
    ) {
        this.session = Objects.requireNonNull(session, "session");
        this.reply = Objects.requireNonNull(reply, "reply");
        this.mutationContext = Objects.requireNonNull(mutationContext, "mutationContext");
    }

    public static CommandExecutionContext forRequest(
            CommandSession session,
            RedisReplyWriter reply,
            ExecutionRequest request
    ) {
        return new CommandExecutionContext(session, reply, MutationContext.of(
                Objects.requireNonNull(request, "request")));
    }

    public CommandSession session() {
        return session;
    }

    public RedisReplyWriter reply() {
        return reply;
    }

    public MutationContext mutationContext() {
        return mutationContext;
    }

    @Override
    public void close() {
        MutationContext owned = mutationContext;
        if (owned == null) {
            return;
        }
        mutationContext = null;
        owned.close();
    }
}
```

`CommandExecutionContext.forRequest(...)` is the only executor construction path. Do not use `MutationContext.none()` for an admitted command and do not let a `PreparedCommand` create its own mutation context.

Before deleting `CommandContext`, migrate the two remaining command-api extension ports in the same compilation step. `ServerInfoProvider` is an explicitly temporary bridge deleted by Task 4; it returns prepared work and never receives a writer:

```java
package yier.bubu.redis.command.api;

import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

public interface ServerInfoProvider {
    PreparedCommand prepareInfo(
            ExecutionRequest request,
            CommandPreparationContext context
    );

    PreparedCommand prepareStats(
            ExecutionRequest request,
            CommandPreparationContext context
    );

    YierdisMemoryStats memoryStats(CommandPreparationContext context);
}
```

Change both `SlowCommandGovernor` methods, including the `UNBOUNDED` and `DEFAULT` implementations, to accept `CommandPreparationContext`:

```java
long keysTimeBudgetNanos(CommandPreparationContext context);

default int keysMaxResults(CommandPreparationContext context) {
    return Integer.MAX_VALUE;
}
```

`CommandSupport.commandDb(...)`, slow-command checks, and the `MEMORY STATS` global-memory lookup receive the preparation context and capture every value needed by execution. The bootstrap's configured governor overrides the same two signatures; its values remain the configured constants.

Migrate the existing `server-main` `ServerCommandModule` to `CommandDefinition` in Task 3, before Task 4 moves it. `HELLO` uses the exact semantic five-pair shape and deferred session mutation described in Step 9. `INFO` and `STATS` delegate from their preparers to `ServerInfoProvider.prepareInfo(...)` and `prepareStats(...)`.

Until Task 4 replaces the bridge with immutable snapshots, `NettyServerInfoProvider` returns `ReplyShape.Maximum` prepared commands, removes every `requireReply(...)`, `ReplyPlans`, and `ReplyPlanMeasurer` call, and invokes its existing renderer only from `execute(...)`. Use this exact bridge helper:

```java
private static PreparedCommand maximumReply(
        java.util.function.Consumer<CommandExecutionContext> execution
) {
    java.util.Objects.requireNonNull(execution, "execution");
    return new PreparedCommand() {
        @Override public ReplyShape replyShape() { return ReplyShapes.maximum(); }
        @Override public ValidationResult validateBeforeExecute() { return ValidationResult.VALID; }
        @Override public void execute(CommandExecutionContext context) { execution.accept(context); }
        @Override public void close() { }
    };
}
```

`prepareInfo(request, context)` returns `maximumReply(execution -> writeInfo(request, execution.session(), execution.reply()))`; `prepareStats(...)` does the same with `writeStats`. Change the old public writer methods into private `writeInfo(ExecutionRequest, CommandSession, RedisReplyWriter)` and `writeStats(ExecutionRequest, CommandSession, RedisReplyWriter)` methods, and read connection counters from `session.connectionStats()`. `memoryStats(CommandPreparationContext)` keeps the existing global-scope behavior. Task 4 removes this maximum-reservation bridge and captures exact immutable reply shapes.

In `ServerCommandComposition`, change transaction replay registration from `processor::execute` to `processor::prepare`. In `YierdisServerBootstrap`, change the engine decorator target from `commandEngine::execute` to `commandEngine::prepare`; the network/executor plan's prepared-task integration consumes that same method reference. Update `package-info.java` and `CoreContractSmokeTest` to name the two new contexts, assert that `CommandExecutionContext` has no public constructor, and assert that its only public factory taking a request is `forRequest(CommandSession, RedisReplyWriter, ExecutionRequest)`. Delete `ReplyPlansTest`; `RespReplySizerTest` owns the replacement formulas.

- [ ] **Step 5: Add the protocol-neutral ReplyShape algebra**

Create `ReplyShape.java`:

```java
package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;

public sealed interface ReplyShape permits
        ReplyShape.SimpleString,
        ReplyShape.Error,
        ReplyShape.IntegerValue,
        ReplyShape.BooleanValue,
        ReplyShape.DoubleValue,
        ReplyShape.BigNumber,
        ReplyShape.VerbatimString,
        ReplyShape.BlobError,
        ReplyShape.BulkString,
        ReplyShape.NullValue,
        ReplyShape.NullArray,
        ReplyShape.Aggregate,
        ReplyShape.ByteSequence,
        ReplyShape.ByteMap,
        ReplyShape.Maximum {

    long retainedSourceBytes();

    @FunctionalInterface
    interface PayloadLengths {
        void visit(java.util.function.IntConsumer consumer);
    }

    enum AggregateKind {
        ARRAY,
        MAP,
        SET,
        PUSH,
        ATTRIBUTE
    }

    record SimpleString(int payloadLength) implements ReplyShape {
        public SimpleString {
            if (payloadLength < 0) throw new IllegalArgumentException("payloadLength must be >= 0");
        }
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record Error(int payloadLength) implements ReplyShape {
        public Error {
            if (payloadLength < 0) throw new IllegalArgumentException("payloadLength must be >= 0");
        }
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record IntegerValue(long value) implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record BooleanValue(boolean value) implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record DoubleValue(double value) implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record BigNumber(int asciiLength) implements ReplyShape {
        public BigNumber {
            if (asciiLength < 0) throw new IllegalArgumentException("asciiLength must be >= 0");
        }
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record VerbatimString(int formatLength, int payloadLength) implements ReplyShape {
        public VerbatimString {
            if (formatLength < 0 || payloadLength < 0) {
                throw new IllegalArgumentException("verbatim lengths must be >= 0");
            }
        }
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record BlobError(int payloadLength) implements ReplyShape {
        public BlobError {
            if (payloadLength < 0) throw new IllegalArgumentException("payloadLength must be >= 0");
        }
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record BulkString(int payloadLength, long retainedSourceBytes) implements ReplyShape {
        public BulkString {
            if (payloadLength < 0 || retainedSourceBytes < 0L) {
                throw new IllegalArgumentException("bulk-string lengths must be >= 0");
            }
        }
    }

    record NullValue() implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record NullArray() implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }

    record Aggregate(
            AggregateKind kind,
            List<ReplyShape> elements,
            long retainedSourceBytes
    ) implements ReplyShape {
        public Aggregate {
            Objects.requireNonNull(kind, "kind");
            elements = List.copyOf(Objects.requireNonNull(elements, "elements"));
            if (retainedSourceBytes < 0L) {
                throw new IllegalArgumentException("retainedSourceBytes must be >= 0");
            }
        }
    }

    record ByteSequence(
            int elementCount,
            PayloadLengths payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteSequence {
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            if (elementCount < 0 || retainedSourceBytes < 0L) {
                throw new IllegalArgumentException("sequence count and retained bytes must be >= 0");
            }
        }
    }

    record ByteMap(
            int pairCount,
            PayloadLengths payloadLengths,
            long retainedSourceBytes
    ) implements ReplyShape {
        public ByteMap {
            Objects.requireNonNull(payloadLengths, "payloadLengths");
            if (pairCount < 0 || retainedSourceBytes < 0L) {
                throw new IllegalArgumentException("map pair count and retained bytes must be >= 0");
            }
        }
    }

    record Maximum() implements ReplyShape {
        @Override public long retainedSourceBytes() { return 0L; }
    }
}
```

The public nested-record constructors enforce the same non-negative/non-null invariants as the factories, and `Aggregate` always performs `List.copyOf`; callers cannot bypass validation with `new ReplyShape.*(...)`. `ReplyShapes` additionally sums child retained bytes with saturation. `RespReplySizer` must fail fast if a `PayloadLengths` callback emits a value below `-1`, emits a number of values different from `elementCount` for `ByteSequence`, or different from `pairCount * 2` for `ByteMap`; `-1` is the only semantic null payload length. Its public factory surface is:

```java
public static ReplyShape simpleString(String value);
public static ReplyShape error(String value);
public static ReplyShape integer(long value);
public static ReplyShape integerUpperBound();
public static ReplyShape booleanValue(boolean value);
public static ReplyShape doubleValue(double value);
public static ReplyShape bigNumber(String ascii);
public static ReplyShape verbatimString(String format, int payloadLength);
public static ReplyShape blobError(String value);
public static ReplyShape bulkString(int payloadLength, long retainedSourceBytes);
public static ReplyShape nullValue();
public static ReplyShape nullArray();
public static ReplyShape array(List<? extends ReplyShape> elements);
public static ReplyShape map(List<? extends ReplyShape> fieldValues);
public static ReplyShape set(List<? extends ReplyShape> elements);
public static ReplyShape push(List<? extends ReplyShape> elements);
public static ReplyShape attribute(List<? extends ReplyShape> fieldValues);
public static ReplyShape sequence(int count, long retainedSourceBytes, ReplyShape.PayloadLengths lengths);
public static ReplyShape byteMap(int pairCount, long retainedSourceBytes, ReplyShape.PayloadLengths lengths);
public static ReplyShape maximum();
```

`integerUpperBound()` returns `new ReplyShape.IntegerValue(Long.MIN_VALUE)`, whose decimal representation is the maximum signed-long width; it is for mutations whose integer result is not known until commit.

- [ ] **Step 6: Move all wire-size formulas behind ReplySizer**

Create the shared port and keep `ReplyPlan` free of shape factories:

```java
package yier.bubu.redis.execution.api;

@FunctionalInterface
public interface ReplySizer {
    ReplyPlan plan(CommandSession session, ReplyShape shape);
}
```

```java
package yier.bubu.redis.execution.api;

public final class ReplyPlan {
    private final long encodedUpperBoundBytes;
    private final long retainedSourceBytes;
    private final boolean reserveMaximum;

    private ReplyPlan(long encodedUpperBoundBytes, long retainedSourceBytes, boolean reserveMaximum) {
        if (encodedUpperBoundBytes < 0L || retainedSourceBytes < 0L) {
            throw new IllegalArgumentException("reply plan bytes must be non-negative");
        }
        this.encodedUpperBoundBytes = encodedUpperBoundBytes;
        this.retainedSourceBytes = retainedSourceBytes;
        this.reserveMaximum = reserveMaximum;
    }

    public static ReplyPlan exact(long encodedUpperBoundBytes, long retainedSourceBytes) {
        return new ReplyPlan(encodedUpperBoundBytes, retainedSourceBytes, false);
    }

    public static ReplyPlan maximum() {
        return new ReplyPlan(0L, 0L, true);
    }

    public long encodedUpperBoundBytes() { return encodedUpperBoundBytes; }
    public long retainedSourceBytes() { return retainedSourceBytes; }
    public boolean reserveMaximum() { return reserveMaximum; }

    public long totalUpperBoundBytes() {
        return encodedUpperBoundBytes > Long.MAX_VALUE - retainedSourceBytes
                ? Long.MAX_VALUE
                : encodedUpperBoundBytes + retainedSourceBytes;
    }
}
```

`ReplyPlan` is opaque to command and storage code even though executor/reply adapters need public accessors across Maven modules. Only `yierdis-server-executor`, `yierdis-networking-resp`, and the reply adapters in `yierdis-server-main` may call its factories or accessors; extend `ArchitectureBoundaryTest` with source/import guards rejecting `ReplyPlan` references from `yierdis-command` and `yierdis-db`.

`RespReplySizer.plan(...)` switches exhaustively over every permitted shape. It chooses RESP2 or RESP3 from `session.respVersion()`, visits semantic payload lengths without consuming them, validates callback values/counts as specified above, owns every `$`, `*`, `%`, `~`, `>`, `|`, decimal digit, and CRLF formula, and returns `ReplyPlan.maximum()` only for `ReplyShape.Maximum`. Remove `ReplyPlans`, the `NettyServerInfoProvider.ReplyPlanMeasurer`, and every command/storage encoded-byte helper.

Remove `RedisReplyWriter.requireReply(...)`, `requireReplyEnvelope(...)`, `writeMeasuredBulkStringArray(...)`, and `writeMeasuredBulkStringMap(...)`. Rendering methods remain; capacity has already been reserved when rendering begins.

- [ ] **Step 7: Make the engine prepare once and let the executor own prepared lifecycle**

Use this exact boundary:

```java
package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;

@FunctionalInterface
public interface CommandExecutionEngine {
    PreparedCommand prepare(CommandSession session, ExecutionRequest request);
}
```

`YierdisEngine` has the same `prepare(...)` signature plus maintenance/close. `DefaultYierdisEngine.prepare(...)` delegates to:

```java
return commandProcessor.prepare(
        request,
        new CommandPreparationContext(session)
);
```

Update `ArchitectureBoundaryTest.engineAndExecutorMustExposeSessionRequestReplyBoundary` again in this atomic task: both source assertions now require `PreparedCommand prepare(CommandSession session, ExecutionRequest request);`, and the method rejects `void execute(`, `CommandContext`, and `RedisReplyWriter` in both public engine interfaces.

Make the `CommandExecutor` migration atomic. Its only constructor receives `ReplySizer replySizer` between `SerialOwnerExecutor ownerExecutor` and `RedisReplyWriterFactory replyWriterFactory`, stores it, and passes it to `CommandExecutorExecutionSupport`; do not retain an overload without the sizer. `YierdisServerBootstrap` and every named `server-main` fixture above pass `new RespReplySizer()`. The executor-test fixtures use one deterministic `ReplySizer` (for example, `(session, shape) -> ReplyPlan.exact(64L, shape.retainedSourceBytes())`), and `ExecutorCoreTestSupport.simpleCommandEngine()` returns a matching `PreparedCommand` rather than an `execute(...)` lambda. Update every named direct constructor call and test-local engine lambda in this task, including the `ExecutorAdmissionTest` created by network/executor Task 1.

The executor call point is fixed and must not call the engine a second time after `VALID`. `capacityWakeup` is the network Task 1 blocked-head callback supplied by the drain loop; it marks this connection's blocked head ready and schedules one owner drain. `CommandExecutorTask` stores a nullable `CapacityRegistration capacityRegistration`:

```java
if (task.prepared == null) {
    task.prepared = commandProcessor.prepare(connection.session(), task.request);
    task.replyPlan = replySizer.plan(connection.session(), task.prepared.replyShape());
}
switch (task.reply.tryReserve(task.replyPlan)) {
    case WAITING -> {
        if (task.capacityRegistration == null) {
            task.capacityRegistration = task.reply.onCapacityAvailable(capacityWakeup);
        }
        return ExecutionAttempt.REPLY_CAPACITY_BLOCKED;
    }
    case TOO_LARGE -> {
        RedisReplyWriter error = replyWriterFactory.newWriter(
                connection.session(), task.reply.sink());
        error.error("ERR reply exceeds configured maximum");
        task.reply.markReady(error.closeAfterReplyRequested());
        return ExecutionAttempt.TERMINAL_REPLY;
    }
    case CLOSED -> {
        return ExecutionAttempt.CONNECTION_CLOSED;
    }
    case RESERVED -> {
        if (task.capacityRegistration != null) {
            task.capacityRegistration.cancel();
            task.capacityRegistration = null;
        }
    }
}
if (task.prepared.validateBeforeExecute() == ValidationResult.STALE) {
    task.prepared.close();
    task.prepared = null;
    task.replyPlan = null;
    return ExecutionAttempt.REPREPARE;
}
RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), task.reply.sink());
try (CommandExecutionContext context = CommandExecutionContext.forRequest(
        connection.session(), writer, task.request)) {
    task.prepared.execute(context);
}
task.reply.markReady(writer.closeAfterReplyRequested());
```

The ordered reply slot keeps any reservation already obtained before a `STALE` result; the re-prepared shape is passed to `tryReserve(...)` again, which either reuses or expands that slot-owned lease. There is no `releaseExpansionReservation` API. Task 3 adds `ExecutionAttempt.REPREPARE` and makes `CommandExecutorDrainLoop` requeue the same owned task through the Task 1 queue discipline; it retains the request and reply slot and does not execute the command while stale. Network/executor Task 2 replaces that provisional requeue with its guarded cleanup and failure handling. `CommandExecutorTask` owns nullable `PreparedCommand prepared`, `ReplyPlan replyPlan`, and `CapacityRegistration capacityRegistration`; all terminal paths cancel the registration and close `prepared` exactly once before clearing them. Capacity-blocked paths retain the prepared object and plan. Post-mutation rendering failure calls `markResultUnknown()`, closes the connection, and never returns `REPLY_CAPACITY_BLOCKED`.

- [ ] **Step 8: Convert command processing into parse/prepare and rebuild EXEC as a prepared envelope**

Create `PreparedCommands` as the fixed-reply building block:

```java
package yier.bubu.redis.command.kernel;

import java.util.Objects;
import java.util.function.Consumer;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ValidationResult;

final class PreparedCommands {
    private PreparedCommands() {
    }

    static PreparedCommand fixed(ReplyShape shape, Consumer<CommandExecutionContext> execution) {
        Objects.requireNonNull(shape, "shape");
        Objects.requireNonNull(execution, "execution");
        return new PreparedCommand() {
            @Override public ReplyShape replyShape() { return shape; }
            @Override public ValidationResult validateBeforeExecute() { return ValidationResult.VALID; }
            @Override public void execute(CommandExecutionContext context) { execution.accept(context); }
            @Override public void close() { }
        };
    }

    static PreparedCommand error(String message) {
        return fixed(
                yier.bubu.redis.execution.api.ReplyShapes.error(message),
                context -> context.reply().error(message));
    }
}
```

`YierdisFastCommandProcessor.prepare(...)` performs, in order: empty/null validation, registry lookup, transaction preflight/queue decision, definition arity+parser, and `definition.preparer().prepare(parsed.value(), context)`. Every error returns `PreparedCommands.error(...)`; it never writes a reply during preparation.

`EXEC` preparation must:

1. Reject `EXEC` without `MULTI` or dirty transactions with a fixed error prepared command.
2. Call `tx.forEachQueued(...)`, look up and parse each queued definition, and prepare every child without draining.
3. On a child failure, close previously prepared children in reverse order, mark the transaction aborted when Redis dirty-transaction semantics require it, and return an error prepared command.
4. Return a prepared command whose shape is `ReplyShapes.array(childShapes)`.
5. Validate every child immediately before execution; return `STALE` if any child is stale, causing the framework to close and reprepare the entire set.
6. Only inside `execute(...)`, call `tx.drain()`, render `arrayHeader(children.size())`, execute children in queue order, close each drained request, and close unexecuted tail requests on failure.

The prepared EXEC `close()` closes child prepared commands but does not drain the transaction; connection/session cleanup still owns queued requests until successful execution or discard.

- [ ] **Step 9: Migrate every builtin definition and remove handler-side reservation**

Use this complete syntax matrix. `K` means `new CommandKeySpec(1, 1, 1)`, `KM` means `new CommandKeySpec(1, -1, 1)`, and `N` means `CommandKeySpec.NONE`. Every row is `QUEUEABLE` unless explicitly marked.

| File | Commands and exact `CommandArity` / key spec / policy |
|---|---|
| `CoreConnectionCommands.java` | `PING oneOf(1,2) N`; `ECHO exact(2) N`; `COMMAND min(1) N`; `SELECT exact(2) N`; `QUIT exact(1) N`; `CLIENT min(2) N`; `AUTH min(2) N`; `FLUSHDB oneOf(1,2) N` |
| `KeyCommands.java` | `TYPE exact(2) K`; `MEMORY min(2) N`; `OBJECT min(2) N`; `KEYS exact(2) N`; `SCAN min(2) N`; `DEL min(2) KM`; `EXISTS min(2) KM`; `EXPIRE exact(3) K`; `PEXPIRE exact(3) K`; `EXPIREAT exact(3) K`; `PEXPIREAT exact(3) K`; `PERSIST exact(2) K`; `TTL exact(2) K`; `PTTL exact(2) K` |
| `StringCommands.java` | `SET min(3) K`; `GET exact(2) K`; `STRLEN exact(2) K`; `APPEND exact(3) K`; `SETBIT exact(4) K`; `GETBIT exact(3) K`; `BITCOUNT oneOf(2,4) K`; `INCR exact(2) K`; `DECR exact(2) K` |
| `ListCommands.java` | `LPUSH min(3) K`; `RPUSH min(3) K`; `LRANGE exact(4) K`; `LPOP oneOf(2,3) K`; `RPOP oneOf(2,3) K` |
| `HashCommands.java` | `HSET pairTail(4,2) K`; `HGET exact(3) K`; `HGETALL exact(2) K`; `HLEN exact(2) K`; `HDEL min(3) K`; `HSCAN min(3) K` |
| `SetCommands.java` | `SADD min(3) K`; `SREM min(3) K`; `SMEMBERS exact(2) K`; `SISMEMBER exact(3) K`; `SCARD exact(2) K`; `SSCAN min(3) K` |
| `ZSetCommands.java` | `ZADD pairTail(4,2) K`; `ZRANGE range(4,6) K`; `ZREVRANGE oneOf(4,5) K`; `ZRANGEBYSCORE min(4) K`; `ZREVRANGEBYSCORE min(4) K`; `ZREMRANGEBYSCORE exact(4) K`; `ZREMRANGEBYRANK exact(4) K`; `ZREM min(3) K`; `ZSCAN min(3) K` |
| `HllCommands.java` | `PFADD min(3) K`; `PFCOUNT min(2) KM`; `PFMERGE min(3) KM` |
| `TransactionCommands.java` | `MULTI exact(1) N TRANSACTION_CONTROL`; `DISCARD exact(1) N TRANSACTION_CONTROL`; `EXEC exact(1) N TRANSACTION_CONTROL` |
| Current `server-main/ServerCommandModule.java` (moved in Task 4) | `HELLO min(1) N DISALLOWED_IN_MULTI`; `INFO oneOf(1,2) N`; `STATS exact(1) N` |

Use this execution classification, with no handler left unassigned:

- Request-retained bulk replies: `PING <message>` and `ECHO` retain the request in preparation, expose a bulk shape with `request.admittedMemoryBytes()`, render the captured argument, and release the retained request in `close()`.
- Read-result sources prepared before reservation: `GET`, `HGET`, `HGETALL`, `LRANGE`, `SMEMBERS`, `ZRANGE`, `ZREVRANGE`, both score ranges, `KEYS`, `SCAN`, `HSCAN`, `SSCAN`, and `ZSCAN`; their prepared object owns the `ByteSequenceSource`, `ByteMapSource`, or scalar value until close.
- Result-dependent prepared mutations: `SET ... GET`, uncounted/counted `LPOP` and `RPOP`, and storage operations whose storage-plan API returns `PreparedMutation`; `validateBeforeExecute()` maps `isCurrent()` to `VALID`/`STALE`, and `execute(...)` calls `commit(context.mutationContext())` once.
- Request-bounded mutations using conservative shapes: `FLUSHDB`, `DEL`, expiration writes, `PERSIST`, `APPEND`, bit writes, `INCR`, `DECR`, pushes, hash/set/zset/hll writes, and removals. Use `ReplyShapes.integerUpperBound()`, `simpleString("OK")`, or the exact null/bulk alternative known from parsed options before mutation.
- Session-only state: `SELECT`, `QUIT`, `CLIENT`, `AUTH`, `MULTI`, and `DISCARD` capture validation during preparation and mutate session/transaction state only inside `execute(...)`.
- Pure scalar reads: `TYPE`, `MEMORY USAGE`, `OBJECT ENCODING`, `EXISTS`, `TTL`, `PTTL`, `STRLEN`, `GETBIT`, `BITCOUNT`, `HLEN`, `SISMEMBER`, `SCARD`, and `PFCOUNT` compute and capture their scalar during read-only preparation.
- Metadata replies: `COMMAND`, `COMMAND COUNT`, and `COMMAND INFO` capture `CommandDefinition.syntax()` values and construct an aggregate shape; they must not calculate encoded bytes.

Delete every inline arity check already guaranteed by `CommandSyntax`. Custom parsers retain only option/value validation. Delete every `requireReply*`, `ReplyPlans.*`, `encodedElementBytes`, and `transferReplyOwnership` call; prepared object `close()` is the sole source-owner cleanup.

Rename `TestCommandContexts` to `PreparedCommandTestSupport` and migrate `YierdisFastCommandProcessorModuleTest`, `YierdisFastCommandProcessorPolicyTest`, and `YierdisFastCommandProcessorRegistrationTest` away from direct `processor.execute(...)` calls. Their common execution helper is:

```java
static void execute(
        YierdisFastCommandProcessor processor,
        ExecutionRequest request,
        CommandSession session,
        RedisReplyWriter reply
) {
    try (PreparedCommand prepared = processor.prepare(
            request, new CommandPreparationContext(session))) {
        org.junit.Assert.assertEquals(
                ValidationResult.VALID, prepared.validateBeforeExecute());
        try (CommandExecutionContext context =
                     CommandExecutionContext.forRequest(session, reply, request)) {
            prepared.execute(context);
        }
    }
}
```

Registration tests construct `CommandDefinition` with `CommandSyntax`, parser, and a preparer returning `PreparedCommands.fixed(...)`; registry reflection asserts `definition(ExecutionRequest)` returns `CommandDefinition`. Rename `CommandBuiltinDbAccessBoundaryTest.ordinaryCommandHandlersUseCommandDbFacadeInsteadOfDbEngine` to `ordinaryCommandsUseCommandDbFacadeInsteadOfDbEngine` so the old type name is absent from the zero-residue scan. `DefaultYierdisEngineTest` calls `prepare(...)`, validates, executes with `CommandExecutionContext.forRequest(...)`, and closes both prepared work and context.

- [ ] **Step 10: Run focused GREEN tests for command preparation and RESP sizing**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command,yierdis-networking/yierdis-networking-resp,yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests \
  -am \
  -Dtest=CommandDefinitionTest,ReplyShapeTest,CoreContractSmokeTest,RespReplySizerTest,PreparedCommandProcessorTest,ReplyCapacityBlockedSchedulingTest,ReplyPreflightCommandTest,TransactionCommandTest,StringCommandTest,ListCommandTest,HashCommandTest,SetCommandTest,ZSetCommandTest,HllCommandTest,YierdisServerBootstrapCommandWiringTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; capacity waiting preserves a single prepared instance, stale preparations are replaced before mutation, EXEC reserves one aggregate plan, and all builtin command behavior remains intact.

- [ ] **Step 11: Enforce zero residue for command-side wire sizing and old execution context**

Run:

```bash
rg -n 'CommandSpec|CommandHandler|ReplyPlans|requireReply|requireReplyEnvelope|writeMeasuredBulkString|encodedElementBytes|decimalDigits|CommandReplyPlanner|CommandContext|RedisReplyWriter out' \
  --glob '*.java' \
  yierdis-command yierdis-db \
  yierdis-server/yierdis-server-api
```

Expected: no output. `RedisReplyWriter out` is forbidden in command preparer/provider signatures; rendering lambdas use `CommandExecutionContext.reply()` after reservation.

Run the narrower transition scan for `server-main`, where the temporary `NettyServerInfoProvider` renderer may still accept a writer until Task 4:

```bash
rg -n 'CommandSpec|CommandHandler|ReplyPlans|requireReply|requireReplyEnvelope|encodedElementBytes|decimalDigits|CommandReplyPlanner|CommandContext' \
  --glob '*.java' \
  yierdis-server/yierdis-server-main
```

Expected: no output; the temporary provider reserves `ReplyShape.Maximum` through prepared work and contains no old context, planner, or reservation API.

Run:

```bash
rg -n 'ReplyPlan' --glob '*.java' yierdis-command yierdis-db
```

Expected: no output; command and storage code return `ReplyShape`, never wire capacity plans.

- [ ] **Step 12: Commit Task 3**

```bash
git add \
  yierdis-server/yierdis-server-api \
  yierdis-server/yierdis-server-core \
  yierdis-server/yierdis-server-executor \
  yierdis-server/yierdis-server-main \
  yierdis-command \
  yierdis-networking/yierdis-networking-resp \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/ReplyPreflightCommandTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: prepare semantic replies before mutation"
```

## Task 4: Move Server Commands And Rendering Behind Immutable Snapshot Ports

**Files:**

- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerIdentity.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerSnapshotProvider.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/RuntimeSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ExecutorSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ConnectionSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/InboundSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/OutboundSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommitStreamSnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/MemorySnapshot.java`
- Create: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/HealthSnapshot.java`
- Create: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerCommandModule.java`
- Create: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerInfoRenderer.java`
- Create: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/server/ServerStatsRenderer.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/DefaultCommandModules.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java`
- Modify: `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- Delete: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerSnapshotProvider.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandComposition.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Test: `yierdis-command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/ServerSnapshotContractTest.java`
- Test: `yierdis-command/yierdis-command-builtin/src/test/java/yier/bubu/redis/command/defaults/server/ServerCommandModuleTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespHandshakeIntegrationTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RedisCliCompatibilityTest.java`
- Modify test: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Interfaces:**

- Consumes: Task 3's `CommandDefinition<T>`, `PreparedCommand`, `ReplyShape`, and read-only `CommandPreparationContext`; current executor, connection, inbound, outbound, commit-stream, memory, and runtime observability snapshots only inside the `server-main` adapter.
- Produces: `ServerIdentity`; `ServerSnapshotProvider.snapshot(ConnectionStatsView)`; the aggregate `ServerSnapshot` and exactly eight transport/runtime-neutral snapshot records; builtin-owned HELLO/INFO/STATS definitions and renderers.

- [ ] **Step 1: Write RED contract, rendering, and architecture tests**

Create `ServerSnapshotContractTest`:

```java
package yier.bubu.redis.command.api;

import java.lang.reflect.RecordComponent;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ConnectionStatsView;

public class ServerSnapshotContractTest {
    @Test
    public void providerAcceptsOnlySemanticConnectionStatsAndReturnsOneSnapshot() throws Exception {
        Assert.assertEquals(
                ServerSnapshot.class,
                ServerSnapshotProvider.class
                        .getMethod("snapshot", ConnectionStatsView.class)
                        .getReturnType()
        );
    }

    @Test
    public void aggregateContainsExactlyEightSnapshotCategories() {
        Set<Class<?>> expected = Set.of(
                RuntimeSnapshot.class,
                ExecutorSnapshot.class,
                ConnectionSnapshot.class,
                InboundSnapshot.class,
                OutboundSnapshot.class,
                CommitStreamSnapshot.class,
                MemorySnapshot.class,
                HealthSnapshot.class
        );
        Set<Class<?>> actual = java.util.Arrays.stream(ServerSnapshot.class.getRecordComponents())
                .map(RecordComponent::getType)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void snapshotApiContainsNoRequestWriterRuntimeOrTransportTypes() {
        for (Class<?> type : Set.of(
                ServerIdentity.class, ServerSnapshot.class, RuntimeSnapshot.class,
                ExecutorSnapshot.class, ConnectionSnapshot.class, InboundSnapshot.class,
                OutboundSnapshot.class, CommitStreamSnapshot.class, MemorySnapshot.class,
                HealthSnapshot.class
        )) {
            for (RecordComponent component : type.getRecordComponents()) {
                String name = component.getGenericType().getTypeName();
                Assert.assertFalse(name, name.contains("ExecutionRequest"));
                Assert.assertFalse(name, name.contains("CommandContext"));
                Assert.assertFalse(name, name.contains("RedisReplyWriter"));
                Assert.assertFalse(name, name.contains("io.netty"));
                Assert.assertFalse(name, name.contains("runtime.embedded"));
            }
        }
    }
}
```

Create `ServerCommandModuleTest` in the renderer's package so it tests the pure port boundary and stable output without a transport fixture:

```java
package yier.bubu.redis.command.defaults.server;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.ServerIdentity;
import yier.bubu.redis.command.api.ServerSnapshot;
import yier.bubu.redis.execution.api.PreparedCommand;

public class ServerCommandModuleTest {
    private static final ServerIdentity IDENTITY =
            new ServerIdentity("yierdis", "9.8.7", "standalone", "master");

    @Test
    public void moduleConstructorExposesOnlyPureIdentityAndSnapshotPorts() throws Exception {
        Assert.assertNotNull(ServerCommandModule.class.getConstructor(
                ServerIdentity.class,
                yier.bubu.redis.command.api.ServerSnapshotProvider.class));
        Assert.assertTrue(CommandModule.class.isAssignableFrom(ServerCommandModule.class));
    }

    @Test
    public void redisServerInfoUsesIdentityAndRuntimeSnapshot() {
        byte[] rendered = new ServerInfoRenderer().redisInfo(
                "server", IDENTITY, ServerSnapshot.EMPTY);
        String text = new String(rendered, StandardCharsets.UTF_8);

        Assert.assertTrue(text.contains("# Server\r\n"));
        Assert.assertTrue(text.contains("redis_version:9.8.7\r\n"));
        Assert.assertTrue(text.contains("tcp_port:0\r\n"));
    }

    @Test
    public void statsRendererUsesStableOrderedKeyValuePairs() {
        List<Map.Entry<String, Object>> pairs =
                new ServerStatsRenderer().stats(ServerSnapshot.EMPTY);

        Assert.assertEquals("queued_tasks", pairs.get(0).getKey());
        Assert.assertEquals(0L, pairs.get(0).getValue());
        Assert.assertTrue(pairs.stream().anyMatch(entry ->
                entry.getKey().equals("commands_executed_total")
                        && entry.getValue().equals(0L)));
    }
}
```

Add this preparation-level case to the same class using its local `CapturingRegistration` and `TestCommandSession` helpers (both implement the complete Task 3 interfaces, with no defaults):

```java
@Test
public void infoPreparationCapturesExactlyOneSnapshot() {
    AtomicInteger snapshots = new AtomicInteger();
    CapturingRegistration registration = new CapturingRegistration();
    new ServerCommandModule(IDENTITY, ignored -> {
        snapshots.incrementAndGet();
        return ServerSnapshot.EMPTY;
    }).register(registration);

    PreparedCommand prepared = registration.prepare(
            "INFO", request("INFO", "server"), new TestCommandSession());

    Assert.assertEquals(1, snapshots.get());
    prepared.close();
}
```

`CapturingRegistration.prepare(...)` looks up the captured `CommandDefinition<?>`, calls `definition.parse(request)`, fails the test on a parse error, and invokes the preparer through one private generic helper with `new CommandPreparationContext(session)`. `request(...)` returns `ByteArrayExecutionRequest.fromUtf8(...)`. The session stores RESP version/client name and supplies an empty required `TransactionState`; this test does not execute through a detached writer.

Append these complete helpers to `ServerCommandModuleTest`:

```java
private static final class CapturingRegistration implements CommandModule.Registration {
    private final java.util.Map<String, CommandDefinition<?>> definitions = new java.util.TreeMap<>();

    @Override
    public void register(CommandDefinition<?> definition) {
        CommandDefinition<?> previous = definitions.put(
                definition.syntax().nameUpper(), definition);
        if (previous != null) throw new IllegalArgumentException("duplicate definition");
    }

    PreparedCommand prepare(String upper, ExecutionRequest request, CommandSession session) {
        CommandDefinition<?> definition = definitions.get(upper);
        Assert.assertNotNull("missing command definition: " + upper, definition);
        return prepareTyped(definition, request, session);
    }

    private static <T> PreparedCommand prepareTyped(
            CommandDefinition<T> definition,
            ExecutionRequest request,
            CommandSession session
    ) {
        CommandParseResult<T> parsed = definition.parse(request);
        Assert.assertTrue(parsed.ok());
        return definition.preparer().prepare(
                parsed.value(), new CommandPreparationContext(session));
    }

    @Override public int commandCount() { return definitions.size(); }
    @Override public boolean containsUpperName(String name) { return definitions.containsKey(name); }
    @Override public CommandDefinition<?> definitionByUpperName(String name) { return definitions.get(name); }
    @Override public String[] upperNamesSorted() { return definitions.keySet().toArray(String[]::new); }
}

private static ExecutionRequest request(String command, String... args) {
    return ByteArrayExecutionRequest.fromUtf8(command, java.util.List.of(args));
}

private static final class TestCommandSession implements CommandSession {
    private final TransactionState transaction = new EmptyTransactionState();
    private int dbIndex;
    private String clientName;
    private boolean authenticated;
    private int respVersion = 2;

    @Override public int dbIndex() { return dbIndex; }
    @Override public void setDbIndex(int value) { dbIndex = value; }
    @Override public long clientId() { return 1L; }
    @Override public String clientName() { return clientName; }
    @Override public void setClientName(String value) { clientName = value; }
    @Override public boolean authenticated() { return authenticated; }
    @Override public void setAuthenticated(boolean value) { authenticated = value; }
    @Override public TransactionState transaction() { return transaction; }
    @Override public ConnectionStatsView connectionStats() { return EmptyConnectionStats.INSTANCE; }
    @Override public int respVersion() { return respVersion; }
    @Override public void setRespVersion(int value) { respVersion = value; }
}

private enum EmptyConnectionStats implements ConnectionStatsView {
    INSTANCE;
    @Override public int pending() { return 0; }
    @Override public long pendingBytes() { return 0; }
    @Override public boolean inputDisabledByExecutor() { return false; }
    @Override public boolean inputPausedByReply() { return false; }
    @Override public boolean closing() { return false; }
    @Override public long commandsEnqueued() { return 0; }
    @Override public long commandsExecuted() { return 0; }
    @Override public long commandsRejected() { return 0; }
    @Override public long commandsSkippedClosing() { return 0; }
    @Override public long closeAfterReply() { return 0; }
    @Override public long backpressureEnter() { return 0; }
    @Override public long backpressureExit() { return 0; }
}

private static final class EmptyTransactionState implements TransactionState {
    private boolean active;
    private boolean aborted;
    @Override public boolean active() { return active; }
    @Override public boolean aborted() { return aborted; }
    @Override public void begin() { active = true; aborted = false; }
    @Override public void markAborted() { aborted = true; }
    @Override public String tryEnqueue(ExecutionRequest request) { return null; }
    @Override public int size() { return 0; }
    @Override public void forEachQueued(java.util.function.Consumer<? super ExecutionRequest> visitor) { }
    @Override public java.util.List<ExecutionRequest> drain() { return java.util.List.of(); }
    @Override public void discard() { active = false; aborted = false; }
    @Override public void close() { discard(); }
}
```

Add imports for the referenced `CommandDefinition`, `CommandParseResult`, `ByteArrayExecutionRequest`, `CommandPreparationContext`, `CommandSession`, `ConnectionStatsView`, `ExecutionRequest`, and `TransactionState` types.

Extend `ArchitectureBoundaryTest` with this source rule:

```java
@Test
public void serverMainContainsNoCommandDefinitionsOrReplyRenderers() throws Exception {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("cannot locate repository root", repoRoot);
    List<String> offenders = new ArrayList<>();
    int scanned = scanForForbiddenText(
            repoRoot,
            repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java"),
            offenders,
            "implements CommandModule",
            "CommandDefinition.",
            "import yier.bubu.redis.execution.api.RedisReplyWriter;",
            "ReplyShapes."
    );
    Assert.assertTrue("server-main source guard scanned no Java files", scanned > 0);
    Assert.assertTrue("server-main contains command/rendering code: " + offenders, offenders.isEmpty());
}
```

- [ ] **Step 2: Run the Task 4 RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command/yierdis-command-api,yierdis-command/yierdis-command-builtin,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests \
  -am \
  -Dtest=ServerSnapshotContractTest,ServerCommandModuleTest,YierdisServerBootstrapCommandWiringTest,RespHandshakeIntegrationTest,RedisCliCompatibilityTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because the snapshot ports and builtin server module do not exist and `server-main` still contains `ServerCommandModule`, writer-based INFO/STATS rendering, and reply-size measurement.

- [ ] **Step 3: Add identity, provider, aggregate, runtime, memory, and health contracts**

Create these exact public records and provider in `yier.bubu.redis.command.api`:

```java
public record ServerIdentity(String productName, String version, String mode, String role) {
    public ServerIdentity {
        productName = requireText(productName, "productName");
        version = requireText(version, "version");
        mode = requireText(mode, "mode");
        role = requireText(role, "role");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value.trim();
    }
}
```

```java
@FunctionalInterface
public interface ServerSnapshotProvider {
    ServerSnapshot snapshot(yier.bubu.redis.execution.api.ConnectionStatsView connection);
}
```

```java
public record RuntimeSnapshot(
        int tcpPort,
        int ioThreads,
        long startedMillis,
        long uptimeMillis,
        int databaseCount,
        java.util.Map<Integer, Long> keyCountByDatabase,
        java.util.Map<Integer, Long> expireCountByDatabase
) {
    public static final RuntimeSnapshot EMPTY = new RuntimeSnapshot(0, 0, 0, 0, 0, java.util.Map.of(), java.util.Map.of());

    public RuntimeSnapshot {
        if (tcpPort < 0 || tcpPort > 65535 || ioThreads < 0 || startedMillis < 0
                || uptimeMillis < 0 || databaseCount < 0) {
            throw new IllegalArgumentException("invalid runtime snapshot value");
        }
        keyCountByDatabase = java.util.Map.copyOf(keyCountByDatabase);
        expireCountByDatabase = java.util.Map.copyOf(expireCountByDatabase);
        keyCountByDatabase.forEach((db, count) -> {
            if (db < 0 || db >= databaseCount || count < 0L) {
                throw new IllegalArgumentException("invalid key-count entry");
            }
        });
        expireCountByDatabase.forEach((db, count) -> {
            if (db < 0 || db >= databaseCount || count < 0L) {
                throw new IllegalArgumentException("invalid expire-count entry");
            }
        });
    }
}
```

```java
public record MemorySnapshot(
        boolean globalScope,
        long configuredMaxmemoryBytes,
        String maxmemoryScope,
        String maxmemoryPolicy,
        java.util.Optional<yier.bubu.redis.storage.api.YierdisMemoryStats> aggregate
) {
    public static final MemorySnapshot EMPTY = new MemorySnapshot(false, 0, "global", "noeviction", java.util.Optional.empty());

    public MemorySnapshot {
        if (configuredMaxmemoryBytes < 0) throw new IllegalArgumentException("configuredMaxmemoryBytes must be >= 0");
        if (maxmemoryScope == null || maxmemoryScope.isBlank()) throw new IllegalArgumentException("maxmemoryScope must not be blank");
        if (maxmemoryPolicy == null || maxmemoryPolicy.isBlank()) throw new IllegalArgumentException("maxmemoryPolicy must not be blank");
        aggregate = java.util.Objects.requireNonNull(aggregate, "aggregate");
    }
}
```

```java
public record HealthSnapshot(
        String lifecycleState,
        boolean ready,
        boolean writable,
        int databases,
        int degradedDatabases,
        String commitStreamState,
        String firstFailureType,
        String firstFailureMessage
) {
    public static final HealthSnapshot EMPTY = new HealthSnapshot("STARTING", false, false, 0, 0, "DISABLED", "", "");

    public HealthSnapshot {
        lifecycleState = java.util.Objects.requireNonNull(lifecycleState, "lifecycleState");
        commitStreamState = java.util.Objects.requireNonNull(commitStreamState, "commitStreamState");
        firstFailureType = firstFailureType == null ? "" : firstFailureType;
        firstFailureMessage = firstFailureMessage == null ? "" : firstFailureMessage;
        if (databases < 0 || degradedDatabases < 0 || degradedDatabases > databases) {
            throw new IllegalArgumentException("invalid database health counts");
        }
    }
}
```

```java
public record ServerSnapshot(
        RuntimeSnapshot runtime,
        ExecutorSnapshot executor,
        ConnectionSnapshot connection,
        InboundSnapshot inbound,
        OutboundSnapshot outbound,
        CommitStreamSnapshot commitStream,
        MemorySnapshot memory,
        HealthSnapshot health
) {
    public static final ServerSnapshot EMPTY = new ServerSnapshot(
            RuntimeSnapshot.EMPTY, ExecutorSnapshot.EMPTY, ConnectionSnapshot.EMPTY,
            InboundSnapshot.EMPTY, OutboundSnapshot.EMPTY, CommitStreamSnapshot.EMPTY,
            MemorySnapshot.EMPTY, HealthSnapshot.EMPTY);

    public ServerSnapshot {
        java.util.Objects.requireNonNull(runtime, "runtime");
        java.util.Objects.requireNonNull(executor, "executor");
        java.util.Objects.requireNonNull(connection, "connection");
        java.util.Objects.requireNonNull(inbound, "inbound");
        java.util.Objects.requireNonNull(outbound, "outbound");
        java.util.Objects.requireNonNull(commitStream, "commitStream");
        java.util.Objects.requireNonNull(memory, "memory");
        java.util.Objects.requireNonNull(health, "health");
    }
}
```

- [ ] **Step 4: Add executor, connection, inbound, outbound, and commit-stream snapshots**

Create the remaining five records with exact fields copied from the current concrete snapshots, but with enums converted to stable strings and reply configuration included in outbound data:

```java
public record ExecutorSnapshot(
        long commandsExecuted, long commandsSkippedClosing,
        int queuedTasks, long queuedBytes, String schedulingPolicy,
        int queueCapacity, long queueMaxBytes,
        int backpressureHighWatermark, int backpressureLowWatermark,
        long backpressureBytesHighWatermark, long backpressureBytesLowWatermark,
        int maxDrainCommands, long drainTimeLimitMillis,
        int channelsAutoReadDisabled,
        long submitAccepted, long submitRejectedNotRunning,
        long submitRejectedClosing, long submitRejectedQueueFull,
        long submitRejectedBytesBudget, long submitRejectedRequestTooLarge,
        long submitRejectedOfferFailed, long closeAfterReply,
        long backpressureEnter, long backpressureExit,
        long drainLimitedByMaxCommands, long drainLimitedByTimeBudget,
        long deferredFairReplyHeads, long deferredGlobalReplyHeads
) {
    public static final ExecutorSnapshot EMPTY = new ExecutorSnapshot(
            0, 0, 0, 0, "FAIR", 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    public ExecutorSnapshot {
        schedulingPolicy = java.util.Objects.requireNonNull(schedulingPolicy, "schedulingPolicy");
        long[] values = {
                commandsExecuted, commandsSkippedClosing, queuedTasks, queuedBytes,
                queueCapacity, queueMaxBytes, backpressureHighWatermark,
                backpressureLowWatermark, backpressureBytesHighWatermark,
                backpressureBytesLowWatermark, maxDrainCommands, drainTimeLimitMillis,
                channelsAutoReadDisabled, submitAccepted, submitRejectedNotRunning,
                submitRejectedClosing, submitRejectedQueueFull, submitRejectedBytesBudget,
                submitRejectedRequestTooLarge, submitRejectedOfferFailed, closeAfterReply,
                backpressureEnter, backpressureExit, drainLimitedByMaxCommands,
                drainLimitedByTimeBudget, deferredFairReplyHeads, deferredGlobalReplyHeads
        };
        for (long value : values) {
            if (value < 0L) throw new IllegalArgumentException("executor snapshot values must be non-negative");
        }
    }
}
```

```java
public record ConnectionSnapshot(
        int activeConnections, long acceptedConnections,
        long rejectedClosingConnections, long rejectedMaxClientsConnections,
        int maxClients, boolean currentConnectionPresent,
        int pending, long pendingBytes,
        boolean inputDisabledByExecutor, boolean inputPausedByReply, boolean closing,
        long commandsEnqueued, long commandsExecuted, long commandsRejected,
        long commandsSkippedClosing, long closeAfterReply,
        long backpressureEnter, long backpressureExit
) {
    public static final ConnectionSnapshot EMPTY = new ConnectionSnapshot(
            0, 0, 0, 0, 0, false, 0, 0, false, false, false,
            0, 0, 0, 0, 0, 0, 0);

    public long rejectedConnections() {
        return rejectedClosingConnections > Long.MAX_VALUE - rejectedMaxClientsConnections
                ? Long.MAX_VALUE : rejectedClosingConnections + rejectedMaxClientsConnections;
    }

    public ConnectionSnapshot {
        long[] values = {activeConnections, acceptedConnections,
                rejectedClosingConnections, rejectedMaxClientsConnections,
                maxClients, pending, pendingBytes, commandsEnqueued, commandsExecuted,
                commandsRejected, commandsSkippedClosing, closeAfterReply,
                backpressureEnter, backpressureExit};
        for (long value : values) {
            if (value < 0L) throw new IllegalArgumentException("connection snapshot values must be non-negative");
        }
    }
}
```

```java
public record InboundSnapshot(
        long capacityBytes, long reservedBytes, int waitingConnections,
        boolean backpressured, long rejectedConnections, long peakReservedBytes,
        long readCreditBytes, long retainedInputCapacityBytes,
        long consolidationBytes, boolean closed
) {
    public static final InboundSnapshot EMPTY = new InboundSnapshot(0, 0, 0, false, 0, 0, 0, 0, 0, false);

    public InboundSnapshot {
        long[] values = {capacityBytes, reservedBytes, waitingConnections,
                rejectedConnections, peakReservedBytes, readCreditBytes,
                retainedInputCapacityBytes, consolidationBytes};
        for (long value : values) {
            if (value < 0L) throw new IllegalArgumentException("inbound snapshot values must be non-negative");
        }
    }
}
```

```java
public record OutboundSnapshot(
        long globalCapacityBytes, long perConnectionCapacityBytes,
        long maxReplyTotalBytes, int chunkPayloadBytes,
        long controlReservationBytes, long drainTimeoutMillis,
        long reservedBytes, long allocatedBytes,
        long peakReservedBytes, long peakAllocatedBytes,
        long capacityRejects, int waitingConnections,
        int activeConnections, long activeSlots, boolean closed,
        long activeChunks, long activeSources, long oversizedReplies,
        long cancelledSlots, long failedSlots, long writeFailures,
        long resultUnknownCloses, long shutdownTimeouts
) {
    public static final OutboundSnapshot EMPTY = new OutboundSnapshot(
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
            0, 0, 0, 0, 0, 0, 0, 0);

    public OutboundSnapshot {
        long[] values = {globalCapacityBytes, perConnectionCapacityBytes,
                maxReplyTotalBytes, chunkPayloadBytes, controlReservationBytes,
                drainTimeoutMillis, reservedBytes, allocatedBytes, peakReservedBytes,
                peakAllocatedBytes, capacityRejects, waitingConnections,
                activeConnections, activeSlots, activeChunks, activeSources,
                oversizedReplies, cancelledSlots, failedSlots, writeFailures,
                resultUnknownCloses, shutdownTimeouts};
        for (long value : values) {
            if (value < 0L) throw new IllegalArgumentException("outbound snapshot values must be non-negative");
        }
    }
}
```

```java
public record CommitStreamSnapshot(
        String state, long reservedEvents, long reservedBytes,
        long rejectedWrites, long lastAssignedSequence,
        long lastAcknowledgedSequence, String firstFailureType,
        String firstFailureMessage, boolean callbackActive,
        boolean shutdownTimedOut
) {
    public static final CommitStreamSnapshot EMPTY = new CommitStreamSnapshot(
            "DISABLED", 0, 0, 0, 0, 0, "", "", false, false);

    public CommitStreamSnapshot {
        state = java.util.Objects.requireNonNull(state, "state");
        firstFailureType = firstFailureType == null ? "" : firstFailureType;
        firstFailureMessage = firstFailureMessage == null ? "" : firstFailureMessage;
        long[] values = {reservedEvents, reservedBytes, rejectedWrites,
                lastAssignedSequence, lastAcknowledgedSequence};
        for (long value : values) {
            if (value < 0L) throw new IllegalArgumentException("commit-stream snapshot values must be non-negative");
        }
    }
}
```

Add one table-driven negative-constructor case per record to `ServerSnapshotContractTest`; do not rely on the concrete adapters to sanitize invalid snapshots.

- [ ] **Step 5: Move HELLO, INFO, STATS, and both renderers into command-builtin**

`ServerCommandModule` is public and constructed only with pure ports:

```java
public ServerCommandModule(ServerIdentity identity, ServerSnapshotProvider snapshots) {
    this.identity = java.util.Objects.requireNonNull(identity, "identity");
    this.snapshots = java.util.Objects.requireNonNull(snapshots, "snapshots");
}
```

Register the Task 2 syntax unchanged. Parsing produces private immutable values `HelloRequest(int respVersion, String clientName)` and `InfoRequest(String section)`. Preparation follows these exact rules:

- `HELLO`: validate version `2` or `3`, reject `AUTH` with the current Redis-compatible message, build a five-pair shape from `ServerIdentity`, and defer `setRespVersion`/`setClientName` until `execute(...)`.
- `INFO`: call `snapshots.snapshot(context.session().connectionStats())` once. `health` and `yierdis` produce map shapes; all Redis sections produce one bulk string from `ServerInfoRenderer.redisInfo(...)`.
- `STATS`: capture one snapshot and the ordered pairs from `ServerStatsRenderer.stats(...)`, then return one map shape.

Both renderers use `List<Map.Entry<String, Object>>`, where values are only `String` or `Long`. The module converts each key/string to `ReplyShapes.bulkString(utf8Length, 0)` and each number to `ReplyShapes.integer(value)`, and execution writes the captured list without asking the provider again. Reject any other value class at preparation with `IllegalStateException`.

The exact package-private methods are `byte[] ServerInfoRenderer.redisInfo(String section, ServerIdentity identity, ServerSnapshot snapshot)`, `List<Map.Entry<String, Object>> ServerInfoRenderer.yierdis(ServerIdentity identity, ServerSnapshot snapshot)`, and `List<Map.Entry<String, Object>> ServerStatsRenderer.stats(ServerSnapshot snapshot)`.

Each list is returned with `List.copyOf`, and every entry is created with `Map.entry`; the renderers never expose a mutable builder.

Move the current key names and output ordering from `NettyServerInfoProvider` verbatim. The exact mapping is:

- `ServerInfoRenderer.redisInfo`: `server` reads identity/runtime; `health` reads health/connection; `clients` reads connection; `memory` reads memory; `stats` reads executor/inbound/outbound/commit-stream/connection; `keyspace` reads the two runtime DB maps.
- `ServerInfoRenderer.yierdis`: identity plus every runtime value, executor configuration/counters, inbound field, outbound field, commit-stream field, and health/connection summary currently emitted by `writeYierdisStructuredInfo`.
- `ServerStatsRenderer.stats`: every current `writeStats` key; include the current connection-specific keys only when `snapshot.connection().currentConnectionPresent()` is true.

Preserve the exact INFO section names, CRLF layout, RESP2 array versus RESP3 map behavior, key order, null/failure sanitization, and `instantaneous_ops_per_sec = commandsExecuted / max(1, uptimeSeconds)`. Delete `ReplyPlanMeasurer`; the captured values build `ReplyShape` before execution and RESP owns all encoded sizing.

Change `DefaultCommandModules.create(...)` and `CommandSupport` from `ServerInfoProvider` to nullable `ServerSnapshotProvider`. Register `ServerCommandModule` inside `DefaultCommandModules` when both identity and provider are non-null. `ServerCommandComposition.createProcessor(...)` keeps the Task 3 registry/transaction setup and changes only its module composition:

```java
CommandRegistry registry = new CommandRegistry();
YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
CommandRegistries.registerTransactionSupport(registry, processor::prepare);
CommandRegistries.registerInto(
        registry,
        DefaultCommandModules.create(dbRouter, identity, snapshots, slowGovernor)
);
return processor;
```

For `MEMORY STATS`, use `snapshots.snapshot(session.connectionStats()).memory().aggregate()` only when `globalScope()` is true and the optional is present; otherwise use the selected DB's `memory().memoryStats()` as today.

- [ ] **Step 6: Replace the writer/provider implementation with a snapshot-only adapter**

Rename `NettyServerInfoProvider` to `NettyServerSnapshotProvider` and implement only:

```java
@Override
public ServerSnapshot snapshot(ConnectionStatsView connectionStats) {
    long nowMillis = System.currentTimeMillis();
    return new ServerSnapshot(
            runtimeSnapshot(nowMillis),
            executorSnapshot(),
            connectionSnapshot(connectionStats),
            inboundSnapshot(),
            outboundSnapshot(),
            commitStreamSnapshot(),
            memorySnapshot(),
            healthSnapshot()
    );
}
```

Keep exactly `bindExecutor(...)`, `bindObservability(...)`, `bindInboundMemoryBudget(...)`, `bindOutboundMemoryBudget(...)`, `bindChildChannelRegistry(...)`, `bindReplyEgressStats(...)`, and `bindLifecycleState(...)`. Use these exact adapter methods and mappings:

| Adapter method | Concrete source -> semantic record |
|---|---|
| `RuntimeSnapshot runtimeSnapshot(long nowMillis, YierdisInstanceObservability observability)` | config `port`, `ioThreads`, provider `startedMillis`, saturated `now-started`, config `databases`; copy each `dbSummaries()` entry's `dbIndex/keyCount/expireCount` into the two immutable maps |
| `ExecutorSnapshot executorSnapshot(CommandExecutor.StatsSnapshot stats)` | all 20 `StatsSnapshot` accessors; assert `stats.schedulingPolicy().name()` equals config `executorSchedulingPolicy().name()` and store it once; also copy config `executorQueueCapacity`, `executorQueueMaxBytes`, both command watermarks, both byte watermarks, `executorMaxDrainCommands`, and `executorDrainTimeLimitMillis` |
| `ConnectionSnapshot connectionSnapshot(ConnectionStatsView connection, ChildChannelRegistry.StatsSnapshot children)` | child active/accepted/two rejected counters plus config `maxClients`; set `currentConnectionPresent = connection != null`; copy all 12 `ConnectionStatsView` values when present and use per-connection zeros otherwise |
| `InboundSnapshot inboundSnapshot(InboundMemoryBudgetStats stats)` | all ten accessors in declaration order; when unbound use config `protocolGlobalInFlightBytes` and zero/false counters |
| `OutboundSnapshot outboundSnapshot(OutboundMemoryBudgetStats budget, ReplyEgressStats.Snapshot egress)` | six configured reply limits; assert `budget.capacityBytes()` equals configured `replyGlobalCapacityBytes` and store it once as `globalCapacityBytes`; copy the other nine budget record components and all eight egress accessors |
| `CommitStreamSnapshot commitStreamSnapshot(CommitStreamStats stats)` | `state().name()`, five sequence/capacity counters, nullable failure strings normalized to `""`, callback-active and shutdown-timeout flags |
| `MemorySnapshot memorySnapshot(YierdisInstanceObservability observability)` | global-scope flag, configured maxmemory, normalized scope/policy strings, and `Optional.of(observability.memoryStats())`; use `Optional.empty()` only before observability is bound |
| `HealthSnapshot healthSnapshot(RuntimeHealthSnapshot db, CommitStreamSnapshot commit, InboundSnapshot inbound, OutboundSnapshot outbound)` | normalized lifecycle string; `ready = lifecycle == RUNNING && !inbound.closed() && !outbound.closed() && db.healthy() && (commit.state is DISABLED or RUNNING)`; writable equals ready; DB/degraded counts, commit state, and normalized first failure strings |

At the start of `snapshot(...)`, copy every volatile binding and `lifecycleState` supplier into locals. Capture each concrete stats object once, then call the eight adapter methods with those locals. Convert concrete enum values to `name()`, copy DB maps with `Map.copyOf`, and contain no request parsing, INFO section selection, string building, RESP key constant, reply writer, or wire-size logic.

In bootstrap create:

```java
ServerIdentity identity = new ServerIdentity(
        "yierdis", YierdisBuildInfo.version(), "standalone", "master");
snapshotProvider = new NettyServerSnapshotProvider(runtimeConfig);
YierdisFastCommandProcessor commandProcessor = ServerCommandComposition.createProcessor(
        dbRouter(instance), identity, snapshotProvider, slowGovernor);
```

Update `YierdisServerBootstrapCommandWiringTest` to inspect `snapshotProviderForTests()` and add these concrete assertions (the test's CLI fixture uses port `0`, executor queue capacity `1024`, inbound capacity `128 MiB`, reply global capacity `256 MiB`, max clients `1024`, and maxmemory `0`):

```java
ConnectionStatsView connection = new TestConnectionStats(
        3, 96L, true, true, false,
        7L, 5L, 2L, 1L, 0L, 4L, 3L);
ServerSnapshot snapshot = bootstrap.snapshotProviderForTests().snapshot(connection);

Assert.assertEquals(bootstrap.port(), snapshot.runtime().tcpPort());
Assert.assertEquals(1024, snapshot.executor().queueCapacity());
Assert.assertEquals(3, snapshot.connection().pending());
Assert.assertEquals(96L, snapshot.connection().pendingBytes());
Assert.assertTrue(snapshot.connection().currentConnectionPresent());
Assert.assertTrue(snapshot.connection().inputPausedByReply());
Assert.assertEquals(1024, snapshot.connection().maxClients());
Assert.assertEquals(128L * 1024L * 1024L, snapshot.inbound().capacityBytes());
Assert.assertEquals(256L * 1024L * 1024L, snapshot.outbound().globalCapacityBytes());
Assert.assertEquals(0L, snapshot.memory().configuredMaxmemoryBytes());
Assert.assertEquals("RUNNING", snapshot.health().lifecycleState());
Assert.assertTrue(snapshot.health().ready());
```

Add the test-local `record TestConnectionStats(int pending, long pendingBytes, boolean inputDisabledByExecutor, boolean inputPausedByReply, boolean closing, long commandsEnqueued, long commandsExecuted, long commandsRejected, long commandsSkippedClosing, long closeAfterReply, long backpressureEnter, long backpressureExit) implements ConnectionStatsView {}`. Do not call INFO/STATS directly on the adapter.

- [ ] **Step 7: Run Task 4 GREEN and behavior regression tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-command,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests \
  -am \
  -Dtest=ServerSnapshotContractTest,ServerCommandModuleTest,YierdisServerBootstrapCommandWiringTest,RespHandshakeIntegrationTest,RedisCliCompatibilityTest,CommandMetadataRegressionTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; HELLO/INFO/STATS output remains compatible, each command captures one immutable snapshot during preparation, and `server-main` has no command definition or renderer.

- [ ] **Step 8: Enforce zero residue and commit Task 4**

Run:

```bash
rg -n 'ServerInfoProvider|NettyServerInfoProvider|class ServerCommandModule|ReplyPlanMeasurer|implements CommandModule|import .*RedisReplyWriter;|ReplyShapes\.' \
  --glob '*.java' \
  yierdis-server/yierdis-server-main/src/main/java
```

Expected: no output.

Run:

```bash
rg -n 'ExecutionRequest|CommandContext|RedisReplyWriter|io\.netty|runtime\.embedded' \
  yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/*Snapshot*.java \
  yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerIdentity.java
```

Expected: no output except the intentional `ConnectionStatsView` import in `ServerSnapshotProvider.java`, which is not matched by this expression.

Commit:

```bash
git add yierdis-command/yierdis-command-api yierdis-command/yierdis-command-builtin \
  yierdis-server/yierdis-server-main \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: move server commands behind snapshot ports"
```

## Task 5: Replace The Flat Runtime Record With Five Validated Configuration Groups

**Files:**

- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/YierdisServerConfig.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/NetworkConfig.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/ExecutorConfig.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/ReplyConfig.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/StorageConfig.java`
- Create: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/config/MaintenanceConfig.java`
- Delete: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerConfig.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerSnapshotProvider.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/config/YierdisServerConfigTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerConfigArgsTest.java`
- Modify test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- Modify test: every `yierdis-server/yierdis-server-main/src/test/java` fixture constructing `YierdisServerRuntimeConfig` directly.
- Modify test: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

**Interfaces:**

- Consumes: storage-plan `DbDefragConfig`, `DbEngineConfig`, `DbEngineFactory.create(DbEngineConfig)`, and db-memory `YierdisDbBackendConfig`; Task 4's `NettyServerSnapshotProvider`.
- Produces: `YierdisServerConfig(NetworkConfig, ExecutorConfig, ReplyConfig, StorageConfig, MaintenanceConfig)`; `YierdisServerArgs.toServerConfig()`; group-specific bootstrap, executor, channel, snapshot, runtime, and backend construction calls.
- Invariant: native defrag has exactly one value path: CLI fields -> `StorageConfig.defrag()` -> `YierdisInstanceConfig.defrag()` -> `DbEngineConfig.defrag()`. `YierdisServerBootstrap` and `YierdisDbEngineFactory` never unpack or reconstruct defrag values.
- Invariant: native slot capacity is not a per-DB policy and follows `StorageConfig.nativeSlotCapacity()` -> `new YierdisDbBackendConfig(int)` exactly once at backend-factory composition.
- Ownership: storage/memory Task 1 already deleted the flat `YierdisInstanceConfig` defrag API and changed `YierdisInstance` to pass `config.defrag()` into `DbEngineConfig`. This task only replaces the temporary composition mapping with its grouped source; it must not recreate, change, or overload either runtime API.

- [ ] **Step 1: Write RED grouped-shape, independent-validation, conversion, and call-site tests**

Create `YierdisServerConfigTest`:

```java
package yier.bubu.redis.app.server.config;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

public class YierdisServerConfigTest {
    @Test
    public void rootContainsExactlyFiveNamedGroups() {
        Assert.assertEquals(
                List.of(NetworkConfig.class, ExecutorConfig.class, ReplyConfig.class,
                        StorageConfig.class, MaintenanceConfig.class),
                Arrays.stream(YierdisServerConfig.class.getRecordComponents())
                        .map(RecordComponent::getType)
                        .toList()
        );
    }

    @Test
    public void eachGroupRejectsItsOwnInvalidValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new NetworkConfig(
                " ", 6379, 1, 1, 0, 0, 1024, 16, 64, 2048,
                4096, 0, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ExecutorConfig(
                16, 1024, ExecutorConfig.SchedulingPolicy.FAIR,
                4, 4, 0, 0, 8, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ReplyConfig(
                4096, 8192, 4096, 128, 1539, 1000));
        Assert.assertThrows(IllegalArgumentException.class, () -> new StorageConfig(
                0, 0, StorageConfig.MaxmemoryScope.GLOBAL,
                MaxmemoryPolicy.NOEVICTION, 5, 0,
                new DbDefragConfig(false, 0, 0, 0)));
        Assert.assertThrows(IllegalArgumentException.class, () -> new MaintenanceConfig(
                -1, 5, 5, 0, 1));
    }

    @Test
    public void storageGroupPreservesOneDefragValueAndBackendCapacity() {
        DbDefragConfig defrag = new DbDefragConfig(true, 4096, 7, 3);
        StorageConfig storage = new StorageConfig(
                3, 10, StorageConfig.MaxmemoryScope.PER_DB,
                MaxmemoryPolicy.ALLKEYS_LRU, 5, 2048, defrag);

        Assert.assertSame(defrag, storage.defrag());
        Assert.assertEquals(2048, storage.nativeSlotCapacity());
    }
}
```

Update `YierdisServerArgsTest` so its current all-options fixture asserts values through `config.network()`, `executor()`, `reply()`, `storage()`, and `maintenance()`, and change both round-trip assertions to:

```java
Assert.assertEquals(copied.toServerConfig(), reparsed.toServerConfig());
```

Add reflection checks to `YierdisServerBootstrapCommandWiringTest`:

```java
@Test
public void downstreamConstructorsConsumeOnlyTheirConfigurationGroups() throws Exception {
    Assert.assertNotNull(CommandExecutorConfigs.class.getDeclaredMethod("from", ExecutorConfig.class));
    boolean found = java.util.Arrays.stream(YierdisServerChannelInitializer.class.getDeclaredConstructors())
            .map(java.lang.reflect.Constructor::getParameterTypes)
            .anyMatch(types -> types.length == 8
                    && types[0] == NetworkConfig.class
                    && types[1] == ReplyConfig.class);
    Assert.assertTrue("missing group-specific channel initializer", found);
}
```

- [ ] **Step 2: Run the Task 5 RED tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main,yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-architecture-tests \
  -am \
  -Dtest=YierdisServerConfigTest,YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,DbEngineFactoryInjectionTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because the five records and `toServerConfig()` do not exist and all downstream components still accept `YierdisServerRuntimeConfig`.

- [ ] **Step 3: Add the root, network, and executor records**

Create `YierdisServerConfig`:

```java
package yier.bubu.redis.app.server.config;

import java.util.Objects;

public record YierdisServerConfig(
        NetworkConfig network,
        ExecutorConfig executor,
        ReplyConfig reply,
        StorageConfig storage,
        MaintenanceConfig maintenance
) {
    public YierdisServerConfig {
        Objects.requireNonNull(network, "network");
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(reply, "reply");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(maintenance, "maintenance");
    }
}
```

Create `NetworkConfig` with all transport, decoder, client, and transaction-session limits:

```java
package yier.bubu.redis.app.server.config;

import yier.bubu.redis.protocol.resp.RespProtocolLimits;

public record NetworkConfig(
        String bind, int port, int maxClients, int ioThreads,
        int transactionQueueMaxCommands, long transactionQueueMaxBytes,
        int protocolMaxBulkBytes, int protocolMaxArgs,
        int protocolMaxLineBytes, int protocolMaxCommandBytes,
        long protocolGlobalInFlightBytes, long clientIdleTimeoutMillis,
        long clientOutputBufferLimitBytes,
        long clientOutputBufferOverLimitMillis
) {
    public NetworkConfig {
        if (bind == null || bind.isBlank()) throw new IllegalArgumentException("bind must not be blank");
        bind = bind.trim();
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port must be in range 0..65535");
        if (maxClients <= 0 || ioThreads <= 0) throw new IllegalArgumentException("client and I/O counts must be positive");
        if (transactionQueueMaxCommands < 0 || transactionQueueMaxBytes < 0) {
            throw new IllegalArgumentException("transaction limits must be non-negative");
        }
        if (protocolMaxBulkBytes <= 0 || protocolMaxBulkBytes > RespProtocolLimits.MAX_BULK_BYTES
                || protocolMaxArgs <= 0 || protocolMaxArgs > RespProtocolLimits.MAX_ARGS
                || protocolMaxLineBytes <= 0
                || protocolMaxCommandBytes <= 0
                || protocolMaxCommandBytes > RespProtocolLimits.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException("protocol limit is outside the decoder-safe range");
        }
        if (protocolGlobalInFlightBytes < 0 || clientIdleTimeoutMillis < 0
                || clientOutputBufferLimitBytes < 0 || clientOutputBufferOverLimitMillis < 0) {
            throw new IllegalArgumentException("network byte/time limits must be non-negative");
        }
        if (clientOutputBufferLimitBytes > 0 && clientOutputBufferOverLimitMillis == 0) {
            throw new IllegalArgumentException("enabled output limit requires a positive grace period");
        }
    }
}
```

Create `ExecutorConfig`:

```java
package yier.bubu.redis.app.server.config;

import java.util.Locale;
import java.util.Objects;

public record ExecutorConfig(
        int queueCapacity, long queueMaxBytes, SchedulingPolicy schedulingPolicy,
        int backpressureHighWatermark, int backpressureLowWatermark,
        long backpressureBytesHighWatermark, long backpressureBytesLowWatermark,
        int maxDrainCommands, long drainTimeLimitMillis
) {
    public ExecutorConfig {
        Objects.requireNonNull(schedulingPolicy, "schedulingPolicy");
        if (queueCapacity <= 0 || queueMaxBytes < 0) throw new IllegalArgumentException("invalid executor queue limit");
        if (backpressureHighWatermark <= 0 || backpressureLowWatermark < 0
                || backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("invalid command backpressure watermarks");
        }
        if (backpressureBytesHighWatermark < 0 || backpressureBytesLowWatermark < 0
                || (backpressureBytesHighWatermark == 0 && backpressureBytesLowWatermark != 0)
                || (backpressureBytesHighWatermark > 0
                    && backpressureBytesLowWatermark >= backpressureBytesHighWatermark)) {
            throw new IllegalArgumentException("invalid byte backpressure watermarks");
        }
        if (maxDrainCommands <= 0 || drainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("executor drain limits must be positive");
        }
    }

    public enum SchedulingPolicy {
        GLOBAL, FAIR;

        public static SchedulingPolicy parse(String raw) {
            if (raw == null || raw.isBlank()) throw new IllegalArgumentException("executor scheduling policy must not be blank");
            return switch (raw.trim().toLowerCase(Locale.ROOT)) {
                case "global" -> GLOBAL;
                case "fair" -> FAIR;
                default -> throw new IllegalArgumentException("unsupported executor scheduling policy: " + raw);
            };
        }

        public String argvValue() { return name().toLowerCase(Locale.ROOT); }
    }
}
```

- [ ] **Step 4: Add reply, storage, and maintenance records**

Create `ReplyConfig` and move the three reply-overhead constants here from the deleted flat record:

```java
package yier.bubu.redis.app.server.config;

public record ReplyConfig(
        long globalCapacityBytes, long perConnectionCapacityBytes,
        long maxTotalBytes, int chunkPayloadBytes,
        long controlReservationBytes, long drainTimeoutMillis
) {
    public static final int FIXED_OVERHEAD_BYTES = 1_024;
    public static final int MAX_CONTROL_ERROR_FRAME_BYTES = 515;
    public static final long MIN_CONTROL_RESERVATION_BYTES =
            (long) FIXED_OVERHEAD_BYTES + MAX_CONTROL_ERROR_FRAME_BYTES;

    public ReplyConfig {
        if (globalCapacityBytes <= 0 || perConnectionCapacityBytes <= 0
                || maxTotalBytes <= 0 || chunkPayloadBytes <= 0
                || controlReservationBytes < MIN_CONTROL_RESERVATION_BYTES
                || drainTimeoutMillis <= 0) {
            throw new IllegalArgumentException("reply capacities and timeout must be positive and fit control overhead");
        }
        if (controlReservationBytes > maxTotalBytes
                || maxTotalBytes > perConnectionCapacityBytes
                || perConnectionCapacityBytes > globalCapacityBytes) {
            throw new IllegalArgumentException("reply capacity hierarchy is invalid");
        }
        long minimum = saturatedAdd(saturatedAdd(controlReservationBytes, chunkPayloadBytes), FIXED_OVERHEAD_BYTES);
        if (minimum > maxTotalBytes) throw new IllegalArgumentException("one reply chunk and control overhead must fit maxTotalBytes");
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
```

Create `StorageConfig`:

```java
package yier.bubu.redis.app.server.config;

import java.util.Locale;
import java.util.Objects;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

public record StorageConfig(
        int databases, long maxmemoryBytes, MaxmemoryScope maxmemoryScope,
        MaxmemoryPolicy maxmemoryPolicy, int maxmemorySamples,
        int nativeSlotCapacity, DbDefragConfig defrag
) {
    public StorageConfig {
        if (databases <= 0 || databases > 1024) throw new IllegalArgumentException("databases must be in range 1..1024");
        if (maxmemoryBytes < 0 || maxmemorySamples <= 0 || nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("invalid storage capacity or sample count");
        }
        Objects.requireNonNull(maxmemoryScope, "maxmemoryScope");
        Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        Objects.requireNonNull(defrag, "defrag");
    }

    public enum MaxmemoryScope {
        GLOBAL("global"), PER_DB("per-db");
        private final String argvValue;
        MaxmemoryScope(String argvValue) { this.argvValue = argvValue; }
        public String argvValue() { return argvValue; }
        public static MaxmemoryScope parse(String raw) {
            if (raw == null || raw.isBlank()) throw new IllegalArgumentException("maxmemory scope must not be blank");
            String value = raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
            if (value.equals("perdb")) value = "per-db";
            return switch (value) {
                case "global" -> GLOBAL;
                case "per-db" -> PER_DB;
                default -> throw new IllegalArgumentException("unsupported maxmemory scope: " + raw);
            };
        }
    }
}
```

Create `MaintenanceConfig`:

```java
package yier.bubu.redis.app.server.config;

public record MaintenanceConfig(
        long cleanupIntervalMillis,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis,
        long keysTimeBudgetMillis,
        int keysMaxResults
) {
    public MaintenanceConfig {
        if (cleanupIntervalMillis < 0 || evictionTimeLimitMillis <= 0
                || expireCleanupTimeLimitMillis <= 0 || keysTimeBudgetMillis < 0
                || keysMaxResults < 0) {
            throw new IllegalArgumentException("invalid maintenance interval, budget, or result limit");
        }
    }
}
```

- [ ] **Step 5: Convert normalized CLI values into named groups once**

Rename `YierdisServerArgs.toRuntimeConfig()` to `toServerConfig()` and construct the five values with named locals, keeping the current derived inbound-budget algorithm exactly:

```java
public YierdisServerConfig toServerConfig() {
    long inboundBytes = deriveProtocolGlobalInFlightBytes(
            executorQueueMaxBytes, protocolGlobalInFlightBytes);
    NetworkConfig network = new NetworkConfig(
            bind, port, maxClients, ioThreads,
            transactionQueueMaxCommands, transactionQueueMaxBytes,
            protocolMaxBulkBytes, protocolMaxArgs, protocolMaxLineBytes,
            protocolMaxCommandBytes, inboundBytes, clientIdleTimeoutMillis,
            clientOutputBufferLimitBytes, clientOutputBufferOverLimitMillis);
    ExecutorConfig executor = new ExecutorConfig(
            executorQueueCapacity, executorQueueMaxBytes,
            ExecutorConfig.SchedulingPolicy.parse(executorSchedulingPolicy),
            backpressureHighWatermark, backpressureLowWatermark,
            backpressureBytesHighWatermark, backpressureBytesLowWatermark,
            executorMaxDrainCommands, executorDrainTimeLimitMillis);
    ReplyConfig reply = new ReplyConfig(
            replyGlobalCapacityBytes, replyPerConnectionCapacityBytes,
            replyMaxTotalBytes, replyChunkPayloadBytes,
            replyControlReservationBytes, replyDrainTimeoutMillis);
    StorageConfig storage = new StorageConfig(
            databases, maxmemoryBytes, StorageConfig.MaxmemoryScope.parse(maxmemoryScope),
            MaxmemoryPolicy.parse(maxmemoryPolicy), maxmemorySamples, nativeSlotCapacity,
            new DbDefragConfig(nativeDefragEnabled, nativeDefragMaxMoveBytes,
                    nativeDefragMaxObjects, nativeDefragTimeLimitMillis));
    MaintenanceConfig maintenance = new MaintenanceConfig(
            noCleanup ? 0L : cleanupIntervalMillis,
            evictionTimeLimitMillis, expireCleanupTimeLimitMillis,
            keysTimeBudgetMillis, keysMaxResults);
    return new YierdisServerConfig(network, executor, reply, storage, maintenance);
}
```

`normalizeAndValidate()` normalizes the three CLI strings (`executorSchedulingPolicy`, `maxmemoryScope`, `maxmemoryPolicy`) and then calls `toServerConfig()`; delete its duplicated per-group numeric validation. `toArgv()` continues to serialize the raw normalized fields, including `0` for an unspecified derived inbound budget. `ServerConfig` becomes:

```java
package yier.bubu.redis.app.server;

import picocli.CommandLine;
import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerArgNames;
import yier.bubu.redis.app.server.args.YierdisServerArgs;
import yier.bubu.redis.app.server.config.YierdisServerConfig;

record ServerConfig(YierdisServerConfig server) {
    ServerConfig {
        java.util.Objects.requireNonNull(server, "server");
    }

    static ServerConfig fromArgs(String[] args) {
        YierdisServerArgs parsed = new YierdisServerArgs();
        CommandLine commandLine = new CommandLine(parsed);
        try {
            CommandLine.ParseResult result = commandLine.parseArgs(args);
            if (!parsed.help && !result.hasMatchedOption(YierdisServerArgNames.MAXMEMORY_BYTES)) {
                throw new CommandLine.ParameterException(
                        commandLine,
                        YierdisServerArgNames.MAXMEMORY_BYTES
                                + " must be specified explicitly (use 0 to acknowledge unlimited memory)"
                );
            }
        } catch (CommandLine.ParameterException failure) {
            System.err.println(failure.getMessage());
            commandLine.usage(System.err);
            throw YierdisCliException.usageError(failure.getMessage(), failure);
        }
        if (parsed.help) {
            commandLine.usage(System.out);
            return null;
        }
        try {
            parsed.normalizeAndValidate();
            return new ServerConfig(parsed.toServerConfig());
        } catch (IllegalArgumentException failure) {
            System.err.println(failure.getMessage());
            commandLine.usage(System.err);
            throw YierdisCliException.usageError(failure.getMessage(), failure);
        }
    }
}
```

- [ ] **Step 6: Give every downstream component only the group it consumes**

Change the executor conversion signature and field mapping exactly:

```java
static CommandExecutorConfig from(ExecutorConfig config) {
    return new CommandExecutorConfig(
            config.queueCapacity(), config.queueMaxBytes(),
            config.backpressureHighWatermark(), config.backpressureLowWatermark(),
            config.backpressureBytesHighWatermark(), config.backpressureBytesLowWatermark(),
            config.maxDrainCommands(), config.drainTimeLimitMillis(),
            switch (config.schedulingPolicy()) {
                case GLOBAL -> yier.bubu.redis.execution.executor.SchedulingPolicy.GLOBAL;
                case FAIR -> yier.bubu.redis.execution.executor.SchedulingPolicy.FAIR;
            });
}
```

Use this channel initializer boundary and delete all convenience constructors that accepted the flat record:

```java
YierdisServerChannelInitializer(
        NetworkConfig network,
        ReplyConfig reply,
        CommandExecutor<NettyExecutionConnection> executor,
        RedisReplyWriterFactory replyWriterFactory,
        InboundMemoryBudget inboundMemoryBudget,
        OutboundMemoryBudget outboundMemoryBudget,
        ChildChannelRegistry childChannelRegistry,
        ReplyEgressStats replyEgressStats
)
```

Every decoder, idle/output limit, transaction-session limit, max-client, and I/O-related read comes from `network`; every reply lease/chunk/control limit comes from `reply`. Change helper signatures to `perConnectionHardLimit(NetworkConfig)` and `receiveBufferCapacity(NetworkConfig)`.

`NettyServerSnapshotProvider` receives only the static groups it reports:

```java
NettyServerSnapshotProvider(
        NetworkConfig network,
        ExecutorConfig executor,
        ReplyConfig reply,
        StorageConfig storage
)
```

In `YierdisServerBootstrap`, store `YierdisServerConfig serverConfig` and immediately bind local group variables in `startInternal()`:

```java
NetworkConfig network = serverConfig.network();
ExecutorConfig executorConfig = serverConfig.executor();
ReplyConfig reply = serverConfig.reply();
StorageConfig storage = serverConfig.storage();
MaintenanceConfig maintenance = serverConfig.maintenance();
```

Use `network` for bind/event loops/budgets/clients, `executorConfig` only in `CommandExecutorConfigs.from(...)`, `reply` for outbound/reply construction and drain timeout, `storage` for DB count/maxmemory/policy/backend, and `maintenance` for cleanup scheduling and slow-command limits. `port()` reads `serverConfig.network().port()`.

- [ ] **Step 7: Wire StorageConfig as the defrag source and compose native backend config explicitly**

Storage/memory Task 1 already replaced the four native-defrag components and
builder setters in `YierdisInstanceConfig` with its required
`DbDefragConfig defrag` value and preserved the existing default
`new DbDefragConfig(false, 64L * 1024L, 64L, 1L)`. Do not alter that API or
default here. Replace only the temporary flat-runtime mapping in bootstrap with
the grouped source:

```java
YierdisInstanceConfig.Builder instanceConfig = YierdisInstanceConfig.builder()
        .databases(storage.databases())
        .maxmemoryBytes(storage.maxmemoryBytes())
        .maxmemoryScope(toRuntimeScope(storage.maxmemoryScope()))
        .maxmemoryPolicy(storage.maxmemoryPolicy())
        .maxmemorySamples(storage.maxmemorySamples())
        .evictionTimeLimitMillis(maintenance.evictionTimeLimitMillis())
        .expireCleanupTimeLimitMillis(maintenance.expireCleanupTimeLimitMillis())
        .defrag(storage.defrag());
```

`YierdisInstance` already passes `config.defrag()` directly to every
`new DbEngineConfig(...)`; storage/memory Task 1 owns the per-DB
quotient/remainder and identity assertions in `DbEngineFactoryInjectionTest`.
This task adds the bootstrap-level assertion that the same
`storage.defrag()` object reaches the `YierdisInstanceConfig.Builder` without
reconstruction.

Compose db-memory backend policy separately and exactly once:

```java
YierdisDbBackendConfig backend = new YierdisDbBackendConfig(storage.nativeSlotCapacity());
instanceConfig.engineFactory(new YierdisDbEngineFactory(
        YierdisFfmStableMemoryBackend::new,
        backend
));
```

`YierdisFfmStableMemoryBackend` is the storage plan's only public FFM composition class, and `YierdisDbEngineFactory` has the exact constructor `(StableMemoryBackendFactory, YierdisDbBackendConfig)`. Neither `DbEngineConfig` nor `YierdisDbBackendConfig` duplicates the other's fields.

- [ ] **Step 8: Run Task 5 GREEN tests and the server-main regression suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main,yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-architecture-tests \
  -am \
  -Dtest=YierdisServerConfigTest,YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; CLI round trips retain every value, each group rejects invalid construction independently, downstream signatures accept only their group, and bootstrap passes the original `storage.defrag()` object into the already-migrated runtime builder.

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main -am test
```

Expected: PASS with no ignored or disabled server-main regression.

- [ ] **Step 9: Enforce zero residue and commit Task 5**

Run:

```bash
rg -n 'YierdisServerRuntimeConfig|toRuntimeConfig\(|runtimeConfig\(\)' \
  --glob '*.java' \
  yierdis-server yierdis-tests
```

Expected: no output.

Run:

```bash
rg -n 'nativeDefrag(Enabled|MaxMoveBytes|MaxObjects|TimeLimitMillis)' \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server \
  yierdis-server/yierdis-server-runtime/src/main/java
```

Expected: no output; bootstrap/runtime refer only to `storage.defrag()` or `config.defrag()`.

Run:

```bash
rg -n 'new DbDefragConfig' \
  yierdis-server/yierdis-server-main/src/main/java \
  yierdis-server/yierdis-server-runtime/src/main/java
```

Expected: exactly one result in `YierdisServerArgs.toServerConfig()`.

Run:

```bash
rg -n 'YierdisServerConfig' \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/CommandExecutorConfigs.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java \
  yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerSnapshotProvider.java
```

Expected: no output; leaf components consume named groups, not the root.

Commit:

```bash
git add yierdis-server/yierdis-server-main \
  yierdis-server/yierdis-server-runtime-api \
  yierdis-server/yierdis-server-runtime \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "refactor: group standalone server configuration"
```

## Final Reactor Verification

- [ ] **Step 1: Verify all coordinated plans have landed in dependency order**

Required order before this plan's final reactor run:

1. This plan Tasks 1-2 provide complete sessions and command syntax while retaining the legacy `execute(...)` engine boundary.
2. Storage/memory Tasks 1-4 provide `DbEngineConfig` and backend injection.
3. Network/executor Task 1 provides serial ownership, two-phase admission, and shared typed reply reservation contracts.
4. Storage/memory Task 5 completes Storage/memory Tasks 1-5 by providing `PreparedMutation` and semantic sources.
5. This plan Task 3 atomically replaces `execute(...)` with prepared execution, `ReplyShape`, and `ReplySizer`.
6. Network/executor Tasks 2-4 consume `PreparedCommand` and `ReplySizer`.
7. This plan Tasks 4-5 provide snapshots and grouped configuration after the network/executor contracts have their final shape.
8. Architecture-governance work evaluates the resulting Maven graph and source boundaries.

- [ ] **Step 2: Run the complete JDK 25 Maven reactor**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn test
```

Expected: `BUILD SUCCESS`, zero test failures, zero test errors, and no skipped regression introduced by this rewrite.

- [ ] **Step 3: Run zero-residue architecture searches**

Run:

```bash
rg -n 'CommandSessionCapabilities|execution\.api\.Session|CommandDescriptor|CommandSpec|CommandReplyPlanner|CommandHandler|ServerInfoProvider|YierdisServerRuntimeConfig|ReplyPlans|requireReply|requireReplyEnvelope|releaseExpansionReservation' \
  --glob '*.java' \
  yierdis-command yierdis-networking yierdis-server yierdis-tests
```

Expected: no output.

Run:

```bash
rg -n 'ReplyPlan' --glob '*.java' yierdis-command yierdis-db
```

Expected: no output.

- [ ] **Step 4: Run standalone smoke checks**

Run the repository's existing no-external-service server smoke profile on JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-server/yierdis-server-main -am \
  -Dtest=RespProtocolIntegrationTest,RespHandshakeIntegrationTest,OrderedReplyPipelineTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; RESP2/RESP3 handshakes, ordered replies, and shutdown complete without an external service.

## Execution Handoff

Plan execution should use `superpowers:subagent-driven-development` in the dependency order above, with a specification-compliance review and code-quality review after every task. Use `superpowers:executing-plans` only when execution must remain inline; do not interleave Task 3 with a partially complete network/executor Task 1 or storage contract task.
