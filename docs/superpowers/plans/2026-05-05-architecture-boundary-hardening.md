# Architecture Boundary Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden Yierdis architecture boundaries so dependency rules, session ownership, change emission, server composition, and storage facade responsibilities are enforced by code instead of convention.

**Architecture:** Keep the existing Maven module split. Add structural architecture tests, make `ServerSession` the explicit command-session contract, replace ThreadLocal change tracking with storage write outcomes recorded by command handlers, and contract `YierdisDb` internals behind lifecycle/support classes.

**Tech Stack:** Java 25, Maven, JUnit 4, ArchUnit `1.4.2`, Netty, existing Yierdis modules.

---

## Scope Check

This plan implements the approved spec in `docs/superpowers/specs/2026-05-04-architecture-boundary-hardening-design.md`.

The work touches several modules, but it is one coherent architecture hardening effort:

- dependency boundary enforcement;
- explicit command session contract;
- explicit mutation/change reporting;
- storage facade contraction;
- documentation and verification.

## File Structure

### Create

- `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`  
  Structural class dependency checks using ArchUnit.

- `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/MutationOutcome.java`  
  Storage-facing value object representing whether a write changed value and/or TTL metadata.

- `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/WriteResult.java`  
  Generic storage write result that carries a command-facing value plus `MutationOutcome`.

### Modify

- `pom.xml`  
  Add `archunit.version` and dependency management for `archunit-junit4`.

- `yierdis-architecture-tests/pom.xml`  
  Add ArchUnit test dependency.

- `yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`  
  Keep current text guards, then add explicit guards for removed optional session accessors and removed `YierdisChangeTracking`.

- `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`  
  Remove the allowed `YierdisChangeTracking` SPI exception once storage no longer imports it.

- `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ServerSession.java`  
  Make connection stats part of the explicit session contract.

- `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/CommandContext.java`  
  Require `ServerSession`, remove optional capability accessors, add explicit mutation recording booleans.

- `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ConnectionStatsProvider.java`  
  Delete after `ServerSession.connectionStats()` replaces it.

- `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/package-info.java`  
  Update audience notes.

- `yierdis-execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java`  
  Fail fast unless the boundary receives a `ServerSession`.

- `yierdis-execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/EngineSession.java`  
  Implement `ServerSession.connectionStats()` directly.

- `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/YierdisDbRouter.java`  
  Route by `ServerSession` instead of `DbIndexProvider`.

- `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/DefaultCommandModules.java`  
  Update router signature and single-DB adapter.

- `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/CommandSupport.java`  
  Use explicit session routing and add a helper to record `MutationOutcome`.

- `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/*Commands.java`  
  Record write outcomes in command handlers.

- `yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`  
  Use explicit session, clear/inspect command mutation state, remove ThreadLocal tracking.

- `yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/TransactionCommands.java`  
  Use explicit session.

- `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/*WriteOps.java`  
  Return `WriteResult<T>` or storage-specific results that carry `MutationOutcome`.

- `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/*Ops.java`  
  Return explicit mutation outcomes instead of calling `YierdisChangeTracking`.

- `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeTracking.java`  
  Delete after all call sites are migrated.

- `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java`  
  Remove references to `YierdisChangeTracking`.

- `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/package-info.java`  
  Remove the legacy SPI note.

- `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDb.java`  
  Make raw storage internals private and route support classes through focused collaborators.

- `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`  
  Expose narrow package-private methods needed by expiration and maxmemory support.

- `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`  
  Stop reading `db.store` and `db.expires` directly.

- Tests in `yierdis-command`, `yierdis-execution`, `yierdis-runtime`, `yierdis-storage`, `yierdis-app`, `yierdis-integration-tests` that currently construct `CommandContext(null, ...)`, implement `ServerSession`, or assert `YierdisChangeTracking` behavior.

- Documentation:
  - `docs/module-architecture.md`
  - `docs/request-execution-flow.md`
  - `docs/main-path-walkthrough.md`
  - `docs/superpowers/specs/2026-05-04-architecture-boundary-hardening-design.md`

---

## Task 1: Add Structural Dependency Boundary Tests

**Files:**
- Modify: `pom.xml`
- Modify: `yierdis-architecture-tests/pom.xml`
- Create: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`
- Test: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`

- [x] **Step 1: Add ArchUnit dependency management**

In `pom.xml`, add this property after the existing version properties:

```xml
        <archunit.version>1.4.2</archunit.version>
```

In `pom.xml`, add this dependency inside `<dependencyManagement><dependencies>` after the JUnit dependency:

```xml
            <dependency>
                <groupId>com.tngtech.archunit</groupId>
                <artifactId>archunit-junit4</artifactId>
                <version>${archunit.version}</version>
            </dependency>
```

In `yierdis-architecture-tests/pom.xml`, add this test dependency after the JUnit dependency:

```xml
        <dependency>
            <groupId>com.tngtech.archunit</groupId>
            <artifactId>archunit-junit4</artifactId>
            <scope>test</scope>
        </dependency>
```

- [x] **Step 2: Write the structural architecture test**

Create `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java` with this exact content:

```java
package yier.bubu.redis.architecture;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class ArchitectureDependencyRuleTest {
    private static final JavaClasses PRODUCTION_CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("yier.bubu.redis", "io.netty");

    @Test
    public void commandImplementationDoesNotDependOnProtocolStorageInternalsExecutorOrNetty() {
        assertNoDependencies(
                "command implementation boundary",
                name -> name.startsWith("yier.bubu.redis.command."),
                List.of(
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.db.",
                        "yier.bubu.redis.offheap.api.",
                        "yier.bubu.redis.executor.",
                        "io.netty."
                )
        );
    }

    @Test
    public void storageMemoryDoesNotDependOnCommandProtocolExecutorOrNetty() {
        assertNoDependencies(
                "storage-memory boundary",
                name -> name.startsWith("yier.bubu.redis.db."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.executor.",
                        "io.netty."
                )
        );
    }

    @Test
    public void executorCoreDoesNotDependOnCommandStorageRuntimeProtocolOrNetty() {
        assertNoDependencies(
                "executor-core boundary",
                name -> name.startsWith("yier.bubu.redis.executor."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.db.",
                        "yier.bubu.redis.runtime.",
                        "yier.bubu.redis.protocol.",
                        "io.netty."
                )
        );
    }

    @Test
    public void executionApiDoesNotDependOnImplementationLayers() {
        assertNoDependencies(
                "execution-api boundary",
                name -> name.startsWith("yier.bubu.redis.contract."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.db.",
                        "yier.bubu.redis.runtime.",
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.ops.",
                        "io.netty."
                )
        );
    }

    private static void assertNoDependencies(
            String ruleName,
            Predicate<String> originMatcher,
            List<String> forbiddenPrefixes
    ) {
        List<String> offenders = new ArrayList<>();
        for (JavaClass origin : PRODUCTION_CLASSES) {
            String originName = origin.getName();
            if (!originMatcher.test(originName)) {
                continue;
            }
            for (Dependency dependency : origin.getDirectDependenciesFromSelf()) {
                String targetName = dependency.getTargetClass().getName();
                for (String forbiddenPrefix : forbiddenPrefixes) {
                    if (targetName.startsWith(forbiddenPrefix)) {
                        offenders.add(ruleName + ": " + originName + " -> " + targetName);
                    }
                }
            }
        }
        Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
    }
}
```

- [x] **Step 3: Run architecture tests and record current failures**

Run:

```bash
mvn -pl yierdis-architecture-tests test
```

Expected: the new test compiles and may fail on the current `YierdisChangeTracking` storage/runtime coupling. Keep the failure output. Later tasks remove the coupling and this test must pass.

- [x] **Step 4: Commit**

```bash
git add pom.xml yierdis-architecture-tests/pom.xml yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java
git commit -m "test: add structural architecture dependency rules"
```

---

## Task 2: Make ServerSession the Explicit Command Session

**Files:**
- Modify: `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ServerSession.java`
- Modify: `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/CommandContext.java`
- Delete: `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ConnectionStatsProvider.java`
- Modify: `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/package-info.java`
- Modify: `yierdis-execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/EngineSession.java`
- Modify: `yierdis-execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/DefaultYierdisEngine.java`
- Modify: `yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/YierdisDbRouter.java`
- Modify: `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/DefaultCommandModules.java`
- Modify: `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify: `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java`
- Modify: `yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Modify: `yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/TransactionCommands.java`
- Modify tests that construct `CommandContext(null, ...)` or implement `ServerSession`

- [x] **Step 1: Write failing tests for fail-fast engine boundary**

Modify `yierdis-execution/yierdis-engine/src/test/java/yier/bubu/redis/engine/DefaultYierdisEngineTest.java`.

Replace the `executeDelegatesThroughOwnedCommandProcessor` test body with this:

```java
    @Test
    public void executeDelegatesThroughOwnedCommandProcessor() {
        YierdisEngine engine = new DefaultYierdisEngine(
                () -> {
                },
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        engine.execute(
                new EngineSession(16, 1024),
                ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                out
        );

        Assert.assertEquals("LOCAL_OK", out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }
```

Add this test below it:

```java
    @Test
    public void executeRejectsNonServerSessionBeforeCommandModulesRun() {
        YierdisEngine engine = new DefaultYierdisEngine(
                () -> {
                },
                registration -> registration.register(
                        "LOCAL",
                        CommandDescriptor.of(1, 0, 0, 0),
                        CommandParsers.exactRequest(1, "local"),
                        (request, ctx) -> ctx.out().simpleString("LOCAL_OK")
                )
        );

        CapturingReplyWriter out = new CapturingReplyWriter();
        try {
            engine.execute(
                    null,
                    ByteArrayExecutionRequest.fromUtf8("LOCAL", List.of()),
                    out
            );
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("YierdisEngine requires ServerSession", e.getMessage());
        }

        Assert.assertNull(out.simpleStringValue);
        Assert.assertNull(out.errorValue);
    }
```

Run:

```bash
mvn -pl yierdis-execution/yierdis-engine -Dtest=DefaultYierdisEngineTest test
```

Expected: FAIL because `DefaultYierdisEngine` currently accepts `null` and `CommandContext` currently allows a generic nullable session.

- [x] **Step 2: Replace ServerSession contract**

Replace `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ServerSession.java` with:

```java
package yier.bubu.redis.contract;

/**
 * Server-side per-connection session state exposed to the command layer.
 * <p>
 * This is transport-agnostic and models Redis-like connection state such as selected DB, client metadata, AUTH state,
 * MULTI transaction queue, and read-only connection stats.
 */
public interface ServerSession extends Session {
    int dbIndex();

    void setDbIndex(int dbIndex);

    long clientId();

    String clientName();

    void setClientName(String clientName);

    boolean authenticated();

    void setAuthenticated(boolean authenticated);

    TransactionState transaction();

    ConnectionStatsView connectionStats();
}
```

Delete `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/ConnectionStatsProvider.java`.

- [x] **Step 3: Replace CommandContext**

Replace `yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/CommandContext.java` with:

```java
package yier.bubu.redis.contract;

import java.util.Objects;

/**
 * Command execution context (transport-agnostic).
 * <p>
 * Groups the required server-side session and the output port. The command path must not silently run with a weaker
 * marker session because DB routing, transactions, connection metadata, and change emission all depend on explicit
 * server session semantics.
 */
public final class CommandContext {
    private ServerSession session;
    private ReplyWriter out;
    private boolean valueChanged;
    private boolean ttlChanged;

    public CommandContext(ServerSession session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
    }

    public CommandContext reset(ServerSession session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        clearMutationOutcome();
        return this;
    }

    public ServerSession session() {
        return session;
    }

    public ReplyWriter out() {
        return out;
    }

    public void clearMutationOutcome() {
        valueChanged = false;
        ttlChanged = false;
    }

    public void recordMutation(boolean changedValue, boolean changedTtl) {
        valueChanged |= changedValue;
        ttlChanged |= changedTtl;
    }

    public boolean valueChanged() {
        return valueChanged;
    }

    public boolean ttlChanged() {
        return ttlChanged;
    }

    public boolean changedAny() {
        return valueChanged || ttlChanged;
    }
}
```

- [x] **Step 4: Update EngineSession and DefaultYierdisEngine**

In `EngineSession.java`, remove `ConnectionStatsProvider` from the implements list and delete its import.

Ensure the class declaration is:

```java
public final class EngineSession implements ServerSession {
```

Keep the existing `connectionStats()` method as the `ServerSession` implementation:

```java
    @Override
    public ConnectionStatsView connectionStats() {
        return connectionStatsSupplier.get();
    }
```

In `DefaultYierdisEngine.java`, add this import:

```java
import yier.bubu.redis.contract.ServerSession;
```

Replace `execute` with:

```java
    @Override
    public void execute(Session session, ExecutionRequest request, ReplyWriter out) {
        if (!(session instanceof ServerSession serverSession)) {
            throw new IllegalArgumentException("YierdisEngine requires ServerSession");
        }
        commandProcessor.execute(request, new CommandContext(serverSession, out));
    }
```

- [x] **Step 5: Update routing to use ServerSession**

Replace `YierdisDbRouter.java` with:

```java
package yier.bubu.redis.command;

import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.ops.DbEngine;

/**
 * DB routing abstraction: decouples command implementations from concrete DB arrays while keeping selected DB state
 * explicit in the server session.
 */
public interface YierdisDbRouter {
    DbEngine dbFor(ServerSession session);

    int databases();
}
```

In `DefaultCommandModules.java`, replace the `DbIndexProvider` import with `ServerSession`:

```java
import yier.bubu.redis.contract.ServerSession;
```

In the `singleDbRouter` method, replace the `dbFor` signature with:

```java
            @Override
            public DbEngine dbFor(ServerSession session) {
                return fixed;
            }
```

In `CommandSupport.java`, remove the `DbIndexProvider` import and replace `db(CommandContext ctx)` with:

```java
    DbEngine db(CommandContext ctx) {
        java.util.Objects.requireNonNull(ctx, "ctx");
        return dbRouter.dbFor(ctx.session());
    }
```

- [x] **Step 6: Update command code to use explicit session**

In `CoreConnectionCommands.select`, replace:

```java
        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            s.setDbIndex(dbIndex);
        } else if (dbIndex != 0) {
            out.error("ERR DB index is out of range");
            return;
        }
        out.simpleString("OK");
```

with:

```java
        ctx.session().setDbIndex(dbIndex);
        out.simpleString("OK");
```

In `TransactionCommands.java`, replace `txOrNull` with:

```java
    private TransactionState tx(CommandContext ctx) {
        return ctx.session().transaction();
    }
```

Then replace these three call sites:

```java
        TransactionState tx = txOrNull(ctx);
        if (tx == null) {
            out.error("ERR MULTI is only supported on server connections");
            return;
        }
```

with:

```java
        TransactionState tx = tx(ctx);
```

Replace:

```java
        TransactionState tx = txOrNull(ctx);
        if (tx == null || !tx.active()) {
```

with:

```java
        TransactionState tx = tx(ctx);
        if (!tx.active()) {
```

Apply that replacement in both `discard` and `exec`.

In `YierdisFastCommandProcessor.execute`, replace the transaction lookup block:

```java
        TransactionState tx = null;
        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            tx = s.transaction();
        }
```

with:

```java
        TransactionState tx = ctx.session().transaction();
```

Replace db index extraction:

```java
                int dbIndex = 0;
                DbIndexProvider provider = ctx.dbIndexProviderOrNull();
                if (provider != null) {
                    dbIndex = Math.max(0, provider.dbIndex());
                }
```

with:

```java
                int dbIndex = Math.max(0, ctx.session().dbIndex());
```

Remove the unused `DbIndexProvider` and `ServerSession` imports from `YierdisFastCommandProcessor.java`.

- [x] **Step 7: Update test sessions**

Every class implementing `ServerSession` must add:

```java
        @Override
        public yier.bubu.redis.contract.ConnectionStatsView connectionStats() {
            return null;
        }
```

Update these known files:

- `yierdis-runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeEmissionTest.java`
- `yierdis-runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- `yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- any additional compile errors reported by Maven.

For command-kernel tests that currently pass `new CommandContext(null, out)`, add this private helper class once per test file:

```java
    private static final class TestSession implements yier.bubu.redis.contract.ServerSession {
        private final yier.bubu.redis.contract.TransactionState tx = new TestTransactionState();

        @Override
        public int dbIndex() {
            return 0;
        }

        @Override
        public void setDbIndex(int dbIndex) {
        }

        @Override
        public long clientId() {
            return 1L;
        }

        @Override
        public String clientName() {
            return null;
        }

        @Override
        public void setClientName(String clientName) {
        }

        @Override
        public boolean authenticated() {
            return false;
        }

        @Override
        public void setAuthenticated(boolean authenticated) {
        }

        @Override
        public yier.bubu.redis.contract.TransactionState transaction() {
            return tx;
        }

        @Override
        public yier.bubu.redis.contract.ConnectionStatsView connectionStats() {
            return null;
        }
    }

    private static final class TestTransactionState implements yier.bubu.redis.contract.TransactionState {
        @Override
        public boolean active() {
            return false;
        }

        @Override
        public void begin() {
        }

        @Override
        public void discard() {
        }

        @Override
        public void enqueue(yier.bubu.redis.contract.ExecutionRequest request) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public java.util.List<yier.bubu.redis.contract.ExecutionRequest> drain() {
            return java.util.List.of();
        }
    }

    private static CommandContext context(ReplyWriter out) {
        return new CommandContext(new TestSession(), out);
    }
```

Then replace each `new CommandContext(null, out)` and `new CommandContext(null, writer)` with `context(out)` or `context(writer)`.

- [x] **Step 8: Update CommandContext smoke test**

In `yierdis-execution/yierdis-execution-api/src/test/java/yier/bubu/redis/contract/CoreContractSmokeTest.java`, replace the `CommandContext` assertion block with:

```java
        ServerSession session = new ServerSession() {
            private final TransactionState tx = new TransactionState() {
                @Override
                public boolean active() {
                    return false;
                }

                @Override
                public void begin() {
                }

                @Override
                public void discard() {
                }

                @Override
                public void enqueue(ExecutionRequest request) {
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public java.util.List<ExecutionRequest> drain() {
                    return java.util.List.of();
                }
            };

            @Override
            public int dbIndex() {
                return 0;
            }

            @Override
            public void setDbIndex(int dbIndex) {
            }

            @Override
            public long clientId() {
                return 1L;
            }

            @Override
            public String clientName() {
                return null;
            }

            @Override
            public void setClientName(String clientName) {
            }

            @Override
            public boolean authenticated() {
                return false;
            }

            @Override
            public void setAuthenticated(boolean authenticated) {
            }

            @Override
            public TransactionState transaction() {
                return tx;
            }

            @Override
            public ConnectionStatsView connectionStats() {
                return null;
            }
        };

        CommandContext ctx = new CommandContext(session, writer);
        Assert.assertSame(session, ctx.session());
        Assert.assertSame(writer, ctx.out());
        ctx.recordMutation(true, false);
        Assert.assertTrue(ctx.valueChanged());
        Assert.assertFalse(ctx.ttlChanged());
        Assert.assertTrue(ctx.changedAny());
        ctx.clearMutationOutcome();
        Assert.assertFalse(ctx.changedAny());
        writer.requestCloseAfterReply();
        Assert.assertTrue(writer.closeAfterReplyRequested());
```

- [x] **Step 9: Run focused verification**

Run:

```bash
mvn -pl yierdis-execution/yierdis-execution-api,yierdis-execution/yierdis-engine,yierdis-command/yierdis-command-kernel,yierdis-command/yierdis-command-defaults test
```

Expected: PASS after updating all explicit session call sites.

- [x] **Step 10: Commit**

```bash
git add yierdis-execution yierdis-command yierdis-runtime yierdis-integration-tests
git commit -m "refactor: require explicit server session in command context"
```

---

## Task 3: Add Explicit Storage Mutation Outcome Types

**Files:**
- Create: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/MutationOutcome.java`
- Create: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/WriteResult.java`
- Modify: `yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/CommandSupport.java`
- Modify: `yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- Test: `yierdis-storage/yierdis-storage-api/src/test/java/yier/bubu/redis/ops/MutationOutcomeTest.java` if the module already has test source; otherwise use `yierdis-storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/db/MutationOutcomeContractTest.java`

- [x] **Step 1: Add mutation outcome value object**

Create `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/MutationOutcome.java`:

```java
package yier.bubu.redis.ops;

/**
 * Explicit facts about durable state changed by a storage write.
 */
public final class MutationOutcome {
    public static final MutationOutcome NONE = new MutationOutcome(false, false);
    public static final MutationOutcome VALUE_CHANGED = new MutationOutcome(true, false);
    public static final MutationOutcome TTL_CHANGED = new MutationOutcome(false, true);
    public static final MutationOutcome VALUE_AND_TTL_CHANGED = new MutationOutcome(true, true);

    private final boolean valueChanged;
    private final boolean ttlChanged;

    private MutationOutcome(boolean valueChanged, boolean ttlChanged) {
        this.valueChanged = valueChanged;
        this.ttlChanged = ttlChanged;
    }

    public static MutationOutcome of(boolean valueChanged, boolean ttlChanged) {
        if (valueChanged && ttlChanged) {
            return VALUE_AND_TTL_CHANGED;
        }
        if (valueChanged) {
            return VALUE_CHANGED;
        }
        if (ttlChanged) {
            return TTL_CHANGED;
        }
        return NONE;
    }

    public boolean valueChanged() {
        return valueChanged;
    }

    public boolean ttlChanged() {
        return ttlChanged;
    }

    public boolean changedAny() {
        return valueChanged || ttlChanged;
    }

    public MutationOutcome plus(MutationOutcome other) {
        if (other == null || other == NONE) {
            return this;
        }
        return of(valueChanged || other.valueChanged, ttlChanged || other.ttlChanged);
    }
}
```

Create `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/WriteResult.java`:

```java
package yier.bubu.redis.ops;

import java.util.Objects;

/**
 * Generic command-facing write result.
 */
public final class WriteResult<T> {
    private final T value;
    private final MutationOutcome mutationOutcome;

    private WriteResult(T value, MutationOutcome mutationOutcome) {
        this.value = value;
        this.mutationOutcome = mutationOutcome == null ? MutationOutcome.NONE : mutationOutcome;
    }

    public static <T> WriteResult<T> of(T value, MutationOutcome mutationOutcome) {
        return new WriteResult<>(value, mutationOutcome);
    }

    public static <T> WriteResult<T> unchanged(T value) {
        return new WriteResult<>(value, MutationOutcome.NONE);
    }

    public T value() {
        return value;
    }

    public MutationOutcome mutationOutcome() {
        return mutationOutcome;
    }

    public boolean changedAny() {
        return mutationOutcome.changedAny();
    }
}
```

If the compiler flags `Objects` as unused, remove the `java.util.Objects` import from `WriteResult.java`.

- [x] **Step 2: Add CommandSupport recording helper**

In `CommandSupport.java`, add this import:

```java
import yier.bubu.redis.ops.MutationOutcome;
```

Add this method after `dbWrites(CommandContext ctx)`:

```java
    void recordMutation(CommandContext ctx, MutationOutcome outcome) {
        if (outcome == null) {
            return;
        }
        ctx.recordMutation(outcome.valueChanged(), outcome.ttlChanged());
    }
```

- [x] **Step 3: Update processor to inspect CommandContext mutation flags**

In `YierdisFastCommandProcessor.java`, remove the import:

```java
import yier.bubu.redis.runtime.api.YierdisChangeTracking;
```

Replace the command execution block:

```java
            boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
            boolean changed = false;
            if (sinkEnabled) {
                try (YierdisChangeTracking.Scope ignored = YierdisChangeTracking.beginScope()) {
                    executeSpec(spec, request, ctx);
                    changed = YierdisChangeTracking.changedAny();
                }
            } else {
                executeSpec(spec, request, ctx);
            }
```

with:

```java
            boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
            ctx.clearMutationOutcome();
            executeSpec(spec, request, ctx);
            boolean changed = ctx.changedAny();
```

Keep the existing `if (sinkEnabled && changed)` block from Task 2.

- [x] **Step 4: Run focused compile to expose write API migration failures**

Run:

```bash
mvn -pl yierdis-command/yierdis-command-kernel,yierdis-storage/yierdis-storage-api test
```

Expected: command-kernel compiles after the processor change; storage-api compiles with the new value objects.

- [x] **Step 5: Commit**

```bash
git add yierdis-storage/yierdis-storage-api yierdis-command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/CommandSupport.java yierdis-command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java
git commit -m "feat: add explicit mutation outcome plumbing"
```

---

## Task 4: Migrate String, TTL, and Keyspace Writes to Explicit Outcomes

**Files:**
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/StringWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/TtlWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/KeyspaceWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/DbLifecycleOps.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbLifecycleOps.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: command handlers for `SET`, `APPEND`, `SETBIT`, `INCR`, `DECR`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `DEL`, `FLUSHDB`
- Test: `yierdis-runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeEmissionTest.java`

- [x] **Step 1: Change string write API**

In `StringWriteOps.SetStringResult`, add a `MutationOutcome` field and update factory/accessors:

```java
        private final MutationOutcome mutationOutcome;

        private SetStringResult(boolean applied, byte[] oldValue, MutationOutcome mutationOutcome) {
            this.applied = applied;
            this.oldValue = oldValue;
            this.mutationOutcome = mutationOutcome == null ? MutationOutcome.NONE : mutationOutcome;
        }

        public static SetStringResult of(boolean applied, byte[] oldValue, MutationOutcome mutationOutcome) {
            return new SetStringResult(applied, oldValue, mutationOutcome);
        }

        public MutationOutcome mutationOutcome() {
            return mutationOutcome;
        }
```

Replace these method signatures in `StringWriteOps`:

```java
    boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    long append(byte[] keyBytes, BytesSlice value);

    int setBit(byte[] keyBytes, long offset, int value);

    long incrBy(byte[] keyBytes, long delta);
```

with:

```java
    WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption);

    WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption);

    WriteResult<Long> append(byte[] keyBytes, BytesSlice value);

    WriteResult<Integer> setBit(byte[] keyBytes, long offset, int value);

    WriteResult<Long> incrBy(byte[] keyBytes, long delta);
```

- [x] **Step 2: Change TTL, keyspace, and lifecycle write APIs**

Replace `TtlWriteOps` method signatures with:

```java
    WriteResult<Boolean> expire(BytesView keyView, long seconds);

    WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds);

    WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds);

    WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis);

    WriteResult<Boolean> persist(BytesView keyView);
```

Replace `KeyspaceWriteOps.del` with:

```java
    WriteResult<Long> del(Collection<byte[]> keys);
```

Replace `DbLifecycleOps.flushDb` with:

```java
    MutationOutcome flushDb();
```

- [x] **Step 3: Update string storage implementation**

In `YierdisStringOps.set`, replace each `SetStringResult.of(...)` call with the three-argument form:

```java
SetStringResult.of(true, oldValue[0], MutationOutcome.VALUE_CHANGED)
```

When the operation changes both value and TTL, return:

```java
SetStringResult.of(true, oldValue[0], MutationOutcome.VALUE_AND_TTL_CHANGED)
```

When the operation does not apply, return:

```java
SetStringResult.of(false, oldValue[0], MutationOutcome.NONE)
```

Remove all `YierdisChangeTracking.markValueChanged()` and `YierdisChangeTracking.markTtlChanged()` calls from `YierdisStringOps`.

Replace `setString` implementations with:

```java
    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        SetStringResult result = set(keyBytes, sliceOf(value), mode, expireOption, false);
        return WriteResult.of(result.applied(), result.mutationOutcome());
    }

    @Override
    public WriteResult<Boolean> setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        SetStringResult result = set(keyBytes, value, mode, expireOption, false);
        return WriteResult.of(result.applied(), result.mutationOutcome());
    }
```

For `append`, `setBit`, and `incrBy`, return `WriteResult.of(value, MutationOutcome.VALUE_CHANGED)` only when the method previously marked value changed. Return `WriteResult.unchanged(value)` when no mutation occurred.

- [x] **Step 4: Update TTL/keyspace/lifecycle implementations**

In `YierdisTtlOps`, return:

```java
WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED)
```

when an expire/persist operation actually changes TTL metadata, and:

```java
WriteResult.unchanged(Boolean.FALSE)
```

when it returns false.

For TTL code paths that delete an already expired key and previously marked value changed, return:

```java
WriteResult.of(Boolean.TRUE, MutationOutcome.VALUE_CHANGED)
```

In `YierdisKeyspaceOps.del`, return:

```java
WriteResult.of(deletedCount, deletedCount > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE)
```

In `YierdisDbLifecycleOps.flushDb`, return `db.flushDb()`.

In `YierdisDb.flushDb`, change the return type to `MutationOutcome` and replace the end of the method with:

```java
        return MutationOutcome.of(hadKeys, hadTtl);
```

- [x] **Step 5: Update command handlers**

In `StringCommands.set`, after receiving the `SetStringResult`, add:

```java
        support.recordMutation(ctx, result.mutationOutcome());
```

In `append`, replace:

```java
        long len = support.dbWrites(ctx).strings().append(request.readOnlyByteArray(1), support.argSlice(request, 2));
        out.integer(len);
```

with:

```java
        var result = support.dbWrites(ctx).strings().append(request.readOnlyByteArray(1), support.argSlice(request, 2));
        support.recordMutation(ctx, result.mutationOutcome());
        out.integer(result.value());
```

Apply the same pattern to `setbit`, `incrBy`, keyspace `DEL`, TTL commands, and `FLUSHDB`:

```java
        var result = support.dbWrites(ctx).ttl().expire(support.argView(request, 1), seconds);
        support.recordMutation(ctx, result.mutationOutcome());
        out.integer(result.value() ? 1 : 0);
```

For `FLUSHDB`, use:

```java
        support.recordMutation(ctx, support.db(ctx).lifecycle().flushDb());
        out.simpleString("OK");
```

- [x] **Step 6: Run focused tests**

Run:

```bash
mvn -pl yierdis-storage/yierdis-storage-api,yierdis-storage/yierdis-storage-memory,yierdis-command/yierdis-command-defaults,yierdis-runtime/yierdis-runtime-embedded -Dtest=YierdisChangeEmissionTest,StringCommandsTest,KeysBudgetTest test
```

Expected: PASS after all signatures and handlers are updated. If `StringCommandsTest` does not exist in this repo, Maven reports that no such test was executed; continue with the remaining named tests and then run the full module command in Step 7.

- [x] **Step 7: Run full affected modules**

```bash
mvn -pl yierdis-storage/yierdis-storage-api,yierdis-storage/yierdis-storage-memory,yierdis-command/yierdis-command-defaults,yierdis-runtime/yierdis-runtime-embedded test
```

Expected: PASS.

- [x] **Step 8: Commit**

```bash
git add yierdis-storage yierdis-command yierdis-runtime
git commit -m "refactor: report string ttl and keyspace mutations explicitly"
```

---

## Task 5: Migrate Hash, List, Set, ZSet, and HLL Writes

**Files:**
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/HashWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/ListWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/SetWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/ZSetWriteOps.java`
- Modify: `yierdis-storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/HllWriteOps.java`
- Modify: corresponding `Yierdis*Ops.java` storage-memory implementations
- Modify: corresponding `*Commands.java` command handlers
- Test: existing command and change emission tests

- [x] **Step 1: Change remaining write interfaces**

Replace `HashWriteOps` with:

```java
package yier.bubu.redis.ops;

import java.util.List;

public interface HashWriteOps {
    WriteResult<Long> hset(byte[] keyBytes, List<byte[]> fieldValuePairs);

    WriteResult<Long> hdel(byte[] keyBytes, List<byte[]> fields);
}
```

Replace `ListWriteOps` with:

```java
package yier.bubu.redis.ops;

import java.util.List;

public interface ListWriteOps {
    WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<List<byte[]>> lpop(byte[] keyBytes, int count);

    WriteResult<List<byte[]>> rpop(byte[] keyBytes, int count);
}
```

Replace `SetWriteOps` with:

```java
package yier.bubu.redis.ops;

import java.util.List;

public interface SetWriteOps {
    WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members);

    WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members);
}
```

Replace `ZSetWriteOps` with:

```java
package yier.bubu.redis.ops;

import java.util.List;

public interface ZSetWriteOps {
    WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs);

    WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive);

    WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop);

    WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members);
}
```

Replace `HllWriteOps` with:

```java
package yier.bubu.redis.ops;

import java.util.List;

public interface HllWriteOps {
    WriteResult<Integer> pfadd(byte[] keyBytes, List<byte[]> elements);

    WriteResult<Void> pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys);
}
```

- [x] **Step 2: Update storage implementations**

In each storage implementation, replace previous primitive return values with `WriteResult`.

Use these exact mapping rules:

- If a method previously called `YierdisChangeTracking.markValueChanged()`, return `WriteResult.of(value, MutationOutcome.VALUE_CHANGED)`.
- If a method made no durable change, return `WriteResult.unchanged(value)`.
- For `ZADD`, use the existing `changedAny[0]` flag: `added` may be `0`, but score updates must still return `MutationOutcome.VALUE_CHANGED`.
- For `PFMERGE`, return `WriteResult.of(null, MutationOutcome.VALUE_CHANGED)` when the merge writes the destination key.

- [x] **Step 3: Update command handlers**

For every command handler changed by this task, use this pattern:

```java
        var result = support.dbWrites(ctx).hashes().hset(key, fieldValuePairs);
        support.recordMutation(ctx, result.mutationOutcome());
        out.integer(result.value());
```

Use the same pattern for list, set, zset, and HLL handlers.

For list pop handlers:

```java
        var result = support.dbWrites(ctx).lists().lpop(key, count);
        support.recordMutation(ctx, result.mutationOutcome());
        out.bulkStringArray(result.value());
```

For `PFMERGE`:

```java
        var result = support.dbWrites(ctx).hll().pfmerge(destKey, sourceKeys);
        support.recordMutation(ctx, result.mutationOutcome());
        out.simpleString("OK");
```

- [x] **Step 4: Remove storage imports of YierdisChangeTracking**

Run:

```bash
rg -n "YierdisChangeTracking" yierdis-storage/yierdis-storage-memory/src/main/java
```

Expected before the final edits in this task: one or more matches.

Remove all imports and all calls from storage-memory production code.

Run the same command again.

Expected after edits: no output.

- [x] **Step 5: Run change emission and storage tests**

```bash
mvn -pl yierdis-storage/yierdis-storage-memory,yierdis-command/yierdis-command-defaults,yierdis-runtime/yierdis-runtime-embedded -Dtest=YierdisChangeEmissionTest test
```

Expected: PASS.

Then run:

```bash
mvn -pl yierdis-storage/yierdis-storage-memory,yierdis-command/yierdis-command-defaults,yierdis-runtime/yierdis-runtime-embedded test
```

Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add yierdis-storage yierdis-command yierdis-runtime
git commit -m "refactor: report collection mutations explicitly"
```

---

## Task 6: Remove YierdisChangeTracking and Tighten Guards

**Files:**
- Delete: `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeTracking.java`
- Modify: `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java`
- Modify: `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/package-info.java`
- Modify: `yierdis-runtime/yierdis-runtime-api/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/db/MutationExecutorReservationTest.java`
- Modify: `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Delete ThreadLocal tracking class**

Delete:

```text
yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeTracking.java
```

- [x] **Step 2: Update runtime API docs**

In `YierdisChangeEvent.java`, replace the paragraph that references `YierdisChangeTracking` with:

```java
 * 发射语义（命令层约定）：仅当命令执行成功，且本次命令造成 Keyspace/Value/TTL 元数据的真实变化时才应发射事件。
 * 该“真实变更”判定由 storage-api 写结果显式返回，避免依赖隐藏的线程本地状态。
```

In `yierdis-runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/package-info.java`, remove any sentence naming `YierdisChangeTracking`.

- [x] **Step 3: Update tests that asserted ThreadLocal behavior**

In `YierdisChangeSinkTest.java`, remove the test method that calls `YierdisChangeTracking.markValueChanged()`, `beginScope()`, `changedAny()`, `changedValue()`, or `changedTtl()`.

In `MutationExecutorReservationTest.java`, replace tracking assertions with storage result assertions. Use this pattern:

```java
        var result = db.writes().strings().setString(
                "k".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                "v".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                yier.bubu.redis.ops.SetMode.NORMAL,
                null
        );
        Assert.assertTrue(result.value());
        Assert.assertTrue(result.mutationOutcome().valueChanged());
```

- [x] **Step 4: Tighten architecture policy**

In `architecture-policy.yml`, delete these lines under `yierdis-storage-memory`:

```yaml
    allowed_spi_imports:
      yier.bubu.redis.runtime.api.YierdisChangeTracking:
        - yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db
```

In `ArchitectureBoundaryTest.java`, add a test:

```java
    @Test
    public void productionCodeMustNotUseYierdisChangeTracking() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-command").normalize(),
                offenders,
                "YierdisChangeTracking"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-storage").normalize(),
                offenders,
                "YierdisChangeTracking"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-runtime").normalize(),
                offenders,
                "YierdisChangeTracking"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何文件", scanned > 0);
        Assert.assertTrue("YierdisChangeTracking must not remain in production code:\n" + String.join("\n", offenders), offenders.isEmpty());
    }
```

- [x] **Step 5: Run removal verification**

```bash
rg -n "YierdisChangeTracking" yierdis-command yierdis-storage yierdis-runtime yierdis-architecture-tests
```

Expected: no output.

Run:

```bash
mvn -pl yierdis-runtime/yierdis-runtime-api,yierdis-storage/yierdis-storage-memory,yierdis-architecture-tests test
```

Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add yierdis-runtime yierdis-storage yierdis-architecture-tests
git commit -m "refactor: remove thread local change tracking"
```

---

## Task 7: Contract YierdisDb Raw Storage Internals

**Files:**
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbKeyLifecycle.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java`
- Modify: `yierdis-storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- Modify: tests that access `db.store` or `db.expires`
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`

- [x] **Step 1: Write failing architecture guard**

In `YierdisDbArchitectureGuardTest.java`, add:

```java
    @Test
    public void yierdisDbRawContainersMustBePrivate() throws Exception {
        Assert.assertTrue(java.lang.reflect.Modifier.isPrivate(YierdisDb.class.getDeclaredField("store").getModifiers()));
        Assert.assertTrue(java.lang.reflect.Modifier.isPrivate(YierdisDb.class.getDeclaredField("expires").getModifiers()));
    }
```

Run:

```bash
mvn -pl yierdis-architecture-tests -Dtest=YierdisDbArchitectureGuardTest test
```

Expected: FAIL because `store` and `expires` are currently package-private.

- [x] **Step 2: Add narrow key lifecycle methods**

In `YierdisDbKeyLifecycle.java`, add these package-private methods:

```java
    int keyCount() {
        return store.size();
    }

    int expireCount() {
        return expires.size();
    }

    KeyHandle randomKeyHandle() {
        return store.randomKeyHandle();
    }

    KeyHandle randomExpireKeyHandle() {
        return expires.randomKeyHandle();
    }

    YierdisObject object(KeyHandle keyHandle) {
        return store.get(keyHandle);
    }

    Long expireAtMillis(KeyHandle keyHandle) {
        return expires.get(keyHandle);
    }

    boolean removeObject(KeyHandle keyHandle, YierdisObject object) {
        return store.remove(keyHandle, object);
    }

    void forEachKeyHandle(java.util.function.BiConsumer<KeyHandle, YierdisObject> consumer) {
        store.forEachKeyHandle(consumer);
    }
```

- [x] **Step 3: Update expiration and maxmemory support**

In `YierdisDbExpirationSupport`, replace direct `db.expires` and `db.store` uses with `db.keyLifecycle()` methods.

Use this mapping:

- `db.expires.size()` -> `db.keyLifecycle().expireCount()`
- `db.expires.randomKeyHandle()` -> `db.keyLifecycle().randomExpireKeyHandle()`
- `db.expires.get(keyHandle)` -> `db.keyLifecycle().expireAtMillis(keyHandle)`
- `db.store.get(keyHandle)` -> `db.keyLifecycle().object(keyHandle)`
- `db.store.remove(keyHandle, e)` -> `db.keyLifecycle().removeObject(keyHandle, e)`

In `YierdisDbMaxmemorySupport`, use this mapping:

- `db.store.size()` -> `db.keyLifecycle().keyCount()`
- `db.store.randomKeyHandle()` -> `db.keyLifecycle().randomKeyHandle()`
- `db.store.get(keyHandle)` -> `db.keyLifecycle().object(keyHandle)`
- `db.store.remove(keyHandle, e)` -> `db.keyLifecycle().removeObject(keyHandle, e)`
- `db.store.forEachKeyHandle(...)` -> `db.keyLifecycle().forEachKeyHandle(...)`

In `YierdisDb.java`, add:

```java
    YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }
```

Then change:

```java
    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
```

to:

```java
    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
```

- [x] **Step 4: Update tests using raw containers**

Run:

```bash
rg -n "db\\.(store|expires)" yierdis-storage/yierdis-storage-memory/src/test/java yierdis-integration-tests/src/test/java
```

For each match:

- replace `db.store.randomKey() == null` with `db.size() == 0`;
- replace `db.expires.randomKey() == null` with a behavioral TTL assertion such as `db.reads().ttl().ttl(keyView) == -2` after cleanup;
- replace direct `db.expires.setExpireAtMillis(...)` setup with public TTL write APIs.

For `OffHeapBytesViewTtlRegressionTest`, use public commands through `db.writes().ttl().pexpire(...)` instead of mutating `expires` directly.

- [x] **Step 5: Run storage and architecture tests**

```bash
mvn -pl yierdis-storage/yierdis-storage-memory,yierdis-architecture-tests test
```

Expected: PASS.

- [x] **Step 6: Commit**

```bash
git add yierdis-storage yierdis-architecture-tests
git commit -m "refactor: hide raw db containers behind lifecycle"
```

---

## Task 8: Update Server Composition Root Guards

**Files:**
- Modify: `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [x] **Step 1: Update dbRouter signature in server bootstrap**

In `YierdisServerBootstrap.dbRouter`, replace:

```java
            public DbEngine dbFor(yier.bubu.redis.contract.DbIndexProvider dbIndexProvider) {
                if (dbViews.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = dbIndexProvider == null ? 0 : dbIndexProvider.dbIndex();
                if (idx < 0 || idx >= dbViews.length) {
                    idx = 0;
                }
                return dbViews[idx];
            }
```

with:

```java
            public DbEngine dbFor(yier.bubu.redis.contract.ServerSession session) {
                if (dbViews.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = session == null ? 0 : session.dbIndex();
                if (idx < 0 || idx >= dbViews.length) {
                    idx = 0;
                }
                return dbViews[idx];
            }
```

- [x] **Step 2: Update NettyServerInfoProvider stats access**

Replace:

```java
    private static ConnectionStatsView connectionStats(CommandContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.connectionStatsOrNull();
    }
```

with:

```java
    private static ConnectionStatsView connectionStats(CommandContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.session().connectionStats();
    }
```

- [x] **Step 3: Add architecture guards for command context construction**

In `ArchitectureBoundaryTest.java`, strengthen the server guard that already forbids `new CommandContext(` in server production code. Ensure the forbidden scan includes:

```java
                "new CommandContext(",
                "serverSessionOrNull(",
                "dbIndexProviderOrNull(",
                "connectionStatsOrNull("
```

Run:

```bash
mvn -pl yierdis-app/yierdis-server-app,yierdis-architecture-tests test
```

Expected: PASS.

- [x] **Step 4: Commit**

```bash
git add yierdis-app/yierdis-server-app yierdis-architecture-tests
git commit -m "test: guard server composition root boundaries"
```

---

## Task 9: Update Architecture Policy and Documentation

**Files:**
- Modify: `docs/module-architecture.md`
- Modify: `docs/request-execution-flow.md`
- Modify: `docs/main-path-walkthrough.md`
- Modify: `docs/superpowers/specs/2026-05-04-architecture-boundary-hardening-design.md`
- Modify: `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java`

- [x] **Step 1: Update docs with final request path**

In `docs/request-execution-flow.md` and `docs/main-path-walkthrough.md`, ensure the request path is documented as:

```text
Custom Protocol v1 bytes
-> CustomRequestDecoder
-> ProtocolCommandAdapter
-> ExecutionRequest
-> CommandExecutor owner-thread scheduling
-> YierdisEngine.execute(ServerSession, ExecutionRequest, ReplyWriter)
-> CommandContext(ServerSession, ReplyWriter)
-> CommandSpec parser
-> typed command handler
-> storage-api write result with MutationOutcome
-> explicit change event emission
-> ReplyWriter
```

In `docs/module-architecture.md`, document:

```text
CommandContext no longer probes optional session capabilities. Production command execution requires ServerSession. Storage write paths report MutationOutcome explicitly; YierdisChangeTracking has been removed.
```

- [x] **Step 2: Update architecture policy resource checks**

In `ArchitecturePolicyResourceTest.java`, remove any assertion that requires `YierdisChangeTracking` to be present in `architecture-policy.yml`.

Add assertions that require these strings:

```java
            Assert.assertTrue(policy.contains("archunit_structural_rules"));
            Assert.assertFalse(policy.contains("YierdisChangeTracking"));
```

In `architecture-policy.yml`, add this marker under `yierdis-architecture-tests` or at the root metadata area if one exists:

```yaml
architecture_enforcement:
  - archunit_structural_rules
  - legacy_text_guards_as_backstop
```

- [x] **Step 3: Run docs-independent architecture tests**

```bash
mvn -pl yierdis-architecture-tests test
```

Expected: PASS.

- [x] **Step 4: Commit**

```bash
git add docs yierdis-architecture-tests
git commit -m "docs: document hardened architecture boundaries"
```

---

## Task 10: Final Verification

**Files:**
- No planned source edits unless verification exposes a missed compile or test issue.

- [x] **Step 1: Search for removed optional session and tracking APIs**

Run:

```bash
rg -n "serverSessionOrNull|dbIndexProviderOrNull|connectionStatsOrNull|YierdisChangeTracking|ConnectionStatsProvider" .
```

Expected: no production references. Documentation references are acceptable only if they describe the old problem in the approved spec.

- [x] **Step 2: Run targeted modules**

```bash
mvn -pl yierdis-execution/yierdis-execution-api,yierdis-execution/yierdis-engine,yierdis-command/yierdis-command-api,yierdis-command/yierdis-command-kernel,yierdis-command/yierdis-command-defaults,yierdis-storage/yierdis-storage-api,yierdis-storage/yierdis-storage-memory,yierdis-runtime/yierdis-runtime-api,yierdis-runtime/yierdis-runtime-embedded,yierdis-app/yierdis-server-app,yierdis-architecture-tests test
```

Expected: PASS.

- [x] **Step 3: Run full test suite**

```bash
mvn test
```

Expected: PASS.

- [x] **Step 4: Commit final verification fixes**

If Step 1, Step 2, or Step 3 required code or doc fixes, commit them:

```bash
git add .
git commit -m "chore: complete architecture boundary hardening"
```

If no files changed, do not create an empty commit.

---

## Self-Review

Spec coverage:

- Build-time boundary enforcement: Tasks 1, 6, 8, 9.
- Explicit engine session contract: Task 2.
- Explicit change reporting: Tasks 3, 4, 5, 6.
- Storage facade contraction: Task 7.
- Single composition root: Task 8.
- Documentation and verification: Tasks 9 and 10.

Placeholder scan:

- The plan intentionally contains no `TBD`, no `TODO`, and no unspecified file paths.

Type consistency:

- `ServerSession.connectionStats()` returns `ConnectionStatsView`.
- `CommandContext` records mutation as booleans to avoid making execution-api depend on storage-api.
- `MutationOutcome` and `WriteResult<T>` live in storage-api.
- Command modules use `CommandSupport.recordMutation(ctx, outcome)` because command-defaults already depends on storage-api.
