# Server Session Capability Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete `ServerSession` and make command execution depend on explicit session capability interfaces.

**Architecture:** `EngineSession` remains the single runtime owner of per-connection state, but implements `DbIndexSession`, `ClientMetadataSession`, `TransactionSession`, `ConnectionStatsSession`, and `ProtocolNegotiationSession` directly. `CommandSessionCapabilities` becomes the only bundle used at command execution boundaries, while narrow consumers continue to depend on only the capability they need.

**Tech Stack:** Java 25, Maven, JUnit, Yierdis server API/core/command/runtime modules.

---

## File Structure

- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java`: removes the aggregate interface.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Session.java`: remove Javadoc reference to `ServerSession`.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`: remove `ServerSession` audience entry.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java`: delete `from(ServerSession)`.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`: remove constructors/reset overloads that accept `ServerSession`.
- Modify `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`: implement five narrow capability interfaces directly.
- Modify command/core/runtime tests and helpers currently importing or implementing `ServerSession`.
- Modify `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`: replace "ServerSession remains compatibility aggregate" guard with deletion and direct-capability guards.
- Modify docs that mention `ServerSession` as a compatibility aggregate.

Execute this plan after `docs/superpowers/plans/2026-06-05-internal-legacy-compatibility-removal.md` is complete. This plan uses the post-cleanup `RedisReplyWriter` and `RedisReplyWriterFactory` names.

### Task 1: Add Failing Architecture Guards for `ServerSession` Deletion

**Files:**
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] **Step 1: Replace aggregate-retention guard**

In `serverSessionProtocolNegotiationMustBeSplitFromGeneralSessionState`, replace the current assertion that `ServerSession.java` exists and extends all capabilities with assertions that it is gone:

```java
Path serverSessionFile = apiPackage.resolve("ServerSession.java");
Assert.assertFalse(
        "ServerSession aggregate must be deleted; use narrow session capabilities or CommandSessionCapabilities",
        Files.exists(serverSessionFile)
);
```

Keep required file assertions for:

```java
dbIndexSessionFile
clientMetadataSessionFile
transactionSessionFile
connectionStatsSessionFile
protocolNegotiationSessionFile
```

Remove the loop that checks `ServerSession` does not directly redeclare methods, because the file should no longer exist.

- [ ] **Step 2: Add import scan for deleted aggregate**

Add a scan across production and test source roots. Use `scanForForbiddenTextExcluding` so the guard does not match its own forbidden string literals:

```java
List<String> serverSessionOffenders = new ArrayList<>();
Path architectureTestFile = repoRoot.resolve(
        "yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java"
).normalize();
int scanned = 0;
scanned += scanForForbiddenTextExcluding(
        repoRoot,
        repoRoot.resolve("yierdis-server").normalize(),
        serverSessionOffenders,
        List.of(architectureTestFile),
        "import yier.bubu.redis.execution.api.ServerSession;",
        "implements ServerSession",
        "from(ServerSession",
        "CommandContext(ServerSession",
        "reset(ServerSession"
);
scanned += scanForForbiddenTextExcluding(
        repoRoot,
        repoRoot.resolve("yierdis-command").normalize(),
        serverSessionOffenders,
        List.of(architectureTestFile),
        "import yier.bubu.redis.execution.api.ServerSession;",
        "implements ServerSession",
        "from(ServerSession",
        "CommandContext(ServerSession",
        "reset(ServerSession"
);
scanned += scanForForbiddenTextExcluding(
        repoRoot,
        repoRoot.resolve("yierdis-tests").normalize(),
        serverSessionOffenders,
        List.of(architectureTestFile),
        "import yier.bubu.redis.execution.api.ServerSession;",
        "implements ServerSession",
        "from(ServerSession",
        "CommandContext(ServerSession",
        "reset(ServerSession"
);
Assert.assertTrue("ServerSession guard did not scan any Java files", scanned > 0);
if (!serverSessionOffenders.isEmpty()) {
    Assert.fail("ServerSession aggregate references remain:\n" + String.join("\n", serverSessionOffenders));
}
```

Then add a docs scan limited to project documentation:

```java
List<String> docOffenders = new ArrayList<>();
Path projectDocsRoot = repoRoot.resolve("docs/project-docs").normalize();
if (Files.exists(projectDocsRoot)) {
    try (Stream<Path> paths = Files.walk(projectDocsRoot)) {
        paths.filter(p -> p != null && p.toString().endsWith(".md"))
                .sorted()
                .forEach(p -> {
                    try {
                        String source = Files.readString(p, StandardCharsets.UTF_8);
                        if (source.contains("ServerSession")) {
                            docOffenders.add(relativePath(repoRoot, p));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
if (!docOffenders.isEmpty()) {
    Assert.fail("Project docs still describe ServerSession:\n" + String.join("\n", docOffenders));
}
```

- [ ] **Step 3: Add EngineSession direct implementation guard**

Read `EngineSession.java` and assert it implements all five narrow interfaces:

```java
Path engineSessionFile = repoRoot.resolve(
        "yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java"
).normalize();
String engineSession = Files.readString(engineSessionFile, StandardCharsets.UTF_8).replaceAll("\\s+", " ");
Assert.assertTrue(
        "EngineSession must implement narrow session capabilities directly",
        engineSession.contains("implements DbIndexSession, ClientMetadataSession, TransactionSession, ConnectionStatsSession, ProtocolNegotiationSession")
);
```

- [ ] **Step 4: Run architecture test and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest#serverSessionProtocolNegotiationMustBeSplitFromGeneralSessionState
```

Expected: FAIL because `ServerSession.java` still exists and many references remain.

- [ ] **Step 5: Commit guard-only change**

```bash
git add yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "test: require server session aggregate removal"
```

### Task 2: Delete ServerSession from Server API

**Files:**
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Session.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`
- Modify: `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/CoreContractSmokeTest.java`

- [ ] **Step 1: Delete aggregate file and package docs**

Delete:

```text
yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java
```

In `package-info.java`, remove:

```java
 *     <li>ServerSession - API. Audience: engine/session implementations that already aggregate all command session capabilities.</li>
```

In `Session.java`, replace:

```java
 * Concrete transports may expose richer session objects (e.g. server-side {@link ServerSession}).
```

with:

```java
 * Concrete transports may expose richer session objects by implementing the narrow session capability interfaces.
```

- [ ] **Step 2: Update `CommandSessionCapabilities`**

Delete:

```java
public static CommandSessionCapabilities from(ServerSession session) {
    Objects.requireNonNull(session, "session");
    return new CommandSessionCapabilities(session, session, session, session, session);
}
```

Keep these existing methods:

```java
public static CommandSessionCapabilities from(Session session) {
    if (!(session instanceof DbIndexSession dbIndexSession)
            || !(session instanceof ClientMetadataSession clientMetadataSession)
            || !(session instanceof TransactionSession transactionSession)
            || !(session instanceof ConnectionStatsSession connectionStatsSession)
            || !(session instanceof ProtocolNegotiationSession protocolNegotiationSession)) {
        throw new IllegalArgumentException("YierdisEngine requires " + REQUIRED_CAPABILITIES);
    }
    return new CommandSessionCapabilities(
            dbIndexSession,
            clientMetadataSession,
            transactionSession,
            connectionStatsSession,
            protocolNegotiationSession
    );
}

public static CommandSessionCapabilities of(
        DbIndexSession dbIndexSession,
        ClientMetadataSession clientMetadataSession,
        TransactionSession transactionSession,
        ConnectionStatsSession connectionStatsSession,
        ProtocolNegotiationSession protocolNegotiationSession
) {
    return new CommandSessionCapabilities(
            dbIndexSession,
            clientMetadataSession,
            transactionSession,
            connectionStatsSession,
            protocolNegotiationSession
    );
}
```

- [ ] **Step 3: Update `CommandContext` constructors**

Remove:

```java
public CommandContext(ServerSession session, RedisReplyWriter out) {
    this(CommandSessionCapabilities.from(session), out);
}

public CommandContext reset(ServerSession session, RedisReplyWriter out) {
    return reset(CommandSessionCapabilities.from(session), out);
}
```

Keep only:

```java
public CommandContext(CommandSessionCapabilities session, RedisReplyWriter out) {
    this.session = Objects.requireNonNull(session, "session");
    this.out = Objects.requireNonNull(out, "out");
}

public CommandContext reset(CommandSessionCapabilities session, RedisReplyWriter out) {
    this.session = Objects.requireNonNull(session, "session");
    this.out = Objects.requireNonNull(out, "out");
    clearMutationOutcome();
    return this;
}
```

- [ ] **Step 4: Update server-api tests**

In `CoreContractSmokeTest`, replace anonymous `ServerSession` usage with explicit capability construction. For tests whose purpose is proving narrow capability construction, use:

```java
CommandSessionCapabilities capabilities = CommandSessionCapabilities.of(
        dbIndexSession,
        clientMetadataSession,
        transactionSession,
        connectionStatsSession,
        protocolNegotiationSession
);
CommandContext ctx = new CommandContext(capabilities, writer);
```

For tests that need a reusable mutable session, introduce a private class with the same method bodies as the old anonymous session but implementing the five real capabilities:

```java
private static final class TestCommandSession implements
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
    private int dbIndex;
    private String clientName;
    private boolean authenticated;
    private int respVersion = 2;
    private final TransactionState transaction = new TestTransactionState();

    @Override
    public int dbIndex() {
        return dbIndex;
    }

    @Override
    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public long clientId() {
        return 1L;
    }

    @Override
    public String clientName() {
        return clientName;
    }

    @Override
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    @Override
    public boolean authenticated() {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    public TransactionState transaction() {
        return transaction;
    }

    @Override
    public ConnectionStatsView connectionStats() {
        return null;
    }

    @Override
    public int respVersion() {
        return respVersion;
    }

    @Override
    public void setRespVersion(int respVersion) {
        this.respVersion = respVersion;
    }
}
```

- [ ] **Step 5: Run server-api tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api test
```

Expected: PASS.

- [ ] **Step 6: Commit API deletion**

```bash
git add yierdis-server/yierdis-server-api
git commit -m "refactor: remove server session aggregate api"
```

### Task 3: Make EngineSession Implement Narrow Capabilities Directly

**Files:**
- Modify: `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`
- Modify: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
- Modify: `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java`

- [ ] **Step 1: Update imports and implements clause**

In `EngineSession.java`, remove:

```java
import yier.bubu.redis.execution.api.ServerSession;
```

Add:

```java
import yier.bubu.redis.execution.api.ClientMetadataSession;
import yier.bubu.redis.execution.api.ConnectionStatsSession;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.execution.api.ProtocolNegotiationSession;
```

`TransactionSession` may already be indirectly referenced; import it explicitly:

```java
import yier.bubu.redis.execution.api.TransactionSession;
```

Change:

```java
public final class EngineSession implements ServerSession {
```

to:

```java
public final class EngineSession implements
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
```

Do not change fields or method bodies.

- [ ] **Step 2: Update engine tests**

Search server-core tests:

```bash
rg -n 'ServerSession|new CommandContext\\(' yierdis-server/yierdis-server-core/src/test/java
```

Replace any `ServerSession` typed variable with `EngineSession`, `Session`, or the specific capability interface used by that assertion.

For contexts, use:

```java
new CommandContext(CommandSessionCapabilities.from(session), out)
```

where `session` is an `EngineSession`.

- [ ] **Step 3: Run server-core tests and architecture guard**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-core test
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest#serverSessionProtocolNegotiationMustBeSplitFromGeneralSessionState
```

Expected: server-core PASS; architecture may still fail until test helpers are updated, but the `EngineSession` direct implementation assertion should pass.

- [ ] **Step 4: Commit**

```bash
git add yierdis-server/yierdis-server-core
git commit -m "refactor: implement session capabilities directly"
```

### Task 4: Update Command-Core Test Sessions and Context Construction

**Files:**
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/TestCommandContexts.java`
- Modify: `yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java`
- Modify: any other `yierdis-command/yierdis-command-core/src/test/java` file importing `ServerSession`

- [ ] **Step 1: Replace `TestCommandContexts` helper**

In `TestCommandContexts.java`, remove `ServerSession` import. Make `TestSession` implement the five narrow interfaces:

```java
private static final class TestSession implements
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
    private int dbIndex;
    private String clientName;
    private boolean authenticated;
    private int respVersion = 2;
    private final TransactionState transaction = new TestTransactionState();

    @Override
    public int dbIndex() {
        return dbIndex;
    }

    @Override
    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public long clientId() {
        return 1L;
    }

    @Override
    public String clientName() {
        return clientName;
    }

    @Override
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    @Override
    public boolean authenticated() {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    public TransactionState transaction() {
        return transaction;
    }

    @Override
    public ConnectionStatsView connectionStats() {
        return null;
    }

    @Override
    public int respVersion() {
        return respVersion;
    }

    @Override
    public void setRespVersion(int respVersion) {
        this.respVersion = respVersion;
    }
}
```

Change context construction to:

```java
return new CommandContext(CommandSessionCapabilities.from(new TestSession()), out);
```

or:

```java
TestSession session = new TestSession();
return new CommandContext(CommandSessionCapabilities.from(session), out);
```

- [ ] **Step 2: Update `YierdisFastCommandProcessorPolicyTest`**

Remove `ServerSession` import. Change private `TestSession implements ServerSession` to implement the five narrow interfaces directly.

Replace all:

```java
new CommandContext(session, out)
```

with:

```java
new CommandContext(CommandSessionCapabilities.from(session), out)
```

Add import:

```java
import yier.bubu.redis.execution.api.CommandSessionCapabilities;
```

- [ ] **Step 3: Run command-core tests**

Run:

```bash
mvn -pl yierdis-command/yierdis-command-core test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-command/yierdis-command-core
git commit -m "test: use explicit session capabilities in command core"
```

### Task 5: Update Runtime and Integration Test Helpers

**Files:**
- Modify: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- Modify: integration tests with private `TestSession implements ServerSession`

- [ ] **Step 1: Update runtime `FastTestClient`**

Remove `ServerSession` import.

Change field and constructor from:

```java
private final ServerSession session;

public FastTestClient(YierdisFastCommandProcessor processor, ServerSession session) {
```

to:

```java
private final CommandSessionCapabilities sessionCapabilities;

public FastTestClient(YierdisFastCommandProcessor processor, Session session) {
    this.processor = Objects.requireNonNull(processor, "processor");
    this.sessionCapabilities = CommandSessionCapabilities.from(session);
}
```

If the constructor currently accepts only sessions implementing all capabilities, this preserves the runtime check.

Change command execution:

```java
processor.execute(request, new CommandContext(sessionCapabilities, writer));
```

Change `DefaultTestSession implements ServerSession` to implement the five narrow interfaces directly.

- [ ] **Step 2: Update integration `FastTestClient`**

Apply the same changes in:

```text
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java
```

- [ ] **Step 3: Update private integration test sessions**

For each current match:

```text
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/ContractsIntegrationSmokeTest.java
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisChangeEmissionTest.java
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TransactionCommandTest.java
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java
```

Replace:

```java
private static final class TestSession implements ServerSession
```

with:

```java
private static final class TestSession implements
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession
```

Replace any `new CommandContext(session, out)` with:

```java
new CommandContext(CommandSessionCapabilities.from(session), out)
```

- [ ] **Step 4: Run runtime and integration tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-runtime test
mvn -pl yierdis-tests/yierdis-integration-tests test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/yierdis-server-runtime yierdis-tests/yierdis-integration-tests
git commit -m "test: replace server session aggregate in runtime helpers"
```

### Task 6: Update Remaining References and Docs

**Files:**
- Modify: `docs/project-docs/*.md`
- Scan: `README.md` for `ServerSession`; current expected result is no matches and no README edit.
- Modify: any remaining source docs/Javadocs under `yierdis-*`

- [ ] **Step 1: Scan for remaining aggregate references**

Run:

```bash
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'ServerSession|from\\(ServerSession|CommandContext\\(ServerSession|implements ServerSession|import yier\\.bubu\\.redis\\.execution\\.api\\.ServerSession;' yierdis-* docs/project-docs README.md
```

Expected: only architecture test messages may remain during this task. No production/test imports or implementations should remain.

- [ ] **Step 2: Update docs wording**

Replace docs that describe `ServerSession` with capability wording. Use this form:

```text
Connection state is exposed through narrow session capability interfaces:
DbIndexSession, ClientMetadataSession, TransactionSession,
ConnectionStatsSession, and ProtocolNegotiationSession. Command execution
bundles the required set with CommandSessionCapabilities.
```

Remove descriptions saying `ServerSession` remains as a compatibility aggregate.

- [ ] **Step 3: Run architecture tests**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 4: Commit docs and guards**

```bash
git add docs/project-docs README.md yierdis-tests/yierdis-architecture-tests
git commit -m "docs: document explicit session capabilities"
```

### Task 7: Final Full Verification

**Files:**
- No planned edits. If verification reveals a missed reference, fix only the file reported by the failing test or forbidden-symbol scan.

- [ ] **Step 1: Run full required module tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-server/yierdis-server-core test
mvn -pl yierdis-command/yierdis-command-core test
mvn -pl yierdis-server/yierdis-server-runtime test
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-integration-tests test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 2: Run final forbidden-symbol scan**

Run:

```bash
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ServerSession.java
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'import yier\\.bubu\\.redis\\.execution\\.api\\.ServerSession;|implements ServerSession|from\\(ServerSession|CommandContext\\(ServerSession|reset\\(ServerSession' yierdis-* docs/project-docs README.md
```

Expected: `test` succeeds and `rg` prints no forbidden references.

- [ ] **Step 3: Commit final fixes only when needed**

When verification required a concrete fix, commit that fix:

```bash
git add yierdis-* docs/project-docs README.md
git commit -m "refactor: finish server session capability split"
```

When verification did not require a fix, do not create an empty commit.
