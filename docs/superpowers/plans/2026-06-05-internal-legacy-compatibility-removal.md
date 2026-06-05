# Internal Legacy Compatibility Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove internal historical compatibility aliases and deprecated Java API overloads while preserving Redis wire/client behavior.

**Architecture:** Replace compatibility names with canonical execution and storage interfaces: `ExecutionRequest`, `RedisReplyWriter`, `RedisReplyWriterFactory`, `length()`, `getByte()`, and `MaxmemoryPolicy`. Keep all RESP/Redis compatibility behavior intact; this plan is API cleanup and documentation/test guard cleanup only.

**Tech Stack:** Java 25, Maven, JUnit, Netty, Yierdis multi-module Maven build.

---

## File Structure

- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java`: removes the deprecated request alias.
- Delete `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java`: removes the reply writer compatibility alias.
- Rename `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java` to `RedisReplyWriterFactory.java`: makes the factory name match `RedisReplyWriter`.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriter.java`: no behavior changes; keep as the canonical reply model.
- Modify `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`: use `RedisReplyWriter` for `out`.
- Modify all command, engine, executor, server, networking, CLI test helper, and integration test sources that import or implement `ReplyWriter` / `ReplyWriterFactory`: move to `RedisReplyWriter` / `RedisReplyWriterFactory`.
- Modify `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java`: implement `RedisReplyWriter`.
- Modify `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java`: implement `RedisReplyWriterFactory`.
- Modify `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java`: remove `len()` and `byteAt(int)` default aliases.
- Modify `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/KeyHandle.java`: remove `len()` and `byteAt(int)` and rely on `BytesView.length()` / `getByte(int)`.
- Modify `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`: remove duplicate `len()` and `byteAt(int)`.
- Modify `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/AllocatorKeyHandle.java`: implement `length()` and `getByte(int)`.
- Modify DB-memory production and tests that call `KeyHandle.len()` or `BytesView.len()`: use `length()` / `getByte(int)`.
- Modify `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`: delete deprecated `String maxmemoryPolicy` overloads and helper.
- Modify `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java`: delete deprecated `Builder.maxmemoryPolicy(String)`.
- Modify tests currently using string maxmemory policy overloads: parse strings at the test boundary or use enum constants directly.
- Delete `yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java`: remove forwarding wrapper.
- Modify `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`: import shared RESP `InlineCommandParser` directly.
- Modify `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`: assert deleted aliases and overloads stay deleted.
- Modify `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java`: extend byte-view/key-handle guards.
- Modify `docs/project-docs/*`, `docs/project-docs/glossary.md`, and test helper Javadocs that mention `Command`, `ReplyWriter`, or compatibility aliases.

### Task 1: Remove `Command` Alias

**Files:**
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`
- Modify: `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java`

- [ ] **Step 1: Add/adjust architecture guard for deleted `Command.java`**

In `ArchitectureBoundaryTest`, update `productionCodeMustNotUseDeprecatedCommandRequestCompatibility` so it also asserts the file is gone:

```java
Path commandAlias = repoRoot.resolve(
        "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java"
).normalize();
Assert.assertFalse(
        "Command compatibility alias must be deleted; use ExecutionRequest directly",
        Files.exists(commandAlias)
);
```

Keep the existing forbidden import scans for:

```java
"import yier.bubu.redis.execution.api.Command;",
"instanceof yier.bubu.redis.execution.api.Command",
"execute(Command"
```

- [ ] **Step 2: Run architecture test and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest#productionCodeMustNotUseDeprecatedCommandRequestCompatibility
```

Expected: FAIL because `Command.java` still exists.

- [ ] **Step 3: Delete the alias and update docs/comments**

Delete `Command.java`.

In `package-info.java`, remove this list item:

```java
 *     <li>Command - compatibility/deprecated. Audience: legacy embedders only; new code uses ExecutionRequest.</li>
```

In both `FastTestClient.java` files, update Javadocs from:

```java
以协议无关的 {@link Command}/{@link ReplyWriter} 语义执行命令
```

to the current canonical wording, adjusted for Task 2 if it has not yet run:

```java
以协议无关的 {@link yier.bubu.redis.execution.api.ExecutionRequest} /
{@link yier.bubu.redis.execution.api.RedisReplyWriter} 语义执行命令
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest#productionCodeMustNotUseDeprecatedCommandRequestCompatibility
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java \
  yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java
git commit -m "refactor: remove command request compatibility alias"
```

### Task 2: Rename Reply Writer Boundary

**Files:**
- Delete: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java`
- Rename: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java` -> `RedisReplyWriterFactory.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandContext.java`
- Modify: `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/package-info.java`
- Modify: every source importing `yier.bubu.redis.execution.api.ReplyWriter` or `ReplyWriterFactory`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: docs under `docs/project-docs`

- [ ] **Step 1: Strengthen architecture guard for reply writer aliases**

In `replyWriterMustBeDocumentedAsRedisReplyModelAndKeepProtocolOutOfCommand`, replace the current `ReplyWriter` alias assertion with deletion assertions:

```java
Path replyWriterFile = apiPackage.resolve("ReplyWriter.java");
Assert.assertFalse(
        "ReplyWriter compatibility alias must be deleted; use RedisReplyWriter",
        Files.exists(replyWriterFile)
);

Path replyWriterFactoryFile = apiPackage.resolve("ReplyWriterFactory.java");
Assert.assertFalse(
        "ReplyWriterFactory compatibility name must be deleted; use RedisReplyWriterFactory",
        Files.exists(replyWriterFactoryFile)
);

Path redisReplyWriterFactoryFile = apiPackage.resolve("RedisReplyWriterFactory.java");
Assert.assertTrue("缺少 RedisReplyWriterFactory.java", Files.isRegularFile(redisReplyWriterFactoryFile));
```

Update execution boundary assertions around `YierdisEngine` and `CommandExecutionEngine` from `ReplyWriter out` to `RedisReplyWriter out`.

- [ ] **Step 2: Run architecture test and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest#replyWriterMustBeDocumentedAsRedisReplyModelAndKeepProtocolOutOfCommand
```

Expected: FAIL because `ReplyWriter.java` and `ReplyWriterFactory.java` still exist and `RedisReplyWriterFactory.java` does not.

- [ ] **Step 3: Rename factory and update server API**

Use git-aware rename:

```bash
git mv yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java \
  yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RedisReplyWriterFactory.java
```

Edit `RedisReplyWriterFactory.java` to:

```java
package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

import java.util.Objects;

/**
 * Factory for creating wire-specific {@link RedisReplyWriter} instances that encode the Redis reply model.
 * <p>
 * This indirection allows the server/executor to remain decoupled from a concrete wire protocol implementation.
 */
@FunctionalInterface
public interface RedisReplyWriterFactory {
    RedisReplyWriter newWriter(BytesSink out);

    default RedisReplyWriter newWriter(Session session, BytesSink out) {
        return newWriter(Objects.requireNonNull(out, "out"));
    }
}
```

Delete `ReplyWriter.java`.

In `CommandContext.java`, replace all `ReplyWriter` field, constructor, reset, and `out()` return types with `RedisReplyWriter`.

In `package-info.java`, remove the `ReplyWriter` list item and replace the factory item with:

```java
 *     <li>RedisReplyWriterFactory - API. Audience: executor, server/protocol adapter composition, tests.</li>
```

- [ ] **Step 4: Mechanically update imports and type names**

For production and tests, replace:

```java
import yier.bubu.redis.execution.api.ReplyWriter;
```

with:

```java
import yier.bubu.redis.execution.api.RedisReplyWriter;
```

Replace type uses `ReplyWriter` with `RedisReplyWriter`.

Replace:

```java
import yier.bubu.redis.execution.api.ReplyWriterFactory;
```

with:

```java
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
```

Replace type uses `ReplyWriterFactory` with `RedisReplyWriterFactory`.

Current source files with `ReplyWriter` / `ReplyWriterFactory` imports are:

```text
yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/ServerInfoProvider.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/BulkStringReplyAdapter.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/CommandSupport.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hash/HashCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/hll/HllCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/list/ListCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/set/SetCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/string/StringCommands.java
yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/zset/ZSetCommands.java
yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandExceptionTranslator.java
yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionCommands.java
yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/TransactionQueuePolicy.java
yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java
yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/TestCommandContexts.java
yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorModuleTest.java
yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorPolicyTest.java
yierdis-command/yierdis-command-core/src/test/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessorRegistrationTest.java
yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java
yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriter.java
yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java
yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactoryTest.java
yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java
yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java
yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/DefaultYierdisEngineTest.java
yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionEngine.java
yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java
yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java
yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorCoreTestSupport.java
yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java
yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/ServerCommandModule.java
yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java
yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java
yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java
yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java
yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/testutil/FastTestClient.java
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/FastTestClient.java
```

Update `RespReplyWriter.java`:

```java
public final class RespReplyWriter implements RedisReplyWriter {
```

Update `RespReplyWriterFactory.java`:

```java
public final class RespReplyWriterFactory implements RedisReplyWriterFactory {
    @Override
    public RedisReplyWriter newWriter(BytesSink out) {
        return newWriter(null, out);
    }

    @Override
    public RedisReplyWriter newWriter(Session session, BytesSink out) {
        if (!(session instanceof ProtocolNegotiationSession protocolSession)) {
            return new RespReplyWriter(Objects.requireNonNull(out, "out"), RespProtocolVersion.RESP2);
        }
        return new RespReplyWriter(Objects.requireNonNull(out, "out"), protocolSession::respVersion);
    }
}
```

- [ ] **Step 5: Update docs and test helper Javadocs**

Run:

```bash
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'ReplyWriter|ReplyWriterFactory' docs/project-docs yierdis-*
```

Replace user-facing compatibility references with `RedisReplyWriter` / `RedisReplyWriterFactory`. Keep class names such as `RespReplyWriter` unchanged.

Examples:

- `ReplyWriter 是兼容别名` becomes `RedisReplyWriter 是命令层唯一 Redis reply 语义模型`.
- `ReplyWriterFactory` becomes `RedisReplyWriterFactory`.

- [ ] **Step 6: Run focused compile/tests**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-command test
mvn -pl yierdis-networking test
mvn -pl yierdis-server/yierdis-server-core test
mvn -pl yierdis-server/yierdis-server-executor test
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 7: Verify deleted names are gone**

Run:

```bash
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'import yier\\.bubu\\.redis\\.execution\\.api\\.ReplyWriter;|import yier\\.bubu\\.redis\\.execution\\.api\\.ReplyWriterFactory;' yierdis-* docs/project-docs
```

Expected: first two commands succeed; `rg` returns no output.

- [ ] **Step 8: Commit**

```bash
git add yierdis-server yierdis-command yierdis-networking yierdis-tests docs/project-docs
git commit -m "refactor: make redis reply writer canonical"
```

### Task 3: Remove Byte View and KeyHandle Legacy Methods

**Files:**
- Modify: `yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java`
- Modify: `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/KeyHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/AllocatorKeyHandle.java`
- Modify: all DB-memory production/tests that call `KeyHandle.len()`, `KeyHandle.byteAt()`, `BytesView.len()`, or `BytesView.byteAt()`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/storage/memory/YierdisDbArchitectureGuardTest.java` or `ArchitectureBoundaryTest.java`

- [ ] **Step 1: Add architecture guard for deleted byte-view aliases**

Add a test that reads these files and asserts the old one-argument APIs are absent:

```java
Path bytesView = repoRoot.resolve(
        "yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java"
).normalize();
Path storageKeyHandle = repoRoot.resolve(
        "yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/KeyHandle.java"
).normalize();
Path memoryKeyHandle = repoRoot.resolve(
        "yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java"
).normalize();
for (Path file : List.of(bytesView, storageKeyHandle, memoryKeyHandle)) {
    String source = Files.readString(file, StandardCharsets.UTF_8);
    Assert.assertFalse(relativePath(repoRoot, file) + " must not expose legacy len()", source.contains(" len()"));
    Assert.assertFalse(relativePath(repoRoot, file) + " must not expose legacy byteAt(int)", source.contains(" byteAt(int"));
}
```

Do not assert against `ExecutionRequest.byteAt(int index, int offset)`.

- [ ] **Step 2: Run guard and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=YierdisDbArchitectureGuardTest
```

Expected: FAIL because the old methods still exist.

- [ ] **Step 3: Remove aliases from `BytesView`**

In `BytesView.java`, remove:

```java
default int len() {
    return length();
}

default byte byteAt(int index) {
    return getByte(index);
}
```

Keep `length()`, `getByte(int)`, and the existing default `getBytes(int index, byte[] dst, int dstOff, int len)` implementation.

- [ ] **Step 4: Rewrite storage `KeyHandle`**

In `yierdis-db-api` `KeyHandle.java`, remove:

```java
int len();
byte byteAt(int index);
default int length() {
    return len();
}
default byte getByte(int index) {
    return byteAt(index);
}
```

The interface should become:

```java
public interface KeyHandle extends BytesView {
    /**
     * Storage-local dictionary hash. It is stable only inside the owning keyspace.
     */
    int dictHash();
}
```

In memory internal `KeyHandle.java`, remove duplicate `len()` / `byteAt(int)` declarations and default bridge methods. Keep `dictHash()` and `forNative(NativeAllocator allocator, NativeHandle handle, int dictHash)`.

- [ ] **Step 5: Update `AllocatorKeyHandle`**

Replace:

```java
public int len()
public byte byteAt(int index)
```

with:

```java
@Override
public int length() {
    try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
        return view.size();
    }
}

@Override
public byte getByte(int index) {
    try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
        if (index < 0 || index >= view.size()) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + view.size());
        }
        return view.getByte(index);
    }
}
```

Update equality logic:

```java
int len = length();
if (other.length() != len) {
    return false;
}
for (int i = 0; i < len; i++) {
    if (getByte(i) != other.getByte(i)) {
        return false;
    }
}
```

- [ ] **Step 6: Update DB-memory callers**

For `BytesView` variables, replace:

```java
view.len()
view.byteAt(i)
```

with:

```java
view.length()
view.getByte(i)
```

For `KeyHandle` variables, replace:

```java
keyHandle.len()
keyHandle.byteAt(i)
stored.len()
stored.byteAt(i)
```

with:

```java
keyHandle.length()
keyHandle.getByte(i)
stored.length()
stored.getByte(i)
```

Concrete files from the current scan include:

```text
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbKeyLifecycle.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java
yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/YierdisGlobMatcher.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/key/KeyHandleContractTest.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectoryTest.java
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java
yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernorTest.java
```

Do not change `ExecutionRequest.len(int)` or `ExecutionRequest.byteAt(int,int)` in command parser/request code.

- [ ] **Step 7: Run DB and architecture tests**

Run:

```bash
mvn -pl yierdis-common/yierdis-common-bytes test
mvn -pl yierdis-db/yierdis-db-api test
mvn -pl yierdis-db/yierdis-db-memory test
mvn -pl yierdis-server/yierdis-server-runtime test -Dtest=YierdisGlobalMaxmemoryGovernorTest
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 8: Verify only request-model byteAt/len remain**

Run:

```bash
rg -n --glob '!**/target/**' '\\.len\\(\\)|\\.byteAt\\(' yierdis-common yierdis-db yierdis-server yierdis-command yierdis-networking yierdis-cli yierdis-tests
```

Expected: remaining `.len()` / `.byteAt()` occurrences are either `ExecutionRequest` request-model calls or domain-specific methods such as `StringRoot.byteAt(handle, byteIndex)`, not `BytesView`/`KeyHandle` one-argument legacy calls.

- [ ] **Step 9: Commit**

```bash
git add yierdis-common yierdis-db yierdis-server/yierdis-server-runtime yierdis-tests/yierdis-architecture-tests
git commit -m "refactor: remove legacy byte view aliases"
```

### Task 4: Remove String Maxmemory Policy Overloads

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- Modify: `yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/TestDbs.java`
- Modify: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-server/yierdis-server-runtime-api/src/test/java/yier/bubu/redis/runtime/api/YierdisChangeSinkTest.java`
- Modify: architecture guards

- [ ] **Step 1: Add architecture guard for deleted overloads**

Add assertions that production API files no longer expose string overloads:

```java
Path yierdisDb = repoRoot.resolve(
        "yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java"
).normalize();
String dbSource = Files.readString(yierdisDb, StandardCharsets.UTF_8);
Assert.assertFalse("YierdisDb must not keep String maxmemoryPolicy overloads", dbSource.contains("String maxmemoryPolicy"));
Assert.assertFalse("YierdisDb must not keep compatibilityMaxmemoryPolicy", dbSource.contains("compatibilityMaxmemoryPolicy"));

Path instanceConfig = repoRoot.resolve(
        "yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java"
).normalize();
String configSource = Files.readString(instanceConfig, StandardCharsets.UTF_8);
Assert.assertFalse("YierdisInstanceConfig.Builder must not keep String maxmemoryPolicy overload", configSource.contains("maxmemoryPolicy(String"));
```

- [ ] **Step 2: Run guard and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest
```

Expected: FAIL because overloads still exist.

- [ ] **Step 3: Delete deprecated overloads**

In `YierdisDb.java`, delete:

```java
private static MaxmemoryPolicy compatibilityMaxmemoryPolicy(String policy) {
    if (policy == null || policy.isBlank()) {
        return MaxmemoryPolicy.NOEVICTION;
    }
    return MaxmemoryPolicy.parse(policy);
}
```

Delete both deprecated overloads accepting `String maxmemoryPolicy`.

In `YierdisInstanceConfig.java`, delete:

```java
@Deprecated
public Builder maxmemoryPolicy(String rawPolicy) {
    if (rawPolicy == null || rawPolicy.isBlank()) {
        this.maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
    } else {
        this.maxmemoryPolicy = MaxmemoryPolicy.parse(rawPolicy);
    }
    return this;
}
```

- [ ] **Step 4: Update tests and helpers**

In `TestDbs.java`, change helper signature from:

```java
public static void forEachDbWithMaxmemory(long maxmemoryBytes, String maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test)
```

to:

```java
public static void forEachDbWithMaxmemory(long maxmemoryBytes, MaxmemoryPolicy maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test)
```

If a caller still passes a string, convert at the caller:

```java
MaxmemoryPolicy.parse("allkeys-random")
```

In `YierdisInstanceTest`, replace:

```java
.maxmemoryPolicy("ALLKEYS_RANDOM")
```

with:

```java
.maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_RANDOM)
```

Remove or rewrite tests whose sole purpose was verifying blank string defaults. The internal builder no longer accepts strings; external CLI parsing still tests strings in server args tests.

In `YierdisChangeSinkTest`, replace:

```java
YierdisInstanceConfig.builder().maxmemoryPolicy("allkeys-lru").build().maxmemoryPolicy()
```

with:

```java
YierdisInstanceConfig.builder().maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU).build().maxmemoryPolicy()
```

- [ ] **Step 5: Run tests**

Run:

```bash
mvn -pl yierdis-db/yierdis-db-memory test
mvn -pl yierdis-server/yierdis-server-runtime-api test
mvn -pl yierdis-tests/yierdis-integration-tests test -Dtest=YierdisInstanceTest,YierdisChangeEmissionTest,TransactionCommandTest
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 6: Verify string overloads are gone**

Run:

```bash
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'maxmemoryPolicy\\(String|String maxmemoryPolicy|compatibilityMaxmemoryPolicy|maxmemoryPolicy\\(\"' yierdis-db yierdis-server/yierdis-server-runtime-api yierdis-tests
```

Expected: no output. External `String maxmemoryPolicy` fields in server args remain allowed and are intentionally outside this internal API scan.

- [ ] **Step 7: Commit**

```bash
git add yierdis-db yierdis-server/yierdis-server-runtime-api yierdis-tests
git commit -m "refactor: remove string maxmemory policy overloads"
```

### Task 5: Remove CLI Inline Parser Wrapper

**Files:**
- Delete: `yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java`
- Modify: `yierdis-cli/src/main/java/yier/bubu/redis/app/client/YierdisCli.java`
- Scan: `yierdis-cli/src/test/java` for `yier.bubu.redis.app.client.InlineCommandParser`; current expected result is no matches and no CLI test edit.
- Modify: architecture guard

- [ ] **Step 1: Add guard for deleted wrapper**

Add an architecture assertion:

```java
Path cliWrapper = repoRoot.resolve(
        "yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java"
).normalize();
Assert.assertFalse(
        "CLI InlineCommandParser wrapper must be deleted; use protocol.resp.InlineCommandParser",
        Files.exists(cliWrapper)
);
```

- [ ] **Step 2: Run guard and verify it fails**

Run:

```bash
mvn -pl yierdis-tests/yierdis-architecture-tests test -Dtest=ArchitectureBoundaryTest
```

Expected: FAIL because the wrapper still exists.

- [ ] **Step 3: Delete wrapper and update CLI import**

Delete:

```text
yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java
```

In `YierdisCli.java`, add:

```java
import yier.bubu.redis.protocol.resp.InlineCommandParser;
```

Keep:

```java
return InlineCommandParser.splitUtf8(line, RespProtocolLimits.DEFAULT_MAX_ARGS);
```

This should now resolve to the shared RESP parser.

Run:

```bash
rg -n --glob '!**/target/**' 'yier\\.bubu\\.redis\\.app\\.client\\.InlineCommandParser' yierdis-cli/src/test/java
```

Expected: no output.

- [ ] **Step 4: Run CLI and parser tests**

Run:

```bash
mvn -pl yierdis-cli test
mvn -pl yierdis-networking/yierdis-networking-resp test -Dtest=InlineCommandParserTest
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-cli yierdis-tests/yierdis-architecture-tests
git commit -m "refactor: remove cli inline parser wrapper"
```

### Task 6: Final Documentation and Full Verification

**Files:**
- Scan: `README.md` for deleted internal aliases; current expected result is no matches and no README edit.
- Modify: `docs/project-docs/*.md`
- Modify: `docs/project-docs/glossary.md`
- Modify: `docs/project-docs/core-logic-index.md`

- [ ] **Step 1: Search docs for stale compatibility names**

Run:

```bash
rg -n 'ReplyWriter|ReplyWriterFactory|Command - compatibility|Command compatibility|BytesView.*len|byteAt\\(i\\)|KeyHandle.*len|String maxmemoryPolicy|InlineCommandParser.*wrapper|兼容别名|compatibility alias' README.md docs/project-docs yierdis-*/src/test yierdis-*/src/main
```

Expected: output identifies any remaining references to deleted compatibility surfaces. Mentions of `RespReplyWriter`, `RedisReplyWriter`, `ExecutionRequest`, `ExecutionRequest.byteAt`, and external Redis compatibility are allowed.

- [ ] **Step 2: Update docs**

Make these wording changes where present:

```text
ReplyWriter 是兼容别名
```

to:

```text
RedisReplyWriter 是命令层唯一 Redis reply 语义接口
```

Change:

```text
ReplyWriterFactory
```

to:

```text
RedisReplyWriterFactory
```

Remove references to `Command` as a deprecated compatibility alias. Keep `ExecutionRequest` as the request model.

- [ ] **Step 3: Run full required verification**

Run:

```bash
mvn -pl yierdis-server/yierdis-server-api test
mvn -pl yierdis-command test
mvn -pl yierdis-networking test
mvn -pl yierdis-cli test
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-integration-tests test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Expected: PASS.

- [ ] **Step 4: Final stale-symbol scan**

Run:

```bash
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriter.java
test ! -f yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReplyWriterFactory.java
test ! -f yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'import yier\\.bubu\\.redis\\.execution\\.api\\.Command;|import yier\\.bubu\\.redis\\.execution\\.api\\.ReplyWriter;|import yier\\.bubu\\.redis\\.execution\\.api\\.ReplyWriterFactory;' yierdis-* docs/project-docs README.md
rg -n --glob '!**/target/**' --glob '!**/ArchitectureBoundaryTest.java' 'maxmemoryPolicy\\(String|String maxmemoryPolicy|compatibilityMaxmemoryPolicy' yierdis-db yierdis-server/yierdis-server-runtime-api yierdis-tests
```

Expected: `test` commands succeed; both `rg` commands print no forbidden internal API matches.

- [ ] **Step 5: Commit final docs and guard adjustments**

```bash
git add README.md docs/project-docs yierdis-tests
git commit -m "docs: align internal compatibility cleanup references"
```
