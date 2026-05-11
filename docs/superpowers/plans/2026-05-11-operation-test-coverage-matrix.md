# Operation Test Coverage Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first implementation slice for operation test coverage: a maintained matrix document, registry-backed matrix guards, shared reply assertions, and a String/Bitmap template test that later command families can copy.

**Architecture:** Keep production code unchanged. Put coverage inventory in `docs/project-docs/operation-test-coverage-matrix.md`, then add tests that compare the matrix against registered command metadata from the default command processor and server-only command module. Add small test-only helpers under the existing `yierdis.testutil` package and prove their shape with focused String/Bitmap coverage tests.

**Tech Stack:** Java 25, Maven, JUnit 4, Markdown docs, `YierdisFastCommandProcessor`, `CommandRegistry`, `FastTestClient`, `ServerCommandModule`.

---

## Scope

This plan implements the coverage-matrix foundation only. It does not fill every missing domain test in this first pass. After this plan lands, later plans can fill list, hash, set, zset, hll, keyspace, TTL, memory, object, transaction, server, DB API, and native-structure gaps one family at a time while the matrix guard prevents command inventory drift.

## File Structure

- Create `docs/project-docs/operation-test-coverage-matrix.md`: canonical coverage matrix with every currently registered command plus DB API and native/internal inventory sections.
- Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java`: default command processor matrix guard and status vocabulary guard.
- Create `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java`: server-only command module matrix guard.
- Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/ReplyAssertions.java`: small JUnit assertions for protocol-neutral reply objects.
- Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/StringBitmapOperationCoverageTest.java`: template command-layer tests that use `ReplyAssertions`.
- Modify `docs/project-docs/testing-and-debugging.md`: link the new matrix into the existing testing guide and document the guard command.

---

### Task 1: Add Registry-Backed Matrix Guard Tests

**Files:**
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java`
- Create: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java`

- [ ] **Step 1: Write the failing integration matrix guard**

Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java` with this content:

```java
package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class OperationCoverageMatrixTest {
    private static final Path MATRIX = Path.of("docs/project-docs/operation-test-coverage-matrix.md");
    private static final Pattern COMMAND_HEADING = Pattern.compile("(?m)^### ([A-Z][A-Z0-9-]*)$");
    private static final Pattern STATUS_LINE = Pattern.compile(
            "^- \\*\\*(Command layer|DB API|Native internals)\\*\\*: `([^`]+)` - .+$"
    );
    private static final Set<String> VALID_STATUSES = Set.of(
            "covered",
            "covered-by-shared-test",
            "missing",
            "not-applicable"
    );

    @Test
    public void matrixContainsEveryRegisteredDefaultCommand() {
        String matrix = readMatrix();
        Set<String> headings = commandHeadings(matrix);

        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            CommandRegistry registry = registryOf(processor);
            for (String upperName : registry.upperNamesSorted()) {
                Assert.assertTrue(
                        "missing matrix entry for registered command " + upperName,
                        headings.contains(upperName)
                );
            }
        });
    }

    @Test
    public void matrixRowsUseKnownStatusVocabulary() {
        List<String> invalid = invalidStatusLines(readMatrix());
        Assert.assertTrue("invalid matrix status lines: " + invalid, invalid.isEmpty());
    }

    @Test
    public void stringAndBitmapRowsStartAsConcreteTemplate() {
        String matrix = readMatrix();

        assertRowStatus(matrix, "SET", "Command layer", "covered");
        assertRowStatus(matrix, "GET", "Command layer", "covered");
        assertRowStatus(matrix, "STRLEN", "Command layer", "covered");
        assertRowStatus(matrix, "APPEND", "Command layer", "covered");
        assertRowStatus(matrix, "SETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "GETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "BITCOUNT", "Command layer", "covered");
        assertRowStatus(matrix, "INCR", "Command layer", "covered");
        assertRowStatus(matrix, "DECR", "Command layer", "covered");
    }

    private static String readMatrix() {
        Assert.assertTrue("missing coverage matrix file: " + MATRIX, Files.isRegularFile(MATRIX));
        try {
            return Files.readString(MATRIX, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("unable to read coverage matrix " + MATRIX, e);
        }
    }

    private static Set<String> commandHeadings(String matrix) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        Matcher matcher = COMMAND_HEADING.matcher(matrix);
        while (matcher.find()) {
            out.add(matcher.group(1));
        }
        return out;
    }

    private static List<String> invalidStatusLines(String matrix) {
        ArrayList<String> invalid = new ArrayList<>();
        String[] lines = matrix.split("\\R");
        for (String line : lines) {
            if (!line.startsWith("- **Command layer**:")
                    && !line.startsWith("- **DB API**:")
                    && !line.startsWith("- **Native internals**:")) {
                continue;
            }

            Matcher matcher = STATUS_LINE.matcher(line);
            if (!matcher.matches() || !VALID_STATUSES.contains(matcher.group(2))) {
                invalid.add(line);
            }
        }
        return invalid;
    }

    private static void assertRowStatus(String matrix, String command, String layer, String expectedStatus) {
        Pattern row = Pattern.compile(
                "(?ms)^### " + Pattern.quote(command) + "$.*?^- \\*\\*" + Pattern.quote(layer)
                        + "\\*\\*: `([^`]+)` - .+$"
        );
        Matcher matcher = row.matcher(matrix);
        Assert.assertTrue("missing " + layer + " row for " + command, matcher.find());
        Assert.assertEquals(command + " " + layer, expectedStatus, matcher.group(1));
    }

    private static CommandRegistry registryOf(YierdisFastCommandProcessor processor) {
        try {
            Field field = YierdisFastCommandProcessor.class.getDeclaredField("registry");
            field.setAccessible(true);
            return (CommandRegistry) field.get(processor);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access command processor registry", e);
        }
    }
}
```

- [ ] **Step 2: Write the failing server command matrix guard**

Create `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java` with this content:

```java
package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerOperationCoverageMatrixTest {
    private static final Path MATRIX = Path.of("docs/project-docs/operation-test-coverage-matrix.md");
    private static final Pattern COMMAND_HEADING = Pattern.compile("(?m)^### ([A-Z][A-Z0-9-]*)$");

    @Test
    public void matrixContainsEveryRegisteredServerCommand() {
        CommandRegistry registry = new CommandRegistry();
        new ServerCommandModule(new TestServerInfoProvider()).register(registry);

        Set<String> headings = commandHeadings(readMatrix());
        for (String upperName : registry.upperNamesSorted()) {
            Assert.assertTrue(
                    "missing matrix entry for registered server command " + upperName,
                    headings.contains(upperName)
            );
        }
    }

    private static String readMatrix() {
        Assert.assertTrue("missing coverage matrix file: " + MATRIX, Files.isRegularFile(MATRIX));
        try {
            return Files.readString(MATRIX, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("unable to read coverage matrix " + MATRIX, e);
        }
    }

    private static Set<String> commandHeadings(String matrix) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        Matcher matcher = COMMAND_HEADING.matcher(matrix);
        while (matcher.find()) {
            out.add(matcher.group(1));
        }
        return out;
    }

    private static final class TestServerInfoProvider implements ServerInfoProvider {
        @Override
        public void info(ExecutionRequest request, CommandContext ctx) {
            ctx.out().emptyArray();
        }

        @Override
        public void stats(ExecutionRequest request, CommandContext ctx) {
            ctx.out().emptyArray();
        }
    }
}
```

- [ ] **Step 3: Run the new guards and verify the expected red state**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am \
  -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE`. The failure message must include `missing coverage matrix file: docs/project-docs/operation-test-coverage-matrix.md`.

---

### Task 2: Add The Initial Operation Coverage Matrix

**Files:**
- Create: `docs/project-docs/operation-test-coverage-matrix.md`
- Test: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java`
- Test: `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java`

- [ ] **Step 1: Create the matrix document**

Create `docs/project-docs/operation-test-coverage-matrix.md` with this content:

```markdown
# Operation Test Coverage Matrix

This document is the operation coverage inventory for command behavior, DB API behavior, and native/internal storage behavior.

Status values:

- `covered`: this layer has direct, named coverage for the operation.
- `covered-by-shared-test`: this layer is exercised through a broader cross-layer test, but does not yet have a dedicated narrow test.
- `missing`: this layer needs a direct test or a more explicit shared test reference.
- `not-applicable`: this operation does not touch that layer.

## Command Layer Coverage

### AUTH

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest.authReportsNoPasswordConfigured`.
- **DB API**: `not-applicable` - authentication currently has no DB state.
- **Native internals**: `not-applicable` - authentication currently has no native storage state.

### APPEND

- **Command layer**: `covered` - `CommandProcessorTest.stringIsBinarySafe` and `MaxmemoryDoubleReplyRegressionTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringWriteOps.append`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw string growth through native string storage.

### BITCOUNT

- **Command layer**: `covered` - `BitmapCommandTest.bitcountRangeFollowsRedisByteRangeRules`.
- **DB API**: `covered-by-shared-test` - command tests exercise full and ranged `StringReadOps.bitcount`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw byte-backed bit counting.

### CLIENT

- **Command layer**: `covered` - `CommandProcessorTest.clientMetadataCommandsAreAccepted`.
- **DB API**: `not-applicable` - client metadata lives on `ServerSession`.
- **Native internals**: `not-applicable` - client metadata has no native storage state.

### COMMAND

- **Command layer**: `covered` - `CommandMetadataRegressionTest` and `CommandDescriptorRegistryTest`.
- **DB API**: `not-applicable` - command metadata is registry state.
- **Native internals**: `not-applicable` - command metadata has no native storage state.

### DECR

- **Command layer**: `covered` - `CommandProcessorTest` integer-string tests cover decrement semantics.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringWriteOps.incrBy` with negative delta.
- **Native internals**: `covered-by-shared-test` - command tests exercise integer-like raw string replacement.

### DEL

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest.binaryKeyIsSupportedEndToEnd`.
- **DB API**: `covered-by-shared-test` - command tests exercise `KeyspaceWriteOps.del`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native key removal.

### DISCARD

- **Command layer**: `covered` - `TransactionCommandTest`.
- **DB API**: `not-applicable` - transaction queue control does not directly mutate DB API state.
- **Native internals**: `not-applicable` - transaction queue control has no native storage state.

### ECHO

- **Command layer**: `covered-by-shared-test` - connection command coverage exercises bulk-string echoing.
- **DB API**: `not-applicable` - echo has no DB state.
- **Native internals**: `not-applicable` - echo has no native storage state.

### EXEC

- **Command layer**: `covered` - `TransactionCommandTest`.
- **DB API**: `covered-by-shared-test` - transaction tests execute queued DB operations.
- **Native internals**: `covered-by-shared-test` - transaction tests execute queued storage mutations.

### EXISTS

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest.binaryKeyIsSupportedEndToEnd`.
- **DB API**: `covered-by-shared-test` - command tests exercise `KeyspaceReadOps.existsKey`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native key lookup.

### EXPIRE

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest` and server INFO keyspace coverage.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlWriteOps.expire`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise expire index insertion.

### EXPIREAT

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlWriteOps.expireAtSeconds`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise expire index timestamp storage.

### FLUSHDB

- **Command layer**: `covered-by-shared-test` - DB lifecycle command coverage exercises default and mode parsing.
- **DB API**: `covered-by-shared-test` - command tests exercise `DbLifecycleOps.flushDb`.
- **Native internals**: `covered-by-shared-test` - lifecycle tests exercise native table clearing.

### GET

- **Command layer**: `covered` - `CommandProcessorTest.stringIsBinarySafe`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringReadOps.getStringBytes`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw string read through native key lookup.

### GETBIT

- **Command layer**: `covered` - `BitmapCommandTest.getbitSetbitBasicSemantics`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringReadOps.getBit`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw string bit addressing.

### HELLO

- **Command layer**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest` and `RespHandshakeIntegrationTest`.
- **DB API**: `not-applicable` - HELLO changes session protocol state.
- **Native internals**: `not-applicable` - HELLO has no native storage state.

### HDEL

- **Command layer**: `covered` - `HashCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HashWriteOps.hdel`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native hash value mutation.

### HGET

- **Command layer**: `covered` - `HashCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HashReadOps.hget`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native hash lookup.

### HGETALL

- **Command layer**: `covered` - `HashCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HashReadOps.hgetall`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native hash iteration.

### HLEN

- **Command layer**: `covered` - `HashCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HashReadOps.hlen`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native hash cardinality.

### HSET

- **Command layer**: `covered` - `HashCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HashWriteOps.hset`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native hash field mutation.

### INFO

- **Command layer**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest`.
- **DB API**: `covered-by-shared-test` - INFO keyspace and memory sections read DB observability state.
- **Native internals**: `covered-by-shared-test` - INFO memory coverage reads native memory reporting.

### INCR

- **Command layer**: `covered` - `CommandProcessorTest.incrWorksAfterAppendWhenRawStringHasSpareCapacity`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringWriteOps.incrBy`.
- **Native internals**: `covered-by-shared-test` - command tests exercise integer-like raw string replacement.

### KEYS

- **Command layer**: `covered` - `CommandProcessorTest.keysGlobMatchesOnRawBytes` and `CommandProcessorTest.keysGlobSupportsBracketsNegationRangesAndEscapes`.
- **DB API**: `covered-by-shared-test` - command tests exercise `KeyspaceReadOps.keys`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native key scanning and byte glob matching.

### LPOP

- **Command layer**: `covered` - `ListCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ListWriteOps.lpop`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native list head removal.

### LPUSH

- **Command layer**: `covered` - `ListCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ListWriteOps.lpush`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native list head insertion.

### LRANGE

- **Command layer**: `covered` - `ListCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ListReadOps.lrange`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native list range traversal.

### MEMORY

- **Command layer**: `covered` - `MemoryStatsCommandTest`.
- **DB API**: `covered-by-shared-test` - memory command tests exercise `MemoryOps`.
- **Native internals**: `covered-by-shared-test` - memory tests exercise native memory ledger and reporter output.

### MULTI

- **Command layer**: `covered` - `TransactionCommandTest`.
- **DB API**: `not-applicable` - MULTI only opens transaction queue state.
- **Native internals**: `not-applicable` - MULTI has no native storage state.

### OBJECT

- **Command layer**: `covered-by-shared-test` - object encoding coverage exercises command replies.
- **DB API**: `covered-by-shared-test` - object coverage reads introspection state.
- **Native internals**: `covered-by-shared-test` - object coverage reads root encoding metadata.

### PERSIST

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlWriteOps.persist`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise expire index removal.

### PEXPIRE

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlWriteOps.pexpire`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise millisecond expire index insertion.

### PEXPIREAT

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlWriteOps.expireAtMillis`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise millisecond expire timestamp storage.

### PFADD

- **Command layer**: `covered` - `HllCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HllWriteOps.pfadd`.
- **Native internals**: `covered-by-shared-test` - HLL command tests exercise raw string-backed HLL storage.

### PFCOUNT

- **Command layer**: `covered` - `HllCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HllReadOps.pfcount`.
- **Native internals**: `covered-by-shared-test` - HLL command tests exercise raw string-backed HLL reads.

### PFMERGE

- **Command layer**: `covered` - `HllCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `HllWriteOps.pfmerge`.
- **Native internals**: `covered-by-shared-test` - HLL command tests exercise raw string-backed HLL merge storage.

### PING

- **Command layer**: `covered-by-shared-test` - command processor connection coverage exercises PING.
- **DB API**: `not-applicable` - PING has no DB state.
- **Native internals**: `not-applicable` - PING has no native storage state.

### PTTL

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlReadOps.ttlMillis`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise expire index reads.

### QUIT

- **Command layer**: `covered-by-shared-test` - connection command coverage exercises close-after-reply semantics.
- **DB API**: `not-applicable` - QUIT has no DB state.
- **Native internals**: `not-applicable` - QUIT has no native storage state.

### RPOP

- **Command layer**: `covered` - `ListCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ListWriteOps.rpop`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native list tail removal.

### RPUSH

- **Command layer**: `covered` - `ListCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ListWriteOps.rpush`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native list tail insertion.

### SADD

- **Command layer**: `covered` - `SetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `SetWriteOps.sadd`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native set member insertion.

### SCAN

- **Command layer**: `covered-by-shared-test` - keyspace scan coverage exercises cursor and match behavior.
- **DB API**: `covered-by-shared-test` - scan coverage exercises `KeyspaceReadOps.scan`.
- **Native internals**: `covered-by-shared-test` - scan coverage exercises native key iteration.

### SCARD

- **Command layer**: `covered` - `SetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `SetReadOps.scard`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native set cardinality.

### SELECT

- **Command layer**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest`.
- **DB API**: `covered-by-shared-test` - server tests exercise DB routing.
- **Native internals**: `not-applicable` - SELECT changes session DB index, not native storage.

### SET

- **Command layer**: `covered` - `CommandProcessorTest.stringIsBinarySafe`, `Milestone1CompatTest`, and `CommandErrorTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringWriteOps.set` and `setString`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw string writes through native key lookup.

### SETBIT

- **Command layer**: `covered` - `BitmapCommandTest.getbitSetbitBasicSemantics` and `BitmapCommandTest.setbitZeroFillsGrownBytesWithinCapacity`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringWriteOps.setBit`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw byte growth and bit mutation.

### SISMEMBER

- **Command layer**: `covered` - `SetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `SetReadOps.sismember`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native set membership lookup.

### SMEMBERS

- **Command layer**: `covered` - `SetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `SetReadOps.smembers`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native set iteration.

### SREM

- **Command layer**: `covered` - `SetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `SetWriteOps.srem`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native set member removal.

### STATS

- **Command layer**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest`.
- **DB API**: `not-applicable` - STATS reads server executor and connection counters.
- **Native internals**: `not-applicable` - STATS has no native storage state.

### STRLEN

- **Command layer**: `covered` - `CommandProcessorTest.stringIsBinarySafe`.
- **DB API**: `covered-by-shared-test` - command tests exercise `StringReadOps.strlen`.
- **Native internals**: `covered-by-shared-test` - command tests exercise raw string length reads.

### TTL

- **Command layer**: `covered-by-shared-test` - `ExpireSemanticsTest`.
- **DB API**: `covered-by-shared-test` - TTL tests exercise `TtlReadOps.ttlSeconds`.
- **Native internals**: `covered-by-shared-test` - TTL tests exercise expire index reads.

### TYPE

- **Command layer**: `covered-by-shared-test` - type coverage exercises command replies for multiple value types.
- **DB API**: `covered-by-shared-test` - type coverage exercises `KeyspaceReadOps.typeOf`.
- **Native internals**: `covered-by-shared-test` - type coverage reads root type metadata.

### ZADD

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetWriteOps.zadd`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset member and score writes.

### ZRANGE

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetReadOps.zrange`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset range traversal.

### ZRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetReadOps.zrangeByScore`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset score index traversal.

### ZREM

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetWriteOps.zrem`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset member removal.

### ZREMRANGEBYRANK

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetWriteOps.zremrangeByRank`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset rank deletion.

### ZREMRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetWriteOps.zremrangeByScore`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset score deletion.

### ZREVRANGE

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetReadOps.zrevrange`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset reverse range traversal.

### ZREVRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest`.
- **DB API**: `covered-by-shared-test` - command tests exercise `ZSetReadOps.zrevrangeByScore`.
- **Native internals**: `covered-by-shared-test` - command tests exercise native zset reverse score traversal.

## Option And Subcommand Inventory

| Operation | Required variants |
| --- | --- |
| `COMMAND` | base, `COUNT`, `INFO`, unknown name |
| `CLIENT` | `SETINFO`, `SETNAME`, `GETNAME`, unknown subcommand |
| `HELLO` | RESP2, RESP3, `SETNAME`, unsupported proto, `AUTH`, disallowed in `MULTI` |
| `INFO` | no section, `yierdis`, `memory`, `keyspace`, unknown section |
| `MEMORY` | `STATS`, `USAGE`, invalid subcommand |
| `OBJECT` | `ENCODING`, invalid subcommand |
| `SCAN` | cursor, `MATCH`, `COUNT`, invalid cursor, duplicate option |
| `SET` | plain, `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`, conflicts |
| `BITCOUNT` | full string, positive byte range, negative byte range, invalid bounds |
| `LPOP` | single pop, counted pop, zero count, negative count |
| `RPOP` | single pop, counted pop, zero count, negative count |
| `ZRANGE` | normal, `WITHSCORES`, `REV`, bounds, invalid option |
| `ZREVRANGE` | normal, `WITHSCORES`, invalid option |
| `ZRANGEBYSCORE` | inclusive bounds, exclusive bounds, infinities, `WITHSCORES`, `LIMIT`, invalid syntax |
| `ZREVRANGEBYSCORE` | inclusive bounds, exclusive bounds, infinities, `WITHSCORES`, `LIMIT`, invalid syntax |
| `FLUSHDB` | default, `SYNC`, `ASYNC`, invalid mode |

## DB API Inventory

| API family | Methods that require rows when direct API tests are added |
| --- | --- |
| `StringReadOps` | `getStringBytes`, `getStringValue`, `strlen`, `getBit`, `bitcount`, ranged `bitcount` |
| `StringWriteOps` | `set`, `setString`, `append`, `setBit`, `incrBy` |
| `HashReadOps` | `hget`, `hgetall`, `hlen` |
| `HashWriteOps` | `hset`, `hdel` |
| `ListReadOps` | `lrange` |
| `ListWriteOps` | `lpush`, `rpush`, `lpop`, `rpop` |
| `SetReadOps` | `smembers`, `sismember`, `scard` |
| `SetWriteOps` | `sadd`, `srem` |
| `ZSetReadOps` | `zrange`, `zrevrange`, `zrangeByScore`, `zrevrangeByScore` |
| `ZSetWriteOps` | `zadd`, `zremrangeByScore`, `zremrangeByRank`, `zrem` |
| `HllReadOps` | `pfcount` |
| `HllWriteOps` | `pfadd`, `pfmerge` |
| `KeyspaceReadOps` | `typeOf`, `existsKey`, `keys`, `scan` |
| `KeyspaceWriteOps` | `del` |
| `TtlReadOps` | `ttlSeconds`, `ttlMillis` |
| `TtlWriteOps` | `expire`, `pexpire`, `expireAtSeconds`, `expireAtMillis`, `persist` |
| `DbLifecycleOps` | `flushDb` |
| `MemoryOps` | stats, usage, reporter integration |

## Native/Internal Inventory

| Area | Structures and behavior that require direct internal tests |
| --- | --- |
| Entry table | `EntryRecord`, `EntryTable`, `EntryHandle`, `ValueHandle` |
| Key handles | `KeyHandle`, `HeapKeyHandle`, `FfmKeyHandle`, byte equality, hash stability, lifecycle |
| Native key directory | `NativeKeyDirectory` lookup, insert, replace, remove, scan, tombstone, rehash |
| FFM keyspace | `YierdisFfmBlobStore`, `YierdisFfmKeyspace`, allocation failure cleanup |
| Heap keyspace | `ByteArrayKeyspace`, binary key matching, scan cursor behavior |
| String roots | `StringRoot`, raw bytes, integer-like bytes, spare capacity, bitmap growth |
| Collection roots | `ListRoot`, `HashRoot`, `SetRoot`, `ZSetRoot` |
| Collection values | `ListValue`, `HashValue`, `SetValue`, `ZSetValue` |
| HLL storage | `YierdisHyperLogLog` stored as `StringRoot` with `ValueType.STRING` and `ValueEncoding.STRING_RAW` |
| Expiration | `YierdisExpireIndex`, `YierdisHeapExpireIndex`, `YierdisFfmExpireIndex` |
| Memory accounting | `YierdisDbMemoryLedger`, `MemoryLedger`, `InMemoryLedger`, reserve, commit, rollback |
| Mutation executor | `YierdisDbMutationExecutor`, type conversion, wrong-type errors, cleanup on failure |
| Observability | `YierdisDbMemoryEstimator`, `YierdisDbMemoryReporter`, `YierdisDbIntrospection` |
| Maxmemory | sampling, eviction policy, double-reply regression, noeviction behavior |

## Current Gap Queue

1. Add direct DB API tests for each API family instead of relying only on command-layer traversal.
2. Add direct native/internal tests for every root/value/keyspace/expiration/memory structure in the inventory.
3. Expand command option rows into one test row per option group for `SET`, `SCAN`, `ZRANGE`, `ZRANGEBYSCORE`, `HELLO`, `INFO`, `MEMORY`, `OBJECT`, and counted list pops.
4. Add one dedicated matrix section per command family as later plans fill the missing direct tests.
```

- [ ] **Step 2: Run the guard tests and verify the green state**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am \
  -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit the guard and matrix**

Run:

```bash
git add docs/project-docs/operation-test-coverage-matrix.md \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java \
  yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ServerOperationCoverageMatrixTest.java
git commit -m "test: guard operation coverage matrix"
```

Expected: commit succeeds and includes only the two guard tests plus the matrix document.

---

### Task 3: Add Shared Reply Assertions And String/Bitmap Template Tests

**Files:**
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/StringBitmapOperationCoverageTest.java`
- Create: `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/ReplyAssertions.java`

- [ ] **Step 1: Write the failing template test**

Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/StringBitmapOperationCoverageTest.java` with this content:

```java
package yier.bubu.redis.integration.command;

import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.ReplyAssertions.assertBulkString;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.ReplyAssertions.assertSimpleString;
import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class StringBitmapOperationCoverageTest {
    @Test
    public void stringTemplateCoversBinarySafeSetGetStrlenAppendIncrAndDecr() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
                byte[] value = new byte[]{0, (byte) 0xFE, 'v'};

                assertSimpleString("OK", client.execute(Arrays.asList(b("SET"), key, value)));
                assertBulkString(value, client.execute(Arrays.asList(b("GET"), key)));
                assertInteger(value.length, client.execute(Arrays.asList(b("STRLEN"), key)));

                assertInteger(value.length + 2, client.execute(Arrays.asList(b("APPEND"), key, new byte[]{1, 2})));
                assertBulkString(new byte[]{0, (byte) 0xFE, 'v', 1, 2}, client.execute(Arrays.asList(b("GET"), key)));

                assertSimpleString("OK", client.execute(cmd("SET", "counter", "11")));
                assertInteger(12, client.execute(cmd("INCR", "counter")));
                assertInteger(11, client.execute(cmd("DECR", "counter")));
                assertBulkString("11", client.execute(cmd("GET", "counter")));
            }
        });
    }

    @Test
    public void bitmapTemplateCoversMutationReadsAndByteRanges() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] key = b("bitmap");

                assertInteger(0, client.execute(Arrays.asList(b("GETBIT"), key, b("0"))));
                assertInteger(0, client.execute(Arrays.asList(b("SETBIT"), key, b("0"), b("1"))));
                assertInteger(1, client.execute(Arrays.asList(b("GETBIT"), key, b("0"))));

                assertInteger(0, client.execute(Arrays.asList(b("SETBIT"), key, b("15"), b("1"))));
                assertInteger(2, client.execute(Arrays.asList(b("BITCOUNT"), key)));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("0"), b("0"))));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("1"), b("1"))));
                assertInteger(1, client.execute(Arrays.asList(b("BITCOUNT"), key, b("-1"), b("-1"))));
            }
        });
    }
}
```

- [ ] **Step 2: Run the template test and verify the expected red state**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=StringBitmapOperationCoverageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD FAILURE` with a Java compilation error containing `cannot find symbol` and `ReplyAssertions`.

- [ ] **Step 3: Add the reply assertion helper**

Create `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/ReplyAssertions.java` with this content:

```java
package yier.bubu.redis.testutil;

import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class ReplyAssertions {
    private ReplyAssertions() {
    }

    public static ReplySimpleString assertSimpleString(String expected, ReplyObject reply) {
        Assert.assertTrue("expected simple string reply but got " + typeName(reply), reply instanceof ReplySimpleString);
        ReplySimpleString actual = (ReplySimpleString) reply;
        Assert.assertEquals(expected, actual.value());
        return actual;
    }

    public static ReplyBulkString assertBulkString(String expected, ReplyObject reply) {
        return assertBulkString(expected.getBytes(StandardCharsets.UTF_8), reply);
    }

    public static ReplyBulkString assertBulkString(byte[] expected, ReplyObject reply) {
        Assert.assertTrue("expected bulk string reply but got " + typeName(reply), reply instanceof ReplyBulkString);
        ReplyBulkString actual = (ReplyBulkString) reply;
        Assert.assertTrue(
                "expected bulk bytes " + Arrays.toString(expected) + " but got " + Arrays.toString(actual.data()),
                Arrays.equals(expected, actual.data())
        );
        return actual;
    }

    public static ReplyInteger assertInteger(long expected, ReplyObject reply) {
        Assert.assertTrue("expected integer reply but got " + typeName(reply), reply instanceof ReplyInteger);
        ReplyInteger actual = (ReplyInteger) reply;
        Assert.assertEquals(expected, actual.value());
        return actual;
    }

    public static ReplyNull assertNull(ReplyObject reply) {
        Assert.assertTrue("expected null reply but got " + typeName(reply), reply instanceof ReplyNull);
        return (ReplyNull) reply;
    }

    public static ReplyError assertErrorContaining(String expectedFragment, ReplyObject reply) {
        Assert.assertTrue("expected error reply but got " + typeName(reply), reply instanceof ReplyError);
        ReplyError actual = (ReplyError) reply;
        Assert.assertTrue(
                "expected error containing " + expectedFragment + " but got " + actual.message(),
                actual.message().contains(expectedFragment)
        );
        return actual;
    }

    public static ReplyArray assertArraySize(int expectedSize, ReplyObject reply) {
        Assert.assertTrue("expected array reply but got " + typeName(reply), reply instanceof ReplyArray);
        ReplyArray actual = (ReplyArray) reply;
        Assert.assertEquals(expectedSize, actual.values().size());
        return actual;
    }

    private static String typeName(ReplyObject reply) {
        return reply == null ? "null" : reply.getClass().getSimpleName();
    }
}
```

- [ ] **Step 4: Run the template test and verify the green state**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=StringBitmapOperationCoverageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Commit the helper and template tests**

Run:

```bash
git add yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/ReplyAssertions.java \
  yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/StringBitmapOperationCoverageTest.java
git commit -m "test: add string bitmap coverage template"
```

Expected: commit succeeds and includes only the helper plus the template test.

---

### Task 4: Link The Matrix From The Testing Guide

**Files:**
- Modify: `docs/project-docs/testing-and-debugging.md`

- [ ] **Step 1: Add the matrix section**

In `docs/project-docs/testing-and-debugging.md`, after the paragraph that ends with `这层最适合命令开发者，因为它不需要起 Netty server 就能把大部分行为跑清楚。`, insert this section:

````markdown
#### 操作覆盖矩阵

`docs/project-docs/operation-test-coverage-matrix.md` 是命令、DB API、native 内部结构三层测试覆盖的索引。新增命令或新增 server-only 命令时，先补矩阵行，再补对应测试；否则 `OperationCoverageMatrixTest` 或 `ServerOperationCoverageMatrixTest` 会失败。

常用 guard：

```bash
mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am \
  -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest,StringBitmapOperationCoverageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
````

- [ ] **Step 2: Verify the docs mention the matrix and guards**

Run:

```bash
rg -n "operation-test-coverage-matrix|OperationCoverageMatrixTest|ServerOperationCoverageMatrixTest" docs/project-docs/testing-and-debugging.md
```

Expected: output contains all three names.

- [ ] **Step 3: Commit the testing guide update**

Run:

```bash
git add docs/project-docs/testing-and-debugging.md
git commit -m "docs: document operation coverage matrix"
```

Expected: commit succeeds and includes only `docs/project-docs/testing-and-debugging.md`.

---

### Task 5: Final Verification

**Files:**
- Verify all files created or modified by Tasks 1-4.

- [ ] **Step 1: Run targeted coverage-matrix tests**

Run:

```bash
mvn -pl yierdis-tests/yierdis-integration-tests,yierdis-server/yierdis-server-main -am \
  -Dtest=OperationCoverageMatrixTest,ServerOperationCoverageMatrixTest,StringBitmapOperationCoverageTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 2: Run the full Maven test suite**

Run:

```bash
mvn test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Check formatting and working tree state**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` prints no errors. `git status --short` is empty after the commits from Tasks 2-4.

## Self-Review Notes

- Spec coverage: this plan implements the coverage matrix document, command registry guards, server command guard, assertion helper, String/Bitmap template coverage, and testing-guide handoff. It intentionally leaves the remaining family-specific test filling to later matrix-driven plans.
- Placeholder scan: the plan contains no placeholder markers, no unexpanded code steps, and no references to files without exact paths.
- Type consistency: `ReplyAssertions` uses the existing `ReplyObject`, `ReplySimpleString`, `ReplyBulkString`, `ReplyInteger`, `ReplyNull`, `ReplyError`, and `ReplyArray` test classes; matrix guards use existing `YierdisFastCommandProcessor`, `CommandRegistry`, and `ServerCommandModule`.
