# Package Structure Rearchitecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize Yierdis Maven paths, Java package ownership, test placement, and architecture guards so the source layout matches the approved package-structure rearchitecture spec.

**Architecture:** Execute the migration in small behavior-preserving phases. Keep artifact IDs stable, move physical module paths first, then rename package families in low-risk-to-high-risk order, and only remove legacy facades after target imports are verified. Architecture tests must evolve with each phase so the repository remains guarded while both legacy and target package names temporarily coexist.

**Tech Stack:** Java 25, Maven, JUnit 4, ArchUnit, Netty, existing Yierdis Maven modules.

---

## Approved Spec

Implement the approved design in:

```text
docs/superpowers/specs/2026-05-06-package-structure-rearchitecture-design.md
```

Resolved implementation decisions:

- Move physical Maven module paths before broad Java package renames.
- Keep Maven artifact IDs stable during the first migration.
- Use temporary legacy package facades for `contract`, `ops`, and `offheap.api`.
- Move server-app package first; move client and bench app package names after server-app is stable.
- Keep integration-test fixtures in `tests/yierdis-integration-tests` unless duplication remains after Task 11.
- Remove compatibility facades only in the final task and only after target imports are verified.

These choices are locked for the implementation run. If one changes, update the
spec and this plan together before editing source.

## Worktree And Commit Policy

Run implementation in a dedicated worktree or branch. Each task below ends with
a commit. Do not mix package moves with semantic behavior changes.

Recommended setup before Task 1:

```bash
git worktree add .worktrees/package-structure-rearchitecture -b package-structure-rearchitecture
cd .worktrees/package-structure-rearchitecture
```

Expected:

```text
Preparing worktree (new branch 'package-structure-rearchitecture')
HEAD is now at <current commit>
```

## File Structure Map

Current top-level module paths that will move:

```text
yierdis-memory                         -> libs/memory
yierdis-bytes                          -> libs/bytes
yierdis-execution                      -> libs/execution
yierdis-storage                        -> libs/storage
yierdis-runtime                        -> libs/runtime
yierdis-protocol                       -> libs/protocol
yierdis-command                        -> libs/command
yierdis-executor-core                  -> libs/executor/yierdis-executor-core
yierdis-app/yierdis-server-app         -> apps/yierdis-server-app
yierdis-client                         -> apps/yierdis-client
yierdis-bench                          -> apps/yierdis-bench
yierdis-architecture-tests             -> tests/yierdis-architecture-tests
yierdis-integration-tests              -> tests/yierdis-integration-tests
```

Current Maven child directories that need normalized names after the move:

```text
libs/memory/api                        -> libs/memory/yierdis-memory-api
libs/memory/foreign                    -> libs/memory/yierdis-memory-foreign
```

Primary package migration targets:

```text
yier.bubu.redis                       -> yier.bubu.redis.app.server
yier.bubu.redis.args                  -> yier.bubu.redis.app.server.args
yier.bubu.redis.executor              -> yier.bubu.redis.execution.executor
yier.bubu.redis.command               -> yier.bubu.redis.command.api/kernel/defaults
yier.bubu.redis.contract              -> yier.bubu.redis.execution.api
yier.bubu.redis.ops                   -> yier.bubu.redis.storage.api
yier.bubu.redis.ops.result            -> yier.bubu.redis.storage.api.result
yier.bubu.redis.db                    -> yier.bubu.redis.storage.memory
yier.bubu.redis.db.key                -> yier.bubu.redis.storage.memory.internal.key
yier.bubu.redis.db.memory             -> yier.bubu.redis.storage.memory.internal.ledger
yier.bubu.redis.db.memory.ffm         -> yier.bubu.redis.storage.memory.internal.ffm
yier.bubu.redis.db.memory.foreign     -> yier.bubu.redis.memory.foreign
yier.bubu.redis.offheap.api           -> yier.bubu.redis.memory.api
yier.bubu.redis.runtime               -> yier.bubu.redis.runtime.embedded or runtime.api
yier.bubu.redis.protocol.v1           -> yier.bubu.redis.protocol.custom.v1.wire or execution
yier.bubu.redis.protocol.json         -> yier.bubu.redis.protocol.custom.v1.json
yier.bubu.redis.protocol.reply        -> yier.bubu.redis.protocol.custom.v1.reply
yier.bubu.redis.protocol.netty        -> yier.bubu.redis.protocol.custom.v1.netty
```

Architecture guard files:

```text
tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml
tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java
tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java
tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java
```

Important docs to update as paths and packages move:

```text
README.md
docs/module-architecture.md
docs/development-navigation.md
docs/project-overview.md
docs/request-execution-flow.md
docs/main-path-walkthrough.md
docs/db-internals.md
docs/bytes-and-fast-paths.md
docs/testing-and-debugging.md
docs/configuration-and-operations.md
```

---

### Task 1: Add Target Policy Baseline Without Moving Source

**Files:**
- Modify: `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java`
- Modify: `docs/module-architecture.md`

- [ ] **Step 1: Add target policy section to `architecture-policy.yml`**

Append a top-level `target_packages` section below the current `modules`
section. Keep the current `modules` rules intact.

```yaml
target_packages:
  app_server:
    owns:
      - yier.bubu.redis.app.server
    legacy_allowed_during_migration:
      - yier.bubu.redis
      - yier.bubu.redis.args
  execution:
    owns:
      - yier.bubu.redis.execution.api
      - yier.bubu.redis.execution.engine
      - yier.bubu.redis.execution.executor
    legacy_allowed_during_migration:
      - yier.bubu.redis.contract
      - yier.bubu.redis.engine
      - yier.bubu.redis.executor
  command:
    owns:
      - yier.bubu.redis.command.api
      - yier.bubu.redis.command.kernel
      - yier.bubu.redis.command.defaults
    legacy_allowed_during_migration:
      - yier.bubu.redis.command
  storage:
    owns:
      - yier.bubu.redis.storage.api
      - yier.bubu.redis.storage.api.result
      - yier.bubu.redis.storage.memory
    legacy_allowed_during_migration:
      - yier.bubu.redis.ops
      - yier.bubu.redis.db
  runtime:
    owns:
      - yier.bubu.redis.runtime.api
      - yier.bubu.redis.runtime.embedded
    legacy_allowed_during_migration:
      - yier.bubu.redis.runtime
  memory:
    owns:
      - yier.bubu.redis.memory.api
      - yier.bubu.redis.memory.foreign
    legacy_allowed_during_migration:
      - yier.bubu.redis.offheap.api
      - yier.bubu.redis.db.memory.foreign
  protocol:
    owns:
      - yier.bubu.redis.protocol.custom.v1
    legacy_allowed_during_migration:
      - yier.bubu.redis.protocol
```

- [ ] **Step 2: Add a resource test for the target section**

Add this JUnit test method to `ArchitecturePolicyResourceTest`:

```java
@Test
public void policyDocumentsTargetPackageOwnership() throws IOException {
    Path policyFile = Paths.get("src/test/resources/architecture-policy.yml");
    if (!Files.isRegularFile(policyFile)) {
        policyFile = Paths.get("tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml");
    }
    String policy = Files.readString(policyFile, StandardCharsets.UTF_8);

    Assert.assertTrue(policy.contains("target_packages:"));
    Assert.assertTrue(policy.contains("yier.bubu.redis.app.server"));
    Assert.assertTrue(policy.contains("yier.bubu.redis.execution.api"));
    Assert.assertTrue(policy.contains("yier.bubu.redis.command.kernel"));
    Assert.assertTrue(policy.contains("yier.bubu.redis.storage.memory"));
    Assert.assertTrue(policy.contains("legacy_allowed_during_migration:"));
}
```

If the class lacks imports, add:

```java
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
```

- [ ] **Step 3: Update architecture documentation**

Add a short "Package Ownership Migration" subsection to `docs/module-architecture.md` after the current module overview:

```markdown
## Package Ownership Migration

The Maven split is now stable enough to make Java package names match module
ownership. New production code should use the target package families documented
in `docs/superpowers/specs/2026-05-06-package-structure-rearchitecture-design.md`.
Legacy packages such as `yier.bubu.redis.db`, `yier.bubu.redis.ops`,
`yier.bubu.redis.contract`, and the server root package are migration-only names.
```

- [ ] **Step 4: Run architecture policy test**

Run:

```bash
mvn -pl yierdis-architecture-tests -Dtest=ArchitecturePolicyResourceTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add yierdis-architecture-tests/src/test/resources/architecture-policy.yml \
        yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java \
        docs/module-architecture.md
git commit -m "test: document target package ownership policy"
```

---

### Task 2: Move Physical Maven Module Layout

**Files:**
- Modify: `pom.xml`
- Modify: moved aggregator POMs under `libs/*/pom.xml`
- Modify: moved app/test module POMs under `apps/*/pom.xml` and `tests/*/pom.xml`
- Move: all top-level module directories listed in the file structure map

- [ ] **Step 1: Create target parent directories**

```bash
mkdir -p libs apps tests libs/executor
```

Expected: command exits with status 0.

- [ ] **Step 2: Move top-level module directories with `git mv`**

```bash
git mv yierdis-memory libs/memory
git mv yierdis-bytes libs/bytes
git mv yierdis-execution libs/execution
git mv yierdis-storage libs/storage
git mv yierdis-runtime libs/runtime
git mv yierdis-protocol libs/protocol
git mv yierdis-command libs/command
git mv yierdis-executor-core libs/executor/yierdis-executor-core
git mv yierdis-app/yierdis-server-app apps/yierdis-server-app
git mv yierdis-client apps/yierdis-client
git mv yierdis-bench apps/yierdis-bench
git mv yierdis-architecture-tests tests/yierdis-architecture-tests
git mv yierdis-integration-tests tests/yierdis-integration-tests
git mv libs/memory/api libs/memory/yierdis-memory-api
git mv libs/memory/foreign libs/memory/yierdis-memory-foreign
```

- [ ] **Step 3: Update root `pom.xml` modules**

Replace the `<modules>` block in `pom.xml` with:

```xml
    <modules>
        <module>libs/memory</module>
        <module>libs/bytes</module>
        <module>libs/execution</module>
        <module>libs/storage</module>
        <module>libs/runtime</module>
        <module>libs/protocol</module>
        <module>libs/command</module>
        <module>libs/executor/yierdis-executor-core</module>
        <module>apps/yierdis-client</module>
        <module>apps/yierdis-server-app</module>
        <module>apps/yierdis-bench</module>
        <module>tests/yierdis-architecture-tests</module>
        <module>tests/yierdis-integration-tests</module>
    </modules>
```

- [ ] **Step 4: Update moved aggregator parent relative paths**

For each moved aggregator POM below, add `<relativePath>../../pom.xml</relativePath>` inside `<parent>`:

```text
libs/memory/pom.xml
libs/bytes/pom.xml
libs/execution/pom.xml
libs/storage/pom.xml
libs/runtime/pom.xml
libs/protocol/pom.xml
libs/command/pom.xml
```

Example parent block:

```xml
    <parent>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
        <relativePath>../../pom.xml</relativePath>
    </parent>
```

- [ ] **Step 5: Update root-parent module relative paths**

For these module POMs, ensure the `<parent>` block has the shown relative path:

```text
libs/executor/yierdis-executor-core/pom.xml        ../../../pom.xml
apps/yierdis-client/pom.xml                        ../../pom.xml
apps/yierdis-server-app/pom.xml                    ../../pom.xml
apps/yierdis-bench/pom.xml                         ../../pom.xml
tests/yierdis-architecture-tests/pom.xml           ../../pom.xml
tests/yierdis-integration-tests/pom.xml            ../../pom.xml
```

Example for executor-core:

```xml
    <parent>
        <groupId>yier.bubu.redis</groupId>
        <artifactId>yierdis-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
        <relativePath>../../../pom.xml</relativePath>
    </parent>
```

- [ ] **Step 6: Update memory aggregator child module names**

In `libs/memory/pom.xml`, replace the child modules with:

```xml
    <modules>
        <module>yierdis-memory-api</module>
        <module>yierdis-memory-foreign</module>
    </modules>
```

- [ ] **Step 7: Update architecture-test path references**

Run this search:

```bash
rg -n 'yierdis-(memory|bytes|execution|storage|runtime|protocol|command|executor-core|client|bench|architecture-tests|integration-tests)|yierdis-app' tests/yierdis-architecture-tests docs README.md
```

Update path strings so they point to the new `libs`, `apps`, and `tests` layout. Do not change artifact names in text where the text is explicitly naming Maven artifact IDs.

- [ ] **Step 8: Verify Maven sees the moved reactor**

Run:

```bash
mvn -q -DskipTests validate
```

Expected:

```text
```

The command is quiet; success is exit code 0.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "build: move modules under libs apps and tests"
```

---

### Task 3: Make Architecture Tests Path-Layout Aware

**Files:**
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`

- [ ] **Step 1: Add a module path helper to each architecture test class that hardcodes module paths**

Use this helper shape in classes that resolve repository paths manually:

```java
private static Path modulePath(Path repoRoot, String artifactId) {
    return switch (artifactId) {
        case "yierdis-memory-api" -> repoRoot.resolve("libs/memory/yierdis-memory-api");
        case "yierdis-memory-foreign" -> repoRoot.resolve("libs/memory/yierdis-memory-foreign");
        case "yierdis-bytes-lib" -> repoRoot.resolve("libs/bytes/yierdis-bytes-lib");
        case "yierdis-bytes-netty" -> repoRoot.resolve("libs/bytes/yierdis-bytes-netty");
        case "yierdis-execution-api" -> repoRoot.resolve("libs/execution/yierdis-execution-api");
        case "yierdis-engine" -> repoRoot.resolve("libs/execution/yierdis-engine");
        case "yierdis-command-api" -> repoRoot.resolve("libs/command/yierdis-command-api");
        case "yierdis-command-kernel" -> repoRoot.resolve("libs/command/yierdis-command-kernel");
        case "yierdis-command-defaults" -> repoRoot.resolve("libs/command/yierdis-command-defaults");
        case "yierdis-storage-api" -> repoRoot.resolve("libs/storage/yierdis-storage-api");
        case "yierdis-storage-testkit" -> repoRoot.resolve("libs/storage/yierdis-storage-testkit");
        case "yierdis-storage-memory" -> repoRoot.resolve("libs/storage/yierdis-storage-memory");
        case "yierdis-runtime-api" -> repoRoot.resolve("libs/runtime/yierdis-runtime-api");
        case "yierdis-runtime-embedded" -> repoRoot.resolve("libs/runtime/yierdis-runtime-embedded");
        case "yierdis-custom-v1-wire" -> repoRoot.resolve("libs/protocol/yierdis-custom-v1-wire");
        case "yierdis-custom-v1-execution-adapter" -> repoRoot.resolve("libs/protocol/yierdis-custom-v1-execution-adapter");
        case "yierdis-custom-v1-netty" -> repoRoot.resolve("libs/protocol/yierdis-custom-v1-netty");
        case "yierdis-executor-core" -> repoRoot.resolve("libs/executor/yierdis-executor-core");
        case "yierdis-server-app" -> repoRoot.resolve("apps/yierdis-server-app");
        case "yierdis-client" -> repoRoot.resolve("apps/yierdis-client");
        case "yierdis-bench" -> repoRoot.resolve("apps/yierdis-bench");
        case "yierdis-architecture-tests" -> repoRoot.resolve("tests/yierdis-architecture-tests");
        case "yierdis-integration-tests" -> repoRoot.resolve("tests/yierdis-integration-tests");
        default -> throw new IllegalArgumentException("Unknown module artifactId: " + artifactId);
    };
}
```

- [ ] **Step 2: Replace hardcoded paths**

Replace source roots such as:

```java
workspaceRoot.resolve("yierdis-storage/yierdis-storage-api/src/main/java")
```

with:

```java
modulePath(repoRoot, "yierdis-storage-api").resolve("src/main/java")
```

Replace paths such as:

```java
repoRoot.getParent().resolve("yierdis-command/yierdis-command-api/pom.xml")
```

with:

```java
modulePath(repoRoot, "yierdis-command-api").resolve("pom.xml")
```

- [ ] **Step 3: Update repo root detection**

If a test method checks for old module locations, replace the condition with:

```java
Path current = Paths.get("").toAbsolutePath().normalize();
for (Path p = current; p != null; p = p.getParent()) {
    if (Files.isRegularFile(p.resolve("pom.xml"))
            && Files.isDirectory(p.resolve("libs/execution/yierdis-execution-api"))
            && Files.isDirectory(p.resolve("libs/storage/yierdis-storage-memory"))) {
        return p;
    }
}
return null;
```

- [ ] **Step 4: Run architecture tests**

```bash
mvn -pl tests/yierdis-architecture-tests test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add tests/yierdis-architecture-tests/src/test/java
git commit -m "test: resolve architecture modules by artifact id"
```

---

### Task 4: Rename Server-App And Executor Packages

**Files:**
- Move: `apps/yierdis-server-app/src/main/java/yier/bubu/redis/*.java`
- Move: `apps/yierdis-server-app/src/main/java/yier/bubu/redis/args/*.java`
- Move: `libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/executor/*.java`
- Move corresponding tests under `src/test/java`

- [ ] **Step 1: Move server-app source packages**

```bash
mkdir -p apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server
mkdir -p apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/args
git mv apps/yierdis-server-app/src/main/java/yier/bubu/redis/*.java \
       apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/
git mv apps/yierdis-server-app/src/main/java/yier/bubu/redis/args/*.java \
       apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/args/
```

- [ ] **Step 2: Move server-app test packages**

```bash
mkdir -p apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server
mkdir -p apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/args
git mv apps/yierdis-server-app/src/test/java/yier/bubu/redis/*.java \
       apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/
git mv apps/yierdis-server-app/src/test/java/yier/bubu/redis/args/*.java \
       apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/args/
```

- [ ] **Step 3: Move executor source and tests**

```bash
mkdir -p libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor
mkdir -p libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/execution/executor
git mv libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/executor/*.java \
       libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/
git mv libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/executor/*.java \
       libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/execution/executor/
```

- [ ] **Step 4: Rewrite package declarations and imports**

Run:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis;/package yier.bubu.redis.app.server;/' apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/*.java apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.args;/package yier.bubu.redis.app.server.args;/' apps/yierdis-server-app/src/main/java/yier/bubu/redis/app/server/args/*.java apps/yierdis-server-app/src/test/java/yier/bubu/redis/app/server/args/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.executor;/package yier.bubu.redis.execution.executor;/' libs/executor/yierdis-executor-core/src/main/java/yier/bubu/redis/execution/executor/*.java libs/executor/yierdis-executor-core/src/test/java/yier/bubu/redis/execution/executor/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.args\\./import yier.bubu.redis.app.server.args./g; s/import yier\\.bubu\\.redis\\.executor\\./import yier.bubu.redis.execution.executor./g; s/import yier\\.bubu\\.redis\\.([A-Z][A-Za-z0-9_]*)/import yier.bubu.redis.app.server.$1/g' $(rg --files -g '*.java')
perl -pi -e 's/yier\\.bubu\\.redis\\.args\\./yier.bubu.redis.app.server.args./g; s/yier\\.bubu\\.redis\\.executor\\./yier.bubu.redis.execution.executor./g' $(rg --files -g '*.java' -g '*.md' -g '*.yml')
```

- [ ] **Step 5: Check there are no server root package declarations**

```bash
rg -n '^package yier\\.bubu\\.redis;' apps/yierdis-server-app libs tests
```

Expected: no output and exit code 1.

- [ ] **Step 6: Run focused tests**

```bash
mvn -pl apps/yierdis-server-app,libs/executor/yierdis-executor-core test
mvn -pl tests/yierdis-architecture-tests test
```

Expected:

```text
BUILD SUCCESS
BUILD SUCCESS
```

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: move server app and executor packages"
```

---

### Task 5: Rename Command API, Kernel, And Defaults Packages

**Files:**
- Move: `libs/command/yierdis-command-api/src/main/java/yier/bubu/redis/command/*.java`
- Move: `libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/*.java`
- Move: `libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/*.java`
- Move corresponding tests

- [ ] **Step 1: Move command API package**

```bash
mkdir -p libs/command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api
mkdir -p libs/command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api
git mv libs/command/yierdis-command-api/src/main/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/
git mv libs/command/yierdis-command-api/src/test/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/
```

- [ ] **Step 2: Move command kernel package**

```bash
mkdir -p libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/kernel
mkdir -p libs/command/yierdis-command-kernel/src/test/java/yier/bubu/redis/command/kernel
git mv libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/kernel/
git mv libs/command/yierdis-command-kernel/src/test/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-kernel/src/test/java/yier/bubu/redis/command/kernel/
```

- [ ] **Step 3: Move command defaults package**

```bash
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults
mkdir -p libs/command/yierdis-command-defaults/src/test/java/yier/bubu/redis/command/defaults
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/
git mv libs/command/yierdis-command-defaults/src/test/java/yier/bubu/redis/command/*.java \
       libs/command/yierdis-command-defaults/src/test/java/yier/bubu/redis/command/defaults/
```

- [ ] **Step 4: Split default command families into subpackages**

```bash
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/string
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hash
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/list
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/set
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/zset
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hll
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/keyspace
mkdir -p libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/connection
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/StringCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/string/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/HashCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hash/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/ListCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/list/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/SetCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/set/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/ZSetCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/zset/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/HllCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hll/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/KeyCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/keyspace/
git mv libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/CoreConnectionCommands.java libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/connection/
```

- [ ] **Step 5: Rewrite command packages and imports**

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.api;/' libs/command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/*.java libs/command/yierdis-command-api/src/test/java/yier/bubu/redis/command/api/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.kernel;/' libs/command/yierdis-command-kernel/src/main/java/yier/bubu/redis/command/kernel/*.java libs/command/yierdis-command-kernel/src/test/java/yier/bubu/redis/command/kernel/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/*.java libs/command/yierdis-command-defaults/src/test/java/yier/bubu/redis/command/defaults/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.string;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/string/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.hash;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hash/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.list;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/list/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.set;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/set/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.zset;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/zset/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.hll;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/hll/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.keyspace;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/keyspace/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.command.defaults.connection;/' libs/command/yierdis-command-defaults/src/main/java/yier/bubu/redis/command/defaults/connection/*.java
```

- [ ] **Step 6: Update remaining imports using the ownership mapping**

Start with these deterministic replacements:

```bash
perl -pi -e 's/import yier\\.bubu\\.redis\\.command\\.(ArgReader|CommandArity|CommandDescriptor|CommandHandler|CommandModule|CommandParseError|CommandParseResult|CommandParser|CommandParsers|CommandSpec|ServerInfoProvider|SlowCommandGovernor|YierdisDbRouter);/import yier.bubu.redis.command.api.$1;/g' $(rg --files -g '*.java')
perl -pi -e 's/import yier\\.bubu\\.redis\\.command\\.(CommandRegistry|TransactionCommands|YierdisFastCommandProcessor);/import yier.bubu.redis.command.kernel.$1;/g' $(rg --files -g '*.java')
perl -pi -e 's/import yier\\.bubu\\.redis\\.command\\.DefaultCommandModules;/import yier.bubu.redis.command.defaults.DefaultCommandModules;/g' $(rg --files -g '*.java')
perl -pi -e 's/import yier\\.bubu\\.redis\\.command\\.BulkStringReplyAdapter;/import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;/g' $(rg --files -g '*.java')
```

Then run the focused compile command in Step 7. If an import still points at
`yier.bubu.redis.command.<Type>`, use this mapping:

```text
ArgReader                                  -> yier.bubu.redis.command.api.ArgReader
CommandArity                               -> yier.bubu.redis.command.api.CommandArity
CommandDescriptor                          -> yier.bubu.redis.command.api.CommandDescriptor
CommandHandler                             -> yier.bubu.redis.command.api.CommandHandler
CommandModule                              -> yier.bubu.redis.command.api.CommandModule
CommandParseError                          -> yier.bubu.redis.command.api.CommandParseError
CommandParseResult                         -> yier.bubu.redis.command.api.CommandParseResult
CommandParser                              -> yier.bubu.redis.command.api.CommandParser
CommandParsers                             -> yier.bubu.redis.command.api.CommandParsers
CommandSpec                                -> yier.bubu.redis.command.api.CommandSpec
ServerInfoProvider                         -> yier.bubu.redis.command.api.ServerInfoProvider
SlowCommandGovernor                        -> yier.bubu.redis.command.api.SlowCommandGovernor
YierdisDbRouter                            -> yier.bubu.redis.command.api.YierdisDbRouter
CommandRegistry                            -> yier.bubu.redis.command.kernel.CommandRegistry
TransactionCommands                        -> yier.bubu.redis.command.kernel.TransactionCommands
YierdisFastCommandProcessor                -> yier.bubu.redis.command.kernel.YierdisFastCommandProcessor
DefaultCommandModules                      -> yier.bubu.redis.command.defaults.DefaultCommandModules
BulkStringReplyAdapter                     -> yier.bubu.redis.command.defaults.BulkStringReplyAdapter
StringCommands                             -> yier.bubu.redis.command.defaults.string.StringCommands
HashCommands                               -> yier.bubu.redis.command.defaults.hash.HashCommands
ListCommands                               -> yier.bubu.redis.command.defaults.list.ListCommands
SetCommands                                -> yier.bubu.redis.command.defaults.set.SetCommands
ZSetCommands                               -> yier.bubu.redis.command.defaults.zset.ZSetCommands
HllCommands                                -> yier.bubu.redis.command.defaults.hll.HllCommands
KeyCommands                                -> yier.bubu.redis.command.defaults.keyspace.KeyCommands
CoreConnectionCommands                     -> yier.bubu.redis.command.defaults.connection.CoreConnectionCommands
CommandSupport                             -> yier.bubu.redis.command.defaults.CommandSupport
```

For static references inside `DefaultCommandModules`, add imports for each moved
command family listed above.

- [ ] **Step 7: Run command module tests**

```bash
mvn -pl libs/command/yierdis-command-api,libs/command/yierdis-command-kernel,libs/command/yierdis-command-defaults test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 8: Run dependent compile check**

```bash
mvn -q -DskipTests compile
```

Expected: exit code 0.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: split command packages by module ownership"
```

---

### Task 6: Move Execution API And Engine Packages With Legacy Facades

**Files:**
- Move: `libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/*.java`
- Move: `libs/execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/*.java`
- Create: legacy facade classes under `libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/`

- [ ] **Step 1: Move execution API implementation package**

```bash
mkdir -p libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/execution/api
mkdir -p libs/execution/yierdis-execution-api/src/test/java/yier/bubu/redis/execution/api
git mv libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/*.java \
       libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/execution/api/
git mv libs/execution/yierdis-execution-api/src/test/java/yier/bubu/redis/contract/*.java \
       libs/execution/yierdis-execution-api/src/test/java/yier/bubu/redis/execution/api/
```

- [ ] **Step 2: Move engine package**

```bash
mkdir -p libs/execution/yierdis-engine/src/main/java/yier/bubu/redis/execution/engine
mkdir -p libs/execution/yierdis-engine/src/test/java/yier/bubu/redis/execution/engine
git mv libs/execution/yierdis-engine/src/main/java/yier/bubu/redis/engine/*.java \
       libs/execution/yierdis-engine/src/main/java/yier/bubu/redis/execution/engine/
git mv libs/execution/yierdis-engine/src/test/java/yier/bubu/redis/engine/*.java \
       libs/execution/yierdis-engine/src/test/java/yier/bubu/redis/execution/engine/
```

- [ ] **Step 3: Rewrite package declarations**

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.contract;/package yier.bubu.redis.execution.api;/' libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/execution/api/*.java libs/execution/yierdis-execution-api/src/test/java/yier/bubu/redis/execution/api/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.engine;/package yier.bubu.redis.execution.engine;/' libs/execution/yierdis-engine/src/main/java/yier/bubu/redis/execution/engine/*.java libs/execution/yierdis-engine/src/test/java/yier/bubu/redis/execution/engine/*.java
```

- [ ] **Step 4: Rewrite imports to target packages**

```bash
perl -pi -e 's/import yier\\.bubu\\.redis\\.contract\\./import yier.bubu.redis.execution.api./g; s/import yier\\.bubu\\.redis\\.engine\\./import yier.bubu.redis.execution.engine./g' $(rg --files -g '*.java')
perl -pi -e 's/yier\\.bubu\\.redis\\.contract\\./yier.bubu.redis.execution.api./g; s/yier\\.bubu\\.redis\\.engine\\./yier.bubu.redis.execution.engine./g' $(rg --files -g '*.java' -g '*.md' -g '*.yml')
```

- [ ] **Step 5: Add legacy package facades only for public API**

Create legacy facade files for interfaces where source compatibility is needed. Example for `ReplyWriter`:

```java
package yier.bubu.redis.contract;

/**
 * @deprecated use {@link yier.bubu.redis.execution.api.ReplyWriter}
 */
@Deprecated(forRemoval = true)
public interface ReplyWriter extends yier.bubu.redis.execution.api.ReplyWriter {
}
```

Apply the same pattern for public interfaces that can extend target interfaces:

```text
ReplySink
ReplyWriter
ReplyWriterFactory
Session
ServerSession
ConnectionStatsView
DbIndexProvider
ExecutionRequest
```

For final classes and records, do not add inheritance facades in this phase. If any external compatibility need appears, add a static factory facade in a separate compatibility task after the compiler identifies the exact class.

- [ ] **Step 6: Run execution tests and full compile**

```bash
mvn -pl libs/execution/yierdis-execution-api,libs/execution/yierdis-engine test
mvn -q -DskipTests compile
```

Expected:

```text
BUILD SUCCESS
```

Second command expected: exit code 0.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: move execution packages to execution namespace"
```

---

### Task 7: Move Storage API Packages With Legacy Facades

**Files:**
- Move: `libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/*.java`
- Move: `libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/result/*.java`
- Create: legacy facade interfaces under `libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/`

- [ ] **Step 1: Move storage API implementation packages**

```bash
mkdir -p libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api
mkdir -p libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api/result
mkdir -p libs/storage/yierdis-storage-api/src/test/java/yier/bubu/redis/storage/api
git mv libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/*.java \
       libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api/
git mv libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/result/*.java \
       libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api/result/
git mv libs/storage/yierdis-storage-api/src/test/java/yier/bubu/redis/ops/*.java \
       libs/storage/yierdis-storage-api/src/test/java/yier/bubu/redis/storage/api/
```

- [ ] **Step 2: Rewrite storage API package declarations**

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.ops;/package yier.bubu.redis.storage.api;/' libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api/*.java libs/storage/yierdis-storage-api/src/test/java/yier/bubu/redis/storage/api/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.ops\\.result;/package yier.bubu.redis.storage.api.result;/' libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/storage/api/result/*.java
```

- [ ] **Step 3: Rewrite imports to target package**

```bash
perl -pi -e 's/import yier\\.bubu\\.redis\\.ops\\.result\\./import yier.bubu.redis.storage.api.result./g; s/import yier\\.bubu\\.redis\\.ops\\./import yier.bubu.redis.storage.api./g' $(rg --files -g '*.java')
perl -pi -e 's/yier\\.bubu\\.redis\\.ops\\.result\\./yier.bubu.redis.storage.api.result./g; s/yier\\.bubu\\.redis\\.ops\\./yier.bubu.redis.storage.api./g' $(rg --files -g '*.java' -g '*.md' -g '*.yml')
```

- [ ] **Step 4: Add legacy interface facades for command-facing APIs**

Create `libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/DbEngine.java` with:

```java
package yier.bubu.redis.ops;

/**
 * @deprecated use {@link yier.bubu.redis.storage.api.DbEngine}
 */
@Deprecated(forRemoval = true)
public interface DbEngine extends yier.bubu.redis.storage.api.DbEngine {
}
```

Repeat the same facade pattern for interfaces in the storage API package:

```text
DbReads
DbWrites
DbLifecycleOps
ExpirationManager
HashReadOps
HashWriteOps
HllReadOps
HllWriteOps
KeyspaceReadOps
KeyspaceWriteOps
ListReadOps
ListWriteOps
MemoryOps
RuntimeDbEngine
SetReadOps
SetWriteOps
StringReadOps
StringWriteOps
TtlReadOps
TtlWriteOps
ZSetReadOps
ZSetWriteOps
MaxmemoryCoordinator
MaxmemoryCoordinatorAware
MaxmemoryParticipant
MaxmemoryUsageSource
KeyHandle
```

Do not create facades for enums or final result types in this task. Target imports should be used in production code.

- [ ] **Step 5: Run storage API tests and full compile**

```bash
mvn -pl libs/storage/yierdis-storage-api test
mvn -q -DskipTests compile
```

Expected:

```text
BUILD SUCCESS
```

Second command expected: exit code 0.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: move storage api packages"
```

---

### Task 8: Move Storage-Memory Internal Packages In Slices

**Files:**
- Move: `libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/**`
- Move matching tests under `src/test/java`

- [ ] **Step 1: Move key and keyspace packages**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/key/*.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisKeyspace.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/
```

Rewrite moved package declarations:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db\\.key;/package yier.bubu.redis.storage.memory.internal.key;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory.internal.keyspace;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\.key\\./import yier.bubu.redis.storage.memory.internal.key./g; s/import yier\\.bubu\\.redis\\.db\\.YierdisKeyspace;/import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;/g; s/import yier\\.bubu\\.redis\\.db\\.ByteArrayKeyspace;/import yier.bubu.redis.storage.memory.internal.keyspace.ByteArrayKeyspace;/g' $(rg --files -g '*.java')
```

Run:

```bash
mvn -pl libs/storage/yierdis-storage-memory -Dtest=KeyHandleContractTest,ByteArrayKeyspaceTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 2: Move expire and TTL support**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisExpireIndex.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisHeapExpireIndex.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbExpirationManager.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisTtlOps.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory.internal.expire;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/expire/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\.(YierdisExpireIndex|YierdisHeapExpireIndex|YierdisDbExpirationSupport|YierdisDbExpirationManager|YierdisTtlOps);/import yier.bubu.redis.storage.memory.internal.expire.$1;/g' $(rg --files -g '*.java')
```

Run:

```bash
mvn -pl libs/storage/yierdis-storage-memory -Dtest=ExpireIndexTest,ExpireKeySharingTest,OffHeapBytesViewTtlRegressionTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 3: Move ledger and mutation support**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/memory/*.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisDbMutationExecutor.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/DbMemoryAccounting.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db\\.memory;/package yier.bubu.redis.storage.memory.internal.ledger;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/Memory*.java libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/InMemoryLedger.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory.internal.ledger;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMemoryLedger.java libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMutationExecutor.java libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\.memory\\./import yier.bubu.redis.storage.memory.internal.ledger./g; s/import yier\\.bubu\\.redis\\.db\\.(YierdisDbMemoryLedger|YierdisDbMutationExecutor|DbMemoryAccounting);/import yier.bubu.redis.storage.memory.internal.ledger.$1;/g' $(rg --files -g '*.java')
```

Run:

```bash
mvn -pl libs/storage/yierdis-storage-memory -Dtest=MemoryLedgerContractTest,MutationExecutorReservationTest,MemoryStatsAccountingConsistencyTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 4: Move value objects and encodings**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisValue.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisObject.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/HashValue.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/ListValue.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/SetValue.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/ZSetValue.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisListpack.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/ZSkipList.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/ValueEncoding.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisEncodingThresholds.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/YierdisHyperLogLog.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory.internal.value;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\.(YierdisValue|YierdisObject|HashValue|ListValue|SetValue|ZSetValue|YierdisListpack|ZSkipList|ValueEncoding|YierdisEncodingThresholds|YierdisHyperLogLog);/import yier.bubu.redis.storage.memory.internal.value.$1;/g' $(rg --files -g '*.java')
```

Run:

```bash
mvn -pl libs/storage/yierdis-storage-memory -Dtest=HashValueTest,ListValueTest,ZSetValueTest,YierdisListpackTest,OffHeapCollectionReadStreamingTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Move storage-memory FFM structures**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/memory/ffm/*.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db\\.memory\\.ffm;/package yier.bubu.redis.storage.memory.internal.ffm;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\.memory\\.ffm\\./import yier.bubu.redis.storage.memory.internal.ffm./g' $(rg --files -g '*.java')
```

Run:

```bash
mvn -pl libs/storage/yierdis-storage-memory -Dtest=YierdisFfmRehashConsistencyTest,UnsafeOffHeapKeyspaceTest,UnsafeOffHeapDbSmokeTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 6: Move storage-memory facade and ops package**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory
git mv libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/db/*.java \
       libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/
```

Rewrite remaining declarations:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory;/' libs/storage/yierdis-storage-memory/src/main/java/yier/bubu/redis/storage/memory/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.db\\./import yier.bubu.redis.storage.memory./g' $(rg --files -g '*.java')
perl -pi -e 's/yier\\.bubu\\.redis\\.db\\./yier.bubu.redis.storage.memory./g' $(rg --files -g '*.java' -g '*.md' -g '*.yml')
```

- [ ] **Step 7: Move storage-memory tests to matching packages**

```bash
mkdir -p libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory
mkdir -p libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm
git mv libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/db/*.java \
       libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/
git mv libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/db/memory/ffm/*.java \
       libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/
perl -pi -e 's/package yier\\.bubu\\.redis\\.db;/package yier.bubu.redis.storage.memory;/' libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.db\\.memory\\.ffm;/package yier.bubu.redis.storage.memory.internal.ffm;/' libs/storage/yierdis-storage-memory/src/test/java/yier/bubu/redis/storage/memory/internal/ffm/*.java
```

- [ ] **Step 8: Run storage-memory and architecture tests**

```bash
mvn -pl libs/storage/yierdis-storage-memory test
mvn -pl tests/yierdis-architecture-tests test
```

Expected:

```text
BUILD SUCCESS
BUILD SUCCESS
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: move storage memory internals under storage namespace"
```

---

### Task 9: Move Runtime And Memory Packages

**Files:**
- Move: `libs/runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/**`
- Move: `libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/*.java`
- Move: `libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/offheap/api/*.java`
- Move: `libs/memory/yierdis-memory-foreign/src/main/java/yier/bubu/redis/db/memory/foreign/*.java`

- [ ] **Step 1: Move memory API and foreign backend**

```bash
mkdir -p libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api
mkdir -p libs/memory/yierdis-memory-foreign/src/main/java/yier/bubu/redis/memory/foreign
git mv libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/offheap/api/*.java \
       libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/
git mv libs/memory/yierdis-memory-foreign/src/main/java/yier/bubu/redis/db/memory/foreign/*.java \
       libs/memory/yierdis-memory-foreign/src/main/java/yier/bubu/redis/memory/foreign/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.offheap\\.api;/package yier.bubu.redis.memory.api;/' libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.db\\.memory\\.foreign;/package yier.bubu.redis.memory.foreign;/' libs/memory/yierdis-memory-foreign/src/main/java/yier/bubu/redis/memory/foreign/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.offheap\\.api\\./import yier.bubu.redis.memory.api./g; s/import yier\\.bubu\\.redis\\.db\\.memory\\.foreign\\./import yier.bubu.redis.memory.foreign./g' $(rg --files -g '*.java')
```

- [ ] **Step 2: Add legacy memory API facades**

Create `libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/offheap/api/OffHeapAllocator.java`:

```java
package yier.bubu.redis.offheap.api;

/**
 * @deprecated use {@link yier.bubu.redis.memory.api.OffHeapAllocator}
 */
@Deprecated(forRemoval = true)
public interface OffHeapAllocator extends yier.bubu.redis.memory.api.OffHeapAllocator {
}
```

Repeat the interface facade pattern for:

```text
OffHeapBuf
OffHeapSlice
```

For `OffHeapOutOfMemoryException`, create a subclass facade:

```java
package yier.bubu.redis.offheap.api;

/**
 * @deprecated use {@link yier.bubu.redis.memory.api.OffHeapOutOfMemoryException}
 */
@Deprecated(forRemoval = true)
public class OffHeapOutOfMemoryException extends yier.bubu.redis.memory.api.OffHeapOutOfMemoryException {
    public OffHeapOutOfMemoryException(String message) {
        super(message);
    }

    public OffHeapOutOfMemoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 3: Move runtime embedded implementation**

```bash
mkdir -p libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/embedded
mkdir -p libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/embedded
git mv libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/*.java \
       libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/embedded/
git mv libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/*.java \
       libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/embedded/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.runtime;/package yier.bubu.redis.runtime.embedded;/' libs/runtime/yierdis-runtime-embedded/src/main/java/yier/bubu/redis/runtime/embedded/*.java libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/runtime/embedded/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.runtime\\.(YierdisInstance|YierdisInstanceMaintenance|YierdisInstanceObservability|YierdisInstanceResources|YierdisInstanceRuntimeAccess|YierdisGlobalMaxmemoryGovernor);/import yier.bubu.redis.runtime.embedded.$1;/g' $(rg --files -g '*.java')
```

- [ ] **Step 4: Move runtime config API**

Move only `YierdisInstanceConfig.java` from runtime root to runtime API subpackage:

```bash
mkdir -p libs/runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api
git mv libs/runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java \
       libs/runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/
```

Rewrite:

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.runtime;/package yier.bubu.redis.runtime.api;/' libs/runtime/yierdis-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.runtime\\.YierdisInstanceConfig;/import yier.bubu.redis.runtime.api.YierdisInstanceConfig;/g; s/yier\\.bubu\\.redis\\.runtime\\.YierdisInstanceConfig/yier.bubu.redis.runtime.api.YierdisInstanceConfig/g' $(rg --files -g '*.java' -g '*.md')
```

- [ ] **Step 5: Run focused tests**

```bash
mvn -pl libs/memory/yierdis-memory-api,libs/memory/yierdis-memory-foreign,libs/runtime/yierdis-runtime-api,libs/runtime/yierdis-runtime-embedded test
mvn -q -DskipTests compile
```

Expected:

```text
BUILD SUCCESS
```

Second command expected: exit code 0.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: move runtime and memory packages"
```

---

### Task 10: Move Protocol Custom V1 Packages

**Files:**
- Move: `libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/**`
- Move: `libs/protocol/yierdis-custom-v1-execution-adapter/src/main/java/yier/bubu/redis/protocol/v1/*.java`
- Move: `libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/netty/*.java`

- [ ] **Step 1: Move wire protocol packages**

```bash
mkdir -p libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/wire
mkdir -p libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/json
mkdir -p libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/reply
git mv libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/v1/*.java \
       libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/wire/
git mv libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/json/*.java \
       libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/json/
git mv libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/reply/*.java \
       libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/reply/
git mv libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/ProtocolLimits.java \
       libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/wire/
```

- [ ] **Step 2: Move execution adapter and Netty packages**

```bash
mkdir -p libs/protocol/yierdis-custom-v1-execution-adapter/src/main/java/yier/bubu/redis/protocol/custom/v1/execution
mkdir -p libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty
git mv libs/protocol/yierdis-custom-v1-execution-adapter/src/main/java/yier/bubu/redis/protocol/v1/*.java \
       libs/protocol/yierdis-custom-v1-execution-adapter/src/main/java/yier/bubu/redis/protocol/custom/v1/execution/
git mv libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/netty/*.java \
       libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty/
```

- [ ] **Step 3: Rewrite package declarations and imports**

```bash
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol\\.v1;/package yier.bubu.redis.protocol.custom.v1.wire;/' libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/wire/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol\\.json;/package yier.bubu.redis.protocol.custom.v1.json;/' libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/json/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol\\.reply;/package yier.bubu.redis.protocol.custom.v1.reply;/' libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/reply/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol;/package yier.bubu.redis.protocol.custom.v1.wire;/' libs/protocol/yierdis-custom-v1-wire/src/main/java/yier/bubu/redis/protocol/custom/v1/wire/ProtocolLimits.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol\\.v1;/package yier.bubu.redis.protocol.custom.v1.execution;/' libs/protocol/yierdis-custom-v1-execution-adapter/src/main/java/yier/bubu/redis/protocol/custom/v1/execution/*.java
perl -pi -e 's/package yier\\.bubu\\.redis\\.protocol\\.netty;/package yier.bubu.redis.protocol.custom.v1.netty;/' libs/protocol/yierdis-custom-v1-netty/src/main/java/yier/bubu/redis/protocol/custom/v1/netty/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.protocol\\.json\\./import yier.bubu.redis.protocol.custom.v1.json./g; s/import yier\\.bubu\\.redis\\.protocol\\.reply\\./import yier.bubu.redis.protocol.custom.v1.reply./g; s/import yier\\.bubu\\.redis\\.protocol\\.netty\\./import yier.bubu.redis.protocol.custom.v1.netty./g; s/import yier\\.bubu\\.redis\\.protocol\\.v1\\./import yier.bubu.redis.protocol.custom.v1.wire./g; s/import yier\\.bubu\\.redis\\.protocol\\.ProtocolLimits;/import yier.bubu.redis.protocol.custom.v1.wire.ProtocolLimits;/g' $(rg --files -g '*.java')
```

After this command, update imports in execution adapter classes so they import wire classes from `protocol.custom.v1.wire` and adapter classes from `protocol.custom.v1.execution`.

- [ ] **Step 4: Run protocol tests**

```bash
mvn -pl libs/protocol/yierdis-custom-v1-wire,libs/protocol/yierdis-custom-v1-execution-adapter,libs/protocol/yierdis-custom-v1-netty test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: move custom protocol packages"
```

---

### Task 11: Move Client And Bench To App Packages

**Files:**
- Move: `apps/yierdis-client/src/main/java/yier/bubu/redis/client/*.java`
- Move: `apps/yierdis-bench/src/main/java/yier/bubu/redis/bench/*.java`
- Move corresponding tests

- [ ] **Step 1: Move client package**

```bash
mkdir -p apps/yierdis-client/src/main/java/yier/bubu/redis/app/client
mkdir -p apps/yierdis-client/src/test/java/yier/bubu/redis/app/client
git mv apps/yierdis-client/src/main/java/yier/bubu/redis/client/*.java \
       apps/yierdis-client/src/main/java/yier/bubu/redis/app/client/
git mv apps/yierdis-client/src/test/java/yier/bubu/redis/client/*.java \
       apps/yierdis-client/src/test/java/yier/bubu/redis/app/client/
perl -pi -e 's/package yier\\.bubu\\.redis\\.client;/package yier.bubu.redis.app.client;/' apps/yierdis-client/src/main/java/yier/bubu/redis/app/client/*.java apps/yierdis-client/src/test/java/yier/bubu/redis/app/client/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.client\\./import yier.bubu.redis.app.client./g; s/yier\\.bubu\\.redis\\.client\\./yier.bubu.redis.app.client./g' $(rg --files -g '*.java' -g '*.md')
```

- [ ] **Step 2: Move bench package**

```bash
mkdir -p apps/yierdis-bench/src/main/java/yier/bubu/redis/app/bench
mkdir -p apps/yierdis-bench/src/test/java/yier/bubu/redis/app/bench
git mv apps/yierdis-bench/src/main/java/yier/bubu/redis/bench/*.java \
       apps/yierdis-bench/src/main/java/yier/bubu/redis/app/bench/
git mv apps/yierdis-bench/src/test/java/yier/bubu/redis/bench/*.java \
       apps/yierdis-bench/src/test/java/yier/bubu/redis/app/bench/
perl -pi -e 's/package yier\\.bubu\\.redis\\.bench;/package yier.bubu.redis.app.bench;/' apps/yierdis-bench/src/main/java/yier/bubu/redis/app/bench/*.java apps/yierdis-bench/src/test/java/yier/bubu/redis/app/bench/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.bench\\./import yier.bubu.redis.app.bench./g; s/yier\\.bubu\\.redis\\.bench\\./yier.bubu.redis.app.bench./g' $(rg --files -g '*.java' -g '*.md')
```

- [ ] **Step 3: Run app tests**

```bash
mvn -pl apps/yierdis-client,apps/yierdis-bench test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: move client and bench packages under app namespace"
```

---

### Task 12: Move Misplaced Command Integration Tests

**Files:**
- Move: `libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/command/*.java`
- Move: `libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/testutil/*.java`
- Modify: `tests/yierdis-integration-tests/pom.xml`

- [ ] **Step 1: Move command behavior tests to integration tests**

```bash
mkdir -p tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command
git mv libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/command/*.java \
       tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/
perl -pi -e 's/package yier\\.bubu\\.redis\\.command;/package yier.bubu.redis.integration.command;/' tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/*.java
```

- [ ] **Step 2: Consolidate runtime test utilities in integration tests**

If `tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil` already exists, move only missing utility files. Use `git mv` for each file that is not already present:

```bash
mkdir -p tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil
for f in libs/runtime/yierdis-runtime-embedded/src/test/java/yier/bubu/redis/testutil/*.java; do
  base="$(basename "$f")"
  if [ ! -f "tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/$base" ]; then
    git mv "$f" "tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/$base"
  fi
done
```

- [ ] **Step 3: Update package imports in moved tests**

```bash
perl -pi -e 's/import yier\\.bubu\\.redis\\.command\\.TestCommandProcessors;/import yier.bubu.redis.integration.command.TestCommandProcessors;/g' tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/*.java
perl -pi -e 's/import yier\\.bubu\\.redis\\.runtime\\.TestDbRouters;/import yier.bubu.redis.integration.runtime.TestDbRouters;/g' tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/*.java
```

If `TestCommandProcessors.java` was moved with the command tests, keep it in `integration.command`.

- [ ] **Step 4: Ensure integration test POM has required test dependencies**

In `tests/yierdis-integration-tests/pom.xml`, ensure these artifacts exist with `<scope>test</scope>`:

```xml
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-command-api</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-command-kernel</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-command-defaults</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-storage-memory</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>yier.bubu.redis</groupId>
            <artifactId>yierdis-runtime-embedded</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 5: Run runtime and integration tests**

```bash
mvn -pl libs/runtime/yierdis-runtime-embedded test
mvn -pl tests/yierdis-integration-tests test
```

Expected:

```text
BUILD SUCCESS
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: move command behavior tests to integration module"
```

---

### Task 13: Tighten Architecture Policy To Target Packages

**Files:**
- Modify: `tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`

- [ ] **Step 1: Replace old forbidden import prefixes**

Update `architecture-policy.yml` so target ownership is enforced. Use these prefixes in `forbidden_imports` for each module family:

```yaml
forbidden_imports:
  - yier.bubu.redis.app.server
  - yier.bubu.redis.protocol.custom.v1.netty
  - io.netty
```

For command modules, include:

```yaml
  - yier.bubu.redis.storage.memory
  - yier.bubu.redis.protocol.custom
  - yier.bubu.redis.app
  - yier.bubu.redis.memory.api
```

For storage-memory, include:

```yaml
  - yier.bubu.redis.command
  - yier.bubu.redis.protocol
  - yier.bubu.redis.execution.executor
  - yier.bubu.redis.app
  - io.netty
```

For execution API and executor, include:

```yaml
  - yier.bubu.redis.command
  - yier.bubu.redis.storage.memory
  - yier.bubu.redis.runtime.embedded
  - yier.bubu.redis.protocol
  - yier.bubu.redis.app
  - io.netty
```

- [ ] **Step 2: Add legacy import rejection for production source**

Add a test method to `ArchitectureBoundaryTest`:

```java
@Test
public void productionSourcesMustNotUseLegacyPackageImportsAfterMigration() throws IOException {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("Cannot locate repository root", repoRoot);

    List<String> offenders = new ArrayList<>();
    int scanned = 0;
    for (String module : List.of(
            "yierdis-command-api",
            "yierdis-command-kernel",
            "yierdis-command-defaults",
            "yierdis-storage-memory",
            "yierdis-runtime-embedded",
            "yierdis-execution-api",
            "yierdis-engine",
            "yierdis-executor-core",
            "yierdis-server-app")) {
        scanned += scanForForbiddenText(
                repoRoot,
                modulePath(repoRoot, module).resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.db.",
                "import yier.bubu.redis.ops.",
                "import yier.bubu.redis.contract.",
                "import yier.bubu.redis.offheap.api.",
                "import yier.bubu.redis.executor.",
                "import yier.bubu.redis.engine."
        );
    }
    Assert.assertTrue("No production Java files scanned", scanned > 0);
    if (!offenders.isEmpty()) {
        Assert.fail("Legacy package imports remain in production source:\n" + String.join("\n", offenders));
    }
}
```

- [ ] **Step 3: Add root server package rejection**

Add this assertion to the existing server-app architecture guard:

```java
int rootServerPackages = scanForForbiddenText(
        repoRoot,
        modulePath(repoRoot, "yierdis-server-app").resolve("src/main/java"),
        offenders,
        "package yier.bubu.redis;"
);
Assert.assertTrue("No server app Java files scanned", rootServerPackages > 0);
```

- [ ] **Step 4: Run architecture tests**

```bash
mvn -pl tests/yierdis-architecture-tests test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml \
        tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
        tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java
git commit -m "test: enforce target package architecture policy"
```

---

### Task 14: Update Documentation And Developer Commands

**Files:**
- Modify: `README.md`
- Modify: `docs/module-architecture.md`
- Modify: `docs/development-navigation.md`
- Modify: `docs/project-overview.md`
- Modify: `docs/request-execution-flow.md`
- Modify: `docs/main-path-walkthrough.md`
- Modify: `docs/db-internals.md`
- Modify: `docs/bytes-and-fast-paths.md`
- Modify: `docs/testing-and-debugging.md`
- Modify: `docs/configuration-and-operations.md`

- [ ] **Step 1: Locate stale paths and packages**

Run:

```bash
rg -n 'yierdis-(memory|bytes|execution|storage|runtime|protocol|command|executor-core|client|bench|architecture-tests|integration-tests)|yierdis-app|yier\\.bubu\\.redis\\.(db|ops|contract|executor|engine|offheap\\.api|command|client|bench)' README.md docs
```

Expected: output lists stale references to update.

- [ ] **Step 2: Update module path examples**

For every file listed in the Files section, update path examples to use:

```text
libs/<area>/<artifact>
apps/<artifact>
tests/<artifact>
```

Keep artifact names unchanged when the text discusses Maven dependencies.

- [ ] **Step 3: Update request flow package references**

In `docs/request-execution-flow.md`, ensure the main chain uses target ownership names:

```text
Netty ByteBuf
  -> protocol.custom.v1.netty.CustomRequestDecoder
  -> protocol.custom.v1.wire request DTO
  -> protocol.custom.v1.netty.ProtocolCommandAdapter
  -> execution.api.ExecutionRequest
  -> app.server.YierdisFastCommandHandler
  -> execution.executor.CommandExecutor
  -> execution.engine.YierdisEngine
  -> command.kernel.YierdisFastCommandProcessor
  -> command.defaults.* command handler
  -> storage.api.DbEngine / DbReads / DbWrites
  -> storage.memory storage
  -> execution.api.ReplyWriter
  -> app.server.NettyExecutionIoAdapter
```

- [ ] **Step 4: Update DB internals package references**

In `docs/db-internals.md`, replace DB implementation package names with:

```text
yier.bubu.redis.storage.memory
yier.bubu.redis.storage.memory.internal.key
yier.bubu.redis.storage.memory.internal.keyspace
yier.bubu.redis.storage.memory.internal.expire
yier.bubu.redis.storage.memory.internal.ledger
yier.bubu.redis.storage.memory.internal.value
yier.bubu.redis.storage.memory.internal.ffm
```

- [ ] **Step 5: Run docs stale reference scan**

```bash
rg -n 'yierdis-app|yier\\.bubu\\.redis\\.(db|ops|contract|executor|engine|offheap\\.api)' README.md docs
```

Expected: no output for production-current docs. Historical specs under `docs/superpowers/specs` may still mention legacy names as history; do not edit old approved specs unless they claim to describe current state.

- [ ] **Step 6: Commit**

```bash
git add README.md docs
git commit -m "docs: update package structure and module paths"
```

---

### Task 15: Full Verification Before Legacy Facade Removal

**Files:**
- No source edits unless verification exposes a compile failure.

- [ ] **Step 1: Run full test suite**

```bash
mvn test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 2: Scan production source for legacy imports**

```bash
rg -n 'import yier\\.bubu\\.redis\\.(contract|ops|offheap\\.api|db|executor|engine)\\.' libs apps tests -g '*.java'
```

Expected: no output and exit code 1. Legacy facade source files themselves may contain target references; the scan pattern checks imports, not package declarations.

- [ ] **Step 3: Scan production package declarations for legacy implementation packages**

```bash
rg -n '^package yier\\.bubu\\.redis\\.(db|executor|engine);|^package yier\\.bubu\\.redis;$' libs apps -g '*.java'
```

Expected: no output and exit code 1.

- [ ] **Step 4: Commit verification-only marker if docs changed during fixes**

If no files changed, do not create an empty commit. If verification required small docs or import fixes:

```bash
git add -A
git commit -m "chore: finish package migration verification fixes"
```

---

### Task 16: Remove Legacy Facades

**Files:**
- Delete: `libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/**`
- Delete: `libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops/**`
- Delete: `libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/offheap/api/**`
- Modify: architecture tests to reject legacy package declarations in production source

- [ ] **Step 1: Confirm removal gate**

Run:

```bash
rg -n 'import yier\\.bubu\\.redis\\.(contract|ops|offheap\\.api)\\.' libs apps tests -g '*.java'
```

Expected: no output and exit code 1.

- [ ] **Step 2: Delete legacy facade packages**

In an implementation session, request explicit user confirmation before this
step. After confirmation, use `git rm -r` so deletion is tracked and reviewable:

```bash
git rm -r libs/execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract
git rm -r libs/storage/yierdis-storage-api/src/main/java/yier/bubu/redis/ops
git rm -r libs/memory/yierdis-memory-api/src/main/java/yier/bubu/redis/offheap/api
```

This is the only destructive step in the plan. Do not run it until Step 1 passes
and the user has confirmed legacy facade removal for the implementation session.

- [ ] **Step 3: Add package declaration rejection**

Extend `productionSourcesMustNotUseLegacyPackageImportsAfterMigration` to also check:

```java
"package yier.bubu.redis.contract;",
"package yier.bubu.redis.ops;",
"package yier.bubu.redis.offheap.api;"
```

- [ ] **Step 4: Run full test suite**

```bash
mvn test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: remove legacy package facades"
```

---

## Final Acceptance Checklist

Run these checks before claiming the rearchitecture is complete:

```bash
mvn test
rg -n '^package yier\\.bubu\\.redis;$' apps libs -g '*.java'
rg -n '^package yier\\.bubu\\.redis\\.db|import yier\\.bubu\\.redis\\.db\\.' apps libs -g '*.java'
rg -n '^package yier\\.bubu\\.redis\\.command;' apps libs -g '*.java'
rg -n '^package yier\\.bubu\\.redis\\.(contract|ops|offheap\\.api);' apps libs -g '*.java'
rg -n 'src/main/java/yier/bubu/redis/(db|command|contract|ops|executor|engine)' apps libs tests docs README.md
```

Expected:

```text
BUILD SUCCESS
```

All `rg` checks should produce no output except historical references inside old
approved specs under `docs/superpowers/specs`.
