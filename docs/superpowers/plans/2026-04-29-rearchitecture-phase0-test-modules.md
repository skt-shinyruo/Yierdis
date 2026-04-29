# Rearchitecture Phase 0 Test Modules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move whole-repo architecture guards and one cross-module integration smoke test out of `yierdis-core-runtime`.

**Architecture:** Add top-level `yierdis-architecture-tests` and `yierdis-integration-tests` Maven modules while the production module graph still uses current names. Architecture tests own source/dependency guard policy; integration tests own cross-module runtime/protocol smoke behavior.

**Tech Stack:** Maven multi-module project, JUnit 4, Java 25, existing source-scanning guard tests.

---

### Task 1: Add Architecture-Test Module Skeleton

**Files:**
- Modify: `pom.xml`
- Create: `yierdis-architecture-tests/pom.xml`
- Create: `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- Create: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java`

- [ ] **Step 1: Write the failing policy resource test**

Create `ArchitecturePolicyResourceTest` with:

```java
package yier.bubu.redis.architecture;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ArchitecturePolicyResourceTest {
    @Test
    public void architecturePolicyResourceNamesCurrentBoundaryRules() throws Exception {
        try (InputStream in = ArchitecturePolicyResourceTest.class.getResourceAsStream("/architecture-policy.yml")) {
            Assert.assertNotNull("missing architecture-policy.yml", in);
            String policy = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue(policy.contains("yierdis-core-command:"));
            Assert.assertTrue(policy.contains("yierdis-executor-core:"));
            Assert.assertTrue(policy.contains("yierdis-server:"));
            Assert.assertTrue(policy.contains("forbidden_imports:"));
            Assert.assertTrue(policy.contains("yier.bubu.redis.protocol.reply"));
        }
    }
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests -Dtest=ArchitecturePolicyResourceTest test
```

Expected: FAIL because `yierdis-architecture-tests` is not yet a module.

- [ ] **Step 3: Add the Maven module and policy resource**

Add `yierdis-architecture-tests` to the root `pom.xml` module list after `yierdis-bench`.

Create `yierdis-architecture-tests/pom.xml` as a jar module depending on JUnit, `yierdis-core-db`, `yierdis-core-runtime`, `yierdis-core-command`, and `yierdis-protocol-codec` with test scope.

Create `architecture-policy.yml` with current-module rules for `yierdis-core-command`, `yierdis-executor-core`, `yierdis-server`, `yierdis-protocol-codec`, and `yierdis-core-runtime`.

- [ ] **Step 4: Run the focused test and verify it passes**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests -am -Dtest=ArchitecturePolicyResourceTest test
```

Expected: PASS.

### Task 2: Move Architecture Guards Out Of Runtime

**Files:**
- Move: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java` to `yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Move: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java` to `yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java`
- Move: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java` to `yierdis-architecture-tests/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`

- [ ] **Step 1: Move the three existing guard tests with `git mv`**

Run:

```bash
mkdir -p yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol
mkdir -p yierdis-architecture-tests/src/test/java/yier/bubu/redis/db
git mv yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git mv yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java yierdis-architecture-tests/src/test/java/yier/bubu/redis/protocol/ReplySsoTGuardTest.java
git mv yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java yierdis-architecture-tests/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java
```

- [ ] **Step 2: Run the moved architecture tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests -am -Dtest=ArchitectureBoundaryTest,ReplySsoTGuardTest,YierdisDbArchitectureGuardTest test
```

Expected: PASS.

- [ ] **Step 3: Verify runtime no longer owns whole-repo architecture guards**

Run:

```bash
rg -n "class ArchitectureBoundaryTest|class ReplySsoTGuardTest|class YierdisDbArchitectureGuardTest" yierdis-core/yierdis-core-runtime/src/test/java
```

Expected: no matches.

### Task 3: Add Integration-Test Module And Move Runtime Smoke

**Files:**
- Modify: `pom.xml`
- Create: `yierdis-integration-tests/pom.xml`
- Move: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java` to `yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java`
- Create: `yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/TestDbRouters.java`

- [ ] **Step 1: Add integration module dependencies**

Create `yierdis-integration-tests/pom.xml` as a jar module depending on JUnit, `yierdis-core-runtime`, `yierdis-core-command`, `yierdis-core-db`, and `yierdis-core-contract` with test scope. Add `yierdis-integration-tests` to the root `pom.xml`.

- [ ] **Step 2: Move the smoke test and copy the package-local helper**

Move `ContractsIntegrationSmokeTest.java` with `git mv`.

Create `TestDbRouters.java` in the integration module with the same package and content as the runtime test helper so the moved smoke test remains package-local and does not force runtime test helpers into a public API.

- [ ] **Step 3: Run the moved smoke test**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-integration-tests -am -Dtest=ContractsIntegrationSmokeTest test
```

Expected: PASS.

### Task 4: Phase 0 Verification

**Files:**
- No new source files.

- [ ] **Step 1: Run focused Phase 0 tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests,yierdis-integration-tests -am test
```

Expected: PASS.

- [ ] **Step 2: Run runtime focused tests to confirm moved tests are no longer required there**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-core/yierdis-core-runtime -am -Dtest=YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add pom.xml yierdis-architecture-tests yierdis-integration-tests yierdis-core/yierdis-core-runtime/src/test/java
git commit -m "test: split architecture and integration test modules"
```
