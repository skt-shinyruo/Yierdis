# JDK 25 Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the full Maven reactor to JDK 25 while keeping the `foreign` off-heap backend buildable and runnable.

**Architecture:** Upgrade the shared Maven release level to 25, migrate the `foreign` backend from Java 17 incubator APIs to the JDK 25 `java.lang.foreign` API, and remove the old module-relaunch startup path that only existed for `jdk.incubator.foreign`. Keep provider discovery, CLI options, and off-heap semantics intact.

**Tech Stack:** Java 25, Maven multi-module reactor, JUnit 4, Netty

---

### Task 1: Lock In JDK 25 Runtime Expectations With Tests

**Files:**
- Modify: `yierdis-memory/api/src/test/java/yier/bubu/redis/db/memory/api/YierdisOffHeapAllocatorsTest.java`
- Create: `yierdis-server/src/test/java/yier/bubu/redis/ForeignMemoryAutoModulesTest.java`

- [ ] **Step 1: Tighten the allocator-provider expectation for `foreign`**

```java
@Test
public void createForeignAllocatorWhenProviderIsPresent() {
    boolean foreignPresent = hasProvider(YierdisOffHeapBackend.FOREIGN);
    if (!foreignPresent) {
        Assert.fail("foreign provider should be present in the default build");
    }
    try (OffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
        Assert.assertNotNull(allocator);
    }
}
```

- [ ] **Step 2: Add a server test proving no relaunch is needed for `foreign`**

```java
@Test
public void doesNotRelaunchWhenForeignApiIsAvailable() {
    YierdisServerRuntimeConfig config = YierdisServerRuntimeConfig.builder()
            .offheapBackend(YierdisServerRuntimeConfig.OffheapBackend.FOREIGN)
            .build();

    Assert.assertNull(ForeignMemoryAutoModules.maybeRelaunchIfNeeded(config, new String[0]));
}
```

- [ ] **Step 3: Run focused tests to verify RED**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/api,yierdis-server -am -Dtest=YierdisOffHeapAllocatorsTest,ForeignMemoryAutoModulesTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: FAIL because the current `foreign` implementation and server startup path are still tied to Java 17 incubator behavior.

### Task 2: Migrate Build Configuration and Foreign Backend Implementation

**Files:**
- Modify: `pom.xml`
- Modify: `yierdis-memory/foreign/pom.xml`
- Modify: `yierdis-memory/foreign/src/main/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocator.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/ForeignMemoryAutoModules.java`

- [ ] **Step 1: Upgrade the Maven release baseline**

```xml
<maven.compiler.release>25</maven.compiler.release>
```

- [ ] **Step 2: Remove incubator-only compiler and surefire flags from the foreign module**

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <release>${maven.compiler.release}</release>
        <encoding>${project.build.sourceEncoding}</encoding>
    </configuration>
</plugin>
```

- [ ] **Step 3: Rewrite the foreign allocator to use `Arena` and `ValueLayout.JAVA_BYTE`**

```java
Arena arena = Arena.ofConfined();
MemorySegment segment = arena.allocate(capacity);
segment.set(ValueLayout.JAVA_BYTE, index, value);
byte b = segment.get(ValueLayout.JAVA_BYTE, index);
```

- [ ] **Step 4: Replace module relaunch behavior with availability validation**

```java
if (config.offheapBackend() != YierdisServerRuntimeConfig.OffheapBackend.FOREIGN) {
    return null;
}
try {
    Class.forName("java.lang.foreign.Arena");
    return null;
} catch (ClassNotFoundException e) {
    throw YierdisCliException.userError(
            "当前 JVM 不支持 java.lang.foreign，无法启用 --offheapBackend foreign。请使用 JDK 25 运行，或改用 --offheapBackend unsafe/netty。",
            e
    );
}
```

- [ ] **Step 5: Run the focused tests to verify GREEN**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/api,yierdis-server -am -Dtest=YierdisOffHeapAllocatorsTest,ForeignMemoryAutoModulesTest -Dsurefire.failIfNoSpecifiedTests=false test`
Expected: PASS

### Task 3: Update Foreign-Backend Tests and README

**Files:**
- Modify: `yierdis-memory/foreign/src/test/java/yier/bubu/redis/db/memory/foreign/YierdisForeignOffHeapAllocatorTest.java`
- Modify: `README.md`

- [ ] **Step 1: Keep the foreign allocator contract tests aligned with the new baseline**

```java
@Test
public void factoryCreatesForeignAllocatorWhenAvailable() {
    try (OffHeapAllocator allocator = YierdisOffHeapAllocators.create("foreign", 0)) {
        Assert.assertNotNull(allocator);
        Assert.assertTrue(allocator instanceof YierdisForeignOffHeapAllocator);
    }
}
```

- [ ] **Step 2: Rewrite README environment and off-heap instructions for JDK 25**

```markdown
- JDK 25
- `foreign`: 基于 JDK 25 正式 `java.lang.foreign` FFM API
```

- [ ] **Step 3: Run focused foreign-module tests**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/foreign -am test`
Expected: PASS

- [ ] **Step 4: Run repository build verification**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -DskipTests package`
Expected: PASS
