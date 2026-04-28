# Maxmemory Policy Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace duplicate maxmemory policy models with the single core API enum `yier.bubu.redis.ops.MaxmemoryPolicy`.

**Architecture:** Keep CLI options as strings at the picocli boundary, then convert once into the core API enum. Carry that typed enum through args runtime config, instance config, DB factory SPI, DB construction, local DB eviction, and global maxmemory governance; stringify only at argv/INFO output boundaries.

**Tech Stack:** Java 25, Maven, JUnit 4, picocli, Yierdis core-api/core-db/core-runtime/args/server modules.

---

## Source Design

This plan implements:

- `docs/superpowers/specs/2026-04-27-maxmemory-policy-unification-design.md`

Work in this order:

1. Add a normalized output name to the core enum.
2. Move args runtime config to the core enum.
3. Type the runtime config and DB factory SPI, using a temporary DB string bridge.
4. Remove the DB-local maxmemory enum and DB-local switch parser.
5. Update server wiring and output rendering.
6. Run final duplication guards and full verification.

## File Map

### Create

- `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/DbEngineFactoryPolicyContractTest.java`
  Reflection guard that the DB factory SPI receives `MaxmemoryPolicy`.

### Modify

- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java`
  Add normalized Redis/config output name.
- `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/MaxmemoryPolicyTest.java`
  Cover normalized output names.
- `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
  Change factory policy parameter from `String` to `MaxmemoryPolicy`.
- `yierdis-args/pom.xml`
  Add `yierdis-core-api` dependency.
- `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
  Replace nested maxmemory enum with the core enum.
- `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
  Parse CLI policy through the core enum and serialize with `redisName()`.
- `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
  Assert runtime config uses the core enum and underscore aliases normalize.
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
  Store typed maxmemory policy and keep deprecated string compatibility setter.
- `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
  Pass typed policy to DB factories and global governor.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
  Accept typed policy from SPI; initially bridge to existing string constructors, then pass typed policy after DB migration.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  Remove nested enum, remove duplicate switch parser, and store the core enum.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java`
  Store and compare the core enum.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
  Store and compare the core enum.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/TestDbs.java`
  Add typed maxmemory helper and keep string compatibility helper.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java`
  Update test factory signature and assert typed policy.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
  Cover typed builder and deprecated string compatibility.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`
  Prefer typed policy in direct builder usage.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java`
  Prefer typed policy in direct builder usage.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
  Guard that DB no longer declares or stores a local maxmemory enum.
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
  Pass typed policy to instance config.
- `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
  Render maxmemory policy with `redisName()`.
- `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`
  Assert parsed server config uses the core enum and accepts underscore aliases.
- `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`
  Use the core enum in test runtime config construction.

## Task 1: Add Core Enum Output Name

**Files:**
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java`
- Modify: `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/MaxmemoryPolicyTest.java`

- [ ] **Step 1: Write the failing test**

Add this test method to `MaxmemoryPolicyTest`:

```java
@Test
public void redisName_shouldReturnNormalizedConfigNames() {
    Assert.assertEquals("noeviction", MaxmemoryPolicy.NOEVICTION.redisName());
    Assert.assertEquals("allkeys-random", MaxmemoryPolicy.ALLKEYS_RANDOM.redisName());
    Assert.assertEquals("allkeys-lru", MaxmemoryPolicy.ALLKEYS_LRU.redisName());
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=MaxmemoryPolicyTest test
```

Expected: FAIL at test compilation with `cannot find symbol: method redisName()`.

- [ ] **Step 3: Implement normalized output names**

Replace the enum constants and add the field, constructor, and method in `MaxmemoryPolicy`:

```java
public enum MaxmemoryPolicy {
    NOEVICTION("noeviction"),
    ALLKEYS_RANDOM("allkeys-random"),
    ALLKEYS_LRU("allkeys-lru");

    private final String redisName;

    MaxmemoryPolicy(String redisName) {
        this.redisName = redisName;
    }

    public String redisName() {
        return redisName;
    }
```

Keep the existing `parse(String)` method and its current switch behavior.

- [ ] **Step 4: Run the focused test and verify it passes**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=MaxmemoryPolicyTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java \
        yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/MaxmemoryPolicyTest.java
git commit -m "refactor: add maxmemory policy redis names"
```

## Task 2: Move Args Runtime Config To Core Policy Enum

**Files:**
- Modify: `yierdis-args/pom.xml`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`
- Modify: `yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`
- Modify: `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`

- [ ] **Step 1: Write failing args tests**

In `YierdisServerArgsTest`, add this import:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the existing maxmemory assertion in `normalizedArgsConvertToRuntimeConfigWithoutLegacyOffheapFields`:

```java
Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, runtimeConfig.get("maxmemoryPolicy"));
```

Add this test method:

```java
@Test
public void normalizeAcceptsCorePolicyUnderscoreAliases() {
    YierdisServerArgs args = parse("--maxmemoryPolicy", "ALLKEYS_RANDOM");

    args.normalizeAndValidate();

    Assert.assertEquals("allkeys-random", args.maxmemoryPolicy);
    Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, args.toRuntimeConfig().maxmemoryPolicy());
}
```

- [ ] **Step 2: Run args tests and verify they fail**

Run:

```bash
jdk25 mvn -pl yierdis-args test
```

Expected: FAIL because `yierdis-args` does not depend on `yierdis-core-api`, and the runtime config still returns `YierdisServerRuntimeConfig.MaxmemoryPolicy`.

- [ ] **Step 3: Add the args dependency on core API**

In `yierdis-args/pom.xml`, add this dependency inside `<dependencies>`:

```xml
<dependency>
    <groupId>yier.bubu.redis</groupId>
    <artifactId>yierdis-core-api</artifactId>
</dependency>
```

- [ ] **Step 4: Change runtime config to use the core enum**

In `YierdisServerRuntimeConfig`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Keep the record component named `maxmemoryPolicy`, but let it resolve to the imported core enum:

```java
long maxmemoryBytes,
MaxmemoryScope maxmemoryScope,
MaxmemoryPolicy maxmemoryPolicy,
int maxmemorySamples,
```

Delete the entire nested `public enum MaxmemoryPolicy` block from `YierdisServerRuntimeConfig`.

- [ ] **Step 5: Convert CLI policy through the core parser**

In `YierdisServerArgs`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

In `toRuntimeConfig()`, replace the maxmemory policy argument with:

```java
MaxmemoryPolicy.parse(maxmemoryPolicy),
```

Replace `normalizeMaxmemoryPolicy` with:

```java
private static String normalizeMaxmemoryPolicy(String rawValue) {
    return MaxmemoryPolicy.parse(rawValue).redisName();
}
```

Leave `normalizeExecutorSchedulingPolicy` and `normalizeMaxmemoryScope` unchanged.

- [ ] **Step 6: Run args tests and verify they pass**

Run:

```bash
jdk25 mvn -pl yierdis-args test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-args/pom.xml \
        yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java \
        yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java \
        yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java
git commit -m "refactor: use core maxmemory policy in args config"
```

## Task 3: Type Runtime Config And DB Factory SPI

**Files:**
- Create: `yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/DbEngineFactoryPolicyContractTest.java`
- Modify: `yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`

- [ ] **Step 1: Write the SPI contract test**

Create `DbEngineFactoryPolicyContractTest.java`:

```java
package yier.bubu.redis.ops;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class DbEngineFactoryPolicyContractTest {
    @Test
    public void createReceivesDomainMaxmemoryPolicy() throws Exception {
        Method create = DbEngineFactory.class.getMethod(
                "create",
                int.class,
                long.class,
                MaxmemoryPolicy.class,
                int.class,
                long.class,
                long.class
        );

        Assert.assertEquals(RuntimeDbEngine.class, create.getReturnType());
        Assert.assertEquals(MaxmemoryPolicy.class, create.getParameterTypes()[2]);
    }
}
```

- [ ] **Step 2: Add runtime tests for typed builder behavior**

In `YierdisInstanceTest`, add this import:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Add this test method:

```java
@Test
public void maxmemoryPolicyBuilderUsesDomainEnumAndKeepsStringCompatibility() {
    YierdisInstanceConfig typed = YierdisInstanceConfig.builder()
            .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
            .build();
    Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, typed.maxmemoryPolicy());

    YierdisInstanceConfig defaulted = YierdisInstanceConfig.builder()
            .maxmemoryPolicy((MaxmemoryPolicy) null)
            .build();
    Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, defaulted.maxmemoryPolicy());

    YierdisInstanceConfig legacyString = YierdisInstanceConfig.builder()
            .maxmemoryPolicy("ALLKEYS_RANDOM")
            .build();
    Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, legacyString.maxmemoryPolicy());

    YierdisInstanceConfig legacyBlank = YierdisInstanceConfig.builder()
            .maxmemoryPolicy(" ")
            .build();
    Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, legacyBlank.maxmemoryPolicy());
}
```

- [ ] **Step 3: Run focused core/runtime tests and verify they fail**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=DbEngineFactoryPolicyContractTest test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisInstanceTest,DbEngineFactoryInjectionTest test
```

Expected: FAIL because `DbEngineFactory.create` still accepts `String`, and `YierdisInstanceConfig.maxmemoryPolicy()` still returns `String`.

- [ ] **Step 4: Change `DbEngineFactory` to typed policy**

In `DbEngineFactory.java`, change the third parameter:

```java
RuntimeDbEngine create(
        int dbIndex,
        long maxmemoryBytes,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis
);
```

No import is needed because `MaxmemoryPolicy` is in the same package.

- [ ] **Step 5: Change `YierdisInstanceConfig` to typed policy**

In `YierdisInstanceConfig.java`, replace the `java.util.Locale` import with:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the field, getter, and builder field:

```java
private final MaxmemoryPolicy maxmemoryPolicy;
```

```java
public MaxmemoryPolicy maxmemoryPolicy() {
    return maxmemoryPolicy;
}
```

```java
private MaxmemoryPolicy maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
```

Replace the builder setter with these two overloads:

```java
public Builder maxmemoryPolicy(MaxmemoryPolicy maxmemoryPolicy) {
    this.maxmemoryPolicy = maxmemoryPolicy;
    return this;
}

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

In `build()`, replace string normalization with:

```java
MaxmemoryPolicy policy = maxmemoryPolicy == null ? MaxmemoryPolicy.NOEVICTION : maxmemoryPolicy;

Builder normalized = this;
normalized.databases = dbs;
normalized.maxmemoryScope = scope;
normalized.maxmemoryPolicy = policy;
return new YierdisInstanceConfig(normalized);
```

- [ ] **Step 6: Pass typed policy through `YierdisInstance`**

In `YierdisInstance.java`, keep the existing `MaxmemoryPolicy` import and replace:

```java
config.maxmemoryPolicy(),
```

as the third argument to `engineFactory.create(...)`.

Replace the global governor policy argument:

```java
config.maxmemoryPolicy(),
```

Remove this old expression:

```java
MaxmemoryPolicy.parse(config.maxmemoryPolicy())
```

- [ ] **Step 7: Temporarily bridge `YierdisDbEngineFactory` to DB string constructors**

In `YierdisDbEngineFactory.java`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the `create` signature:

```java
public RuntimeDbEngine create(
        int dbIndex,
        long maxmemoryBytes,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis
)
```

At the top of the method body, add:

```java
String policyName = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy").redisName();
```

Pass `policyName` to the current `YierdisDb.createWithOwnedFfmRuntime(...)` and
`YierdisDb.createWithSharedFfmRuntime(...)` calls. Task 4 removes this temporary
bridge after DB constructors become typed.

- [ ] **Step 8: Update runtime test factories and direct builder calls**

In `DbEngineFactoryInjectionTest`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Also add:

```java
import java.util.concurrent.atomic.AtomicReference;
```

Change every anonymous/lambda factory third parameter type from `String maxmemoryPolicy` to:

```java
MaxmemoryPolicy maxmemoryPolicy
```

In `createUsesInjectedFactory`, capture and assert the typed policy:

```java
AtomicReference<MaxmemoryPolicy> receivedPolicy = new AtomicReference<>();
```

Inside the factory:

```java
receivedPolicy.set(maxmemoryPolicy);
```

After instance creation assertions:

```java
Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, receivedPolicy.get());
```

In `YierdisInstanceTest`, `ContractsIntegrationSmokeTest`, and
`GlobalMaxmemoryLruAcrossDbsTest`, replace direct builder calls such as:

```java
.maxmemoryPolicy("noeviction")
```

with:

```java
.maxmemoryPolicy(MaxmemoryPolicy.NOEVICTION)
```

and replace:

```java
.maxmemoryPolicy("allkeys-lru")
```

with:

```java
.maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
```

Add `import yier.bubu.redis.ops.MaxmemoryPolicy;` to each modified test file.

- [ ] **Step 9: Run focused tests and verify they pass**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=DbEngineFactoryPolicyContractTest test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisInstanceTest,DbEngineFactoryInjectionTest,ContractsIntegrationSmokeTest,GlobalMaxmemoryLruAcrossDbsTest test
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java \
        yierdis-core/yierdis-core-api/src/test/java/yier/bubu/redis/ops/DbEngineFactoryPolicyContractTest.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java \
        yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/DbEngineFactoryInjectionTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/ContractsIntegrationSmokeTest.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/GlobalMaxmemoryLruAcrossDbsTest.java
git commit -m "refactor: type maxmemory policy through runtime SPI"
```

## Task 4: Remove DB-Local Policy Enum

**Files:**
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/TestDbs.java`
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`

- [ ] **Step 1: Write the DB architecture guard**

In `YierdisDbArchitectureGuardTest`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Add this test method:

```java
@Test
public void maxmemoryPolicyMustUseCoreApiEnumOnly() {
    Assert.assertNull(findDeclaredClass(YierdisDb.class, "MaxmemoryPolicy"));
    Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDb.class, "maxmemoryPolicy"));
    Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDbMemoryLedger.class, "maxmemoryPolicy"));
    Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDbMaxmemorySupport.class, "maxmemoryPolicy"));
    Assert.assertNull(findDeclaredMethod(YierdisDb.class, "parseMaxmemoryPolicy", String.class));
}
```

Add these helper methods near `findDeclaredMethod`:

```java
private static Class<?> findDeclaredClass(Class<?> type, String simpleName) {
    for (Class<?> nested : type.getDeclaredClasses()) {
        if (nested.getSimpleName().equals(simpleName)) {
            return nested;
        }
    }
    return null;
}

private static Class<?> fieldType(Class<?> type, String fieldName) {
    try {
        java.lang.reflect.Field field = type.getDeclaredField(fieldName);
        return field.getType();
    } catch (NoSuchFieldException e) {
        Assert.fail("missing field " + type.getName() + "." + fieldName);
        return null;
    }
}
```

- [ ] **Step 2: Run the guard and verify it fails**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test
```

Expected: FAIL because `YierdisDb` still declares `YierdisDb.MaxmemoryPolicy` and the field types still use it.

- [ ] **Step 3: Change `YierdisDb` to store the core enum**

In `YierdisDb.java`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Delete this nested enum:

```java
public enum MaxmemoryPolicy {
    NOEVICTION,
    ALLKEYS_RANDOM,
    ALLKEYS_LRU
}
```

Keep the field name but let it use the imported core enum:

```java
private final MaxmemoryPolicy maxmemoryPolicy;
```

Add these compatibility helpers near the constructor section:

```java
private static MaxmemoryPolicy defaultMaxmemoryPolicy(MaxmemoryPolicy policy) {
    return policy == null ? MaxmemoryPolicy.NOEVICTION : policy;
}

private static MaxmemoryPolicy compatibilityMaxmemoryPolicy(String policy) {
    if (policy == null || policy.isBlank()) {
        return MaxmemoryPolicy.NOEVICTION;
    }
    return MaxmemoryPolicy.parse(policy);
}
```

Delete the old `parseMaxmemoryPolicy(String)` method.

Change the runtime participant overrides to use the imported core enum:

```java
public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
    checkThread();
    return maxmemorySupport.sampleCandidate(policy, nowMillis);
}

public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
    checkThread();
    return maxmemorySupport.scanBestCandidate(policy, nowMillis);
}
```

- [ ] **Step 4: Add typed DB constructors and keep deprecated string adapters**

Change the no-arg/default constructors to use the enum:

```java
public YierdisDb() {
    this(new YierdisFfmMemoryRuntime("db"), true, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
}

public YierdisDb(OffHeapAllocator offHeapAllocator) {
    this(offHeapAllocator, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
}
```

Change private constructor/static factory/public constructor parameters from
`String maxmemoryPolicy` to `MaxmemoryPolicy maxmemoryPolicy`.

For source compatibility, keep these deprecated adapters:

```java
@Deprecated
public static YierdisDb createWithSharedFfmRuntime(
        YierdisFfmMemoryRuntime memoryRuntime,
        long maxmemoryBytes,
        String maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis
) {
    return createWithSharedFfmRuntime(
            memoryRuntime,
            maxmemoryBytes,
            compatibilityMaxmemoryPolicy(maxmemoryPolicy),
            maxmemorySamples,
            evictionTimeLimitMillis,
            expireCleanupTimeLimitMillis
    );
}

@Deprecated
public static YierdisDb createWithOwnedFfmRuntime(
        long maxmemoryBytes,
        String maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis
) {
    return createWithOwnedFfmRuntime(
            maxmemoryBytes,
            compatibilityMaxmemoryPolicy(maxmemoryPolicy),
            maxmemorySamples,
            evictionTimeLimitMillis,
            expireCleanupTimeLimitMillis
    );
}

@Deprecated
public YierdisDb(
        OffHeapAllocator offHeapAllocator,
        long maxmemoryBytes,
        String maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis
) {
    this(
            offHeapAllocator,
            maxmemoryBytes,
            compatibilityMaxmemoryPolicy(maxmemoryPolicy),
            maxmemorySamples,
            evictionTimeLimitMillis,
            expireCleanupTimeLimitMillis
    );
}
```

Inside the main constructor, replace:

```java
this.maxmemoryPolicy = parseMaxmemoryPolicy(maxmemoryPolicy);
```

with:

```java
this.maxmemoryPolicy = defaultMaxmemoryPolicy(maxmemoryPolicy);
```

Keep `lruEnabled` as:

```java
this.lruEnabled = maxmemoryBytes > 0 && this.maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU;
```

- [ ] **Step 5: Change DB maxmemory collaborators to the core enum**

In `YierdisDbMemoryLedger.java`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the field and constructor parameter:

```java
private final MaxmemoryPolicy maxmemoryPolicy;
```

```java
MaxmemoryPolicy maxmemoryPolicy,
```

Change the noeviction comparison:

```java
if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
```

In `YierdisDbMaxmemorySupport.java`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the field and constructor parameter:

```java
private final MaxmemoryPolicy maxmemoryPolicy;
```

```java
MaxmemoryPolicy maxmemoryPolicy,
```

Change method signatures:

```java
MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis)
```

```java
MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis)
```

Change comparisons:

```java
if (policy == null || policy == MaxmemoryPolicy.NOEVICTION) {
```

```java
long lruClock = policy == MaxmemoryPolicy.ALLKEYS_LRU ? e.lruClock : 0L;
```

```java
if (policy != MaxmemoryPolicy.ALLKEYS_LRU) {
```

```java
if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
```

```java
if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
```

- [ ] **Step 6: Remove the temporary factory string bridge**

In `YierdisDbEngineFactory.java`, delete:

```java
String policyName = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy").redisName();
```

Pass the typed `maxmemoryPolicy` directly to `YierdisDb.createWithOwnedFfmRuntime(...)` and `YierdisDb.createWithSharedFfmRuntime(...)`.

- [ ] **Step 7: Add typed test utility overload**

In `TestDbs.java`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Replace the existing helper with this typed helper plus a deprecated string adapter:

```java
public static void forEachDbWithMaxmemory(long maxmemoryBytes, MaxmemoryPolicy maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test) {
    Objects.requireNonNull(test, "test");
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("test-db")) {
        YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, 5, 5);
        try {
            db.bindToCurrentThread();
            test.accept(db);
        } finally {
            db.shutdown();
        }
    }
}

@Deprecated
public static void forEachDbWithMaxmemory(long maxmemoryBytes, String maxmemoryPolicy, int maxmemorySamples, Consumer<YierdisDb> test) {
    MaxmemoryPolicy policy = maxmemoryPolicy == null || maxmemoryPolicy.isBlank()
            ? MaxmemoryPolicy.NOEVICTION
            : MaxmemoryPolicy.parse(maxmemoryPolicy);
    forEachDbWithMaxmemory(maxmemoryBytes, policy, maxmemorySamples, test);
}
```

- [ ] **Step 8: Run maxmemory and guard tests**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest,MaxmemoryEvictionTest,TtlMaxmemoryTest,GlobalMaxmemoryLruAcrossDbsTest test
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryLedger.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java \
        yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbEngineFactory.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/testutil/TestDbs.java \
        yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java
git commit -m "refactor: remove db-local maxmemory policy"
```

## Task 5: Update Server Wiring And Output Rendering

**Files:**
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`
- Modify: `yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java`
- Modify: `yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java`

- [ ] **Step 1: Write failing server config assertions**

In `ServerConfigArgsTest`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the existing normalized config assertion:

```java
Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, runtimeConfig.get("maxmemoryPolicy"));
```

Add this test method:

```java
@Test
public void maxmemoryPolicyUnderscoreInputNormalizesToCoreEnum() {
    ServerConfig config = ServerConfig.fromArgs(new String[]{
            "--maxmemoryPolicy", "ALLKEYS_RANDOM"
    });

    Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_RANDOM, config.runtimeConfig().maxmemoryPolicy());
}
```

In `YierdisServerBootstrapCommandWiringTest`, add:

```java
import yier.bubu.redis.ops.MaxmemoryPolicy;
```

Change the test runtime config constructor argument:

```java
MaxmemoryPolicy.NOEVICTION,
```

- [ ] **Step 2: Run server tests and verify they fail**

Run:

```bash
jdk25 mvn -pl yierdis-server -Dtest=ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: FAIL because server code still calls `argvValue()` on maxmemory policy and tests still expose old enum references until implementation is complete.

- [ ] **Step 3: Pass typed policy in server bootstrap**

In `YierdisServerBootstrap.java`, replace:

```java
.maxmemoryPolicy(runtimeConfig.maxmemoryPolicy().argvValue())
```

with:

```java
.maxmemoryPolicy(runtimeConfig.maxmemoryPolicy())
```

- [ ] **Step 4: Render INFO output using `redisName()`**

In `NettyServerInfoProvider.java`, replace:

```java
sb.append("maxmemory_policy:").append(config.maxmemoryPolicy().argvValue()).append("\r\n");
```

with:

```java
sb.append("maxmemory_policy:").append(config.maxmemoryPolicy().redisName()).append("\r\n");
```

Leave `config.maxmemoryScope().argvValue()` unchanged because maxmemory scope is not part of this policy unification.

- [ ] **Step 5: Run server tests and verify they pass**

Run:

```bash
jdk25 mvn -pl yierdis-server -Dtest=ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java \
        yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java \
        yierdis-server/src/test/java/yier/bubu/redis/ServerConfigArgsTest.java \
        yierdis-server/src/test/java/yier/bubu/redis/YierdisServerBootstrapCommandWiringTest.java
git commit -m "refactor: pass typed maxmemory policy through server"
```

## Task 6: Final Duplication Scan And Full Verification

**Files:**
- No production file changes expected.
- Modify the previous task's files only if this task finds a real miss.

- [ ] **Step 1: Scan for duplicate policy models**

Run:

```bash
rg -n "enum MaxmemoryPolicy" yierdis-core yierdis-args yierdis-server
```

Expected: exactly one production enum hit:

```text
yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java:8:public enum MaxmemoryPolicy {
```

- [ ] **Step 2: Scan for stale maxmemory policy references**

Run:

```bash
rg -n "YierdisDb\\.MaxmemoryPolicy|YierdisServerRuntimeConfig\\.MaxmemoryPolicy|parseMaxmemoryPolicy|maxmemoryPolicy\\(\\)\\.argvValue\\(\\)" yierdis-core yierdis-args yierdis-server
```

Expected: no output.

- [ ] **Step 3: Run focused module verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=MaxmemoryPolicyTest,DbEngineFactoryPolicyContractTest test
jdk25 mvn -pl yierdis-args test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisInstanceTest,DbEngineFactoryInjectionTest,YierdisDbArchitectureGuardTest,MaxmemoryEvictionTest,TtlMaxmemoryTest,GlobalMaxmemoryLruAcrossDbsTest,ContractsIntegrationSmokeTest test
jdk25 mvn -pl yierdis-server -Dtest=ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest test
```

Expected: all commands PASS.

- [ ] **Step 4: Run the full suite**

Run:

```bash
jdk25 mvn test
```

Expected: PASS.

- [ ] **Step 5: Close the verification task**

If Step 1-4 required a code correction, return to the task that owns the missed
file and repeat that task's test and commit step. If Step 1-4 made no code
changes, leave the worktree unchanged and record the passing commands in the
final handoff.

Expected: no extra commit from this task unless a missed implementation detail
was fixed by rerunning the owning task.

## Completion Checklist

- [ ] `MaxmemoryPolicy.redisName()` exists and tests pass.
- [ ] `YierdisServerRuntimeConfig.maxmemoryPolicy()` returns `yier.bubu.redis.ops.MaxmemoryPolicy`.
- [ ] `YierdisServerRuntimeConfig` no longer declares a nested `MaxmemoryPolicy`.
- [ ] `YierdisInstanceConfig.maxmemoryPolicy()` returns `yier.bubu.redis.ops.MaxmemoryPolicy`.
- [ ] `DbEngineFactory.create(...)` accepts `MaxmemoryPolicy`.
- [ ] `YierdisDb` no longer declares `YierdisDb.MaxmemoryPolicy`.
- [ ] `YierdisDb` no longer declares `parseMaxmemoryPolicy(String)`.
- [ ] Local DB eviction and global eviction both compare the same enum type.
- [ ] CLI normalization accepts `ALLKEYS_RANDOM` and serializes it as `allkeys-random`.
- [ ] INFO output still renders `maxmemory_policy:noeviction`, `maxmemory_policy:allkeys-random`, or `maxmemory_policy:allkeys-lru`.
- [ ] Full `jdk25 mvn test` passes.
