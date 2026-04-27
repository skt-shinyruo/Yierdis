# Maxmemory Policy Unification Design

## Summary

Unify maxmemory eviction policy modeling on the existing core API enum
`yier.bubu.redis.ops.MaxmemoryPolicy`.

After this change, CLI parsing remains a command-line concern, but runtime and DB
assembly pass a typed domain policy instead of re-parsing strings. The DB module
no longer declares its own `YierdisDb.MaxmemoryPolicy`, and `yierdis-args` no
longer declares a separate maxmemory policy enum.

## Problem Statement

The repository currently has three maxmemory policy models:

- `yierdis-core-api`: `yier.bubu.redis.ops.MaxmemoryPolicy`
- `yierdis-core-db`: `YierdisDb.MaxmemoryPolicy`
- `yierdis-args`: `YierdisServerRuntimeConfig.MaxmemoryPolicy`

The policy also crosses module boundaries as a raw `String` through
`YierdisInstanceConfig`, `DbEngineFactory`, `YierdisDbEngineFactory`, and
`YierdisDb` constructors.

This creates repeated parsing and repeated switch statements. It also creates
real semantic drift:

- `core-api` accepts trim, case normalization, and underscores.
- `YierdisDb` accepts trim, case normalization, underscores, and treats null or
  blank as `NOEVICTION`.
- `yierdis-args` accepts trim and case normalization, but does not accept
  underscores.

Global maxmemory already uses the core API enum in `YierdisGlobalMaxmemoryGovernor`.
Per-DB maxmemory still uses the DB-local enum. The two paths should not interpret
the same configured policy through different models.

## Goals

- Make `yier.bubu.redis.ops.MaxmemoryPolicy` the single domain enum for runtime,
  DB, maxmemory governor, and maxmemory participant APIs.
- Keep CLI input as raw strings at the picocli boundary, with conversion to the
  domain enum before runtime config is created.
- Remove `YierdisDb.MaxmemoryPolicy`.
- Remove `YierdisServerRuntimeConfig.MaxmemoryPolicy`.
- Change DB construction and `DbEngineFactory` to accept the core API enum.
- Keep current defaults where no explicit policy is provided: `NOEVICTION`.
- Preserve current eviction behavior for `NOEVICTION`, `ALLKEYS_RANDOM`, and
  `ALLKEYS_LRU`.
- Add focused tests that prevent policy model duplication from coming back.

## Non-Goals

- No new maxmemory policies.
- No changes to eviction algorithms.
- No changes to maxmemory scope behavior.
- No broader cleanup of executor or maxmemory scope enums.
- No removal of compatibility string overloads in the same change if they are
  needed to keep existing callers compiling.

## Considered Approaches

### Approach A: Keep duplicate enums and add conversion helpers

This would make conversions more explicit without changing public signatures.
It is low risk, but it keeps the root problem: every new policy still requires
touching multiple enums and multiple switch statements.

Rejected.

### Approach B: Use core API enum internally, keep CLI enum

This would remove the DB-local enum and stop passing strings through runtime and
DB construction, while keeping `YierdisServerRuntimeConfig.MaxmemoryPolicy` as a
CLI/runtime adapter enum.

This improves the DB side, but the args module would still own a duplicate
policy list and parser. It does not fully satisfy the single-domain-model goal.

Rejected as incomplete.

### Approach C: Use core API enum everywhere except raw CLI input

This approach keeps `YierdisServerArgs.maxmemoryPolicy` as a raw option string,
then converts it to `yier.bubu.redis.ops.MaxmemoryPolicy` during validation and
runtime config construction. `YierdisServerRuntimeConfig`, `YierdisInstanceConfig`,
`DbEngineFactory`, and `YierdisDb` all carry the typed domain enum.

Chosen because it makes `core-api` the semantic source of truth while keeping the
CLI responsible only for accepting and serializing command-line values.

## Architectural Decision

Adopt Approach C.

`MaxmemoryPolicy` in `yierdis-core-api` is the only enum that describes Redis-like
maxmemory eviction policy semantics. It should remain Netty-free and usable by
args, runtime, DB, tests, and any future tools.

The enum should expose a stable normalized external value, for example
`redisName()`:

- `NOEVICTION.redisName()` returns `noeviction`
- `ALLKEYS_RANDOM.redisName()` returns `allkeys-random`
- `ALLKEYS_LRU.redisName()` returns `allkeys-lru`

The existing `parse(String)` method remains the single parser. CLI normalization
should call it instead of maintaining another maxmemory policy switch.

## Target Data Flow

Current flow:

`CLI String -> args enum -> String -> runtime String -> DB String -> DB enum`

Target flow:

`CLI String -> core-api MaxmemoryPolicy -> runtime config -> instance config -> DbEngineFactory -> YierdisDb`

Server info and argv serialization should convert the domain enum back to its
normalized Redis/config name only at output boundaries.

## Target Changes

### `yierdis-core-api`

Modify:

`yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryPolicy.java`

Responsibilities:

- Keep `parse(String)` as the only parser.
- Add a normalized output method such as `redisName()`.
- Keep underscore normalization in `parse(String)`.
- Keep null and blank as invalid in `parse(String)`.

Tests:

- Extend `MaxmemoryPolicyTest` to cover `redisName()`.

### `yierdis-args`

Modify:

`yierdis-args/pom.xml`

- Add a dependency on `yierdis-core-api`.

Modify:

`yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerRuntimeConfig.java`

- Import `yier.bubu.redis.ops.MaxmemoryPolicy`.
- Change the `maxmemoryPolicy` record component to the core API enum.
- Remove the nested `MaxmemoryPolicy` enum.

Modify:

`yierdis-args/src/main/java/yier/bubu/redis/args/YierdisServerArgs.java`

- Keep the picocli field `public String maxmemoryPolicy = "noeviction"`.
- Change `normalizeMaxmemoryPolicy(raw)` to:
  - parse through `yier.bubu.redis.ops.MaxmemoryPolicy.parse(raw)`
  - return `policy.redisName()`
- Change `toRuntimeConfig()` to pass `MaxmemoryPolicy.parse(maxmemoryPolicy)`.

Expected behavior change:

- CLI accepts underscore variants such as `ALLKEYS_RANDOM`, matching the core API
  parser. This is an intentional compatibility expansion.

Tests:

- Update args tests to assert the core API enum.
- Add or update a CLI normalization test for underscore input.
- Keep invalid and blank policy tests.

### `yierdis-core-runtime`

Modify:

`yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceConfig.java`

- Store `MaxmemoryPolicy` instead of `String`.
- Default to `MaxmemoryPolicy.NOEVICTION`.
- Add `Builder.maxmemoryPolicy(MaxmemoryPolicy policy)` as the primary setter.
- Keep `Builder.maxmemoryPolicy(String rawPolicy)` as a deprecated compatibility
  overload if current tests or external callers depend on it.
- Normalize null policy in `build()` to `NOEVICTION`.

Modify:

`yierdis-core/yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java`

- Pass `config.maxmemoryPolicy()` directly to `DbEngineFactory.create(...)`.
- Pass `config.maxmemoryPolicy()` directly to `YierdisGlobalMaxmemoryGovernor`.
- Remove `MaxmemoryPolicy.parse(config.maxmemoryPolicy())`.

Tests:

- Update runtime tests to prefer `MaxmemoryPolicy.NOEVICTION` or
  `MaxmemoryPolicy.ALLKEYS_LRU`.
- Keep compatibility tests for the deprecated string setter if it remains.

### `yierdis-core-api` SPI

Modify:

`yierdis-core/yierdis-core-api/src/main/java/yier/bubu/redis/ops/DbEngineFactory.java`

- Change `create(..., String maxmemoryPolicy, ...)` to
  `create(..., MaxmemoryPolicy maxmemoryPolicy, ...)`.

This is the key boundary change that prevents DB creation from receiving policy
semantics as an unvalidated string.

### `yierdis-core-db`

Modify:

`yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

- Delete the nested `YierdisDb.MaxmemoryPolicy`.
- Change internal `maxmemoryPolicy` field to the core API enum.
- Change primary constructors and static factories to receive the core API enum.
- Keep deprecated string overloads only as compatibility adapters, delegating to
  the typed constructors.
- Remove `parseMaxmemoryPolicy(String)` from DB logic.
- Compute `lruEnabled` from the core API enum.

Modify:

`YierdisDbMemoryLedger`

- Store core API `MaxmemoryPolicy`.
- Compare against `MaxmemoryPolicy.NOEVICTION`.

Modify:

`YierdisDbMaxmemorySupport`

- Store core API `MaxmemoryPolicy`.
- Use it for local eviction key selection and global candidate sampling.
- Remove fully qualified references where imports make the code clearer.

Modify:

`YierdisDbEngineFactory`

- Accept and forward the core API enum through `create(...)`.

Tests:

- Update maxmemory eviction tests to use the core API enum where they call typed
  APIs directly.
- Keep constructor compatibility tests for string overloads if retained.

### `yierdis-server`

Modify:

`yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java`

- Pass `runtimeConfig.maxmemoryPolicy()` directly to
  `YierdisInstanceConfig.Builder.maxmemoryPolicy(...)`.
- Stop calling `argvValue()` on maxmemory policy.

Modify:

`yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java`

- Render `maxmemory_policy` using `config.maxmemoryPolicy().redisName()`.

Tests:

- Update server wiring tests to compare against the core API enum.
- Keep INFO output tests, if present, expecting `allkeys-random`,
  `allkeys-lru`, or `noeviction` strings.

## Compatibility Strategy

The preferred API becomes typed:

```java
YierdisInstanceConfig.builder()
        .maxmemoryPolicy(MaxmemoryPolicy.ALLKEYS_LRU)
        .build();
```

Compatibility string overloads may remain temporarily:

```java
@Deprecated
public Builder maxmemoryPolicy(String rawPolicy) {
    this.maxmemoryPolicy = rawPolicy == null || rawPolicy.isBlank()
            ? MaxmemoryPolicy.NOEVICTION
            : MaxmemoryPolicy.parse(rawPolicy);
    return this;
}
```

The compatibility overload preserves the old runtime builder behavior where null
or blank means `noeviction`. New typed APIs should treat null as unset/default at
builder build time, not as a separate policy value.

DB public string constructors and factories should follow the same compatibility
rule if they are kept. Internal DB construction must use the typed enum.

## Error Handling

- CLI blank input remains invalid at CLI validation time.
- CLI unknown input remains invalid at CLI validation time.
- Core API `MaxmemoryPolicy.parse(null)` and `parse(blank)` remain invalid.
- Runtime builder typed null is normalized to `NOEVICTION` during `build()` to
  preserve the existing default behavior.
- Deprecated string overload null or blank is normalized to `NOEVICTION` to
  preserve existing non-CLI construction behavior.

## Interaction With Existing Decomposition Spec

The existing `YierdisDb` decomposition design mentions extracting a
`YierdisDbMaxmemoryPolicies` helper for DB-local string parsing. This spec should
supersede that specific part.

If both designs are implemented, do not introduce a new DB policy parsing helper.
Instead, let the decomposition work use typed `MaxmemoryPolicy` in any extracted
`YierdisDbConfig` or component factory.

## Implementation Slices

### Slice 1: Core enum output value

- Add `redisName()` to `MaxmemoryPolicy`.
- Add enum constructor values.
- Extend `MaxmemoryPolicyTest`.

Verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-api -Dtest=MaxmemoryPolicyTest test`

### Slice 2: Args runtime config uses core enum

- Add `yierdis-core-api` dependency to `yierdis-args`.
- Remove args maxmemory enum.
- Convert CLI raw value through core `MaxmemoryPolicy.parse`.
- Update args tests.

Verification:

- `jdk25 mvn -pl yierdis-args test`

### Slice 3: Runtime and SPI become typed

- Change `YierdisInstanceConfig`.
- Change `DbEngineFactory`.
- Change `YierdisInstance`.
- Update runtime tests and test doubles.

Verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisInstanceTest,DbEngineFactoryInjectionTest,GlobalMaxmemoryLruAcrossDbsTest test`

### Slice 4: DB removes local policy enum

- Delete `YierdisDb.MaxmemoryPolicy`.
- Change DB support classes to core API `MaxmemoryPolicy`.
- Remove DB parser from main DB logic.
- Keep deprecated compatibility string overloads only if needed.

Verification:

- `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MaxmemoryEvictionTest,TtlMaxmemoryTest,GlobalMaxmemoryLruAcrossDbsTest test`

### Slice 5: Server wiring and final guards

- Update bootstrap and INFO rendering.
- Update server tests.
- Add an architecture guard or focused test that asserts:
  - `YierdisDb` does not declare `MaxmemoryPolicy`
  - `YierdisServerRuntimeConfig` does not declare `MaxmemoryPolicy`
  - `DbEngineFactory.create` accepts core API `MaxmemoryPolicy`

Verification:

- `jdk25 mvn -pl yierdis-server test`
- `jdk25 mvn test`

## Acceptance Criteria

- There is exactly one maxmemory policy enum in production code:
  `yier.bubu.redis.ops.MaxmemoryPolicy`.
- `YierdisServerRuntimeConfig.maxmemoryPolicy()` returns the core API enum.
- `YierdisInstanceConfig.maxmemoryPolicy()` returns the core API enum.
- `DbEngineFactory.create(...)` receives the core API enum.
- `YierdisDb` stores and compares the core API enum.
- `YierdisDb` no longer has `parseMaxmemoryPolicy(String)`.
- Global and per-DB maxmemory paths use the same enum instance type.
- CLI output and INFO output still render normalized strings.
- Existing eviction behavior remains unchanged.

## Risks And Mitigations

### Risk: Source compatibility break for embedded callers

Mitigation:

- Keep deprecated string setters/constructors for one release cycle.
- Route them through `MaxmemoryPolicy.parse` or the documented null/blank
  compatibility default.

### Risk: Args module dependency becomes heavier

Mitigation:

- Depend only on `yierdis-core-api`, which is already the stable Netty-free API
  boundary. Do not depend on runtime or DB modules.

### Risk: Output naming pollutes the domain enum

Mitigation:

- Name the output method after the domain/config representation, such as
  `redisName()`, rather than `argvValue()`.

### Risk: Existing decomposition work reintroduces DB-local parsing

Mitigation:

- Treat this design as superseding the DB policy helper from the decomposition
  spec.
- Add an architecture/focused test to prevent `YierdisDb.MaxmemoryPolicy` from
  returning.
