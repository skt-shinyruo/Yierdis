# Operation Test Coverage Matrix Design

> **Status:** Approved design, ready for implementation planning.

## Goal

为 Yierdis 建立一套可持续维护的“全操作覆盖”测试体系，覆盖三层：

1. 命令层：每个已注册 Redis command 都能通过 `FastTestClient -> YierdisFastCommandProcessor -> CommandModule -> DB` 路径验证。
2. DB API 层：每个对外 storage API 都有直接测试，能绕开命令解析，精确验证存储语义。
3. native 内部结构层：`EntryTable`、`NativeKeyDirectory`、各 `TypeRoot` 等核心 native 结构有生命周期和边界测试。

这次工作的第一阶段不是立刻补齐所有测试，而是先建立覆盖矩阵和测试基建，让后续批量填空有统一标准、统一 helper 和可审查的缺口列表。

## Non-Goals

- 不要求 server/socket 层按每个 command 重复铺满。server 层只保留代表性 smoke、协议、pipeline、backpressure、redis-cli 兼容测试。
- 不测试私有方法本身。私有方法通过命令、DB API 或 native 结构公开行为间接覆盖。
- 不把当前已有测试全部重写成新风格。已有测试能清晰覆盖行为时，矩阵记录其覆盖即可。
- 不在第一阶段改变 production 行为。

## Coverage Model

覆盖矩阵按 domain 分组：

- Connection / Server
- Keyspace
- TTL
- String
- Bitmap
- List
- Hash
- Set
- ZSet
- HLL
- Transaction
- Memory / Maxmemory

每个 operation 记录三类覆盖：

```text
Operation:
  command:
    happy path
    missing / existing key
    binary-safe inputs where applicable
    arity / syntax errors
    wrong type
    option-specific behavior
  db-api:
    direct read/write API behavior
    mutation vs no-op outcome
    TTL interaction
    maxmemory / OOM interaction where applicable
  native:
    allocate / lookup
    replace / overwrite
    delete / release
    scan / iteration
    rehash / growth
    memory accounting
```

The matrix should identify the test file and method for each checked behavior, not only mark a boolean. Missing cells stay explicit as `missing`, so coverage gaps are visible and reviewable.

## Matrix Artifact

Add a maintained document:

```text
docs/project-docs/operation-test-coverage-matrix.md
```

The document is the human-readable source of truth for operation coverage. It should start with the domains that already have the clearest coverage, especially `String`, `Bitmap`, `List`, `Hash`, `Set`, `ZSet`, and `HLL`.

Example shape:

```text
## String

### SET

Command coverage:
- happy path: CommandProcessorTest#setGetIncrExpireTtl
- binary-safe value: CommandProcessorTest#stringIsBinarySafe
- NX no-op: CommandProcessorTest#setNxReturnsNilWhenKeyExists
- EX / KEEPTTL / GET: CommandProcessorTest#setGetAndKeepTtlSemanticsRemainIntact
- syntax errors: CommandErrorTest#arityAndSyntaxErrorsMatchExpectedMessages
- wrong type: CommandProcessorTest#setGetOnNonStringKeyReturnsWrongType

DB API coverage:
- setString happy path: OffHeapStringStorageTest#setGetUsesFfmSliceAndDelFrees
- overwrite reuse: OffHeapStringStorageTest#overwriteReusesFfmBufferUnderHardCap
- oversized value rejection: OffHeapStringStorageTest#ffmMaxBytesRejectsOversizedSet
- missing: mutation outcome no-op matrix for NX / XX

Native coverage:
- StringRoot store / overwrite / slice: StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads
- EntryTable allocate / replace / release: EntryTableContractTest
- NativeKeyDirectory lookup / scan / rehash: NativeKeyDirectoryTest
```

## Test Infrastructure

Add shared test helpers only where they remove real duplication.

### Command Test Helper

Add or extend a helper in integration tests for command-level assertions:

```text
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/testutil/
```

Useful helper responsibilities:

- create a default `FastTestClient` for a `YierdisDb`
- assert reply types and values
- assert common Redis errors
- provide canonical binary key/value/member/field samples
- make command sequences concise without hiding command intent

Possible helper names:

- `CommandAssertions`
- `ReplyAssertions`
- `CommandFixtures`

Keep helper methods small and explicit. Avoid building a DSL that makes tests harder to read than direct command arrays.

### DB API Test Helper

Add storage test helpers under:

```text
yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/
```

Useful responsibilities:

- create and bind a `YierdisDb`
- create FFM-backed DB variants with predictable cleanup
- assert memory usage before and after mutation
- assert key count, type, TTL, and released bytes

### Native Structure Helper

Native tests should keep direct visibility into handles and lifecycle. Helpers are allowed for repeated setup and teardown, but assertions should name the structure under test:

- `EntryTable`
- `NativeKeyDirectory`
- `StringRoot`
- `ListRoot`
- `HashRoot`
- `SetRoot`
- `ZSetRoot`

## Coverage Inventory Guard

Add a lightweight guard test that prevents command coverage from silently drifting:

```text
yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/OperationCoverageMatrixTest.java
```

The guard should compare the command names reported by the registered command metadata with entries listed in the matrix document. The first implementation can be conservative:

- verify every command from `COMMAND` / registry appears at least once in the matrix
- do not require every cell to be complete
- fail with a message naming missing command entries

This turns the matrix into a living artifact without making it a brittle coverage percentage tool.

## Rollout Plan

### Phase 1 - Matrix and Helpers

- Add `operation-test-coverage-matrix.md`.
- Add command reply assertion helpers.
- Add DB/native lifecycle assertion helpers only where existing tests repeat enough code.
- Add `OperationCoverageMatrixTest` to detect command entries missing from the matrix.
- Populate the matrix with existing coverage for all known commands at a coarse level.

### Phase 2 - String and Bitmap Template

- Use String and Bitmap as the first complete examples.
- Fill command, DB API, and native cells.
- Refine helper names and assertion shape based on real tests.

### Phase 3 - Keyspace, TTL, Memory

- Cover cross-cutting operations after the String template is stable.
- Focus on state transitions: create, overwrite, expire, delete, scan, evict.

### Phase 4 - Collection Domains

- Fill List, Hash, Set, and ZSet.
- Include upgrade thresholds, binary-safe members/fields, empty-key deletion, wrong type, and range semantics.

### Phase 5 - HLL and Transaction

- Cover HLL sparse/dense behavior and merge semantics.
- Cover transaction queueing, parser errors inside MULTI, command copy semantics, and EXEC/DISCARD state transitions.

## Success Criteria

- Every registered command has a matrix entry.
- Every matrix entry links to at least one command-level behavior test or is explicitly marked `missing`.
- Every DB API group has direct tests for happy path, no-op or missing-key behavior, wrong type where applicable, TTL interaction where applicable, and memory/OOM behavior where applicable.
- Every native structure has direct tests for allocation, lookup, replacement, release, and at least one growth or iteration path when the structure supports it.
- New commands require updating the matrix, enforced by `OperationCoverageMatrixTest`.
- The test suite remains understandable: behavior tests should still show actual commands and expected replies.

## Risks

- Matrix drift: mitigated by the coverage inventory guard.
- Over-abstracted helpers: mitigated by keeping tests command-shaped and using helpers only for assertions/setup.
- Duplicated behavior tests across layers: mitigated by assigning each layer a clear purpose.
- Large first PR: mitigated by separating matrix/helper work from bulk test fill.

## Implementation Notes

The first implementation plan should not attempt to fill all missing cells. It should create the matrix, add the guard, add minimal helpers, and complete String/Bitmap as proof that the approach works.
