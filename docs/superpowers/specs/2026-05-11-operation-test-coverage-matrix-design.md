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

### Matrix Status Values

Every matrix cell must use one of these status values:

- `covered`: the behavior has a named test file and method.
- `covered-by-shared-test`: the behavior is covered by a broader cross-command or cross-structure test; the matrix must name that test.
- `missing`: the behavior should be covered but does not have a test yet.
- `not-applicable`: the behavior does not apply to this operation; the matrix must state the reason briefly.

Avoid blank cells. A blank cell means the matrix itself is incomplete.

### Initial Command Inventory

The matrix must include every command registered by the kernel, default command bundle, and server command module.

Kernel transaction commands:

- `MULTI`
- `DISCARD`
- `EXEC`

Connection and server commands:

- `PING`
- `ECHO`
- `COMMAND`
- `SELECT`
- `QUIT`
- `CLIENT`
- `AUTH`
- `FLUSHDB`
- `HELLO`
- `INFO`
- `STATS`

Keyspace, TTL, memory, and object commands:

- `TYPE`
- `MEMORY`
- `OBJECT`
- `KEYS`
- `SCAN`
- `DEL`
- `EXISTS`
- `EXPIRE`
- `PEXPIRE`
- `EXPIREAT`
- `PEXPIREAT`
- `PERSIST`
- `TTL`
- `PTTL`

String and bitmap commands:

- `SET`
- `GET`
- `STRLEN`
- `APPEND`
- `SETBIT`
- `GETBIT`
- `BITCOUNT`
- `INCR`
- `DECR`

List commands:

- `LPUSH`
- `RPUSH`
- `LRANGE`
- `LPOP`
- `RPOP`

Hash commands:

- `HSET`
- `HGET`
- `HGETALL`
- `HLEN`
- `HDEL`

Set commands:

- `SADD`
- `SREM`
- `SMEMBERS`
- `SISMEMBER`
- `SCARD`

Sorted-set commands:

- `ZADD`
- `ZRANGE`
- `ZREVRANGE`
- `ZRANGEBYSCORE`
- `ZREVRANGEBYSCORE`
- `ZREMRANGEBYSCORE`
- `ZREMRANGEBYRANK`
- `ZREM`

HyperLogLog commands:

- `PFADD`
- `PFCOUNT`
- `PFMERGE`

### Subcommand And Option Inventory

The matrix must represent behavior-changing subcommands and options explicitly. They can be nested under their parent command, but they cannot be hidden behind a generic “options covered” note.

Required initial entries:

- `COMMAND`: base command, `COUNT`, `INFO`.
- `CLIENT`: `SETINFO`, `SETNAME`, `GETNAME`, unknown subcommand error.
- `HELLO`: RESP2, RESP3, `SETNAME`, unsupported protocol version, `AUTH` error, disallowed in `MULTI`.
- `INFO`: no section, named section.
- `MEMORY`: `STATS`, `USAGE`, invalid subcommand.
- `OBJECT`: `ENCODING`, invalid subcommand.
- `SCAN`: cursor parsing, `MATCH`, `COUNT`, duplicate or invalid option syntax.
- `SET`: `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`, conflicting options.
- `BITCOUNT`: full string and byte-range variants.
- `LPOP` / `RPOP`: single-element and count variants.
- `ZRANGE`: `WITHSCORES`, `REV`, and range bounds.
- `ZREVRANGE`: optional `WITHSCORES`.
- `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE`: inclusive/exclusive bounds, infinities, `WITHSCORES`, `LIMIT`, invalid option syntax.
- `FLUSHDB`: default mode and accepted optional mode tokens.

### DB API Inventory

The matrix must enumerate public DB API methods, not only API groups.

Read API:

- `StringReadOps`: `getStringBytes`, `getStringValue`, `strlen`, `getBit`, `bitcount`, ranged `bitcount`.
- `HashReadOps`: `hget`, `hgetall`, `hlen`.
- `ListReadOps`: `lrange`.
- `SetReadOps`: `smembers`, `sismember`, `scard`.
- `ZSetReadOps`: `zrange`, `zrevrange`, `zrangeByScore`, `zrevrangeByScore`.
- `HllReadOps`: `pfcount`.
- `KeyspaceReadOps`: `typeOf`, `existsKey`, `keys`, `scan`.
- `TtlReadOps`: `ttlSeconds`, `ttlMillis`.

Write API:

- `StringWriteOps`: `set`, both `setString` overloads, `append`, `setBit`, `incrBy`.
- `HashWriteOps`: `hset`, `hdel`.
- `ListWriteOps`: `lpush`, `rpush`, `lpop`, `rpop`.
- `SetWriteOps`: `sadd`, `srem`.
- `ZSetWriteOps`: `zadd`, `zremrangeByScore`, `zremrangeByRank`, `zrem`.
- `HllWriteOps`: `pfadd`, `pfmerge`.
- `KeyspaceWriteOps`: `del`.
- `TtlWriteOps`: `expire`, `pexpire`, `expireAtSeconds`, `expireAtMillis`, `persist`.
- `DbLifecycleOps`: `flushDb`.

Runtime and observability API:

- `MemoryOps`: `memoryUsage`, `memoryStats`, `objectEncoding`.
- `ExpirationManager`: cleanup and maintenance behavior exposed through the DB engine.
- `DbEngine`: `reads`, `writes`, `expiration`, `memory`, `lifecycle`.

### Native/Internal Inventory

The native/internal matrix must cover structures by responsibility, not only by class name.

Entry and handle model:

- `EntryRecord`
- `EntryTable`
- `EntryHandle`
- `ValueHandle`
- `KeyHandle`
- `HeapKeyHandle`
- `FfmKeyHandle`

Key directory and key bytes:

- `NativeKeyDirectory`
- `YierdisFfmBlobStore`
- `YierdisFfmKeyspace`
- `ByteArrayKeyspace`
- key lookup, insertion, replacement, removal, scan, random sampling, tombstone handling, and rehash/growth.

Type roots and value encodings:

- `StringRoot`
- `ListRoot`
- `HashRoot`
- `SetRoot`
- `ZSetRoot`
- `ListValue`
- `HashValue`
- `SetValue`
- `ZSetValue`
- `YierdisHyperLogLog`
- HLL storage through `StringRoot` with `ValueType.STRING` / `ValueEncoding.STRING_RAW`, because the current HLL path does not have a separate `HllRoot`.

Expiration, memory, and mutation accounting:

- `YierdisExpireIndex`
- `YierdisHeapExpireIndex`
- `YierdisFfmExpireIndex`
- `YierdisDbMemoryLedger`
- `MemoryLedger`
- `InMemoryLedger`
- `YierdisDbMutationExecutor`
- `YierdisDbMemoryEstimator`
- `YierdisDbMemoryReporter`
- `YierdisDbIntrospection`
- maxmemory candidate sampling, reserve/commit/rollback behavior, and cleanup after failed mutation plans.

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
- happy path: covered - CommandProcessorTest#setGetIncrExpireTtl
- binary-safe value: covered - CommandProcessorTest#stringIsBinarySafe
- NX no-op: covered - CommandProcessorTest#setNxReturnsNilWhenKeyExists
- EX / KEEPTTL / GET: covered - CommandProcessorTest#setGetAndKeepTtlSemanticsRemainIntact
- syntax errors: covered - CommandErrorTest#arityAndSyntaxErrorsMatchExpectedMessages
- wrong type: covered - CommandProcessorTest#setGetOnNonStringKeyReturnsWrongType

DB API coverage:
- setString happy path: covered - OffHeapStringStorageTest#setGetUsesFfmSliceAndDelFrees
- overwrite reuse: covered - OffHeapStringStorageTest#overwriteReusesFfmBufferUnderHardCap
- oversized value rejection: covered - OffHeapStringStorageTest#ffmMaxBytesRejectsOversizedSet
- mutation outcome no-op matrix for NX / XX: missing

Native coverage:
- StringRoot store / overwrite / slice: covered - StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads
- EntryTable allocate / replace / release: covered-by-shared-test - EntryTableContractTest
- NativeKeyDirectory lookup / scan / rehash: covered-by-shared-test - NativeKeyDirectoryTest
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

The guard should compare command names reported by registered command metadata with entries listed in the matrix document. It must build two inventories:

- default processor inventory: kernel transaction commands plus `DefaultCommandModules`
- server processor inventory: default processor inventory plus `ServerCommandModule`

The first implementation can be conservative:

- verify every command from both inventories appears at least once in the matrix
- verify every matrix cell uses one of the allowed status values
- do not require every cell to be complete
- fail with a message naming missing command entries or unknown status values

This turns the matrix into a living artifact without making it a brittle coverage percentage tool.

## Rollout Plan

### Phase 1 - Matrix and Helpers

- Add `operation-test-coverage-matrix.md`.
- Add command reply assertion helpers.
- Add DB/native lifecycle assertion helpers only where existing tests repeat enough code.
- Add `OperationCoverageMatrixTest` to detect command entries missing from the matrix.
- Populate the matrix with existing coverage for every command in the initial command inventory at a coarse level.
- Populate DB API and native/internal inventories with `covered`, `covered-by-shared-test`, `missing`, or `not-applicable`.

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
- Every behavior-changing subcommand and option listed in the subcommand inventory has its own matrix row or nested entry.
- Every matrix cell uses `covered`, `covered-by-shared-test`, `missing`, or `not-applicable`.
- Every `covered` and `covered-by-shared-test` cell links to at least one test file and method.
- Every DB API method listed in the DB API inventory has direct tests for happy path, no-op or missing-key behavior, wrong type where applicable, TTL interaction where applicable, and memory/OOM behavior where applicable.
- Every native/internal responsibility listed in the native inventory has direct tests for allocation, lookup, replacement, release, and at least one growth or iteration path when the structure supports it.
- New commands require updating the matrix, enforced by `OperationCoverageMatrixTest`.
- New public DB API methods and new native storage structures require updating the matrix, enforced by review and, where practical, guard tests.
- The test suite remains understandable: behavior tests should still show actual commands and expected replies.

## Risks

- Matrix drift: mitigated by the coverage inventory guard.
- Over-abstracted helpers: mitigated by keeping tests command-shaped and using helpers only for assertions/setup.
- Duplicated behavior tests across layers: mitigated by assigning each layer a clear purpose.
- Large first PR: mitigated by separating matrix/helper work from bulk test fill.

## Implementation Notes

The first implementation plan should not attempt to fill all missing cells. It should create the matrix, add the guard, add minimal helpers, and complete String/Bitmap as proof that the approach works.
