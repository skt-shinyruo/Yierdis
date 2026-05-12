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
