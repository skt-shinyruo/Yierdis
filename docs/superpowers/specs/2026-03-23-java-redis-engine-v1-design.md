# Java Redis-Class Engine V1 Design

**Date:** 2026-03-23

## Goal

Design a pure in-memory Java engine that can plausibly compete with Redis on memory density and tail latency for mixed read/write workloads with medium-sized values, while keeping the first implementation wave focused enough to build and verify.

The V1 target is not "a Redis-like Java service built from standard library collections." It is "a Java implementation of a compact, low-latency, self-managed in-memory engine" with Redis-style semantics for a core command subset.

## Problem Summary

A conventional JVM design built around `HashMap<String, Object>`, boxed metadata, and per-element heap objects can deliver Redis-like behavior, but it will usually fail on the two metrics that matter for this project:

1. Memory density: object headers, references, node wrappers, and repeated byte copies inflate per-key overhead.
2. Tail latency: large object graphs and allocation churn make GC a visible participant in p99/p999 behavior.

For this project, those tradeoffs are not acceptable. The data plane therefore cannot rely on ordinary Java object graphs as its steady-state storage model.

## Scope

### In Scope for V1

- Single-process, single-thread event-loop execution model.
- Pure in-memory engine.
- RESP2 as the canonical wire protocol for V1 so existing Redis tooling can be used for basic interoperability checks and benchmark comparisons.
- Redis-style core types:
  - `String`
  - `Hash`
  - `Set`
  - `List`
  - `ZSet`
- Redis-style TTL support:
  - lazy expiration
  - active expiration
- RESP-oriented request/response execution path.
- Compact storage encodings and one-way encoding upgrades.
- Custom memory management for the data plane.
- Benchmarking and observability sufficient to compare memory usage and tail latency against a Redis baseline.

### Out of Scope for V1

- Persistence (`AOF`, `RDB`)
- Replication, clustering, or sharding
- Multi-threaded shared-state execution
- Lua / scripting
- ACL / TLS / PubSub
- Full Redis command coverage
- Automatic encoding downgrade
- Final `maxmemory` eviction policy work

## Non-Goals

- Do not optimize for fastest possible time-to-market at the expense of memory layout quality.
- Do not preserve a "mostly Java collections, a little off-heap" architecture in the hot data path.
- Do not chase every Redis feature before the storage core, allocator, and benchmark harness are stable.

## Design Constraints

The design is anchored by these explicit constraints:

1. Primary workload:
   - medium-sized values
   - mixed reads and writes
   - general-purpose usage rather than an ultra-specialized microbenchmark profile
2. First-wave data structures:
   - `String / Hash / Set / ZSet / List`
3. Runtime model:
   - single process
   - single-thread event loop
4. Durability:
   - no persistence in V1
5. Performance target:
   - memory density and p99/p999 latency are first-class metrics, not secondary diagnostics

## Protocol and Command Subset

V1 should be narrow but explicit. The planning phase should not need to rediscover which commands are in scope.

### Protocol

- RESP2 is the only required wire protocol for V1.
- The engine should be usable with common Redis-compatible clients for the supported command subset.
- RESP3 and non-Redis wire formats are out of scope for this design.

### Minimum Command Subset

#### Core / Keyspace / TTL

- `PING`
- `DEL`
- `TYPE`
- `EXPIRE`
- `PEXPIRE`
- `TTL`
- `PTTL`

#### `String`

- `GET`
- `SET`

#### `Hash`

- `HSET`
- `HGET`
- `HDEL`

#### `Set`

- `SADD`
- `SREM`
- `SISMEMBER`

#### `List`

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `LRANGE`

#### `ZSet`

- `ZADD`
- `ZREM`
- `ZRANGE`

Anything beyond this subset should be treated as follow-up scope unless explicitly added in the implementation plan.

## Recommended Approach

Use a split architecture:

- heap-resident control plane
- off-heap-first compact data plane

The control plane may use normal Java objects where that improves maintainability and does not dominate steady-state memory behavior:

- protocol decode/encode state
- command registry
- connection state
- logging
- metrics wiring
- test harnesses

The data plane must not depend on Java standard collections as its steady-state storage representation:

- primary keyspace
- TTL index
- type payloads
- object metadata
- packed encodings
- upgraded hash/set/zset/list structures

This is the minimum architecture that keeps the project on a path where Redis-class memory density and latency remain realistic goals.

## Architecture Overview

### 1. Execution Model

The engine runs as a single-thread event loop:

- all commands execute serially on one owner thread
- the hot path stays lock-free
- maintenance is incremental and piggybacks on command execution

This keeps correctness and tail-latency analysis tractable in V1. It also avoids prematurely mixing storage-engine work with concurrency-control work.

### 2. Control Plane vs Data Plane

The architecture must keep these responsibilities separate:

- Control plane:
  - network I/O
  - protocol state
  - command dispatch
  - connection/session state
  - diagnostics wiring
- Data plane:
  - keyspace lookup
  - TTL ownership
  - compact payload storage
  - encoding upgrades
  - memory accounting
  - deletion and reclamation

The command layer talks to the engine through semantic operations. It does not manipulate allocators, raw addresses, or low-level storage layouts.

## Core Components

### `RedisServer`

Owns the single-thread event loop and drives:

- protocol read/decode
- command dispatch
- bounded maintenance work per command
- response writeback

### `ConnectionContext`

Heap-resident per-connection state:

- protocol parser state
- output buffering
- transaction/session state if later added

### `CommandRegistry`

Maps command names to handlers. This stays in the control plane.

### `DbEngine`

The only public data-plane entry point. It exposes semantic operations such as:

- `get/set/del`
- `expire/ttl`
- `hset/hget/hdel`
- `sadd/srem/smembers`
- `lpush/rpush/lpop/rpop/lrange`
- `zadd/zrem/zrange`

`DbEngine` coordinates:

- `KeyspaceTable`
- `ExpireIndex`
- `MemoryManager`
- type-specific `*Ops`

### `KeyspaceTable`

Owns the primary key dictionary:

- off-heap open-addressing hash table
- slot metadata optimized for scan locality
- stable `entryHandle` references
- incremental rehash

### `ExpireIndex`

Owns active expiration scheduling:

- secondary time-based index
- no independent key ownership
- resolves to entry handles only

### `MemoryManager`

Owns allocator composition and memory statistics:

- `EntryArena`
- `SmallObjectArena`
- `LargeObjectAllocator`

### `EntryLayout`

Defines the fixed-size entry header and exposes static offset-based read/write helpers. This should behave more like a layout/access module than a heap object model.

### Type-Specific Ops

Separate internal components interpret payloads by type:

- `StringOps`
- `HashOps`
- `SetOps`
- `ListOps`
- `ZSetOps`

These components share the same entry lifecycle protocol and memory ownership rules.

## Memory Model

### Handle-Based Access

The engine should use stable 64-bit handles instead of leaking raw addresses through higher layers.

Recommended properties:

- handle encodes arena/page/type identity
- handle can be validated in debug mode
- handle remains stable across hash-table rehash
- upper layers never need raw addresses

The engine may decode a handle into an address internally on the hot path, but the system boundary is handle-based.

### Allocator Strategy

V1 should not use a single generic allocator policy for all object shapes. It should split responsibilities:

#### `EntryArena`

- fixed-size blocks
- stores primary entries only
- page-based management
- free-list recycling

#### `SmallObjectArena`

- size-class based
- stores packed blobs, small string blocks, list segments, small nodes
- optimized for low overhead and predictable reclamation

#### `LargeObjectAllocator`

- stores large strings and large blobs
- coarser-grained allocation policy
- exposes fragmentation visibility

This separation avoids mixing fixed-width metadata with variable-size payload lifecycles.

### Required Allocator Diagnostics

The memory system must expose at least:

- `allocatedBytes`
- `activeBytes`
- `fragmentBytes`
- `residentPages`
- `objectCountByEncoding`
- size-class utilization
- leak report at shutdown in debug/test mode

## Keyspace Design

### Hash Table Strategy

The primary dictionary should be:

- off-heap
- open addressing
- optimized for locality
- incrementally rehashed

`Robin Hood` probing is a strong default because it reduces lookup variance and probe tail behavior.

Each slot stores only minimal metadata, such as:

- hash fingerprint
- entry handle
- probe metadata

The slot does not own the full object payload. The actual key/value state lives in the stable entry referenced by the handle.

### Stable Entry Ownership

Each key has exactly one owning entry. That entry owns:

- key bytes
- value reference
- expiration metadata
- type/encoding metadata

The design must avoid duplicating key bytes across:

- primary keyspace
- TTL structures
- command-layer copies

## Entry Layout

Each primary entry is fixed-size and contains the minimum hot metadata required for common operations.

Recommended fields:

- `type`
- `encoding`
- `flags`
- `ttlVersion`
- `keyLen`
- `payloadLen`
- `keyRef`
- `valueRef`
- `expireAt`
- `accessMeta`
- `aux/count`

The exact byte layout may evolve, but V1 should target a stable, inspectable header rather than a hierarchy of small heap wrapper objects.

## TTL Design

### Lazy Expiration

`expireAt` lives inside the primary entry. That makes lazy expiration a single-entry read rather than a secondary hash-table lookup.

### Active Expiration

Use a secondary time index implemented as an off-heap min-heap.

Each heap node stores:

- `expireAt`
- `entryHandle`
- `ttlVersion`

On TTL overwrite:

1. update `entry.expireAt`
2. increment `entry.ttlVersion`
3. push a new heap node

Old heap nodes become stale and are discarded lazily when popped.

This model keeps ownership simple and avoids copying keys into a separate TTL dictionary.

## Type and Encoding Strategy

All values are represented as:

- `type`
- `encoding`
- `valueRef`

Encodings upgrade in one direction only. V1 does not perform automatic downgrade.

### `String`

Two encodings:

- `INLINE/EMBSTR` for small strings
- `RAW` contiguous block for medium/large strings

### `Hash`

Two encodings:

- `PACKED` sequential blob for small objects
- off-heap hash table for larger objects

Upgrade triggers should consider:

- field count
- maximum field/value size

### `Set`

Two encodings:

- `PACKED` member blob for small objects
- off-heap hash table for larger objects

### `List`

Do not use a traditional linked list.

Use a `quicklist-like` design:

- top-level segment chain
- packed entries within each segment
- efficient push/pop at both ends
- acceptable locality for range reads

### `ZSet`

Two encodings:

- `PACKED` sequential member/score representation for small objects
- `DICT + SKIPLIST` for larger objects

Large `ZSet` mode must not duplicate payload ownership unnecessarily.

## Command Path

The steady-state command path should be:

`decode -> argv views -> dispatch -> expire check -> lookup/mutate -> encode`

Key rules:

- request arguments stay as views as long as possible
- command handlers call semantic engine APIs only
- large replies stream directly to the reply writer where practical
- each command advances bounded maintenance work

This avoids both large temporary heap object creation and maintenance spikes.

## Mutation Discipline

All writes should follow the same invariant:

1. validate and locate
2. allocate/build new representation
3. switch entry references only after the new value is complete
4. release old representation last

This applies to:

- overwrite writes
- type changes
- expiration deletes
- explicit deletes
- encoding upgrades

The goal is not transaction isolation. The goal is deterministic, leak-free state transitions under failure.

## Failure and Debug Invariants

The data plane should treat these as mandatory debug-time invariants:

- no double free
- no use-after-free through stale handles
- no silent handle forgery
- no leaked pages on clean shutdown
- no dangling payload after entry deletion

Recommended debug aids:

- handle generation/version checks
- poisoned memory on free
- allocator dumps
- integrity checks at startup/shutdown in test mode

## Benchmarking and Observability

V1 benchmarking must track more than throughput.

### Required Metrics

- `ops/s`
- `p50/p95/p99/p999`
- resident bytes per key
- bytes per payload byte
- fragmentation ratio
- encoding distribution
- maintenance cost under TTL and rehash activity

### Required Benchmark Profiles

1. `String` mixed workload:
   - medium values
   - approximately `70/30` read/write
2. mixed structure workload:
   - `Hash/Set/List/ZSet`
   - mixed read/write
3. TTL-heavy workload:
   - sustained short-lived key churn

### Required Stability Runs

Run longer soak tests, not just short bursts, to observe:

- latency drift
- memory growth
- fragmentation creep
- allocator health under repeated churn

## Delivery Strategy

V1 should be delivered in distinct implementation phases with hard gates between them.

### Phase 0: Skeleton and Observability

Build:

- event loop skeleton
- RESP path
- benchmark harness
- allocator debug infrastructure
- baseline memory stats

Gate:

- leak-free clean startup/shutdown
- repeatable benchmark harness
- inspectable allocator state

### Phase 1: `String + TTL + Keyspace`

Build:

- memory manager
- entry arena
- keyspace table
- entry layout
- expire heap
- `GET/SET/DEL/EXPIRE/TTL`
- incremental rehash

Gate:

- semantic correctness
- no leaks under overwrite/delete/expire
- no severe TTL maintenance spikes

### Phase 2: `Hash` and `Set`

Build:

- packed encodings
- upgrade path to off-heap hash table

Gate:

- packed and upgraded forms remain behaviorally equivalent
- upgrade path is stable and one-way

### Phase 3: `List`

Build:

- quicklist-like segmented list
- push/pop/range path

Gate:

- no allocator pathologies under churn
- acceptable range-read latency

### Phase 4: `ZSet`

Build:

- packed `ZSet`
- `DICT + SKIPLIST` upgrade path

Gate:

- member lookup and ordered range semantics are stable
- no obvious payload duplication regressions

### Phase 5: Hardening

Build:

- memory/encoding inspection commands
- traversal support
- fuzzing
- long-run soak validation
- Redis baseline comparisons

Gate:

- stable multi-hour runs
- allocator/accounting consistency
- benchmark results good enough to justify maxmemory/eviction follow-up work

## Risks and Tradeoffs

### Benefits

- Keeps the project aligned with Redis-class memory-density goals from day one.
- Avoids a later rewrite from heap-object data structures to compact storage layouts.
- Makes tail-latency work visible early instead of as a late-stage surprise.

### Costs

- Much higher implementation complexity than a Java-collections-first design.
- More difficult debugging and test infrastructure requirements.
- Requires allocator discipline before many user-facing features can be added safely.

### Rejected Alternative

The rejected baseline is a mostly heap-based design that stores steady-state data in Java collections and only opportunistically moves some values off-heap. That design is acceptable for a Redis-like JVM service, but not for a project whose explicit goal is to compete with Redis on memory density and latency.

## Follow-Up

After this design is accepted, the next artifact should be an implementation plan that:

- turns each phase into executable tasks
- names the first concrete file/module boundaries
- defines the first benchmark commands and expected failure modes
- keeps `maxmemory/eviction` explicitly deferred until the allocator, keyspace, TTL, and memory accounting paths are stable
