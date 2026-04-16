# FFM Java Redis Kernel Design

**Date:** 2026-04-16

## Goal

Refocus Yierdis around one primary objective:

- implement a Java version of the Redis kernel
- use JDK 25 FFM as the only data-plane memory foundation
- build Redis-style basic data structures on top of off-heap memory
- keep the existing custom protocol instead of adopting RESP

The repository should still be runnable as one server, but the codebase should teach and express a Redis-like kernel first, not a protocol/server assembly first.

## Why This Redesign Is Needed

The current repository already contains useful Redis-like pieces:

- single-thread command execution
- FFM-backed keyspace and expire structures
- Redis-style encoding names
- compact-vs-upgraded container transitions
- TTL and maxmemory support

However, the main code path is still organized around current project-specific engineering boundaries:

- protocol DTOs
- server wiring
- command module registration
- per-type ops helpers
- mixed heap/FFM fallback implementations

That structure is workable for an evolving Java server, but it is the wrong teaching surface for a Redis kernel project. A reader has to understand too much Yierdis-specific composition before they can understand the Redis-like lookup, expire, mutation, and encoding story.

If the project keeps this shape, FFM and data structures remain implementation details under a server-first architecture. That conflicts with the intended direction.

## Confirmed Constraints

- The redesign must be based on the existing codebase, not a second implementation.
- Break changes are allowed when they make the Redis-kernel teaching path clearer.
- Old module paths and compatibility layers may be deleted after migration.
- The external protocol remains Custom Protocol v1.
- RESP2/RESP3 support is not part of this design.
- The execution model remains single-process, single-threaded command execution in phase 1.
- Migration must preserve one runnable mainline implementation rather than two long-lived variants.
- The kernel should move closer to Redis semantics even when the wire format remains custom.
- FFM is the only supported data-plane memory backend.
- The first phase must focus on basic Redis data structures and the kernel path needed to exercise them.

## Problem Summary

### 1. The repository entry point is command wiring, not kernel execution

`YierdisFastCommandProcessor` currently reads as the main system entry point. It is organized around command registration and handler dispatch. That is useful infrastructure, but it teaches the wrong primary story. The central learning path should be:

`request -> lookup -> lazy expire -> type dispatch -> write reservation -> encoding upgrade -> commit -> bookkeeping`

instead of:

`request -> command module -> per-type helper`

### 2. `YierdisDb` still owns too much assembly

`YierdisDb` already contains the right ingredients, but too many responsibilities still converge there:

- keyspace ownership
- expire index ownership
- maxmemory wiring
- per-type ops assembly
- memory reporting
- lifecycle hooks

That makes it hard to see which concepts correspond to Redis DB state, expire cycle, eviction cycle, object metadata, and mutation orchestration.

### 3. Data structures still expose a mixed heap/FFM story

Several value implementations still keep both heap and FFM paths inside the same class:

- `HashValue`
- `SetValue`
- `ListValue`
- `ZSetValue`

This was useful for incremental evolution, but it now obscures the intended direction. For a Redis-kernel-oriented repository, the steady-state story must be:

- one data-plane implementation
- FFM-native structures
- Redis-style encodings and upgrades

not "heap version plus FFM version plus fallback logic".

### 4. Some current FFM structures are still wrappers around heap collections

The repository already has useful FFM components, but not all of them are yet Redis-like native structures. For example, some compact structures still look like heap-managed collections that store native byte references, rather than contiguous native representations that act like true Redis-style packed encodings.

That means the code direction is correct, but the teaching story is not yet clean enough.

## Recommended Approach

Restructure the repository into one kernel-first implementation path:

- keep one server
- keep one protocol
- keep one kernel
- move the codebase center of gravity from protocol/server assembly to FFM memory plus Redis-kernel execution

This is a converging redesign, not a parallel rewrite.

The implementation should be staged:

- phase 1 converges the execution path and the highest-value FFM-native structures
- phase 2 finishes the broader module/path cleanup and lower-priority structure migrations

## Alternatives Considered

### Option A: Keep the current architecture and only align semantics

- Pros: smallest short-term change
- Cons: leaves the teaching surface centered on current project boundaries instead of Redis kernel concepts

### Option B: Add a second "clean kernel" implementation beside the current one

- Pros: easier experimentation
- Cons: violates the single-implementation constraint and guarantees long-term drift

### Option C: Restructure the current codebase into one kernel-first implementation

- Pros: preserves existing work, removes duplicate stories, and creates a clean teaching path
- Cons: requires deliberate module renames, path deletions, and staged break changes

**Recommendation:** Option C.

## Target Architecture

### Primary Layers

These layers describe the target end-state architecture, not a requirement that every directory rename happen immediately in phase 1.

The repository should be legible as four layers:

1. `memory-ffm`
2. `kernel`
3. `protocol`
4. `server`

The order matters. Lower layers define the main teaching story.

### `yierdis-memory-ffm`

This module becomes the only data-plane memory foundation. It should own:

- FFM runtime and ownership model
- region/allocation primitives
- native byte/blob storage
- native packed encodings
- native dict/rehash primitives
- native skiplist primitives
- low-level byte/address helpers

This module is not a generic pluggable off-heap abstraction layer. It is explicitly FFM-first.

Phase 1 does not require a fully generalized malloc-style allocator. Structure-specific region/blob ownership is acceptable if it keeps the migration smaller and clearer.

### `yierdis-kernel`

This module becomes the repository center. It should own:

- DB state
- keyspace and expire dictionaries
- Redis object metadata
- lookup rules
- mutation pipeline
- active expire
- eviction
- encoding thresholds and upgrade rules
- kernel command semantics

This module should read like a Java Redis kernel, not like transport-neutral helper glue.

In phase 1, this kernel boundary may still be implemented by reshaping existing modules before the final rename lands.

### `yierdis-protocol-*`

Protocol modules continue to own:

- Custom Protocol v1 DTOs
- codecs
- request/reply framing helpers
- protocol parser tests

Protocol modules must not own kernel semantics.

### `yierdis-server`

Server remains the composition root for:

- Netty bootstrap
- connection lifecycle
- queueing/backpressure
- protocol adaptation into kernel requests
- reply writeback

Server should not own Redis data structure behavior.

## Required Break Changes And Why They Are Worth It

### Break Change 1: Collapse generic off-heap abstractions into FFM-first memory modules

Reason:

- the project is already FFM-only in practice
- keeping backend-neutral abstractions teaches the wrong lesson
- the core design objective is specifically FFM-based Redis-like data structures

This break change removes abstraction noise that no longer matches product direction.

Phase 1 should prefer targeted region/blob abstractions over inventing a broad allocator subsystem too early.

### Break Change 2: Reorganize `YierdisDb` into explicit kernel collaborators

Reason:

- a teaching-oriented Redis kernel needs named concepts for lookup, mutation, expire, and eviction
- one large owner object hides the execution pipeline
- explicit collaborators make the Redis model easier to read, test, and change

### Break Change 3: Delete heap fallback implementations for steady-state data structures

Reason:

- dual-path structures were useful during transition, but they now create two implementation stories
- the project goal is one FFM-native implementation, not a compatibility matrix
- keeping both paths would violate the single-implementation constraint

### Break Change 4: Reframe command execution around one kernel runner

Reason:

- command handlers should parse arguments and format replies
- kernel execution order should be centralized and reusable
- this is the clearest way to teach Redis command semantics

This is the most important phase 1 break change.

### Break Change 5: Defer large-scale path/module renames until the kernel path is stable

Reason:

- renaming first creates churn before the architecture has actually converged
- the current codebase already contains enough moving parts without adding early large-scale path edits
- once the kernel runner and first FFM-native structures are stable, renames become a cleanup step instead of a source of migration noise

This is still a real break change, but it should land later than the kernel-path convergence work.

### Break Change 6: Allow heap-resident control metadata in phase 1 while keeping payloads off-heap

Reason:

- Java does not need to copy Redis's C object layout mechanically in phase 1
- the important early win is moving payload and core structures onto FFM
- small heap-resident metadata objects can keep the first migration tractable without changing the architectural direction

## Kernel Execution Model

All teaching-core commands should execute through one kernel path:

1. adapt the protocol request into an execution request
2. resolve the target DB
3. perform key lookup
4. lazily expire the key if needed
5. apply type dispatch
6. perform pre-write reservation for growing writes
7. trigger encoding upgrade when thresholds require it
8. commit the mutation atomically
9. run post-write bookkeeping

### Core Semantic Rules

- Lazy expiration happens before type mismatch handling.
- A key that expires during lookup behaves as absent.
- Growing writes must fail before commit if reservation, allocation, or eviction cannot succeed.
- Empty `Hash`, `Set`, `List`, and `ZSet` containers are deleted as keys instead of remaining visible as empty values.
- Rehash and maintenance work must not change externally visible semantics.

## Memory Model And Redis Object Model

The kernel should adopt an explicit Redis-like object model:

- `type`
- `encoding`
- LRU metadata
- native payload handle

Keyspace entries should point to compact control metadata plus native payload, rather than storing large Java object graphs as the main data-plane representation.

Phase 1 does not require a fully native Redis object header. It is acceptable for small control metadata records to stay on heap while payload bytes and primary structures move to FFM. A more aggressively native object layout is a phase 2 optimization.

## Allocator Strategy

Phase 1 should avoid overreaching into a fully general allocator project.

The preferred order is:

1. keep or refine the current structure-oriented FFM runtime and blob ownership model
2. make keyspace/expire/container payloads FFM-native
3. introduce lower-level reusable primitives only when multiple structures genuinely need them

This keeps the project focused on "Java Redis kernel on FFM" rather than drifting into "build a new memory manager first".

## Data Structure Design

Phase 1 priority is:

1. keyspace
2. expire dictionary
3. string
4. hash
5. set

`List` and `ZSet` remain part of the target design, but they are phase 2 priorities unless the earlier migrations finish cleanly.

### Keyspace

- native dictionary
- incremental rehash
- key handle support
- authoritative ownership of key existence

### Expire Dictionary

- separate native dictionary
- keyed by the same logical key identity used by keyspace
- supports lazy expire and active expire cycles

### String

Phase 1 should support Redis-style encodings:

- `int`
- `embstr`
- `raw`

`embstr` should be treated as a compact native layout, not merely a naming distinction.

### Hash

- small hashes use native listpack
- upgraded hashes use native dictionary

### Set

- small integer-only sets use native intset
- upgraded sets use native dictionary

### List

- small lists should eventually use native listpack
- upgraded lists should eventually use native quicklist-style nodes

This is a phase 2 structure unless phase 1 finishes ahead of scope.

### ZSet

- small sorted sets should eventually use native listpack
- upgraded sorted sets should eventually use native dictionary plus native skiplist

This is a phase 2 structure unless phase 1 finishes ahead of scope.

### HLL

`HLL` may remain an extension outside the initial teaching-core path. It should not drive the phase 1 architecture.

## Teaching-Core Command Scope

### Phase 1 kernel-first command scope

### Core / Keyspace / TTL

- `PING`
- `DEL`
- `TYPE`
- `EXPIRE`
- `PEXPIRE`
- `TTL`
- `PTTL`

### String

- `GET`
- `SET`

### Hash

- `HSET`
- `HGET`
- `HDEL`

### Set

- `SADD`
- `SREM`
- `SISMEMBER`

Other commands may remain in the repository, but they are not allowed to dictate the phase 1 kernel design.

### Phase 2 teaching-core expansion

Once phase 1 is stable, expand the same kernel path to:

#### List

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `LRANGE`

#### ZSet

- `ZADD`
- `ZREM`
- `ZRANGE`

## Module Migration Plan

### Step 1: Introduce one kernel command runner inside the current repository shape

- centralize lookup, expire, type dispatch, reservation, commit, and bookkeeping
- keep the existing server/protocol entry points stable
- avoid immediate large-scale path churn

### Step 2: Make keyspace and expire dict first-class kernel objects

- move TTL and lookup semantics behind a unified kernel path
- keep them runnable through the existing server mainline

### Step 3: Migrate the first FFM-native structure set

Migration order:

1. `String`
2. `Hash`
3. `Set`

After each migration:

- remove the old heap fallback for that type
- move tests to the kernel/encoding/native-structure split

### Step 4: Reattach active expire and eviction to kernel maintenance

- active expire becomes a kernel-owned maintenance cycle
- eviction becomes a kernel-owned memory-pressure response

### Step 5: Migrate `List` and `ZSet`

- move `List` toward native listpack plus quicklist-style nodes
- move `ZSet` toward native packed form plus dictionary and skiplist
- remove the old heap fallback for each structure after migration

### Step 6: Finalize module renames and delete obsolete paths

- move current FFM/native memory concerns under a clearly named memory module
- collapse current core API/contract/db/command layers into a kernel-first boundary
- remove compatibility facades
- remove stale module boundaries
- remove tests that lock the old architecture in place

## Test Strategy

### 1. Kernel Semantic Tests

These become the primary correctness proof:

- lookup semantics
- lazy expire before type dispatch
- empty-container deletion
- atomic failure before commit
- mutation bookkeeping

### 2. Encoding Tests

These lock Redis-style upgrade behavior:

- string `int/embstr/raw`
- hash `listpack -> ht`
- set `intset -> ht`

Once phase 2 structure work begins, extend the same suite to:

- list `listpack -> quicklist`
- zset `listpack -> dict+skiplist`

### 3. Native Structure Tests

These validate FFM-based building blocks directly:

- allocator/runtime behavior
- native dict correctness
- incremental rehash
- expire dict correctness
- native listpack/intset/skiplist behavior
- ownership and release correctness

### 4. Protocol/Server Integration Tests

These remain necessary, but they move to the outermost layer:

- custom protocol request adaptation
- Netty execution handoff
- reply framing
- server lifecycle and backpressure integration

## First-Phase Deliverable

Phase 1 succeeds when the repository provides:

- one runnable server using the existing custom protocol
- one FFM-native mainline data-plane implementation
- one kernel-first execution path
- FFM-native keyspace and expire dictionaries
- FFM-native `String`, `Hash`, and `Set` implementations with Redis-style encoding upgrades
- phase 1 teaching-core commands that exercise those structures through Redis-like semantics

Phase 1 does not require:

- `List` and `ZSet` migration completion
- full Redis command coverage
- persistence
- replication
- cluster
- ACL/TLS
- encoding downgrade
- a fully native Redis object header
- a generalized allocator subsystem
- peak performance tuning beyond correctness and reasonable baseline behavior

## Explicit Non-Goals

- adopting RESP2 or RESP3
- preserving old module names purely for compatibility
- keeping heap and FFM data-plane implementations in parallel
- building a second clean-room implementation next to the current repository
- broadening scope to full Redis feature parity before the kernel path is clean

## Consequences

After this redesign, the repository will be less backwards-compatible internally, but more coherent:

- contributors will learn one kernel story
- FFM will become a visible architectural choice instead of a hidden implementation detail
- data structures will better mirror Redis concepts
- protocol and server concerns will stop dominating the core design
- early phases will carry less rename churn than the original broader proposal

That trade-off is intentional and correct for the stated project goal.
