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

### Break Change 5: Rename modules and paths to match the kernel-first model

Reason:

- names shape how contributors read the repository
- if the names still center server/protocol boundaries, the redesign remains conceptual only
- the new names should make the intended architecture obvious without reading design docs first

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
- LRU metadata in phase 1
- native payload handle

Keyspace entries should point to a compact object header plus native payload, rather than storing large Java object graphs as the main data-plane representation.

## Data Structure Design

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

- small lists use native listpack
- upgraded lists use native quicklist-style nodes

### ZSet

- small sorted sets use native listpack
- upgraded sorted sets use native dictionary plus native skiplist

### HLL

`HLL` may remain an extension outside the initial teaching-core path. It should not drive the phase 1 architecture.

## Teaching-Core Command Scope

The first phase teaching-core command set is:

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

### List

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `LRANGE`

### ZSet

- `ZADD`
- `ZREM`
- `ZRANGE`

Other commands may remain in the repository, but they are not allowed to dictate the phase 1 kernel design.

## Module Migration Plan

### Step 1: Rename and shrink module boundaries

- move current FFM/native memory concerns under a clearly named memory module
- collapse current core API/contract/db/command layers into a kernel-first boundary
- keep protocol and server as outer layers

### Step 2: Introduce one kernel command runner

- centralize lookup, expire, type dispatch, reservation, commit, and bookkeeping
- keep existing protocol and reply behavior stable where possible

### Step 3: Make keyspace and expire dict first-class kernel objects

- move TTL and lookup semantics behind a unified kernel path

### Step 4: Migrate each core data structure to one FFM-native path

Migration order:

1. `String`
2. `Hash`
3. `Set`
4. `List`
5. `ZSet`

After each migration:

- remove the old heap fallback for that type
- move tests to the kernel/encoding/native-structure split

### Step 5: Reattach active expire and eviction to kernel maintenance

- active expire becomes a kernel-owned maintenance cycle
- eviction becomes a kernel-owned memory-pressure response

### Step 6: Delete obsolete paths

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
- one FFM-native data-plane implementation
- one kernel-first execution path
- Redis-style basic data structures with encoding upgrades
- teaching-core commands that exercise those structures through Redis-like semantics

Phase 1 does not require:

- full Redis command coverage
- persistence
- replication
- cluster
- ACL/TLS
- encoding downgrade
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

That trade-off is intentional and correct for the stated project goal.
