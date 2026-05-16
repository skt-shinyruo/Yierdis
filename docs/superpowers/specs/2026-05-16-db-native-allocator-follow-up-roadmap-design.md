# DB Native Allocator Follow-Up Roadmap

## Status

Draft roadmap spec for the work that remains after `docs/superpowers/specs/2026-05-14-db-native-allocator-unification-design.md` was completed and merged.

This document does not reopen the completed migration. It defines the remaining work as four sequenced follow-up tracks so each one can later become its own concrete spec and implementation plan.

## Problem

The allocator unification work finished the core DB migration, but four useful gaps remain:

- benchmark comparisons do not yet have a trustworthy before/after baseline pair in the current environment
- collection internals are still partly transitional and not fully native-backed
- some project docs still describe older transitional shapes alongside the new allocator-backed state
- production hardening around soak, CI smoke, and metrics stability still needs more depth

These are related, but they are not one change. They should stay separated so each track can be validated and committed independently.

## Goals

- Define the remaining work as a small set of ordered follow-up specs rather than one oversized implementation blob.
- Preserve the completed allocator migration as a fixed baseline.
- Make future work measurable, reviewable, and easy to decompose into sub-specs and plans.
- Keep documentation and benchmark records aligned with current behavior.

## Non-Goals

- Do not change the already merged DB native allocator migration in this document.
- Do not re-specify entry/string/key/root allocator ownership here.
- Do not expand the scope back into unrelated systems such as AOF, RDB, clustering, or protocol work.
- Do not implement any code in this document.

## Roadmap Structure

This roadmap is intentionally split into four follow-up specs:

1. benchmark baseline repair
2. collection internal node nativeization
3. documentation convergence
4. production hardening

The order matters because later work becomes easier to validate once the benchmark baseline is stable and the docs are aligned.

### Track 1: Benchmark Baseline Repair

**Purpose**

Restore a trustworthy before/after benchmark comparison for the allocator migration work.

**Current state**

The current branch has benchmark results for the new allocator path, but the older baseline commit used for comparison does not currently produce a reliable comparable workload in this environment. Minimal probes against the baseline server have returned `ERR internal error`, which makes the raw numbers unsuitable as a direct before/after pair.

**Desired state**

- baseline and current can both execute the same RESP workload set
- the benchmark report records comparable numbers for both sides
- the report clearly states any remaining environment assumptions or limitations

**Primary files to inspect in the child spec**

- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBench.java`
- `yierdis-benchmark/src/main/java/yier/bubu/redis/app/bench/YierdisBenchServerArgs.java`
- `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/*`
- `docs/superpowers/reports/*benchmark*`

**Acceptance criteria**

- baseline and current run the same command shape without extra errors
- benchmark output includes a clear before/after comparison
- the report states the exact commit or environment caveat if the baseline still cannot be made fully comparable

### Track 2: Collection Internal Node Nativeization

**Purpose**

Move collection internal structures behind allocator-backed handles, not just collection root identity.

**Current state**

Collection roots are allocator-backed, but some internal structures remain transitional adapter-owned data. That is acceptable for the completed migration, but it is the next structural step if we want the allocator model to cover more of the DB graph.

**Desired state**

- internal list/hash/set/zset nodes are allocator-backed in a controlled order
- handle liveness stays with the allocator, not with heap adapter tables
- defrag can move live internal nodes without changing logical DB behavior
- stale handle checks continue to fail correctly after delete, recycle, or generation reuse

**Recommended decomposition**

This track may need to split again if it becomes too large:

- list / quicklist internals
- hash internals
- set internals
- zset / skiplist internals

Each child spec should define:

- object kind
- layout
- ownership
- free order
- defrag move rules
- stats ownership
- stale handle behavior

**Primary files to inspect in the child spec**

- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/*Root.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/*`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraph.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/*`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraphTest.java`

**Acceptance criteria**

- collection behavior stays logically identical
- allocator stats include the new internal node kinds
- deleting a collection makes its node handles stale
- defrag does not break a live collection graph
- mixed churn and shutdown still return native used bytes to zero

### Track 3: Documentation Convergence

**Purpose**

Make the project docs match the current allocator-backed state without leaving contradictory historical descriptions in core docs.

**Current state**

The migration and benchmark work updated a lot of docs, but the doc set still has multiple layers of explanation. Some pages are current-state reference docs; some are transitional notes. A follow-up pass should make those distinctions explicit and consistent.

**Desired state**

- current-state docs describe the allocator-backed behavior directly
- transitional docs clearly label what is still adapter-owned
- glossary and walkthrough docs do not conflict with the actual code path
- benchmark and report docs point to the same verified state

**Primary files to review**

- `docs/project-docs/native-allocator-and-handles.md`
- `docs/project-docs/db-internals.md`
- `docs/project-docs/ffm-usage.md`
- `docs/project-docs/core-logic-index.md`
- `docs/project-docs/glossary.md`
- `docs/project-docs/request-execution-flow.md`
- `docs/project-docs/offheap-copy-behavior.md`
- `docs/project-docs/module-architecture.md`

**Acceptance criteria**

- searches for old transitional phrases only return explicitly transitional sections
- core docs agree on which objects are allocator-backed and which remain adapter-owned
- readers can understand current behavior without cross-reading conflicting notes

### Track 4: Production Hardening

**Purpose**

Strengthen confidence in the completed allocator model through deeper soak, smoke, and observability checks.

**Current state**

Core tests already pass, and benchmark output is recorded. The next step is to make long-running and operational validation more durable.

**Desired state**

- longer churn / soak runs stay stable
- native leak checks remain clean over repeated executions
- benchmark and maintenance metrics remain interpretable
- lightweight CI smoke exists for the allocator path

**Primary areas to inspect in the child spec**

- churn / soak coverage in DB tests
- benchmark smoke or scripted runs
- allocator / defrag metric assertions
- maxmemory policy regressions
- shutdown and cleanup stability checks

**Acceptance criteria**

- repeated runs do not accumulate native leaks
- the allocator metrics remain stable enough to compare between runs
- CI-style smoke checks can detect a regression without needing a huge benchmark

## Child Spec Outputs

This roadmap should be turned into these next design documents:

- `docs/superpowers/specs/2026-05-16-db-native-allocator-benchmark-baseline-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-collection-internal-node-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-documentation-convergence-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-production-hardening-design.md`

Track 2 may split further if one child spec would become too large; if that happens, split by object family or test surface instead of mixing unrelated concerns.

## Delivery Order

Recommended order:

1. benchmark baseline repair
2. collection internal node nativeization
3. documentation convergence
4. production hardening

Reasoning:

- the benchmark baseline should be fixed first so later work has a comparable measurement path
- collection nativeization is the main structural follow-up and should be measured against the repaired baseline
- docs should be tightened once the next state is clear
- hardening is most effective after the behavior and documentation are stable

## Completion Criteria

This roadmap is satisfied when all four follow-up tracks have been turned into concrete specs and their implementation work has been completed or explicitly documented as out of scope.

At that point:

- benchmark comparison is trustworthy
- collection internal nodes have a clear nativeization path
- docs match current behavior
- production hardening is demonstrably stronger than the completed migration baseline
