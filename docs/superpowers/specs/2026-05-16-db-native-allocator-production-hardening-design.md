# DB Native Allocator Production Hardening Design

## Status

Draft child spec for Track 4 of the DB native allocator follow-up roadmap.

This document narrows the remaining work to operational hardening only. It does not reopen allocator unification, collection nativeization, scan/snapshot behavior, or benchmark framework design.

## Problem

The allocator model is now functionally complete enough for core tests, but production confidence is still thin in the places that matter after a merge:

- long-running churn can regress without being caught by a short unit test
- repeated runs can hide native leak accumulation unless cleanup is asserted explicitly
- allocator and defrag metrics need a stable interpretation so runs can be compared
- smoke validation exists, but it is not yet a dedicated allocator regression sentinel

The next step is to harden the current behavior with repeatable soak, smoke, and metric checks that stay small enough to run often.

## Goals

- Add deterministic repeated native churn / soak coverage for the DB path.
- Assert native cleanup across repeated executions, including shutdown paths.
- Make allocator / defrag metrics stable enough to compare across runs.
- Provide a lightweight CI-style smoke entry point for allocator regressions.
- Keep the scope focused on validation, not on allocator policy or data-structure redesign.

## Non-Goals

- No broad CI infrastructure changes.
- No new benchmark framework.
- No defrag policy changes.
- No new collection nativeization.
- No scan/snapshot implementation changes.
- No key-byte migration work.
- No unrelated production system changes.

## Current State

The roadmap already identifies this as the final follow-up track after the allocator migration work.

Current validation already includes:

- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
  - root release-to-zero coverage after delete
  - production collection root release and shutdown cleanup
  - scan/snapshot quarantine assertions
  - mixed native DB churn and defrag-oriented regression coverage
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
  - stale-handle detection
  - allocator defrag / quarantine / stats coverage
  - deterministic churn stress with accounting checks
- `scripts/smoke.sh`
  - lightweight server startup plus PING / SET / GET / bench smoke
  - tiny bench defaults intended for correctness, not scale
- `scripts/bench.sh`
  - benchmark launcher with configurable server and workload settings
  - already supports small overrides that can be reused for smoke-like checks

Relevant documentation context:

- `docs/project-docs/native-allocator-and-handles.md`
  - allocator-owned handle semantics
  - quarantine / epoch / defrag behavior
  - stable handle and object-table boundaries
- `docs/project-docs/ffm-usage.md`
  - project-wide FFM context
  - useful for keeping smoke / benchmark wording grounded
- `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`
  - defines Track 4 scope and acceptance criteria

## Design

### 1. Deterministic repeated native churn / soak suite

Add or extend a small DB-facing regression path that runs the same native churn sequence multiple times with a fixed seed and explicit cleanup between iterations.

The point is not raw load. The point is to prove that allocator state returns to baseline after repeated create / mutate / delete / shutdown cycles.

The suite should:

- use deterministic data and operation ordering
- include mixed string and collection churn where allocator cleanup is exercised
- run enough repetitions to expose state accumulation, but stay short enough for regular test runs
- assert post-iteration allocator / runtime cleanup rather than only end-of-test success

### 2. Native leak cleanup assertions across repeated executions

Add repeatable assertions that the runtime returns to a clean native state after each cycle and after final shutdown.

Preferred checks:

- runtime used bytes returns to zero when the DB is closed
- collection-root counts or equivalent allocation counts return to expected baselines
- no retained quarantine state remains after safe shutdown and cleanup

This track should explicitly test repeated execution, not just one-off cleanup.

### 3. Allocator and defrag metric stability assertions

Use existing allocator stats and DB memory stats as the comparison surface.

The spec should require:

- metrics are asserted in relative terms that remain stable across reruns
- deterministic churn produces the same or bounded stats profile from run to run
- defrag / quarantine counters are checked for presence or absence where meaningful, without depending on exact global totals that can drift with unrelated implementation changes

The intent is to keep metrics interpretable, not to freeze every counter forever.

### 4. Maxmemory and allocator regression checks

Add a narrow regression check for allocator behavior under maxmemory policy pressure.

This should focus on whether native allocator cleanup and admission behavior remain sane when the DB is configured near its memory limit, not on changing policy semantics.

The check should cover:

- no unexpected leak after bounded writes / deletes / shutdown
- no regression in cleanup when maxmemory settings are enabled
- behavior remains deterministic enough for comparison in CI or local smoke runs

### 5. Lightweight CI-style smoke entry point

Provide a small operational smoke path that can catch allocator regressions without invoking a large benchmark run.

The smoke path should:

- use the existing `scripts/smoke.sh` and/or a small wrapper convention rather than introducing a new framework
- keep workload tiny
- verify server startup, basic commands, and a representative allocator-sensitive path
- fail fast on cleanup, leak, or allocator-accounting regressions

If a script needs a new mode or override, it should remain a thin wrapper around existing launch behavior.

## Primary Files To Inspect Later

- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/NativeStorageRegressionTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
- `scripts/smoke.sh`
- `scripts/bench.sh`
- `docs/project-docs/native-allocator-and-handles.md`
- `docs/project-docs/ffm-usage.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`

## Acceptance Criteria

- repeated native churn runs do not accumulate leaks across execution cycles
- shutdown and cleanup checks return the runtime to a clean state
- allocator and defrag metrics are stable enough to compare between runs
- maxmemory-regression coverage catches cleanup or accounting regressions
- a lightweight smoke path can fail on allocator regressions without requiring a huge benchmark

## Risks And Caveats

- Overfitting assertions to exact metric values can make the hardening suite brittle.
- A smoke path that is too small may miss slow accumulation bugs.
- A soak path that is too large will stop being run often enough to be useful.
- This track should not grow into a new benchmark suite; the benchmark baseline belongs to the earlier roadmap track.

## Implementation Decomposition Notes

Keep the later implementation plan split along the validation surfaces, not the allocator internals:

1. extend or parameterize the DB regression tests for deterministic repeated churn
2. add cleanup and leak assertions around repeated runtime shutdowns
3. add bounded allocator / defrag metric assertions
4. add a narrow maxmemory regression check
5. wire a lightweight smoke entry path that reuses existing scripts

This should stay a small hardening track that improves confidence in the allocator path without changing core storage semantics.
