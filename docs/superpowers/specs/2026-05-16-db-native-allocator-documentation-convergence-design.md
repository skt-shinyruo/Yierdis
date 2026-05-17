# DB Native Allocator Documentation Convergence Design

## Status

Track 3 child spec for documentation convergence after the DB native allocator migration and the Track 1 benchmark reporting repair.

This document is scoped to project documentation only. It builds on `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md` but does not change that parent roadmap.

## Problem

The allocator migration and follow-up benchmark work changed the current DB memory model faster than the surrounding docs converged. Some docs now describe the current production path directly, while older roadmap and transitional notes still use wording that can read as current-state truth if encountered out of context.

The highest-risk documentation drift is around these boundaries:

- key bytes are now represented as stable allocator `KEY_BYTES` objects in the production key directory path.
- entry records and string payloads are allocator-backed stable handles.
- collection root handles are allocator-backed root records; list quicklist metadata nodes are allocator-backed after Track 2, but collection payload bytes and hash/set/zset internals remain adapter-owned or legacy FFM-owned.
- DB native defrag is a maintenance hook over allocator-backed objects, not a generic compactor for legacy adapter payload bytes.
- scan and snapshot safety is expressed through allocator epochs and copied output, not exposed native addresses.
- benchmark reports include useful current numbers, but before/after comparisons must stay caveated unless both baseline and current complete the same workload shape.

Readers should not need to infer which of those statements are current behavior by reconciling several partially historical docs.

## Goals

- Make current-state docs agree on the allocator-backed status of key bytes, entry records, string payloads, collection roots, and list quicklist metadata nodes.
- Clearly label transitional or future-facing notes about remaining collection internals, adapter-owned payload bytes, DB defrag boundaries, scan/snapshot epoch behavior, and benchmarks.
- Preserve historical roadmap/spec content as historical design context without letting it masquerade as the current reference.
- Define a stale-phrase audit that can be run before the later documentation update work is planned task by task.
- Keep benchmark documentation aligned with the Track 1 comparison caveat.

## Non-Goals

- Do not change allocator, DB, benchmark, or test implementation behavior.
- Do not rewrite or edit the parent roadmap spec.
- Do not touch `yierdis.md`.
- Do not create the implementation plan in this document.
- Do not expand Track 3 into new key-byte migration, additional collection internal node nativeization, DB defrag policy, scan/snapshot epoch implementation, or benchmark harness work.
- Do not remove useful historical specs or reports solely because their content is no longer current-state reference material.

## Current State

`docs/project-docs/native-allocator-and-handles.md` is the closest current-state allocator reference, but it still contains collection-root wording from an earlier transitional state. It correctly states that `EntryHandle` wraps `ENTRY_RECORD` and string `ValueHandle` wraps `STRING_BYTES`, but the later documentation pass must update the collection section to say that list/hash/set/zset root records are allocator-backed while only remaining internals and payload adapters are transitional.

`docs/project-docs/db-internals.md` describes the DB storage graph: `NativeKeyDirectory` maps key bytes to entry handles, `EntryTable` stores native `ENTRY_RECORD` metadata, string payloads use allocator-backed `STRING_BYTES`, and collection roots wrap internal payload adapters behind allocator-backed root records.

`docs/project-docs/ffm-usage.md` already documents that production key bytes are stable allocator `KEY_BYTES` objects while table arrays and some adapter structures remain heap or legacy FFM-owned.

`docs/project-docs/request-execution-flow.md`, `docs/project-docs/core-logic-index.md`, `docs/project-docs/glossary.md`, and `docs/project-docs/module-architecture.md` contain concise allocator summaries that should agree with the reference docs.

`docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md` records current benchmark output and explicitly caveats the baseline/current comparison because the historical baseline did not complete minimal probes cleanly in this environment.

The implementation truth to preserve in documentation is:

- `NativeObjectKind.KEY_BYTES` exists and is counted by allocator stats.
- `NativeKeyDirectory` owns production key byte handles and frees them on removal.
- `NativeCollectionRootTable` allocates root records through `NativeAllocator` for list/hash/set/zset roots.
- `NativeObjectKind.LIST_QUICKLIST_NODE` exists for FFM list quicklist metadata records, and those records are counted separately from `LIST_NODE` root records.
- List quicklist payload bytes remain owned by listpack/blob-store structures; `LIST_QUICKLIST_NODE` counts metadata records only.
- `YierdisDbNativeHandleGraph` visits key bytes, entry records, string values, and collection roots, but does not traverse list quicklist internals or other collection internals.
- `YierdisDb.defragMaintenance()` runs allocator `defragCycle(...)` under configured DB maintenance options.
- `YierdisDbIntrospection.snapshot(...)` opens a `SNAPSHOT` epoch while copying snapshot output.
- `NativeEpochKind` includes command, scan, snapshot, and defrag scopes.

## Desired State

The docs should present one consistent model:

```text
NativeKeyDirectory
  key bytes -> KEY_BYTES NativeHandle -> EntryHandle

EntryTable
  EntryHandle -> ENTRY_RECORD NativeHandle

EntryRecord
  ValueType + ValueEncoding + ValueHandle

TypeRoot
  string -> STRING_BYTES NativeHandle
  list root -> LIST_NODE root record
    FFM quicklist metadata -> LIST_QUICKLIST_NODE records
    list payload bytes -> listpack/blob-store ownership
  hash/set/zset root -> HASH_NODE / SET_NODE / ZSET_NODE root record
    remaining internals and payload bytes -> adapter-owned or legacy FFM-owned structures
```

The current-state docs should say directly that key bytes are allocator-backed in the production path. Phrases that describe key bytes as still waiting for migration should appear only in historical specs or explicitly labeled migration-history sections.

The collection docs should distinguish four layers:

- root record: allocator-backed `LIST_NODE`, `HASH_NODE`, `SET_NODE`, or `ZSET_NODE`.
- list quicklist metadata record: allocator-backed `LIST_QUICKLIST_NODE`, owned by `ListValue`, root-only in handle graph traversal for this split.
- root adapter: Java object retained by `NativeCollectionRootTable` to operate on the payload.
- internal payload structures: still adapter-owned or legacy FFM-owned except for list quicklist metadata records; payload bytes are not counted as `LIST_QUICKLIST_NODE`.

The defrag docs should say that DB maintenance defrag moves allocator-backed objects through stable handles. They should not imply that current DB defrag compacts all legacy blob-store, listpack, hash, set, or zset payload internals.

The scan/snapshot docs should say that native views are resolved only inside bounded operations, output bytes are copied, and allocator epochs protect retired blocks from premature reclamation.

The benchmark docs should separate:

- allocator micro/eval output
- current branch RESP benchmark output
- DB native defrag comparison output
- baseline/current comparison status and caveats

## Primary Docs To Review

Current-state reference docs:

- `docs/project-docs/native-allocator-and-handles.md`
- `docs/project-docs/db-internals.md`
- `docs/project-docs/ffm-usage.md`
- `docs/project-docs/core-logic-index.md`
- `docs/project-docs/glossary.md`
- `docs/project-docs/request-execution-flow.md`
- `docs/project-docs/offheap-copy-behavior.md`
- `docs/project-docs/module-architecture.md`

Related docs that should be checked for cross-link drift:

- `docs/project-docs/bytes-and-fast-paths.md`
- `docs/project-docs/commands-and-data-model.md`
- `docs/project-docs/main-path-walkthrough.md`
- `docs/project-docs/development-navigation.md`
- `docs/project-docs/testing-and-debugging.md`
- `docs/project-docs/client-and-bench-internals.md`
- `docs/project-docs/configuration-and-operations.md`
- `docs/project-docs/operation-test-coverage-matrix.md`
- `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`

Historical specs and plans may be referenced, but should not be rewritten as part of Track 3 unless a later plan explicitly scopes a small clarifying note:

- `docs/superpowers/specs/2026-05-14-db-native-allocator-unification-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-benchmark-baseline-design.md`
- `docs/superpowers/specs/2026-05-16-db-native-allocator-list-quicklist-node-design.md`
- `docs/superpowers/plans/*db-native-allocator*`

## Stale Phrase Audit Strategy

The later implementation plan should begin with a phrase audit before editing prose. The audit should classify matches as one of:

- current reference text, keep or tighten
- stale current-state text, update
- historical/spec text, keep but ensure context is clear
- future work boundary, keep only if it is explicitly labeled

Suggested searches:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs docs/superpowers
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs docs/superpowers
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs docs/superpowers
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs docs/superpowers
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
```

Audit rules:

- `key bytes remain blob-store-owned` is stale as current-state docs; it is acceptable only in historical migration context.
- `key bytes will migrate later` is stale as current-state docs; it is acceptable only when describing the older unification plan timeline.
- `collection roots are adapter-owned` is stale as current-state docs; current docs should say root records are allocator-backed while internal adapters and most payload structures remain transitional.
- `collection internals are all adapter-owned` is now too broad because list quicklist metadata records are allocator-backed after Track 2.
- `collection handles can always be resolved as allocator objects` is too broad; docs should explain which root records and list metadata records are allocator-backed and avoid implying every internal collection value or payload byte is nativeized.
- `DB defrag moves all DB native memory` is too broad; current docs should restrict DB defrag to allocator-backed objects.
- `snapshot/scan exposes native memory` is wrong for current reference docs; docs should describe copied output and bounded resolve scopes.
- benchmark numbers must not be called a trustworthy before/after comparison unless the report names clean baseline and current runs.

## Acceptance Criteria

- Current-state docs agree that production key bytes are allocator-backed `KEY_BYTES` objects owned by `NativeKeyDirectory`.
- Current-state docs agree that entry metadata is allocator-backed `ENTRY_RECORD` and string payloads are allocator-backed `STRING_BYTES`.
- Current-state docs distinguish allocator-backed collection root records and list quicklist metadata records from remaining adapter-owned collection internals and payload bytes.
- DB defrag documentation describes allocator-backed stable-handle movement and does not claim to compact legacy adapter payload structures.
- Scan and snapshot documentation describes bounded handle resolution, copied output, and epoch/quarantine safety without exposing physical addresses as stable references.
- Benchmark docs preserve the Track 1 caveat: current branch numbers are useful, but baseline/current deltas are non-comparable unless both sides complete the same workload shape.
- Searches for stale transitional phrases either return no current-state contradictions or return explicitly historical/future-context sections.
- Cross-links point readers from summary docs to the current reference docs instead of forcing them to reconcile contradictory summaries.
- No edits are made to the parent roadmap spec or `yierdis.md`.

## Delivery And Decomposition

This Track 3 spec is suitable for later task-by-task planning in five documentation slices:

1. Truth-source audit

   Review the implementation and the current reference docs for key bytes, collection roots, list quicklist metadata records, DB defrag maintenance, scan/snapshot epochs, and benchmark reporting. Produce a small audit note in the later plan before editing docs.

2. Current-state reference convergence

   Update the main reference docs first: `native-allocator-and-handles.md`, `db-internals.md`, and `ffm-usage.md`. These docs should establish the authoritative wording for allocator ownership and transitional boundaries.

3. Summary and navigation convergence

   Update concise docs such as `core-logic-index.md`, `glossary.md`, `request-execution-flow.md`, `module-architecture.md`, and navigation/testing docs so they point to the same model without duplicating too much detail.

4. Benchmark/report wording convergence

   Verify benchmark docs and reports preserve the distinction between current metrics, DB native defrag comparison, and baseline/current comparability.

5. Stale phrase closure

   Re-run the phrase audit and classify any remaining matches. The final documentation PR should state which remaining matches are intentionally historical or future-facing.

Each later task should be small enough to review as documentation-only work and should name the docs it edits. None of these slices should introduce code changes.

## Risks

- Historical specs can look contradictory if readers land there first; cross-links and labels should make the current reference docs obvious.
- Collection wording is easy to overcorrect. The docs must not erase the fact that root records and list quicklist metadata records are allocator-backed, and must not claim payload bytes or all internal nodes are fully nativeized.
- Benchmark wording can overstate confidence. The report should keep numeric results useful while preserving the baseline caveat.
- The phrase audit can produce false positives in historical plans; classification matters more than mechanically deleting every match.

## Blockers

No blockers.
