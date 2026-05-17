# DB Native Allocator Documentation Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Each implementation task must be handled by a fresh subagent; the main agent reviews the subagent's diff, runs the task verification, and commits only after that review. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Converge project documentation on the current DB native allocator model without changing code, benchmarks, historical roadmap content, or `yierdis.md`.

**Architecture:** Treat `docs/project-docs/native-allocator-and-handles.md`, `docs/project-docs/db-internals.md`, and `docs/project-docs/ffm-usage.md` as the current-state reference layer, then align summary/navigation docs and benchmark/report wording to those references. Preserve historical specs and old plans as historical context, classifying stale phrases instead of mechanically deleting every match. Keep the scope limited to key bytes, collection root/list quicklist metadata boundaries, DB defrag maintenance wording, scan/snapshot epoch wording, and benchmark caveats.

**Tech Stack:** Markdown documentation, `rg` stale-phrase audits, `git diff --check`, `git status`.

---

## Scope

This plan implements Track 3 from `docs/superpowers/specs/2026-05-16-db-native-allocator-documentation-convergence-design.md`.

In scope:

- Current-state wording that production key bytes are allocator-backed `KEY_BYTES` objects owned by `NativeKeyDirectory`.
- Current-state wording that entry records are allocator-backed `ENTRY_RECORD` objects and string payloads are allocator-backed `STRING_BYTES` objects.
- Collection ownership boundaries: allocator-backed list/hash/set/zset root records, allocator-backed `LIST_QUICKLIST_NODE` metadata records after Track 2, root adapters retained for operation, and remaining collection payload bytes/internals still adapter-owned or legacy FFM-owned.
- DB defrag maintenance wording: allocator-backed stable-handle movement only, not generic compaction for legacy adapter payload bytes.
- Scan/snapshot wording: bounded handle resolution, copied output, and allocator epoch/quarantine safety, not stable exposed physical addresses.
- Benchmark/report caveats: current branch numbers are useful, but baseline/current deltas are non-comparable unless both sides complete the same workload shape.

Out of scope:

- Code, test, Maven, benchmark harness, allocator, DB, or runtime behavior changes.
- Editing `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`.
- Editing `yierdis.md`.
- Rewriting historical specs or old plans except for a small clarifying label only if a task explicitly scopes that edit and it is not the parent roadmap spec.
- Expanding Track 3 into new key-byte migration, new collection internal nativeization, DB defrag policy changes, scan/snapshot implementation work, or benchmark execution work.

## File Map

- Modify: `docs/project-docs/native-allocator-and-handles.md`
  - Establish the allocator-backed object model and collection boundary terminology.
- Modify: `docs/project-docs/db-internals.md`
  - Align DB storage graph wording with key bytes, entry records, string payloads, collection roots, list quicklist metadata, defrag, and scan/snapshot boundaries.
- Modify: `docs/project-docs/ffm-usage.md`
  - Align FFM ownership and transitional boundary wording.
- Modify: `docs/project-docs/core-logic-index.md`
  - Keep concise current-state summary and links consistent with reference docs.
- Modify: `docs/project-docs/glossary.md`
  - Update terms for `KEY_BYTES`, `ENTRY_RECORD`, `STRING_BYTES`, collection roots, `LIST_QUICKLIST_NODE`, epochs, and DB defrag.
- Modify: `docs/project-docs/request-execution-flow.md`
  - Align command, scan, and snapshot descriptions with bounded resolve/copy/epoch wording.
- Modify: `docs/project-docs/module-architecture.md`
  - Align module-level allocator ownership summary.
- Modify as needed: `docs/project-docs/offheap-copy-behavior.md`
  - Ensure output-copy wording does not imply exposed stable native addresses.
- Modify as needed: `docs/project-docs/bytes-and-fast-paths.md`
  - Align key bytes and copied output wording.
- Modify as needed: `docs/project-docs/commands-and-data-model.md`
  - Align data model summaries without duplicating reference detail.
- Modify as needed: `docs/project-docs/main-path-walkthrough.md`
  - Align request path summaries for allocator handles and copied outputs.
- Modify as needed: `docs/project-docs/development-navigation.md`
  - Point readers to current reference docs for allocator ownership.
- Modify as needed: `docs/project-docs/testing-and-debugging.md`
  - Keep diagnostics wording consistent with current allocator boundaries.
- Modify as needed: `docs/project-docs/client-and-bench-internals.md`
  - Preserve benchmark caveats and avoid overclaiming before/after comparability.
- Modify as needed: `docs/project-docs/configuration-and-operations.md`
  - Align DB defrag maintenance wording with allocator-backed objects only.
- Modify as needed: `docs/project-docs/operation-test-coverage-matrix.md`
  - Align concise notes if stale allocator wording appears.
- Modify: `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`
  - Preserve Track 1 caveat and separate current metrics from non-comparable baseline/current deltas.

Do not modify:

- `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md`
- `yierdis.md`

## Audit Commands And Classification Rules

Every implementation task that edits docs must begin by running the relevant audit command(s), and Task 6 must run all of them:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs docs/superpowers
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs docs/superpowers
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs docs/superpowers
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs docs/superpowers
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
```

Classify every relevant match as one of:

- current reference text, keep or tighten
- stale current-state text, update
- historical/spec text, keep but ensure context is clear
- future work boundary, keep only if it is explicitly labeled

Expected rules:

- `key bytes remain blob-store-owned` is stale as current-state docs; it is acceptable only in historical migration context.
- `key bytes will migrate later` is stale as current-state docs; it is acceptable only when describing the older unification plan timeline.
- `collection roots are adapter-owned` is stale as current-state docs; current docs should say root records are allocator-backed while internal adapters and most payload structures remain transitional.
- `collection internals are all adapter-owned` is now too broad because list quicklist metadata records are allocator-backed after Track 2.
- `collection handles can always be resolved as allocator objects` is too broad; docs should explain which root records and list metadata records are allocator-backed and avoid implying every internal collection value or payload byte is nativeized.
- `DB defrag moves all DB native memory` is too broad; current docs should restrict DB defrag to allocator-backed objects.
- `snapshot/scan exposes native memory` is wrong for current reference docs; docs should describe copied output and bounded resolve scopes.
- benchmark numbers must not be called a trustworthy before/after comparison unless the report names clean baseline and current runs.

## Task 1: Truth-Source And Stale Phrase Audit

**Files:**
- Modify: no project docs in this task unless recording a short audit note in the task handoff message
- Read: `docs/superpowers/specs/2026-05-16-db-native-allocator-documentation-convergence-design.md`
- Read: `docs/project-docs/native-allocator-and-handles.md`
- Read: `docs/project-docs/db-internals.md`
- Read: `docs/project-docs/ffm-usage.md`
- Read: `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`

- [x] **Step 1: Dispatch a fresh subagent for Task 1**

Ask the subagent to run the audit commands from "Audit Commands And Classification Rules" and return a concise classification table. The subagent must not edit files.

- [x] **Step 2: Run key-byte audit**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs docs/superpowers
```

Expected: matches are classified as current reference text, stale current-state text, historical/spec text, or future work boundary. Any current-state claim that key bytes remain blob-store-owned or will migrate later is marked for update in Tasks 2 or 3.

- [x] **Step 3: Run collection audit**

Run:

```bash
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs docs/superpowers
```

Expected: current-state docs distinguish allocator-backed root records and `LIST_QUICKLIST_NODE` metadata from remaining adapter-owned internals and payload bytes. Broad current-state claims that all collection internals are adapter-owned are marked for update.

- [x] **Step 4: Run defrag audit**

Run:

```bash
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs docs/superpowers
```

Expected: current-state docs restrict DB defrag to allocator-backed stable-handle movement. Any claim that DB defrag compacts all DB native memory or legacy adapter payloads is marked for update.

- [x] **Step 5: Run scan/snapshot audit**

Run:

```bash
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs docs/superpowers
```

Expected: current-state docs describe bounded handle resolution, copied output, and epoch/quarantine safety. Any claim that scan/snapshot exposes stable native physical addresses is marked for update.

- [x] **Step 6: Run benchmark audit**

Run:

```bash
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
```

Expected: benchmark docs separate allocator micro/eval output, current branch RESP benchmark output, DB native defrag comparison output, and baseline/current comparison caveats. Any uncaveated before/after performance claim is marked for update.

- [x] **Step 7: Main agent review and commit**

The main agent reviews the subagent's classification, confirms no project docs changed, marks Task 1 complete in this plan, and commits the plan-only progress change before starting Task 2.

## Task 2: Current-State Reference Docs Convergence

**Files:**
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/db-internals.md`
- Modify: `docs/project-docs/ffm-usage.md`

- [x] **Step 1: Dispatch a fresh subagent for Task 2**

Ask the subagent to update only the three reference docs listed above. The subagent must use Task 1 classifications and must not edit historical specs, the parent roadmap spec, or `yierdis.md`.

- [x] **Step 2: Update key-byte reference wording**

Ensure the reference docs say production key bytes are allocator-backed `KEY_BYTES` objects owned by `NativeKeyDirectory`, and that key deletion frees the allocator handle. Remove or relabel any current-state wording that says key bytes are still blob-store-owned or waiting for migration.

- [x] **Step 3: Update entry and string reference wording**

Ensure the reference docs consistently say `EntryTable` stores allocator-backed `ENTRY_RECORD` metadata and string values use allocator-backed `STRING_BYTES` handles. Avoid implying string output exposes long-lived allocator views.

- [x] **Step 4: Update collection boundary reference wording**

Ensure the reference docs distinguish:

- allocator-backed root records: `LIST_NODE`, `HASH_NODE`, `SET_NODE`, `ZSET_NODE`
- allocator-backed list quicklist metadata records: `LIST_QUICKLIST_NODE`
- retained root adapters used to operate on payloads
- remaining payload bytes and hash/set/zset internals that stay adapter-owned or legacy FFM-owned

- [x] **Step 5: Update defrag and scan/snapshot reference wording**

Ensure DB defrag is described as maintenance over allocator-backed stable handles only. Ensure scan/snapshot wording describes bounded `NativeObjectView` resolution, copied output, and command/scan/snapshot/defrag epoch scopes.

- [x] **Step 6: Verify Task 2 docs**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|collection roots|collection root|adapter-owned|LIST_QUICKLIST_NODE|defrag|epoch|SNAPSHOT|SCAN|physical address|NativeObjectView" docs/project-docs/native-allocator-and-handles.md docs/project-docs/db-internals.md docs/project-docs/ffm-usage.md
git diff --check
git status --short
```

Expected: matches in the three reference docs are current-state accurate or explicitly transitional; `git diff --check` prints no errors; `git status --short` lists only the intended reference doc changes.

- [x] **Step 7: Main agent review and commit**

The main agent reviews the diff for overbroad collection claims and uncaveated defrag/scan wording, reruns the verification commands, and commits this task before starting Task 3.

## Task 3: Summary And Navigation Docs Convergence

**Files:**
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/glossary.md`
- Modify: `docs/project-docs/request-execution-flow.md`
- Modify: `docs/project-docs/module-architecture.md`
- Modify as needed: `docs/project-docs/offheap-copy-behavior.md`
- Modify as needed: `docs/project-docs/bytes-and-fast-paths.md`
- Modify as needed: `docs/project-docs/commands-and-data-model.md`
- Modify as needed: `docs/project-docs/main-path-walkthrough.md`
- Modify as needed: `docs/project-docs/development-navigation.md`
- Modify as needed: `docs/project-docs/testing-and-debugging.md`
- Modify as needed: `docs/project-docs/configuration-and-operations.md`
- Modify as needed: `docs/project-docs/operation-test-coverage-matrix.md`

- [x] **Step 1: Dispatch a fresh subagent for Task 3**

Ask the subagent to update concise current-state docs and navigation docs only. The subagent should prefer linking or pointing to reference docs over duplicating long ownership explanations.

- [x] **Step 2: Align summary docs**

Update concise summaries so they agree with Task 2 on `KEY_BYTES`, `ENTRY_RECORD`, `STRING_BYTES`, collection root records, `LIST_QUICKLIST_NODE`, remaining adapter-owned payload boundaries, DB defrag maintenance, and scan/snapshot copied output.

- [x] **Step 3: Align navigation and operational docs**

Update navigation/testing/configuration docs only where stale wording appears. Make links point readers to `native-allocator-and-handles.md`, `db-internals.md`, or `ffm-usage.md` for current allocator ownership details.

- [x] **Step 4: Verify Task 3 docs**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|collection roots|collection root|root-local|adapter-owned|payload adapter|LIST_QUICKLIST_NODE|defrag|epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs
git diff --check
git status --short
```

Expected: remaining `docs/project-docs` matches are current-state accurate, explicitly transitional, or links into the reference docs; `git diff --check` prints no errors; `git status --short` lists only intended project-doc changes for this task.

- [x] **Step 5: Main agent review and commit**

The main agent reviews for duplicated drift-prone explanations, reruns verification, and commits this task before starting Task 4.

## Task 4: Benchmark And Report Wording Convergence

**Files:**
- Modify: `docs/superpowers/reports/2026-05-16-db-native-allocator-benchmark-results.md`
- Modify as needed: `docs/project-docs/client-and-bench-internals.md`
- Modify as needed: `docs/project-docs/testing-and-debugging.md`

- [ ] **Step 1: Dispatch a fresh subagent for Task 4**

Ask the subagent to update only benchmark/report wording. The subagent must not rerun benchmarks and must not edit benchmark code or Maven configuration.

- [ ] **Step 2: Preserve Track 1 comparison caveat**

Ensure benchmark docs separate current branch RESP benchmark output, allocator micro/eval output, DB native defrag comparison output, and baseline/current comparison status. Keep the caveat that baseline/current deltas are non-comparable unless both baseline and current complete the same workload shape.

- [ ] **Step 3: Remove overconfident before/after wording**

Update any wording that calls numbers a trustworthy before/after comparison without naming clean baseline and current runs. Keep useful current metrics and explain failures such as `ERR internal error` as comparability blockers when present.

- [ ] **Step 4: Verify Task 4 docs**

Run:

```bash
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
git diff --check
git status --short
```

Expected: benchmark matches preserve the Track 1 caveat; no uncaveated before/after performance claim remains in current-state docs or reports; `git diff --check` prints no errors; `git status --short` lists only intended benchmark/report wording changes.

- [ ] **Step 5: Main agent review and commit**

The main agent reviews for overstated benchmark confidence, reruns verification, and commits this task before starting Task 5.

## Task 5: Stale Phrase Closure

**Files:**
- Modify as needed: any file already listed in this plan except `docs/superpowers/specs/2026-05-16-db-native-allocator-follow-up-roadmap-design.md` and `yierdis.md`

- [ ] **Step 1: Dispatch a fresh subagent for Task 5**

Ask the subagent to rerun all stale phrase audits, classify every remaining match, and make only small closure edits where a current-state contradiction remains.

- [ ] **Step 2: Run all stale phrase audits**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs docs/superpowers
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs docs/superpowers
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs docs/superpowers
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs docs/superpowers
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
```

Expected: remaining matches are either accurate current reference text, explicitly historical/spec text, or explicitly labeled future work boundaries.

- [ ] **Step 3: Apply closure edits only for current-state contradictions**

If a remaining current-state contradiction appears, edit the smallest relevant current-state doc to resolve it. Do not rewrite historical specs or plans merely because their old context contains stale phrases.

- [ ] **Step 4: Verify Task 5 docs**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` prints no errors; `git status --short` lists only intended closure edits.

- [ ] **Step 5: Main agent review and commit**

The main agent reviews the final classification, confirms any remaining stale-looking phrases are historical or future-context only, reruns verification, and commits this task before starting Task 6.

## Task 6: Final Verification

**Files:**
- Modify: no files expected

- [ ] **Step 1: Dispatch a fresh subagent for Task 6**

Ask the subagent to run final docs-only verification and report results. The subagent must not edit files.

- [ ] **Step 2: Run full stale phrase audit**

Run:

```bash
rg -n "key bytes.*blob|blob-store-owned|still.*key bytes|migrate key bytes|KEY_BYTES|NativeKeyDirectory" docs/project-docs docs/superpowers
rg -n "collection roots|collection root|root-local|adapter-owned|payload adapter|NativeCollectionRootTable|LIST_NODE|LIST_QUICKLIST_NODE|HASH_NODE|SET_NODE|ZSET_NODE" docs/project-docs docs/superpowers
rg -n "defrag|active defrag|defragMaintenance|legacy.*payload|blob-store|listpack" docs/project-docs docs/superpowers
rg -n "epoch|SNAPSHOT|SCAN|snapshot|scan.*copy|physical address|NativeObjectView" docs/project-docs docs/superpowers
rg -n "benchmark|baseline|current|non-comparable|before/after|ERR internal error" docs/project-docs docs/superpowers/reports docs/superpowers/specs
```

Expected: no remaining current-state contradictions. Remaining matches are classified as accurate current reference text, historical/spec text with clear context, or explicitly labeled future work boundaries.

- [ ] **Step 3: Run final whitespace and status checks**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` prints no errors. `git status --short` is clean after the final task commit.

- [ ] **Step 4: Main agent final review**

The main agent confirms:

- no Maven or code/test verification was needed because the plan is docs-only
- the parent roadmap spec was not edited
- `yierdis.md` was not edited
- stale phrase audit classifications match the rules in this plan
- all task commits are present and reviewed

- [ ] **Step 5: Main agent final plan commit**

The main agent marks Task 6 complete in this plan and commits the plan-only final verification update.
