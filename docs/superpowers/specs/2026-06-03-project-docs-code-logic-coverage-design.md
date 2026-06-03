# Project Docs Code Logic Coverage Design

## Status

Design approved in conversation on 2026-06-03.

This document defines how Yierdis project documentation should expand from the current layered manuals into a coverage-driven system that records and closes missing code-logic explanations. It scopes documentation structure, coverage tracking, document-splitting rules, execution order, and completion criteria for the next planning and implementation phase.

## Problem

`docs/project-docs` already has a usable layered structure, but it is still driven mainly by topic organization, not by an explicit inventory of code logic. That creates three practical gaps:

- Some important runtime behavior is described only partially, especially when it spans multiple modules or crosses protocol, executor, command, DB, runtime, and native-memory boundaries.
- There is no auditable source of truth for "which core code logic is already documented" versus "which logic still exists only in code and tests."
- The current docs can guide a reader into the system, but they cannot yet guarantee that a maintainer will find every important behavior contract, state transition, rollback path, or verification entry when changing the code.

The next documentation effort should therefore stop treating the existing docs tree as the boundary of the work. Instead, the work should start from the codebase, build a coverage model of the real logic, and then decide whether each missing explanation belongs in an existing document or a new one.

## Goals

- Document as much important code logic as reasonably possible, based on the current codebase and tests rather than current document boundaries.
- Add a maintainable coverage artifact that tracks which core logic is already explained and which logic is still missing from documentation.
- Keep existing project-doc entry and navigation layers useful while allowing focused new documents when current topic pages cannot carry the missing detail cleanly.
- Write documentation for maintainers and contributors who need to understand, modify, verify, and debug Yierdis behavior.
- Record logic at the granularity of `class + key method or logic block`, not only module names and not every trivial helper.
- Explicitly cover the logic that changes maintenance risk: request paths, state machines, thread boundaries, memory lifetime, rollback order, configuration and protocol branches, invariants, and test entry points.
- Treat current code and tests as the factual source of truth. When code, tests, and old docs diverge, describe current implementation reality and mark uncertainty clearly.

## Non-Goals

- Do not rewrite the entire documentation set from scratch unless a specific topic boundary demands it.
- Do not preserve the existing project-doc topology as a hard constraint.
- Do not create one document per class.
- Do not document trivial getters, setters, DTO fields, or obvious local helpers that add no behavioral understanding.
- Do not change Java, Maven, shell, or test behavior as part of this documentation design.
- Do not describe intended future architecture as if it were already implemented.

## Audience

The target reader is a developer who is preparing to maintain or modify Yierdis. The docs should help that reader answer:

- what this class or method is responsible for;
- what state or lifecycle it participates in;
- what can go wrong if it is changed carelessly;
- which tests or verification paths should be checked after a change;
- which adjacent docs explain the neighboring behavior.

This is not a beginner user manual and not a Redis compatibility guide.

## Design Overview

The documentation system should operate in three layers:

1. **Coverage Matrix Layer**

   A new maintenance document records the inventory of code logic and its documentation status. This becomes the control plane for the work: missing logic is discovered here first, then routed into topic docs.

2. **Topic Coverage Layer**

   Existing project-doc manuals remain the main place where behavior is explained. Missing logic is added to existing topic docs when the topic boundary is still coherent. New topic docs are created only when the logic forms its own stable behavioral unit.

3. **Navigation And Index Layer**

   Existing navigation docs such as `readme.md`, `core-logic-index.md`, and `development-navigation.md` continue to connect readers to the deeper material. New topic docs must never become orphaned files.

This design keeps the current layered docs useful, but removes the assumption that "if it does not fit neatly into an existing page, it can be skipped."

## Documentation Topology

### Coverage Matrix Layer

Create a new document at:

- `docs/project-docs/code-logic-coverage.md`

This file is not a tutorial. It is a maintainer-facing inventory of code logic coverage. It should be written for scanning, updating, and auditability rather than narrative reading.

### Topic Coverage Layer

Use the existing project-doc set as the default sink for missing explanations, especially these major areas:

- `request-execution-flow.md`
- `main-path-walkthrough.md`
- `protocol-reference.md`
- `commands-and-data-model.md`
- `executor-and-backpressure.md`
- `db-internals.md`
- `native-memory-runtime.md`
- `native-allocator-and-handles.md`
- `offheap-copy-behavior.md`
- `change-event-and-proxy-logic.md`
- `configuration-and-operations.md`
- `client-and-bench-internals.md`

These files remain the preferred destination when missing logic naturally belongs to their topic boundaries.

### Navigation And Index Layer

At minimum, these files must be updated when new topic material appears:

- `docs/project-docs/readme.md` when a new document changes the reading map
- `docs/project-docs/core-logic-index.md` when new logic needs source-entry visibility
- `docs/project-docs/development-navigation.md` when the new explanation changes "what should I open first?"

The navigation layer is a hard requirement. A new topic document without an index path is incomplete work.

## Coverage Matrix Model

`code-logic-coverage.md` should not be a vague list of modules. Each record should be actionable and should support both gap-finding and maintenance work.

Each entry should include:

- subsystem or module area
- class
- key method or logic block
- behavior responsibility
- key branches, states, or invariants
- thread or memory boundary
- relevant tests
- documentation destination
- coverage status: `covered`, `partial`, or `missing`
- notes for implementation-truth mismatches or unresolved semantics

This granularity is intentional:

- module-only rows are too coarse to guide documentation work;
- per-helper rows are too fine and create noise;
- `class + key method or logic block` is usually the correct maintenance unit.

## Initial Coverage Grouping

The first version of the coverage matrix should group records under these subsystem headings:

- `server-main / bootstrap / connection`
- `networking-resp / networking-netty`
- `executor`
- `engine / session / transaction`
- `command-api / command-core / builtin commands`
- `runtime / multi-db / maxmemory governor`
- `db-api / db-memory`
- `bytes / memory / ffm`
- `cli / benchmark / smoke`
- `tests / architecture guards / contract tests`

These groups match the way maintainers usually reason about behavior boundaries and should make missing logic easier to locate.

## What Must Be Documented

Documentation work should prioritize logic that affects behavior understanding, change risk, or verification strategy. This includes:

- request execution main paths
- transaction and replay behavior
- command parsing, registration, and dispatch
- state machines and lifecycle transitions
- thread ownership and scheduling boundaries
- memory lifetime and handle validation rules
- reserve/apply/commit/rollback paths
- TTL and expiration behavior
- maxmemory and eviction logic
- change-event production and bridging
- protocol and configuration branches
- close paths, error paths, and recovery behavior
- test entry points and verification expectations

Logic that is trivial, mechanically obvious, or behaviorally irrelevant should remain undocumented unless it helps explain one of the categories above.

## Rules For Creating New Documents

The default policy is to extend existing topic manuals. New project-doc files should be created only when at least one of these conditions is true:

1. **Cross-topic behavior**

   The logic spans multiple established topic boundaries and cannot be explained cleanly inside one existing manual.

2. **Independent state machine or lifecycle**

   The logic has its own meaningful state transitions, phases, or lifecycle rules that deserve a focused explanation.

3. **Independent invariant set**

   The logic has a coherent group of correctness rules that a maintainer must keep together, such as dual-write consistency, handle validation, or owner-thread restrictions.

4. **Readability breakdown in an existing doc**

   Adding the missing logic to an existing page would make that page lose its topic coherence or turn it into an unstructured dump of details.

Potential examples include:

- `transaction-and-replay.md`
- `ttl-and-expiration-lifecycle.md`
- `maxmemory-and-eviction.md`
- `command-parsing-and-dispatch.md`
- `change-event-lifecycle.md`

These are candidate topics only. The implementation phase should confirm them against the actual source scan before creating files.

## Execution Strategy

The documentation effort should run as a repeated five-step loop rather than a one-pass rewrite.

### 1. Source Inventory

Scan the codebase by subsystem and record core classes, key methods, logic blocks, major branches, and invariants into `code-logic-coverage.md` before expanding topic prose.

### 2. Gap Classification

For each missing or partial item, decide whether it should:

- extend an existing topic doc;
- create a new topic doc;
- appear only as an index/reference note.

This decision should follow the document-creation rules above rather than ad hoc judgment.

### 3. Topic Documentation Pass

Fill the missing logic into the appropriate docs, prioritizing system-behavior-critical areas first. Each meaningful addition should also update the relevant navigation and index documents.

### 4. Consistency Review

After each batch, compare the new documentation against:

- the current implementation,
- nearby related docs,
- the relevant tests.

This is especially important for transaction, TTL, maxmemory, change-event, and native-memory material, where cross-document drift is likely.

### 5. Coverage Re-Check

Return to `code-logic-coverage.md` and update each affected item to `covered` or `partial`, or leave it open with a concrete note explaining the remaining gap.

## Scan Order

The implementation should scan and document in this order:

1. `networking -> executor -> engine/session/transaction -> command -> db -> runtime/maxmemory -> memory/ffm`
2. `cli/benchmark/smoke -> tests/architecture guards`

This ordering prioritizes the behavior path that most directly determines system semantics and maintenance risk before moving to tooling and verification support material.

## Verification Expectations

Each important logic explanation should include the relevant test entry or verification surface. The documentation should help a maintainer answer not only "how does this work?" but also "how do I verify a change here?"

At a minimum, important logic additions should point to:

- relevant contract tests
- module or subsystem unit tests
- integration tests
- architecture guard tests when they enforce the boundary being described
- smoke or benchmark entry points when they meaningfully exercise the logic

## Acceptance Criteria

The design is considered successfully implemented only when all of the following are true:

- `docs/project-docs/code-logic-coverage.md` exists and is usable as a maintainer-facing inventory.
- Core subsystem coverage is tracked at the `class + key method or logic block` level.
- Readers can follow the main execution path without major logic blind spots.
- High-risk logic has explicit behavioral documentation, including at least:
  - transaction and replay
  - TTL and expiration
  - maxmemory and eviction
  - command parsing and dispatch
  - change-event behavior
  - native handle and allocator lifecycle
  - executor backpressure and close paths
- Each important logic area includes relevant tests or verification entry points.
- New topic docs, if any, are linked from the navigation or index layer and are not orphaned.
- Documentation reflects current implementation and current tests. Known mismatches are marked as implementation reality or pending clarification rather than silently normalized away.

## Risks

- The source inventory can grow large and become noisy if the coverage unit is too fine-grained. The implementation must hold the line on `class + key method or logic block`.
- Some existing topic docs may look complete at a glance but still hide major logic gaps. The implementation must trust source scanning over document appearance.
- Cross-topic logic such as transaction, change events, and maxmemory can easily drift across multiple docs. The consistency pass is essential.
- Over-eager new-document creation can fragment the reading path. New docs must clear the explicit creation rules.

## Blockers

No blockers.
