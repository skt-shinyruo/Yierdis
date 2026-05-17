# Project Docs Rewrite Design

## Status

Design approved in conversation on 2026-05-17.

This document scopes a full rewrite of `docs/project-docs` into a layered documentation system. It also permits synchronized updates to external entry points such as the repository `README.md` when links, document names, or reading paths change.

## Problem

`docs/project-docs` has grown into a large set of useful but uneven Markdown files. The current content contains strong technical material, but it is hard to use as a coherent documentation system:

- The first-read path repeats across `README.md`, `docs/project-docs/readme.md`, `project-introduction.md`, and `project-overview.md`.
- Some docs try to serve both tutorial and reference roles, which makes them long and difficult to scan.
- `core-logic-index.md` is a long explanation instead of a compact index.
- `ffm-usage.md` mixes JDK FFM teaching material, Yierdis memory architecture, allocator details, and operational caveats in one oversized document.
- Cross-links point readers through a mostly linear order, even though readers have different tasks: learning the project, tracing requests, changing commands, debugging tests, or studying native memory.
- The current directory lacks an explicit information architecture that separates "read this first", "understand the system", "use as a topic manual", "change code", and "look up details".

The rewrite should keep the useful current knowledge but reshape it into a comprehensive, navigable, maintainable set of docs.

## Goals

- Rewrite all Markdown files under `docs/project-docs`.
- Keep the docs comprehensive: support both first-time readers and developers using the docs as a source-code index.
- Use a layered structure:
  - entry guide
  - system main path
  - topic manuals
  - development navigation
  - reference material
- Reduce repeated project-positioning prose across root `README.md` and project docs.
- Split or reshape oversized docs when that improves readability and maintenance.
- Keep common entry filenames where useful, but allow renames, additions, deletions, and directory restructuring.
- Update external references such as `README.md` when document paths change.
- Preserve important current-state details about RESP, command dispatch, DB internals, executor/backpressure, bytes abstractions, FFM/native memory, allocator handles, tests, CLI, and benchmark tooling.
- Make each document answer:
  - who should read it
  - what it covers
  - what it deliberately does not cover
  - what to read next

## Non-Goals

- Do not change Java, Maven, shell, benchmark, or test implementation behavior.
- Do not edit generated binary or image assets unless a later implementation plan explicitly scopes them.
- Do not preserve every old paragraph. The goal is a rewrite, not a conservative copy edit.
- Do not make root `README.md` a full internal manual. It remains the project entry and quick-start page.
- Do not treat historical `docs/superpowers` specs and plans as current project reference docs.
- Do not touch a `yierdis.md` file unless it exists in the working tree during implementation; no such file was found during design exploration.

## Current Inputs

The current project docs include these main files:

- `readme.md`
- `project-introduction.md`
- `project-overview.md`
- `request-execution-flow.md`
- `main-path-walkthrough.md`
- `module-architecture.md`
- `core-logic-index.md`
- `protocol-reference.md`
- `commands-and-data-model.md`
- `db-internals.md`
- `executor-and-backpressure.md`
- `bytes-and-fast-paths.md`
- `configuration-and-operations.md`
- `client-and-bench-internals.md`
- `testing-and-debugging.md`
- `development-navigation.md`
- `operation-test-coverage-matrix.md`
- `glossary.md`
- `ffm-usage.md`
- `native-allocator-and-handles.md`
- `offheap-copy-behavior.md`

The repository currently has uncommitted edits in several documentation files and one integration test file. The later implementation must work with those edits and must not revert unrelated user changes.

## Target Information Architecture

### 1. Entry Guide

Purpose: help readers choose a path without reading every document.

Recommended files:

- `readme.md`
- `project-introduction.md`
- `project-overview.md`

Responsibilities:

- `readme.md` becomes the documentation map for `docs/project-docs`.
- `project-introduction.md` explains why the project exists, what mindset to use, and why it is not a Redis replacement.
- `project-overview.md` explains the current capability boundary, module map, runtime features, and first source files to open.

The root `README.md` should link into this layer instead of duplicating the full internal reading map.

### 2. System Main Path

Purpose: give readers a coherent mental model of the running system.

Recommended files:

- `request-execution-flow.md`
- `main-path-walkthrough.md`
- `module-architecture.md`

Responsibilities:

- `request-execution-flow.md` explains the runtime request path from Netty bytes through RESP decoding, `ExecutionRequest`, executor, engine, command layer, DB, `ReplyWriter`, RESP reply encoding, and flush.
- `main-path-walkthrough.md` follows source files and methods in a stable order, using representative paths such as `PING` and `SET`.
- `module-architecture.md` explains Maven modules, package ownership, dependency direction, and architecture tests.

### 3. Topic Manuals

Purpose: provide focused technical explanations that can be read independently after the main path.

Recommended files:

- `protocol-reference.md`
- `commands-and-data-model.md`
- `db-internals.md`
- `executor-and-backpressure.md`
- `bytes-and-fast-paths.md`
- `configuration-and-operations.md`
- `client-and-bench-internals.md`

Native-memory material should be split into a clearer set of topic manuals. The final file names may be chosen during planning, but the responsibilities should be separated:

- FFM basics for readers who need a minimum JDK FFM primer.
- Yierdis FFM usage and runtime ownership.
- Stable native allocator, handles, object table, pin/epoch/quarantine, and active defrag.
- Off-heap copy behavior and where heap materialization still exists.

### 4. Development Navigation

Purpose: help maintainers decide where to edit and what to verify.

Recommended files:

- `development-navigation.md`
- `testing-and-debugging.md`
- `operation-test-coverage-matrix.md`

Responsibilities:

- `development-navigation.md` is organized by task: add a command, change a command family, change protocol behavior, change TTL/maxmemory/native memory, change executor/backpressure, update observability.
- `testing-and-debugging.md` is organized by change type and failure symptom, with concrete test suites and troubleshooting routes.
- `operation-test-coverage-matrix.md` remains a maintainable command and option coverage matrix, but the format should be compact and easy to update.

### 5. Reference Material

Purpose: provide lookup pages that do not compete with the tutorial path.

Recommended files:

- `core-logic-index.md`
- `glossary.md`

Responsibilities:

- `core-logic-index.md` becomes a compact index of core classes, methods, modules, and responsibilities. It should link to topic manuals instead of repeating full explanations.
- `glossary.md` defines high-frequency terms and links back to the most relevant manuals.

## Naming And Structure Policy

The implementation may:

- keep existing filenames when they remain clear and useful;
- rename files whose role changes significantly;
- add subdirectories if they improve navigation;
- split oversized documents;
- delete or replace obsolete documents;
- update all affected relative links.

The implementation should prefer stable, descriptive names over numeric ordering. Numeric prefixes are allowed only if a later plan determines that reading order is otherwise unclear.

## Content Style

The rewritten docs should:

- use Chinese prose, matching the current project-docs audience;
- keep English technical identifiers unchanged;
- avoid marketing language and avoid overstating Redis compatibility;
- distinguish current implementation from later roadmap directions;
- use short sections and tables where they improve scanning;
- include code paths and class names when they help developers jump into source;
- avoid duplicating the same system explanation across many files;
- use cross-links to connect summaries to deeper references.

## Migration Strategy

The later implementation plan should decompose the rewrite into ordered slices:

1. Documentation inventory and link audit.

   Record current docs, inbound links from `README.md` and other docs, and any stale or duplicate concepts that must be removed or merged.

2. Target structure and navigation layer.

   Rewrite `docs/project-docs/readme.md`, update root `README.md` links, and establish the final file list.

3. Entry and main-path docs.

   Rewrite the introduction, overview, request flow, source walkthrough, and module architecture docs first because they define the mental model for the rest.

4. Topic manuals.

   Rewrite protocol, command/data model, DB internals, executor/backpressure, bytes, configuration/operations, client/bench, and native-memory manuals.

5. Developer and reference docs.

   Rewrite development navigation, testing/debugging, coverage matrix, core logic index, and glossary after the topic manuals settle.

6. Cross-link and consistency pass.

   Verify every relative link, remove obsolete references, check repeated positioning language, and run Markdown whitespace checks.

## Verification

Minimum verification for the rewrite:

- `git diff --check`
- link/path audit for Markdown links under `docs/project-docs` and root `README.md`
- phrase audit for stale current-state wording around Redis compatibility, unsupported features, RESP path, allocator ownership, and off-heap boundaries
- manual review that `docs/project-docs/readme.md` gives distinct reading paths for:
  - first-time reader
  - request-flow reader
  - command/data-model reader
  - native-memory reader
  - contributor debugging a change

If the implementation adds a link-check script, it should be lightweight and should not become a required build dependency unless explicitly approved later.

## Acceptance Criteria

- Every Markdown file under `docs/project-docs` has been intentionally rewritten, replaced, renamed, split, or removed.
- `docs/project-docs/readme.md` is the authoritative map for the documentation set.
- Root `README.md` links to the new documentation map and no longer duplicates the internal manual.
- The docs clearly state that Yierdis is a Java 25 + Netty + JDK FFM Redis-style single-node in-memory KV server, not a Redis drop-in replacement.
- Unsupported or future capabilities such as AOF/RDB, replication/cluster, Lua, ACL/TLS, PubSub, and full Redis ecosystem compatibility are described as not currently implemented rather than permanently impossible.
- The request path is consistently described with `RespCommandRequest`, `RespExecutionAdapter`, `ByteArrayExecutionRequest`, `ExecutionRequest`, `CommandExecutor`, engine, command processor, DB, `ReplyWriter`, `RespReplyWriter`, and Netty write-back.
- Native-memory docs distinguish FFM runtime ownership, stable allocator handles, allocator-backed records, heap materialization boundaries, and off-heap copy behavior.
- Development docs answer "what file should I open first?" and "what tests should I run?" for common change types.
- `core-logic-index.md` is no longer an oversized explanation page; it is a usable index that points into deeper manuals.
- Markdown links updated by the rewrite resolve to existing files or anchors.

## Risks

- The rewrite is large enough to conflict with existing uncommitted user edits. The implementation must inspect diffs before editing and preserve unrelated work.
- Renames can break links outside `docs/project-docs`. The implementation must audit root `README.md` and Markdown links in the docs tree.
- Splitting `ffm-usage.md` can accidentally lose useful learning material. The implementation should classify content before deleting or moving it.
- Over-compression can make reference docs less useful. The docs should remove duplication, not erase necessary source-level detail.
- Existing historical specs may still mention older architecture. The project docs should point to current truth without rewriting historical specs.

## Blockers

No blockers.
