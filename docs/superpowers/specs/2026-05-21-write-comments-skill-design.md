# Write Comments Skill Design

## Status

Design approved in conversation on 2026-05-21.

## Goal

Create a project-local Codex skill that writes, updates, and reviews code comments with a strict correctness bar. The skill must improve maintainability at the places where code alone does not expose the full behavior, while avoiding low-value comments that merely restate the implementation.

The skill will live at `.agents/skills/write-comments/SKILL.md`.

## Problem

Comment-writing tasks are easy to do badly:

- Local comments can be technically correct for a few lines but misleading in the full execution path.
- Async and concurrent behavior is often invisible from a local code fragment, so incomplete comments can create false assumptions about ordering, completion, cancellation, retries, or thread ownership.
- Existing comments can become stale after code changes and then become worse than no comment.
- Broad "add comments" requests can produce noisy comments that restate obvious code and make important comments harder to notice.

The skill must make Codex behave like a strict reviewer: verify first, comment only where useful, correct stale comments, and avoid unsupported claims.

## Goals

- Require global context before writing comments that describe behavior, ordering, state, ownership, protocols, async execution, concurrency, caching, transactions, memory, persistence, security, compatibility, or external contracts.
- Ensure every added or edited comment is correct against the current code and tests.
- Require review of existing nearby comments so stale or misleading comments are fixed or removed during related edits.
- Explain all non-obvious behavior that maintainers cannot reliably infer from the local code alone.
- Treat async and concurrent behavior as high-risk: comments must clearly state triggering, timing, ordering, callback or future dispatch, failure, cancellation, retry, timeout, and what callers must not assume when those facts matter.
- Prefer no comment over a low-quality comment.
- Follow the requested comment language. If no language is requested, follow the current file's existing comment language. If the file has no clear comment language, ask the user before writing comments.

## Non-Goals

- Do not create comments everywhere.
- Do not use comments to compensate for unclear names or tangled structure when a scoped code improvement is more appropriate.
- Do not invent design intent that cannot be verified from code, tests, documentation, or the user's explicit context.
- Do not enforce a single global language across the repository.
- Do not introduce a script-based comment generator. Comment quality depends on semantic review.

## Triggering

The skill should trigger when the user asks to:

- write, add, improve, update, review, audit, or remove comments;
- add or revise Javadocs;
- document async, concurrent, protocol, state-machine, memory, cache, transaction, command, or compatibility behavior in code;
- check whether comments are stale or misleading after code changes.

The skill should also trigger when comment changes are a natural part of a requested code change and the user explicitly wants comment quality handled.

## Required Workflow

1. Identify the code region and the kind of comment being requested.
2. Read the local implementation.
3. Read enough global context to verify behavior:
   - callers and callees;
   - interfaces and public contracts;
   - tests and documented behavior;
   - async entry points, callbacks, executors, schedulers, futures, event loops, queues, or background tasks;
   - state ownership and mutation paths;
   - protocol, serialization, memory, cache, transaction, and compatibility boundaries when relevant.
4. Inspect existing comments in the target region and adjacent related code.
5. Decide whether each possible comment is necessary:
   - add or keep comments that explain non-obvious facts;
   - update comments that are stale, incomplete, or misleading;
   - remove comments that are wrong, redundant, or noise.
6. Write concise comments in the correct language and local style.
7. Self-check every added, changed, or preserved relevant comment against current code behavior.

If the required facts cannot be verified after reasonable code exploration, Codex must not guess. It should either ask the user or write a deliberately limited comment that states only verified facts.

## Global Context Rules

Comments must not be based only on the nearest few lines when they describe behavior beyond those lines. A comment is global-context-sensitive if it mentions any of the following:

- execution order;
- lifecycle or ownership;
- async or concurrent behavior;
- visibility, publication, or synchronization;
- retries, cancellation, or timeout;
- protocol or wire shape;
- persistence or memory ownership;
- cache authority or invalidation;
- transaction boundaries;
- error mapping;
- compatibility with Redis, clients, or previous behavior;
- invariants that other code relies on.

For these comments, the skill must verify the statement through related code, tests, or docs before writing it.

## Async And Concurrency Rules

Async and concurrent comments require extra precision. When relevant, explain:

- who starts the async work;
- when the work runs relative to the caller;
- which executor, event loop, scheduler, queue, or thread owns execution;
- whether ordering is guaranteed;
- whether multiple instances can run concurrently;
- when completion becomes observable;
- when callbacks run;
- how failures, cancellation, retries, and timeouts propagate;
- which assumptions callers or maintainers must not make.

Do not imply synchronous completion, single-threaded execution, ordering, or callback timing unless the code enforces it.

## Language Rules

- If the user requests a language, use that language.
- If the user does not request a language, follow the current file's existing comment language.
- If the file has no clear existing comment language, ask the user before writing comments.
- If the file mixes languages, follow the nearest related comment style or the same kind of API comment. If that is still ambiguous, ask the user.
- Preserve project terminology exactly where terms are established by code, public docs, or tests.

## Comment Quality Rules

Good comments explain facts that code does not make obvious:

- why the implementation must use this approach;
- what invariant must hold;
- what boundary or compatibility behavior is being preserved;
- what hidden dependency exists between distant code paths;
- what future changes would break correctness;
- what async ordering or visibility rule matters.

Bad comments are forbidden:

- comments that restate the next line of code;
- comments that describe obvious getters, setters, loops, assignments, or null checks;
- generic comments such as "process data", "handle logic", or "execute operation";
- speculative intent;
- broad claims that are not verified;
- outdated comments left in place because they are nearby;
- comments that promise behavior not covered by implementation.

When code is clear without a comment, leave it uncommented. When code is unclear because of naming or structure, prefer a scoped code improvement or explicitly report that the code shape is the real problem.

## Java Comment Style

For Java code:

- Use Javadocs for public APIs, extension points, SPIs, configuration objects, contracts, and behavior that callers depend on.
- Use implementation comments only for non-obvious local constraints or global relationships.
- Keep inline comments rare and close to the statement they constrain.
- Avoid excessive `@param` and `@return` text that repeats names or types.
- Use `TODO` or `FIXME` only when the user asks or the surrounding project style already uses them for tracked work.

## Existing Comment Review

When editing or adding comments in a code area, the skill must inspect existing nearby comments and comments on directly related symbols. It must:

- update comments that no longer match the code;
- delete comments that are wrong or redundant;
- flag comments that appear suspicious but cannot be verified within the current scope;
- avoid preserving stale comments simply because they predate the current task.

## Examples

Low-quality comment:

```java
// Check if the future is done.
if (future.isDone()) {
```

High-quality async comment:

```java
// Completion can become visible before callbacks run because callbacks are
// dispatched on the executor after the future state is published.
```

Low-quality Chinese comment:

```java
// 获取用户。
User user = getUser();
```

High-quality Chinese comment:

```java
// 下游按提交顺序合并异步结果，因此这里不能改成并行无序收集；否则响应项会和请求项错位。
```

## Acceptance Criteria

- `.agents/skills/write-comments/SKILL.md` exists and uses a clear trigger description.
- The skill explicitly prioritizes correctness, global context, async clarity, stale-comment review, and low-noise output.
- The skill requires asking the user about language when no language is requested and the target file has no clear existing comment language.
- The skill forbids unsupported guesses and low-quality comments.
- The skill includes concise good and bad examples.
- The skill does not add scripts or unrelated resources.

## Verification

Minimum verification after implementation:

- Read the generated `SKILL.md` and check it against every acceptance criterion.
- Run `git diff --check`.
- Confirm no unrelated working-tree changes were modified.
