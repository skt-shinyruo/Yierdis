# Write Comments Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the project-local `write-comments` Codex skill for high-quality, globally verified, low-noise code comments.

**Architecture:** Add one skill folder under `.agents/skills/write-comments` with a required `SKILL.md` and UI metadata in `agents/openai.yaml`. Keep all operational rules in `SKILL.md`; do not add scripts, references, or assets because comment quality depends on semantic code review rather than deterministic generation.

**Tech Stack:** Codex skill Markdown, YAML frontmatter, repository-local `.agents/skills` convention.

---

## File Structure

- Create: `.agents/skills/write-comments/SKILL.md`
  - Contains the trigger frontmatter and the complete comment-writing workflow.
  - Encodes correctness, global context, async/concurrency clarity, language selection, existing-comment review, and examples.
- Create: `.agents/skills/write-comments/agents/openai.yaml`
  - Provides UI metadata for the skill list and default prompt.
- Read: `docs/superpowers/specs/2026-05-21-write-comments-skill-design.md`
  - Source of truth for requirements and acceptance criteria.
- Read: `.agents/skills/use-jdk25/SKILL.md`
  - Existing project-local skill structure example.

No tests are required because this change adds Codex skill documentation, not executable project code.

### Task 1: Create The Skill

**Files:**
- Create: `.agents/skills/write-comments/SKILL.md`
- Create: `.agents/skills/write-comments/agents/openai.yaml`
- Read: `docs/superpowers/specs/2026-05-21-write-comments-skill-design.md`
- Read: `.agents/skills/use-jdk25/SKILL.md`

- [ ] **Step 1: Review source requirements**

Run:

```bash
sed -n '1,260p' docs/superpowers/specs/2026-05-21-write-comments-skill-design.md
sed -n '1,120p' .agents/skills/use-jdk25/SKILL.md
```

Expected: the spec lists global-context, async, language, stale-comment, quality, and verification requirements; the existing skill shows project-local skill layout.

- [ ] **Step 2: Initialize the skill folder**

Run:

```bash
/home/feng/.codex/skills/.system/skill-creator/scripts/init_skill.py write-comments --path .agents/skills --interface display_name="Write Comments" --interface short_description="Write correct, low-noise code comments" --interface default_prompt="Use $write-comments to add or review comments in this code."
```

Expected: `.agents/skills/write-comments/SKILL.md` and `.agents/skills/write-comments/agents/openai.yaml` exist.

- [ ] **Step 3: Replace generated `SKILL.md` with final content**

Write `.agents/skills/write-comments/SKILL.md` as:

````markdown
---
name: write-comments
description: Write, update, review, audit, or remove code comments with strict correctness, global context, and low-noise standards. Use when Codex is asked to add or revise comments or Javadocs, document async/concurrent/protocol/state/memory/cache/transaction/compatibility behavior, or check whether existing comments are stale or misleading after code changes.
---

# Write Comments

## Non-Negotiable Rules

- Verify before commenting. Every added, changed, or preserved relevant comment must match current code behavior.
- Use global context for behavioral comments. Do not infer ordering, ownership, lifecycle, async behavior, compatibility, memory semantics, or invariants from a local snippet alone.
- Prefer no comment over a low-quality comment. Do not add comments that merely restate obvious code.
- Review existing nearby and directly related comments. Update or remove stale, redundant, incomplete, or misleading comments during related edits.
- Do not invent intent. If a fact cannot be verified from code, tests, docs, or explicit user context, keep investigating, ask the user, or write only the verified narrower fact.

## Required Workflow

1. Identify the target code region and whether the request is for Javadocs, implementation comments, review, cleanup, or stale-comment correction.
2. Read the local implementation.
3. Read enough global context to verify behavior:
   - callers and callees;
   - interfaces and public contracts;
   - tests and documented behavior;
   - async entry points, callbacks, executors, schedulers, futures, event loops, queues, and background tasks;
   - state ownership and mutation paths;
   - protocol, serialization, memory, cache, transaction, and compatibility boundaries when relevant.
4. Inspect existing comments in the target region and on directly related symbols.
5. Decide comment-by-comment:
   - add or keep comments that explain non-obvious facts;
   - update comments that are stale, incomplete, or misleading;
   - remove comments that are wrong, redundant, or noise.
6. Write concise comments in the correct language and local style.
7. Self-check every added, changed, or preserved relevant comment against current code behavior.

## What Deserves A Comment

Write comments for facts that maintainers cannot reliably infer from the local code alone:

- why the implementation must use this approach;
- what invariant must hold;
- what hidden relationship exists between distant code paths;
- what boundary or compatibility behavior is being preserved;
- what future changes would break correctness;
- what async ordering, visibility, callback, cancellation, retry, or timeout rule matters;
- what protocol, wire shape, memory ownership, cache authority, transaction, or state-machine constraint applies.

Leave clear code uncommented. If the code is hard to understand because of naming or structure, prefer a scoped code improvement or report that the code shape is the real problem.

## Global Context Rules

Treat a comment as global-context-sensitive if it mentions:

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

For these comments, verify the statement through related implementation, tests, or docs before writing it.

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

Do not imply synchronous completion, single-threaded execution, ordering, visibility, or callback timing unless the code enforces it.

## Language Rules

- If the user requests a language, use that language.
- If the user does not request a language, follow the current file's existing comment language.
- If the file has no clear existing comment language, ask the user before writing comments.
- If the file mixes languages, follow the nearest related comment style or the same kind of API comment. If that is still ambiguous, ask the user.
- Preserve project terms exactly when they are established by code, public docs, or tests.

## Java Comment Style

- Use Javadocs for public APIs, extension points, SPIs, configuration objects, contracts, and behavior that callers depend on.
- Use implementation comments only for non-obvious local constraints or global relationships.
- Keep inline comments rare and close to the statement they constrain.
- Avoid `@param` and `@return` text that repeats names, types, or obvious return values.
- Use `TODO` or `FIXME` only when the user asks or the surrounding project style already uses them for tracked work.

## Forbidden Comments

Do not write or preserve:

- comments that restate the next line of code;
- comments that describe obvious getters, setters, loops, assignments, or null checks;
- generic comments such as "process data", "handle logic", or "execute operation";
- speculative design intent;
- broad claims that are not verified;
- comments that promise behavior not covered by implementation;
- stale comments left in place because they predate the current task.

## Examples

Low quality:

```java
// Check if the future is done.
if (future.isDone()) {
```

High quality:

```java
// Completion can become visible before callbacks run because callbacks are
// dispatched on the executor after the future state is published.
```

Low quality:

```java
// 获取用户。
User user = getUser();
```

High quality:

```java
// 下游按提交顺序合并异步结果，因此这里不能改成并行无序收集；否则响应项会和请求项错位。
```

## Final Check

Before finishing a comment task, verify:

- each comment explains something non-obvious and useful;
- each behavioral claim is correct in the full code path;
- async or concurrent claims do not overpromise ordering, timing, or thread ownership;
- stale nearby comments were updated or removed;
- language and terminology match the user request or current file;
- low-quality comments were not added.
````

Expected: `SKILL.md` contains only the final skill instructions and valid YAML frontmatter.

- [ ] **Step 4: Verify generated UI metadata**

Read `.agents/skills/write-comments/agents/openai.yaml`.

Expected content:

```yaml
interface:
  display_name: "Write Comments"
  short_description: "Write correct, low-noise code comments"
  default_prompt: "Use $write-comments to add or review comments in this code."
```

If the generated file differs only by quote style or trailing newline, keep it if it is valid YAML and semantically equivalent. If fields are missing or incorrect, update it to the expected content.

- [ ] **Step 5: Validate the skill**

Run:

```bash
/home/feng/.codex/skills/.system/skill-creator/scripts/quick_validate.py .agents/skills/write-comments
```

Expected: validation passes.

- [ ] **Step 6: Check acceptance criteria**

Run:

```bash
sed -n '1,260p' .agents/skills/write-comments/SKILL.md
sed -n '1,80p' .agents/skills/write-comments/agents/openai.yaml
```

Confirm manually:

- `SKILL.md` has clear trigger frontmatter.
- Correctness, global context, async clarity, stale-comment review, and low-noise output are explicit.
- Language fallback requires asking the user when the target file has no clear comment language.
- Unsupported guesses and low-quality comments are forbidden.
- Good and bad examples are present.
- No `scripts`, `references`, or `assets` directories were added.

- [ ] **Step 7: Run whitespace verification**

Run:

```bash
git diff --check -- .agents/skills/write-comments docs/superpowers/plans/2026-05-21-write-comments-skill.md
```

Expected: no output and exit code 0.

- [ ] **Step 8: Check working tree scope**

Run:

```bash
git status --short
```

Expected: new files under `.agents/skills/write-comments/` and this plan file may appear. Existing unrelated Java modifications may still be present and must not be reverted or staged.

- [ ] **Step 9: Commit skill implementation only**

Run:

```bash
git add .agents/skills/write-comments docs/superpowers/plans/2026-05-21-write-comments-skill.md
git commit -m "Add write-comments skill"
```

Expected: commit includes only the skill files and this implementation plan.

## Self-Review

- Spec coverage: Task 1 implements every acceptance criterion from `docs/superpowers/specs/2026-05-21-write-comments-skill-design.md`.
- Placeholder scan: no `TBD`, incomplete TODOs, or unspecified implementation steps remain.
- Scope control: no Java, Maven, tests, generated binaries, scripts, references, or assets are edited.
