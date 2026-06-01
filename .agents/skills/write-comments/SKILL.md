---
name: write-comments
description: Use when adding, rewriting, reviewing, auditing, or removing code comments or language-native doc comments, especially Chinese source-learning comments, implementation comments, stale comments, TODO/FIXME, or async/concurrent/protocol/state/cache/transaction/compatibility behavior.
---

# Write Comments

## Non-Negotiable Rules

- Verify before commenting. Every added, changed, or preserved relevant comment must match current code behavior.
- Use global context for behavioral comments. Do not infer ordering, ownership, lifecycle, async behavior, compatibility, memory semantics, or invariants from a local snippet alone.
- Prefer no comment over a low-quality comment. Do not add comments that merely restate obvious code.
- When comments are meant to help readers learn the source, prefer useful implementation comments at important logic points in addition to any language-native doc comments.
- Review existing nearby and directly related comments. Update or remove stale, redundant, incomplete, or misleading comments during related edits.
- Do not invent intent. If a fact cannot be verified from code, tests, docs, or explicit user context, keep investigating, ask the user, or write only the verified narrower fact.
- Use Chinese for added or rewritten comments unless the current request or higher-priority repository instruction explicitly asks for another language.

## Default Comment Policy

- Use Chinese for added or rewritten comments unless the current request or a higher-priority repository instruction explicitly asks for another language.
- Optimize comments for source learning and maintenance understanding: explain why the code is shaped this way, what constraints it protects, and what future maintainers must not accidentally break.
- Do not stop at module, type, class, function, or method comments. Add concise implementation comments inside implementation bodies at the key logic points where a reader would naturally pause.
- Treat public doc comments as public contracts. Include only behavior, constraints, errors, lifecycle rules, and compatibility guarantees that callers may rely on; keep internal state flow, implementation strategy, and private coordination details in implementation comments unless callers must know them.
- Prefer comments over structural refactoring for comment-focused tasks. Only make tiny, low-risk readability edits when they clearly help and are stated in the result.
- Update or translate existing English comments only in the touched target region and on directly related symbols when they are stale, misleading, important to understanding, or would be jarring next to new Chinese comments. Do not perform broad comment translation churn.
- Keep established technical terms, protocol names, command names, class names, method names, configuration keys, and code identifiers in their original form inside Chinese sentences.
- Production code is the priority. Apply the same standard to tests when comments explain regression background, protocol boundaries, compatibility behavior, concurrency timing, or other learning-relevant facts.
- Do not edit generated, third-party, or vendored code only to add Chinese comments. Explain the boundary in project-owned code instead.

## Comment Quality Gate

Before adding, keeping, or rewriting a comment, make it pass this gate:

- The reader cannot reliably infer the fact from the nearby code alone, or the nearby logic is complex enough that a source-learning reader benefits from a concise explanation of the phase, invariant, branch purpose, or data-flow shape.
- The claim is verified through code, tests, docs, or explicit user context.
- The comment explains why, an invariant, a boundary, lifecycle, compatibility, concurrency timing, failure semantics, or another non-obvious constraint instead of restating what the next line does.
- The comment is placed at the narrowest useful scope: caller contracts in public doc comments, internal behavior near the implementation block that depends on it.
- The claim is scoped precisely enough that it cannot be mistaken for a broader guarantee than the code provides.
- The comment is stable enough to maintain. If it is likely to go stale and its value is low, do not write it.
- The comment is not compensating for a small readability problem that a clear name, local variable, or tiny structure improvement would solve better.
- `TODO` and `FIXME` comments include a verified fact and an actionable next step.

## Required Workflow

1. Identify the target code region and whether the request is for language-native doc comments, implementation comments, review, cleanup, or stale-comment correction.
2. If the task will add or rewrite comments and neither the current request nor a higher-priority repository instruction specifies the comment language, use the default Chinese comments. Ask only when the requested language is ambiguous.
3. Read the local implementation.
4. Read enough global context to verify behavior:
   - callers and callees;
   - interfaces and public contracts;
   - tests and documented behavior;
   - async entry points, callbacks, executors, schedulers, futures, event loops, queues, and background tasks;
   - state ownership and mutation paths;
   - protocol, serialization, memory, cache, transaction, and compatibility boundaries when relevant.
5. Inspect existing comments in the target region and on directly related symbols.
6. Decide comment-by-comment and placement-by-placement:
   - add or keep comments that pass the Comment Quality Gate;
   - place implementation comments before meaningful logic blocks, state transitions, boundary checks, ownership/lifetime changes, rollback paths, protocol conversions, or concurrency-sensitive operations when they help source readers understand the flow;
   - update comments that are stale, incomplete, or misleading;
   - remove comments that are wrong, redundant, or noise.
7. Write concise comments in the default, confirmed, or explicitly requested language and local style.
8. Self-check every added, changed, or preserved relevant comment against current code behavior.
9. When language-native doc comments are parsed by the toolchain, or when the project has relevant formatter, doc generator, doctest, lint, or compile checks, run the narrowest useful verification to catch syntax, formatting, or generated-documentation breakage.

## What Deserves A Comment

Write comments for facts that maintainers cannot reliably infer from the local code alone:

- why the implementation must use this approach;
- what invariant must hold;
- what hidden relationship exists between distant code paths;
- what boundary or compatibility behavior is being preserved;
- what future changes would break correctness;
- what async ordering, visibility, callback, cancellation, retry, or timeout rule matters;
- what protocol, wire shape, memory ownership, cache authority, transaction, or state-machine constraint applies.

For source-learning tasks, do not stop at module, type, class, function, or method doc comments. Add concise implementation comments at the logic points a reader would pause on: phase changes, guard clauses that protect an invariant, branches that preserve compatibility, retry/cancel/rollback handling, resource ownership transfer, publication/visibility boundaries, or cleanup/reclamation decisions.

Leave clear code uncommented. If the code is hard to understand because of naming or structure, prefer a scoped code improvement or report that the code shape is the real problem.

When a verified defect, technical debt, or future improvement belongs in source context, add a correct `TODO` or `FIXME` instead of hiding the issue in prose. Use `FIXME` for verified incorrect, risky, or incomplete behavior; use `TODO` for acceptable current behavior with a clear improvement path. Do not require issue numbers, owners, or dates. The comment must include the concrete condition, impact, or next action, not just "optimize later."

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

## Precision And Scope Rules

Comments are often misleading because they are too broad rather than completely false. When writing behavioral comments:

- State the path, mode, protocol version, lifecycle phase, executor/event loop, cache authority, transaction boundary, or failure boundary where the claim is true.
- Avoid absolute words such as `always`, `never`, `guaranteed`, `safe`, or `impossible` unless the code enforces that absolute behavior across the full stated scope.
- Prefer narrow claims such as "this RESP2 error branch" or "within this event-loop task" over broad claims such as "error handling" or "runs serially".
- For concurrency, cache, transaction, compatibility, retry, cancellation, and error-mapping comments, say which boundary the guarantee does and does not cover when that distinction matters.
- If only part of a cleanup, rollback, publication, or ordering guarantee is true, name the part that is covered and avoid implying the rest.

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

- Chinese is the default for added or rewritten comments. If the current request or a higher-priority repository instruction specifies another comment language, use that language.
- If the user asks for a different comment language but leaves it ambiguous, ask before editing comments.
- Reviewing, auditing, or removing comments does not require a language confirmation unless the task will also add or rewrite comments.
- Preserve project terms exactly when they are established by code, public docs, or tests.

## Language-Agnostic Comment Style

- Use the target language's native doc-comment form for public APIs, extension points, SPIs, configuration objects, contracts, and behavior that callers depend on. Examples include Java Javadocs, JavaScript/TypeScript JSDoc or TSDoc, Python docstrings, Go doc comments, Rust `///` or `//!`, and Doxygen-style comments only where that project already uses them.
- Do not expose unstable implementation details in public doc comments. If a detail explains how this version works but is not part of the caller contract, put it near the relevant implementation block instead.
- Use implementation comments for non-obvious local constraints, global relationships, and key logic steps that help readers learn how the source works.
- Prefer a short comment immediately before the logic block it explains. Inline end-of-line comments should stay rare and only constrain the adjacent statement.
- Avoid parameter, return, and exception documentation that merely repeats names, types, or obvious return values, regardless of syntax (`@param`, `Args:`, `Returns:`, `# Parameters`, and similar forms).
- Treat examples inside doc comments as executable or parseable unless the project clearly treats them as prose. Keep doctest, rustdoc, JSDoc/TSDoc, Doxygen, Javadoc, Go doc, and similar syntax valid.
- Use `TODO` or `FIXME` for verified defects, technical debt, or future improvements that need to stay visible in source. Do not add issue numbers, owners, or dates.

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

The examples use `//` for brevity. Adapt the syntax to the target language's normal line, block, or doc-comment style.

Low quality:

```text
// Check if the future is done.
if (future.isDone()) {
```

High quality:

```text
// future 状态发布后完成结果就可能可见，但回调仍会稍后由 executor 调度执行。
```

Low quality:

```text
// 获取用户。
User user = getUser();
```

High quality:

```text
// 下游按提交顺序合并异步结果，因此这里不能改成并行无序收集；否则响应项会和请求项错位。
```

High-quality source-learning implementation comment:

```text
// 先发布新块再回收旧块，保证稳定句柄不会在校验或计费失败时解析到半迁移对象。
```

High-quality FIXME:

```text
// FIXME: 当前分支只覆盖 RESP2 错误映射，补齐 RESP3 push 消息路径后才能复用该转换逻辑。
```

## Final Check

Before finishing a comment task, verify:

- each comment explains something non-obvious and useful;
- each added, rewritten, or preserved relevant comment passes the Comment Quality Gate;
- source-learning requests include comments at important logic points, not only module, type, class, function, or method doc comments;
- public doc comments do not promise private implementation details that callers cannot safely depend on;
- behavioral claims are scoped to the exact path, boundary, phase, protocol, thread, executor, cache, transaction, or failure semantics where they hold;
- each behavioral claim is correct in the full code path;
- async or concurrent claims do not overpromise ordering, timing, or thread ownership;
- stale nearby comments were updated or removed;
- language and terminology match the default, user-confirmed, explicitly requested, or higher-priority repository language;
- parsed doc comments and embedded examples still satisfy relevant formatter, doc, doctest, lint, or compile checks when those checks are available and proportionate;
- `TODO` and `FIXME` comments describe verified facts and actionable follow-up without invented issue numbers, owners, or dates;
- low-quality comments were not added.
