# Command Contract Unification Design

## Summary

Unify command metadata, argument validation, option parsing, and handler
dispatch behind one command contract.

Today `CommandDescriptor` is already the source of registration metadata for
`COMMAND INFO`, but command execution still relies on each handler to hand-roll
arity checks, option scans, syntax errors, and numeric parsing. The target design
keeps `CommandDescriptor` focused on external metadata and makes `CommandSpec`
the complete execution contract:

```text
CommandSpec<T> = descriptor + parser + handler + multi policy
```

`YierdisFastCommandProcessor` should own the single command lifecycle:

```text
lookup -> parse -> handle parse error -> execute typed handler -> map runtime errors
```

This is deliberately not a full Redis command DSL. The design introduces a small
typed parser boundary and a low-allocation argument reader. Simple commands use
stock arity parsers; complex commands such as `SET`, `SCAN`, and `ZRANGE` keep
local Java parsing code, but return structured parse results instead of writing
reply errors directly.

## Problem Statement

The command layer currently has two partially overlapping sources of truth:

- Registration metadata lives in `CommandDescriptor` and `CommandSpec`.
- Runtime syntax validation lives inside each command handler.

That split causes repeated and drifting logic:

- `CommandDescriptor.arity()` is used by `COMMAND INFO`, but it is not enforced
  by the execution pipeline.
- Fixed arity commands duplicate the same `request.argc()` checks in handlers.
- Flexible arity commands encode min, max, pair-tail, or one-of rules manually.
- Options such as `SET NX XX GET KEEPTTL EX PX EXAT PXAT`, `SCAN MATCH COUNT`,
  and `ZRANGE WITHSCORES REV` are parsed by local loops that also write
  `ERR syntax error`.
- Numeric parsing is partly centralized in `CommandSupport`, but parse failure
  propagation is implicit through exceptions.
- Some command-shape validation leaks below the command layer, for example
  pair-count validation for collection writes.

The result is not only duplicated code. The deeper issue is that a command's
public metadata, executable syntax, transaction queuing behavior, and business
handler are not expressed as one contract.

## Goals

- Make `CommandSpec` the single executable contract for a command.
- Keep `CommandDescriptor` as the stable `COMMAND INFO` metadata model.
- Centralize the execution pipeline in `YierdisFastCommandProcessor`.
- Move arity and syntax errors out of command handlers where practical.
- Keep command parsing low allocation and compatible with `ExecutionRequest`.
- Support incremental migration without rewriting every command at once.
- Preserve current reply messages unless a test intentionally changes behavior.
- Make transaction queuing validate command syntax before returning `QUEUED`.
- Prevent future commands from adding ad hoc arity and syntax handling.

## Non-Goals

- Do not build a general-purpose Redis grammar engine.
- Do not introduce annotation processing, reflection dispatch, or code
  generation.
- Do not force every command into a declarative option schema.
- Do not change Redis command semantics beyond syntax validation placement.
- Do not remove `CommandDescriptor` or change `COMMAND INFO` output shape.
- Do not make the DB layer responsible for request syntax validation.

## Current Architecture

`CommandRegistry` maps command names to `CommandSpec`.

`CommandSpec` currently contains:

```text
handler
descriptor
disallowedInMultiError
```

`CommandDescriptor` contains:

```text
arity
firstKeyIndex
lastKeyIndex
keyStep
```

`YierdisFastCommandProcessor` currently:

- rejects empty commands and invalid null bulk strings,
- handles transaction queuing,
- looks up `CommandSpec`,
- invokes `spec.handler()` directly,
- maps runtime exceptions to reply errors.

`CommandSupport` currently provides shared execution helpers:

- DB routing helpers,
- argument view and slice helpers,
- ASCII case-insensitive comparison,
- integer parsing,
- score-bound parsing,
- scratch-buffer reuse.

Each `*Commands` class currently combines three responsibilities:

- parse request syntax,
- call the DB operation,
- write the reply.

This design keeps the existing module boundary: `yierdis-core-command` remains
the transport-neutral command layer. The change is internal to that layer.

## Considered Approaches

### Approach A: Enforce `CommandDescriptor.arity()` directly

This would be small, but it would conflate external metadata with executable
syntax. Redis arity metadata cannot accurately express all command shapes:

- `PING` accepts one or two arguments.
- `ZRANGE` accepts a small bounded range.
- `HSET` and `ZADD` require pair-shaped tails.
- `SET` has mutually exclusive option groups.
- `SCAN` has repeated option/value pairs.

Rejected because it fixes only the easiest duplication while making
`CommandDescriptor` carry responsibilities it should not own.

### Approach B: Add a standalone option parser helper

This would reduce duplication in `SET`, `SCAN`, and `ZRANGE`, but command
lifecycle would remain split. Handlers would still decide when to validate,
how to return parse errors, and whether a request is valid before transaction
queuing.

Rejected as a local cleanup rather than an architectural fix.

### Approach C: Make `CommandSpec` the executable command contract

This makes each command definition contain both metadata and execution syntax.
The processor becomes the only owner of lookup, parse, parse-error reply, and
handler invocation.

Chosen because it solves the global ownership problem while keeping the parser
surface intentionally small.

## Architectural Decision

Adopt Approach C.

Introduce a typed command contract:

```java
final class CommandSpec<T> {
    CommandDescriptor descriptor();
    CommandParser<T> parser();
    CommandHandler<T> handler();
    String disallowedInMultiError();
}
```

The exact Java shape can be adjusted during implementation. The important
boundary is semantic:

- `CommandDescriptor` describes the command externally.
- `CommandParser<T>` validates `ExecutionRequest` and returns typed arguments.
- `CommandHandler<T>` executes business behavior using parsed arguments.
- `CommandSpec<T>` binds those pieces into one registration unit.

The registry can store erased specs internally. Generic typing is most valuable
at the command definition site, where parser output and handler input must
match. The registry and processor do not need to expose generic complexity to
callers.

## Target Execution Flow

Normal execution:

```text
request
  -> validate non-empty command and null bulk strings
  -> registry lookup
  -> unknown command reply if missing
  -> spec.parser().parse(request)
  -> parse error reply if invalid
  -> spec.handler().execute(parsedArgs, ctx)
  -> runtime exception mapping
```

Transaction queuing:

```text
request in MULTI
  -> allow MULTI / EXEC / DISCARD to run immediately
  -> registry lookup
  -> unknown command marks transaction aborted and returns unknown command error
  -> disallowed-in-multi marks transaction aborted and returns configured error
  -> parse request with the same spec parser
  -> parse error marks transaction aborted and returns parse error
  -> enqueue original request and return QUEUED
```

The queued item remains the original request copy, not the parsed object. `EXEC`
replays commands through the same processor path so runtime semantics and change
tracking stay unchanged.

## Core Abstractions

### `CommandParser<T>`

Small functional interface:

```java
interface CommandParser<T> {
    CommandParseResult<T> parse(ArgReader args);
}
```

The parser should not write replies. It either returns typed arguments or a
structured parse error.

### `CommandParseResult<T>`

Represents either success or failure:

```text
ok(T parsed)
error(CommandParseError error)
```

Implementation can use a small final class rather than exceptions on the normal
syntax-error path.

### `CommandParseError`

Represents stable command-layer parse failures:

```text
wrongArity(commandName)
syntax()
integerOutOfRange()
custom(message)
```

The processor owns conversion to RESP errors:

- `wrongArity("set")` -> `ERR wrong number of arguments for 'set' command`
- `syntax()` -> `ERR syntax error`
- `integerOutOfRange()` -> `ERR value is not an integer or out of range`
- `custom("ERR invalid expire time in 'set' command")` -> exact message

### `CommandArity`

Reusable command-shape validator:

```text
exact(n)
min(n)
range(min, max)
oneOf(...)
pairTail(minArgc, tailStartIndex)
```

This type should be a helper for parsers, not a replacement for
`CommandDescriptor`.

### `ArgReader`

Low-allocation wrapper over `ExecutionRequest`:

```text
argc()
bytes(index)
view(index)
slice(index)
is(index, literal)
longAt(index)
nonNegativeLongAt(index)
positiveLongAt(index)
intClampedAt(index)
```

`ArgReader` should reuse the existing `CommandSupport` parsing primitives where
possible. It should not eagerly allocate strings.

### Parsed Argument Records

Each command family can define small package-private records near the command
module:

```text
SetArgs
ScanArgs
ZRangeArgs
ZRangeByScoreArgs
```

Simple commands may not need custom records. They can parse into a shared
request-backed view or small value object.

## Command Definition Shape

Simple command example:

```java
registration.register(CommandSpec.of(
        "GET",
        CommandDescriptor.of(2, 1, 1, 1),
        CommandParsers.exact(2, GetArgs::parse),
        this::get
));
```

Complex command example:

```java
registration.register(CommandSpec.of(
        "SET",
        CommandDescriptor.of(-3, 1, 1, 1),
        SetArgs::parse,
        this::set
));
```

The final API should be concise enough that registering a command is easier than
hand-writing validation in a handler.

## Parser Policy

The design intentionally avoids a universal option schema.

Use stock parser helpers when command shape is simple:

- fixed arity,
- min arity,
- bounded range,
- pair tail.

Use local typed parsers when command syntax is command-specific:

- `SET` expiration and mode option groups,
- `SCAN` repeated option/value pairs,
- sorted-set range commands,
- commands with command-specific error messages.

Local parsers should still use `ArgReader`, `CommandArity`, and
`CommandParseError`. They should not write to `ReplyWriter`.

## Handler Policy

Handlers should receive parsed arguments and `CommandContext`.

Handlers may still throw runtime semantic exceptions:

- wrong type,
- DB-level command errors,
- off-heap OOM,
- command-specific semantic validation that depends on current time or DB state.

Handlers should not perform basic arity checks or write `ERR syntax error`.

The line is:

- request-shape validation belongs to parser,
- command semantics belong to handler,
- storage invariants belong to DB.

## Migration Strategy

Migration must be incremental. Add compatibility constructors/factories so
existing handlers can keep working while commands move to the new contract.

### Phase 1: Add the contract types

Add:

- `CommandParser<T>`
- `CommandHandler<T>`
- `CommandParseResult<T>`
- `CommandParseError`
- `CommandArity`
- `ArgReader`
- new `CommandSpec` factories

Keep existing handler registration working through a legacy adapter.

### Phase 2: Move processor to parse-before-handle

Update `YierdisFastCommandProcessor` to use `spec.parser()` before invoking the
handler.

Legacy specs use a pass-through parser so behavior is unchanged.

### Phase 3: Validate before transaction queuing

Use the same parser path for commands queued inside `MULTI`.

Unknown commands, disallowed commands, arity errors, and syntax errors should
mark the transaction aborted before returning the error.

### Phase 4: Migrate simple commands

Start with fixed/min arity commands:

- connection commands,
- string commands except `SET` and `BITCOUNT`,
- list/set/hash/HLL commands with simple shapes,
- key commands with exact arity.

Move pair-tail validation for `HSET` and `ZADD` into the command parser so the
DB layer no longer owns request syntax.

### Phase 5: Migrate option-heavy commands

Migrate:

- `SCAN`
- `ZRANGE`
- `ZRANGEBYSCORE`
- `ZREVRANGEBYSCORE`
- `SET`

`SET` should be last because it has the richest option interaction and the
largest compatibility surface.

### Phase 6: Remove legacy registration path

After built-in and server commands are migrated, remove or restrict legacy
handler registration so new commands must provide a parser.

## Testing Strategy

Add tests at three levels.

### Unit tests

Cover:

- `CommandArity` exact/min/range/oneOf/pairTail behavior,
- `CommandParseError` to reply-message mapping,
- `ArgReader` numeric parsing and ASCII matching.

### Command behavior tests

For migrated commands, preserve existing behavior:

- wrong arity messages,
- syntax error messages,
- integer parse errors,
- command-specific errors such as invalid `SET` expire time,
- duplicate and mutually exclusive options.

Important cases:

- `SET k v NX XX` -> syntax error
- `SET k v EX abc` -> integer error
- `SET k v EX 0` -> invalid expire time
- `SCAN 0 COUNT x` -> integer error
- `SCAN 0 MATCH` -> syntax error
- `ZRANGE k 0 -1 BAD` -> syntax error
- `ZADD k 1 a 2` -> wrong arity from command layer
- `HSET k f` -> wrong arity from command layer

### Transaction tests

Add or adjust tests for `MULTI`:

- unknown command inside transaction aborts before `EXEC`,
- wrong arity inside transaction aborts before `EXEC`,
- syntax error inside transaction aborts before `EXEC`,
- valid commands still return `QUEUED`,
- `EXEC` still replays through normal execution and emits one reply per queued
  command.

## Acceptance Criteria

- `CommandDescriptor` remains focused on `COMMAND INFO` metadata.
- New command definitions can bind descriptor, parser, handler, and MULTI policy
  in one `CommandSpec`.
- `YierdisFastCommandProcessor` owns parse-before-handle for non-legacy specs.
- Basic arity validation is no longer hand-written in migrated handlers.
- Migrated handlers no longer write `ERR syntax error` directly.
- Pair-tail validation for `HSET` and `ZADD` is owned by the command layer.
- Transaction queuing validates request syntax before `QUEUED`.
- Existing command behavior tests continue to pass, with explicit updates only
  where transaction syntax validation intentionally moves earlier.
- Architecture tests or source guards prevent reintroducing ad hoc command
  metadata defaults outside the command contract.

## Risks and Mitigations

### Risk: Generic `CommandSpec<T>` complicates registry code

Mitigation: keep generics at command definition sites and erase internally in
`CommandRegistry`. The processor only needs an executable spec interface.

### Risk: Typed parsed args allocate too much

Mitigation: parsed args can hold request-backed views, byte arrays already
provided by `ExecutionRequest`, or primitive values. Do not convert all argv to
strings.

### Risk: Parser layer becomes a second command framework

Mitigation: keep parser primitives small. Prefer Java parser methods for complex
commands instead of a declarative option DSL.

### Risk: Transaction behavior changes surprise existing tests

Mitigation: migrate transaction validation deliberately and add focused tests
that document the Redis-compatible behavior.

### Risk: Migration stalls halfway

Mitigation: allow legacy specs temporarily, but add tests or source guards that
track remaining legacy registrations and fail once the migration is declared
complete.

## Implementation Notes

The best implementation is likely a narrow package-private model inside
`yier.bubu.redis.command`.

Public extension points can remain conservative:

- keep existing registration overloads during migration,
- add new `CommandSpec` factories for typed parsers,
- avoid exposing parser internals outside `core-command` until the model proves
  stable.

This design should integrate with the existing architecture roadmap's command
definition single-source-of-truth direction, but it is more specific about the
execution contract: a command is not only metadata plus a handler; it is metadata
plus a parser plus a handler plus transaction policy.
