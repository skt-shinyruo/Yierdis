# Command Pipeline Simplification Design

## Status

Approved in conversation on 2026-07-28.

## Program Context

This design is the first stage of a four-stage implementation simplification
program:

1. simplify the request and command path;
2. simplify the networking and executor state machines;
3. simplify the DB and FFM implementation;
4. simplify the Maven module and test topology.

Each stage gets its own design, implementation plan, implementation, and
verification cycle. This document covers only stage 1. Later stages may build
on its contracts, but they must not be mixed into this change.

## Context

Yierdis deliberately keeps Java 25, Netty, RESP2 and basic RESP3, one serial
owner thread, FFM-backed storage, TTL, maxmemory, transactions, backpressure,
and explicit resource ownership. Those mechanisms define the project and are
not candidates for removal.

The current command path nevertheless represents one command through too many
pass-through and paired abstractions:

```text
CommandExecutor
  -> CommandExecutionEngine
  -> YierdisEngine
  -> DefaultYierdisEngine
  -> YierdisFastCommandProcessor
  -> CommandRegistry
  -> CommandDefinition
  -> CommandParser
  -> CommandPreparer
  -> PreparedCommand
  -> ReplyShape plus RedisReplyWriter calls
```

Several of those boundaries own no invariant. `DefaultYierdisEngine` wraps one
processor call, `CommandPreparationContext` wraps one session, parser and
preparer are registered as separate generic functions, and command handlers
usually express the same reply twice: once as a `ReplyShape` and again as a
writer action. `CommandRegistry` also contains a custom open-addressed table
whose allocation behavior is no longer a project requirement.

The 2026-07-28 core-path refactor already removed unused compatibility
surfaces and duplicated implementation. This design continues that direction
by deleting concepts, not by adding another facade over the existing path.

## Goals

- Give the executor one direct command preparation entry point.
- Make one command specification the source of command syntax, metadata,
  transaction policy, parsing, and preparation.
- Preserve a side-effect-free parse phase for transaction preflight without
  retaining separate parser and preparer registration APIs.
- Express an ordinary command reply once as a semantic value.
- Keep reply reservation before mutation, stale re-preparation, exactly-once
  mutation execution, request leases, and deterministic cleanup explicit.
- Reduce registration boilerplate and duplicated argument helpers.
- Use conventional JDK collections and direct control flow where custom fast
  paths do not protect a correctness invariant.
- Produce a net reduction in touched production Java code.

## Non-Goals

- Do not change supported commands, wire replies, error text, configuration,
  transaction behavior, or connection-close behavior.
- Do not change decoder, ingress, executor scheduling, backpressure, reply-gate,
  or shutdown state machines.
- Do not change DB semantics, mutation staging, memory accounting, native
  storage, or FFM lifecycle behavior.
- Do not merge, rename, or otherwise reorganize Maven modules in this stage.
- Do not preserve source or binary compatibility for internal Java APIs.
- Do not retain deprecated adapters or parallel old and new command paths.
- Do not use annotations, reflection, or code generation for command discovery.
- Do not use throughput, latency, or allocation measurements as acceptance
  gates for this stage.

## Frozen Invariants

The rewrite must preserve these invariants even though performance is not an
acceptance criterion:

1. The Netty thread decodes and submits; the serial owner thread executes
   commands and accesses the DB.
2. Every admitted request retains its input memory until its final consumer
   releases it.
3. A command mutation starts only after its reply envelope is reserved.
4. A prepared command that waits for capacity is not parsed or prepared again.
5. A stale prepared command is closed before the original request is prepared
   again.
6. Once mutation execution starts, the framework never automatically retries
   it.
7. Bulk, sequence, and map results remain streamable and are not materialized
   into a second complete payload.
8. Transaction queueing retains `ExecutionRequest`; it does not create a
   second transaction-only command representation.
9. Every request, prepared command, retained source, transaction child, and
   reply slot has one clear owner and is closed or transferred exactly once.
10. RESP sizing remains in the protocol adapter, not in command or DB code.

## Target Architecture

The target request-to-reply path is:

```text
CommandExecutor
  -> CommandDispatcher.prepare(session, request)
  -> CommandSpec.handler().parse(args)
  -> CommandInvocation.prepare(session)
  -> PreparedCommand
  -> reserve reply capacity
  -> validate
  -> execute(CommandExecutionContext)
  -> CommandResult
  -> RedisReplyRenderer
```

`CommandDispatcher` is the sole command-layer entry. The executor keeps its
small transport-neutral functional boundary but receives a direct method
reference to the dispatcher. `YierdisEngine`, `DefaultYierdisEngine`, and
`CommandPreparationContext` are deleted. Runtime maintenance is wired directly
from the runtime owner and is not routed through a command facade.

Stage 1 may make the minimal executor edits needed to consume the new
`PreparedCommand` result contract. It must not alter queueing, capacity waiting,
fairness, backpressure, or connection lifecycle transitions.

## Command Specification And Registration

`CommandSpec` contains exactly two things:

```java
public record CommandSpec(
        CommandSyntax syntax,
        CommandHandler handler
) {
}
```

`CommandSyntax` remains the single source for normalized name, arity, key
metadata, and transaction policy. The standalone `CommandParser<T>`,
`CommandPreparer<T>`, `CommandParsers`, and generic `CommandDefinition<T>` APIs
are deleted.

The replacement preserves two semantic phases through one handler contract:

```java
@FunctionalInterface
public interface CommandHandler {
    CommandInvocation parse(CommandArgs args) throws CommandParseException;
}

@FunctionalInterface
public interface CommandInvocation {
    PreparedCommand prepare(CommandSession session);
}
```

The framework passes parsing no session, DB router, or mutation context.
Command modules may capture immutable command configuration, but parsing must
not call captured runtime services or mutate connection or DB state. Parse-only
transaction tests enforce this contract for every registered command. Typed
parsed values are captured by the returned invocation rather than passed
through a generic framework pipeline.

Registration uses a small explicit builder over `CommandSpec`. Every command
has one registration expression. The builder may provide overloads for a
direct handler and for a separately named parse method, but those overloads
must produce the same `CommandHandler -> CommandInvocation` contract. They must
not recreate parser and preparer interface hierarchies under new names.

The registry uses a conventional JDK map keyed by normalized uppercase command
name. It is mutable only during composition and sealed before serving traffic.
Duplicate names, invalid metadata, null specs, or null handlers fail startup.
Sorted command names and `COMMAND INFO` lookup derive from this same map.

Runtime lookup may allocate a normalized command-name string. This is an
accepted trade-off because performance is not an acceptance criterion.

## Command Arguments

`CommandArgs` is the one argv reader used by command code. It wraps the current
`ExecutionRequest` and owns no request memory. It provides:

- argc, null, length, byte, slice, and read-only byte-array access;
- ASCII case-insensitive literal comparison;
- integer parsing and range helpers;
- access to the underlying request only where a prepared action must retain or
  reference it.

The equivalent parsing and ASCII helpers in `CommandSupport`,
`CommandRequestSupport`, and other command helpers are removed. Expected input
failures throw `CommandParseException` with an explicit Redis error message.
Broad translation of arbitrary `IllegalArgumentException` into a client error
is removed; migrated command code uses the explicit parse exception for
user-controlled invalid input.

## Dispatcher

`CommandDispatcher.prepare(session, request)` performs one visible sequence:

1. reject an empty command;
2. enforce the existing null-bulk argument policy;
3. normalize and look up the command name;
4. validate arity from `CommandSyntax`;
5. apply transaction policy;
6. call the handler's side-effect-free parse method;
7. prepare the resulting invocation for immediate execution.

The dispatcher contains the behavior currently split across
`YierdisFastCommandProcessor`, `TransactionQueuePolicy`,
`CommandExceptionTranslator`, and `CommandRequestSupport`. Small private
helpers are allowed, but they are not separately injectable policy objects
unless a second production implementation exists.

The dispatcher also provides an internal replay entry that skips transaction
queueing. `EXEC` uses this entry; it is not exposed as a second public command
engine.

## Prepared Execution

`PreparedCommand` keeps the two-phase safety boundary but returns a result
instead of writing directly:

```java
public interface PreparedCommand extends AutoCloseable {
    ReplyShape reservationShape();

    ValidationResult validateBeforeExecute();

    CommandResult execute(CommandExecutionContext context);

    @Override
    void close();
}
```

`CommandExecutionContext` retains `CommandSession` and the request-scoped
`MutationContext`. It no longer exposes `RedisReplyWriter`.

`CommandResult` contains a non-null semantic reply and a close-after-reply
flag. Static factories cover ordinary replies, errors, and the close-after-
reply result used by `QUIT`.

Prepared-command factories cover three real cases:

- a ready reply whose exact shape is already known;
- an action whose result is known only after reservation and which declares a
  conservative semantic reply bound;
- an owned or prepared-mutation result with validation and cleanup behavior.

The factories centralize expected execution-error translation and exact-once
resource cleanup. They do not hide DB operations or transaction control flow.

## Semantic Replies

`RedisReply` is a protocol-neutral semantic value model. It covers the scalar,
null, aggregate, bulk, streamed sequence, and streamed map replies already
supported by `RedisReplyWriter`.

For an exact reply, one `RedisReply` instance supplies both its semantic shape
and its data for rendering. Command handlers no longer pair code such as
`ReplyShapes.integer(value)` with `writer.integer(value)`.

Protocol-independent `RedisReplyRenderer` is the only production component
that traverses a `RedisReply` and invokes `RedisReplyWriter`. Architecture tests
forbid builtin command implementations from importing or invoking the writer.
`RespReplySizer` continues to convert the reply's semantic `ReplyShape` into a
RESP2 or RESP3 `ReplyPlan`.

Commands whose result is available only after mutation declare a conservative
reservation shape, such as an integer upper bound, and return the actual
`RedisReply` after execution. The reserved sink remains the final enforcement
boundary if an implementation returns a larger reply than declared.

Streaming DB result types are adapted in the command layer to generic reply
length and emission contracts. `server-api` does not depend on `db-api`.
Prepared commands own all sources referenced by their eventual result until
the executor finishes rendering and closes the prepared command.

A top-level execution-time command error may select the existing control reply
reservation through a dedicated semantic control-error reply. When `EXEC`
embeds such a child result in its already reserved aggregate envelope, it
converts it to an ordinary semantic error element rather than switching the
outer reservation.

## Normal Request Flow

The normal request lifecycle is:

1. The executor calls the dispatcher with the owned request and session.
2. The dispatcher validates, looks up, parses, and prepares one command.
3. The protocol sizer plans `PreparedCommand.reservationShape()`.
4. If capacity is unavailable, the executor waits with the same prepared
   command.
5. After reservation, the executor validates the prepared command.
6. A stale result is closed and re-prepared from the original request.
7. A valid command executes once and returns `CommandResult`.
8. The central renderer emits the semantic reply into the reserved writer.
9. The executor applies close-after-reply, marks the slot ready, and closes the
   prepared command and request.

No command handler writes protocol bytes or manages reply-slot readiness.

## Transaction Flow

While a transaction is active, the dispatcher uses the same map, syntax, and
handler:

1. transaction-control commands continue to immediate preparation;
2. disallowed commands return their existing error and abort the transaction;
3. ordinary commands run arity validation and side-effect-free parse;
4. successful preflight retains the original request and returns `QUEUED`;
5. unknown commands and parse failures return their existing errors and mark
   the transaction aborted.

`EXEC` drains and replays the retained requests through the dispatcher's
internal replay entry. A single child may retain its exact prepared envelope.
Multiple children retain the current maximum envelope and prepare and execute
in order because an earlier mutation may change a later reply.

Each child returns a semantic reply. The outer prepared `EXEC` command owns
child prepared commands, child reply sources, and queued requests until the
aggregate reply has rendered. It closes completed and unconsumed resources in
reverse ownership order on failure. Expected child command errors become array
elements and do not reorder or skip later commands. Unexpected framework or
result-unknown failures terminate the connection.

## Error Handling

Errors remain owned by the layer that can interpret them:

- RESP framing errors stay in decoder and ingress code.
- Empty command, illegal null argument, unknown command, arity error, and
  `CommandParseException` become prepared semantic error replies.
- Unknown, disallowed, and parse-invalid commands in `MULTI` preserve existing
  transaction-abort behavior.
- `WrongTypeException` and `YierdisCommandException` remain expected command
  errors and are translated centrally during preparation or guarded execution.
- Unexpected `IllegalArgumentException` is an internal defect after migrated
  input-validation sites use `CommandParseException`.
- Duplicate command names and invalid specifications fail composition before
  the server accepts traffic.

An expected execution error that is contractually known to precede mutation
visibility becomes a semantic control-error result. An unexpected exception
before a visible result uses the existing control reservation for
`ERR internal error` and closes the connection. `ResultUnknownException`, any
failure explicitly reported after mutation visibility may have changed, a
render failure after mutation, written bytes followed by failure, or a
reservation-bound violation is never retried; the reply is cancelled, the
result is marked unknown where applicable, and the connection closes.

Cleanup is exhaustive. Failure to close one prepared command or source does
not skip the remaining requests, children, sources, or reply slot. The primary
failure is preserved and later cleanup failures are suppressed or recorded.

## Module Ownership

Stage 1 preserves the current Maven graph:

- `server-api` owns execution request, session, prepared execution, semantic
  reply, rendering, sizing boundary, and writer contracts;
- `command-api` owns syntax, command specification, argument parsing,
  invocation, module registration, and command-facing services;
- `command-core` owns the dispatcher, registry composition, transaction
  control, and replay orchestration;
- `command-builtin` owns builtin command implementations and DB-result reply
  adapters;
- `server-executor` owns reservation timing and calls the new result contract;
- `networking-resp` owns RESP-specific sizing and encoding;
- `server-main` wires the dispatcher directly into the executor and wires
  runtime maintenance separately.

Physical module consolidation is deferred to stage 4.

## Testing

### Characterization And Unit Coverage

- Preserve all existing command, transaction, protocol, executor, and
  integration tests as the behavioral baseline.
- Add table-driven dispatcher tests for empty input, null arguments, unknown
  commands, arity, parse errors, transaction policies, queueing, and replay.
- Test registry sealing, duplicate rejection, normalized lookup, sorted names,
  and `COMMAND INFO` metadata from one spec source.
- Test `CommandArgs` numeric bounds, ASCII matching, null handling, slices, and
  explicit parse errors.
- Test every prepared factory for reservation shape, valid and stale paths,
  execution exactly once, close exactly once, and cleanup after failure.

### Reply Contract Coverage

- For every semantic reply variant, calculate the RESP2 and RESP3 plan and
  render it. Exact and conservative plans must never be smaller than actual
  bytes; maximum plans must select the maximum-reservation path.
- Cover scalar, aggregate, nested aggregate, null, bulk, streamed sequence,
  streamed map, conservative bounds, and control errors.
- Use tracked fake sources to prove that render success, render failure, stale
  re-preparation, cancellation, and `EXEC` close every source exactly once.

### Fault Injection

- Inject failures during parse, prepare, validate, execute, render, and close.
- Verify that no mutation is retried after execution starts.
- Verify result-unknown connection closure and control-error behavior before
  visible output.
- Verify `EXEC` closes completed children and the unconsumed queue tail without
  changing reply order.

### Architecture And Full Verification

- Update architecture tests to reject the deleted engine, preparation-context,
  parser/preparer, and custom-registry types.
- Reject direct `RedisReplyWriter` use from builtin command implementations.
- Require one registration expression per command and one authoritative
  argument helper implementation.
- Run affected module tests and the full Maven test suite with JDK 25.
- Do not use performance benchmarks as a pass/fail criterion.

## Acceptance Criteria

Stage 1 is complete when all of the following are true:

1. The target command path is the only production path.
2. `YierdisEngine`, `DefaultYierdisEngine`, `CommandPreparationContext`,
   `CommandParser`, `CommandPreparer`, `CommandParsers`, generic
   `CommandDefinition`, and the open-addressed registry are absent.
3. Every command is registered once through `CommandSpec`.
4. Ordinary command replies are expressed once as semantic replies; builtin
   commands do not invoke `RedisReplyWriter`.
5. Argument parsing and ASCII helpers have one command-layer implementation.
6. Request, prepared-command, transaction-child, streamed-source, and reply
   ownership tests pass on success and every injected failure path.
7. Existing wire behavior, error text, transaction behavior, and close behavior
   remain unchanged.
8. No executor scheduling, networking state-machine, DB/FFM implementation, or
   Maven-topology change is included.
9. Touched production Java code has a net line-count reduction.
10. The affected module tests and full JDK 25 Maven suite pass.

## Follow-On Stages

After this stage is planned, implemented, and verified, the program continues
in the approved order:

2. networking and executor state-machine simplification;
3. DB and FFM implementation simplification;
4. Maven module and test-topology simplification.

Each follow-on stage starts with a fresh repository review and its own approved
design. Stage 1 does not pre-authorize changes in those boundaries.
