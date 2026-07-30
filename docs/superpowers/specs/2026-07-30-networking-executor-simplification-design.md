# Networking And Executor Simplification Design

## Status

Approved by the existing four-stage simplification directive. The user asked
to complete every stage in order and explicitly removed performance from the
acceptance criteria.

## Program Context

This is stage 2 of the approved simplification program:

1. request and command path;
2. networking and executor state machines;
3. DB and FFM implementation;
4. Maven module and test topology.

Stage 1 ended at commit `935c2417` with one command path and centrally rendered
semantic results. This stage removes state and adapter structure that no
longer protects a live behavior. It does not enter DB/FFM or module-topology
work.

## Context

The networking and executor path still contains several layers introduced for
older execution and reply models:

```text
NettyExecutionRequestIngress
  -> CommandExecutor.tryAcquire
  -> ExecutorAdmission
  -> CommandExecutorSubmitter
  -> ExecutorBacklogBudget
  -> ExecutorTaskQueue
  -> CommandExecutorDrainLoop
  -> CommandExecutorExecutionSupport
  -> ExecutionIoAdapter
```

The broad shape is valid: ingress and reply capacity must be reserved before
ownership moves, the owner thread must serialize DB work, and reply capacity
waiting must preserve command order. The implementation nevertheless carries
avoidable duplication:

- `CommandExecutorExecutionSupport` still has a buffered-reply error path,
  touched-connection flush collection, and `ExecutionIoAdapter` methods that
  production Netty code rejects or ignores. Stage 1 made that path unreachable
  by requiring a registered ordered reply slot for every command.
- `ExecutorBackpressureIo`, `ExecutorBackpressureRuntime`, and
  `ExecutorBackpressureObserver` are one-implementation projections assembled
  with anonymous classes in `CommandExecutor`. Their information is already
  available through `ExecutionConnection`, `ExecutionConnectionContext`, and
  `ExecutionIoAdapter`.
- `ExecutorKeyState` and `ExecutorKeyStateProvider` expose queue internals
  through `ExecutionConnectionContext`, force an unchecked cast, and split one
  scheduling state machine across three types.
- `ExecutorBacklogBudget` uses separate CAS loops for count and bytes, then
  rolls one reservation back when the other fails. Performance is not an
  acceptance criterion, so one small lock can express the invariant directly.
- ingress duplicates acquired/unavailable/rejected branching between first
  submission and waiter retry.
- `RespProtocolErrorReplyHandler` is not installed by the production pipeline;
  ordered protocol errors are handled by `NettyExecutionRequestIngress`.
- `YierdisServerChannelInitializer` retains private protocol-close helpers that
  have no caller.

## Goals

- Remove unreachable reply and protocol paths.
- Put FAIR/GLOBAL queue state in one conventional implementation.
- Make backlog task/byte reservation one atomic operation.
- Remove single-implementation backpressure projection interfaces.
- Express ingress admission handling once.
- Preserve every ownership, ordering, capacity, close, and wire behavior.
- Produce a net reduction in touched production Java.

## Non-Goals

- Do not change command parsing, command registration, DB behavior, mutation
  staging, FFM ownership, TTL, maxmemory, or transaction semantics.
- Do not remove or rename `GLOBAL` or `FAIR` scheduling configuration.
- Do not remove ingress, executor, per-connection reply, global reply, or
  single-reply capacity limits.
- Do not replace manual reads with unbounded `autoRead`.
- Do not rewrite RESP grammar or change protocol-error text.
- Do not remove ordered reply slots, streamed chunking, result-unknown close,
  or owner-thread source cleanup.
- Do not merge, add, remove, or rename Maven modules.
- Do not use throughput, latency, allocation, or benchmark results as gates.

## Frozen Invariants

1. Netty event loops decode and submit; only the serial owner thread executes
   commands or accesses DB state.
2. Executor admission reserves one task slot and its retained request bytes
   before request/reply ownership transfers.
3. Closing an unpublished admission releases exactly that reservation; a
   published admission never releases it from the caller.
4. FIFO order is preserved in `GLOBAL`; per-connection FIFO and round-robin
   progress are preserved in `FAIR`.
5. A reply-capacity-blocked command remains ahead of later commands at the
   ordering scope selected by the scheduling policy.
6. A stale prepared command is closed and retried at that same head; an
   executed command is never retried.
7. Ingress-memory, executor-backlog, reply-capacity, transport-writability, and
   terminal-close pause causes remain independent. Clearing one cause must not
   resume reads while another remains active.
8. Protocol errors receive an ordered reply slot, appear after earlier replies,
   and close only after their terminal reply is flushed when possible.
9. Every request, admission, prepared command, capacity waiter, reply slot,
   chunk, lease, and streamed source has one terminal owner.
10. Shutdown rejects new work, wakes/cancels waiters, drains or cancels queued
    owners, then closes budgets and event loops in the existing order.

## Target Executor Structure

The public execution path remains:

```text
NettyExecutionRequestIngress
  -> CommandExecutor.tryAcquire(connection, retainedBytes)
  -> ExecutorAdmission.publish(request, replySlot)
  -> owner-thread drain
  -> prepare -> reserve -> validate -> execute -> render
  -> ReplySlot READY
```

### Queue Ownership

`ExecutorTaskQueue` owns all scheduling state under one private lock.

For `GLOBAL`, it owns one `ArrayDeque<T>` and an optional blocked head. For
`FAIR`, it owns an identity-keyed map from connection to a private per-key
state, plus an active-key deque. A per-key state contains its FIFO task deque,
optional blocked head, blocked readiness, and scheduled flag. Empty states are
removed after they are no longer active or blocked.

This deletes `ExecutorKeyState`, `ExecutorKeyStateProvider`, the queue state
stored in `ExecutionConnectionContext`, atomics that only coordinate the old
split ownership, and the unchecked cast in `CommandExecutor`.

The implementation remains generic and testable, but it becomes
package-private because it is an executor detail.

### Backlog Reservation

`ExecutorBacklogBudget` owns task count, retained bytes, and capacity waiters
under one lock. `tryReserve(retainedBytes)` checks both limits and commits both
counters in one critical section. `release(retainedBytes)` checks underflow,
updates both counters, selects eligible waiters, then invokes callbacks outside
the lock.

One waiter is signalled at most once. Cancellation removes it or makes later
selection a no-op. Shutdown detaches all waiters and invokes their callbacks
outside the lock so ingress can observe the stopped executor.

The existing metrics and high/low-watermark decisions remain unchanged.

### Backpressure Boundary

`ExecutorBackpressureController<C extends ExecutionConnection>` receives the
real `ExecutionIoAdapter<C>` and reads state directly from
`connection.context()`. It records per-connection and global enter/exit counts
itself. This removes:

- `ExecutorBackpressureIo`;
- `ExecutorBackpressureRuntime`;
- `ExecutorBackpressureObserver`;
- the three anonymous adapters in `CommandExecutor`.

`ExecutionIoAdapter` keeps only live operations: active/writable checks,
input pause/resume, close callback registration, and transport close. The
buffered reply sink/write/flush methods are deleted.

### Execution Outcome

`CommandExecutorExecutionSupport.execute(task)` returns the same four logical
outcomes used by the drain loop: completed/closed, reply-capacity blocked, and
stale reprepare. It no longer accepts a touched-connection collection because
ordered reply slots flush themselves. The unreachable direct buffered-error
method is deleted.

## Target Networking Structure

### Ordered Protocol And Command Replies

The production pipeline remains:

```text
InboundReadCreditHandler
  -> InboundByteAccountingHandler
  -> RespRequestDecoder
  -> RespDecodedMessageGate / RegisteredRespMessage
  -> NettyExecutionRequestIngress
  -> CommandExecutor
  -> ReplySlot / ConnectionReplySequencer
```

`RespProtocolErrorReplyHandler` and its isolated tests are deleted because this
handler is not in that pipeline. `NettyExecutionRequestIngress` remains the one
owner of ordered protocol/control replies.

### Ingress Submission

Ingress uses one helper to classify `ExecutorAdmissionAttempt` for both initial
submission and capacity-wait retry. The helper either transfers ownership,
puts the same submission back at the deque head and arms one waiter, or
terminates the submission with the existing reply/close behavior.

The pending deque is still required: decoder/reply-gate admission may complete
before executor backlog capacity returns. It is not replaced with an unbounded
queue or a second request representation.

### State Machines Kept Intact

`RespRequestDecoder`, `InboundMemoryBudget`, `InboundReadCreditHandler`,
`OutboundMemoryBudget`, `ReplySlot`, `BoundedChunkedReplySink`, and
`ConnectionReplySequencer` retain their behavioral algorithms in this stage.
They own protocol progress, bounded allocation, cross-thread cleanup, and
receive-order invariants. They may receive narrow dead-code or naming cleanup,
but no representation rewrite is accepted without a separate failing
characterization test.

## Error And Cleanup Rules

- Expected command errors continue through the reserved `ReplySlot`.
- A request larger than the executor byte limit receives the existing error
  and closes only the request owner, not unrelated queued work.
- A stopped or closing executor rejects without transferring ownership.
- Any failure after command execution starts remains result-unknown and closes
  the transport.
- Cleanup continues after individual close failures. Primary failures retain
  later cleanup failures as suppressed where the existing contract exposes
  them.
- Callback invocation occurs outside queue/budget locks.

## Testing

- Keep all existing executor, ingress, ordered reply, pressure, fuzz, shutdown,
  and architecture tests.
- Add direct queue characterization for GLOBAL FIFO, FAIR rotation, blocked
  head, stale retry, cancellation, and empty-state reclamation.
- Add backlog reservation races and waiter cancellation/shutdown tests against
  the new single-lock implementation.
- Keep admission ownership tests for publish/close races.
- Add architecture guards rejecting deleted projection interfaces, buffered
  reply methods, and the unused protocol handler.
- Run affected reactors and the full JDK 25 Maven suite.

## Acceptance Criteria

Stage 2 is complete when:

1. All dead buffered-reply and standalone protocol-handler paths are absent.
2. FAIR and GLOBAL scheduling use one queue implementation with no external
   key-state SPI or unchecked connection-context cast.
3. Backlog count/byte reservation is atomic under one implementation lock.
4. Backpressure uses `ExecutionConnectionContext` and `ExecutionIoAdapter`
   directly; the three projection interfaces are absent.
5. Initial and retried ingress submissions share one attempt-classification
   path.
6. Existing wire replies, ordering, close behavior, scheduling configuration,
   capacity limits, and ownership tests pass unchanged.
7. No DB/FFM or Maven-topology file changes are included.
8. Touched production Java has a net line-count reduction.
9. The affected and full JDK 25 Maven test suites pass.

## Follow-On Stages

After Stage 2 is committed and verified, the program continues with:

3. DB and FFM implementation simplification;
4. Maven module and test-topology simplification.

