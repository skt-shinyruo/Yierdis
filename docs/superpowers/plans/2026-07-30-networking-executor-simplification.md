# Networking And Executor Simplification Implementation Plan

**Goal:** Remove dead networking/executor paths and collapse split queue,
backlog, backpressure, and ingress state without changing scheduling, capacity,
ordering, ownership, wire, or close behavior.

**Architecture:** Keep the existing ordered ingress-to-reply pipeline. Use one
synchronized executor queue for `GLOBAL` and `FAIR`, one synchronized backlog
reservation state, and direct `ExecutionConnectionContext` /
`ExecutionIoAdapter` backpressure access. Delete only paths proven unreachable
from the production pipeline.

**Tech Stack:** Java 25, Maven, JUnit 4, Netty 4.1, RESP2/RESP3, existing
semantic command result pipeline, ordered reply slots, explicit JDK 25 prefix
for every Java/Maven command.

## Global Constraints

- Execute after Stage 1 commit `935c2417`.
- Preserve every CLI option, including `--executorSchedulingPolicy` values
  `fair` and `global`.
- Preserve serial owner-thread execution, request leases, reply preflight,
  backpressure, ordered egress, result-unknown close, and graceful shutdown.
- Preserve all ingress, executor, per-connection reply, global reply, and
  single-reply limits and their metrics.
- Do not edit DB, memory/FFM, command semantics, or Maven topology.
- Internal Java compatibility is not required; deleted internal adapters must
  not remain as deprecated aliases.
- Callbacks selected under a queue/budget lock run after releasing that lock.
- Performance and benchmark results are not acceptance gates.
- Touched production Java must have a negative net line count relative to
  `935c2417`.
- Every Java/Maven command uses:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH
```

## Task 1: Freeze Stage 2 State And Ownership Behavior

**Files:**

- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudgetTest.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ReplyCapacityBlockedSchedulingTest.java`
- Create: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorTaskQueueTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

- [ ] Add queue characterization for GLOBAL FIFO, FAIR round-robin,
  reply-capacity blocked heads, stale retry at head, cancellation, drain, and
  removal of empty per-key state.
- [ ] Add backlog characterization proving task/byte reservation is all-or-none,
  waiter callbacks run once, cancellation suppresses callbacks, and shutdown
  wakes all waiters.
- [ ] Add a failing architecture guard that rejects the Stage 2 deletion set:
  `ExecutorKeyState`, `ExecutorKeyStateProvider`, `ExecutorBackpressureIo`,
  `ExecutorBackpressureRuntime`, `ExecutorBackpressureObserver`, buffered reply
  methods on `ExecutionIoAdapter`, and `RespProtocolErrorReplyHandler`.
- [ ] Run the focused RED suite and record that only the final-tree guard fails:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-tests/yierdis-architecture-tests -am -Dtest=ExecutorTaskQueueTest,ExecutorBacklogBudgetTest,ReplyCapacityBlockedSchedulingTest,ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## Task 2: Delete The Unreachable Buffered Reply Path

**Files:**

- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionIoAdapter.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionIoAdapter.java`
- Modify executor test adapters under `yierdis-server-executor/src/test`.

- [ ] Remove `newReplySink`, `writeBufferedReply`, and `flushPending` from
  `ExecutionIoAdapter` and all implementations.
- [ ] Remove `handleExecutionFailure`, touched-connection collection, and drain
  flush plumbing. Keep registered-slot internal-error and result-unknown paths.
- [ ] Ensure `execute(task)` still distinguishes completed/closed, capacity
  blocked, and stale reprepare without changing cleanup ownership.
- [ ] Run executor and Netty adapter tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=CommandExecutorTest,ReplyCapacityBlockedSchedulingTest,NettyExecutionAdapterIntegrationTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [ ] Review and commit:

```bash
git commit -m "refactor: remove buffered executor reply path"
```

## Task 3: Make Backlog Reservation One Locked State

**Files:**

- Rewrite: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudget.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorBacklogBudgetTest.java`
- Modify: `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/ExecutorAdmissionTest.java`

- [ ] Replace separate atomic task/byte reservation with one private lock and
  primitive counters. Preserve existing metrics and watermark formulas.
- [ ] Make `tryReserve(retainedBytes)` validate both limits and update both
  counters in one critical section; reject underflow rather than silently
  clamping corrupt counters.
- [ ] Store waiters in one deque. Detach eligible/cancelled/shutdown waiters
  under the lock and invoke callbacks outside it.
- [ ] Run backlog, admission, backpressure, and ingress lifecycle tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=ExecutorBacklogBudgetTest,ExecutorAdmissionTest,CommandExecutorBackpressureTest,RespIngressLifecycleIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [ ] Review and commit:

```bash
git commit -m "refactor: simplify executor backlog reservations"
```

## Task 4: Consolidate GLOBAL And FAIR Queue State

**Files:**

- Rewrite: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorTaskQueue.java`
- Delete: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorKeyState.java`
- Delete: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorKeyStateProvider.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- Modify: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutor.java`
- Modify/create tests from Task 1.

- [ ] Give `ExecutorTaskQueue` one private lock, a standard global deque, and an
  identity-keyed private FAIR state map. Do not expose queue state through the
  connection context.
- [ ] Preserve GLOBAL FIFO and its single blocked head.
- [ ] Preserve FAIR per-key FIFO, round-robin scheduling, one blocked head per
  key, ready wakeup, retry-at-head, and later-command exclusion while blocked.
- [ ] Remove empty FAIR states and prove the queue does not retain inactive
  connection keys.
- [ ] Delete key-state APIs and the unchecked cast in `CommandExecutor`.
- [ ] Run queue, fairness, reply-capacity, executor, and shutdown tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=ExecutorTaskQueueTest,CommandExecutorFairSchedulingTest,ReplyCapacityBlockedSchedulingTest,CommandExecutorTest,YierdisServerBootstrapCloseTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [ ] Review and commit:

```bash
git commit -m "refactor: consolidate executor queue state"
```

## Task 5: Collapse Backpressure Projection Interfaces

**Files:**

- Rewrite: `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutorBackpressureController.java`
- Delete: `ExecutorBackpressureIo.java`
- Delete: `ExecutorBackpressureRuntime.java`
- Delete: `ExecutorBackpressureObserver.java`
- Modify: `CommandExecutor.java`
- Modify tests in `yierdis-server-executor`.

- [ ] Parameterize the controller with `C extends ExecutionConnection`, accept
  `ExecutionIoAdapter<C>`, and read `connection.context()` directly.
- [ ] Move global enter/exit counters into the controller while preserving
  per-connection counters in `ExecutionConnectionContext`.
- [ ] Delete anonymous projection adapters from `CommandExecutor`.
- [ ] Preserve the disabled-connection tracking set, close-listener removal,
  owner-thread global recovery, and all independent pause checks.
- [ ] Run backpressure, executor, ingress, and transport-writability tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=CommandExecutorBackpressureTest,CommandExecutorTest,RespIngressLifecycleIntegrationTest,NettyExecutionAdapterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [ ] Review and commit:

```bash
git commit -m "refactor: simplify executor backpressure state"
```

## Task 6: Unify Ingress Attempts And Remove The Detached Protocol Handler

**Files:**

- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyExecutionRequestIngress.java`
- Modify: `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- Delete: `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolErrorReplyHandler.java`
- Delete: its isolated unit test.
- Modify: `ClosingSkipSideEffectsIntegrationTest.java` to use the production
  ingress path for any still-required characterization.

- [ ] Implement one attempt-classification helper shared by initial submission
  and capacity-wait retry. Keep the pending deque and retry its head.
- [ ] Preserve request-too-large error text, stopped/closing rejection,
  capacity waiter cancellation, protocol error ordering, and terminal close.
- [ ] Delete unused initializer protocol-close helpers.
- [ ] Delete `RespProtocolErrorReplyHandler`; do not leave a compatibility alias.
- [ ] Run decoder, ingress admission/lifecycle/pressure/fuzz, ordered reply, and
  closing tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-integration-tests -am -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest,RespIngressLifecycleIntegrationTest,RespIngressPressureTest,RespIngressFuzzTest,OrderedReplyPipelineTest,ClosingSkipSideEffectsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [ ] Review and commit:

```bash
git commit -m "refactor: simplify Netty execution ingress"
```

## Task 7: Lock Architecture, Update Docs, And Verify Stage 2

**Files:**

- Modify: `ArchitectureBoundaryTest.java`
- Modify relevant files under `docs/project-docs`.
- Finalize this plan.

- [ ] Require the final executor/network tree and reject all deleted types and
  methods.
- [ ] Document the one queue owner, atomic backlog reservation, direct
  backpressure boundary, and one ordered protocol/command ingress path.
- [ ] Prove forbidden symbols are absent:

```bash
rg -n "ExecutorKeyState|ExecutorKeyStateProvider|ExecutorBackpressureIo|ExecutorBackpressureRuntime|ExecutorBackpressureObserver|RespProtocolErrorReplyHandler|newReplySink|writeBufferedReply|flushPending" yierdis-server yierdis-networking yierdis-tests --glob '*.java' --glob '!ArchitectureBoundaryTest.java'
```

- [ ] Prove Stage 2 did not edit DB/FFM or POMs:

```bash
git diff --exit-code 935c2417 -- ':(glob)**/pom.xml'
git diff --name-only 935c2417 -- yierdis-db yierdis-memory
```

- [ ] Run affected reactors:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main,yierdis-tests/yierdis-architecture-tests,yierdis-tests/yierdis-integration-tests -am test
```

- [ ] Run the full suite:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

- [ ] Verify production Java reduction:

```bash
git diff --numstat 935c2417 -- ':(glob)**/src/main/java/**/*.java' | awk '{ added += $1; deleted += $2 } END { printf "added=%d deleted=%d net=%d\n", added, deleted, added-deleted; exit !(deleted > added) }'
```

- [ ] Run `git diff --check`, obtain independent review, close findings, and
  commit:

```bash
git commit -m "refactor: complete networking executor simplification"
```

- [ ] Confirm clean status and focused Stage 2 commit history.

