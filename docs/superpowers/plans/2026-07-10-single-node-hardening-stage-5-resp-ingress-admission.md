# RESP Ingress Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound receive-buffer, partial, decoded, and transaction-queued RESP request memory across connections before each controllable allocation, hold that admission lease until rejection or the last retained request consumer releases it, and provide the decoder handoff gate Stage 7 uses to register ordered reply slots before downstream emission.

**Architecture:** Add a transport-neutral reference-counted request lease to `server-api` and a Netty-owned global FIFO budget. Before Netty issues an application read, the connection acquires one fixed receive credit and a one-buffer read cycle; retained ByteBuf capacity and component metadata remain charged until actual release. An accounted cumulator pre-admits consolidation peaks. The RESP decoder reserves argv and payload copies before allocation, then passes each complete request or terminal protocol error through a default-pass-through decoded-message gate before downstream emission. The decoded lease keeps only a lightweight connection-account token, never Netty channel/decoder/buffer objects, and may be released from the command owner, MULTI queue, Stage 6 sink worker, or Stage 7 reply-slot handoff. Temporary global pressure pauses only the waiting connection and resumes on its event loop; hard-limit violations send the documented error and close.

**Tech Stack:** Java 25, Maven, JUnit 4, Netty 4.1, existing single-owner executor and connection context, Stage 4 observability conventions.

## Global Constraints

- Execute after Stage 4 full verification, including both `MaxmemoryScopeTest` regressions, passes.
- Preserve existing protocol max option names and meanings.
- Add optional `--protocolGlobalInFlightBytes`; `0` derives `max(128 MiB, saturatedMultiply(executorQueueMaxBytes, 2))`.
- Per-connection hard limit is `protocolMaxCommandBytes + 48 + protocolMaxArgs * 32`, using saturating arithmetic.
- Reserve `16 + argc * 8` before creating `byte[argc][]`; before each `new byte[len]`, reserve `16 + align8(len)`; reserve the fixed 32-byte request estimate before emitting the request.
- Count retained inbound buffer capacity plus component metadata, argv/reference overhead, copied bulk/inline payloads, parser storage, and every retained view of the live execution request until the final view closes.
- Global pressure begins at 75% and clears at 50%; waiters are FIFO.
- If a declaration fits hard limits but global capacity is temporarily unavailable, do not allocate or reject; pause and wait once.
- Acquire a fixed read credit before each one-buffer Netty read cycle; an unexpected larger upstream buffer must reserve the difference or be rejected.
- Use an accounted composite cumulator: release only fully consumed components and pre-admit any consolidation target before allocation.
- Executor queued bytes and inbound bytes remain separate metrics and are never summed as distinct physical memory.
- Every release path is idempotent: normal completion, MULTI EXEC/DISCARD, executor rejection, closing-command skip, protocol error, reset, disconnect, and server shutdown.
- Decoder handoff emits no complete request/protocol error until `RespDecodedMessageGate` admits it; WAITING retains exactly one already-accounted message, stops the decode loop, and emits no later message from the same input.
- A decoded request or retained event must not keep a `Channel`, `ChannelHandlerContext`, decoder, cumulator, or `ByteBuf` reachable.
- Every Maven/Java command uses the explicit JDK 25 prefix.

---

## File Structure

Create:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/RequestMemoryLease.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ReferenceCountedRequestMemoryLease.java`
- `yierdis-server/yierdis-server-api/src/test/java/yier/bubu/redis/execution/api/ReferenceCountedRequestMemoryLeaseTest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundMemoryBudget.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundMemoryBudgetStats.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundConnectionMemory.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/ConnectionMemoryAccount.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RetainedRespExecutionRequest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundByteAccountingHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/InboundReadCreditHandler.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/AccountedRespCumulator.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespDecodedMessageGate.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/InboundMemoryBudgetTest.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespIngressAdmissionTest.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespDecodedMessageGateTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/RespIngressLifecycleIntegrationTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/RespIngressPressureTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/protocol/RespIngressFuzzTest.java`

Modify:

- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRequest.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ByteArrayExecutionRequest.java`
- `yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java`
- `yierdis-networking/yierdis-networking-netty/pom.xml`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoder.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespProtocolError.java`
- `yierdis-networking/yierdis-networking-netty/src/test/java/yier/bubu/redis/protocol/resp/netty/RespRequestDecoderTest.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorSubmitter.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorExecutionSupport.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutorDrainLoop.java`
- `yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/ExecutionConnectionContext.java`
- `yierdis-server/yierdis-server-executor/src/test/java/yier/bubu/redis/execution/executor/CommandExecutorTest.java`
- `yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java`
- `yierdis-server/yierdis-server-core/src/test/java/yier/bubu/redis/execution/engine/EngineSessionTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerChannelInitializer.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgNames.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerArgs.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/args/YierdisServerRuntimeConfig.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/args/YierdisServerArgsTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/YierdisServerBootstrapCommandWiringTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/ClosingSkipSideEffectsIntegrationTest.java`
- `yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TransactionQueueCleanupTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

Delete:

Delete these files only after the direct `ExecutionRequest` pipeline tests are green:

- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespCommandRequest.java`
- `yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespExecutionAdapter.java`
- `yierdis-networking/yierdis-networking-resp/src/test/java/yier/bubu/redis/protocol/resp/RespExecutionAdapterTest.java`
- `yierdis-networking/yierdis-networking-netty/src/main/java/yier/bubu/redis/protocol/resp/netty/RespCommandAdapter.java`

## Stable Interfaces Produced By This Stage

```java
public interface RequestMemoryLease extends AutoCloseable {
    RequestMemoryLease NOOP = new NoopRequestMemoryLease();

    long reservedBytes();
    boolean released();
    RequestMemoryLease retain();

    @Override
    void close();
}
```

`ExecutionRequest` adds:

```java
default long admittedMemoryBytes() { return retainedBytes(); }
default ExecutionRequest retain() { return ByteArrayExecutionRequest.copyOf(this); }
```

`RetainedRespExecutionRequest.retain()` shares immutable argv and retains its lease; every retained reference must call `close()` exactly once. Stage 6 relies on this method to keep a committed event without copying argv. Lease views are safe to close from any thread; the final close updates the global budget under its lock and schedules waiter callbacks on their event loops.

Both heap and Netty-backed request implementations use this exact saturated retained-memory estimate:

```text
32 request bytes
  + 16 outer argv bytes
  + argc * 8 reference bytes
  + sum(16 + align8(argumentLength)) for every non-null argument
```

`retainedBytes()` remains payload-only `int` scheduling compatibility. `ByteArrayExecutionRequest.admittedMemoryBytes()` returns the full estimate above. `RetainedRespExecutionRequest.admittedMemoryBytes()` returns its lease's full reservation, which uses the same formula plus any still-owned frame/offset storage rather than double-counting payload.

```java
public final class InboundMemoryBudget implements AutoCloseable {
    public InboundMemoryBudget(long capacityBytes);
    public ReservationResult tryReserve(InboundConnectionMemory connection, long bytes);
    public ReservationResult tryTransfer(
            InboundConnectionMemory connection,
            long newBytes,
            long inputCapacityReleasedAfterCopy
    );
    public void cancelWaiter(InboundConnectionMemory connection);
    public InboundMemoryBudgetStats stats();

    public enum ReservationResult { RESERVED, WAITING, REQUEST_LIMIT, CLOSED }
}
```

```java
public record InboundMemoryBudgetStats(
        long capacityBytes,
        long reservedBytes,
        int waitingConnections,
        boolean backpressured,
        long rejectedConnections,
        long peakReservedBytes,
        long readCreditBytes,
        long retainedInputCapacityBytes,
        long consolidationBytes,
        boolean closed
) {}
```

The decoder handoff extension is Netty-local and default-compatible:

```java
public interface RespDecodedMessageGate {
    RespDecodedMessageGate PASS_THROUGH = (ctx, decoded, resume) ->
            Admission.admitted(decoded);

    Admission tryAdmit(
            ChannelHandlerContext ctx,
            Object decoded,
            Runnable resumeOnEventLoop
    );

    enum Status { ADMITTED, WAITING, CLOSED }

    record Admission(Status status, Object forwardedMessage) {
        public static Admission admitted(Object forwardedMessage);
        public static Admission waiting();
        public static Admission closed();
    }
}
```

`RespRequestDecoder` retains one `pendingDecodedMessage` only after the message's inbound allocations are fully admitted. `ADMITTED` emits exactly `forwardedMessage`; Stage 7 may replace the raw message with a slot-bearing wrapper. `WAITING` emits nothing, stops `callDecode`, and accepts one event-loop callback that retries the same message without a fake `ByteBuf`. `CLOSED` releases the decoded message and enters terminal state. The existing constructor delegates to `PASS_THROUGH`, so Stage 5 wire behavior is unchanged.

---

### Task 1: Add Reference-Counted Request Leases

**Interfaces:** Produces `RequestMemoryLease` and retainable `ExecutionRequest` behavior.

- [ ] **Step 1: Add failing lease lifecycle tests**

```java
@Test
public void retainedReferencesReleaseBudgetExactlyOnceAtZero() {
    AtomicLong released = new AtomicLong();
    RequestMemoryLease first = new ReferenceCountedRequestMemoryLease(123, released::addAndGet);
    RequestMemoryLease second = first.retain();
    first.close();
    first.close();
    Assert.assertEquals(0L, released.get());
    second.close();
    Assert.assertEquals(123L, released.get());
    Assert.assertTrue(first.released());
}

@Test
public void retainAfterFinalReleaseFails() {
    RequestMemoryLease lease = new ReferenceCountedRequestMemoryLease(1, ignored -> {});
    lease.close();
    Assert.assertThrows(IllegalStateException.class, lease::retain);
}
```

- [ ] **Step 2: Run and observe missing lease types**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api -am -Dtest=ReferenceCountedRequestMemoryLeaseTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the types do not exist.

- [ ] **Step 3: Implement atomic reference counting**

Reject negative reservation bytes. `retain()` increments only while count is positive; `close()` decrements once per lease view and invokes the release callback only on the transition to zero. `NOOP.retain()` returns itself and close does nothing. Update `ByteArrayExecutionRequest` to share an immutable owned argv holder across retained views, release only its NOOP lease, keep payload-only `retainedBytes()`, and return the full saturated formula from `admittedMemoryBytes()`.

- [ ] **Step 4: Run server-api tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-api -am -Dtest=ReferenceCountedRequestMemoryLeaseTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server/yierdis-server-api
git commit -m "feat: add retainable request memory leases"
```

Expected: PASS with one final release callback and no refcount underflow.

### Task 2: Implement The Global FIFO Inbound Budget

**Interfaces:** Produces `InboundMemoryBudget`, atomic transfer admission, stats, and one waiter per `InboundConnectionMemory`.

- [ ] **Step 1: Add capacity, hysteresis, FIFO, and close tests**

Create three fake connections with a 100-byte budget. Reserve 75 and assert backpressure; queue requests of 20 from B and C; release 25 and assert B is scheduled first but C remains waiting; release another 20 and assert C is scheduled. Queue an 80-byte head while backpressured, release usage to 50, and assert that head is reserved and resumed even though it immediately reenters backpressure; it must not starve behind the 75% watermark. Close C before release and assert it is removed without consuming a wakeup. Assert a single 101-byte request returns `REQUEST_LIMIT` rather than waiting. With global capacity 200 and connection hard limit 100, reserve 90 input bytes then transfer to 80 output bytes with 90 release credit; assert admission succeeds, the measured temporary global peak is 170, and final connection usage is 80 after input release. Repeat with global capacity 160 and assert it waits without allocating despite the projected connection total fitting. Close a budget with one active lease: waiters become zero and new reservations return CLOSED, while reserved bytes remain until the active lease closes from another thread; then assert reserved bytes reach zero without underflow.

- [ ] **Step 2: Run and verify missing budget**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=InboundMemoryBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement owner-safe reservation accounting**

Use a lock around capacity, reservations, and `ArrayDeque<Waiter>`; never run a channel callback while holding the lock. `ConnectionMemoryAccount` contains only an id, hard limit, reserved-byte counter, and closed flag. `InboundConnectionMemory` owns the event-loop callback/context separately and drops those references on channel close. A waiter stores the account, requested bytes, post-copy input-capacity release credit, and one resume callback. A request larger than either its connection hard limit or the global capacity returns `REQUEST_LIMIT` and is never queued. `tryTransfer` checks the connection hard limit against `connectionReserved - inputCapacityReleasedAfterCopy + newBytes`, but checks global capacity against the real temporary peak `globalReserved + newBytes`. It reserves the new bytes first; the decoder releases only credited component capacities after copying succeeds and those components are actually removed. A successful reservation may cross the 75% high watermark and then enters backpressure. While backpressured, new requests queue even when they fit the absolute capacity. Release clears backpressure only at or below 50%, then considers the strict FIFO head. If the head fits absolute capacity, reserve it even when that one reservation crosses 75%, reenter backpressure, and stop waking more waiters; this prevents requests in the 75%-100% size range from starving. If the head does not fit current capacity, never skip it to serve a later request. Reserve each awakened request before scheduling and invoke callbacks afterward on their supplied event executors. `close()` marks admission closed and cancels waiters but does not fabricate release of active request leases; those counters reach zero when their final views close. Saturate all additions.

- [ ] **Step 4: Run budget tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=InboundMemoryBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-networking/yierdis-networking-netty
git commit -m "feat: bound global inbound request memory"
```

Expected: PASS for capacity, FIFO, hysteresis, transfer-peak, and late-release cases.

### Task 3: Pre-Admit Socket Buffers And Account Cumulation Capacity

**Interfaces:** `InboundReadCreditHandler` reserves one fixed receive-buffer credit before `ctx.read()`; `InboundByteAccountingHandler` converts that credit to a retained-capacity component lease; `AccountedRespCumulator` releases charges only with actual component release and pre-admits consolidation.

- [ ] **Step 1: Add fragmented-input and disconnect tests**

Configure a fixed receive capacity and one message per read cycle. Assert no `ctx.read()` occurs until its credit is reserved, one in-flight read cannot allocate a second buffer, and a one-readable-byte partial component remains charged at full retained capacity plus component overhead. Write half of `*2\r\n$4\r\nPING\r\n$1048576\r\n`, close the channel, run pending tasks, and assert read-credit, component, global, and connection counters reach zero. Feed enough one-byte components to hit the cumulator limit; assert consolidation first reserves the allocator-selected target capacity, observes the old-plus-new peak, then releases old component leases. Add an unexpected buffer-larger-than-credit failure that emits exactly one request-memory error, releases the buffer, and closes.

- [ ] **Step 2: Run and observe partial bytes are unaccounted**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=RespIngressAdmissionTest#fragmentedInputIsAccountedAndReleasedOnDisconnect+unreservableInboundBufferIsRejected -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Install composite cumulation**

Configure a fixed receive allocator and `maxMessagesPerRead(1)`, keep automatic reads disabled, and let `InboundReadCreditHandler` call `ctx.read()` only after reserving one full buffer capacity plus conservative wrapper/component overhead. On `channelRead`, convert that credit to an `InboundBufferLease` charged to the actual retained root-buffer capacity plus component overhead, immediately release any unused credit, and reserve any excess before `ctx.fireChannelRead`; a derived unexpected buffer is charged conservatively to the retained root allocation. `AccountedRespCumulator` owns ordered component leases, discards and closes only fully consumed components, and keeps a partial component's whole retained allocation charged. It must replace Netty's implicit max-component consolidation: calculate the allocator-selected target capacity, use `tryTransfer` to reserve the full copying peak, allocate/copy, then close old components. The decoder may calculate which complete components will be released after a copy and use only that exact capacity as transfer credit; reader-index movement alone releases zero. `handlerRemoved`, `channelInactive`, protocol terminal state, and decoder reset close read credit, component leases, partial parser/output leases, and the waiter.

- [ ] **Step 4: Run Netty leak-sensitive tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dio.netty.leakDetection.level=paranoid -Dtest=RespIngressAdmissionTest,RespRequestDecoderTest,RespProtocolErrorReplyHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS and no leak report.

- [ ] **Step 5: Commit input accounting**

```bash
git add yierdis-networking/yierdis-networking-netty
git commit -m "fix: pre-admit and account RESP receive buffers"
```

Expected: PASS and the credited socket/cumulation admission commit succeeds.

### Task 4: Reserve Argv And Bulk Arrays Before Allocation

**Interfaces:** Decoder emits `RetainedRespExecutionRequest` directly through `RespDecodedMessageGate`; reservation state is `READ_COMMAND`, `WAITING_FOR_ARGV`, `READ_ARRAY_BODY`, `WAITING_FOR_BULK`, `WAITING_FOR_HANDOFF`, or `CLOSING`.

- [ ] **Step 1: Add huge argc and declared bulk tests**

```java
@Test
public void argvIsRejectedBeforeOuterArrayAllocation() {
    Harness h = harness(1_024, 1_000_000, 16 * 1024, 16 * 1024);
    Object reply = h.write("*1000000\r\n");
    Assert.assertEquals("ERR request exceeds configured memory limit", protocolError(reply).message());
    Assert.assertEquals(0L, h.budget().stats().reservedBytes());
}

@Test
public void declaredBulkWaitsWithoutAllocatingBodyArray() {
    Harness h = harness(64, 16, 1_024, 1_024);
    h.exhaustGlobalBudgetExcept(8);
    h.write("*1\r\n$32\r\n");
    Assert.assertEquals("WAITING_FOR_BULK", h.decoderState());
    Assert.assertEquals(0, h.allocatedBulkArrays());
    Assert.assertFalse(h.channel().config().isAutoRead());
}

@Test
public void decodedRequestWaitsAtGateWithoutEmittingLaterPipelineMessages() {
    RecordingGate gate = new RecordingGate(WAITING);
    Harness h = harnessWithGate(gate);
    h.write("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
    Assert.assertEquals("WAITING_FOR_HANDOFF", h.decoderState());
    Assert.assertEquals(0, h.readInboundMessages());
    Assert.assertEquals(1, gate.attempts());
}
```

- [ ] **Step 2: Run and verify current decoder allocates first**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because it executes `new byte[argc][]` at header parse and `new byte[len]` before admission.

- [ ] **Step 3: Implement pre-allocation admission**

Parse length into `long`, check protocol hard maxima, configured maxima, command cumulative bytes, and projected post-transfer connection hard limit. Use transfer admission for outer argv/reference bytes before creating the outer array, for `16 + align8(len)` before each payload array, and for the fixed request estimate before emission. For inline commands transfer line input into parser output argv/array overhead and decoded payload before copying. Transfer credit includes only complete cumulator component capacities that the successful copy/parse step will actually release; a partially consumed component remains fully charged. A hard-limit result emits the exact request-memory error; a waiting result records new bytes plus exact component-release credit once and leaves parser state after the declaration header without repeating admission.

- [ ] **Step 4: Emit a retained execution request**

`RetainedRespExecutionRequest` owns immutable argv and a request lease containing the exact full estimate above plus any separately retained frame/offset arrays. Its retained holder may reference `ConnectionMemoryAccount` but no Netty type or decoder state. Its `retainedBytes()` remains payload-only for executor scheduling compatibility; `admittedMemoryBytes()` returns the full lease reservation. Before appending a complete request or terminal `RespProtocolError` to decoder output, call the gate. ADMITTED appends only the returned wrapper/message. WAITING stores the original decoded object in the single pending field, stops the decode loop before parsing another command from the current cumulator, disables input through ingress pause ownership, and retries only through the provided event-loop callback. CLOSED releases an `ExecutionRequest`, enters terminal state, and emits nothing. Delete the RESP request/adapter intermediate path and update decoder tests to read `ExecutionRequest` directly under the default gate.

- [ ] **Step 5: Run decoder tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=RespRequestDecoderTest,RespIngressAdmissionTest,RespDecodedMessageGateTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-networking
git commit -m "fix: admit RESP arrays before allocation"
```

Expected: PASS with argv, payload, frame, and request allocations admitted before construction.

### Task 5: Resume FIFO Waiters On Their Event Loops

**Interfaces:** Inbound-budget and decoded-message-gate wakeups reserve their awaited capacity first, then schedule decoder resumption through the channel event loop without a fake input buffer.

- [ ] **Step 1: Add two-channel FIFO resumption tests**

Pause channels B then C on bulk reservations. Release capacity from A, run B's event-loop tasks, and assert B emits its request while C remains paused. Release B's completed request, run C's tasks, and assert C resumes. Close B before its turn and assert C receives the next wakeup. Separately return WAITING from the decoded-message gate for two complete pipelined requests in one input buffer; invoke its callback, switch it to ADMITTED with a wrapper sentinel, and assert only the first wrapped message is emitted before the decoder asks the gate about the second. Closing while WAITING releases the retained request lease and never invokes a stale callback.

- [ ] **Step 2: Run and verify no waiting state exists**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=InboundMemoryBudgetTest,RespIngressAdmissionTest,RespDecodedMessageGateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement event-loop resumption**

On wakeup, schedule a task with `ctx.executor().execute`. If active, mark the awaited reservation consumed, clear only the owning pause reason, and ask the decoder to retry either its pending allocation or its pending decoded-message gate from the existing cumulator without allocating a fake ByteBuf. Once parsing blocks on more socket data, acquire the next read credit and issue exactly one `ctx.read()`. If closed, release pre-reserved bytes, close a pending decoded request, and ask the relevant budget/gate to serve the next waiter. Register no more than one inbound-budget waiter, one gate waiter, or one outstanding read credit per connection.

- [ ] **Step 4: Coordinate ingress and executor autoRead ownership**

Keep transport `autoRead` disabled and track `inputPausedByIngress` separately from `inputPausedByExecutor`. Issue a credited manual read only when both flags are false, the channel is active/writable, no read is in flight, and budget policy allows the credit. Add both flags, read-credit state, and global wait count to connection/server stats.

- [ ] **Step 5: Run multi-channel tests and commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking/yierdis-networking-netty -am -Dtest=InboundMemoryBudgetTest,RespIngressAdmissionTest,RespDecodedMessageGateTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-networking yierdis-server
git commit -m "feat: resume RESP decoding after memory pressure"
```

Expected: PASS; waiters resume in FIFO order on their own event loops without duplicate reads.

### Task 6: Transfer Leases Through Executor Completion

**Interfaces:** Whoever accepts an `ExecutionRequest` owns one reference and must close it; rejection leaves ownership with the handler, which closes it after the reply decision.

- [ ] **Step 1: Add completion, rejection, and closing-skip tests**

For accepted execution, assert budget remains reserved while queued and reaches zero after execution when no retained view remains. Retain a request after channel close and assert the decoded request graph contains no Netty type while its lightweight account remains charged; closing the final retained view from a different thread releases the global and per-connection counters and schedules FIFO wakeups correctly. Enter MULTI, queue several commands, and assert each queued request keeps one lease view after its original executor view closes; EXEC releases each after replay unless Stage 6 retains a committed event, while DISCARD, transaction reset, disconnect, and shutdown release every queued view. For queue-full and bytes-budget rejections, assert the handler sends the existing busy reply and releases the request. For a connection marked closing before drain, assert the skipped task releases its lease without executing side effects.

- [ ] **Step 2: Run and locate missing releases**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=EngineSessionTest,CommandExecutorTest,RespIngressLifecycleIntegrationTest,TransactionQueueCleanupTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on one or more non-normal paths.

- [ ] **Step 3: Make ownership explicit**

`CommandExecutorSubmitter` does not close on accepted submission. `CommandExecutorExecutionSupport` closes in a `finally` after reply handling and backlog release. The transaction queue calls `retain()` exactly once when accepting a queued command and closes that view exactly once after replay, discard, reset, disconnect, or shutdown. Drain-leftover, skipped-closing, shutdown, and internal-failure paths use the same idempotent close helper. `YierdisFastCommandHandler` closes on every rejected submit, protocol terminal request, inactive connection, and exception before transfer. Do not assert that executor completion always releases the bytes: a transaction view or Stage 6 commit event may legitimately keep the shared lease charged.

- [ ] **Step 4: Run executor and server lifecycle tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-core,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am -Dtest=EngineSessionTest,CommandExecutorTest,CommandExecutorBackpressureTest,ClosingSkipSideEffectsIntegrationTest,RespIngressLifecycleIntegrationTest,TransactionQueueCleanupTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with budget zero after every test that leaves no retained request view.

- [ ] **Step 5: Commit lease transfer**

```bash
git add yierdis-server
git commit -m "fix: release inbound leases after request lifecycle"
```

Expected: PASS and the executor lease-transfer commit succeeds.

### Task 7: Add Configuration And Observability

**Interfaces:** Adds `protocolGlobalInFlightBytes` to args/runtime config and publishes inbound stats separately from executor queue stats.

- [ ] **Step 1: Add CLI derivation and round-trip tests**

Assert explicit `--protocolGlobalInFlightBytes 1048576` round-trips through copy, argv, and runtime config. Assert zero with queue bytes 64 MiB derives 128 MiB; queue bytes 80 MiB derives 160 MiB; a near-overflow queue value derives `Long.MAX_VALUE`.

- [ ] **Step 2: Run and verify the option is absent**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Wire one budget per server**

Add the arg constant, Picocli field default zero, non-negative validation, copy/toArgv/runtime record field, and derivation helper. `YierdisServerBootstrap` owns one budget and passes it to every channel initializer. On shutdown mark it closed after channels and executor stop accepting requests; cancel waiters immediately, but permit already-issued request/event lease views to release their counters later. Stage 6 orders normal shutdown so the commit stream drains before the final zero-leak assertion.

- [ ] **Step 4: Expose stats**

Add server INFO/STATS fields for inbound capacity, reserved, peak, waiters, backpressured state, rejected connections, and closed state. Keep executor queued bytes unchanged and do not add the two metrics together.

- [ ] **Step 5: Run config and wiring tests, then commit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-main -am -Dtest=YierdisServerArgsTest,ServerConfigArgsTest,YierdisServerBootstrapCommandWiringTest -Dsurefire.failIfNoSpecifiedTests=false test
git add yierdis-server
git commit -m "feat: configure and report inbound memory admission"
```

Expected: PASS with exact CLI round trips, derived defaults, and separate ingress statistics.

### Task 8: Stress Partial Requests And Fuzz Release Paths

**Interfaces:** Completes Stage 5 acceptance.

- [ ] **Step 1: Add multi-connection pressure integration**

Start with a 1 MiB global budget and open 32 connections. Send fragmented declared bulks without bodies until pressure engages. Assert every socket read had a prior credit, at most one credited buffer is in flight per connection, observed reserved bytes never exceed capacity, at least one connection pauses, completed/disconnected connections release component/request leases, and a final PING succeeds after pressure clears.

- [ ] **Step 2: Add deterministic protocol fuzz**

Generate 10,000 fixed-seed cases containing valid arrays, inline commands, malformed lengths/terminators, disconnects, and resets. Include one valid request delivered as 100,000 one-byte fragments so the accounted consolidation path is exercised repeatedly without an implicit Netty copy. After each case finish pending event-loop tasks and release outbound messages. Assert read credits, retained input capacity, consolidation bytes, total reserved bytes, and waiters return to zero; peak reserved bytes never exceed capacity, no negative counters occur, and Netty paranoid leak detection reports nothing.

- [ ] **Step 3: Run ingress stress suites**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dio.netty.leakDetection.level=paranoid -Dtest=RespIngressPressureTest,RespIngressFuzzTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 4: Run protocol, architecture, and full tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-networking,yierdis-server/yierdis-server-executor,yierdis-server/yierdis-server-main -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS.

- [ ] **Step 5: Commit Stage 5 acceptance**

```bash
git add yierdis-networking yierdis-server yierdis-tests
git commit -m "test: prove bounded RESP ingress memory"
```
