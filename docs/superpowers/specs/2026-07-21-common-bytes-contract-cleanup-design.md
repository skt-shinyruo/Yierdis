# Common Bytes Contract Cleanup Design

## Status

Approved for implementation on 2026-07-21.

## Context

`yierdis-common-bytes` currently provides five low-level byte contracts:
`BytesSource`, `BytesView`, `BytesSlice`, `BytesSink`, and
`DirectBytesSink`. The first four are used by request, command, storage,
native-value, RESP, and bounded reply paths. `DirectBytesSink` has only one
implementation, `NettyByteBufSink`, and neither type has a production caller.

The optional raw-address methods on `BytesSource` likewise have no active
consumer. The only non-default override is the reusable command argument slice,
whose current request implementations do not expose a frame with a memory
address. These APIs therefore publish lifetime-sensitive capabilities without
an exercised ownership, pinning, chunk-budget, or benchmark contract.

The current production reply path writes through `ReplyReservationSink` and
`BoundedChunkedReplySink`. It allocates bounded `ByteBuf` chunks only after
reply-capacity reservation. A single-buffer writer cursor and raw destination
address do not model that path.

`BytesView.getBytes(...)` also has an incomplete default range contract. It
returns before validating positions for zero-length copies, does not validate
the source range against `length()`, and checks `dstOff + len` with an
overflow-prone addition.

## Goals

- Keep the four byte contracts that have active cross-module consumers.
- Remove unused raw-address and direct-buffer APIs instead of maintaining a
  speculative fast path.
- Define synchronous consumption and short-lifetime ownership rules at the
  public Java API boundary.
- Make the default `BytesView` bulk-copy implementation validate source and
  destination ranges without integer overflow.
- Add focused unit tests in `yierdis-common-bytes` for the common contracts.
- Describe current behavior as delayed materialization and bounded streaming,
  without claiming a production zero-copy path.

## Non-Goals

- Do not introduce a replacement direct/off-heap fast-path API.
- Do not change RESP wire encoding, command semantics, reply budgeting, native
  allocation, or request ownership.
- Do not eliminate copies that are required by the current protocol, DB lookup,
  persistence, or reply implementations.
- Do not redesign `ExecutionRequest.frame()` or its argument offset contract.
- Do not add deprecation shims for repository-internal APIs that have no caller.

## API Decisions

### Retained interfaces

The retained hierarchy remains:

```text
BytesSource
  -> BytesView
       -> BytesSlice

BytesSink
```

`BytesSource` remains the minimal random-access read contract. It exposes
`getByte(int)` and `getBytes(int, byte[], int, int)`. It no longer exposes a raw
memory address.

`BytesView` remains a length-bounded, read-only, short-lived view. A caller may
read it only while the operation that received it is active. Code that queues,
shares across threads, persists, or otherwise retains the content beyond that
operation must first acquire independent ownership, normally by materializing a
new array or by using a domain-specific retained object.

`BytesSlice` remains a `BytesView` that can synchronously stream its content to
a `BytesSink`. `writeTo(...)` must complete consumption during the call; neither
side may use the call to transfer source ownership.

`BytesSink` remains the minimal byte-array write port. Implementations must
consume the selected bytes before `writeBytes(...)` returns and must not retain
or mutate the supplied array. The contract does not imply thread safety,
buffering strategy, zero-copy behavior, or ownership transfer.

### Removed interfaces and capabilities

Delete `DirectBytesSink` and `NettyByteBufSink`. Delete `hasMemoryAddress()` and
`memoryAddress()` from `BytesSource`, together with the unused overrides in the
command argument slice.

After deleting `NettyByteBufSink`, remove the direct `yierdis-common-bytes`
dependency from `yierdis-networking-netty` if no remaining production or test
source in that module imports the package. Transitive dependencies must not be
used to justify retaining an otherwise unused direct dependency.

No compatibility adapter is added. The deleted APIs are internal snapshot APIs,
have no current caller, and are not part of a released compatibility contract.

## Range And Error Semantics

`BytesView.getBytes(index, dst, dstOff, len)` keeps the established exception
categories while making range validation complete:

- `dst == null` throws `IllegalArgumentException`;
- `len < 0` throws `IllegalArgumentException`;
- invalid source or destination positions/ranges throw
  `IndexOutOfBoundsException`;
- valid zero-length ranges are accepted at `index == length()` and
  `dstOff == dst.length`;
- zero-length calls still validate both positions;
- source validation uses the view's current `length()`;
- validation must not rely on addition that can wrap an `int`.

After validation, the default implementation may continue copying through
`getByte(...)`. Hot-path implementations remain free to override
`getBytes(...)` with a bulk operation while preserving the same public
semantics.

The design does not globally normalize exception behavior of every existing
override. It establishes and tests the common default contract; consumer
implementations can be converged separately when a real inconsistency is found.

## Data Flow After Cleanup

The write-command path remains:

```text
ExecutionRequest argument
  -> command BytesSlice
  -> storage write API
  -> native or heap storage implementation
```

The reply path remains:

```text
heap/native BytesSlice
  -> RespReplyWriter.bulkString(...)
  -> ReplyReservationSink / BoundedChunkedReplySink
  -> bounded ByteBuf chunks
  -> Netty channel write
```

`NativeBytesSlice` may continue using its reusable heap scratch buffer during
synchronous output. This is bounded streaming, not a zero-copy guarantee.

## Tests

Add focused JUnit 4 tests under `yierdis-common-bytes/src/test/java`.

`BytesViewTest` covers:

- copying a valid subrange into a destination offset;
- accepting valid zero-length ranges at both ends;
- rejecting a negative length;
- rejecting null destination arrays;
- rejecting negative source and destination positions even when length is zero;
- rejecting source ranges beyond `length()`;
- rejecting destination ranges beyond the array;
- rejecting values such as `Integer.MAX_VALUE` without overflow bypass;
- proving the default method, rather than a custom override, is under test.

`BytesSinkTest` covers the existing whole-array convenience method:

- delegation with offset zero and the complete array length;
- rejection of a null source.

Reactor verification compiles all consumers after API removal. Focused tests
for RESP, command, DB memory, server main, and architecture boundaries verify
that the retained streaming data flow and dependency rules still hold.

## Documentation

Update current project documentation that names `DirectBytesSink`,
`NettyByteBufSink`, memory-address fast paths, or a production direct-buffer
reply chain. The revised text must distinguish:

- short-lived read views;
- synchronous `BytesSlice` streaming;
- deliberate heap/native materialization boundaries;
- current bounded chunked reply output;
- the absence of a zero-copy contract.

Historical approved specs and implementation plans remain historical records
and are not rewritten. Current navigation, glossary, architecture, copy-boundary,
and code-logic coverage documents are authoritative and must converge on the
new contract.

## Compatibility And Risk

The expected behavioral change is limited to stricter validation in the
default `BytesView.getBytes(...)`: invalid zero-length positions that previously
returned silently will now fail. Valid callers are unchanged.

The principal compile risk is an overlooked reference to a deleted type or
method. Repository-wide source search and reactor compilation address it. The
principal documentation risk is leaving a current document that still promises
direct or zero-copy behavior; a repository-wide terminology scan addresses it.

Removing speculative APIs intentionally gives up an unexercised extension
point. A future fast path must begin from an observed copy cost, integrate with
reply reservation and chunking, define source lifetime without unsafe raw
addresses, and include benchmarks proving the change.

## Success Criteria

- Production and test source contains no `DirectBytesSink`,
  `NettyByteBufSink`, `hasMemoryAddress()`, or `memoryAddress()` reference tied
  to the common bytes contracts.
- `yierdis-common-bytes` contains focused passing contract tests.
- All affected reactor modules compile and their focused tests pass on JDK 25.
- Current project documentation describes bounded streaming and explicit copy
  boundaries without claiming an active direct/zero-copy path.
- Existing unrelated worktree changes remain intact and are not included in
  task commits.
