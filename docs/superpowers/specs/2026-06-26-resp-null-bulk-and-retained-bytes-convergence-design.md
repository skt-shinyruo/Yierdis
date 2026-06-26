# RESP Null Bulk And Retained Bytes Convergence Design

## Status

Design approved in conversation on 2026-06-26.

## Goal

Make the RESP request path, execution request contract, and command-layer validation agree on two boundary rules:

- RESP array requests may carry `null bulk string` arguments through to `ExecutionRequest`.
- All `ByteArrayExecutionRequest` construction paths must report stable, non-negative, saturated `retainedBytes()`.

This change removes the current mismatch where RESP rejects `$-1`, the execution API claims null arguments are allowed, and large request byte accounting can overflow and bypass queue byte budgets.

## Problem

The current request path has two correctness problems and one boundary drift:

1. `ByteArrayExecutionRequest` computes `retainedBytes` with plain `int` addition in `copyOf(List<byte[]>)`, `copyOf(ExecutionRequest)`, and `fromUtf8(...)`. Large requests can overflow to a negative value. Executor queue byte budgeting and transaction queue byte budgeting both normalize negative values to `0`, which can undercount very large requests and let them bypass configured byte limits.

2. `ExecutionRequest` documents that argv elements may be `null` to represent `null bulk string`, but the RESP entry path does not actually support that shape. `RespRequestDecoder` rejects bulk length `< 0`, and `RespCommandRequest` rejects `null` argv elements.

3. The command layer already contains a null-argument gate, but today it behaves like a defensive implementation detail rather than the explicit business rule for accepted nulls. That leaves the protocol layer, execution contract, and command layer describing different realities.

These issues make the code harder to reason about and create real capacity-risk under extreme request sizes.

## Scope

This design covers:

- RESP array decoding of `null bulk string` arguments.
- `RespCommandRequest` null argv support.
- `RespExecutionAdapter` propagation of null argv into `ExecutionRequest`.
- Saturated `retainedBytes` accounting in all `ByteArrayExecutionRequest` constructors/factories.
- Command-layer null-argument policy as the single semantic gate.
- Contract tests, protocol tests, command tests, and Netty/integration tests for the above behavior.
- Documentation and comments that currently describe the old or inconsistent behavior.

## Non-Goals

- Do not change the protocol-to-execution architectural boundary. `RespCommandRequest` must still be adapted into `ExecutionRequest` before the command layer.
- Do not redesign request ownership to force deep copies on the hot path. The existing `wrapReadOnly` fast path remains.
- Do not make null arguments broadly accepted by all commands. The default remains rejection.
- Do not add null support to inline command parsing. Only RESP array bulk arguments can carry `null bulk string`.
- Do not change existing command semantics other than making RESP `$-1` reach the existing command-layer null policy.

## Design

### Protocol decoding

`RespRequestDecoder` should accept RESP bulk length `-1` as a legal null bulk string in array requests.

The array decoding rule becomes:

- `len == -1`: store `argv[i] = null`, do not read payload bytes, do not add to `retainedBytes`
- `len < -1`: protocol error
- `len >= 0`: existing payload read path

This change applies only to RESP arrays. Inline commands remain text-only and never synthesize null arguments.

The decoder remains responsible only for protocol-shape fidelity and protocol limits. It should not apply command-specific null policy.

### RESP DTO behavior

`RespCommandRequest` should allow `null` argv elements because RESP arrays can now legally contain null bulk strings.

Required semantics:

- `copyOf(List<byte[]>)` preserves `null` elements instead of rejecting them.
- `wrapReadOnly(byte[][], int)` preserves `null` elements instead of rejecting them.
- `readOnlyArg(int)` returns `null` when the stored argv element is null.
- `retainedBytes` continues to count only non-null byte lengths and continues to saturate at `Integer.MAX_VALUE`.

`RespCommandRequest` remains a protocol DTO with read-only-by-convention access. This design does not strengthen the ownership model beyond today’s outer-array copy and inner-array sharing convention.

### Execution request accounting

`ByteArrayExecutionRequest` must use one shared saturated-add helper for all retained-byte accounting.

The helper contract:

- add only non-null argument lengths
- clamp negative inputs to zero defensively
- saturate at `Integer.MAX_VALUE`

The following factory paths must all use that helper:

- `copyOf(List<byte[]>)`
- `copyOf(ExecutionRequest)`
- `fromUtf8(String, List<String>)`

After this change, every `ByteArrayExecutionRequest` instance, regardless of source, must satisfy:

- `retainedBytes() >= 0`
- `retainedBytes()` is stable for the request lifetime
- `retainedBytes()` saturates instead of overflowing

This keeps executor queue byte budgeting, transaction queue byte budgeting, replay snapshots, and `ExecutionRecord` snapshots on one accounting rule.

### Command-layer null policy

`YierdisFastCommandProcessor` becomes the single semantic gate for null command arguments.

Policy:

- `argv[0]` must still be present and non-empty
- by default, any null argv element from `argv[1]` onward is rejected with `ERR Protocol error: null bulk string`
- the only allowed null argument cases remain the existing single-message forms of `PING` and `ECHO`

This keeps the current business behavior while making RESP more faithful. Protocol adaptation now carries nulls honestly; command semantics decide whether they are legal.

`TransactionQueuePolicy` should continue to rely on normal command parsing and execution policy. Because null rejection remains in `YierdisFastCommandProcessor` before queueing, null-bearing commands that are not allowed must fail before being enqueued under `MULTI`.

### Documentation and contract convergence

The affected source comments and docs should be updated so they describe the real steady state:

- RESP arrays may carry `null bulk string` into `ExecutionRequest`
- inline commands do not
- `ExecutionRequest` null argv support is a real supported contract, not dead documentation
- command-layer null rejection is an explicit semantic policy, not merely an NPE guard
- `retainedBytes()` is expected to saturate instead of overflow

Project docs that currently describe RESP request objects as always non-null argv should be updated if they are touched by the implementation.

## Behavior Summary

After the change:

- `*2\r\n$4\r\nECHO\r\n$-1\r\n` decodes successfully to an `ExecutionRequest` with `argc() == 2` and `isNull(1) == true`
- `ECHO` with a null message still succeeds because it is explicitly allowed
- `SET key $-1` reaches the command layer and is rejected there as `ERR Protocol error: null bulk string`
- `*2\r\n$4\r\nECHO\r\n$-2\r\n` remains a protocol error
- executor and transaction byte budgets see saturated retained-byte counts instead of overflowed negatives

## Testing Strategy

### Protocol tests

Add or update tests around `RespRequestDecoder` and RESP request adaptation to cover:

- RESP array with `null bulk string` argument decodes successfully
- RESP bulk length `< -1` remains a protocol error
- mixed null/non-null argv reports retained bytes for non-null args only
- inline command parsing continues to produce only non-null argv

### RESP DTO and execution contract tests

Add or update tests for:

- `RespCommandRequest.copyOf(...)` preserving null argv elements
- `RespCommandRequest.wrapReadOnly(...)` preserving null argv elements
- `RespExecutionAdapter` preserving null argv into `ExecutionRequest`
- `ByteArrayExecutionRequest.copyOf(List<byte[]>)` saturated retained-byte accounting
- `ByteArrayExecutionRequest.copyOf(ExecutionRequest)` saturated retained-byte accounting
- `ByteArrayExecutionRequest.fromUtf8(...)` saturated retained-byte accounting

The retained-byte tests should explicitly cover overflow scenarios and assert `Integer.MAX_VALUE`.

### Command-layer tests

Add or update tests for:

- `PING` with a null message argument is accepted
- `ECHO` with a null message argument is accepted
- a representative non-whitelisted command such as `SET key null` is rejected with `ERR Protocol error: null bulk string`
- empty command and null command-name behavior remains unchanged

Add a `MULTI` coverage case proving that a null-bearing non-whitelisted command is rejected before queueing.

### Netty / integration tests

Add or update end-to-end tests proving:

- RESP `$-1` survives `RespRequestDecoder -> RespCommandAdapter -> RespExecutionAdapter -> YierdisFastCommandHandler -> command processor`
- the allowed `ECHO null` or `PING null` path produces the expected wire reply
- a disallowed null-bearing command produces the expected error reply from normal command execution

Add byte-budget tests where practical to show that extreme retained-byte values no longer become negative and do not bypass executor or transaction queue byte caps.

## Risks

- RESP null support changes a previously rejected wire shape into a valid decoded request. Tests must verify that the only semantic change is that null-bearing requests now fail or pass in the command layer according to policy.
- The retained-byte fix touches queue budgeting, replay snapshots, and helper constructors. Missing one factory path would preserve inconsistent accounting.
- Some existing tests may implicitly assume `RespCommandRequest` cannot contain nulls. Those tests need to be updated to the new contract rather than deleted.
- The current read-only fast path still relies on convention rather than enforced immutability. This design keeps that trade-off; it does not solve the broader ownership-hardening problem.

## Acceptance Criteria

- RESP array requests accept bulk length `-1` as a null bulk string and reject lengths `< -1`
- `RespCommandRequest` and `RespExecutionAdapter` preserve null argv elements correctly
- `ByteArrayExecutionRequest.retainedBytes()` never overflows negative on any construction path and saturates at `Integer.MAX_VALUE`
- Executor queue byte budgeting and transaction queue byte budgeting no longer rely on overflowed negative retained-byte values
- Default command behavior still rejects null arguments except for explicitly whitelisted commands
- Source comments and tests consistently describe the new steady state
