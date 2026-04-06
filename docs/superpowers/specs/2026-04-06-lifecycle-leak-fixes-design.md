# Lifecycle Leak Fixes Design

**Goal:** Eliminate two lifecycle leaks: partial `YierdisInstance.create(...)` startup should clean up already-created resources, and `YierdisClient.connect(...)` should always shut down its temporary Netty event loop when connect/setup fails.

## Scope

This design only covers two failure-path fixes:

1. `YierdisInstance.create(...)`
   - If engine creation fails partway through, shut down any already-created `RuntimeDbEngine`s.
   - If global maxmemory wiring fails after engines were created, shut down all created engines.
   - Always close the shared `YierdisFfmMemoryRuntime` when instance creation aborts before returning a `YierdisInstance`.

2. `YierdisClient.connect(...)`
   - If `bootstrap.connect(...)` throws or channel setup fails before a client object is returned, shut down the temporary `NioEventLoopGroup`.

## Approach

### Instance startup rollback

Keep the public API unchanged. Wrap `YierdisInstance.create(...)` in a local startup rollback path:

- Track the shared `YierdisFfmMemoryRuntime` and the partially-filled `RuntimeDbEngine[]`.
- If any exception escapes before the final `new YierdisInstance(...)`, iterate over created engines and call `shutdown()` best-effort, aggregating failures as suppressed exceptions.
- Close the shared memory runtime after engine shutdown, also best-effort with suppression.
- Rethrow the original startup exception with cleanup failures suppressed onto it.

This keeps success-path behavior unchanged while making exceptional startup deterministic and leak-free.

### Client connect cleanup

Keep `YierdisClient.connect(...)` as a factory but guard temporary resources:

- Create the `NioEventLoopGroup` first as today.
- If `bootstrap.connect(...)` or channel initialization fails, call `group.shutdownGracefully().syncUninterruptibly()` in the catch path before rethrowing.

This is intentionally minimal and does not change request/response semantics.

## Testing

Add regression tests first:

1. Runtime test for partial instance startup:
   - Inject a `DbEngineFactory` that returns one close-tracking engine, then throws on the next DB creation.
   - Verify the first engine was shut down and the startup exception still surfaces.

2. Client test for failed connect cleanup:
   - Attempt a connection to an unused localhost port.
   - Capture the active thread set before and after.
   - Assert no extra `nioEventLoopGroup` thread remains after the failure.

## Risks

- Thread-based client leak detection can be flaky if it depends on exact thread counts, so the test should compare thread names with a bounded wait and only target new `nioEventLoopGroup` threads created during the failed connect attempt.
- Startup cleanup must not mask the original exception; cleanup failures should be suppressed, not replace the primary cause.
