# Production Hardening Operations

This guide is the operating contract for the single-node production-hardening program. It describes the runtime limits enforced by the server, how those limits are sized and observed, the conditions under which a reply or mutation result is unknown, the graceful-shutdown ownership order, and the evidence required before a candidate counts as accepted. It does not promise crash durability or recovery.

Read this together with [`configuration-and-operations.md`](./configuration-and-operations.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`protocol-reference.md`](./protocol-reference.md), [`testing-and-debugging.md`](./testing-and-debugging.md), and [`client-and-bench-internals.md`](./client-and-bench-internals.md).

## Runtime Baseline

All build, test, package, and benchmark commands use JDK 25. Set the toolchain explicitly in automation and incident reproduction:

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
export PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH
java -version
mvn -version
```

Start from a packaged artifact only after the command above identifies JDK 25:

```bash
mvn -DskipTests package
java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 6378 --maxmemoryBytes 0
```

Use `INFO`, `INFO stats`, `STATS`, and `MEMORY STATS` to inspect a running process. Do not infer a limit from JVM heap use alone: request, maxmemory/native, and reply ownership are separate bounded domains.

## Admission Limits

The following reply limits are hard capacities validated at startup (`YierdisServerRuntimeConfig.normalizeAndValidate`). A request cannot use direct writes or a fallback growable buffer to bypass them.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--replyGlobalCapacityBytes` | `268435456` | Total admitted RESP reply capacity across all connections (256 MiB). |
| `--replyPerConnectionCapacityBytes` | `134217728` | Total admitted RESP reply capacity for one connection (128 MiB). |
| `--replyMaxTotalBytes` | `67108864` | Maximum charge for one top-level reply, including retained source bytes (64 MiB). |
| `--replyChunkPayloadBytes` | `65536` | Fixed payload capacity of a reply chunk (64 KiB). |
| `--replyControlReservationBytes` | `4096` | Per-reply-slot reservation kept available for a bounded control/error reply before any business reply bytes are written. |
| `--replyDrainTimeoutMillis` | `5000` | Graceful reply-drain deadline during shutdown. |

The server rejects invalid ordering at startup. Control reservation must cover the fixed reply overhead plus the largest normalized scalar error frame; the constants are `REPLY_FIXED_OVERHEAD_BYTES = 1024` and `REPLY_MAX_CONTROL_ERROR_FRAME_BYTES = 515`, so `MIN_REPLY_CONTROL_RESERVATION_BYTES = 1539`. In addition: control reservation must not exceed one-reply capacity; one-reply capacity must not exceed the per-connection capacity; the per-connection capacity must not exceed the global capacity; and the control allowance plus one chunk plus fixed overhead must fit the one-reply limit. Treat a startup validation failure as a configuration error, not as a runtime backpressure signal.

### How the reply and ingress capacities are sized

Sizing is a two-step math problem; never guess a "safe" number.

1. **Ingress.** `--protocolGlobalInFlightBytes` bounds admitted parsed-request ownership. A positive value is taken literally. `0` is not unlimited: it derives `max(128 MiB, 2 × --executorQueueMaxBytes)` (`YierdisServerArgs.deriveProtocolGlobalInFlightBytes`, floor `MIN_PROTOCOL_GLOBAL_IN_FLIGHT_BYTES = 128 MiB`). The protocol parser also enforces `--protocolMaxBulkBytes`, `--protocolMaxArgs`, `--protocolMaxLineBytes`, and `--protocolMaxCommandBytes` before a request reaches the executor.
2. **Reply.** One reply is charged at most `--replyMaxTotalBytes`. Because control reservation and one chunk must fit that charge, the effective per-reply budget is roughly `replyMaxTotalBytes - (replyControlReservationBytes + replyChunkPayloadBytes) - fixed overhead`. Sum across concurrent connections stays under `--replyGlobalCapacityBytes`, and one connection stays under `--replyPerConnectionCapacityBytes`.

Observe the components rather than reasoning about them: read the capacity fields from `INFO stats` (see the observability table) and cross-check against `MEMORY STATS` for the DB side. `--client-output-buffer-limit-bytes` and `--client-output-buffer-over-limit-millis` remain slow-client policy controls; they do not replace the hard reply admission limits above.

### `OutboundMemoryBudget` tracks two different values

- `reserved` is the capacity charged to reply slots for encoded output plus retained-source bytes; the DB source object remains owned by its `PreparedCommand` through synchronous rendering.
- `allocated` is the actual chunk buffer capacity currently materialized from that reservation.

Both gauges are bounded by the same hard admission hierarchy. A DB streamed source is owned by its `PreparedCommand` and closed after synchronous rendering; its retained-source byte charge remains in the reply-slot lease until terminal slot cleanup. Slot, chunk, write-future, listener, queue, and any resource explicitly transferred to the reply sink stay associated with one reply slot (`ReplySlot`, `BoundedChunkedReplySink`) until exactly one terminal cleanup owner releases them.

## Ordering, Preflight, And Scheduler Policy

Every input origin receives a receive-order reply slot: normal commands, BUSY rejection, protocol errors, internal failures, and close-after-reply commands such as `QUIT`. A ready later reply waits behind an earlier slot. The bounded egress owner chunks reply bytes; command handlers do not write them.

Commands declare a reply plan before a mutation when the reply shape can be measured safely. An exact-limit preflight failure occurs before the mutation and leaves the database unchanged. Aggregate reply sources stay owned by their `PreparedCommand` through planning and synchronous rendering; they are not copied into an unbounded detached list.

The executor scheduling policy controls what happens when reply capacity blocks a head:

- `FAIR` rotates runnable connections so a connection waiting on its own reply capacity does not stop independent runnable connections with available capacity.
- `GLOBAL` preserves the global FIFO head: later work must not pass a blocked earlier head.

Neither policy weakens per-connection, per-reply, or global reply limits. A `ReplyTooLargeException` means the configured single-reply limit cannot represent the reply; the server closes that transport without emitting a replacement internal error.

## Result-Unknown Behavior

There are two materially different failures:

1. A preflight rejection before mutation or visible reply output is deterministic. It may produce the normal capacity/command failure and the mutation is not committed.
2. A failure is result-unknown when it occurs after a mutation may have committed, after reply bytes may have become visible, or after a write outcome becomes ambiguous. Examples include a post-commit mutation failure, write failure, source/chunk mismatch, and disconnect during output.

For a result-unknown failure the server cancels the reply slot and closes the connection without a replacement reply. It must not fabricate `-ERR internal error`, because that would claim a result that may contradict a visible mutation or partial reply. Each such close increments `yierdis_result_unknown_closes`. Executor-level handling is pinned by `CommandExecutorTest` (mock IO) and the close counter by `ConnectionReplySequencerTest`; the wire-level close path currently has no integration test after `ReplyResultUnknownTest` was retired. Run the focused reply matrix below.

## Observability And Leak Triage

`INFO stats` exposes the active and peak accounting used for incident triage. In the text `# Stats` section every field is prefixed with `yierdis_`; in the `STATS` map the same fields appear without that prefix. The most useful fields are:

| Domain | Fields to inspect |
| --- | --- |
| Ingress | `yierdis_inbound_capacity_bytes`, `yierdis_inbound_reserved_bytes`, `yierdis_inbound_peak_reserved_bytes`, `yierdis_inbound_waiting_connections`, `yierdis_inbound_backpressured`, `yierdis_inbound_rejected_connections`, `yierdis_inbound_closed` |
| Reply capacity | `yierdis_reply_global_capacity_bytes`, `yierdis_reply_per_connection_capacity_bytes`, `yierdis_reply_max_total_bytes`, `yierdis_reply_chunk_payload_bytes`, `yierdis_reply_control_reservation_bytes`, `yierdis_reply_drain_timeout_millis`, `yierdis_outbound_reserved_bytes`, `yierdis_outbound_allocated_bytes`, `yierdis_outbound_peak_reserved_bytes`, `yierdis_outbound_peak_allocated_bytes` |
| Reply ownership | `yierdis_outbound_active_connections`, `yierdis_outbound_active_slots`, `yierdis_outbound_active_chunks`, `yierdis_outbound_active_sources`, `yierdis_live_child_channels` |
| Reply failures and scheduling | `yierdis_outbound_capacity_rejects`, `yierdis_outbound_oversized_replies`, `yierdis_outbound_cancelled_slots`, `yierdis_outbound_failed_slots`, `yierdis_outbound_write_failures`, `yierdis_result_unknown_closes`, `yierdis_reply_shutdown_timeouts`, `yierdis_deferred_fair_reply_heads`, `yierdis_deferred_global_reply_heads` |
| Shutdown | `yierdis_reply_shutdown_timeouts`, `yierdis_inbound_closed`, and final ownership gauges |
| Maxmemory/native | `INFO memory` fields including `yierdis_maxmemory_used_bytes`, `yierdis_maxmemory_effective_used_bytes`, `yierdis_ledger_used_bytes`, `yierdis_ledger_reserved_bytes`, `yierdis_offheap_used_bytes`, `yierdis_native_metadata_committed_bytes`, `yierdis_native_data_committed_bytes`, `yierdis_native_data_live_bytes`, `yierdis_native_live_objects`, `yierdis_native_live_regions`, plus native defrag summaries |

During normal steady state, peaks may remain non-zero while current reserved/allocated bytes return to zero. After a test fixture or successful graceful shutdown, active slots, chunks, sources, child channels, and inbound reservation must converge to zero. A non-zero current gauge after clients disconnect is a leak signal. Capture `INFO stats`, `INFO memory`, `MEMORY STATS`, process logs, the exact workload seed, and the candidate artifact checksum before restarting. Leak evidence rests on the native counters and ownership gauges; RSS is supplementary because JVM heap residency can grow independently of live ownership.

## Graceful Shutdown

Graceful shutdown is an ownership protocol, not merely a listener close. `YierdisServerBootstrap.closeInternal` performs it in this exact order:

1. Close the server channel so no new server connection is accepted.
2. Close the child-channel registry to late registration (`ChildChannelRegistry.beginShutdown()`) and mark every accepted child as closing (`NettyExecutionConnection.markClosing`), which disables child input.
3. Cancel the maintenance/cleanup future.
4. Ask the executor to shut down gracefully (`CommandExecutor.shutdownGracefully()`), rejecting new work, cancelling non-started or capacity-waiting replies, and draining already-started owners.
5. Let each connection sequencer flush READY heads in receive order (`ConnectionReplySequencer`) and close after the final ordered reply when required; await up to `--replyDrainTimeoutMillis`. On timeout, force-close remaining children (`ChildChannelRegistry.forceClose()`), record `yierdis_reply_shutdown_timeouts`, preserve diagnostics, and report shutdown failure.
6. Only after child ownership drains, close the ingress and outbound budgets, the instance runtime resources (DB/native), and finally the Netty command group, boss group, and worker group.

A timeout is not a successful close. `yierdis_reply_shutdown_timeouts`, live children, reserved/allocated bytes, and active slot counts are the first diagnostics; the timeout exception message itself carries `liveChildren`, `reservedBytes`, `allocatedBytes`, `activeConnections`, and `activeSlots`. Retrying shutdown after a timeout must not hide the original failure or claim that active leases were safely drained.

## Verification Commands

Use the same JDK 25 environment for every gate. The focused reply matrix is the fastest signal for receive-order, capacity, and result-unknown ownership:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=OrderedReplyIntegrationTest,MaxmemoryDoubleReplyRegressionTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsurefire.rerunFailingTestsCount=3 test
```

Run the DB API-shape check after changing its public factory or implementation visibility:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=YierdisDbArchitectureGuardTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Booting the packaged artifact is a manual step: start the jar, then exercise the supported command surface with the bundled CLI or `redis-cli`.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  java -jar yierdis-server/yierdis-server/target/yierdis-server-0.1.0-SNAPSHOT.jar --port 16379 --maxmemoryBytes 0
redis-cli -p 16379 PING
```

`SKIP_BUILD=1` applies to `scripts/bench.sh`.

`scripts/bench.sh` connects to an already started Yierdis process. It does not start Redis and has no AUTH, username, or password. The benchmark sends `SELECT` only when `database != 0`.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 FORMAT=csv HOST=127.0.0.1 PORT=16378 \
  REQUESTS=100000 CLIENTS=50 DATA_SIZE=3 PIPELINE=1 \
  ./scripts/bench.sh > target/yierdis-benchmark.csv
```

## Benchmark Comparison

Comparison uses the canonical title and the first eight shared fields of the CSV. The Yierdis RESP CSV header is exactly (`BenchmarkOutputRenderer.CSV_HEADER`):

```text
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms","status","reason"
```

The first eight fields — `test`, `rps`, `avg_latency_ms`, `min_latency_ms`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, `max_latency_ms` — align with the official Redis-style CSV and are the shared comparison surface; `status` and `reason` are Yierdis extensions. Pairing must be by canonical title, and comparison may only use those eight columns. Non-`SUCCESS` rows leave the seven numeric fields empty, so they must not be read as `0`.

The canonical titles come from `RedisBenchmarkCatalog` and match [`client-and-bench-internals.md`](./client-and-bench-internals.md). Four rows are currently `UNSUPPORTED` with their exact canonical titles: `SPOP`, `ZPOPMIN`, `MSET (10 keys)`, and `XADD`. The default catalog therefore produces 17 `SUCCESS` rows (including the measured `LPUSH (needed to benchmark LRANGE)` setup row) plus those 4 `UNSUPPORTED` rows. To compare both sides on equal footing, fix the same inputs (`requests`, `clients`, `pipeline`, `data-size`, `keyspace`, keep-alive, `database`) on each side, record the environment, and pass an explicit `SEED` when key reproducibility matters.

The benchmark does not compute release thresholds or artifact ratios. Artifact identity, environment record, result storage, ratios, and release thresholds are operator policy outside the benchmark.