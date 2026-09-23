# Production Hardening Operations

This guide is the operating contract for the single-node production-hardening program. It describes the runtime limits that are enforced by the server, the conditions under which a reply or mutation result is unknown, and the evidence required before a candidate is called accepted. It does not promise crash durability or recovery.

Read this together with [`configuration-and-operations.md`](./configuration-and-operations.md), [`executor-and-backpressure.md`](./executor-and-backpressure.md), [`maxmemory-and-eviction.md`](./maxmemory-and-eviction.md), [`protocol-reference.md`](./protocol-reference.md), and [`testing-and-debugging.md`](./testing-and-debugging.md).

## Runtime Baseline

All build, test, smoke, soak, package, and benchmark commands use JDK 25. Set the toolchain explicitly in automation and incident reproduction:

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

The following reply limits are hard startup-validated capacities. A request cannot use direct writes or a fallback growable buffer to bypass them.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--replyGlobalCapacityBytes` | `268435456` | Total admitted RESP reply capacity across all connections. |
| `--replyPerConnectionCapacityBytes` | `134217728` | Total admitted RESP reply capacity for one connection. |
| `--replyMaxTotalBytes` | `67108864` | Maximum charge for one top-level reply, including retained source bytes. |
| `--replyChunkPayloadBytes` | `65536` | Fixed payload capacity of a reply chunk. |
| `--replyControlReservationBytes` | `4096` | Per-reply-slot reservation kept available for a bounded control/error reply before any business reply bytes are written. |
| `--replyDrainTimeoutMillis` | `5000` | Graceful reply-drain deadline during shutdown. |

The server rejects invalid ordering at startup: control reservation must cover the fixed reply overhead plus the largest normalized scalar error frame (`1539` bytes minimum), control reservation must not exceed one-reply capacity, one-reply capacity must not exceed the per-connection capacity, and the per-connection capacity must not exceed the global capacity. The control allowance, one chunk, and fixed overhead must also fit the one-reply limit. Treat a startup validation failure as a configuration error, not as a runtime backpressure signal.

`OutboundMemoryBudget` accounts two different values:

- `reserved` is capacity charged to reply slots for encoded output plus retained-source bytes; the DB source object remains owned by its `PreparedCommand` through synchronous rendering.
- `allocated` is actual chunk buffer capacity currently materialized from that reservation.

Both gauges are bounded by the same hard admission hierarchy. A DB streamed source is owned by its `PreparedCommand` and is closed after synchronous rendering; its retained-source byte charge remains in the reply-slot lease until terminal slot cleanup. Slot, chunk, write-future, listener, queue, and any resource explicitly transferred to the reply sink remain associated with one reply slot until exactly one terminal cleanup owner releases them. `--client-output-buffer-limit-bytes` and `--client-output-buffer-over-limit-millis` remain slow-client policy controls; they are not replacements for the hard reply admission limits above.

Ingress has its own global bound. `--protocolGlobalInFlightBytes` limits admitted parsed request ownership. A positive value is used exactly; `0` derives a bounded value from `--executorQueueMaxBytes` and is not an unlimited mode. The protocol parser also enforces `--protocolMaxBulkBytes`, `--protocolMaxArgs`, `--protocolMaxLineBytes`, and `--protocolMaxCommandBytes` before a request reaches the executor.

## Ordering, Preflight, And Scheduler Policy

Every input origin receives a receive-order reply slot: normal commands, BUSY rejection, protocol errors, internal failures, and close-after-reply commands such as `QUIT`. A ready later reply waits behind an earlier slot. Reply bytes are chunked by the bounded egress owner, not written by command handlers.

Commands declare a reply plan before a mutation when the reply shape can be measured safely. An exact-limit preflight failure occurs before the mutation and leaves the database unchanged. Aggregate reply sources remain owned by their `PreparedCommand` through planning and synchronous rendering rather than being copied into an unbounded detached list.

The executor scheduling policy controls what happens when reply capacity blocks a head:

- `FAIR` rotates runnable connections so a connection waiting on its own reply capacity does not stop independent runnable connections with available capacity.
- `GLOBAL` preserves the global FIFO head: later work must not pass a blocked earlier head.

Neither policy weakens per-connection, per-reply, or global reply limits. A `ReplyTooLargeException` means the configured single-reply limit cannot represent the reply; the server closes that transport without emitting a replacement internal error.

## Result-Unknown Behavior

There are two materially different failures:

1. A preflight rejection before mutation or visible reply output is deterministic. It may produce the normal capacity/command failure and the mutation is not committed.
2. A failure after a mutation may have committed, after reply bytes may have become visible, or after a write outcome is ambiguous is result-unknown. Examples include a post-commit mutation failure, write failure, source/chunk mismatch, and disconnect during output.

For a result-unknown failure the server cancels the reply slot and closes the connection without a replacement reply. It must not fabricate `-ERR internal error`, because that would claim a result that may contradict a visible mutation or partial reply.

## Observability And Leak Triage

`INFO stats` exposes the active and peak accounting used for incident triage. The most useful fields are:

| Domain | Fields to inspect |
| --- | --- |
| Ingress | `yierdis_inbound_reserved_bytes`, `yierdis_inbound_peak_reserved_bytes`, `yierdis_inbound_waiting_connections`, `yierdis_inbound_rejected_connections` |
| Reply capacity | `yierdis_reply_global_capacity_bytes`, `yierdis_reply_per_connection_capacity_bytes`, `yierdis_reply_max_total_bytes`, `yierdis_outbound_reserved_bytes`, `yierdis_outbound_allocated_bytes`, `yierdis_outbound_peak_reserved_bytes`, `yierdis_outbound_peak_allocated_bytes` |
| Reply ownership | `yierdis_outbound_active_connections`, `yierdis_outbound_active_slots`, `yierdis_outbound_active_chunks`, `yierdis_outbound_active_sources`, `yierdis_live_child_channels` |
| Reply failures and scheduling | `yierdis_outbound_capacity_rejects`, `yierdis_outbound_oversized_replies`, `yierdis_outbound_cancelled_slots`, `yierdis_outbound_failed_slots`, `yierdis_outbound_write_failures`, `yierdis_result_unknown_closes`, `yierdis_deferred_fair_reply_heads`, `yierdis_deferred_global_reply_heads` |
| Shutdown | `yierdis_reply_shutdown_timeouts`, inbound closed state, and final ownership gauges |
| Maxmemory/native | `INFO memory` fields including `yierdis_maxmemory_used_bytes`, `yierdis_maxmemory_effective_used_bytes`, `yierdis_ledger_reserved_bytes`, `yierdis_offheap_used_bytes`, `yierdis_native_metadata_committed_bytes`, `yierdis_native_data_committed_bytes`, `yierdis_native_live_objects`, `yierdis_native_live_regions`, and native defrag summaries |

During normal steady state, peaks may remain non-zero while current reserved/allocated bytes return to zero. After a test fixture or successful graceful shutdown, active slots, chunks, sources, child channels, and inbound reservation must converge to zero. A non-zero current gauge after clients disconnect is a leak signal; capture `INFO stats`, `MEMORY STATS`, process logs, the exact workload seed, and the candidate artifact checksum before restarting.

The soak workload runs four fill/cleanup cycles (`ProductionHardeningSoakTest.SOAK_CYCLE_COUNT = 4`). The first completed cycle records the warm baseline; each later cycle must return live native objects and FFM regions to that baseline, and committed native bytes must remain below the metadata high-water mark plus the configured one-warm-page-per-size-class bound. The main client keeps one fixed inbound read credit while it remains connected; that standing credit is its cycle baseline, while retained input, consolidation, reply slots, sources, chunks, and outbound reservations must drain. RSS remains supplementary telemetry because JVM heap residency can grow independently of live ownership; native counters and ownership gauges are the required leak assertions.

## Graceful Shutdown

Graceful shutdown is an ownership protocol, not merely a listener close:

1. Stop accepting new server connections.
2. Close the child-channel registry to late registration and disable child input.
3. Ask the executor to reject new work, cancel non-started or capacity-waiting replies, and drain already-started owners.
4. Let each connection sequencer flush READY heads in receive order; close after the final ordered reply when required.
5. Wait up to `--replyDrainTimeoutMillis` for reply and child-channel drain. On timeout, force-close remaining children, preserve diagnostics, and report shutdown failure.
6. Only after child ownership drains, close ingress and outbound budgets, instance runtime resources, and Netty groups.

A timeout is not a successful close. `yierdis_reply_shutdown_timeouts`, live children, reserved/allocated bytes, and active slot counts are the first diagnostics. Retrying shutdown after a timeout must not hide the original failure or claim that active leases were safely drained.

## Verification Commands

Use the same JDK 25 environment for every gate. The focused reply matrix is the fastest signal for receive-order, capacity, result-unknown, and shutdown ownership:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests -am \
  -Dtest=OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest \
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

Smoke runs the packaged server through the supported command surface:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/smoke.sh
```

`scripts/production-hardening-soak.sh` defaults to 600 seconds. Without `--skip-package` it packages `yierdis-server/yierdis-server` and `yierdis-tests`, then writes commit, duration, seed, JDK, Maven, `uname`, the server jar path, and its SHA-256 to `target/production-hardening-soak/<timestamp>-seed-<seed>/environment.txt`. `--skip-package` uses the existing server jar and fails when that jar is missing. `SKIP_BUILD=1` applies to `scripts/smoke.sh` and `scripts/bench.sh`.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710
```

`scripts/bench.sh` connects to an already started Yierdis process. It does not start Redis and has no AUTH, username, or password. The benchmark sends `SELECT` only when `database != 0`.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 FORMAT=csv HOST=127.0.0.1 PORT=16378 \
  REQUESTS=100000 CLIENTS=50 DATA_SIZE=3 PIPELINE=1 \
  ./scripts/bench.sh > target/yierdis-benchmark.csv
```

CSV comparison uses the canonical title and the first eight shared fields: `test`, `rps`, `avg_latency_ms`, `min_latency_ms`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, and `max_latency_ms`. Yierdis also writes `status` and `reason`. `SPOP`, `ZPOPMIN`, `MSET`, and `XADD` are currently `UNSUPPORTED` and leave the numeric columns empty. The benchmark does not compute release thresholds or artifact ratios.
