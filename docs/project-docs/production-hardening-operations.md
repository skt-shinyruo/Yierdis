# Production Hardening Operations

This guide is the operating contract for the single-node production-hardening program. It describes the runtime limits that are enforced by the server, the conditions under which a reply or mutation result is unknown, and the evidence required before a candidate is called accepted. It does not promise crash durability: the in-process commit-stream is bounded and ordered, but it is not an AOF, RDB, replication, or recovery mechanism.

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
java -jar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar --port 6378
```

Use `INFO`, `INFO stats`, `STATS`, and `MEMORY STATS` to inspect a running process. Do not infer a limit from JVM heap use alone: request, commit-stream, maxmemory/native, and reply ownership are separate bounded domains.

## Admission Limits

The following reply limits are hard startup-validated capacities. A request cannot use direct writes or a fallback growable buffer to bypass them.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--replyGlobalCapacityBytes` | `268435456` | Total admitted RESP reply capacity across all connections. |
| `--replyPerConnectionCapacityBytes` | `134217728` | Total admitted RESP reply capacity for one connection. |
| `--replyMaxTotalBytes` | `67108864` | Maximum charge for one top-level reply, including retained source bytes. |
| `--replyChunkPayloadBytes` | `65536` | Fixed payload capacity of a reply chunk. |
| `--replyControlReservationBytes` | `4096` | Per-request control allowance before streamed chunks are needed. |
| `--replyDrainTimeoutMillis` | `5000` | Graceful reply-drain deadline during shutdown. |

The server rejects invalid ordering at startup: control reservation must cover the fixed reply overhead plus the largest normalized scalar error frame (`1539` bytes minimum), control reservation must not exceed one-reply capacity, one-reply capacity must not exceed the per-connection capacity, and the per-connection capacity must not exceed the global capacity. The control allowance, one chunk, and fixed overhead must also fit the one-reply limit. Treat a startup validation failure as a configuration error, not as a runtime backpressure signal.

`OutboundMemoryBudget` accounts two different values:

- `reserved` is capacity admitted to reply slots and their retained sources.
- `allocated` is actual chunk buffer capacity currently materialized from that reservation.

Both gauges are bounded by the same hard admission hierarchy. Slot, source, chunk, write future, listener, and queue ownership remain associated with one reply slot until exactly one terminal cleanup owner releases them. `--clientOutputBufferLimitBytes` and its grace interval remain slow-client policy controls; they are not replacements for the hard reply admission limits above.

Ingress has its own global bound. `--protocolGlobalInFlightBytes` limits admitted parsed request ownership. A positive value is used exactly; `0` derives a bounded value from `--executorQueueMaxBytes` and is not an unlimited mode. The protocol parser also enforces `--protocolMaxBulkBytes`, `--protocolMaxArgs`, `--protocolMaxLineBytes`, and `--protocolMaxCommandBytes` before a request reaches the executor.

The commit-stream is an independent bounded admission domain. Its reserved event and byte counters describe ordered notification work after mutation commit; it is not durable storage. When commit-stream pressure or failure is observed, do not reinterpret it as evidence that a process restart can recover unpersisted mutations.

## Ordering, Preflight, And Scheduler Policy

Every input origin receives a receive-order reply slot: normal commands, BUSY rejection, protocol errors, internal failures, and close-after-reply commands such as `QUIT`. A ready later reply waits behind an earlier slot. Reply bytes are chunked by the bounded egress owner, not written by command handlers.

Commands declare a reply plan before a mutation when the reply shape can be measured safely. An exact-limit preflight failure occurs before the mutation and leaves the database unchanged. Aggregate reply sources are owned and replayable rather than copied into an unbounded detached list.

The executor scheduling policy controls what happens when reply capacity blocks a head:

- `FAIR` rotates runnable connections so a connection waiting on its own reply capacity does not stop independent runnable connections with available capacity.
- `GLOBAL` preserves the global FIFO head: later work must not pass a blocked earlier head.

Neither policy weakens per-connection, per-reply, or global reply limits. A `ReplyTooLargeException` means the configured single-reply limit cannot represent the reply; the server closes that transport without emitting a replacement internal error.

## Result-Unknown Behavior

There are two materially different failures:

1. A preflight rejection before mutation or visible reply output is deterministic. It may produce the normal capacity/command failure and the mutation is not committed.
2. A failure after a mutation may have committed, after reply bytes may have become visible, or after a write outcome is ambiguous is result-unknown. Examples include a post-commit mutation failure, write failure, source/chunk mismatch, and disconnect during output.

For a result-unknown failure the server cancels the reply slot and closes the connection without a replacement reply. It must not fabricate `-ERR internal error`, because that would claim a result that may contradict a visible mutation or partial reply. A client must reconnect, re-read state where the command semantics allow it, and use an application-level idempotency strategy for mutations that cannot be safely retried.

Operators should distinguish a result-unknown close from a capacity reject by checking `yierdis_result_unknown_closes`, `yierdis_outbound_write_failures`, `yierdis_outbound_failed_slots`, and the client connection lifecycle. The counter is diagnostic, not a durable command journal.

## Observability And Leak Triage

`INFO stats` exposes the active and peak accounting used for incident triage. The most useful fields are:

| Domain | Fields to inspect |
| --- | --- |
| Ingress | `yierdis_inbound_reserved_bytes`, `yierdis_inbound_peak_reserved_bytes`, `yierdis_inbound_waiting_connections`, `yierdis_inbound_rejected_connections` |
| Commit-stream | `yierdis_commit_stream_state`, `yierdis_commit_stream_reserved_events`, `yierdis_commit_stream_reserved_bytes`, `yierdis_commit_stream_rejected_writes`, `yierdis_commit_stream_shutdown_timed_out` |
| Reply capacity | `yierdis_reply_global_capacity_bytes`, `yierdis_reply_per_connection_capacity_bytes`, `yierdis_reply_max_total_bytes`, `yierdis_outbound_reserved_bytes`, `yierdis_outbound_allocated_bytes`, `yierdis_outbound_peak_reserved_bytes`, `yierdis_outbound_peak_allocated_bytes` |
| Reply ownership | `yierdis_outbound_active_connections`, `yierdis_outbound_active_slots`, `yierdis_outbound_active_chunks`, `yierdis_outbound_active_sources`, `yierdis_live_child_channels` |
| Reply failures and scheduling | `yierdis_outbound_capacity_rejects`, `yierdis_outbound_oversized_replies`, `yierdis_outbound_cancelled_slots`, `yierdis_outbound_failed_slots`, `yierdis_outbound_write_failures`, `yierdis_result_unknown_closes`, `yierdis_deferred_fair_reply_heads`, `yierdis_deferred_global_reply_heads` |
| Shutdown | `yierdis_reply_shutdown_timeouts`, commit-stream timeout state, inbound closed state, and final ownership gauges |
| Maxmemory/native | `INFO memory` fields including `yierdis_maxmemory_used_bytes`, `yierdis_maxmemory_effective_used_bytes`, `yierdis_ledger_reserved_bytes`, `yierdis_offheap_used_bytes`, `yierdis_native_metadata_committed_bytes`, `yierdis_native_data_committed_bytes`, `yierdis_native_live_objects`, `yierdis_native_live_regions`, and native defrag summaries |

During normal steady state, peaks may remain non-zero while current reserved/allocated bytes return to zero. After a test fixture or successful graceful shutdown, active slots, chunks, sources, child channels, inbound reservation, and commit-stream ownership must converge to zero. A non-zero current gauge after clients disconnect is a leak signal; capture `INFO stats`, `MEMORY STATS`, process logs, the exact workload seed, and the candidate artifact checksum before restarting.

The soak workload uses one warmup cycle followed by three measured fill/cleanup cycles. At each completed cycle, live native objects and FFM regions must return to the warm baseline, and committed native bytes must remain below the metadata high-water mark plus the configured one-warm-page-per-size-class bound. The main client keeps one fixed inbound read credit while it remains connected; that standing credit is its cycle baseline, while retained input, consolidation, commit records, reply slots, sources, chunks, and outbound reservations must drain. RSS is supplementary evidence: the harness reports it for every sample and fails on uninterrupted growth above 16 MiB across the final three completed cycles. Native counters and ownership gauges remain the required leak assertions.

## Graceful Shutdown

Graceful shutdown is an ownership protocol, not merely a listener close:

1. Stop accepting new server connections.
2. Close the child-channel registry to late registration and disable child input.
3. Ask the executor to reject new work, cancel non-started or capacity-waiting replies, and drain already-started owners.
4. Let each connection sequencer flush READY heads in receive order; close after the final ordered reply when required.
5. Wait up to `--replyDrainTimeoutMillis` for reply and child-channel drain. On timeout, force-close remaining children, preserve diagnostics, and report shutdown failure.
6. Only after child ownership drains, close ingress and outbound budgets, engine/runtime resources, and Netty groups.

A timeout is not a successful close. `yierdis_reply_shutdown_timeouts`, live children, reserved/allocated bytes, and active slot counts are the first diagnostics. Retrying shutdown after a timeout must not hide the original failure or claim that active leases were safely drained.

## Verification Commands

Use the same JDK 25 environment for every gate. The focused reply matrix is the fastest signal for receive-order, capacity, result-unknown, and shutdown ownership:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests/yierdis-integration-tests -am \
  -Dtest=OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsurefire.rerunFailingTestsCount=3 test
```

Run the architecture guard after code or documentation changes:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Smoke runs the packaged server through the supported command surface:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/smoke.sh
```

The bounded soak harness records its seed, argv, environment, server artifact SHA-256 value, peaks, samples, cycle baselines, RSS observations, and final counters under `target/production-hardening-soak`. The soak wrapper does not require a benchmark artifact; the separate performance gate records its own artifact identities. Use a short deterministic duration while developing and the required ten-minute duration for acceptance:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710
```

For final acceptance, package the candidate exactly once. Record the resulting SHA-256 values, then keep those artifacts frozen through smoke, soak, and the comparison suite. `SKIP_BUILD=1` prevents smoke from rebuilding; `--skip-package` makes the soak wrapper fail if the already-packaged server artifact is unavailable instead of silently producing a new candidate.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server-main,yierdis-cli -am -DskipTests package
sha256sum yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 ./scripts/smoke.sh
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --skip-package --duration-seconds 600 --seed 20260710
```

The release performance gate compares immutable baseline and current server artifacts. GET, SET, HSET, and ZADD median throughput must each reach at least `0.90` of baseline. A large pipelined reply scenario is diagnostic for outbound reservation, allocation, waits, and write failures; it does not replace any of the four mandatory gates.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite --suiteProfile release \
  --baselineServerJar artifacts/baseline/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/production-hardening
```

## Acceptance Record

This repository is not accepted merely because this guide exists. Before final acceptance, record one candidate in this section with its candidate commit, baseline commit and artifact checksum, current artifact checksum, JDK/OS/CPU, exact commands, focused/full suite totals, smoke result, soak seed and peak/final counters, and GET/SET/HSET/ZADD medians and ratios. All evidence must come from the same candidate artifact; a rerun after rebuilding is a new candidate.

| Field | Acceptance value |
| --- | --- |
| Candidate commit and current artifact SHA-256 | Pending final acceptance |
| Baseline commit and artifact SHA-256 | Pending final acceptance |
| JDK / OS / CPU | Pending final acceptance |
| Focused, architecture, and full Maven suites | Pending final acceptance |
| Smoke and 600-second soak | Pending final acceptance |
| GET / SET / HSET / ZADD median ratios | Pending final acceptance |
| Final inbound, commit-stream, reply, and child ownership counters | Pending final acceptance |
