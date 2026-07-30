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
| Maxmemory/native | `INFO memory` fields including `yierdis_maxmemory_used_bytes`, `yierdis_maxmemory_effective_used_bytes`, `yierdis_ledger_reserved_bytes`, `yierdis_offheap_used_bytes`, `yierdis_native_metadata_committed_bytes`, `yierdis_native_data_committed_bytes`, `yierdis_native_live_objects`, `yierdis_native_live_regions`, `yierdis_expired_entries_awaiting_physical_deletion`, and native defrag summaries |

During normal steady state, peaks may remain non-zero while current reserved/allocated bytes return to zero. `yierdis_expired_entries_awaiting_physical_deletion` is a current gauge, not a peak: it rises once per logically expired key only when commit-stream admission prevents its synthetic deletion event, and returns to zero when deletion, replacement, or FLUSHDB converges the entry. After a test fixture or successful graceful shutdown, active slots, chunks, sources, child channels, inbound reservation, and commit-stream ownership must converge to zero. A non-zero current gauge after clients disconnect is a leak signal; capture `INFO stats`, `MEMORY STATS`, process logs, the exact workload seed, and the candidate artifact checksum before restarting.

The soak workload uses one warmup cycle followed by three measured fill/cleanup cycles. At each completed cycle, live native objects and FFM regions must return to the warm baseline, and committed native bytes must remain below the metadata high-water mark plus the configured one-warm-page-per-size-class bound. The main client keeps one fixed inbound read credit while it remains connected; that standing credit is its cycle baseline, while retained input, consolidation, commit records, reply slots, sources, chunks, and outbound reservations must drain. RSS is supplementary evidence: the harness reports it for every sample and fails on uninterrupted growth above 16 MiB across the final three completed cycles. Native counters and ownership gauges remain the required leak assertions.

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

The bounded soak harness records its seed, argv, environment, server artifact SHA-256 value, peaks, samples, cycle baselines, RSS observations, and final counters under `target/production-hardening-soak`. The soak wrapper does not require a benchmark artifact. Separately managed performance evidence records its own Yierdis and Redis target identities and raw outputs. Use a short deterministic duration while developing and the required ten-minute duration for acceptance:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710
```

For final acceptance, package the candidate exactly once. Record the resulting SHA-256 values, then keep those artifacts frozen through smoke, soak, and the Yierdis performance run. `SKIP_BUILD=1` prevents smoke and benchmark scripts from rebuilding; `--skip-package` makes the soak wrapper fail if the already-packaged server artifact is unavailable instead of silently producing a new candidate.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -pl yierdis-server/yierdis-server-main,yierdis-cli,yierdis-benchmark -am -DskipTests package
sha256sum yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar
sha256sum yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 ./scripts/smoke.sh
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --skip-package --duration-seconds 600 --seed 20260710
```

Performance evidence consists of two operator-managed executions: the project benchmark connects to a separately started Yierdis candidate, and official `redis-benchmark` connects to a separately managed Redis target. The request count, clients, payload size, pipeline depth, keyspace mode, keepalive, authentication, and database selection must be equivalent. This project never starts or runs Redis and does not define a combined harness; the operator owns Redis configuration, process lifecycle, artifact identity, and result files.

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 FORMAT=csv HOST=127.0.0.1 PORT=16378 \
  REQUESTS=100000 CLIENTS=50 DATA_SIZE=3 PIPELINE=1 \
  ./scripts/bench.sh > target/yierdis-benchmark.csv
redis-benchmark -h 127.0.0.1 -p 6379 \
  -n 100000 -c 50 -d 3 -P 1 --csv > redis-benchmark.csv
```

Compare corresponding canonical titles and only the first eight shared Redis-style CSV fields: `test`, `rps`, `avg_latency_ms`, `min_latency_ms`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, and `max_latency_ms`. Yierdis adds `status` and `reason`; its SPOP, ZPOPMIN, MSET, and XADD rows are currently `UNSUPPORTED` and retain empty numeric metrics. The benchmark computes neither release thresholds nor artifact ratios. Any pass/fail rule or cross-target calculation belongs to external operator policy and must preserve the raw results used for that decision.

## Historical Stage Evidence

The focused tests below were rerun under JDK 25 for the pre-rewrite non-performance candidate. They are retained as historical evidence and do not define the current performance workflow.

| Stage | Non-benchmark evidence |
| --- | --- |
| 1: allocator and usage | `NativeAllocationScopeTest`, `SynchronizedNativeAllocatorTest`, and `EmptyDatabaseFootprintTest` passed. |
| 2: failure-atomic mutations | `NativeCapacityOomRecoveryTest`, `MutationFaultInjectionTest`, `YierdisDbHealthTest`, and `MutationExecutorReservationTest` passed. |
| 3: bounded hash tables | `HashTableMillionOperationChurnTest` and `HashTableMaintenanceTest` passed. |
| 4: maxmemory convergence | `MaxmemoryScopeTest` passed; the full suite also passed physical-accounting, page-trim, and global-governor coverage. |
| 5: RESP ingress admission | `RespIngressPressureTest` and `RespIngressFuzzTest` passed with paranoid Netty leak detection enabled. |
| 6: commit stream convergence | `CommitStreamTest`, `CommitStreamShutdownTest`, `CommitStreamIntegrationTest`, and `CommitStreamExpirationEvictionTest` passed. |
| 7: bounded ordered egress | `OrderedReplyIntegrationTest`, `OutboundReplyPressureTest`, `ReplyResultUnknownTest`, and `ArchitectureBoundaryTest` passed; package, smoke, and the 600-second soak also passed. |
| 7: throughput gate | USER-WAIVED: benchmark execution is intentionally disabled and excluded from this acceptance record. No throughput ratio is claimed. |

## Current Acceptance Record Requirements

This repository is not accepted merely because this guide exists. For a new candidate, record the candidate commit; Yierdis server and benchmark checksums; JDK/OS/CPU; exact functional, smoke, soak, and benchmark commands; focused/full-suite totals; soak seed and peak/final counters; the separately managed Redis artifact and configuration identity; equivalent workload values; raw Yierdis and Redis result paths; and any external operator decision. All Yierdis evidence must come from the same frozen candidate artifact; a rerun after rebuilding is a new candidate. Redis evidence remains an independently managed run and must never be presented as project-orchestrated output.

## Historical Acceptance Records

### Candidate `0e5d527f07ebd15376988e813e15ef7e67e0769c`

The non-performance gates below passed for this candidate. This is deliberately **not** a final Stage 7 acceptance record: the active execution constraint prohibited benchmark commands, so the mandatory GET, SET, HSET, and ZADD throughput comparison was not run. No throughput ratio is inferred from the functional, smoke, or soak evidence.

| Field | Acceptance value |
| --- | --- |
| Candidate commit and current artifact SHA-256 | `0e5d527f07ebd15376988e813e15ef7e67e0769c`; `fd4b015a7ebf06fc4f59527e677e23cd89fe17583b09a8e01e4890b7a2d1f6f0` |
| Baseline commit and artifact SHA-256 | `fb857980^` = `d9d3d36fe9eea93246d4daacd649536508e15d14`; `eb16b734b072131224f82fc72d92a8e2d250a7a1f453af89bb9a7af3bedfecf5` |
| JDK / OS / CPU | OpenJDK `25.0.3+9-2-24.04.2-Ubuntu`; Linux `6.6.87.2-microsoft-standard-WSL2`; AMD Ryzen 9 9950X, 16 cores / 32 threads |
| Focused, architecture, and full Maven suites | PASS: allocation-scope, ledger, commit-stream, deferred-expiry, memory-stat, and reply focused matrices; every Stage 7 focused-matrix class, with socket bootstrap classes rerun outside network isolation; `ArchitectureBoundaryTest`; `mvn -q -pl '!yierdis-benchmark' test` with `1,155` tests, `0` failures, `0` errors |
| Smoke and 600-second soak | PASS: `SKIP_BUILD=1 ./scripts/smoke.sh`; soak seed `20260710`, `600033` ms, `5,171` samples, report `target/production-hardening-soak/20260714T183332Z-seed-20260710/soak-20260710-1784054018385.jsonl` |
| Soak peaks | heap `456178144`; native `1933520`; maxmemory-used `2044780`; inbound reserved `338776`; commit events/bytes `46` / `334104`; outbound reserved/allocated `6970` / `2874`; reply slots/sources/chunks `1` / `0` / `1`; child channels `1`; RSS `692584448` bytes |
| Cycle baselines | Cycles 0-3 each returned to `0` native live objects, `4` native live regions, `294912` metadata-committed bytes, and `208` data-committed bytes |
| GET / SET / HSET / ZADD median ratios | NOT RUN: benchmark execution is prohibited by the active task constraint; final performance acceptance remains incomplete |
| Final inbound, commit-stream, reply, and child ownership counters | All zero: inbound reserved/read-credit/retained/consolidation; outbound reserved/allocated/slots; reply sources/chunks; child channels. Ordering sequence `85701`; delayed commit callbacks `62752`; soak failure empty |

Commands used for this candidate, all under JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl yierdis-db,yierdis-networking,yierdis-command,yierdis-server,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=OutboundMemoryBudgetTest,ConnectionReplySequencerTest,BoundedChunkedReplySinkTest,OrderedReplyPipelineTest,ReplyShutdownTest,OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest,CommitStreamIntegrationTest,CommitStreamShutdownTest,CommitStreamExpirationEvictionTest,YierdisDbMemoryReporterTest,MemoryStatsCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl yierdis-db/yierdis-db-memory,yierdis-server,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=YierdisChangeSinkTest,CommitStreamTest,YierdisInstanceTest,CommitStreamShutdownTest,YierdisServerBootstrapCloseTest,CommitStreamIntegrationTest,CommitStreamExpirationEvictionTest,CommitAwareMutationFaultInjectionTest,MutationExecutorReservationTest,YierdisDbMemoryReporterTest,MemoryStatsAccountingConsistencyTest,YierdisServerBootstrapCommandWiringTest,MemoryStatsCommandTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl '!yierdis-benchmark' -DskipTests package
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl '!yierdis-benchmark' test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 ./scripts/smoke.sh
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --duration-seconds 600 --seed 20260710 --skip-package
```

### Candidate `20bf9aca1efbdc4446003e438f40e74814cd5679` (non-benchmark acceptance)

This candidate passed every functional, ownership, smoke, soak, architecture, and full-suite gate on the frozen artifact below. The user explicitly waived Task 12 and requested that benchmark execution be ignored. This is therefore a non-benchmark acceptance record, not a claim that the original four-command 0.90 throughput gate was measured or passed.

| Field | Acceptance value |
| --- | --- |
| Candidate commit and current artifact SHA-256 | `20bf9aca1efbdc4446003e438f40e74814cd5679`; `285f8c783ab3dd8660f4519c7c9b267de6755ac1070ad1b2cd403f34201875fe` |
| Baseline commit and preserved artifact SHA-256 | `fb857980^` = `d9d3d36fe9eea93246d4daacd649536508e15d14`; `eb16b734b072131224f82fc72d92a8e2d250a7a1f453af89bb9a7af3bedfecf5` |
| JDK / OS / CPU | OpenJDK `25.0.3`; Linux `6.6.87.2-microsoft-standard-WSL2`; AMD Ryzen 9 9950X, 16 cores / 32 threads |
| Focused and architecture suites | PASS: `95` tests across `OutboundMemoryBudgetTest`, `ConnectionReplySequencerTest`, `BoundedChunkedReplySinkTest`, `OrderedReplyPipelineTest`, `ReplyShutdownTest`, `OrderedReplyIntegrationTest`, `OutboundReplyPressureTest`, `ReplyResultUnknownTest`, `CommitStreamIntegrationTest`, `CommitStreamShutdownTest`, and `ArchitectureBoundaryTest` |
| Full non-benchmark Maven suite | PASS: `mvn -q -pl '!yierdis-benchmark' test`; `981` tests, `0` failures, `0` errors |
| Package and smoke | PASS: `mvn -q -pl '!yierdis-benchmark' -DskipTests package`; frozen-JAR `SKIP_BUILD=1 ./scripts/smoke.sh` completed `PING`, `SET`, and `GET` |
| 600-second soak | PASS: seed `20260710`, `600018` ms, `5485` samples, report `target/production-hardening-soak/20260715T042536Z-seed-20260710/soak-20260710-1784089542186.jsonl` |
| Soak peaks | heap `348196424`; native `1933520`; maxmemory-used `2044780`; inbound reserved `404920`; commit events/bytes `53` / `400632`; outbound reserved/allocated `6974` / `2878`; reply slots/sources/chunks `1` / `0` / `1`; child channels `1`; RSS `554696704` bytes |
| Cycle baselines | Cycles 0-3 each returned to `0` native live objects, `4` native live regions, `294912` metadata-committed bytes, and `208` data-committed bytes |
| Final ownership counters | All zero: inbound reserved/read-credit/retained/consolidation; commit-stream reservation; outbound reserved/allocated/slots; reply sources/chunks; child channels. Ordering sequence `90889`; delayed commit callbacks `66526`; soak failure empty |
| Throughput gate | USER-WAIVED: no benchmark command, output, median, or ratio was used for this candidate |

Commands used for this candidate, all under JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl '!yierdis-benchmark' -DskipTests package
sha256sum yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  SKIP_BUILD=1 ./scripts/smoke.sh
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  ./scripts/production-hardening-soak.sh --skip-package --duration-seconds 600 --seed 20260710
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl yierdis-db,yierdis-networking,yierdis-command,yierdis-server,yierdis-tests/yierdis-integration-tests -am \
  -Dtest=OutboundMemoryBudgetTest,ConnectionReplySequencerTest,BoundedChunkedReplySinkTest,OrderedReplyPipelineTest,ReplyShutdownTest,OrderedReplyIntegrationTest,OutboundReplyPressureTest,ReplyResultUnknownTest,CommitStreamIntegrationTest,CommitStreamShutdownTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ArchitectureBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
  mvn -q -pl '!yierdis-benchmark' test
```
