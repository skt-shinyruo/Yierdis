# Redis-Benchmark-Comparable Yierdis Benchmark Rewrite Design

## Status

Approved design for a full replacement of the existing `yierdis-benchmark`
implementation.

Implementation-plan source audit correction: direct inspection of
`af293cf75bf88773f9c04e20276cff57cffa730a` confirmed that the latency
histogram samples only the first configured request count, while throughput
uses `requests_finished` at the event-loop stop boundary. It also confirmed
that AUTH/SELECT are prefixed to a connection's first measured write rather
than completed before timing. The measurement sections below record those
official semantics.

This design supersedes the benchmark direction in:

- `docs/superpowers/specs/2026-06-14-release-grade-benchmark-suite-design.md`
- `docs/superpowers/specs/2026-06-28-redis-benchmark-compatible-benchmark-design.md`
- `docs/superpowers/specs/2026-06-28-redis-suite-comparison-design.md`
- `docs/superpowers/plans/2026-06-14-release-grade-benchmark-suite.md`
- `docs/superpowers/plans/2026-06-28-redis-benchmark-compatible-benchmark.md`
- `docs/superpowers/plans/2026-06-29-redis-suite-comparison.md`

Those documents remain historical records. This document is authoritative for
the replacement benchmark.

## Decision Summary

Replace the existing benchmark instead of preserving or wrapping it.

The new benchmark:

- benchmarks only an already-running Yierdis server;
- does not start a Yierdis child process;
- does not connect to Redis;
- does not invoke or depend on the `redis-benchmark` executable at runtime;
- implements the complete official built-in `redis-benchmark` case catalog;
- sends the same command shapes and follows the same case order and state
  transitions as the official built-in suite;
- uses a single non-blocking event loop to approximate the official load
  generator architecture;
- reports the same core throughput and latency metrics;
- reports unsupported Yierdis cases explicitly without inventing zero-valued
  performance results.

Redis is benchmarked separately by the operator with the official tool. The
operator compares corresponding case results. The Yierdis benchmark neither
orchestrates that Redis run nor calculates cross-server deltas.

## Context

The current benchmark is a Yierdis-specific tool. It mixes server startup,
release suites, baseline/current comparisons, native allocator evaluation,
custom workloads, hidden preparation, warmup iterations, and custom reports.
Its default request count, client count, payload size, pipeline depth, workload
catalog, latency path, and output do not match official `redis-benchmark`.

The current suite can also drive an external Redis endpoint, but using one
Yierdis-defined workload generator against both servers is not the requested
comparison model. The desired model is:

1. Run official `redis-benchmark` against Redis.
2. Run this project benchmark against Yierdis.
3. Compare matching official built-in cases using aligned workload and metric
   semantics.

CLI identity is not the goal. Comparable server load and result semantics are.

## Goals

- Cover the complete official built-in benchmark case catalog.
- Preserve official case names, selection names, order, command arguments,
  request wire format, and state carried between cases.
- Match official defaults that affect workload shape: 100,000 requests, 50
  clients, 3-byte payloads, pipeline depth 1, keepalive enabled, and one fixed
  key when random keyspace is not requested.
- Match official random placeholder behavior, including literal fixed-key mode,
  12 decimal digits in random mode, and independent expansion for every
  `__rand_int__` occurrence.
- Match official pipeline latency semantics and histogram-based percentile
  calculation closely enough for direct result comparison.
- Use real RESP/TCP traffic against a real Yierdis server.
- Distinguish successful, unsupported, dependency-skipped, and failed cases.
- Keep benchmark code small enough that request and measurement semantics can
  be audited independently.
- Verify wire behavior, scheduling, statistics, rendering, and real-server
  execution with automated tests.

## Non-Goals

- Do not preserve any existing benchmark mode or CLI contract.
- Do not preserve release suite, baseline/current comparison, external Redis
  orchestration, native allocator evaluation, server auto-start, or old report
  artifacts.
- Do not make the Yierdis benchmark run Redis.
- Do not calculate Yierdis-versus-Redis ratios or pass/fail thresholds.
- Do not implement arbitrary positional command-template mode.
- Do not implement cluster, TLS, Unix socket, RESP3, client tracking, or
  multi-threaded load generation in the first replacement.
- Do not add hidden warmup, hidden data preparation, or automatic `FLUSHDB`.
- Do not claim that a Java load generator and the official C load generator
  have identical client-side overhead.
- Do not maintain selectable compatibility profiles for Redis releases.

## Reference Contract

The workload contract is derived from the official Redis source, specifically
`src/redis-benchmark.c`. The source inspected while approving this design was
the Redis `unstable` branch at commit
`af293cf75bf88773f9c04e20276cff57cffa730a` on 2026-07-17.

This commit identifier is provenance for tests and documentation, not a runtime
version setting. The benchmark has one catalog. When upstream changes its
built-in catalog or measurement semantics, Yierdis updates the catalog and its
golden tests in a normal project change.

## Replacement Scope

The implementation will delete the existing production and test code under
`yierdis-benchmark`, except that the fully qualified jar entrypoint
`yier.bubu.redis.app.bench.YierdisBench` remains as a thin launcher.

The replacement removes:

- `BenchHarness`;
- `BenchWorkloadKind`, `BenchWorkloadRequest`, and `BenchWorkloadResult`;
- the old benchmark and server argument models;
- all suite classes;
- comparison and native allocator benchmark code embedded in `YierdisBench`;
- tests that assert deleted modes or scripts;
- benchmark-only dependencies that supported child-server or native allocator
  behavior.

The implementation rewrites benchmark scripts, README usage, and benchmark
internals documentation. The six superseded documents listed in this design
stay in Git history and receive an explicit superseded notice.

## User-Facing Run Model

The benchmark is connect-only. A typical run is conceptually:

```bash
java -jar yierdis-benchmark-*.jar \
  --host 127.0.0.1 \
  --port 16378 \
  --requests 100000 \
  --clients 50 \
  --data-size 3 \
  --pipeline 1
```

The CLI does not need to duplicate Redis option spelling. It exposes only the
inputs needed to reproduce the built-in workload:

- `--host`, default `127.0.0.1`;
- `--port`, default `16378`;
- `--requests`, default `100000`;
- `--clients`, default `50`;
- `--data-size`, default `3`;
- `--pipeline`, default `1`;
- optional `--keyspace`; omission disables random expansion, while an explicit
  non-negative value enables it;
- `--keep-alive`, default `true`;
- `--tests`, a comma-separated list of official selection names;
- `--precision`, default `3` HdrHistogram significant digits (official output
  still renders latency fields with three decimal places);
- `--seed`, optional deterministic random seed;
- `--format`, one of `human`, `quiet`, or `csv`;
- optional username, password, and database selection for connection setup.

Authentication and database selection are connection prefix operations. They
are prepended to the first pipeline on every new connection that receives work,
matching official `redis-benchmark`. Their replies never increment benchmark
request or histogram counts, but their first-write/read delay is part of the
first batch latency and the case's elapsed time.

Invalid values and unknown test names fail before any measured request is sent.

## Official Built-In Catalog

The default run processes these 21 output cases in this order:

| Selection trigger | Result title | Request | Yierdis status at design time |
| --- | --- | --- | --- |
| `ping_inline` | `PING_INLINE` | inline `PING\r\n` | supported |
| `ping_mbulk` | `PING_MBULK` | RESP array `PING` | supported |
| `set` | `SET` | `SET key:__rand_int__ <data>` | supported |
| `get` | `GET` | `GET key:__rand_int__` | supported |
| `incr` | `INCR` | `INCR counter:__rand_int__` | supported |
| `lpush` | `LPUSH` | `LPUSH mylist <data>` | supported |
| `rpush` | `RPUSH` | `RPUSH mylist <data>` | supported |
| `lpop` | `LPOP` | `LPOP mylist` | supported |
| `rpop` | `RPOP` | `RPOP mylist` | supported |
| `sadd` | `SADD` | `SADD myset element:__rand_int__` | supported |
| `hset` | `HSET` | `HSET myhash element:__rand_int__ <data>` | supported |
| `spop` | `SPOP` | `SPOP myset` | unsupported: command absent |
| `zadd` | `ZADD` | `ZADD myzset <score> element:__rand_int__` | supported |
| `zpopmin` | `ZPOPMIN` | `ZPOPMIN myzset` | unsupported: command absent |
| any `lrange*` selector | `LPUSH (needed to benchmark LRANGE)` | `LPUSH mylist <data>` | supported |
| `lrange_100` | `LRANGE_100 (first 100 elements)` | `LRANGE mylist 0 99` | supported |
| `lrange_300` | `LRANGE_300 (first 300 elements)` | `LRANGE mylist 0 299` | supported |
| `lrange_500` | `LRANGE_500 (first 500 elements)` | `LRANGE mylist 0 499` | supported |
| `lrange_600` | `LRANGE_600 (first 600 elements)` | `LRANGE mylist 0 599` | supported |
| `mset` | `MSET (10 keys)` | ten key/value pairs | unsupported: command absent |
| `xadd` | `XADD` | `XADD mystream * myfield <data>` | unsupported: command absent |

`ping` selects both ping cases. `lrange` selects the measured LPUSH setup case
and all LRANGE variants. Selecting one LRANGE variant still selects the
measured LPUSH setup case, matching official behavior.

The LRANGE LPUSH run is not hidden fixture preparation. It is a normal measured
case with its own output row.

The catalog owns explicit support declarations and reasons. A guard test checks
that every case marked supported still maps to a registered Yierdis command and
that intentionally unsupported cases remain absent or have their classification
updated when the command is added.

## Command Template Semantics

Each catalog case contains an immutable command template. Templates support:

- raw inline bytes for `PING_INLINE`;
- RESP array encoding for all other cases;
- insertion of the configured payload;
- per-request expansion of every `__rand_int__` occurrence;
- minimum reply-shape validation.

When `--keyspace` is omitted, randomization is disabled and the literal
`__rand_int__` text remains in the request, so every execution uses the same
key or member. When `--keyspace` is supplied, every occurrence independently
resolves to a zero-padded 12-digit decimal value in `[0, keyspace)`. An explicit
zero keyspace resolves every occurrence to `000000000000`. Expansion occurs for
each concrete command in a pipeline, not once when the template is parsed.

The payload is generated once for a complete catalog pass with the official
deterministic linear-congruential data generator and reused by the same cases
that reuse it officially. `--seed` controls placeholder randomization; payload
bytes remain official and deterministic independently of that seed.

For ZADD, the score is `0` without random keyspace and independently randomized
when random keyspace is enabled. MSET contains ten independently expanded key
placeholders if Yierdis later adds MSET support.

Templates are encoded before measurement where possible. Per-request mutation
is restricted to placeholder digits and values that official behavior changes
per execution.

## State And Preparation Semantics

Cases run against the server state that exists when the benchmark starts and
leave their mutations for later cases, matching the official suite.

There is no automatic `FLUSHDB` and no hidden prefill for GET or collection
reads. In particular:

- SET precedes GET in a default full run;
- LPUSH and RPUSH affect the list consumed by LPOP and RPOP;
- SADD precedes SPOP;
- ZADD precedes ZPOPMIN;
- the measured LRANGE setup LPUSH builds the list used by LRANGE variants.

A selected case may therefore observe different data when run alone, just as it
does with official `redis-benchmark`. Operators who require isolated results
must prepare equivalent clean server instances outside the benchmark.

## Architecture

### Entrypoint And Options

`YierdisBench` parses options, validates them, resolves selected catalog cases,
invokes the runner, renders the complete result set, and maps result state to an
exit code. It contains no socket or statistics logic.

### Catalog And Case Model

The catalog supplies immutable case definitions. A case contains:

- selector and canonical result title;
- command template;
- required Yierdis command or protocol capability;
- dependency relationship, if any;
- reply validation policy;
- supported or unsupported classification and reason.

The catalog is the only source of built-in case order and support status.

### Non-Blocking Runner

The runner uses one Java NIO `Selector` thread for all benchmark connections.
This preserves the single-event-loop architecture of the official default
client more closely than one platform thread or virtual thread per connection.

Each client is an explicit state machine:

1. create a non-blocking connection and optional AUTH/SELECT prefix buffer;
2. enter the measured event loop after all client objects are registered;
3. finish connecting and write the optional prefix plus first request batch;
4. discard validated prefix replies from benchmark counters;
5. read and validate the corresponding measured replies;
6. either refill the same connection, reconnect when keepalive is disabled, or
   finish when the global request budget is exhausted.

With keepalive disabled, a client that finishes before the global reply
threshold creates its replacement before the old client is removed, as the
official `createMissingClients` path does. A late replacement may connect but
receive no work after the full-pipeline issuance budget is exhausted.

The measurement clock starts after all non-blocking client objects have been
created, immediately before the selector loop starts. Connection completion is
therefore included in elapsed time, while per-batch latency starts immediately
before that batch's first write attempt. Prefixes share the first batch write
and latency timestamp, as they do in the official client.

Request issuance follows the official pipeline boundary. A client that is
allowed to issue work always sends a complete pipeline batch. Therefore the
wire request count is `ceil(requests / pipeline) * pipeline` when the configured
request count is not divisible by the pipeline depth.

Only the first configured `requests` replies contribute to the latency
histogram. When that threshold is crossed, the runner drains the remainder of
the same client's pipeline batch and then stops the event loop. It does not wait
for replies to other already-issued client batches. Remaining connections are
closed during case cleanup. The completed count used by the official throughput
report is the actual number of validated measured replies at that stop boundary,
which can exceed the configured count for a non-divisible final pipeline.

### RESP Reply Decoder

The NIO path uses a bounded incremental RESP2 reply decoder. It recognizes
simple strings, errors, integers, bulk strings, null bulk strings, and nested
arrays without materializing payloads that are not needed for validation.

The decoder must enforce the project's RESP size limits and retain unread bytes
between selector events. Error replies are returned as structured replies and
then handled by the case policy; framing failures are runner failures.

### Reply Validation

Supported built-in cases validate enough shape to reject invalid measurements:

- PING expects `PONG`;
- SET expects `OK`;
- GET accepts a bulk or null bulk reply;
- INCR and collection writes expect integers;
- LPOP and RPOP accept bulk or null bulk replies;
- LRANGE expects an array;
- HSET, SADD, and ZADD expect integers.

Any RESP error reply aborts the current case as `FAILED`.

## Measurement Semantics

Throughput measurement starts after all non-blocking clients are created and
immediately before the selector loop, then ends when the threshold-crossing
client drains its batch. It excludes rendering, catalog resolution, and
template compilation. Like official `redis-benchmark`, elapsed time includes
non-blocking connection completion and first-use AUTH/SELECT prefixes.

Each client records a batch start timestamp immediately before writing a new
batch. Like official `redis-benchmark`, latency is captured on the first read
readiness for that batch, before reply parsing. Reply parsing overhead is not
part of measured latency. Every measured reply in the same pipeline batch
records that batch latency.

Latency values are stored in an HdrHistogram-compatible histogram using
microsecond input, official-style bounds, and three significant digits. Output
uses milliseconds and includes:

- average;
- minimum;
- p50;
- p95;
- p99;
- maximum.

Requests per second is the actual validated measured-reply count at the stop
boundary divided by measured elapsed milliseconds, matching the official
report's `requests_finished` numerator. Only the first configured number of
validated measured replies enter the latency histogram. Surplus replies in the
threshold-crossing client's batch are counted and validated before the timer
stops; other outstanding client batches are not awaited.

There is no implicit warmup. The benchmark may initialize classes, allocate
buffers, compile templates, and create non-blocking socket objects before the
timer, but it must not send unreported workload commands to warm the client or
server.

## Results And Exit Semantics

Every selected catalog case produces one result with one of four statuses:

- `SUCCESS`: complete metrics are present;
- `UNSUPPORTED`: the catalog declares a missing Yierdis capability;
- `SKIPPED`: a required official dependency did not complete;
- `FAILED`: a supported case encountered connection, protocol, reply, timeout,
  or execution failure.

`UNSUPPORTED`, `SKIPPED`, and `FAILED` never contain zero-valued synthetic
throughput or latency. Their metric fields are absent.

Unsupported cases do not make the process fail. Failed cases do. The runner
continues with later independent cases after a failure and marks dependent cases
as skipped. After rendering the complete report, the process exits non-zero if
any selected case is `FAILED`.

## Output

Human output follows official result terminology and units for successful
cases. It includes request count, elapsed time, client count, payload size,
keepalive, throughput, and the latency summary. Non-success cases retain the
canonical heading and print status plus reason instead of numeric metrics.

Quiet output uses one line per case. Successful lines use the official-style
requests-per-second summary. Non-success lines contain status and reason.

CSV keeps the official shared metric columns first:

```text
test,rps,avg_latency_ms,min_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,max_latency_ms,status,reason
```

For successful rows, the first eight fields can be compared directly with the
official CSV fields. Non-success rows keep the official case title, leave all
numeric fields empty, and populate status and reason.

The renderer is pure: it receives options and results and performs no network
or measurement work.

## Error Handling

The following fail before measurement begins:

- invalid options;
- unknown test selectors;
- invalid template definitions.

The following fail only the active supported case:

- connection establishment failure;
- AUTH or SELECT failure;
- socket disconnect or write failure;
- malformed RESP;
- RESP error reply;
- reply-shape mismatch;
- inability to complete the configured request count.

Errors retain case title, completed-versus-requested count, and the concise root
cause. Reports must never render a success summary for a failed case.

## Testing Strategy

### Catalog Golden Tests

Lock down all 21 output cases, their selection triggers, titles, order,
templates, dependencies, and current support declarations. A catalog change
must be deliberate and reviewable.

### Template And Wire Tests

Assert exact bytes for inline PING and every RESP command template. Verify
payload length, literal fixed-key mode, 12-digit random expansion, independent
placeholders, deterministic seed behavior, and official full final pipelines.

### Scripted Server Tests

A bounded local server records incoming frames and emits controlled RESP
replies. Tests cover:

- configured, histogram, and wire request counts across uneven client and
  pipeline counts;
- one event loop serving many clients;
- keepalive and reconnect behavior;
- prefix reply exclusion from request and histogram counts, while retaining
  official first-batch timing;
- partial writes and fragmented replies;
- multiple replies in one read;
- RESP errors, malformed replies, and disconnects;
- dependency skip behavior.

### Deterministic Measurement Tests

Inject a monotonic clock and deterministic selector events. Verify start and end
boundaries, official pipeline batch latency semantics, histogram summaries, RPS,
and absence of metrics for non-success states.

### Renderer Golden Tests

Lock human, quiet, and CSV output for success, unsupported, skipped, and failed
cases. Check that the first eight CSV columns retain official names and units.

### Command Support Guard

Compare supported catalog requirements with the current Yierdis command
composition. A removed command fails the guard. Adding SPOP, ZPOPMIN, MSET, or
XADD requires updating the corresponding catalog status and integration test.

### Real Yierdis Acceptance

Start Yierdis outside the benchmark and run all cases with a small request
budget. All currently supported cases must succeed. SPOP, ZPOPMIN, MSET, and
XADD must be reported as unsupported without network traffic for those cases.

### Optional Official Differential Check

A developer-only acceptance check may run official `redis-benchmark` and the
Yierdis benchmark against a recording server, then compare command traces and
measurement inputs. The official executable is never a Maven test or runtime
dependency.

## Documentation And Script Migration

- Rewrite `scripts/bench.sh` to package the benchmark and connect to a caller-
  managed Yierdis endpoint.
- Update smoke scripts that call deleted benchmark arguments.
- Replace README benchmark examples with the new connect-only flow.
- Rewrite `docs/project-docs/client-and-bench-internals.md` around the catalog,
  NIO runner, timing boundary, and result statuses.
- Mark obsolete benchmark design and plan documents as superseded.

## Acceptance Criteria

The rewrite is complete when:

- no deleted benchmark mode remains reachable or documented as current;
- the default run contains the 21 official built-in cases in canonical order;
- the 17 currently supported case rows execute successfully against Yierdis;
- SPOP, ZPOPMIN, MSET, and XADD are explicit unsupported rows;
- wire golden tests pass for every case;
- configured request totals, full-pipeline wire totals, random expansion,
  pipeline behavior, and latency boundaries pass deterministic tests;
- human, quiet, and CSV output tests pass;
- the full benchmark module and affected integration tests pass on JDK 25;
- a packaged jar completes a small real-server acceptance run;
- supported results expose directly comparable official metric names and units.

## Known Limitation

Comparable workload and measurement semantics do not eliminate load-generator
implementation cost. Official `redis-benchmark` is a C program; this benchmark
is Java. The single NIO event loop, pre-measurement initialization, exact wire
templates, and aligned timing semantics reduce avoidable differences, but the
two clients cannot be expected to produce numerically identical results against
the same server. The comparison remains a server-performance comparison with a
documented client-side implementation difference.
