# Redis-Benchmark-Compatible Benchmark Design

## Status

Approved design for a breaking-change refactor that turns the default
`yierdis-benchmark` entrypoint into a `redis-benchmark`-style tool.

## Context

`yierdis-benchmark` already drives real RESP/TCP traffic against a real server.
It can start a server process, connect to an existing server, write RESP
requests, read replies, and report throughput and latency. That makes it a good
local benchmark and smoke tool.

The current tool is not shaped like `redis-benchmark`. Its default CLI is
Yierdis-specific, its workload catalog is hardcoded around a small set of
single-run workloads, and its output is not intended to line up with
`redis-benchmark` output. That makes direct result comparison awkward even when
the underlying workload shape is close.

The goal of this redesign is not to compare Yierdis against Redis inside the
tool. The goal is to make Yierdis produce benchmark runs that look and behave
as close as practical to `redis-benchmark`, so operators can run equivalent
commands against both systems and compare the results directly.

This redesign is intentionally a breaking change. The old single-run benchmark
CLI does not need compatibility preservation.

## Goals

- Make the default `yierdis-benchmark` entrypoint behave like
  `redis-benchmark`.
- Align core CLI flags, mode selection, and output format with
  `redis-benchmark`.
- Support both built-in benchmark cases and arbitrary user-provided command
  templates.
- Cover all Yierdis-supported commands in the benchmark catalog, even if not
  all of them belong in the default run set.
- Keep Redis-overlapping benchmark names and result titles as close as
  practical to official `redis-benchmark`.
- Reuse one execution engine for built-in cases and raw command-template mode.
- Preserve real RESP/TCP execution rather than introducing an in-process fast
  path.
- Fail fast on benchmark errors so results do not silently look successful when
  the run is invalid.
- Add guardrails that force the benchmark catalog to stay in sync with the
  project's supported command set.

## Non-Goals

- Do not preserve the old default single-run Yierdis-specific CLI.
- Do not make the first version support every `redis-benchmark` feature.
- Do not emulate unsupported Redis server features inside Yierdis just so the
  benchmark can expose their flags.
- Do not treat benchmark output as a correctness oracle that replaces command,
  protocol, or storage tests.
- Do not add hidden warmup behavior to the default compatibility path.
- Do not make raw command-template mode infer business setup beyond explicit
  connection prefix commands such as `AUTH` and `SELECT`.

## Approved Direction

Three approaches were considered:

- Thin compatibility wrapper over the existing benchmark internals.
- Dual-mode tool that preserves the old CLI and adds a Redis-compatible mode.
- Full refactor with a Redis-compatible default entrypoint and explicit
  non-default advanced modes.

The approved direction is the full refactor. The benchmark jar becomes
Redis-benchmark-first. Existing Yierdis-specific modes remain available only as
explicit subcommands or explicit advanced modes, not as the default entry path.

## Default CLI And Mode Boundary

The default `main(...)` path must parse and behave like `redis-benchmark`
instead of the current `YierdisBenchArgs` single-run model.

The compatibility path owns the default entrypoint:

```bash
java -jar yierdis-benchmark-*.jar
java -jar yierdis-benchmark-*.jar -h 127.0.0.1 -p 6379 -n 100000 -c 50
java -jar yierdis-benchmark-*.jar -t set,get --csv
java -jar yierdis-benchmark-*.jar -r 10000 -n 10000 lpush mylist __rand_int__
```

Like `redis-benchmark`, the default compatibility path is connect-only. It
targets an already running server and does not auto-start a Yierdis child
process. Any auto-started local-server workflow must move to an explicit
non-default advanced mode.

The current non-default capabilities stay available, but only behind explicit
entrypoints such as subcommands or clearly separated advanced modes. That
includes:

- release-grade suite execution
- baseline/current comparison mode
- native allocator evaluation
- Yierdis-specific diagnostics that do not belong in Redis-compatible output

The old single-run parameter style does not need compatibility support.

## Compatibility Scope

The first version should align with the official semantics of the following
`redis-benchmark` options and behaviors as closely as practical:

- `-h <hostname>`
- `--help`
- `--version`
- `-p <port>`
- `-a <password>`
- `--user <username>`
- `-u <uri>`
- `-c <clients>`
- `-n <requests>`
- `-d <size>`
- `--dbnum <db>`
- `-k <boolean>`
- `-r <keyspacelen>`
- `-P <numreq>`
- `-q`
- `--precision`
- `--csv`
- `-l`
- `-t <tests>`
- `-I`
- `-x`
- default command-template mode when positional command arguments are supplied
- `__rand_int__` replacement semantics

`-h` remains the host flag, matching `redis-benchmark`. Help is exposed through
`--help`, not `-h`.

The first version must explicitly reject unsupported flags instead of silently
ignoring them. Unsupported first-version options include:

- `-s <socket>`
- `-3`
- `--threads`
- `--cluster`
- `--enable-tracking`
- TLS-related options
- any feature that depends on Redis server capabilities Yierdis does not expose

Clear error reporting is required for unsupported flags so users do not confuse
Yierdis limitations with parser bugs.

## Architecture

The benchmark module should be split into explicit components with clear
responsibility boundaries.

### Compatibility CLI Layer

This layer parses Redis-compatible arguments, detects whether the run is using
built-in tests or raw command-template mode, validates incompatible flag
combinations, and resolves execution into a normalized benchmark specification.

### Unified Benchmark Spec

All benchmark work, including built-in tests and arbitrary user templates,
should compile into the same internal spec model. The spec defines:

- display title
- test id
- request template
- placeholder expansion rules
- connection prefix commands
- optional preparation plan
- reply success policy
- reporting mode metadata

This normalized model keeps the runner agnostic to whether the case came from a
catalog entry or user-provided command arguments.

### Command Template Engine

This engine builds RESP command templates from either:

- built-in benchmark case definitions
- positional command arguments supplied by the user

It is responsible for placeholder detection and per-execution expansion,
especially `__rand_int__`.

The request template abstraction must support both of these wire encodings:

- RESP array requests for normal built-in and raw command-template cases
- raw inline request bytes for official compatibility cases that require it,
  especially `PING_INLINE`

### Benchmark Runner

The runner owns connection setup, request scheduling, pipelining, keepalive or
reconnect policy, request counting, reply validation, latency capture, and
abort behavior.

Built-in cases and raw template mode must share this runner.

### Renderers

Output renderers produce:

- default human-readable detailed output
- `-q` quiet output
- `--csv` output

Redis-overlapping built-in tests should use titles and formatting that stay as
close as practical to official `redis-benchmark`.

### Explicit Advanced Modes

Existing Yierdis-specific suite/comparison/native-eval behavior should be moved
behind explicit entrypoints that do not pollute the default compatibility path.

## Command Classes

The design uses two command classes:

### Built-In Catalog Cases

These are stable benchmark definitions shipped with Yierdis. They provide:

- canonical Redis-style titles
- known key naming schemes
- optional preparation plans
- known reply-shape expectations
- inclusion in the default test set or optional named catalog

### Raw Command-Template Cases

These are benchmark cases built from user-supplied positional command
arguments. They provide:

- a title derived from the supplied command line
- RESP encoding of the supplied argv
- placeholder expansion such as `__rand_int__`
- minimal validation only

Raw template mode must not attempt to infer business setup beyond connection
prefix commands. The user is responsible for ensuring the chosen command can run
meaningfully.

## Unified Execution Model

Both built-in and raw command-template cases compile into a `BenchmarkCase`
model and go through the same four execution phases.

### Prepare

Optional benchmark preparation runs before measurement. Preparation commands are
not counted as benchmark requests and do not contribute to latency or
throughput.

Preparation is declared by the case itself through a `PreparationPlan`. The
plan uses the same RESP command-template infrastructure as measured commands
instead of maintaining a separate imperative setup system.

### Connect

Each client connection performs protocol-level prefix commands such as `AUTH`
and `SELECT` before measured traffic starts. These commands are not included in
benchmark statistics.

This mirrors the `redis-benchmark` prefix-command model.

### Run

Measured traffic is sent according to the normalized case definition and run
flags:

- pipeline depth
- keepalive or reconnect policy
- total request count
- loop mode
- idle mode
- random keyspace expansion

`__rand_int__` replacement must happen when a concrete command execution is
emitted, not when the template is parsed. Multiple `__rand_int__` occurrences
in the same command are treated as independently randomized placeholders.

### Record

Only measured commands contribute to throughput and latency statistics.
Preparation and connection-prefix commands never do.

## Preparation And Data Fixtures

Preparation should be lazy, reusable, and explicit.

Built-in cases that need existing data, such as `GET`, `HGET`, `SMEMBERS`,
`ZRANGE`, `PFCOUNT`, or `SCAN`, must declare the exact fixture shape they
require. The preparation engine builds it once and allows compatible cases to
reuse it within the same benchmark run when safe.

Default preparation must avoid `FLUSHDB` unless the chosen advanced mode
requires isolation. The compatibility path should favor stable benchmark key
namespaces rather than destructive global cleanup.

Redis-overlapping built-in cases should use official-style key/value patterns
where practical. Yierdis-specific extensions should use their own namespace so
they do not masquerade as official Redis defaults.

Raw command-template mode performs no benchmark-specific data preparation by
default.

## Warmup Policy

The default compatibility path must not add an implicit warmup stage.

This keeps the result semantics closer to official `redis-benchmark`, reduces
surprising hidden work, and avoids changing the meaning of runs that users
expect to compare directly against Redis.

If Yierdis later needs optional warmup for internal diagnostics, it must be
behind a non-default explicit flag or advanced mode.

## Built-In Test Catalog

The built-in catalog should cover every Yierdis-supported command that has a
meaningful benchmark shape, but not every command belongs in the default run
set.

The catalog should classify each supported command into one of four states:

- compatibility default case
- optional built-in case
- raw-template-only support
- explicitly excluded with a documented reason

This classification is required for coverage enforcement.

### Compatibility Default Set

The no-argument default run should stay as close as practical to the official
Redis default benchmark set, limited to commands that Yierdis actually
supports.

The compatibility default set should prefer Redis-overlapping cases such as:

- `PING_INLINE`
- `PING_BULK`
- `SET`
- `GET`
- `INCR`
- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `SADD`
- `HSET`
- `ZADD`
- Redis-style derived cases such as `LRANGE_*` where fixture-backed variants
  are needed

When an official compatibility case depends on a specific request wire format,
the built-in implementation must use that format. In particular,
`PING_INLINE` must send inline protocol bytes rather than a RESP array request.

If official default Redis cases rely on unsupported commands, those cases
should be omitted rather than replaced by Yierdis-specific alternatives.

### All-Supported Catalog

The broader built-in catalog should cover current registered command families in
the repository, including:

- string commands
- list commands
- hash commands
- set commands
- zset commands
- HLL commands
- keyspace commands
- connection commands
- transaction commands
- server info commands

Examples:

- benchmark-friendly built-in cases:
  `GET`, `APPEND`, `SETBIT`, `GETBIT`, `BITCOUNT`, `HGET`, `HLEN`,
  `SMEMBERS`, `SISMEMBER`, `SCARD`, `ZRANGE`, `ZREVRANGE`,
  `ZRANGEBYSCORE`, `PFADD`, `PFCOUNT`, `PFMERGE`, `DEL`, `EXISTS`,
  `TTL`, `PTTL`, `SCAN`
- catalog-only but default-disabled management or connection cases:
  `HELLO`, `INFO`, `STATS`, `CLIENT`, `COMMAND`, `SELECT`, `FLUSHDB`,
  `MULTI`, `EXEC`, `DISCARD`, `QUIT`, `AUTH`

Management and connection-oriented commands should be benchmarkable only when
explicitly selected. They do not belong in the default compatibility run.

### Test Naming

Redis-overlapping tests should use titles and test keys that stay as close as
practical to official `redis-benchmark`.

Yierdis-specific or non-official catalog entries should use a Redis-like naming
style without pretending to be part of the official Redis default set.

If the user supplies a raw command template, `-t` is ignored just as it is in
official `redis-benchmark`.

## Error Handling And Failure Semantics

The compatibility path should follow `redis-benchmark` fail-fast behavior.

The benchmark must not continue producing apparently valid throughput numbers
after connection or command failures in the measured path.

### Pre-Run Failures

The process exits non-zero before benchmark statistics are reported when any of
the following fail:

- CLI validation
- template compilation
- unsupported flag validation
- connection establishment
- `AUTH` or `SELECT`
- case preparation
- fixture verification needed by a built-in case

### Measured-Run Failures

The process exits non-zero and aborts the current benchmark case when any of
the following happen in the measured path:

- RESP error reply
- socket disconnect before completion
- write failure
- reply parse failure
- built-in case reply-shape mismatch

Built-in cases should validate only the minimum expected reply shape needed to
protect benchmark integrity. Examples:

- `INCR` expects integer
- `GET` expects bulk string or null bulk
- `LRANGE` expects array
- `SET` expects simple string `OK`

Raw command-template mode should treat RESP error replies as failures but should
otherwise accept any valid non-error RESP reply shape.

### Statistics Boundary

Only measured benchmark commands contribute to:

- requests completed
- throughput
- latency percentiles

Preparation, `AUTH`, `SELECT`, `HELLO`, and similar setup traffic never
contribute to those statistics.

### Continue-On-Error

The first version must not add a default continue-on-error mode.

If a future diagnostic mode needs this capability, it must be an explicit
Yierdis-specific extension and must not change default compatibility semantics.

## Output Compatibility

Output modes should stay close to official `redis-benchmark` while still being
honest about Yierdis-specific unsupported areas.

### Default Human Output

The default human-readable output should mirror official benchmark structure as
closely as practical for:

- test headings
- request count reporting
- client and payload reporting
- latency percentile summaries
- throughput summary

### Quiet Output

`-q` should emit Redis-style single-line summaries per test.

### CSV Output

`--csv` should emit a stable CSV header and one result row per test. Redis
overlapping fields should use Redis-compatible naming where practical.

### Failure Output

Failures must not print a misleading final success summary. Instead they should
report an explicit aborted state with the failure reason and, when useful, the
completed-versus-requested count at the time of abort.

## Unsupported Features

Unsupported Redis-benchmark features must be reported explicitly and early.

The first version should reject, not ignore:

- unix socket mode
- RESP3-only benchmark startup flags
- multi-thread benchmark client mode
- cluster mode
- client tracking enablement
- TLS-specific benchmark flags

This preserves trust in the benchmark CLI.

## Testing And Acceptance Strategy

The redesign should be accepted through four test categories.

### CLI Compatibility Tests

These tests validate:

- default no-argument mode selection
- Redis-compatible flag parsing
- conflict handling
- raw command-template mode activation on positional command arguments
- unsupported-option rejection
- `-t` ignoring when positional command templates are used

These tests verify parser and mode semantics, not performance numbers.

### Execution Model Tests

These tests validate:

- benchmark case compilation from built-in catalog entries
- benchmark case compilation from raw templates
- `__rand_int__` expansion timing and range behavior
- pipeline cloning behavior
- keepalive and reconnect policy handling
- prefix commands excluded from measurement
- preparation-plan execution and reuse
- fail-fast abort on RESP error, disconnect, and parse failure
- built-in reply-shape validation

### Output Golden Tests

These tests lock down:

- default detailed output format
- `-q` output shape
- `--csv` header and row format
- Redis-overlapping built-in titles
- Yierdis extension title formatting

The goal is structural compatibility, not identical numeric content.

### Command Coverage Guard

An automated guard must compare the benchmark catalog classification against the
project's supported command registry or equivalent command metadata source.

Every registered command must map to one of:

- compatibility default case
- optional built-in case
- raw-template-only support
- explicit exclusion with a documented reason

When a new command is added to the server and the benchmark catalog is not
updated, the guard must fail.

### End-To-End Acceptance

A small real-server acceptance run should verify:

- the compatibility CLI can start and connect to a real Yierdis server
- representative command families execute successfully
- explicit test selection runs the intended cases
- output format is emitted in human, quiet, and CSV modes
- fail-fast behavior produces a non-zero exit when a measured case errors

These runs should validate semantics and formatting, not absolute QPS targets.

## Migration Of Existing Benchmark Features

The existing benchmark module contains useful infrastructure that should be
reused where possible:

- RESP command writing
- reply decoding
- server process management
- readiness probing
- existing benchmark worker patterns
- strict reply validation concepts

Existing advanced benchmark features that are not part of Redis compatibility,
such as suite execution, comparison mode, and native allocator evaluation,
should remain available behind explicit non-default entrypoints.

## Documentation Requirements

Documentation must be updated together with the refactor:

- benchmark usage documentation should describe the Redis-compatible default
  path first
- advanced Yierdis-specific modes should be documented separately
- unsupported Redis-benchmark features should be listed explicitly
- raw command-template examples should show `__rand_int__` usage
- command-catalog documentation should explain which cases are default,
  optional, raw-template-only, or excluded

## References

- Redis benchmark overview:
  <https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/>
- Redis official source:
  <https://github.com/redis/redis/blob/unstable/src/redis-benchmark.c>
