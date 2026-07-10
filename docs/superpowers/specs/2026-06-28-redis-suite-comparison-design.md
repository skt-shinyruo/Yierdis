# Redis Suite Comparison Design

## Status

Approved design for adding Redis as a first-class comparison target to the
existing release-grade benchmark suite. This design is intentionally separate
from the Redis-benchmark-compatible entrypoint design. The suite comparison work
comes first because it is better suited to finding which Yierdis workloads lag
behind Redis.

## Context

`yierdis-benchmark` already has a suite mode that runs fixed scenarios, starts a
fresh Yierdis server for each scenario and artifact, records warmup and repeat
iterations, captures observations, and writes JSON, CSV, and Markdown reports.

The current suite compares Yierdis artifacts only. `baselineServerJar` and
`currentServerJar` are both Java server jars. Redis can be benchmarked manually
with `--noStartServer`, but that path does not produce a combined Redis vs
Yierdis report and does not encode comparability rules.

The goal is to let one suite run compare Redis and Yierdis with the same
scenario shape so the report highlights where Yierdis is slower, noisier, or not
semantically comparable.

This design extends the existing release-grade suite rather than replacing it.
It inherits the existing suite runner, scenario model, report artifacts, and
soft-threshold posture, then makes target selection, observation capture, and
comparability target-aware.

## Goals

- Add Redis as an explicit suite comparison target.
- Keep the existing suite report artifacts: `suite-result.json`, `metrics.csv`,
  `comparisons.csv`, and `report.md`.
- Run identical scenario shapes for Redis and Yierdis: requests, clients,
  pipeline, keyspace, data size, warmup count, repeat count, and latency mode.
- Mark each Redis/Yierdis scenario pair as comparable only when both sides
  complete clean workload measurements with no reply/protocol errors.
- Record clear non-comparable reasons when Redis does not support a command,
  a setup step fails, a measurement is missing, or either side records errors.
- Treat Yierdis-specific observation commands as optional for Redis so
  observability differences do not invalidate otherwise clean workload results.
- Preserve the existing Yierdis baseline/current suite behavior.
- Document recommended Redis runtime settings so results are interpretable.

## Non-Goals

- Do not make Redis the default suite target.
- Do not auto-start or configure Redis in the first version.
- Do not emulate Redis-only features in Yierdis or Yierdis-only observability in
  Redis.
- Do not replace the separate `redis-benchmark` compatibility design.
- Do not claim Redis drop-in compatibility from benchmark success.
- Do not add hidden warmup behavior outside the existing scenario warmup model.
- Do not add a three-way Redis/baseline/current comparison in the first
  version.

## Approved Direction

Three approaches were considered:

- Keep Redis outside suite mode and rely on manual `--noStartServer` runs plus
  ad hoc spreadsheet comparison.
- Add Redis as an explicit external suite artifact while preserving the current
  suite runner, workload engine, and report formats.
- Build a larger three-way suite that compares Redis, baseline Yierdis, and
  current Yierdis in one run.

The approved direction is the second option.

Manual comparison is too easy to misuse because it does not encode scenario
shape equivalence, clean-run rules, or stable non-comparable reasons. The
three-way suite adds significant complexity to CLI validation, port allocation,
report rendering, and review burden before the core Redis-vs-current signal is
working.

The first version therefore adds Redis as a non-default external artifact that
can participate in the same suite flow as current Yierdis. The suite remains a
two-side comparison in Redis mode: Redis is the baseline side, current Yierdis
is the current side, and scenario comparability is decided per scenario.

## CLI

Suite mode gains an external Redis artifact controlled by explicit options:

```bash
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --suite \
  --suiteProfile release \
  --includeRedis \
  --redisHost 127.0.0.1 \
  --redisPort 6379 \
  --currentServerJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --reportDir target/benchmark-reports/redis-comparison
```

Options:

- `--includeRedis`: enable Redis comparison in suite mode.
- `--redisHost <host>`: Redis endpoint host, defaulting to `127.0.0.1`.
- `--redisPort <port>`: Redis endpoint port, defaulting to `6379`.
- `--redisLabel <label>`: report label, defaulting to `redis`.
- `--redisAuth <password>`: optional password for Redis `AUTH`.
- `--redisUser <username>`: optional username for Redis ACL authentication.
- `--redisDb <index>`: optional Redis DB selected before each scenario pass,
  defaulting to `0`.

Validation rules:

- Redis options are valid only with `--suite`.
- `--includeRedis` still requires `--currentServerJar` because the run is a
  Redis vs current-Yierdis comparison.
- `--baselineServerJar` may not be combined with `--includeRedis` in this
  design. Three-way Redis/baseline/current comparison is out of scope for this
  spec.
- Redis labels must not collide with existing artifact labels.
- `--redisPort` must be a valid TCP port.
- `--redisDb` must be a non-negative logical DB index.

If `--reportDir` is omitted, suite mode keeps the existing generated report
directory behavior under `target/benchmark-reports/`.

## Architecture

This design stays inside the existing `yierdis-benchmark` suite architecture.
It does not create a separate Redis-specific runner. Instead it makes the suite
artifact model, harness lifecycle, observation capture, and comparison logic
target-aware.

### Target-Aware Suite Model

Add an artifact kind instead of assuming every artifact is a Yierdis jar.

```text
SuiteArtifact
  label
  kind: YIERDIS_JAR | EXTERNAL_REDIS
  jarPath: only for YIERDIS_JAR
  endpoint: only for EXTERNAL_REDIS
  auth: optional, only for EXTERNAL_REDIS
  db: optional, only for EXTERNAL_REDIS
```

Yierdis-only suite runs keep the current baseline/current behavior. Redis suite
mode builds an ordered artifact list of `redis` then `current`.

The suite runner, pass result model, metric summaries, JSON writer, CSV writer,
and Markdown writer remain shared. Redis mode is not a separate reporting stack.

`ScenarioDefinition` gains Redis compatibility metadata so scenario-level rules
are explicit in data rather than hidden in runner exceptions.

### Runner Flow

Redis mode follows the same high-level suite flow as Yierdis-only mode:

1. Parse and validate suite arguments.
2. Capture environment and artifact metadata.
3. Expand the selected suite profile into stable scenarios.
4. For each scenario and each artifact in run order:
   - prepare the target for that scenario pass;
   - capture before observations;
   - run warmup iterations;
   - run repeat iterations;
   - capture after observations;
   - clean up the target for that pass.
5. Aggregate repeat metrics.
6. Build scenario comparisons only for the two participating artifacts.
7. Apply existing soft-threshold logic.
8. Write JSON, CSV, and Markdown artifacts.

Redis mode changes target preparation, not benchmark semantics. Warmup,
iteration recording, metric aggregation, comparison calculations, and report
emission remain shared.

### Target Lifecycle

Yierdis artifacts keep the existing start/stop lifecycle. Redis artifacts use an
already running endpoint. Both sides are represented through the same
`RunningServer` shape so iteration execution can stay shared.

For Redis passes, `startServer` means:

1. Validate the Redis endpoint is reachable with `PING`.
2. Authenticate if configured.
3. Select the configured DB if configured.
4. Run `FLUSHDB` before the scenario so each pass starts clean.
5. Return a `RunningServer` with no process handle.

For Redis passes, `stopServer` is a no-op.

The suite must not try to auto-start `redis-server`, rewrite Redis
configuration, or manage Redis persistence settings in the first version.

### Endpoint And Port Rules

The configured Redis endpoint is authoritative for Redis passes. The suite does
not allocate a generated suite port for Redis and does not assume Redis can be
rebound onto the suite's `portBase` sequence.

Generated suite ports remain relevant for jar-backed Yierdis artifacts. Redis
mode therefore reduces the number of generated Yierdis ports needed relative to
baseline/current Yierdis comparison mode.

### Observation Capture

The observation client becomes target-aware. A Yierdis target keeps the current
behavior. A Redis target uses Redis-safe observation commands and records command
errors as observation payloads instead of pass failures.

This preserves a low-interference before/after observability model without
pretending that Redis and Yierdis expose identical operational surfaces.

### Reuse Of Existing Code

The implementation should avoid rewriting the current benchmark behavior. It
should progressively extract or adapt:

- suite argument validation;
- artifact construction and run ordering;
- existing RESP command writer and reader;
- existing throughput and latency workers;
- strict reply validation;
- existing result summaries;
- existing comparison status rules and report writers.

The existing Yierdis-only suite path must keep its current behavior unless a
specific compatibility change is approved separately.

## Scenario Compatibility

The first version should attempt all existing suite scenarios against Redis, but
comparability is decided per scenario. That gives the report useful signal even
when one scenario is unsupported or environment-dependent.

Expected comparable scenario groups:

- `PING`
- `SET_GET`
- `APPEND`
- `HLL_SPARSE`
- `HLL_DENSE`
- `HLL_PFCOUNT`
- `TTL_EXPIRATION`
- `LIST_LPUSH`
- `HASH_HSET`
- `SET_SADD`
- `ZSET_ZADD`
- `SCAN`
- `MIXED_READ_WRITE`

Potentially environment-sensitive scenario groups:

- `MAXMEMORY_EVICTION`: comparable only when the operator configures Redis with
  matching maxmemory and eviction policy outside the benchmark. If not
  configured, the scenario should be marked non-comparable and the report should
  explain that Redis memory policy is externally controlled.
- `NATIVE_DEFRAG_APPEND`: Yierdis-specific. It should not be treated as
  comparable against Redis in Redis comparison mode.

The scenario definition should carry a compatibility classification:

```text
redisComparable: YES | EXTERNAL_CONFIG_REQUIRED | NO
redisNonComparableReason: optional text
```

This avoids burying compatibility logic in worker exceptions.

For `redisComparable = NO`, the suite still keeps the scenario in the expanded
profile so the report stays structurally complete. The Redis side may fail
quickly, may produce a measurement, or may only produce observations depending
on the workload shape, but the pair is never considered comparable.

For `redisComparable = EXTERNAL_CONFIG_REQUIRED`, the first version treats the
pair as non-comparable even if both sides appear numerically clean. A future
design may add an explicit operator acknowledgement flag, but this version does
not infer that external Redis configuration matched Yierdis semantics.

## Observation Capture

Current suite observation captures `STATS`, `MEMORY STATS`, and `INFO`.

For Redis:

- `INFO` should be captured.
- `MEMORY STATS` should be captured when available.
- `STATS` should not be required because it is Yierdis-specific.
- Observation command errors should be recorded in the observation payload, not
  treated as workload failure.

The observation payload should keep stable command keys such as `INFO` and
`MEMORY STATS` even when the value is an error string. This keeps JSON and
Markdown summaries deterministic enough for tests.

## Workload Execution

Workload execution should remain shared through the existing RESP/TCP worker
path. The benchmark should not shell out to `redis-benchmark` for suite mode.

Shared execution requirements:

- Use the same `BenchWorkloadRequest` shape for Redis and Yierdis.
- Run the same preparation logic where the command is Redis-compatible.
- Use strict reply validation for both Redis and Yierdis in suite mode.
- Count RESP error replies as benchmark errors.
- Keep latency measurement semantics unchanged: latency scenarios use pipeline
  depth `1`, throughput scenarios use the scenario pipeline.

Preparation rules:

- Redis passes should run `FLUSHDB` before each scenario pass.
- Dense HLL preparation should use the same `PFADD` / `PFMERGE` flow when the
  scenario is Redis-compatible.
- If a preparation step fails on Redis, the pass is failed and the scenario pair
  becomes non-comparable.
- Setup commands such as `AUTH`, `SELECT`, and `FLUSHDB` are not benchmarked and
  do not contribute to throughput or latency statistics.

The benchmark must not add Redis-specific fast paths that bypass the existing
RESP/TCP execution model.

## Metrics And Comparability

Redis comparison mode keeps the existing suite metric families and repeat
aggregation rules. The Redis extension changes when a pair is allowed to produce
comparison deltas, not which metrics are collected.

Comparable requires all of the following:

- both Redis and Yierdis passes completed;
- all required metrics are present;
- workload error count is zero on both sides;
- strict reply validation did not fail;
- scenario compatibility is `YES`;
- the scenario shape matches on both sides.

Observation command failures alone do not make a scenario dirty if the workload
measurement itself stayed clean.

`delta_percent` continues to use the suite-wide formula:

```text
((current - baseline) / baseline) * 100
```

In Redis mode, the baseline side is Redis and the current side is current
Yierdis.

`ratio` should be rendered as:

```text
current / baseline
```

That keeps interpretation simple across report formats:

- throughput ratio `< 1.0` means Yierdis is slower than Redis;
- throughput ratio `> 1.0` means Yierdis is faster than Redis;
- latency ratio `> 1.0` means Yierdis is slower than Redis;
- latency ratio `< 1.0` means Yierdis is faster than Redis.

Non-comparable reasons should be concise and stable enough for tests to assert.

## Reporting

`suite-result.json` should include Redis artifacts, passes, observations,
failures, and comparisons using the same top-level model as Yierdis-only suite
runs.

Report writers should distinguish between:

- performance regression warning;
- measurement failure;
- non-comparable result;
- explanatory observability signal.

### suite-result.json

The JSON file remains the fact source. In Redis mode it should additionally
preserve:

- artifact kind and endpoint metadata for Redis artifacts;
- scenario Redis comparability metadata;
- before/after observation payloads with Redis-safe command keys;
- pass failure messages from Redis readiness or setup failures;
- comparison-level non-comparable reasons.

Auth secrets must not be emitted into the JSON report.

### metrics.csv

`metrics.csv` should include an `artifact` column with values such as `redis`
and `current`.

The CSV remains a flattened metric table, not a comparison-only table.

### comparisons.csv

`comparisons.csv` should compare Redis to current Yierdis for each scenario:

```text
scenario_id,baseline_artifact,current_artifact,metric,baseline_value,current_value,delta_percent,ratio,comparable,reason
```

For Redis comparison mode, use Redis as the baseline artifact and current
Yierdis as the current artifact.

### report.md

`report.md` should add a Redis comparison summary section with:

- worst Yierdis throughput ratios vs Redis;
- worst Yierdis p95/p99 latency ratios vs Redis;
- scenarios with errors;
- non-comparable scenarios and reasons;
- environment notes, including that Redis was externally configured.

The Markdown report should make the baseline/current direction explicit so a
reader can tell that `redis -> current` means Redis is the baseline side and
Yierdis is the comparison side.

## Environment Metadata

Extend suite environment capture with Redis endpoint metadata when Redis is
included:

- Redis host and port.
- Redis logical DB index.
- Redis `INFO server` fields when available, including version.
- Redis `INFO persistence` fields when available, because persistence settings
  materially affect write benchmarks.
- Redis `INFO memory` fields when available.

The benchmark should not infer that Redis and Yierdis are equally configured.
The report must state that Redis runtime configuration is operator-managed.

Recommended manual Redis settings for performance comparison should be
documented:

```text
save ""
appendonly no
maxmemory-policy noeviction   # except explicit eviction scenarios
```

For eviction scenarios, document that Redis should be started with a matching
`maxmemory` and `maxmemory-policy` or the scenario should be treated as
non-comparable.

Auth usernames and passwords must never be written into environment metadata or
report artifacts.

## Error Handling

The suite should fail configuration validation for invalid Redis CLI option
combinations.

During execution, Redis endpoint failures should fail only the affected Redis
pass and make its comparisons non-comparable.

More specifically:

- invalid argument combinations fail before any suite pass starts;
- Redis `PING`, `AUTH`, `SELECT`, or `FLUSHDB` failures fail the Redis pass;
- a Redis workload exception records a failed scenario pass and the suite moves
  on to the next pass;
- observation command errors are recorded inside observations and do not by
  themselves fail the pass;
- report writing is still attempted when some Redis passes fail.

Scenarios classified as `EXTERNAL_CONFIG_REQUIRED` are non-comparable in this
version unless a future spec adds an explicit operator acknowledgement option.

## Documentation Requirements

Update user-facing documentation alongside the implementation.

`README.md` should include:

- the Redis suite comparison command line;
- the meaning of `--includeRedis` and the Redis endpoint options;
- the recommended Redis runtime settings for fair comparison;
- the warning that Redis is externally managed and `FLUSHDB` runs before every
  Redis scenario pass.

`docs/project-docs/client-and-bench-internals.md` should describe:

- how Redis suite mode reuses the existing RESP/TCP benchmark engine;
- how target-aware observation capture works;
- how comparability is decided;
- why externally configured Redis scenarios stay non-comparable in the first
  version.

The first version does not require `scripts/bench.sh` support. If script support
is added later, its environment-variable contract should be documented and
tested separately.

## Testing Strategy

Focused benchmark-module tests:

- `SuiteConfig` parses Redis options, rejects invalid combinations, and keeps
  existing Yierdis-only behavior unchanged.
- `SuiteRunner` includes Redis artifacts in run order and does not try to stop a
  process for Redis artifacts.
- `BenchHarness` validates Redis endpoint readiness, authenticates/selects DB
  when configured, runs `FLUSHDB`, and returns a running target.
- `ObservationClient` captures Redis-safe observations without treating missing
  `STATS` as failure.
- `ScenarioDefinition` and `SuiteProfileFactory` encode stable Redis
  comparability metadata.
- `ScenarioComparison` marks Redis unsupported or externally configured
  scenarios as non-comparable with clear reasons.
- report writers include Redis labels, ratios, and non-comparable reasons.
- environment metadata capture includes Redis runtime fields without leaking
  auth secrets.

Script and documentation tests:

- README and benchmark internals docs show the Redis comparison command.
- shell script contract tests cover any new optional environment variables if
  `scripts/bench.sh` grows Redis comparison support.

Manual smoke:

1. Start Redis locally on `127.0.0.1:6379` with persistence disabled.
2. Package benchmark and server jars with JDK 25.
3. Run suite with `--includeRedis --redisHost 127.0.0.1 --redisPort 6379`.
4. Verify `suite-result.json`, `metrics.csv`, `comparisons.csv`, and
   `report.md` contain Redis and current Yierdis entries.
5. Verify at least common string scenarios are comparable.

## Acceptance Criteria

- Existing Yierdis-only suite runs still work unchanged.
- Suite mode accepts `--includeRedis` plus Redis endpoint options.
- Redis suite mode compares Redis against current Yierdis in one run.
- Redis passes use the configured external Redis endpoint and do not spawn a
  local Redis process.
- Redis passes authenticate, select DB, and `FLUSHDB` before workload execution
  when configured to do so.
- Redis observation capture records `INFO` and `MEMORY STATS` without requiring
  Yierdis-only `STATS`.
- Redis/Yierdis scenario pairs are marked comparable only when both sides are
  clean and the scenario compatibility class is `YES`.
- Redis/Yierdis non-comparable scenarios include a stable human-readable reason.
- JSON, CSV, and Markdown artifacts all include Redis-aware comparison output.
- Environment metadata makes the operator-managed Redis configuration boundary
  explicit.

## Risks

- Benchmark fairness still depends on operator-managed Redis settings, host load,
  CPU throttling, and persistence configuration. The suite can document these
  factors, but it cannot eliminate them.
- `FLUSHDB` before every Redis pass is necessary for isolation, but it is also a
  destructive operation. Documentation must make that visible.
- Redis observation output differs from Yierdis observation output. Treating
  observation gaps as pass failures would create false negatives, but treating
  them as purely informational can hide some operational differences.
- Scenario shapes that are wire-compatible but not semantically equivalent could
  tempt readers to over-interpret raw numbers. Explicit non-comparable reasons
  are the main defense.
- Future three-way comparison work would complicate artifact ordering, report
  semantics, and threshold reporting. This design intentionally avoids that in
  the first version.

## Rollout Order

1. Extend suite config and artifact model for external Redis.
2. Add Redis-aware harness lifecycle and observation capture.
3. Add scenario compatibility metadata and comparison reasons.
4. Update CSV/Markdown/JSON reporting for Redis ratios and diagnostics.
5. Document Redis comparison workflow and recommended Redis settings.
6. After suite comparison is stable, continue with the separate
   Redis-benchmark-compatible entrypoint plan.

## Implementation Decisions

- Keep Redis suite comparison inside the existing `yierdis-benchmark` suite
  module and report pipeline.
- Represent Redis as an explicit artifact kind rather than a special-case flag
  scattered through the runner.
- Keep the shared RESP/TCP workload engine and do not shell out to
  `redis-benchmark` for suite mode.
- Preserve existing suite artifact filenames and top-level report structure.
- Keep Redis observation command failures informational unless they directly
  break the workload measurement path.
- Treat externally configured Redis scenarios as non-comparable in the first
  version even when both sides produced numbers.
- Do not emit Redis auth secrets into logs, environment metadata, or reports.

## Open Follow-Up

After the first version, consider adding managed Redis startup with an explicit
`--redisServerCmd` and generated temporary config. That should remain separate
from the first implementation so the initial Redis comparison path stays simple
and auditable.

Another follow-up is an explicit operator acknowledgement flag for
externally-configured scenarios such as eviction pressure. That should only be
added after the first version proves that the simpler non-comparable-by-default
policy is easy to understand in real reports.

## References

- `docs/superpowers/specs/2026-06-14-release-grade-benchmark-suite-design.md`
- `docs/superpowers/specs/2026-06-28-redis-benchmark-compatible-benchmark-design.md`
