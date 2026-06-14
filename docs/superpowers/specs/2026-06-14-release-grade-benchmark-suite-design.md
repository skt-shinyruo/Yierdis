# Release-Grade Benchmark Suite Design

## Status

Approved design for upgrading the existing `yierdis-benchmark` tooling into a
release-grade performance reporting suite.

## Context

`yierdis-benchmark` already provides a useful RESP/TCP benchmark path. It starts
or connects to a real server, drives real RESP requests through the client codec,
and reports throughput, latency, errors, baseline/current comparability, DB
native defrag impact, and native allocator eval output.

The current benchmark is good for focused local runs, but it is not yet a
release-grade suite. The missing pieces are stable suite profiles, repeated
runs, warmup separation, structured report artifacts, explicit scenario
definitions, soft regression thresholds, per-scenario observability snapshots,
and broader workload coverage.

The goal is to preserve the existing single-run benchmark while adding a
first-class suite runner inside the `yierdis-benchmark` module.

## Goals

- Add a first-class Java suite runner to `yierdis-benchmark`.
- Support formal performance reports that can run for hours.
- Support both current-only reports and baseline/current comparison reports.
- Keep comparison output conservative: failed or dirty sides are
  `non-comparable`, not numeric before/after conclusions.
- Provide `release` and `full` profiles with stable scenario lists.
- Start a fresh server process for every scenario to avoid cross-scenario
  pollution.
- Capture low-interference observability snapshots before and after every
  scenario.
- Write JSON, CSV, and Markdown report artifacts.
- Add soft regression thresholds that annotate warnings without failing the
  command by default.
- Keep the existing single-run CLI and `scripts/bench.sh` behavior intact.

## Non-Goals

- Do not put multi-hour benchmark profiles into the normal Maven test lifecycle.
- Do not replace correctness tests, protocol tests, command tests, or DB direct
  tests with benchmark validation.
- Do not require raw server logs, GC logs, or JFR artifacts in the first suite
  version.
- Do not add hard release gating in the first version. A later flag can promote
  soft thresholds to process failures.
- Do not build an external shell-based experiment framework.
- Do not attempt to make old broken baseline artifacts comparable. The suite
  reports their failure and marks affected scenarios `non-comparable`.

## Proposed CLI

Add a new suite mode to the existing benchmark jar:

```bash
java -jar yierdis-benchmark-*.jar \
  --suite \
  --suiteProfile release|full \
  --currentServerJar path/to/current.jar \
  [--baselineServerJar path/to/baseline.jar] \
  --reportDir target/benchmark-reports/<run-id>
```

`--currentServerJar` is required in suite mode. `--baselineServerJar` is
optional. If it is present, every scenario runs both artifacts and the report
includes comparisons. If it is absent, the report is current-only.

`--reportDir` is optional. If omitted, suite mode writes to a generated
directory under `target/benchmark-reports/` using the run id and profile name.
If supplied, it must point to a non-existing or writable directory.

Suite mode continues to honor relevant existing server launch settings, such as
Java command, heap, direct memory, server args, host, and port base. Suite mode
owns scenario expansion, warmup, repeat counts, and report generation.

The existing non-suite benchmark mode remains available for fast focused runs.

## Profiles

### Release Profile

The `release` profile is the default formal release report. It should be
repeatable and broad enough to catch meaningful regressions without trying to
cover every Redis command shape.

It covers:

- `PING` latency baseline.
- `SET` and `GET` throughput and latency.
- `APPEND` throughput and latency.
- HLL sparse and dense `PFADD` plus `PFCOUNT`.
- Value-size matrix for important payload sizes.
- Concurrency and pipeline matrix for representative client pressure.
- Keyspace matrix for hot and wider working sets.
- Native defrag disabled/enabled comparison.
- Maxmemory and eviction pressure.
- TTL and expiration pressure.

### Full Profile

The `full` profile is a multi-hour report for important releases or performance
investigations. It includes the `release` profile plus broader coverage:

- List workloads.
- Hash workloads.
- Set workloads.
- ZSet workloads.
- `SCAN` and controlled `KEYS` budget scenarios.
- Mixed read/write scenarios.
- Hot-key distribution scenarios.
- Larger matrix coverage for value size, keyspace, clients, and pipeline.

Full profile scenario names must be stable so reports remain comparable across
versions.

## Workload Scope

The suite should reuse the existing RESP worker path for current workloads:

- `PING`
- `SET_RANDOM`
- `SET_SEQUENTIAL`
- `GET_RANDOM`
- `APPEND`
- `PFADD_SPARSE`
- `PFADD_DENSE`
- `PFCOUNT`

New workload families should be added through the suite scenario model rather
than by making the current monolithic benchmark path larger. Each workload
family should define:

- command sequence
- key naming scheme
- data shape
- warmup behavior
- repeat behavior
- strict reply validation
- metrics produced

## Architecture

Add suite-specific components under the `yierdis-benchmark` module in a
`yier.bubu.redis.app.bench.suite` package. Shared low-level helpers may remain
in `yier.bubu.redis.app.bench` when they are still used by the existing
single-run path.

### Suite Model

`SuiteRun` represents a complete report run. It records:

- run id
- profile name
- start and end times
- host and port allocation
- environment metadata
- artifact metadata
- threshold settings
- scenario results

`SuiteArtifact` represents one server artifact:

- side label: `current` or `baseline`
- server jar path
- optional commit label
- server launch command
- JVM settings
- shared server args

`Scenario` represents a repeatable workload definition:

- stable scenario id
- display name
- workload family
- keyspace
- data size
- clients
- pipeline
- warmup iterations
- repeat iterations
- server settings override, if any
- threshold override, if any

`ScenarioPass` represents one artifact executing one scenario with one fresh
server process.

`IterationResult` records one warmup or repeat iteration.

`MetricSummary` aggregates repeat iterations. Warmup data is retained but does
not contribute to release conclusions.

`ObservationSnapshot` stores before/after observability output for a scenario.

### Runner Flow

Suite mode executes this flow:

1. Parse and validate suite arguments.
2. Capture environment and artifact metadata.
3. Expand the selected profile into stable scenarios.
4. For each scenario and each artifact:
   - start a fresh server process
   - wait for readiness
   - capture before snapshots
   - run warmup iterations
   - run repeat iterations
   - capture after snapshots
   - stop the server process
5. Aggregate repeat metrics.
6. Compare baseline/current metrics when both sides are clean.
7. Apply soft thresholds.
8. Write JSON, CSV, and Markdown artifacts.

Each scenario starts a new server process. This makes the report slower, but
keeps memory, fragmentation, eviction, TTL, and cache state easier to interpret.

### Reuse Of Existing Code

The implementation should avoid rewriting the current benchmark behavior. It
should progressively extract or adapt:

- server process launch and shutdown
- readiness probing
- existing RESP command writer
- existing throughput and latency workers
- strict reply validation
- existing result summaries
- comparison status rules

The existing single-run CLI must keep its current behavior unless a specific
compatibility change is approved separately.

## Observability Snapshots

Every scenario pass captures low-interference snapshots before and after the
measured workload:

- `STATS`
- `MEMORY STATS`
- `INFO`

Snapshots are stored in the JSON result and summarized in Markdown. They are
used to explain performance changes, especially executor queueing,
backpressure, rejected commands, memory growth, eviction, expiration, and native
defrag behavior.

The first version captures only before/after snapshots, not continuous
sampling. Continuous sampling can be added later if the report needs deeper
diagnostics.

## Metrics And Comparability

Throughput metrics include:

- operations
- errors
- elapsed time
- QPS

Latency metrics include:

- operations
- errors
- elapsed time
- QPS
- p50
- p95
- p99
- max, if already available in the collected stats

Repeat aggregation should include at least:

- min
- median
- mean
- max
- sample count

Baseline/current comparison is allowed only when both sides complete the same
scenario shape without startup failure, protocol failure, strict reply failure,
or non-zero benchmark errors. Otherwise the scenario is marked
`non-comparable`.

Warmup iterations are visible in JSON but excluded from comparison deltas.

## Soft Thresholds

The first suite version uses soft thresholds. Violations appear in the report,
but the command exits successfully by default.

Default thresholds:

- QPS decrease greater than 10 percent: warning.
- p95 latency increase greater than 15 percent: warning.
- p99 latency increase greater than 15 percent: warning.
- any non-zero error count: critical observation.
- `non-comparable` scenario: critical observation.
- meaningful before/after backpressure or reject counter growth: warning.

The report should distinguish between:

- performance regression warning
- measurement failure
- non-comparable result
- explanatory observability signal

A later `--failOnRegression` flag can convert warnings or critical observations
into a non-zero exit code. That is intentionally outside the first behavior
contract.

## Report Artifacts

Suite mode writes:

```text
target/benchmark-reports/<run-id>/
  suite-result.json
  metrics.csv
  comparisons.csv
  report.md
```

### suite-result.json

The JSON file is the fact source. It includes:

- run metadata
- environment metadata
- artifact metadata
- profile and scenario definitions
- warmup iteration results
- repeat iteration results
- aggregated metrics
- before/after observation snapshots
- failures
- comparability status
- threshold findings

### metrics.csv

The metrics CSV flattens metrics by artifact, scenario, iteration group, and
metric. It is optimized for spreadsheet and plotting tools.

### comparisons.csv

The comparisons CSV contains only baseline/current comparisons:

- scenario id
- metric name
- baseline value
- current value
- delta percent
- threshold status
- comparability status

Current-only runs still write the file with headers and no comparison rows.

### report.md

The Markdown report is for release notes and human review. It includes:

- run summary
- profile summary
- environment summary
- artifact summary
- top warnings and critical observations
- scenario result tables
- baseline/current comparison tables when available
- non-comparable reasons
- observability notes

## Error Handling

The suite should preserve partial results whenever possible.

- Argument and path validation failures stop before any server starts.
- A scenario startup failure records a failed scenario pass.
- A workload exception records the failure and continues to the next scenario.
- Server shutdown is always attempted.
- A failed artifact side makes that scenario `non-comparable`.
- Report writing is attempted even if one or more scenarios fail.

The first version exits zero after writing a report unless argument validation
or report writing itself fails. This matches the soft-threshold design.

## Testing And Verification

Add focused unit tests for:

- suite argument validation
- profile expansion for `release` and `full`
- stable scenario ids
- threshold evaluation
- comparability evaluation
- JSON report shape
- CSV report shape
- Markdown report shape
- warmup exclusion from repeat summaries
- failed scenario preservation
- fresh-server-per-scenario orchestration with fakes

Keep existing benchmark tests for summary rendering, comparison rendering, and
RESP command writing.

Add limited integration coverage for the runner with fake process/workload
adapters. Full multi-hour profiles must not run inside Maven tests.

Verification should use JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

## Acceptance Criteria

- Existing single-run benchmark mode still works.
- Suite mode accepts `release` and `full` profiles.
- Suite mode supports current-only reports.
- Suite mode supports baseline/current reports.
- Every scenario starts a fresh server process.
- Every scenario captures before/after `STATS`, `MEMORY STATS`, and `INFO`
  snapshots.
- Warmup iterations are recorded but excluded from conclusions.
- Repeat iterations are aggregated into stable metric summaries.
- JSON, CSV, and Markdown artifacts are written for every completed suite run.
- Baseline/current deltas are shown only for clean comparable scenario pairs.
- Failed or dirty scenario pairs are explicitly marked `non-comparable`.
- Soft threshold violations are visible in the report and do not fail the
  command by default.
- Tests cover profile expansion, thresholding, report generation, and runner
  orchestration boundaries.

## Risks

- Multi-hour profile runtime can slow feedback. This is intentional for formal
  reports, while the existing single-run benchmark remains available for fast
  checks.
- Fresh server per scenario increases runtime, but avoids ambiguous state
  carryover.
- JSON schema churn can break downstream report consumers. Field names should
  be stable once introduced.
- Benchmark results can still vary with host load, CPU throttling, filesystem
  state, and JVM behavior. Environment metadata and repeated runs reduce, but
  do not eliminate, that risk.
- Observability snapshots may be text-like at first if server replies are not
  fully normalized. The JSON model should still leave room for structured maps.

## Implementation Decisions

- Suite-specific classes live in `yier.bubu.redis.app.bench.suite`.
- The first version emits JSON with a small deterministic internal writer and
  does not add a new JSON dependency.
- Report file names are fixed. The report directory is user-provided through
  `--reportDir` or generated under `target/benchmark-reports/`.
