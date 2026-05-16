# DB Native Allocator Benchmark Baseline Repair Design

## Status

Draft design for Track 1: Benchmark Baseline Repair.

## Problem

The current benchmark path can measure the allocator-migrated branch, but the historical baseline used for comparison is not yet trustworthy in this environment. Minimal probes against the baseline server have returned `-ERR internal error`, so the existing benchmark output does not represent a valid before/after comparison.

Today, `yierdis-benchmark` can run a single server-backed workload set and print per-backend throughput and latency data, but it does not explicitly frame results as a baseline/current comparison. If the baseline is flaky or unavailable, the report can still look quantitative even though the pair is not comparable.

## Goals

- Restore a benchmark flow that compares baseline and current using the same RESP workload shape.
- Make the benchmark report clearly identify which side is baseline and which side is current.
- Record the exact comparison context, including any commit or environment caveat, when the historical baseline cannot be made fully comparable.
- Keep the change small and local to `yierdis-benchmark` and its report output.

## Non-Goals

- Do not change allocator internals, collection internals, or server runtime behavior.
- Do not expand benchmark scope beyond the existing RESP workloads already covered by `yierdis-benchmark`.
- Do not add new production hardening, CI infrastructure, or doc convergence work outside the benchmark baseline report.
- Do not rewrite the parent roadmap spec.

## Proposed Design

Keep the existing benchmark runner and single-run mode, but add a separate explicit comparison mode for Track 1.

The minimal comparison command model is:

- `--comparisonMode`
- `--baselineServerJar <path>`
- `--currentServerJar <path>`

Comparison mode is jar-only and auto-starts both sides from the same process using the same workload and server args for each side. The benchmark should treat them as:

- `baseline`: the historical server/build being compared
- `current`: the allocator-migrated build under test

Both sides must execute the same workload set with the same command shape: same workload names, same request counts, same clients, same pipeline, same latency sampling, and the same strict reply rules when enabled. The only intended differences are the server jar and side label.

Comparison mode covers only the existing main RESP summary output:

- SET
- GET
- APPEND
- PFADD sparse
- PFADD dense
- PFCOUNT
- latency metrics when latency is not skipped

It does not expand into DB native defrag benchmarking. In comparison mode, the benchmark forces the focused DB native defrag comparison off, equivalent to `--skipNativeDefragCompare`, so Track 1 does not change defrag-specific benchmark behavior.

Existing single-run flags remain unchanged for non-comparison runs. In particular:

- `--serverJar` continues to support the existing single-run path
- `--noStartServer` remains available for single external runs
- `--noStartServer` is not part of Track 1 comparison mode

Comparison mode validation must reject invalid combinations up front:

- `--comparisonMode` requires both `--baselineServerJar` and `--currentServerJar`
- `--comparisonMode` rejects `--serverJar`
- `--comparisonMode` rejects `--noStartServer`
- `--baselineServerJar` and `--currentServerJar` must each point to an existing regular file
- both sides use shared workload and server args, except for port offset, side label, and jar path

Track 1 comparison mode does not support mixed external endpoints. It uses the two auto-started jars only.

Execution semantics should be explicit. Parse, flag-combination, and jar-path validation fails before either side launches. After validation, each side launches with the shared workload and server args plus its side-specific jar, port offset, and side label. Workload, protocol, server startup, or partial-measurement failures after launch are captured into the comparison result and rendered as non-comparable instead of silently producing deltas.

The smallest useful extension is to add a new comparison data model and renderer that can reuse the per-side `BackendResult` summaries but adds side labels, provenance, deltas, and non-comparable status. The existing table renderer may continue to serve the single-run path, but comparison mode should not be forced through it.

If the historical baseline still fails to execute the workloads cleanly, the tool must not present the output as comparable. Instead, it should surface a failure state for that side, preserve the exact failed jar command and attempt context, and report that the comparison is environment-limited rather than numerically valid.

## User/CLI Behavior

- The benchmark keeps its existing workload and server-argument flags.
- The comparison run must identify baseline and current by name in the CLI output and in the final report.
- Comparison mode uses explicit baseline and current jar paths only.
- Parse, flag, path, and pre-launch command-shape mismatches fail validation before launch.
- Reply, protocol, workload, server startup, and partial-measurement failures after launch are captured in the comparison result and rendered as non-comparable.
- When baseline execution is not clean, the CLI must print that the comparison is incomplete and name the failing side.

## Report Behavior

The comparison mode should render through a dedicated comparison model/renderer, not by overloading the single-run summary table.

That comparison renderer should:

- show baseline and current labels
- show both sides for the same workload set in one place
- include delta or change information for comparable numeric fields
- include an explicit note when a side failed or was only partially measured
- record baseline/current jar paths and attempted commands, and include the exact baseline/current commit labels when known, or an explicit environment caveat when the commit cannot be tied to the artifact
- mark the pair non-comparable when either side failed or was only partially measured

The existing `docs/superpowers/reports/*benchmark*` artifact should be updated to state the exact comparison source and to avoid presenting raw numbers as a trustworthy before/after pair when the historical baseline remains broken.

## Testing/Verification

Add or extend tests in `yierdis-benchmark/src/test/java/yier/bubu/redis/app/bench/` to cover:

- stable rendering of a baseline/current comparison table
- explicit labeling of baseline and current rows or sections
- delta formatting for comparable numeric fields
- non-comparable failure reporting when one side cannot execute cleanly
- provenance and caveat rendering with jar path, attempted command, and environment fields
- provenance rendering with known baseline/current commit labels
- unknown-commit rendering with an explicit environment caveat when a commit cannot be tied to an artifact
- reuse of the same server argv shape for the two sides when the comparison is driven from shared launch arguments
- CLI validation: `--comparisonMode` requires both jar flags
- CLI validation: `--comparisonMode` rejects `--serverJar`
- CLI validation: `--comparisonMode` rejects `--noStartServer`
- CLI validation: missing or non-regular jar paths fail before launch

Verification should continue to use the repository’s JDK 25 Maven prefix:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-benchmark -am test
```

The benchmark report itself should be checked against the real run output so the final text matches the actual failure or success state of the baseline pair.

## Acceptance Criteria

- Accepted comparable outcome: baseline and current can run the same RESP workload shape without extra protocol errors, and the comparison report shows explicit side labels, provenance, and deltas for comparable numbers.
- Accepted caveated fallback outcome: if either side fails or is only partially measured, the report names the affected side, jar path, attempted command, failing probes or errors, and environment caveat, and marks the pair non-comparable instead of presenting a valid before/after comparison. Historical baseline failure is the expected Track 1 caveat case.
- The benchmark tests cover the comparison formatting, provenance, caveat rendering, and failure reporting behavior.

## Risks/Caveats

- The historical baseline may remain unusable in this environment even after the benchmark surface is clarified.
- A comparison report can still be misleading if it omits provenance, so the report must keep the baseline source visible.
- Any new output shape must stay narrow; it should not turn `yierdis-benchmark` into a broader benchmarking framework.
- The scope stops at benchmark reporting and comparison clarity. It does not include allocator fixes, server repair, or unrelated benchmark areas.
