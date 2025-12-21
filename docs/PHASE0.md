# Phase 0: Measurement & Acceptance Criteria

This repository aims to reduce object count and GC pressure by moving towards a Redis-like `type + encoding + payload`
model and by packing small collections.

Before changing data structures, this phase establishes repeatable workloads and a way to compare:

- allocation/object histograms (Top-N types)
- GC frequency and pauses
- overall heap usage (coarse)

## Workloads

The benchmark driver is an in-process DB workload (no network), to focus measurements on storage/value
representation rather than Netty/protocol parsing.

Scenarios:

- `strings`: 1e6 short string keys (`SET kNNNNNN seed+N`)
- `hashes`: 1e5 hashes, each with 10 field/value pairs (`HSET hNNNNNN f00 v00 ... f09 v09`)
- `lists`: 1e5 lists with 8 small elements (`RPUSH lNNNNNN a b c d e f g h`)
- `zsets`: 1e5 zsets with 8 members (`ZADD zNNNNNN 0 m00 1 m01 ... 7 m07`)

All scenarios are deterministic by default.

## Quick start (scripted)

The script builds the shaded jar, runs a scenario with fixed heap settings, waits until data is loaded,
and then collects `jcmd` outputs into `bench-out/`.

```bash
chmod +x scripts/phase0_measure.sh
./scripts/phase0_measure.sh strings
./scripts/phase0_measure.sh hashes
./scripts/phase0_measure.sh lists
./scripts/phase0_measure.sh zsets
```

Outputs (per run) include:

- `class_histogram.txt` (`jcmd <pid> GC.class_histogram`)
- `heap_info.txt` (`jcmd <pid> GC.heap_info`)
- `native_memory.txt` (best-effort, may require NMT enabled)
- `gc.log` (JDK unified logging: `-Xlog:gc*`)

## Manual run

Build jar:

```bash
mvn -q -DskipTests package
```

Run a scenario (keeps the JVM alive for 10 minutes by default):

```bash
java -Xms2g -Xmx2g -Xlog:gc* \
  -cp target/yierdis-0.1.0-SNAPSHOT.jar \
  yier.bubu.redis.bench.YierdisBench \
  --scenario strings
```

Then run (from another terminal):

```bash
jcmd <pid> GC.class_histogram
jcmd <pid> GC.heap_info
```

## Suggested acceptance criteria (adjust as needed)

- Total object count drops significantly for `strings` (notably fewer `StringValue`/wrappers).
- For future phases:
  - fewer `HashMap$Node` / wrapper key objects after replacing hash/set/zset large encodings
  - fewer `ArrayList` / small entry objects after implementing packed encodings
- Full GC count approaches 0 for these runs.
- Young GC frequency and/or pause time decreases noticeably under the same heap settings.

