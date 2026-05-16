# DB Native Allocator Benchmark Results

Date: 2026-05-16
Branch commit: `9b9f58c`

## Verification

Full repo verification already passed on this worktree:

- `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test`
- `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -DskipTests package`

## Native allocator eval

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --nativeEval --nativeEvalIterations 1000
```

Key output:

- allocate/free: 64B `8.260 us` alloc, `11.558 us` free
- allocate/free: 256B `1.747 us` alloc, `3.549 us` free
- allocate/free: 4KiB `2.766 us` alloc, `4.121 us` free
- allocate/free: 64KiB `18.776 us` alloc, `106.831 us` free
- resolve/close: `8.204 us`
- realloc: `500` in-place, `500` moved, avg `5.253 us`
- pin/unpin: `2.290 us`
- metadata: `72 B` per live object
- small-object churn: `p99 34.181 us`
- defrag p99 impact: disabled `30.657 us`, enabled `287.947 us`

## RESP benchmark

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --serverJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --javaCmd /usr/lib/jvm/java-25-openjdk-amd64/bin/java \
  --xms 512m --xmx 512m --maxDirectMemory 1g \
  --portBase 17378 --keyspace 2000 --requests 4000 --clients 16 --pipeline 8 \
  --latencyRequests 1000 --latencyClients 8 --dataSize 128 --strictReplies \
  --skipNativeDefragCompare -- --maxmemoryBytes 134217728
```

Key output:

- SET_QPS `1050.985`, SET_p95 `12.673 ms`
- GET_QPS `27121.222`, GET_p95 `0.703 ms`
- APPEND_QPS `1076.911`, APPEND_p95 `12.541 ms`
- PFADD sparse QPS `610.920`
- PFADD dense QPS `212.458`
- PFCOUNT QPS `17087.257`
- PFADD sparse p95 `51.732 ms`
- PFADD dense p95 `39.798 ms`

## DB native defrag compare

Command:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
java -jar yierdis-benchmark/target/yierdis-benchmark-0.1.0-SNAPSHOT.jar \
  --serverJar yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar \
  --javaCmd /usr/lib/jvm/java-25-openjdk-amd64/bin/java \
  --xms 512m --xmx 512m --maxDirectMemory 1g \
  --portBase 17578 --keyspace 1000 --requests 1000 --clients 8 --pipeline 4 \
  --latencyRequests 500 --latencyClients 4 --dataSize 128 --strictReplies \
  --skipPrefill --skipLatency -- --maxmemoryBytes 134217728
```

Key output:

- SET_QPS `1582.551`, errors `0`
- GET_QPS `13349.909`, errors `0`
- APPEND_QPS `2211.727`, errors `0`
- PFADD sparse QPS `1488.335`, errors `0`
- PFADD dense QPS `776.669`, errors `0`
- PFCOUNT QPS `9018.415`, errors `0`
- DB native defrag APPEND p99: disabled `10.459 ms`, enabled `8.266 ms`, impact `-20.966%`, enabled errors `0`

## Baseline note

I also tried a pre-migration baseline server from commit `79228e3`, but in this environment it returned `-ERR internal error` even for minimal `SET`, `PFADD`, and `APPEND` probes, so I did not use it as a comparable quantitative baseline.
