package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SuiteProfileFactory {
    private SuiteProfileFactory() {
    }

    public static List<ScenarioDefinition> expand(SuiteProfileName profile) {
        SuiteProfileName requiredProfile = Objects.requireNonNull(profile, "profile");
        return switch (requiredProfile) {
            case RELEASE -> releaseScenarios();
            case FULL -> {
                List<ScenarioDefinition> scenarios = new ArrayList<>(releaseScenarios());
                scenarios.addAll(fullOnlyScenarios());
                yield List.copyOf(scenarios);
            }
        };
    }

    private static List<ScenarioDefinition> releaseScenarios() {
        return List.of(
                scenario("release-ping-latency", "PING latency baseline", BenchWorkloadKind.PING, 10_000, 0, 50_000, 16, 1, 1, 5, true),
                scenario("release-set-get-128b-c32-p4", "SET/GET 128B c32 p4", BenchWorkloadKind.SET_GET, 200_000, 128, 200_000, 32, 4, 1, 5, true),
                scenario("release-set-get-256b-c64-p8", "SET/GET 256B c64 p8", BenchWorkloadKind.SET_GET, 500_000, 256, 500_000, 64, 8, 1, 5, true),
                scenario("release-set-get-1024b-c64-p8", "SET/GET 1024B c64 p8", BenchWorkloadKind.SET_GET, 200_000, 1024, 200_000, 64, 8, 1, 5, true),
                scenario("release-append-256b-c64-p8", "APPEND 256B c64 p8", BenchWorkloadKind.APPEND, 200_000, 256, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-sparse-c64-p8", "HLL sparse PFADD c64 p8", BenchWorkloadKind.HLL_SPARSE, 200_000, 0, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-dense-c64-p8", "HLL dense PFADD c64 p8", BenchWorkloadKind.HLL_DENSE, 4096, 0, 300_000, 64, 8, 1, 5, true),
                scenario("release-hll-pfcount-c64-p8", "HLL PFCOUNT c64 p8", BenchWorkloadKind.HLL_PFCOUNT, 4096, 0, 300_000, 64, 8, 1, 5, false),
                scenario("release-native-defrag-append", "Native defrag APPEND p99", BenchWorkloadKind.NATIVE_DEFRAG_APPEND, 4096, 256, 50_000, 8, 4, 1, 5, true,
                        ScenarioDefinition.ServerOverrides.nativeDefrag(256L * 1024L, 256L, 5L),
                        ScenarioDefinition.RedisComparable.NO,
                        "yierdis-only native defrag scenario"),
                scenario("release-maxmemory-eviction", "Maxmemory eviction pressure", BenchWorkloadKind.MAXMEMORY_EVICTION, 50_000, 512, 100_000, 32, 4, 1, 5, false,
                        ScenarioDefinition.ServerOverrides.maxmemory(16L * 1024L * 1024L, "allkeys-lru", 16, 20L),
                        ScenarioDefinition.RedisComparable.EXTERNAL_CONFIG_REQUIRED,
                        "external Redis config required"),
                scenario("release-ttl-expiration", "TTL expiration pressure", BenchWorkloadKind.TTL_EXPIRATION, 50_000, 128, 100_000, 32, 4, 1, 5, false)
        );
    }

    private static List<ScenarioDefinition> fullOnlyScenarios() {
        return List.of(
                scenario("full-list-lpush", "List LPUSH", BenchWorkloadKind.LIST_LPUSH, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-hash-hset", "Hash HSET", BenchWorkloadKind.HASH_HSET, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-set-sadd", "Set SADD", BenchWorkloadKind.SET_SADD, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-zset-zadd", "ZSet ZADD", BenchWorkloadKind.ZSET_ZADD, 100_000, 128, 300_000, 64, 8, 1, 7, true),
                scenario("full-scan-count-100", "SCAN COUNT 100", BenchWorkloadKind.SCAN, 200_000, 128, 50_000, 16, 1, 1, 7, true),
                scenario("full-mixed-read-write-hot", "Mixed read/write hot keys", BenchWorkloadKind.MIXED_READ_WRITE, 10_000, 256, 500_000, 128, 8, 1, 7, true)
        );
    }

    private static ScenarioDefinition scenario(
            String id,
            String displayName,
            BenchWorkloadKind workload,
            int keyspace,
            int dataSize,
            int requests,
            int clients,
            int pipeline,
            int warmupIterations,
            int repeatIterations,
            boolean latency
    ) {
        return new ScenarioDefinition(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency);
    }

    private static ScenarioDefinition scenario(
            String id,
            String displayName,
            BenchWorkloadKind workload,
            int keyspace,
            int dataSize,
            int requests,
            int clients,
            int pipeline,
            int warmupIterations,
            int repeatIterations,
            boolean latency,
            ScenarioDefinition.ServerOverrides serverOverrides
    ) {
        return new ScenarioDefinition(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency, serverOverrides);
    }

    private static ScenarioDefinition scenario(
            String id,
            String displayName,
            BenchWorkloadKind workload,
            int keyspace,
            int dataSize,
            int requests,
            int clients,
            int pipeline,
            int warmupIterations,
            int repeatIterations,
            boolean latency,
            ScenarioDefinition.ServerOverrides serverOverrides,
            ScenarioDefinition.RedisComparable redisComparable,
            String redisNonComparableReason
    ) {
        return new ScenarioDefinition(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency, serverOverrides, redisComparable, redisNonComparableReason);
    }
}
