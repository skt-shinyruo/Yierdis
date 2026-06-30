package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;
import yier.bubu.redis.app.bench.YierdisBenchServerArgs;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;
import java.util.regex.Pattern;

public record ScenarioDefinition(
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
        ServerOverrides serverOverrides,
        RedisComparable redisComparable,
        String redisNonComparableReason
) {
    private static final Pattern STABLE_ID_PATTERN = Pattern.compile("[a-z0-9]+(?:-[a-z0-9]+)*");

    public ScenarioDefinition {
        requireStableId(id);
        displayName = Objects.requireNonNull(displayName, "displayName");
        if (displayName.isBlank()) {
            throw new IllegalArgumentException("displayName must not be blank");
        }
        Objects.requireNonNull(workload, "workload");
        if (keyspace <= 0) {
            throw new IllegalArgumentException("keyspace must be > 0");
        }
        if (dataSize < 0) {
            throw new IllegalArgumentException("dataSize must be >= 0");
        }
        if (requests <= 0) {
            throw new IllegalArgumentException("requests must be > 0");
        }
        if (clients <= 0) {
            throw new IllegalArgumentException("clients must be > 0");
        }
        if (pipeline <= 0) {
            throw new IllegalArgumentException("pipeline must be > 0");
        }
        if (warmupIterations < 0) {
            throw new IllegalArgumentException("warmupIterations must be >= 0");
        }
        if (repeatIterations <= 0) {
            throw new IllegalArgumentException("repeatIterations must be > 0");
        }
        serverOverrides = serverOverrides == null ? ServerOverrides.none() : serverOverrides;
        redisComparable = redisComparable == null ? RedisComparable.YES : redisComparable;
        redisNonComparableReason = redisNonComparableReason == null ? "" : redisNonComparableReason;
        if (redisComparable == RedisComparable.YES && !redisNonComparableReason.isBlank()) {
            throw new IllegalArgumentException("redisNonComparableReason must be blank when redisComparable is YES");
        }
        if (redisComparable != RedisComparable.YES && redisNonComparableReason.isBlank()) {
            throw new IllegalArgumentException("redisNonComparableReason must be provided when redisComparable is not YES");
        }
    }

    public ScenarioDefinition(
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
        this(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency, ServerOverrides.none(), RedisComparable.YES, "");
    }

    public ScenarioDefinition(
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
            ServerOverrides serverOverrides
    ) {
        this(id, displayName, workload, keyspace, dataSize, requests, clients, pipeline,
                warmupIterations, repeatIterations, latency, serverOverrides, RedisComparable.YES, "");
    }

    public enum RedisComparable {
        YES,
        EXTERNAL_CONFIG_REQUIRED,
        NO
    }

    public void applyServerOverrides(YierdisBenchServerArgs serverArgs) {
        serverOverrides.applyTo(serverArgs);
    }

    private static void requireStableId(String id) {
        if (id == null || !STABLE_ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException("scenario id must be lowercase kebab-case");
        }
    }

    public record ServerOverrides(
            int databases,
            int nativeSlotCapacity,
            boolean nativeDefragEnabled,
            long nativeDefragMaxMoveBytes,
            long nativeDefragMaxObjects,
            long nativeDefragTimeLimitMillis,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis
    ) {
        public ServerOverrides {
            if (databases < 0) {
                throw new IllegalArgumentException("databases must be >= 0");
            }
            if (nativeSlotCapacity < 0) {
                throw new IllegalArgumentException("nativeSlotCapacity must be >= 0");
            }
            if (nativeDefragMaxMoveBytes < 0) {
                throw new IllegalArgumentException("nativeDefragMaxMoveBytes must be >= 0");
            }
            if (nativeDefragMaxObjects < 0) {
                throw new IllegalArgumentException("nativeDefragMaxObjects must be >= 0");
            }
            if (nativeDefragTimeLimitMillis < 0) {
                throw new IllegalArgumentException("nativeDefragTimeLimitMillis must be >= 0");
            }
            if (maxmemoryBytes < 0) {
                throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
            }
            maxmemoryPolicy = maxmemoryPolicy == null ? "" : maxmemoryPolicy;
            if (!maxmemoryPolicy.isBlank()) {
                maxmemoryPolicy = MaxmemoryPolicy.parse(maxmemoryPolicy).redisName();
            }
            if (maxmemorySamples < 0) {
                throw new IllegalArgumentException("maxmemorySamples must be >= 0");
            }
            if (evictionTimeLimitMillis < 0) {
                throw new IllegalArgumentException("evictionTimeLimitMillis must be >= 0");
            }
        }

        public static ServerOverrides none() {
            return new ServerOverrides(0, 0, false, 0, 0, 0, 0, "", 0, 0);
        }

        public static ServerOverrides nativeDefrag(long maxMoveBytes, long maxObjects, long timeLimitMillis) {
            return new ServerOverrides(0, 0, true, maxMoveBytes, maxObjects, timeLimitMillis, 0, "", 0, 0);
        }

        public static ServerOverrides maxmemory(long bytes, String policy, int samples, long evictionTimeLimitMillis) {
            return new ServerOverrides(0, 0, false, 0, 0, 0, bytes, policy, samples, evictionTimeLimitMillis);
        }

        public static ServerOverrides databasesAndNativeSlots(int databases, int nativeSlotCapacity) {
            return new ServerOverrides(databases, nativeSlotCapacity, false, 0, 0, 0, 0, "", 0, 0);
        }

        private void applyTo(YierdisBenchServerArgs serverArgs) {
            Objects.requireNonNull(serverArgs, "serverArgs");
            if (databases > 0) {
                serverArgs.databases = databases;
            }
            if (nativeSlotCapacity > 0) {
                serverArgs.nativeSlotCapacity = nativeSlotCapacity;
            }
            if (nativeDefragEnabled) {
                serverArgs.nativeDefragEnabled = true;
                if (nativeDefragMaxMoveBytes > 0) {
                    serverArgs.nativeDefragMaxMoveBytes = nativeDefragMaxMoveBytes;
                }
                if (nativeDefragMaxObjects > 0) {
                    serverArgs.nativeDefragMaxObjects = nativeDefragMaxObjects;
                }
                if (nativeDefragTimeLimitMillis > 0) {
                    serverArgs.nativeDefragTimeLimitMillis = nativeDefragTimeLimitMillis;
                }
            }
            if (maxmemoryBytes > 0) {
                serverArgs.maxmemoryBytes = maxmemoryBytes;
            }
            if (!maxmemoryPolicy.isBlank()) {
                serverArgs.maxmemoryPolicy = maxmemoryPolicy;
            }
            if (maxmemorySamples > 0) {
                serverArgs.maxmemorySamples = maxmemorySamples;
            }
            if (evictionTimeLimitMillis > 0) {
                serverArgs.evictionTimeLimitMillis = evictionTimeLimitMillis;
            }
        }
    }
}
