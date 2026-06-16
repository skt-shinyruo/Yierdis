package yier.bubu.redis.app.bench.suite;

import yier.bubu.redis.app.bench.BenchWorkloadKind;

import java.util.Objects;

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
        boolean latency
) {
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
    }

    private static void requireStableId(String id) {
        if (id == null || !id.matches("[a-z0-9]+(?:-[a-z0-9]+)*")) {
            throw new IllegalArgumentException("scenario id must be lowercase kebab-case");
        }
    }
}
