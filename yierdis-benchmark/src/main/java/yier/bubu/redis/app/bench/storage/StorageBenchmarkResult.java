package yier.bubu.redis.app.bench.storage;

import java.util.Objects;
import java.util.OptionalLong;

public record StorageBenchmarkResult(
        int completedOperations,
        long elapsedNanos,
        StorageLatencyRecorder.Summary latency,
        Phase ttlChurn,
        Phase deletion,
        StorageMemorySnapshot baseline,
        StorageMemorySnapshot loaded
) {
    public StorageBenchmarkResult {
        if (completedOperations <= 0) {
            throw new IllegalArgumentException("completedOperations must be > 0");
        }
        if (elapsedNanos < 0L) {
            throw new IllegalArgumentException("elapsedNanos must be >= 0");
        }
        latency = Objects.requireNonNull(latency, "latency");
        ttlChurn = Objects.requireNonNull(ttlChurn, "ttlChurn");
        deletion = Objects.requireNonNull(deletion, "deletion");
        baseline = Objects.requireNonNull(baseline, "baseline");
        loaded = Objects.requireNonNull(loaded, "loaded");
        if (latency.count() != completedOperations) {
            throw new IllegalArgumentException("latency count must equal completedOperations");
        }
        if (ttlChurn.latency().count() != completedOperations) {
            throw new IllegalArgumentException("ttl-churn latency count must equal completedOperations");
        }
        if (deletion.latency().count() != completedOperations) {
            throw new IllegalArgumentException("deletion latency count must equal completedOperations");
        }
        if (loaded.keyCount() != completedOperations) {
            throw new IllegalArgumentException("loaded key count must equal completedOperations");
        }
        if (loaded.accountedBytes() < baseline.accountedBytes()) {
            throw new IllegalArgumentException(
                    "loaded accounted footprint must not be smaller than the baseline"
            );
        }
    }

    static StorageBenchmarkResult from(
            int completedOperations,
            long elapsedNanos,
            StorageLatencyRecorder.Summary latency,
            Phase ttlChurn,
            Phase deletion,
            StorageMemorySnapshot baseline,
            StorageMemorySnapshot loaded
    ) {
        if (loaded.accountedBytes() < baseline.accountedBytes()) {
            throw new IllegalStateException("loaded accounted footprint is smaller than the empty baseline");
        }
        return new StorageBenchmarkResult(
                completedOperations,
                elapsedNanos,
                latency,
                ttlChurn,
                deletion,
                baseline,
                loaded
        );
    }

    public double operationsPerSecond() {
        return perSecond(completedOperations, elapsedNanos);
    }

    public long accountedDeltaBytes() {
        return loaded.accountedBytes() - baseline.accountedBytes();
    }

    public double accountedDeltaBytesPerKey() {
        return accountedDeltaBytes() / (double) completedOperations;
    }

    public OptionalLong rssDeltaBytes() {
        return rssDelta(baseline.rssBytes(), loaded.rssBytes());
    }

    private static OptionalLong rssDelta(OptionalLong before, OptionalLong after) {
        if (before.isEmpty() || after.isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Math.subtractExact(after.getAsLong(), before.getAsLong()));
        } catch (ArithmeticException ignored) {
            return OptionalLong.empty();
        }
    }

    public record Phase(long elapsedNanos, StorageLatencyRecorder.Summary latency) {
        public Phase {
            if (elapsedNanos < 0L) {
                throw new IllegalArgumentException("phase elapsedNanos must be >= 0");
            }
            latency = Objects.requireNonNull(latency, "latency");
            if (latency.count() <= 0L) {
                throw new IllegalArgumentException("phase latency count must be > 0");
            }
        }

        public double operationsPerSecond() {
            return perSecond(latency.count(), elapsedNanos);
        }
    }

    private static double perSecond(long operations, long elapsedNanos) {
        return elapsedNanos == 0L
                ? 0.0
                : operations * 1_000_000_000.0 / elapsedNanos;
    }
}
