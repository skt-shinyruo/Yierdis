package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.app.bench.LatencyRecorder;

import java.util.OptionalLong;

public record StorageBenchmarkResult(
        int completedOperations,
        long elapsedNanos,
        LatencyRecorder.Summary latency,
        Phase ttlChurn,
        Phase deletion,
        StorageMemorySnapshot baseline,
        StorageMemorySnapshot loaded
) {
    static StorageBenchmarkResult from(
            int completedOperations,
            long elapsedNanos,
            LatencyRecorder.Summary latency,
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

    public record Phase(long elapsedNanos, LatencyRecorder.Summary latency) {
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
