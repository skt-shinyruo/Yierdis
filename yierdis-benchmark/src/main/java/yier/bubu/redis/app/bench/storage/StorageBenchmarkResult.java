package yier.bubu.redis.app.bench.storage;

import java.util.Objects;
import java.util.OptionalLong;

public record StorageBenchmarkResult(
        int completedOperations,
        long elapsedNanos,
        double operationsPerSecond,
        StorageLatencyRecorder.Summary latency,
        StorageMemorySnapshot baseline,
        StorageMemorySnapshot loaded,
        long accountedDeltaBytes,
        double accountedDeltaBytesPerKey,
        OptionalLong rssDeltaBytes
) {
    public StorageBenchmarkResult {
        if (completedOperations <= 0) {
            throw new IllegalArgumentException("completedOperations must be > 0");
        }
        if (elapsedNanos < 0L) {
            throw new IllegalArgumentException("elapsedNanos must be >= 0");
        }
        if (!Double.isFinite(operationsPerSecond) || operationsPerSecond < 0.0) {
            throw new IllegalArgumentException("operationsPerSecond must be finite and >= 0");
        }
        latency = Objects.requireNonNull(latency, "latency");
        baseline = Objects.requireNonNull(baseline, "baseline");
        loaded = Objects.requireNonNull(loaded, "loaded");
        rssDeltaBytes = Objects.requireNonNull(rssDeltaBytes, "rssDeltaBytes");
        if (latency.count() != completedOperations) {
            throw new IllegalArgumentException("latency count must equal completedOperations");
        }
        if (loaded.keyCount() != completedOperations) {
            throw new IllegalArgumentException("loaded key count must equal completedOperations");
        }
        if (accountedDeltaBytes < 0L) {
            throw new IllegalArgumentException("accountedDeltaBytes must be >= 0");
        }
        if (!Double.isFinite(accountedDeltaBytesPerKey) || accountedDeltaBytesPerKey < 0.0) {
            throw new IllegalArgumentException("accountedDeltaBytesPerKey must be finite and >= 0");
        }
    }

    static StorageBenchmarkResult from(
            int completedOperations,
            long elapsedNanos,
            StorageLatencyRecorder.Summary latency,
            StorageMemorySnapshot baseline,
            StorageMemorySnapshot loaded
    ) {
        if (loaded.accountedBytes() < baseline.accountedBytes()) {
            throw new IllegalStateException("loaded accounted footprint is smaller than the empty baseline");
        }
        long delta = loaded.accountedBytes() - baseline.accountedBytes();
        double operationsPerSecond = elapsedNanos == 0L
                ? 0.0
                : completedOperations * 1_000_000_000.0 / elapsedNanos;
        OptionalLong rssDelta = rssDelta(baseline.rssBytes(), loaded.rssBytes());
        return new StorageBenchmarkResult(
                completedOperations,
                elapsedNanos,
                operationsPerSecond,
                latency,
                baseline,
                loaded,
                delta,
                delta / (double) completedOperations,
                rssDelta
        );
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
}
