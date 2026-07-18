package yier.bubu.redis.app.bench.redis;

import java.util.Objects;

public record BenchmarkStatistics(
        int requestedRequests,
        long completedRequests,
        long wireRequests,
        long histogramSamples,
        long elapsedMillis,
        double requestsPerSecond,
        BenchmarkLatencyRecorder.Summary latency
) {
    public BenchmarkStatistics {
        validateCounters(
                requestedRequests,
                completedRequests,
                wireRequests,
                histogramSamples,
                elapsedMillis,
                latency
        );
    }

    public static BenchmarkStatistics from(
            int requested,
            long completed,
            long wire,
            long samples,
            long elapsedMillis,
            BenchmarkLatencyRecorder.Summary latency
    ) {
        validateCounters(requested, completed, wire, samples, elapsedMillis, latency);
        double requestsPerSecond = elapsedMillis == 0
                ? 0.0
                : completed / (elapsedMillis / 1000.0);
        return new BenchmarkStatistics(
                requested,
                completed,
                wire,
                samples,
                elapsedMillis,
                requestsPerSecond,
                latency
        );
    }

    private static void validateCounters(
            int requested,
            long completed,
            long wire,
            long samples,
            long elapsedMillis,
            BenchmarkLatencyRecorder.Summary latency
    ) {
        if (requested <= 0) {
            throw new IllegalArgumentException("requestedRequests must be > 0");
        }
        if (elapsedMillis < 0) {
            throw new IllegalArgumentException("elapsedMillis must be >= 0");
        }
        if (wire < requested) {
            throw new IllegalArgumentException("wireRequests must be >= requestedRequests");
        }
        if (completed < requested || completed > wire) {
            throw new IllegalArgumentException("completedRequests must be in requested..wire");
        }
        if (samples != requested) {
            throw new IllegalArgumentException("histogramSamples must equal requestedRequests");
        }
        Objects.requireNonNull(latency, "latency");
        if (latency.count() != samples) {
            throw new IllegalArgumentException("latency count must equal histogramSamples");
        }
    }
}
