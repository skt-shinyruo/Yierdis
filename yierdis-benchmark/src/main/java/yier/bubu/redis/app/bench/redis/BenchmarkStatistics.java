package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.app.bench.LatencyRecorder;

import java.util.Objects;

public record BenchmarkStatistics(
        int requestedRequests,
        long completedRequests,
        long wireRequests,
        long histogramSamples,
        long elapsedMillis,
        LatencyRecorder.Summary latency
) {
    public BenchmarkStatistics {
        latency = Objects.requireNonNull(latency, "latency");
        if (histogramSamples != requestedRequests) {
            throw new IllegalArgumentException("histogramSamples must equal requestedRequests");
        }
        if (latency.count() != histogramSamples) {
            throw new IllegalArgumentException("latency count must equal histogramSamples");
        }
    }

    public double requestsPerSecond() {
        return elapsedMillis == 0 ? 0.0 : completedRequests / (elapsedMillis / 1000.0);
    }
}
