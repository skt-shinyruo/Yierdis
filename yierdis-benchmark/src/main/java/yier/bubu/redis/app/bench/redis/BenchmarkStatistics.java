package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.app.bench.LatencyRecorder;

import java.util.Objects;

public record BenchmarkStatistics(
        int requestedRequests,
        long completedRequests,
        long wireRequests,
        long histogramSamples,
        long elapsedNanos,
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
        // 除数用纳秒，避免先截成毫秒把 1.9ms 算成 1ms。0 纳秒保持有限的 0，不当成除零缺陷。
        return elapsedNanos == 0L ? 0.0 : completedRequests * 1_000_000_000.0 / elapsedNanos;
    }
}
