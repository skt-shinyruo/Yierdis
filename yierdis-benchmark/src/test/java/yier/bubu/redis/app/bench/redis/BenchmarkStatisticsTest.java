package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.LatencyRecorder;

public class BenchmarkStatisticsTest {
    @Test
    public void successfulStatisticsUseStopBoundaryCompletedRequestsForRps() {
        LatencyRecorder.Summary latency =
                new LatencyRecorder.Summary(100, 200.0, 100, 200, 300, 400, 500);

        BenchmarkStatistics statistics = new BenchmarkStatistics(100, 102, 104, 100, 40L, latency);

        Assert.assertEquals(100, statistics.requestedRequests());
        Assert.assertEquals(102, statistics.completedRequests());
        Assert.assertEquals(104, statistics.wireRequests());
        Assert.assertEquals(100, statistics.histogramSamples());
        Assert.assertEquals(40, statistics.elapsedMillis());
        Assert.assertEquals(2550.0, statistics.requestsPerSecond(), 0.001);
        Assert.assertSame(latency, statistics.latency());
    }

    @Test
    public void zeroElapsedTimeProducesFiniteZeroRequestsPerSecond() {
        BenchmarkStatistics statistics = new BenchmarkStatistics(
                1, 2, 2, 1, 0, summaryWithCount(1)
        );

        Assert.assertEquals(0.0, statistics.requestsPerSecond(), 0.0);
        Assert.assertTrue(Double.isFinite(statistics.requestsPerSecond()));
    }

    @Test
    public void statisticsRequireOneLatencySamplePerRequestedRequest() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkStatistics(10, 10, 10, 9, 1, summaryWithCount(9)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkStatistics(10, 10, 10, 10, 1, summaryWithCount(9)));
        Assert.assertThrows(NullPointerException.class,
                () -> new BenchmarkStatistics(10, 10, 10, 10, 1, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkStatistics(1, 1, 1, 1, 1, summaryWithCount(0)));
    }

    @Test
    public void statisticsDerivesRateExactlyForExtremeCounters() {
        LatencyRecorder.Summary latency = summaryWithCount(1);
        double expected = Long.MAX_VALUE / (1 / 1000.0);

        BenchmarkStatistics statistics = new BenchmarkStatistics(
                1, Long.MAX_VALUE, Long.MAX_VALUE, 1, 1, latency
        );

        Assert.assertEquals(expected, statistics.requestsPerSecond(), 0.0);
    }

    private static LatencyRecorder.Summary summaryWithCount(long count) {
        if (count == 0) {
            return new LatencyRecorder.Summary(0, 0.0, 0, 0, 0, 0, 0);
        }
        return new LatencyRecorder.Summary(count, 100.0, 96, 103, 103, 103, 103);
    }
}
