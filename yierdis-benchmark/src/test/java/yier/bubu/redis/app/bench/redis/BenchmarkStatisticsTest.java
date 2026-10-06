package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.LatencyRecorder;

public class BenchmarkStatisticsTest {
    @Test
    public void successfulStatisticsUseStopBoundaryCompletedRequestsForRps() {
        LatencyRecorder.Summary latency =
                new LatencyRecorder.Summary(100, 200.0, 100, 200, 300, 400, 500);

        BenchmarkStatistics statistics = new BenchmarkStatistics(100, 102, 104, 100, 40_000_000L, latency);

        Assert.assertEquals(100, statistics.requestedRequests());
        Assert.assertEquals(102, statistics.completedRequests());
        Assert.assertEquals(104, statistics.wireRequests());
        Assert.assertEquals(100, statistics.histogramSamples());
        Assert.assertEquals(40_000_000L, statistics.elapsedNanos());
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
    public void subMillisecondElapsedKeepsTheNanosecondRateFinite() {
        long elapsedNanos = 500_000L;
        long completed = 112L;
        BenchmarkStatistics statistics = new BenchmarkStatistics(
                1, completed, completed, 1, elapsedNanos, summaryWithCount(1)
        );

        double expected = completed * 1_000_000_000.0 / elapsedNanos;
        Assert.assertEquals(expected, statistics.requestsPerSecond(), 0.0);
        Assert.assertTrue(Double.isFinite(statistics.requestsPerSecond()));
        Assert.assertNotEquals(0.0, statistics.requestsPerSecond(), 0.0);
    }

    @Test
    public void onePointNineMillisecondsIsNotTruncatedToOneMillisecond() {
        long elapsedNanos = 1_900_000L;
        long completed = 112L;
        BenchmarkStatistics statistics = new BenchmarkStatistics(
                1, completed, completed, 1, elapsedNanos, summaryWithCount(1)
        );

        double expected = completed * 1_000_000_000.0 / elapsedNanos;
        Assert.assertEquals(expected, statistics.requestsPerSecond(), 0.0);
        Assert.assertTrue(Double.isFinite(statistics.requestsPerSecond()));
        Assert.assertNotEquals(completed / 0.001, statistics.requestsPerSecond(), 1.0);
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
        double expected = Long.MAX_VALUE * 1_000_000_000.0 / 1_000_000.0;

        BenchmarkStatistics statistics = new BenchmarkStatistics(
                1, Long.MAX_VALUE, Long.MAX_VALUE, 1, 1_000_000L, latency
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
