package yier.bubu.redis.app.bench.redis;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

public class BenchmarkLatencyRecorderTest {
    @Test
    public void histogramClampsAtThreeSecondsAndProducesSummary() {
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(3);
        recorder.recordMicros(100);
        recorder.recordMicros(200);
        recorder.recordMicros(4_000_000);

        BenchmarkLatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(3, summary.count());
        Assert.assertEquals(3_000_319L, summary.maxMicros());
        Assert.assertTrue(summary.p50Micros() >= 100);
        Assert.assertTrue(summary.p99Micros() >= summary.p50Micros());
    }

    @Test
    public void precisionFromZeroThroughFourIsSupported() {
        for (int precision = 0; precision <= 4; precision++) {
            BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(precision);
            recorder.recordMicros(100);
            Assert.assertEquals(1, recorder.summary().count());
        }
    }

    @Test
    public void precisionOutsideZeroThroughFourIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new BenchmarkLatencyRecorder(-1));
        Assert.assertThrows(IllegalArgumentException.class, () -> new BenchmarkLatencyRecorder(5));
    }

    @Test
    public void negativeLatencyIsRejected() {
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(3);

        Assert.assertThrows(IllegalArgumentException.class, () -> recorder.recordMicros(-1));
        Assert.assertEquals(0, recorder.summary().count());
    }

    @Test
    public void exactMaximumAndLargerValuesUseTheHistogramMaximumBucket() {
        BenchmarkLatencyRecorder exactMaximum = new BenchmarkLatencyRecorder(3);
        exactMaximum.recordMicros(3_000_000);
        BenchmarkLatencyRecorder aboveMaximum = new BenchmarkLatencyRecorder(3);
        aboveMaximum.recordMicros(3_000_001);

        Assert.assertEquals(exactMaximum.summary(), aboveMaximum.summary());
        Assert.assertEquals(3_000_319L, exactMaximum.summary().maxMicros());
    }

    @Test
    public void summaryReportsHdrHistogramEquivalentValues() {
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(3);
        for (long value : new long[]{100, 200, 300, 400, 500}) {
            recorder.recordMicros(value);
        }

        BenchmarkLatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(5, summary.count());
        Assert.assertEquals(301.6, summary.meanMicros(), 0.0001);
        Assert.assertEquals(96, summary.minMicros());
        Assert.assertEquals(303, summary.p50Micros());
        Assert.assertEquals(503, summary.p95Micros());
        Assert.assertEquals(503, summary.p99Micros());
        Assert.assertEquals(503, summary.maxMicros());
    }

    @Test
    public void summaryMatchesTheUnderlyingHdrHistogramWithoutCorrection() {
        long[] values = {0, 10, 99, 1_001, 3_000_000, 3_500_000};
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(4);
        Histogram expected = new Histogram(10, 3_000_000, 4);
        for (long value : values) {
            recorder.recordMicros(value);
            expected.recordValue(Math.min(value, 3_000_000));
        }

        BenchmarkLatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(expected.getTotalCount(), summary.count());
        Assert.assertEquals(expected.getMean(), summary.meanMicros(), 0.0);
        Assert.assertEquals(expected.getMinValue(), summary.minMicros());
        Assert.assertEquals(expected.getValueAtPercentile(50.0), summary.p50Micros());
        Assert.assertEquals(expected.getValueAtPercentile(95.0), summary.p95Micros());
        Assert.assertEquals(expected.getValueAtPercentile(99.0), summary.p99Micros());
        Assert.assertEquals(expected.getMaxValue(), summary.maxMicros());
    }

    @Test
    public void emptySummaryIsSafeAndExplicitlyZeroValued() {
        BenchmarkLatencyRecorder.Summary summary = new BenchmarkLatencyRecorder(3).summary();

        Assert.assertEquals(0, summary.count());
        Assert.assertEquals(0.0, summary.meanMicros(), 0.0);
        Assert.assertEquals(0, summary.minMicros());
        Assert.assertEquals(0, summary.p50Micros());
        Assert.assertEquals(0, summary.p95Micros());
        Assert.assertEquals(0, summary.p99Micros());
        Assert.assertEquals(0, summary.maxMicros());
    }

    @Test
    public void summaryRejectsMalformedMetrics() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkLatencyRecorder.Summary(1, Double.NaN, 0, 0, 0, 0, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkLatencyRecorder.Summary(1, 1.0, 2, 1, 1, 1, 1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkLatencyRecorder.Summary(0, 1.0, 0, 0, 0, 0, 0));
    }

    @Test
    public void successfulStatisticsUseStopBoundaryCompletedRequestsForRps() {
        BenchmarkLatencyRecorder.Summary latency =
                new BenchmarkLatencyRecorder.Summary(100, 200.0, 100, 200, 300, 400, 500);

        BenchmarkStatistics statistics = BenchmarkStatistics.from(100, 102, 104, 100, 40L, latency);

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
        BenchmarkStatistics statistics = BenchmarkStatistics.from(
                1, 2, 2, 1, 0, summaryWithCount(1)
        );

        Assert.assertEquals(0.0, statistics.requestsPerSecond(), 0.0);
        Assert.assertTrue(Double.isFinite(statistics.requestsPerSecond()));
    }

    @Test
    public void statisticsRequirePositiveRequestedRequestsAndNonNegativeElapsedTime() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(0, 0, 0, 0, 1, summaryWithCount(0)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(-1, 0, 0, -1, 1, summaryWithCount(0)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(1, 1, 1, 1, -1, summaryWithCount(1)));
    }

    @Test
    public void statisticsRequireRequestedCompletedAndWireCountersInOrder() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(10, 10, 9, 10, 1, summaryWithCount(10)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(10, 9, 10, 10, 1, summaryWithCount(10)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(10, 11, 10, 10, 1, summaryWithCount(10)));
    }

    @Test
    public void statisticsRequireOneLatencySamplePerRequestedRequest() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(10, 10, 10, 9, 1, summaryWithCount(9)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(10, 10, 10, 10, 1, summaryWithCount(9)));
        Assert.assertThrows(NullPointerException.class,
                () -> BenchmarkStatistics.from(10, 10, 10, 10, 1, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> BenchmarkStatistics.from(1, 1, 1, 1, 1, summaryWithCount(0)));
    }

    @Test
    public void statisticsDerivesRateExactlyForExtremeCounters() {
        BenchmarkLatencyRecorder.Summary latency = summaryWithCount(1);
        double expected = Long.MAX_VALUE / (1 / 1000.0);

        BenchmarkStatistics statistics = BenchmarkStatistics.from(
                1, Long.MAX_VALUE, Long.MAX_VALUE, 1, 1, latency
        );

        Assert.assertEquals(expected, statistics.requestsPerSecond(), 0.0);
    }

    private static BenchmarkLatencyRecorder.Summary summaryWithCount(long count) {
        if (count == 0) {
            return new BenchmarkLatencyRecorder.Summary(0, 0.0, 0, 0, 0, 0, 0);
        }
        return new BenchmarkLatencyRecorder.Summary(count, 100.0, 96, 103, 103, 103, 103);
    }

}
