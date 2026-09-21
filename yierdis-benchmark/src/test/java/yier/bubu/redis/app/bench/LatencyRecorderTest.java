package yier.bubu.redis.app.bench;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

public class LatencyRecorderTest {
    @Test
    public void microsHistogramClampsAtThreeSecondsAndProducesSummary() {
        LatencyRecorder recorder = LatencyRecorder.micros(3);
        recorder.record(100);
        recorder.record(200);
        recorder.record(4_000_000);

        LatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(3, summary.count());
        Assert.assertEquals(3_000_319L, summary.max());
        Assert.assertTrue(summary.p50() >= 100);
        Assert.assertTrue(summary.p99() >= summary.p50());
    }

    @Test
    public void nanosHistogramRoundsZeroUpToOneAndClampsAtTenSeconds() {
        LatencyRecorder recorder = LatencyRecorder.nanos(2);
        recorder.record(0);
        recorder.record(20_000_000_000L);

        Histogram expected = new Histogram(1, 10_000_000_000L, 2);
        expected.recordValue(1);
        expected.recordValue(10_000_000_000L);

        LatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(2, summary.count());
        Assert.assertEquals(expected.getMinValue(), summary.min());
        Assert.assertEquals(expected.getMaxValue(), summary.max());
    }

    @Test
    public void precisionFromZeroThroughFourIsSupported() {
        for (int precision = 0; precision <= 4; precision++) {
            LatencyRecorder recorder = LatencyRecorder.micros(precision);
            recorder.record(100);
            Assert.assertEquals(1, recorder.summary().count());
        }
    }

    @Test
    public void precisionOutsideZeroThroughFourIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () -> LatencyRecorder.micros(-1));
        Assert.assertThrows(IllegalArgumentException.class, () -> LatencyRecorder.micros(5));
        Assert.assertThrows(IllegalArgumentException.class, () -> LatencyRecorder.nanos(-1));
        Assert.assertThrows(IllegalArgumentException.class, () -> LatencyRecorder.nanos(5));
    }

    @Test
    public void negativeLatencyIsRejected() {
        LatencyRecorder recorder = LatencyRecorder.micros(3);

        Assert.assertThrows(IllegalArgumentException.class, () -> recorder.record(-1));
        Assert.assertEquals(0, recorder.summary().count());
    }

    @Test
    public void exactMaximumAndLargerValuesUseTheHistogramMaximumBucket() {
        LatencyRecorder exactMaximum = LatencyRecorder.micros(3);
        exactMaximum.record(3_000_000);
        LatencyRecorder aboveMaximum = LatencyRecorder.micros(3);
        aboveMaximum.record(3_000_001);

        Assert.assertEquals(exactMaximum.summary(), aboveMaximum.summary());
        Assert.assertEquals(3_000_319L, exactMaximum.summary().max());
    }

    @Test
    public void summaryReportsHdrHistogramEquivalentValues() {
        LatencyRecorder recorder = LatencyRecorder.micros(3);
        for (long value : new long[]{100, 200, 300, 400, 500}) {
            recorder.record(value);
        }

        LatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(5, summary.count());
        Assert.assertEquals(301.6, summary.mean(), 0.0001);
        Assert.assertEquals(96, summary.min());
        Assert.assertEquals(303, summary.p50());
        Assert.assertEquals(503, summary.p95());
        Assert.assertEquals(503, summary.p99());
        Assert.assertEquals(503, summary.max());
    }

    @Test
    public void summaryMatchesTheUnderlyingHdrHistogramWithoutCorrection() {
        long[] values = {0, 10, 99, 1_001, 3_000_000, 3_500_000};
        LatencyRecorder recorder = LatencyRecorder.micros(4);
        Histogram expected = new Histogram(10, 3_000_000, 4);
        for (long value : values) {
            recorder.record(value);
            expected.recordValue(Math.min(value, 3_000_000));
        }

        LatencyRecorder.Summary summary = recorder.summary();
        Assert.assertEquals(expected.getTotalCount(), summary.count());
        Assert.assertEquals(expected.getMean(), summary.mean(), 0.0);
        Assert.assertEquals(expected.getMinValue(), summary.min());
        Assert.assertEquals(expected.getValueAtPercentile(50.0), summary.p50());
        Assert.assertEquals(expected.getValueAtPercentile(95.0), summary.p95());
        Assert.assertEquals(expected.getValueAtPercentile(99.0), summary.p99());
        Assert.assertEquals(expected.getMaxValue(), summary.max());
    }

    @Test
    public void emptySummaryIsSafeAndExplicitlyZeroValued() {
        LatencyRecorder.Summary summary = LatencyRecorder.micros(3).summary();

        Assert.assertEquals(0, summary.count());
        Assert.assertEquals(0.0, summary.mean(), 0.0);
        Assert.assertEquals(0, summary.min());
        Assert.assertEquals(0, summary.p50());
        Assert.assertEquals(0, summary.p95());
        Assert.assertEquals(0, summary.p99());
        Assert.assertEquals(0, summary.max());
    }

}
