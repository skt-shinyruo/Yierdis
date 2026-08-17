package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BenchmarkCaseResultTest {
    private final RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
    private final RedisBenchmarkCase testCase = catalog.caseById("set");

    @Test
    public void benchmarkStatusesAreExactAndOrdered() {
        Assert.assertArrayEquals(new BenchmarkStatus[]{
                BenchmarkStatus.SUCCESS,
                BenchmarkStatus.UNSUPPORTED,
                BenchmarkStatus.SKIPPED,
                BenchmarkStatus.FAILED
        }, BenchmarkStatus.values());
    }

    @Test
    public void successfulResultCarriesStatisticsAndCompletedReplies() {
        BenchmarkStatistics statistics = statistics(2, 3, 4);

        BenchmarkCaseResult result = BenchmarkCaseResult.success(testCase, statistics);

        Assert.assertSame(testCase, result.testCase());
        Assert.assertEquals(BenchmarkStatus.SUCCESS, result.status());
        Assert.assertSame(statistics, result.statistics());
        Assert.assertEquals("", result.reason());
        Assert.assertEquals(3, result.completedReplies());
    }

    @Test
    public void unsupportedSkippedAndFailedResultsHaveNoMetrics() {
        Assert.assertNull(BenchmarkCaseResult.unsupported(testCase, "missing").statistics());
        Assert.assertNull(BenchmarkCaseResult.skipped(testCase, "dependency").statistics());
        Assert.assertNull(BenchmarkCaseResult.failed(testCase, 7, "disconnect").statistics());
    }

    @Test
    public void resultStatusInvariantsRejectMalformedRows() {
        BenchmarkStatistics statistics = statistics(2, 2, 2);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, null, "", 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, statistics, "failed", 2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, statistics, "", 1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.SKIPPED, null, "", 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.FAILED, null, "failure", -1));
        Assert.assertThrows(NullPointerException.class,
                () -> new BenchmarkCaseResult(testCase, BenchmarkStatus.FAILED, null, null, 0));
    }

    @Test
    public void failedResultRetainsItsCompletedReplyCount() {
        BenchmarkCaseResult failed = BenchmarkCaseResult.failed(testCase, 7, "disconnect");

        Assert.assertEquals(BenchmarkStatus.FAILED, failed.status());
        Assert.assertEquals(7, failed.completedReplies());
        Assert.assertEquals("disconnect", failed.reason());
    }

    @Test
    public void unsupportedAndSkippedResultsHaveNoCompletedReplies() {
        Assert.assertEquals(0, BenchmarkCaseResult.unsupported(testCase, "missing").completedReplies());
        Assert.assertEquals(0, BenchmarkCaseResult.skipped(testCase, "dependency").completedReplies());
    }

    @Test
    public void runResultPreservesOrderAndDefensivelyCopiesRows() {
        BenchmarkCaseResult set = BenchmarkCaseResult.success(testCase, statistics(1, 1, 1));
        BenchmarkCaseResult get = BenchmarkCaseResult.unsupported(catalog.caseById("get"), "missing");
        ArrayList<BenchmarkCaseResult> source = new ArrayList<>(List.of(set, get));

        BenchmarkRunResult result = new BenchmarkRunResult(source);
        source.clear();

        Assert.assertEquals(List.of(set, get), result.cases());
        Assert.assertSame(set, result.caseById("set"));
        Assert.assertSame(get, result.caseById("get"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> result.cases().clear());
    }

    @Test
    public void runResultRejectsNullRowsAndDuplicateCaseIds() {
        BenchmarkCaseResult first = BenchmarkCaseResult.success(testCase, statistics(1, 1, 1));
        BenchmarkCaseResult duplicate = BenchmarkCaseResult.failed(testCase, 0, "failure");

        Assert.assertThrows(NullPointerException.class, () -> new BenchmarkRunResult(null));
        Assert.assertThrows(NullPointerException.class,
                () -> new BenchmarkRunResult(Arrays.asList(first, null)));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new BenchmarkRunResult(List.of(first, duplicate)));
    }

    @Test
    public void caseLookupRejectsMissingOrInvalidIds() {
        BenchmarkRunResult result = new BenchmarkRunResult(List.of(
                BenchmarkCaseResult.success(testCase, statistics(1, 1, 1))
        ));

        Assert.assertSame(result.cases().get(0), result.caseById(" SET "));
        Assert.assertThrows(IllegalArgumentException.class, () -> result.caseById("missing"));
        Assert.assertThrows(IllegalArgumentException.class, () -> result.caseById(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> result.caseById(null));
    }

    @Test
    public void emptyAndNonFailedRunsExitZeroWhileAnyFailureExitsOne() {
        BenchmarkCaseResult success = BenchmarkCaseResult.success(testCase, statistics(1, 1, 1));
        BenchmarkCaseResult unsupported = BenchmarkCaseResult.unsupported(
                catalog.caseById("spop"), "missing"
        );
        BenchmarkCaseResult skipped = BenchmarkCaseResult.skipped(
                catalog.caseById("lrange_100"), "dependency"
        );
        BenchmarkCaseResult failed = BenchmarkCaseResult.failed(catalog.caseById("get"), 7, "disconnect");

        Assert.assertTrue(new BenchmarkRunResult(List.of()).cases().isEmpty());
        Assert.assertEquals(0, new BenchmarkRunResult(List.of()).exitCode());
        Assert.assertEquals(0, new BenchmarkRunResult(List.of(success, unsupported, skipped)).exitCode());
        Assert.assertEquals(1, new BenchmarkRunResult(List.of(success, failed, unsupported)).exitCode());
    }

    private static BenchmarkStatistics statistics(int requested, long completed, long wire) {
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(3);
        for (int sample = 0; sample < requested; sample++) {
            recorder.recordMicros(100 + sample);
        }
        return BenchmarkStatistics.from(
                requested, completed, wire, requested, 10, recorder.summary()
        );
    }
}
