package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;

import java.util.List;

public class NativeEvalFormatTest {
    @Test
    public void benchArgsExposeNativeEvalWithoutChangingExternalBackendSelection() {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs("--nativeEval", "--nativeEvalIterations", "17", "--noStartServer");

        YierdisBenchServerArgs serverArgs = new YierdisBenchServerArgs();
        serverArgs.normalizeAndValidate();
        YierdisBench.BenchConfig config = YierdisBench.BenchConfig.from(args, serverArgs);

        Assert.assertTrue(args.nativeEval);
        Assert.assertEquals(17, args.nativeEvalIterations);
        Assert.assertEquals(List.of("external"), config.backends);
    }

    @Test
    public void nativeEvalRejectsNonPositiveIterations() {
        YierdisBenchArgs args = new YierdisBenchArgs();
        new CommandLine(args).parseArgs("--nativeEval", "--nativeEvalIterations", "0");

        try {
            YierdisBench.BenchConfig.from(args, new YierdisBenchServerArgs());
            Assert.fail("expected nativeEvalIterations validation failure");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("nativeEvalIterations"));
        }
    }

    @Test
    public void renderNativeEvalReportUsesStableSectionLabelsAndColumns() {
        YierdisBench.NativeEvalReport report = new YierdisBench.NativeEvalReport(
                List.of(new YierdisBench.NativeSizeClassResult("small-64", 64, 100, 1.25, 0.75)),
                new YierdisBench.NativeResolveResult(100, 0.50),
                new YierdisBench.NativeReallocResult(100, 70, 30, 2.25),
                new YierdisBench.NativePinResult(100, 0.30),
                List.of(new YierdisBench.NativeMetadataResult("small", 64, 24.0, 37.5)),
                new YierdisBench.NativeQuarantineResult(4096, 12.5),
                new YierdisBench.NativeChurnResult(1000, 3.5, 7.0),
                new YierdisBench.NativeDefragImpactResult(1.0, 1.2, 20.0, 16)
        );

        String rendered = YierdisBench.renderNativeEvalReport(report);

        Assert.assertTrue(rendered.contains("[native-allocator] allocate/free"));
        Assert.assertArrayEquals(new String[]{"class", "bytes", "ops", "alloc_us", "free_us"},
                splitColumns(rendered.split("\n")[1]));
        Assert.assertTrue(rendered.contains("small-64"));
        Assert.assertTrue(rendered.contains("[native-allocator] resolve/close"));
        Assert.assertTrue(rendered.contains("[native-allocator] realloc"));
        Assert.assertTrue(rendered.contains("in_place"));
        Assert.assertTrue(rendered.contains("moved"));
        Assert.assertTrue(rendered.contains("[native-allocator] pin/unpin"));
        Assert.assertTrue(rendered.contains("[native-allocator] object-table metadata"));
        Assert.assertTrue(rendered.contains("metadata_pct"));
        Assert.assertTrue(rendered.contains("[native-allocator] quarantine/epoch churn"));
        Assert.assertTrue(rendered.contains("retained_bytes"));
        Assert.assertTrue(rendered.contains("retained_pct_reserved"));
        Assert.assertTrue(rendered.contains("[native-allocator] small-object churn"));
        Assert.assertTrue(rendered.contains("[native-allocator] defrag p99 impact"));
        Assert.assertTrue(rendered.contains("disabled_p99_us"));
        Assert.assertTrue(rendered.contains("enabled_p99_us"));
    }

    @Test
    public void renderDbDefragComparisonUsesStableColumns() {
        YierdisBench.LatencyResult disabled = latencyResult(new long[]{1_000_000, 2_000_000, 3_000_000}, 0);
        YierdisBench.LatencyResult enabled = latencyResult(new long[]{1_000_000, 3_000_000, 6_000_000}, 1);
        YierdisBench.DbDefragComparisonResult result = new YierdisBench.DbDefragComparisonResult(disabled, enabled, 100.0);

        String rendered = YierdisBench.renderDbDefragComparison(result);
        String[] lines = rendered.split("\n");

        Assert.assertArrayEquals(new String[]{"disabled_p99_ms", "enabled_p99_ms", "impact_pct", "enabled_err"},
                splitColumns(lines[0]));
        Assert.assertArrayEquals(new String[]{"3.000", "6.000", "100.000", "1"},
                splitColumns(lines[2]));
    }

    private static YierdisBench.LatencyResult latencyResult(long[] sortedNanos, long errors) {
        return new YierdisBench.LatencyResult(
                YierdisBench.Workload.APPEND,
                sortedNanos.length,
                errors,
                1.0,
                sortedNanos.length,
                YierdisBench.LatencyStats.ofSortedNanos(sortedNanos)
        );
    }

    private static String[] splitColumns(String row) {
        String[] columns = row.split("\\|");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].trim();
        }
        return columns;
    }
}
