package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

public class YierdisBenchSummaryFormatTest {
    @Test
    public void renderSummaryProducesStableDeterministicTable() {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult("foreign", 16378);
        result.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 1000, 0, 1.25, 800.0, Instant.parse("2026-04-19T00:00:00Z")
        );
        result.appendThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.APPEND, 1000, 2, 1.10, 900.0, Instant.parse("2026-04-19T00:00:00Z")
        );
        result.getThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.GET_RANDOM, 1000, 1, 1.00, 1000.0, Instant.parse("2026-04-19T00:00:01Z")
        );
        result.pfaddSparseThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFADD_SPARSE, 1000, 4, 1.00, 1100.0, Instant.parse("2026-04-19T00:00:02Z")
        );
        result.pfaddDenseThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFADD_DENSE, 1000, 5, 1.00, 1200.0, Instant.parse("2026-04-19T00:00:03Z")
        );
        result.pfcountThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFCOUNT, 1000, 6, 1.00, 1300.0, Instant.parse("2026-04-19T00:00:04Z")
        );
        result.pingLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PING, 1000, 0, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );
        result.appendLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.APPEND, 1000, 3, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{2_000_000, 3_000_000, 4_000_000})
        );
        result.pfaddSparseLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PFADD_SPARSE, 1000, 7, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{5_000_000, 6_000_000, 7_000_000})
        );
        result.pfaddDenseLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PFADD_DENSE, 1000, 8, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{6_000_000, 7_000_000, 8_000_000})
        );

        String rendered = YierdisBench.renderSummary(List.of(result), false);
        String[] lines = rendered.split("\n");
        String header = lines[0];
        String data = lines[2];
        String[] headerColumns = splitColumns(header);
        String[] dataColumns = splitColumns(data);

        Assert.assertTrue(rendered.contains("backend"));
        Assert.assertTrue(rendered.contains("SET_QPS"));
        Assert.assertTrue(rendered.contains("APPEND_QPS"));
        Assert.assertTrue(rendered.contains("GET_QPS"));
        Assert.assertTrue(rendered.contains("PFADD_S_QPS"));
        Assert.assertTrue(rendered.contains("PFADD_D_QPS"));
        Assert.assertTrue(rendered.contains("PFCOUNT_QPS"));
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("APPEND_p95(ms)"));
        Assert.assertTrue(rendered.contains("PFADD_S_p95(ms)"));
        Assert.assertTrue(rendered.contains("PFADD_D_p95(ms)"));
        Assert.assertTrue(rendered.contains("foreign"));
        Assert.assertTrue(rendered.contains("800.000"));
        Assert.assertTrue(rendered.contains("1"));
        Assert.assertTrue(header.indexOf("SET_QPS") < header.indexOf("GET_QPS"));
        Assert.assertTrue(header.indexOf("GET_QPS") < header.indexOf("APPEND_QPS"));
        Assert.assertTrue(header.indexOf("PING_p95(ms)") < header.indexOf("SET_p95(ms)"));
        Assert.assertTrue(header.indexOf("SET_p95(ms)") < header.indexOf("GET_p95(ms)"));
        Assert.assertTrue(header.indexOf("GET_E") < header.indexOf("APPEND_QPS"));
        Assert.assertTrue(header.indexOf("APPEND_QPS") < header.indexOf("APPEND_p95(ms)"));
        Assert.assertTrue(header.indexOf("APPEND_E") < header.indexOf("PFADD_S_QPS"));
        Assert.assertTrue(header.indexOf("PFADD_S_QPS") < header.indexOf("PFADD_D_QPS"));
        Assert.assertTrue(header.indexOf("PFADD_D_QPS") < header.indexOf("PFCOUNT_QPS"));
        Assert.assertTrue(header.indexOf("PFCOUNT_E") < header.indexOf("PFADD_S_p95(ms)"));
        Assert.assertTrue(data.indexOf("800.000") < data.indexOf("1000.000"));
        Assert.assertTrue(data.indexOf("1000.000") < data.indexOf("900.000"));
        Assert.assertArrayEquals(new String[]{
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR",
                "PING_p95(ms)", "PING_E", "SET_p95(ms)", "SET_E", "GET_p95(ms)", "GET_E",
                "APPEND_QPS", "APPEND_ERR", "APPEND_p95(ms)", "APPEND_E",
                "PFADD_S_QPS", "PFADD_S_E", "PFADD_D_QPS", "PFADD_D_E", "PFCOUNT_QPS", "PFCOUNT_E",
                "PFADD_S_p95(ms)", "PFADD_S_E2", "PFADD_D_p95(ms)", "PFADD_D_E2"
        }, headerColumns);
        Assert.assertEquals("foreign", dataColumns[0]);
        Assert.assertEquals("800.000", dataColumns[1]);
        Assert.assertEquals("0", dataColumns[2]);
        Assert.assertEquals("1000.000", dataColumns[3]);
        Assert.assertEquals("1", dataColumns[4]);
        Assert.assertEquals("3.000", dataColumns[5]);
        Assert.assertEquals("0", dataColumns[6]);
        Assert.assertEquals("900.000", dataColumns[11]);
        Assert.assertEquals("2", dataColumns[12]);
        Assert.assertEquals("4.000", dataColumns[13]);
        Assert.assertEquals("3", dataColumns[14]);
        Assert.assertEquals("1100.000", dataColumns[15]);
        Assert.assertEquals("4", dataColumns[16]);
        Assert.assertEquals("1200.000", dataColumns[17]);
        Assert.assertEquals("5", dataColumns[18]);
        Assert.assertEquals("1300.000", dataColumns[19]);
        Assert.assertEquals("6", dataColumns[20]);
        Assert.assertEquals("7.000", dataColumns[21]);
        Assert.assertEquals("7", dataColumns[22]);
        Assert.assertEquals("8.000", dataColumns[23]);
        Assert.assertEquals("8", dataColumns[24]);
    }

    @Test
    public void renderSkipLatencySummaryPreservesExistingSetGetColumnsBeforeAppend() {
        YierdisBench.BackendResult result = new YierdisBench.BackendResult("foreign", 16378);
        result.setThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.SET_RANDOM, 1000, 0, 1.25, 800.0, Instant.parse("2026-04-19T00:00:00Z")
        );
        result.appendThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.APPEND, 1000, 2, 1.10, 900.0, Instant.parse("2026-04-19T00:00:00Z")
        );
        result.getThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.GET_RANDOM, 1000, 1, 1.00, 1000.0, Instant.parse("2026-04-19T00:00:01Z")
        );
        result.pfaddSparseThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFADD_SPARSE, 1000, 4, 1.00, 1100.0, Instant.parse("2026-04-19T00:00:02Z")
        );
        result.pfaddDenseThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFADD_DENSE, 1000, 5, 1.00, 1200.0, Instant.parse("2026-04-19T00:00:03Z")
        );
        result.pfcountThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.PFCOUNT, 1000, 6, 1.00, 1300.0, Instant.parse("2026-04-19T00:00:04Z")
        );

        String rendered = YierdisBench.renderSummary(List.of(result), true);
        String header = rendered.split("\n")[0];
        String[] headerColumns = splitColumns(header);

        Assert.assertTrue(header.indexOf("SET_QPS") < header.indexOf("GET_QPS"));
        Assert.assertTrue(header.indexOf("GET_QPS") < header.indexOf("APPEND_QPS"));
        Assert.assertTrue(header.indexOf("APPEND_ERR") < header.indexOf("PFADD_S_QPS"));
        Assert.assertArrayEquals(new String[]{
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR", "APPEND_QPS", "APPEND_ERR",
                "PFADD_S_QPS", "PFADD_S_E", "PFADD_D_QPS", "PFADD_D_E", "PFCOUNT_QPS", "PFCOUNT_E"
        }, headerColumns);
    }

    private static String[] splitColumns(String row) {
        String[] columns = row.split("\\|");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].trim();
        }
        return columns;
    }
}
