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
        result.pingLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PING, 1000, 0, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );
        result.appendLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.APPEND, 1000, 3, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{2_000_000, 3_000_000, 4_000_000})
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
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("APPEND_p95(ms)"));
        Assert.assertTrue(rendered.contains("foreign"));
        Assert.assertTrue(rendered.contains("800.000"));
        Assert.assertTrue(rendered.contains("1"));
        Assert.assertTrue(header.indexOf("SET_QPS") < header.indexOf("GET_QPS"));
        Assert.assertTrue(header.indexOf("GET_QPS") < header.indexOf("APPEND_QPS"));
        Assert.assertTrue(header.indexOf("PING_p95(ms)") < header.indexOf("SET_p95(ms)"));
        Assert.assertTrue(header.indexOf("SET_p95(ms)") < header.indexOf("GET_p95(ms)"));
        Assert.assertTrue(header.indexOf("GET_E") < header.indexOf("APPEND_QPS"));
        Assert.assertTrue(header.indexOf("APPEND_QPS") < header.indexOf("APPEND_p95(ms)"));
        Assert.assertTrue(data.indexOf("800.000") < data.indexOf("1000.000"));
        Assert.assertTrue(data.indexOf("1000.000") < data.indexOf("900.000"));
        Assert.assertArrayEquals(new String[]{
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR",
                "PING_p95(ms)", "PING_E", "SET_p95(ms)", "SET_E", "GET_p95(ms)", "GET_E",
                "APPEND_QPS", "APPEND_ERR", "APPEND_p95(ms)", "APPEND_E"
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

        String rendered = YierdisBench.renderSummary(List.of(result), true);
        String header = rendered.split("\n")[0];
        String[] headerColumns = splitColumns(header);

        Assert.assertTrue(header.indexOf("SET_QPS") < header.indexOf("GET_QPS"));
        Assert.assertTrue(header.indexOf("GET_QPS") < header.indexOf("APPEND_QPS"));
        Assert.assertArrayEquals(new String[]{
                "backend", "SET_QPS", "SET_ERR", "GET_QPS", "GET_ERR", "APPEND_QPS", "APPEND_ERR"
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
