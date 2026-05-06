package yier.bubu.redis.bench;

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
        result.getThroughput = new YierdisBench.ThroughputResult(
                YierdisBench.Workload.GET_RANDOM, 1000, 1, 1.00, 1000.0, Instant.parse("2026-04-19T00:00:01Z")
        );
        result.pingLatency = new YierdisBench.LatencyResult(
                YierdisBench.Workload.PING, 1000, 0, 1.00, 1000.0,
                YierdisBench.LatencyStats.ofSortedNanos(new long[]{1_000_000, 2_000_000, 3_000_000})
        );

        String rendered = YierdisBench.renderSummary(List.of(result), false);

        Assert.assertTrue(rendered.contains("backend"));
        Assert.assertTrue(rendered.contains("SET_QPS"));
        Assert.assertTrue(rendered.contains("GET_QPS"));
        Assert.assertTrue(rendered.contains("PING_p95(ms)"));
        Assert.assertTrue(rendered.contains("foreign"));
        Assert.assertTrue(rendered.contains("800.000"));
        Assert.assertTrue(rendered.contains("1"));
    }
}
