package yier.bubu.redis.integration.benchmark;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.redis.BenchmarkCaseResult;
import yier.bubu.redis.app.bench.redis.BenchmarkConfig;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;
import yier.bubu.redis.app.bench.redis.BenchmarkRunResult;
import yier.bubu.redis.app.bench.redis.BenchmarkStatistics;
import yier.bubu.redis.app.bench.redis.BenchmarkStatus;
import yier.bubu.redis.app.bench.redis.RedisBenchmark;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

public class RedisBenchmarkRealServerTest {
    @Test
    public void allCatalogCasesRunAgainstRealYierdisServer() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0", "--noCleanup"
        )) {
            BenchmarkConfig config = new BenchmarkConfig(
                    "127.0.0.1",
                    server.port(),
                    40,
                    4,
                    3,
                    3,
                    OptionalLong.empty(),
                    true,
                    Set.of(),
                    3,
                    1L,
                    BenchmarkFormat.CSV,
                    "",
                    "",
                    0
            );

            BenchmarkRunResult run = new RedisBenchmark().run(config);
            String diagnostics = diagnostics(run.cases());
            List<BenchmarkCaseResult> successes = run.cases().stream()
                    .filter(result -> result.status() == BenchmarkStatus.SUCCESS)
                    .toList();
            List<String> unsupportedIds = run.cases().stream()
                    .filter(result -> result.status() == BenchmarkStatus.UNSUPPORTED)
                    .map(result -> result.testCase().id())
                    .toList();

            Assert.assertEquals(diagnostics, 21, run.cases().size());
            Assert.assertEquals(diagnostics, 17, successes.size());
            Assert.assertEquals(
                    diagnostics,
                    List.of("spop", "zpopmin", "mset", "xadd"),
                    unsupportedIds
            );
            Assert.assertFalse(
                    diagnostics,
                    run.cases().stream().anyMatch(result ->
                            result.status() == BenchmarkStatus.FAILED
                                    || result.status() == BenchmarkStatus.SKIPPED)
            );
            Assert.assertEquals(diagnostics, 0, run.exitCode());

            for (BenchmarkCaseResult result : run.cases()) {
                String message = diagnostics + "; case=" + result.testCase().id();
                if (result.status() == BenchmarkStatus.SUCCESS) {
                    BenchmarkStatistics statistics = result.statistics();
                    Assert.assertNotNull(message, statistics);
                    Assert.assertEquals(message, 40, statistics.requestedRequests());
                    Assert.assertEquals(message, 40L, statistics.histogramSamples());
                    Assert.assertEquals(message, 42L, statistics.wireRequests());
                    Assert.assertTrue(
                            message,
                            statistics.completedRequests() >= 40L
                                    && statistics.completedRequests() <= 42L
                    );
                } else if (result.status() == BenchmarkStatus.UNSUPPORTED) {
                    Assert.assertNull(message, result.statistics());
                    Assert.assertEquals(message, 0L, result.completedReplies());
                    Assert.assertFalse(message, result.reason().isBlank());
                } else {
                    Assert.fail(message);
                }
            }
        }
    }

    private static String diagnostics(List<BenchmarkCaseResult> results) {
        return results.stream()
                .map(result -> result.testCase().id()
                        + ":" + result.status()
                        + ":" + result.reason())
                .toList()
                .toString();
    }
}
