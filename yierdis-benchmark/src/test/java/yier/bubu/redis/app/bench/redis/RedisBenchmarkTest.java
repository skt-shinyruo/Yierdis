package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisBenchmarkTest {
    @Test
    public void unsupportedCasesProduceRowsWithoutCallingExecutor() {
        RedisBenchmarkCatalog catalog = new RedisBenchmarkCatalog();
        List<String> executed = new ArrayList<>();
        RedisBenchmark benchmark = new RedisBenchmark(catalog, (testCase, config, payload, random) -> {
            executed.add(testCase.id());
            return success(config);
        });

        BenchmarkRunResult result = benchmark.run(configWithTests("spop,zpopmin,mset,xadd"));

        Assert.assertEquals(List.of("spop", "zpopmin", "mset", "xadd"),
                result.cases().stream().map(row -> row.testCase().id()).toList());
        Assert.assertEquals(List.of(
                        BenchmarkStatus.UNSUPPORTED,
                        BenchmarkStatus.UNSUPPORTED,
                        BenchmarkStatus.UNSUPPORTED,
                        BenchmarkStatus.UNSUPPORTED
                ),
                result.cases().stream().map(BenchmarkCaseResult::status).toList());
        for (BenchmarkCaseResult row : result.cases()) {
            Assert.assertEquals(row.testCase().support().reason(), row.reason());
        }
        Assert.assertTrue(executed.isEmpty());
        Assert.assertEquals(0, result.exitCode());
    }

    @Test
    public void failedLrangeSetupSkipsAllSelectedDependentsAndRunContinues() {
        List<String> executed = new ArrayList<>();
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            executed.add(testCase.id());
            if (testCase.id().equals("lrange_setup")) {
                throw new IOException("setup failed");
            }
            return success(config);
        });

        BenchmarkRunResult result = benchmark.run(configWithTests("lrange,set"));

        Assert.assertEquals(List.of("set", "lrange_setup"), executed);
        Assert.assertEquals(BenchmarkStatus.SUCCESS, result.caseById("set").status());
        Assert.assertEquals(BenchmarkStatus.FAILED, result.caseById("lrange_setup").status());
        for (String id : List.of("lrange_100", "lrange_300", "lrange_500", "lrange_600")) {
            BenchmarkCaseResult dependent = result.caseById(id);
            Assert.assertEquals(BenchmarkStatus.SKIPPED, dependent.status());
            Assert.assertEquals("dependency lrange_setup did not succeed", dependent.reason());
        }
        Assert.assertEquals(1, result.exitCode());
    }

    @Test
    public void payloadAndRandomSourceAreSharedAcrossOneCatalogPass() {
        BenchmarkConfig config = new BenchmarkConfig(
                "127.0.0.1", 16378, 2, 1, 3, 1,
                OptionalLong.of(10_000), true, Set.of("set", "get"),
                3, 73L, BenchmarkFormat.HUMAN, "", "", 0
        );
        List<byte[]> payloads = new ArrayList<>();
        List<BenchmarkRandom> randoms = new ArrayList<>();
        List<Long> observed = new ArrayList<>();
        RedisBenchmark benchmark = benchmark((testCase, ignored, payload, random) -> {
            payloads.add(payload);
            randoms.add(random);
            observed.add(random.nextLong(10_000));
            return success(config);
        });

        BenchmarkRunResult result = benchmark.run(config);

        Assert.assertEquals(List.of(BenchmarkStatus.SUCCESS, BenchmarkStatus.SUCCESS),
                result.cases().stream().map(BenchmarkCaseResult::status).toList());
        Assert.assertSame(payloads.get(0), payloads.get(1));
        Assert.assertArrayEquals(BenchmarkPayload.generate(3), payloads.get(0));
        Assert.assertSame(randoms.get(0), randoms.get(1));
        BenchmarkRandom expected = new BenchmarkRandom(73L);
        Assert.assertEquals(
                List.of(expected.nextLong(10_000), expected.nextLong(10_000)),
                observed
        );
    }

    @Test
    public void unknownSelectorFailsBeforeExecutorTraffic() {
        AtomicBoolean executed = new AtomicBoolean();
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            executed.set(true);
            return success(config);
        });

        Assert.assertThrows(IllegalArgumentException.class,
                () -> benchmark.run(configWithTests("no_such_test")));
        Assert.assertFalse(executed.get());
    }

    @Test
    public void typedExecutionFailurePreservesCompletedCountAndDetail() {
        BenchmarkExecutionException typedFailure = new BenchmarkExecutionException(
                "SET",
                7,
                10,
                "typed failure detail",
                new IOException("root cause")
        );
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            throw typedFailure;
        });

        BenchmarkRunResult result = benchmark.run(configWithTests("set"));
        BenchmarkCaseResult failed = result.caseById("set");

        Assert.assertEquals(BenchmarkStatus.FAILED, failed.status());
        Assert.assertEquals(7, failed.completedReplies());
        Assert.assertEquals("typed failure detail", failed.reason());
        Assert.assertNotEquals(typedFailure.getMessage(), failed.reason());
        Assert.assertEquals(1, result.exitCode());
    }

    @Test
    public void ordinaryExceptionBecomesZeroCompletedFailureAndNextCaseRuns() {
        List<String> executed = new ArrayList<>();
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            executed.add(testCase.id());
            if (testCase.id().equals("set")) {
                throw new IOException("connection reset after 9 replies");
            }
            return success(config);
        });

        BenchmarkRunResult result = benchmark.run(configWithTests("set,get"));

        BenchmarkCaseResult failed = result.caseById("set");
        Assert.assertEquals(List.of("set", "get"), executed);
        Assert.assertEquals(BenchmarkStatus.FAILED, failed.status());
        Assert.assertEquals(0, failed.completedReplies());
        Assert.assertEquals("connection reset after 9 replies", failed.reason());
        Assert.assertEquals(BenchmarkStatus.SUCCESS, result.caseById("get").status());
        Assert.assertEquals(1, result.exitCode());
    }

    @Test
    public void blankOrdinaryExceptionMessageFallsBackToSimpleClassName() {
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            throw new IOException(" \t");
        });

        BenchmarkCaseResult failed = benchmark.run(configWithTests("set")).caseById("set");

        Assert.assertEquals(BenchmarkStatus.FAILED, failed.status());
        Assert.assertEquals(0, failed.completedReplies());
        Assert.assertEquals("IOException", failed.reason());
    }

    @Test
    public void errorIsNotSwallowed() {
        AssertionError fatal = new AssertionError("fatal");
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            throw fatal;
        });

        AssertionError thrown = Assert.assertThrows(
                AssertionError.class,
                () -> benchmark.run(configWithTests("set,get"))
        );

        Assert.assertSame(fatal, thrown);
    }

    @Test
    public void nullConfigFailsBeforeExecutorTraffic() {
        AtomicBoolean executed = new AtomicBoolean();
        RedisBenchmark benchmark = benchmark((testCase, config, payload, random) -> {
            executed.set(true);
            return success(config);
        });

        NullPointerException failure = Assert.assertThrows(
                NullPointerException.class,
                () -> benchmark.run(null)
        );

        Assert.assertEquals("config", failure.getMessage());
        Assert.assertFalse(executed.get());
    }

    private static RedisBenchmark benchmark(BenchmarkCaseExecutor executor) {
        return new RedisBenchmark(new RedisBenchmarkCatalog(), executor);
    }

    private static BenchmarkConfig configWithTests(String commaSeparatedTests) {
        return new BenchmarkConfig(
                "127.0.0.1", 16378, 2, 1, 3, 1,
                OptionalLong.of(10_000), true, Set.of(commaSeparatedTests.split(",")),
                3, 73L, BenchmarkFormat.HUMAN, "", "", 0
        );
    }

    private static BenchmarkStatistics success(BenchmarkConfig config) {
        BenchmarkLatencyRecorder recorder = new BenchmarkLatencyRecorder(config.precision());
        for (int sample = 0; sample < config.requests(); sample++) {
            recorder.recordMicros(100 + sample);
        }
        return BenchmarkStatistics.from(
                config.requests(),
                config.requests(),
                config.requests(),
                config.requests(),
                10,
                recorder.summary()
        );
    }
}
