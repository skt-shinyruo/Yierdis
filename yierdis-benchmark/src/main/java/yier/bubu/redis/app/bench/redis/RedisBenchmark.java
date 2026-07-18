package yier.bubu.redis.app.bench.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class RedisBenchmark {
    private final RedisBenchmarkCatalog catalog;
    private final BenchmarkCaseExecutor executor;

    public RedisBenchmark() {
        this(new RedisBenchmarkCatalog(), new NioBenchmarkRunner());
    }

    RedisBenchmark(RedisBenchmarkCatalog catalog, BenchmarkCaseExecutor executor) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public BenchmarkRunResult run(BenchmarkConfig config) {
        BenchmarkConfig requiredConfig = Objects.requireNonNull(config, "config");
        List<RedisBenchmarkCase> selected = catalog.select(requiredConfig.tests());
        byte[] payload = BenchmarkPayload.generate(requiredConfig.dataSize());
        BenchmarkRandom random = new BenchmarkRandom(requiredConfig.seed());
        Map<String, BenchmarkCaseResult> completed = new HashMap<>();
        List<BenchmarkCaseResult> results = new ArrayList<>(selected.size());

        for (RedisBenchmarkCase testCase : selected) {
            BenchmarkCaseResult result = runCase(
                    testCase,
                    requiredConfig,
                    payload,
                    random,
                    completed
            );
            completed.put(testCase.id(), result);
            results.add(result);
        }
        return new BenchmarkRunResult(results);
    }

    private BenchmarkCaseResult runCase(
            RedisBenchmarkCase testCase,
            BenchmarkConfig config,
            byte[] payload,
            BenchmarkRandom random,
            Map<String, BenchmarkCaseResult> completed
    ) {
        if (!testCase.support().supported()) {
            return BenchmarkCaseResult.unsupported(testCase, testCase.support().reason());
        }

        String dependencyId = testCase.dependencyId();
        if (!dependencyId.isEmpty()) {
            BenchmarkCaseResult dependency = completed.get(dependencyId);
            if (dependency == null || dependency.status() != BenchmarkStatus.SUCCESS) {
                return BenchmarkCaseResult.skipped(
                        testCase,
                        "dependency " + dependencyId + " did not succeed"
                );
            }
        }

        try {
            return BenchmarkCaseResult.success(
                    testCase,
                    executor.execute(testCase, config, payload, random)
            );
        } catch (BenchmarkExecutionException failure) {
            return BenchmarkCaseResult.failed(
                    testCase,
                    failure.completedReplies(),
                    failure.detail()
            );
        } catch (Exception failure) {
            return BenchmarkCaseResult.failed(testCase, 0, conciseMessage(failure));
        }
    }

    private static String conciseMessage(Exception failure) {
        String message = failure.getMessage();
        if (message != null && !message.isBlank()) {
            return message;
        }
        String simpleName = failure.getClass().getSimpleName();
        return simpleName.isBlank() ? Exception.class.getSimpleName() : simpleName;
    }
}
