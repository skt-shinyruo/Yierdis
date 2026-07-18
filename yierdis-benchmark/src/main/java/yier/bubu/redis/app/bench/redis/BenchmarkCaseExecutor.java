package yier.bubu.redis.app.bench.redis;

@FunctionalInterface
public interface BenchmarkCaseExecutor {
    BenchmarkStatistics execute(
            RedisBenchmarkCase testCase,
            BenchmarkConfig config,
            byte[] payload,
            BenchmarkRandom random
    ) throws Exception;
}
