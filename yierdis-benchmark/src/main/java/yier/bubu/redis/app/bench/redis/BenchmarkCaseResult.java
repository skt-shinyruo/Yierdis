package yier.bubu.redis.app.bench.redis;

public record BenchmarkCaseResult(
        RedisBenchmarkCase testCase,
        BenchmarkStatus status,
        BenchmarkStatistics statistics,
        String reason,
        long completedReplies
) {
    public static BenchmarkCaseResult success(
            RedisBenchmarkCase testCase,
            BenchmarkStatistics statistics
    ) {
        return new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, statistics, "",
                statistics.completedRequests());
    }

    public static BenchmarkCaseResult unsupported(RedisBenchmarkCase testCase, String reason) {
        return new BenchmarkCaseResult(testCase, BenchmarkStatus.UNSUPPORTED, null, reason, 0);
    }

    public static BenchmarkCaseResult skipped(RedisBenchmarkCase testCase, String reason) {
        return new BenchmarkCaseResult(testCase, BenchmarkStatus.SKIPPED, null, reason, 0);
    }

    public static BenchmarkCaseResult failed(
            RedisBenchmarkCase testCase,
            long completedReplies,
            String reason
    ) {
        return new BenchmarkCaseResult(
                testCase,
                BenchmarkStatus.FAILED,
                null,
                reason,
                completedReplies
        );
    }
}
