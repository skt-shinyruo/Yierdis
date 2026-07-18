package yier.bubu.redis.app.bench.redis;

import java.util.Objects;

public record BenchmarkCaseResult(
        RedisBenchmarkCase testCase,
        BenchmarkStatus status,
        BenchmarkStatistics statistics,
        String reason,
        long completedReplies
) {
    public BenchmarkCaseResult {
        Objects.requireNonNull(testCase, "testCase");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(reason, "reason");

        switch (status) {
            case SUCCESS -> {
                if (statistics == null) {
                    throw new IllegalArgumentException("successful results require statistics");
                }
                if (!reason.isEmpty()) {
                    throw new IllegalArgumentException("successful results must not have a reason");
                }
                if (completedReplies != statistics.completedRequests()) {
                    throw new IllegalArgumentException(
                            "completedReplies must equal statistics.completedRequests for success"
                    );
                }
            }
            case UNSUPPORTED, SKIPPED -> {
                if (statistics != null) {
                    throw new IllegalArgumentException("non-successful results must not have statistics");
                }
                if (reason.isBlank()) {
                    throw new IllegalArgumentException("non-successful results require a reason");
                }
                if (completedReplies != 0) {
                    throw new IllegalArgumentException(
                            status + " results must have zero completed replies"
                    );
                }
            }
            case FAILED -> {
                if (statistics != null) {
                    throw new IllegalArgumentException("non-successful results must not have statistics");
                }
                if (reason.isBlank()) {
                    throw new IllegalArgumentException("non-successful results require a reason");
                }
                if (completedReplies < 0) {
                    throw new IllegalArgumentException("completedReplies must be >= 0");
                }
            }
        }
    }

    public static BenchmarkCaseResult success(
            RedisBenchmarkCase testCase,
            BenchmarkStatistics statistics
    ) {
        return new BenchmarkCaseResult(testCase, BenchmarkStatus.SUCCESS, statistics, "",
                statistics == null ? 0 : statistics.completedRequests());
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
