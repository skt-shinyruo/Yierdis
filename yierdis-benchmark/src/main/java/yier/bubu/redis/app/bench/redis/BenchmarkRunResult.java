package yier.bubu.redis.app.bench.redis;

import java.util.List;

public final class BenchmarkRunResult {
    private final List<BenchmarkCaseResult> cases;

    public BenchmarkRunResult(List<BenchmarkCaseResult> cases) {
        this.cases = List.copyOf(cases);
    }

    public List<BenchmarkCaseResult> cases() {
        return cases;
    }

    public int exitCode() {
        for (BenchmarkCaseResult result : cases) {
            if (result.status() == BenchmarkStatus.FAILED) {
                return 1;
            }
        }
        return 0;
    }
}
