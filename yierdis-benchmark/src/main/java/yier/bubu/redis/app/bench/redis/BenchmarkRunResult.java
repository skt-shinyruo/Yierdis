package yier.bubu.redis.app.bench.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class BenchmarkRunResult {
    private final List<BenchmarkCaseResult> cases;
    private final Map<String, BenchmarkCaseResult> casesById;

    public BenchmarkRunResult(List<BenchmarkCaseResult> cases) {
        Objects.requireNonNull(cases, "cases");
        List<BenchmarkCaseResult> ordered = new ArrayList<>(cases.size());
        Map<String, BenchmarkCaseResult> indexed = new LinkedHashMap<>();
        for (BenchmarkCaseResult result : cases) {
            Objects.requireNonNull(result, "cases element");
            String id = result.testCase().id();
            if (indexed.putIfAbsent(id, result) != null) {
                throw new IllegalArgumentException("duplicate benchmark case id: " + id);
            }
            ordered.add(result);
        }
        this.cases = List.copyOf(ordered);
        this.casesById = Collections.unmodifiableMap(new LinkedHashMap<>(indexed));
    }

    public List<BenchmarkCaseResult> cases() {
        return cases;
    }

    public BenchmarkCaseResult caseById(String id) {
        if (id == null) {
            throw new IllegalArgumentException("case id must not be null");
        }
        String normalized = id.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("case id must not be blank");
        }
        BenchmarkCaseResult result = casesById.get(normalized);
        if (result == null) {
            throw new IllegalArgumentException("unknown benchmark case id: " + normalized);
        }
        return result;
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
