package yier.bubu.redis.app.bench.redis;

import java.util.List;
import java.util.Set;

/** 测试专用按 id 取目录用例；生产代码不再暴露该查找。 */
final class CaseSelection {
    private static final RedisBenchmarkCatalog CATALOG = new RedisBenchmarkCatalog();

    private CaseSelection() {
    }

    static RedisBenchmarkCase caseById(String id) {
        return allCases().stream()
                .filter(testCase -> testCase.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown case id: " + id));
    }

    static List<RedisBenchmarkCase> allCases() {
        return CATALOG.select(Set.of());
    }
}
