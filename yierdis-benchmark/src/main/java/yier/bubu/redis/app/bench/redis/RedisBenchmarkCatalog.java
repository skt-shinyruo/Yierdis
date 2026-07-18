package yier.bubu.redis.app.bench.redis;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static yier.bubu.redis.app.bench.redis.BenchmarkReplyExpectation.ARRAY;
import static yier.bubu.redis.app.bench.redis.BenchmarkReplyExpectation.BULK_OR_NULL;
import static yier.bubu.redis.app.bench.redis.BenchmarkReplyExpectation.INTEGER;
import static yier.bubu.redis.app.bench.redis.BenchmarkReplyExpectation.OK;
import static yier.bubu.redis.app.bench.redis.BenchmarkReplyExpectation.PONG;
import static yier.bubu.redis.app.bench.redis.RedisBenchmarkCommandTemplate.Argument.literal;
import static yier.bubu.redis.app.bench.redis.RedisBenchmarkCommandTemplate.Argument.payload;
import static yier.bubu.redis.app.bench.redis.RedisBenchmarkCommandTemplate.Argument.randomScore;

public final class RedisBenchmarkCatalog {
    private static final List<RedisBenchmarkCase> CASES = List.of(
            supported(
                    "ping_inline", "PING_INLINE", Set.of("ping_inline", "ping"),
                    RedisBenchmarkCommandTemplate.inline("PING\r\n"), Set.of("PING"), PONG, ""
            ),
            supported(
                    "ping_mbulk", "PING_MBULK", Set.of("ping_mbulk", "ping"),
                    resp(literal("PING")), Set.of("PING"), PONG, ""
            ),
            supported(
                    "set", "SET", Set.of("set"),
                    resp(literal("SET"), literal("key:__rand_int__"), payload()),
                    Set.of("SET"), OK, ""
            ),
            supported(
                    "get", "GET", Set.of("get"),
                    resp(literal("GET"), literal("key:__rand_int__")),
                    Set.of("GET"), BULK_OR_NULL, ""
            ),
            supported(
                    "incr", "INCR", Set.of("incr"),
                    resp(literal("INCR"), literal("counter:__rand_int__")),
                    Set.of("INCR"), INTEGER, ""
            ),
            supported(
                    "lpush", "LPUSH", Set.of("lpush"),
                    resp(literal("LPUSH"), literal("mylist"), payload()),
                    Set.of("LPUSH"), INTEGER, ""
            ),
            supported(
                    "rpush", "RPUSH", Set.of("rpush"),
                    resp(literal("RPUSH"), literal("mylist"), payload()),
                    Set.of("RPUSH"), INTEGER, ""
            ),
            supported(
                    "lpop", "LPOP", Set.of("lpop"),
                    resp(literal("LPOP"), literal("mylist")),
                    Set.of("LPOP"), BULK_OR_NULL, ""
            ),
            supported(
                    "rpop", "RPOP", Set.of("rpop"),
                    resp(literal("RPOP"), literal("mylist")),
                    Set.of("RPOP"), BULK_OR_NULL, ""
            ),
            supported(
                    "sadd", "SADD", Set.of("sadd"),
                    resp(literal("SADD"), literal("myset"), literal("element:__rand_int__")),
                    Set.of("SADD"), INTEGER, ""
            ),
            supported(
                    "hset", "HSET", Set.of("hset"),
                    resp(literal("HSET"), literal("myhash"), literal("element:__rand_int__"), payload()),
                    Set.of("HSET"), INTEGER, ""
            ),
            unsupported(
                    "spop", "SPOP", Set.of("spop"),
                    resp(literal("SPOP"), literal("myset")),
                    Set.of("SPOP"), BULK_OR_NULL, "Yierdis does not support SPOP", ""
            ),
            supported(
                    "zadd", "ZADD", Set.of("zadd"),
                    resp(literal("ZADD"), literal("myzset"), randomScore(),
                            literal("element:__rand_int__")),
                    Set.of("ZADD"), INTEGER, ""
            ),
            unsupported(
                    "zpopmin", "ZPOPMIN", Set.of("zpopmin"),
                    resp(literal("ZPOPMIN"), literal("myzset")),
                    Set.of("ZPOPMIN"), ARRAY, "Yierdis does not support ZPOPMIN", ""
            ),
            supported(
                    "lrange_setup", "LPUSH (needed to benchmark LRANGE)",
                    Set.of("lrange", "lrange_100", "lrange_300", "lrange_500", "lrange_600"),
                    resp(literal("LPUSH"), literal("mylist"), payload()),
                    Set.of("LPUSH"), INTEGER, ""
            ),
            supported(
                    "lrange_100", "LRANGE_100 (first 100 elements)", Set.of("lrange", "lrange_100"),
                    resp(literal("LRANGE"), literal("mylist"), literal("0"), literal("99")),
                    Set.of("LRANGE"), ARRAY, "lrange_setup"
            ),
            supported(
                    "lrange_300", "LRANGE_300 (first 300 elements)", Set.of("lrange", "lrange_300"),
                    resp(literal("LRANGE"), literal("mylist"), literal("0"), literal("299")),
                    Set.of("LRANGE"), ARRAY, "lrange_setup"
            ),
            supported(
                    "lrange_500", "LRANGE_500 (first 500 elements)", Set.of("lrange", "lrange_500"),
                    resp(literal("LRANGE"), literal("mylist"), literal("0"), literal("499")),
                    Set.of("LRANGE"), ARRAY, "lrange_setup"
            ),
            supported(
                    "lrange_600", "LRANGE_600 (first 600 elements)", Set.of("lrange", "lrange_600"),
                    resp(literal("LRANGE"), literal("mylist"), literal("0"), literal("599")),
                    Set.of("LRANGE"), ARRAY, "lrange_setup"
            ),
            unsupported(
                    "mset", "MSET (10 keys)", Set.of("mset"),
                    resp(
                            literal("MSET"),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload(),
                            literal("key:__rand_int__"), payload()
                    ),
                    Set.of("MSET"), OK, "Yierdis does not support MSET", ""
            ),
            unsupported(
                    "xadd", "XADD", Set.of("xadd"),
                    resp(literal("XADD"), literal("mystream"), literal("*"), literal("myfield"), payload()),
                    Set.of("XADD"), BULK_OR_NULL, "Yierdis does not support XADD", ""
            )
    );

    private static final Map<String, RedisBenchmarkCase> CASES_BY_ID = CASES.stream()
            .collect(Collectors.toUnmodifiableMap(RedisBenchmarkCase::id, Function.identity()));

    private static final Set<String> SELECTION_TRIGGERS = CASES.stream()
            .flatMap(testCase -> testCase.selectionTriggers().stream())
            .collect(Collectors.toUnmodifiableSet());

    public List<RedisBenchmarkCase> allCases() {
        return CASES;
    }

    public RedisBenchmarkCase caseById(String id) {
        String normalizedId = normalize(id, "case id");
        RedisBenchmarkCase testCase = CASES_BY_ID.get(normalizedId);
        if (testCase == null) {
            throw new IllegalArgumentException("unknown benchmark case id: " + normalizedId);
        }
        return testCase;
    }

    public List<RedisBenchmarkCase> select(Set<String> selectors) {
        if (selectors == null) {
            throw new IllegalArgumentException("selectors must not be null");
        }
        Set<String> normalizedSelectors = selectors.stream()
                .map(selector -> normalize(selector, "selector"))
                .collect(Collectors.toUnmodifiableSet());
        if (normalizedSelectors.isEmpty()) {
            return CASES;
        }

        Set<String> unknownSelectors = new TreeSet<>(normalizedSelectors);
        unknownSelectors.removeAll(SELECTION_TRIGGERS);
        if (!unknownSelectors.isEmpty()) {
            throw new SelectionException("unknown benchmark selector(s): "
                    + String.join(", ", unknownSelectors));
        }

        return CASES.stream()
                .filter(testCase -> !Collections.disjoint(
                        testCase.selectionTriggers(),
                        normalizedSelectors
                ))
                .toList();
    }

    private static RedisBenchmarkCase supported(
            String id,
            String title,
            Set<String> selectionTriggers,
            RedisBenchmarkCommandTemplate template,
            Set<String> requiredCommands,
            BenchmarkReplyExpectation replyExpectation,
            String dependencyId
    ) {
        return new RedisBenchmarkCase(
                id,
                title,
                selectionTriggers,
                template,
                requiredCommands,
                replyExpectation,
                RedisBenchmarkCase.Support.available(),
                dependencyId
        );
    }

    private static RedisBenchmarkCase unsupported(
            String id,
            String title,
            Set<String> selectionTriggers,
            RedisBenchmarkCommandTemplate template,
            Set<String> requiredCommands,
            BenchmarkReplyExpectation replyExpectation,
            String reason,
            String dependencyId
    ) {
        return new RedisBenchmarkCase(
                id,
                title,
                selectionTriggers,
                template,
                requiredCommands,
                replyExpectation,
                RedisBenchmarkCase.Support.unsupported(reason),
                dependencyId
        );
    }

    private static RedisBenchmarkCommandTemplate resp(RedisBenchmarkCommandTemplate.Argument... arguments) {
        return RedisBenchmarkCommandTemplate.resp(arguments);
    }

    private static String normalize(String value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    static final class SelectionException extends IllegalArgumentException {
        SelectionException(String message) {
            super(message);
        }
    }
}
