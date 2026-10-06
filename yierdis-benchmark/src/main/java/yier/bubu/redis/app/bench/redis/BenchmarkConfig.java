package yier.bubu.redis.app.bench.redis;

import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public record BenchmarkConfig(
        String host,
        int port,
        int requests,
        int clients,
        int dataSize,
        int pipeline,
        OptionalLong keyspace,
        boolean keepAlive,
        Set<String> tests,
        int precision,
        long seed,
        BenchmarkFormat format,
        int database
) {
    public BenchmarkConfig {
        host = Objects.requireNonNull(host, "host").trim();
        keyspace = Objects.requireNonNull(keyspace, "keyspace");
        format = Objects.requireNonNull(format, "format");
        tests = tests == null ? Set.of() : tests.stream()
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        if (host.isEmpty()) throw new IllegalArgumentException("host must not be blank");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port must be in range 1..65535");
        if (requests <= 0) throw new IllegalArgumentException("requests must be > 0");
        if (clients <= 0) throw new IllegalArgumentException("clients must be > 0");
        if (dataSize < 1 || dataSize > 1024 * 1024 * 1024) throw new IllegalArgumentException("dataSize out of range");
        if (pipeline <= 0) throw new IllegalArgumentException("pipeline must be > 0");
        if (keyspace.isPresent()) {
            long value = keyspace.getAsLong();
            if (value < 0) {
                throw new IllegalArgumentException("keyspace must be >= 0");
            }
            // 0 合法：PreparedPipeline 把 bound 0 写成固定的 000000000000，不是随机偏移。
            // 10^12 已经是 13 位，keyspace 本身必须落在 12 位十进制里。
            if (value >= BenchmarkRandom.TWELVE_DIGIT_LIMIT) {
                throw new IllegalArgumentException("keyspace values must fit in 12 digits");
            }
        }
        if (precision < 0 || precision > 4) throw new IllegalArgumentException("precision must be in range 0..4");
        if (database < 0) throw new IllegalArgumentException("database must be >= 0");
    }
}
