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
        String username,
        String password,
        int database
) {
    public BenchmarkConfig {
        host = Objects.requireNonNull(host, "host").trim();
        keyspace = Objects.requireNonNull(keyspace, "keyspace");
        format = Objects.requireNonNull(format, "format");
        username = username == null ? "" : username;
        password = password == null ? "" : password;
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
        if (keyspace.isPresent() && keyspace.getAsLong() < 0) throw new IllegalArgumentException("keyspace must be >= 0");
        if (precision < 0 || precision > 4) throw new IllegalArgumentException("precision must be in range 0..4");
        if (database < 0) throw new IllegalArgumentException("database must be >= 0");
        if (!username.isEmpty() && password.isEmpty()) throw new IllegalArgumentException("username requires password");
    }
}
