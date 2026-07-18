package yier.bubu.redis.app.bench.redis;

import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class RedisBenchmarkOptions {
    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Target host to connect.")
    String host = "127.0.0.1";

    @Option(names = "--port", defaultValue = "16378", description = "Target port to connect.")
    int port = 16378;

    @Option(names = "--requests", defaultValue = "100000", description = "Measured requests per benchmark case.")
    int requests = 100_000;

    @Option(names = "--clients", defaultValue = "50", description = "Concurrent benchmark clients.")
    int clients = 50;

    @Option(names = "--data-size", defaultValue = "3", description = "Payload size in bytes.")
    int dataSize = 3;

    @Option(names = "--pipeline", defaultValue = "1", description = "Requests sent per pipeline.")
    int pipeline = 1;

    @Option(names = "--keyspace", description = "Optional random keyspace size.")
    Long keyspace;

    @Option(names = "--keep-alive", defaultValue = "true", description = "Reuse connections between pipelines.")
    boolean keepAlive = true;

    @Option(names = "--tests", description = "Comma-separated official benchmark test names.")
    String tests;

    @Option(names = "--precision", defaultValue = "3", description = "HdrHistogram significant digits.")
    int precision = 3;

    @Option(names = "--seed", description = "Optional deterministic random seed.")
    Long seed;

    @Option(names = "--format", defaultValue = "human", description = "Output format: human, quiet, or csv.")
    String format = "human";

    @Option(names = "--username", description = "Optional ACL username.")
    String username = "";

    @Option(names = "--password", description = "Optional authentication password.")
    String password = "";

    @Option(names = "--database", defaultValue = "0", description = "Logical database to select.")
    int database;

    BenchmarkConfig toConfig(LongSupplier seedSupplier) {
        long resolvedSeed = seed == null ? requireSeed(seedSupplier) : seed;
        return new BenchmarkConfig(
                host,
                port,
                requests,
                clients,
                dataSize,
                pipeline,
                keyspace == null ? OptionalLong.empty() : OptionalLong.of(keyspace),
                keepAlive,
                normalizeTests(tests),
                precision,
                resolvedSeed,
                BenchmarkFormat.parse(format),
                username,
                password,
                database
        );
    }

    private static long requireSeed(LongSupplier seedSupplier) {
        if (seedSupplier == null) {
            throw new NullPointerException("seedSupplier");
        }
        return seedSupplier.getAsLong();
    }

    private static Set<String> normalizeTests(String tests) {
        if (tests == null || tests.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(tests.split(",", -1))
                .map(value -> value.trim().toLowerCase(Locale.ROOT))
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }
}
