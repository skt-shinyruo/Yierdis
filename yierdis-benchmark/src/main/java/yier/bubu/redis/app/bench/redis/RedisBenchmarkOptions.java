package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.app.bench.BenchArgv;

import java.util.Arrays;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class RedisBenchmarkOptions {
    /** 全部受支持的长选项名（供入口测试固定清单）。 */
    public static final Set<String> OPTION_NAMES = Set.of(
            "--host",
            "--port",
            "--requests",
            "--clients",
            "--data-size",
            "--pipeline",
            "--keyspace",
            "--keep-alive",
            "--tests",
            "--precision",
            "--seed",
            "--format",
            "--database"
    );

    String host = "127.0.0.1";
    int port = 16378;
    int requests = 100_000;
    int clients = 50;
    int dataSize = 3;
    int pipeline = 1;
    Long keyspace;
    boolean keepAlive = true;
    String tests;
    int precision = 3;
    Long seed;
    String format = "human";
    int database;

    /** 手写 argv 解析（无 picocli）：支持 "--name value" 与 "--name=value"，未知名称一律报错。 */
    public static RedisBenchmarkOptions parse(String... argv) {
        RedisBenchmarkOptions options = new RedisBenchmarkOptions();
        for (int i = 0; i < argv.length; i++) {
            String name = argv[i];
            String value = null;
            if (name.startsWith("--")) {
                int eq = name.indexOf('=');
                if (eq >= 0) {
                    value = name.substring(eq + 1);
                    name = name.substring(0, eq);
                }
            }
            if (!OPTION_NAMES.contains(name)) {
                throw BenchArgv.unknown(name, i);
            }
            if (name.equals("--keep-alive") && value == null) {
                // flag 形态等价于 --keep-alive=true。
                options.keepAlive = true;
                continue;
            }
            if (value == null) {
                if (++i >= argv.length) {
                    throw new IllegalArgumentException("Missing required parameter for option '" + name + "'");
                }
                value = argv[i];
            }
            assign(options, name, value);
        }
        return options;
    }

    private static void assign(RedisBenchmarkOptions options, String name, String raw) {
        switch (name) {
            case "--host" -> options.host = raw;
            case "--port" -> options.port = BenchArgv.intValue(name, raw);
            case "--requests" -> options.requests = BenchArgv.intValue(name, raw);
            case "--clients" -> options.clients = BenchArgv.intValue(name, raw);
            case "--data-size" -> options.dataSize = BenchArgv.intValue(name, raw);
            case "--pipeline" -> options.pipeline = BenchArgv.intValue(name, raw);
            case "--keyspace" -> options.keyspace = BenchArgv.longValue(name, raw);
            case "--keep-alive" -> options.keepAlive = BenchArgv.booleanValue(name, raw);
            case "--tests" -> options.tests = raw;
            case "--precision" -> options.precision = BenchArgv.intValue(name, raw);
            case "--seed" -> options.seed = BenchArgv.longValue(name, raw);
            case "--format" -> options.format = raw;
            case "--database" -> options.database = BenchArgv.intValue(name, raw);
            default -> throw new IllegalArgumentException("Unknown option: '" + name + "'");
        }
    }

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
