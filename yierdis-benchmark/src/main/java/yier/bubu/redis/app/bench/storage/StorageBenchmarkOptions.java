package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.app.bench.BenchArgv;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

import java.util.Set;

public final class StorageBenchmarkOptions {
    /** 全部受支持的长选项名（供入口测试固定清单）。 */
    public static final Set<String> OPTION_NAMES = Set.of(
            "--keys",
            "--key-size",
            "--value-size",
            "--warmup-operations",
            "--precision",
            "--format"
    );

    int keys = 1_000_000;
    int keySizeBytes = 16;
    int valueSizeBytes = 16;
    int warmupOperations = 50_000;
    int precision = 3;
    String format = "human";

    /** 手写 argv 解析（无 picocli）：支持 "--name value" 与 "--name=value"，未知名称一律报错。 */
    public static StorageBenchmarkOptions parse(String... argv) {
        StorageBenchmarkOptions options = new StorageBenchmarkOptions();
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
            if (value == null) {
                if (++i >= argv.length) {
                    throw new IllegalArgumentException("Missing required parameter for option '" + name + "'");
                }
                value = argv[i];
            }
            switch (name) {
                case "--keys" -> options.keys = BenchArgv.intValue(name, value);
                case "--key-size" -> options.keySizeBytes = BenchArgv.intValue(name, value);
                case "--value-size" -> options.valueSizeBytes = BenchArgv.intValue(name, value);
                case "--warmup-operations" -> options.warmupOperations = BenchArgv.intValue(name, value);
                case "--precision" -> options.precision = BenchArgv.intValue(name, value);
                case "--format" -> options.format = value;
                default -> throw new IllegalArgumentException("Unknown option: '" + name + "'");
            }
        }
        return options;
    }

    StorageBenchmarkConfig toConfig() {
        return new StorageBenchmarkConfig(
                keys,
                keySizeBytes,
                valueSizeBytes,
                warmupOperations,
                precision,
                BenchmarkFormat.parse(format)
        );
    }
}
