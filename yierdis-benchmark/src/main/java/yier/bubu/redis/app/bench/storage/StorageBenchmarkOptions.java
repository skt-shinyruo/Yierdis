package yier.bubu.redis.app.bench.storage;

import picocli.CommandLine.Option;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

public final class StorageBenchmarkOptions {
    @Option(names = "--keys", defaultValue = "1000000", description = "Unique SET keys (maximum 10000000).")
    int keys = 1_000_000;

    @Option(names = "--key-size", defaultValue = "16", description = "Fixed key size in bytes.")
    int keySizeBytes = 16;

    @Option(names = "--value-size", defaultValue = "16", description = "Fixed value size in bytes.")
    int valueSizeBytes = 16;

    @Option(names = "--warmup-operations", defaultValue = "50000", description = "SET operations in a disposable warmup database.")
    int warmupOperations = 50_000;

    @Option(names = "--precision", defaultValue = "3", description = "HdrHistogram significant digits.")
    int precision = 3;

    @Option(names = "--format", defaultValue = "human", description = "Output format: human, quiet, or csv.")
    String format = "human";

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
