package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

import java.util.Objects;

public record StorageBenchmarkConfig(
        int keys,
        int keySizeBytes,
        int valueSizeBytes,
        int warmupOperations,
        int precision,
        BenchmarkFormat format
) {
    public static final int MAX_KEYS = 10_000_000;
    public static final int MAX_KEY_SIZE_BYTES = 1_024;
    public static final int MAX_VALUE_SIZE_BYTES = 1024 * 1024;
    public static final int MAX_WARMUP_OPERATIONS = 1_000_000;

    public StorageBenchmarkConfig {
        if (keys <= 0 || keys > MAX_KEYS) {
            throw new IllegalArgumentException("keys must be in range 1.." + MAX_KEYS);
        }
        int minimumKeySize = minimumKeySize(keys);
        if (keySizeBytes < minimumKeySize || keySizeBytes > MAX_KEY_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    "key-size must be in range " + minimumKeySize + ".." + MAX_KEY_SIZE_BYTES
            );
        }
        if (valueSizeBytes < 0 || valueSizeBytes > MAX_VALUE_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    "value-size must be in range 0.." + MAX_VALUE_SIZE_BYTES
            );
        }
        if (warmupOperations < 0 || warmupOperations > MAX_WARMUP_OPERATIONS) {
            throw new IllegalArgumentException(
                    "warmup-operations must be in range 0.." + MAX_WARMUP_OPERATIONS
            );
        }
        if (precision < 0 || precision > 4) {
            throw new IllegalArgumentException("precision must be in range 0..4");
        }
        format = Objects.requireNonNull(format, "format");
    }

    static int minimumKeySize(int keys) {
        int highestIndex = Math.max(0, keys - 1);
        int digits = 1;
        while (highestIndex >= 10) {
            highestIndex /= 10;
            digits++;
        }
        return digits + 1;
    }
}
