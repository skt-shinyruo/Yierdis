package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;
import picocli.CommandLine;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

import java.nio.charset.StandardCharsets;

public class StorageBenchmarkConfigTest {
    @Test
    public void defaultsDescribeTheMillionKeyAcceptanceWorkload() {
        StorageBenchmarkOptions options = new StorageBenchmarkOptions();
        new CommandLine(options).parseArgs();

        StorageBenchmarkConfig config = options.toConfig();

        Assert.assertEquals(1_000_000, config.keys());
        Assert.assertEquals(16, config.keySizeBytes());
        Assert.assertEquals(16, config.valueSizeBytes());
        Assert.assertEquals(50_000, config.warmupOperations());
        Assert.assertEquals(3, config.precision());
        Assert.assertEquals(BenchmarkFormat.HUMAN, config.format());
    }

    @Test
    public void acceptsTenMillionKeysAndExplicitOutputSettings() {
        StorageBenchmarkOptions options = new StorageBenchmarkOptions();
        new CommandLine(options).parseArgs(
                "--keys", "10000000",
                "--key-size", "8",
                "--value-size", "0",
                "--warmup-operations", "0",
                "--precision", "4",
                "--format", "csv"
        );

        StorageBenchmarkConfig config = options.toConfig();

        Assert.assertEquals(StorageBenchmarkConfig.MAX_KEYS, config.keys());
        Assert.assertEquals(8, config.keySizeBytes());
        Assert.assertEquals(0, config.valueSizeBytes());
        Assert.assertEquals(0, config.warmupOperations());
        Assert.assertEquals(BenchmarkFormat.CSV, config.format());
    }

    @Test
    public void rejectsBoundsAndKeysThatCannotEncodeTheRequestedCardinality() {
        Assert.assertThrows(IllegalArgumentException.class, () -> config(0, 16, 16, 0, 3));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                config(StorageBenchmarkConfig.MAX_KEYS + 1, 16, 16, 0, 3));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(10_000_000, 7, 16, 0, 3));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 2, -1, 0, 3));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 2, 1, -1, 3));
        Assert.assertThrows(IllegalArgumentException.class, () -> config(1, 2, 1, 0, 5));
    }

    @Test
    public void fixedWidthKeyEncodingIsDeterministicAndAllocationFreeForTheLoop() {
        byte[] key = StorageBenchmarkRunner.keyBuffer(8);

        StorageBenchmarkRunner.encodeKeyIndex(key, 0);
        Assert.assertEquals("k0000000", new String(key, StandardCharsets.US_ASCII));
        StorageBenchmarkRunner.encodeKeyIndex(key, 9);
        Assert.assertEquals("k0000009", new String(key, StandardCharsets.US_ASCII));
        StorageBenchmarkRunner.encodeKeyIndex(key, 10);
        Assert.assertEquals("k0000010", new String(key, StandardCharsets.US_ASCII));
        StorageBenchmarkRunner.encodeKeyIndex(key, 9_999_999);
        Assert.assertEquals("k9999999", new String(key, StandardCharsets.US_ASCII));
        StorageBenchmarkRunner.encodeKeyIndex(key, 7);
        Assert.assertEquals("k0000007", new String(key, StandardCharsets.US_ASCII));
    }

    private static StorageBenchmarkConfig config(
            int keys,
            int keySize,
            int valueSize,
            int warmup,
            int precision
    ) {
        return new StorageBenchmarkConfig(
                keys,
                keySize,
                valueSize,
                warmup,
                precision,
                BenchmarkFormat.HUMAN
        );
    }
}
