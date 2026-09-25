package yier.bubu.redis.app.bench.redis;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class RedisBenchmarkOptionsTest {
    @Test
    public void defaultsMatchOfficialWorkloadAndYierdisEndpoint() {
        RedisBenchmarkOptions options = RedisBenchmarkOptions.parse();

        BenchmarkConfig config = options.toConfig(() -> 123L);
        Assert.assertEquals("127.0.0.1", config.host());
        Assert.assertEquals(16378, config.port());
        Assert.assertEquals(100_000, config.requests());
        Assert.assertEquals(50, config.clients());
        Assert.assertEquals(3, config.dataSize());
        Assert.assertEquals(1, config.pipeline());
        Assert.assertTrue(config.keyspace().isEmpty());
        Assert.assertTrue(config.keepAlive());
        Assert.assertEquals(Set.of(), config.tests());
        Assert.assertEquals(3, config.precision());
        Assert.assertEquals(BenchmarkFormat.HUMAN, config.format());
        Assert.assertEquals(123L, config.seed());
        Assert.assertEquals(0, config.database());
    }

    @Test
    public void explicitZeroKeyspaceIsDifferentFromOmittedKeyspace() {
        RedisBenchmarkOptions options = RedisBenchmarkOptions.parse(
                "--keyspace", "0", "--tests", "SET,get", "--format", "csv"
        );

        BenchmarkConfig config = options.toConfig(() -> 999L);
        Assert.assertEquals(0L, config.keyspace().orElseThrow());
        Assert.assertEquals(Set.of("set", "get"), config.tests());
        Assert.assertEquals(BenchmarkFormat.CSV, config.format());
    }

    @Test
    public void everyExplicitOptionConvertsToConfigAndSuppliedSeedWins() {
        RedisBenchmarkOptions options = RedisBenchmarkOptions.parse(
                "--host", " benchmark.example ",
                "--port", "6380",
                "--requests", "321",
                "--clients", "12",
                "--data-size", "4096",
                "--pipeline", "8",
                "--keyspace", "2048",
                "--keep-alive=false",
                "--tests", " PING, set,GET,,ping ",
                "--precision", "4",
                "--seed", "42",
                "--format", "QuIeT",
                "--database", "2"
        );

        BenchmarkConfig config = options.toConfig(() -> {
            throw new AssertionError("seed supplier must not be used when --seed is present");
        });

        Assert.assertEquals("benchmark.example", config.host());
        Assert.assertEquals(6380, config.port());
        Assert.assertEquals(321, config.requests());
        Assert.assertEquals(12, config.clients());
        Assert.assertEquals(4096, config.dataSize());
        Assert.assertEquals(8, config.pipeline());
        Assert.assertEquals(2048L, config.keyspace().orElseThrow());
        Assert.assertFalse(config.keepAlive());
        Assert.assertEquals(Set.of("ping", "set", "get"), config.tests());
        Assert.assertEquals(4, config.precision());
        Assert.assertEquals(42L, config.seed());
        Assert.assertEquals(BenchmarkFormat.QUIET, config.format());
        Assert.assertEquals(2, config.database());
    }

    @Test
    public void exposesOnlyApprovedLongOptionNames() {
        Assert.assertEquals(Set.of(
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
        ), RedisBenchmarkOptions.OPTION_NAMES);
    }

    @Test
    public void formatParserIsCaseInsensitiveAndRejectsUnknownValues() {
        Assert.assertEquals(BenchmarkFormat.HUMAN, BenchmarkFormat.parse("HuMaN"));
        Assert.assertEquals(BenchmarkFormat.QUIET, BenchmarkFormat.parse("QUIET"));
        Assert.assertEquals(BenchmarkFormat.CSV, BenchmarkFormat.parse("csv"));
        Assert.assertThrows(IllegalArgumentException.class, () -> BenchmarkFormat.parse("json"));
        Assert.assertThrows(IllegalArgumentException.class, () -> BenchmarkFormat.parse(" human "));
        Assert.assertThrows(IllegalArgumentException.class, () -> BenchmarkFormat.parse(null));
    }

    @Test
    public void invalidConvertedValuesAreRejectedBeforeExecution() {
        RedisBenchmarkOptions invalidFormat = RedisBenchmarkOptions.parse("--format", "json");
        Assert.assertThrows(IllegalArgumentException.class, () -> invalidFormat.toConfig(() -> 1L));
    }
}
