package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

import java.util.OptionalLong;

public class StorageBenchmarkRendererTest {
    @Test
    public void humanOutputNamesThroughputLatencyAndEveryFootprintCategory() {
        String rendered = new StorageBenchmarkRenderer().render(
                config(BenchmarkFormat.HUMAN),
                result(OptionalLong.of(9_000L), OptionalLong.of(8_000L))
        );

        Assert.assertTrue(rendered.contains("throughput: 10.00 ops/s"));
        Assert.assertTrue(rendered.contains("latency p50: 80 ns"));
        Assert.assertTrue(rendered.contains("latency p99: 200 ns"));
        Assert.assertTrue(rendered.contains("heap estimated: 1100 bytes"));
        Assert.assertTrue(rendered.contains("native metadata committed: 2200 bytes"));
        Assert.assertTrue(rendered.contains("native data committed: 3300 bytes"));
        Assert.assertTrue(rendered.contains("accounted delta per key: 600.000 bytes/key"));
        Assert.assertTrue(rendered.contains("live native objects: 30"));
        Assert.assertTrue(rendered.contains("pending hash tables: 0"));
        Assert.assertTrue(rendered.contains("process RSS: 9000 bytes"));
        Assert.assertTrue(rendered.contains("process RSS delta: 8000 bytes"));
    }

    @Test
    public void csvOutputHasStableColumnsAndLeavesUnavailableRssBlank() {
        String rendered = new StorageBenchmarkRenderer().render(
                config(BenchmarkFormat.CSV),
                result(OptionalLong.empty(), OptionalLong.empty())
        );
        String[] lines = rendered.split("\\R");

        Assert.assertTrue(lines[0].contains("\"native_metadata_committed_bytes\""));
        Assert.assertTrue(lines[0].contains("\"accounted_delta_bytes_per_key\""));
        Assert.assertTrue(lines[0].contains("\"live_object_count\""));
        Assert.assertTrue(lines[0].contains("\"rss_bytes\""));
        Assert.assertTrue(lines[0].contains("\"pending_hash_table_count\""));
        Assert.assertEquals(21, lines[0].split(",", -1).length);
        Assert.assertEquals(21, lines[1].split(",", -1).length);
        Assert.assertTrue(lines[1].endsWith(",,"));
    }

    private static StorageBenchmarkConfig config(BenchmarkFormat format) {
        return new StorageBenchmarkConfig(10, 4, 8, 3, 3, format);
    }

    private static StorageBenchmarkResult result(OptionalLong rss, OptionalLong rssDelta) {
        StorageMemorySnapshot baseline = new StorageMemorySnapshot(
                100L, 200L, 300L, 250L, 50L, 600L, 0L, 0, 0, OptionalLong.of(1_000L)
        );
        if (rss.isEmpty()) {
            baseline = new StorageMemorySnapshot(
                    100L, 200L, 300L, 250L, 50L, 600L, 0L, 0, 0, OptionalLong.empty()
            );
        }
        StorageMemorySnapshot loaded = new StorageMemorySnapshot(
                1_100L, 2_200L, 3_300L, 3_000L, 300L, 6_600L, 30L, 0, 10, rss
        );
        return new StorageBenchmarkResult(
                10,
                1_000_000_000L,
                10.0,
                new StorageLatencyRecorder.Summary(10L, 100.0, 80L, 200L, 250L),
                baseline,
                loaded,
                6_000L,
                600.0,
                rssDelta
        );
    }
}
