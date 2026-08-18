package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.bench.redis.BenchmarkFormat;

public class StorageBenchmarkRunnerTest {
    @Test
    public void smallRealDatabaseRunReportsConsistentStructureAndAccounting() {
        StorageBenchmarkConfig config = new StorageBenchmarkConfig(
                128,
                8,
                8,
                16,
                2,
                BenchmarkFormat.HUMAN
        );

        StorageBenchmarkResult result = new StorageBenchmarkRunner().run(config);

        Assert.assertEquals(128, result.completedOperations());
        Assert.assertEquals(128L, result.latency().count());
        Assert.assertTrue(result.elapsedNanos() > 0L);
        Assert.assertTrue(result.operationsPerSecond() > 0.0);
        Assert.assertTrue(result.latency().p99Nanos() >= result.latency().p50Nanos());
        Assert.assertEquals(0, result.baseline().keyCount());
        Assert.assertEquals(128, result.loaded().keyCount());
        Assert.assertTrue(result.loaded().heapEstimatedBytes() >= result.baseline().heapEstimatedBytes());
        Assert.assertTrue(result.loaded().nativeMetadataCommittedBytes()
                > result.baseline().nativeMetadataCommittedBytes());
        Assert.assertTrue(result.loaded().nativeDataCommittedBytes()
                > result.baseline().nativeDataCommittedBytes());
        Assert.assertTrue(result.loaded().nativeDataLiveBytes() > 0L);
        Assert.assertTrue(result.loaded().liveObjectCount() >= result.completedOperations());
        Assert.assertEquals(0, result.loaded().pendingHashTableCount());
        Assert.assertTrue(result.accountedDeltaBytes() > 0L);
        Assert.assertTrue(result.accountedDeltaBytesPerKey() > 0.0);
    }
}
