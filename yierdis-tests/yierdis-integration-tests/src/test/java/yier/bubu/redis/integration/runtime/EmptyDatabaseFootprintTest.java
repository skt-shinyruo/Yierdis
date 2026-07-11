package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

public class EmptyDatabaseFootprintTest {
    @Test
    public void sixteenEmptyDatabasesCommitNoObjectMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("empty-sixteen")) {
            YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                    runtime,
                    new NativeDefragOptions(0, 0, 0),
                    262_144
            );
            try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                    .databases(16)
                    .engineFactory(factory)
                    .build())) {
                instance.bindToCurrentThread();
                for (int db = 0; db < 16; db++) {
                    Assert.assertEquals(
                            0L,
                            instance.engine(db).memory().memoryStats().nativeMetadataCommittedBytes()
                    );
                }
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
