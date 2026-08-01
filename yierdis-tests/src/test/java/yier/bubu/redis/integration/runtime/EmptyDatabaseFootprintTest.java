package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.memory.YierdisDbBackendConfig;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

import java.util.ArrayList;
import java.util.List;

public class EmptyDatabaseFootprintTest {
    @Test
    public void sixteenEmptyDatabasesCommitNoObjectMetadata() {
        List<YierdisFfmStableMemoryBackend> createdBackends = new ArrayList<>();
        StableMemoryBackendFactory backendFactory = (name, maxSlots, owner) -> {
            YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
            createdBackends.add(backend);
            return backend;
        };
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                backendFactory,
                new YierdisDbBackendConfig(262_144)
        );
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(16)
                .engineFactory(factory)
                .build())) {
            instance.bindToCurrentThread();
            Assert.assertEquals(16, createdBackends.size());
            for (int db = 0; db < 16; db++) {
                Assert.assertEquals(
                        0L,
                        instance.engine(db).memory().memoryStats().nativeMetadataCommittedBytes()
                );
            }
            for (YierdisFfmStableMemoryBackend backend : createdBackends) {
                Assert.assertEquals(0L, backend.memoryUsage().nativeMetadataCommittedBytes());
                Assert.assertEquals(0L, backend.memoryUsage().nativeDataLiveBytes());
                Assert.assertEquals(0L, backend.liveRegionCount());
            }
        }
    }
}
