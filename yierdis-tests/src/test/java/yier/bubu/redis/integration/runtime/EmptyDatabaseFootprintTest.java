package yier.bubu.redis.integration.runtime;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstance;

public class EmptyDatabaseFootprintTest {
    @Test
    public void sixteenEmptyDatabasesCommitNoObjectMetadata() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(16)
                .nativeSlotCapacity(262_144)
                .build())) {
            instance.runtimeAccess().bindToCurrentThread();
            for (int db = 0; db < 16; db++) {
                Assert.assertEquals(
                        0L,
                        instance.engines()[db].memoryStats().nativeMetadataCommittedBytes()
                );
            }
        }
    }
}
