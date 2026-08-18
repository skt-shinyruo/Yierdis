package yier.bubu.redis.runtime.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

public class YierdisInstanceConfigTest {
    @Test
    public void builderNormalizesDefaultsAndValidatesLimits() {
        YierdisInstanceConfig config = YierdisInstanceConfig.builder()
                .databases(-8)
                .maxmemoryScope(null)
                .maxmemoryPolicy(null)
                .build();

        Assert.assertEquals(1, config.databases());
        Assert.assertEquals(0, config.nativeSlotCapacity());
        Assert.assertEquals(YierdisInstanceConfig.MaxmemoryScope.PER_DB, config.maxmemoryScope());
        Assert.assertEquals(MaxmemoryPolicy.NOEVICTION, config.maxmemoryPolicy());
        Assert.assertEquals(5, config.maxmemorySamples());
        Assert.assertEquals(5L, config.evictionTimeLimitMillis());
        Assert.assertEquals(5L, config.expireCleanupTimeLimitMillis());
        Assert.assertEquals(new DbDefragConfig(false, 64L * 1024L, 64L, 1L), config.defrag());

        YierdisInstanceConfig configured = YierdisInstanceConfig.builder()
                .nativeSlotCapacity(7)
                .defrag(new DbDefragConfig(true, 1024L, 7L, 3L))
                .build();
        Assert.assertEquals(7, configured.nativeSlotCapacity());
        Assert.assertEquals(new DbDefragConfig(true, 1024L, 7L, 3L), configured.defrag());

        expectIllegalArgument("maxmemoryBytes must be >= 0",
                () -> YierdisInstanceConfig.builder().maxmemoryBytes(-1).build());
        expectIllegalArgument("nativeSlotCapacity must be >= 0",
                () -> YierdisInstanceConfig.builder().nativeSlotCapacity(-1).build());
        expectIllegalArgument("maxmemorySamples must be > 0",
                () -> YierdisInstanceConfig.builder().maxmemorySamples(0).build());
        expectIllegalArgument("evictionTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().evictionTimeLimitMillis(0).build());
        expectIllegalArgument("expireCleanupTimeLimitMillis must be > 0",
                () -> YierdisInstanceConfig.builder().expireCleanupTimeLimitMillis(0).build());
        Assert.assertThrows(NullPointerException.class, () -> YierdisInstanceConfig.builder().defrag(null));
    }

    private static void expectIllegalArgument(String message, Runnable action) {
        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class, action::run);
        Assert.assertEquals(message, failure.getMessage());
    }
}
