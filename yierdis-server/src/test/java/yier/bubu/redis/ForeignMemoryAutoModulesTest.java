package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;

public class ForeignMemoryAutoModulesTest {
    @Test
    public void doesNotRelaunchWhenForeignApiIsAvailable() {
        YierdisServerRuntimeConfig config = new YierdisServerRuntimeConfig(
                0,
                1,
                1000,
                1,
                1024,
                0,
                YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.FAIR,
                256,
                128,
                0,
                0,
                128,
                10,
                128,
                4096,
                1024,
                16,
                4096,
                YierdisServerRuntimeConfig.OffheapBackend.FOREIGN,
                0,
                false,
                0,
                YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                YierdisServerRuntimeConfig.MaxmemoryPolicy.NOEVICTION,
                5,
                5,
                5,
                0,
                0
        );

        Assert.assertNull(ForeignMemoryAutoModules.maybeRelaunchIfNeeded(config, new String[0]));
    }
}
