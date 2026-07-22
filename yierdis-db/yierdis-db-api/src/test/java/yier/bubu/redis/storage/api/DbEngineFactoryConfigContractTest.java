package yier.bubu.redis.storage.api;

import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.Test;

public class DbEngineFactoryConfigContractTest {
    @Test
    public void factoryHasOneConfiguredCreateParameter() throws Exception {
        Method create = DbEngineFactory.class.getMethod("create", DbEngineConfig.class);

        Assert.assertEquals(RuntimeDbEngine.class, create.getReturnType());
        Assert.assertArrayEquals(new Class<?>[]{DbEngineConfig.class}, create.getParameterTypes());
    }

    @Test
    public void engineConfigCarriesDefragAndAdmissionValues() {
        DbDefragConfig defrag = new DbDefragConfig(true, 4096L, 7L, 3L);
        DbEngineConfig config = new DbEngineConfig(
                2,
                1_048_576L,
                MaxmemoryPolicy.ALLKEYS_LRU,
                9,
                5L,
                11L,
                defrag
        );

        Assert.assertEquals(2, config.dbIndex());
        Assert.assertEquals(1_048_576L, config.maxmemoryBytes());
        Assert.assertEquals(MaxmemoryPolicy.ALLKEYS_LRU, config.maxmemoryPolicy());
        Assert.assertEquals(9, config.maxmemorySamples());
        Assert.assertEquals(5L, config.evictionTimeLimitMillis());
        Assert.assertEquals(11L, config.expireCleanupTimeLimitMillis());
        Assert.assertSame(defrag, config.defrag());
    }
}
