package yier.bubu.redis.ops;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class DbEngineFactoryPolicyContractTest {
    @Test
    public void createReceivesDomainMaxmemoryPolicy() throws Exception {
        Method create = DbEngineFactory.class.getMethod(
                "create",
                int.class,
                long.class,
                MaxmemoryPolicy.class,
                int.class,
                long.class,
                long.class
        );

        Assert.assertEquals(RuntimeDbEngine.class, create.getReturnType());
        Assert.assertEquals(MaxmemoryPolicy.class, create.getParameterTypes()[2]);
    }
}
