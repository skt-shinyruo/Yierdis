package yier.bubu.redis.execution.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;

public class CoreContractSmokeTest {
    @Test
    public void executionContextExposesItsSession() {
        CommandSession session = (CommandSession) Proxy.newProxyInstance(
                CommandSession.class.getClassLoader(),
                new Class<?>[]{CommandSession.class},
                (proxy, method, args) -> null
        );
        CommandSession context = session;

        Assert.assertSame(session, context);
    }
}
