package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class YierdisInstanceBoundaryTest {
    @Test
    public void instanceMustNotExposeCommandProcessorFactoryMethods() {
        Method[] methods = YierdisInstance.class.getDeclaredMethods();
        boolean hasFactoryMethod = Arrays.stream(methods)
                .map(Method::getName)
                .anyMatch("newCommandProcessor"::equals);

        Assert.assertFalse(
                "YierdisInstance should own DB lifecycle/routing only, not command processor assembly",
                hasFactoryMethod
        );
    }

    @Test
    public void instanceShouldExposeObservabilitySeam() throws Exception {
        Class<?> observabilityType;
        try {
            observabilityType = Class.forName("yier.bubu.redis.runtime.YierdisInstanceObservability");
        } catch (ClassNotFoundException e) {
            Assert.fail("YierdisInstanceObservability should exist");
            return;
        }

        Method observabilityMethod;
        try {
            observabilityMethod = YierdisInstance.class.getDeclaredMethod("observability");
        } catch (NoSuchMethodException e) {
            Assert.fail("YierdisInstance should expose observability()");
            return;
        }

        Assert.assertEquals(observabilityType, observabilityMethod.getReturnType());
    }

    @Test
    public void observabilityShouldExposeDbSummariesApi() throws Exception {
        Class<?> observabilityType = Class.forName("yier.bubu.redis.runtime.YierdisInstanceObservability");
        Method summariesMethod = observabilityType.getDeclaredMethod("dbSummaries");
        Assert.assertEquals(List.class, summariesMethod.getReturnType());
    }
}
