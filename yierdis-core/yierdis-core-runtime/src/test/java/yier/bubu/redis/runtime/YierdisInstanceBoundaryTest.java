package yier.bubu.redis.runtime;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

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
}
