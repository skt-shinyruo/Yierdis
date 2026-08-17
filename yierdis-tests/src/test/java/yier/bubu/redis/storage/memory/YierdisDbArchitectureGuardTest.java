package yier.bubu.redis.storage.memory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;

public class YierdisDbArchitectureGuardTest {
    @Test
    public void databaseUsesOnePublicFactoryShape() {
        Assert.assertEquals(0, YierdisDb.class.getConstructors().length);
        Assert.assertFalse(java.util.Arrays.stream(YierdisDb.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().startsWith("createWith")));

        Constructor<?>[] constructors = YierdisDbEngineFactory.class.getConstructors();
        Assert.assertEquals(1, constructors.length);
        Assert.assertArrayEquals(
                new Class<?>[]{StableMemoryBackendFactory.class, YierdisDbBackendConfig.class},
                constructors[0].getParameterTypes()
        );
    }

    @Test
    public void databaseImplementationTypesRemainPackagePrivate() {
        for (Class<?> implementation : List.of(
                YierdisDbKernel.class,
                YierdisDbMemoryContext.class,
                YierdisDbKeyLifecycle.class,
                PreparedEntryMutation.class,
                YierdisStringOps.class,
                YierdisListOps.class,
                YierdisHashOps.class,
                YierdisSetOps.class,
                YierdisZSetOps.class,
                YierdisHllOps.class,
                YierdisKeyspaceOps.class,
                YierdisTtlOps.class,
                YierdisDbExpirationSupport.class,
                YierdisDbWrites.class,
                YierdisDbMemoryReporter.class,
                YierdisDbMemoryEstimator.class,
                YierdisDbMaxmemorySupport.class
        )) {
            Assert.assertFalse(
                    implementation.getSimpleName() + " must remain internal",
                    Modifier.isPublic(implementation.getModifiers())
            );
        }
    }
}
