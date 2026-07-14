package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

public class YierdisDbComponentFactoryStructureTest {
    @Test
    public void ownerCallbacksDoesNotExposeConcreteDb() {
        for (Method method : YierdisDbComponentFactory.OwnerCallbacks.class.getDeclaredMethods()) {
            Assert.assertNotEquals("OwnerCallbacks must not expose the full YierdisDb owner", "db", method.getName());
            Assert.assertNotSame(
                    "OwnerCallbacks method must not return the full YierdisDb owner: " + method.getName(),
                    YierdisDb.class,
                    method.getReturnType()
            );
        }
    }

    @Test
    public void yierdisDbConstructorDoesNotReturnFullOwnerFromCallbacks() throws IOException {
        String source = Files.readString(Path.of("src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java"));

        Assert.assertFalse(
                "YierdisDb constructor callbacks must not return YierdisDb.this",
                source.contains("return YierdisDb.this;")
        );
        Assert.assertFalse(
                "YierdisDb constructor callbacks must not route checkThread through partially initialized owner",
                source.contains("YierdisDb.this.checkThread()")
        );
    }

    @Test
    public void ownerCallbacksOnlyExposeThreadGuard() {
        Set<String> callbackNames = java.util.Arrays.stream(YierdisDbComponentFactory.OwnerCallbacks.class.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());

        Assert.assertEquals(
                "OwnerCallbacks should only expose the thread guard; runtime state owns DB identity and maintenance",
                Set.of("checkThread"),
                callbackNames
        );
    }

    @Test
    public void runtimeMaintenanceStateIsPackagePrivateAndOwnedByComponents() throws ReflectiveOperationException {
        Class<?> runtimeState = Class.forName("yier.bubu.redis.storage.memory.YierdisDbRuntimeState");
        Assert.assertFalse(
                "YierdisDbRuntimeState should stay package-private",
                Modifier.isPublic(runtimeState.getModifiers())
        );
        Assert.assertNotNull(YierdisDbComponents.class.getDeclaredField("runtimeState"));
        Assert.assertNotNull(YierdisDbComponents.class.getDeclaredField("maintenance"));
    }

    @Test
    public void factoryUsesRuntimeStateInsteadOfOwnerMaintenanceCallbacks() throws IOException {
        String factorySource = Files.readString(Path.of(
                "src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java"
        ));
        String dbSource = Files.readString(Path.of("src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java"));

        Assert.assertTrue(
                "Factory should receive focused runtime state",
                factorySource.contains("YierdisDbRuntimeState runtimeState")
        );
        Assert.assertTrue(
                "YierdisDb should keep one focused runtime state field",
                dbSource.contains("private final YierdisDbRuntimeState runtimeState;")
        );
        for (String removedCallback : Set.of(
                "owner::maxmemoryCoordinator",
                "owner::nextLruClock",
                "owner::adjustUsedBytes",
                "owner::flushDb",
                "owner::lastNativeDefragReport",
                "owner.maxmemoryCoordinator()"
        )) {
            Assert.assertFalse(
                    "Factory must not use broad owner maintenance callback: " + removedCallback,
                    factorySource.contains(removedCallback)
            );
        }
    }
}
