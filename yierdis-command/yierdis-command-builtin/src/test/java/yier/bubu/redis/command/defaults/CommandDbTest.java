package yier.bubu.redis.command.defaults;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;

public class CommandDbTest {
    @Test
    public void constructionKeepsUnusedCapabilitiesLazyAndBindsContextOnAccess() {
        List<String> events = new ArrayList<>();
        MutationContext context = MutationContext.none();
        DbReads reads = proxy(DbReads.class, unexpected());
        DbWrites writes = proxy(DbWrites.class, (proxy, method, args) -> {
            if ("withMutationContext".equals(method.getName())) {
                events.add("bind writes");
                Assert.assertSame(context, args[0]);
                return proxy;
            }
            throw new AssertionError("unexpected writes access: " + method.getName());
        });
        DbLifecycleOps lifecycle = proxy(DbLifecycleOps.class, (proxy, method, args) -> {
            if ("withMutationContext".equals(method.getName())) {
                events.add("bind lifecycle");
                Assert.assertSame(context, args[0]);
                return proxy;
            }
            throw new AssertionError("unexpected lifecycle access: " + method.getName());
        });
        DbEngine engine = proxy(DbEngine.class, (proxy, method, args) -> switch (method.getName()) {
            case "reads" -> {
                events.add("reads");
                yield reads;
            }
            case "writes" -> {
                events.add("writes");
                yield writes;
            }
            case "lifecycle" -> {
                events.add("lifecycle");
                yield lifecycle;
            }
            default -> throw new AssertionError("unexpected engine access: " + method.getName());
        });

        CommandDb db = new CommandDb(engine, context);

        Assert.assertTrue(events.isEmpty());
        Assert.assertSame(reads, db.reads());
        Assert.assertEquals(List.of("reads"), events);

        events.clear();
        Assert.assertSame(writes, db.writes());
        Assert.assertEquals(List.of("writes", "bind writes"), events);

        events.clear();
        Assert.assertSame(lifecycle, db.lifecycle());
        Assert.assertEquals(List.of("lifecycle", "bind lifecycle"), events);
    }

    @Test
    public void contextFreeViewReturnsRawCapabilitiesWithoutBinding() {
        List<String> events = new ArrayList<>();
        DbWrites writes = proxy(DbWrites.class, unexpected());
        DbLifecycleOps lifecycle = proxy(DbLifecycleOps.class, unexpected());
        DbEngine engine = proxy(DbEngine.class, (proxy, method, args) -> switch (method.getName()) {
            case "writes" -> {
                events.add("writes");
                yield writes;
            }
            case "lifecycle" -> {
                events.add("lifecycle");
                yield lifecycle;
            }
            default -> throw new AssertionError("unexpected engine access: " + method.getName());
        });

        CommandDb db = new CommandDb(engine, null);

        Assert.assertSame(writes, db.writes());
        Assert.assertSame(lifecycle, db.lifecycle());
        Assert.assertEquals(List.of("writes", "lifecycle"), events);
    }

    private static InvocationHandler unexpected() {
        return (proxy, method, args) -> {
            throw new AssertionError("unexpected access: " + method.getName());
        };
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
