package yier.bubu.redis.command.defaults;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;

public class CommandDbTest {
    @Test
    public void capabilitiesStayLazyAndAreReturnedDirectly() {
        List<String> accesses = new ArrayList<>();
        DbReads reads = proxy(DbReads.class);
        DbWrites writes = proxy(DbWrites.class);
        DbLifecycleOps lifecycle = proxy(DbLifecycleOps.class);
        DbEngine engine = (DbEngine) Proxy.newProxyInstance(
                DbEngine.class.getClassLoader(),
                new Class<?>[]{DbEngine.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "reads" -> {
                        accesses.add("reads");
                        yield reads;
                    }
                    case "writes" -> {
                        accesses.add("writes");
                        yield writes;
                    }
                    case "lifecycle" -> {
                        accesses.add("lifecycle");
                        yield lifecycle;
                    }
                    default -> throw new AssertionError("unexpected engine access: " + method.getName());
                }
        );

        CommandDb db = new CommandDb(engine);

        Assert.assertTrue(accesses.isEmpty());
        Assert.assertSame(reads, db.reads());
        Assert.assertSame(writes, db.writes());
        Assert.assertSame(lifecycle, db.lifecycle());
        Assert.assertEquals(List.of("reads", "writes", "lifecycle"), accesses);
    }

    private static <T> T proxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (proxy, method, args) -> {
                    throw new AssertionError("unexpected access: " + method.getName());
                }
        ));
    }
}
