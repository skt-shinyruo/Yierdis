package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;

import java.lang.reflect.Method;

public class YierdisDbArchitectureGuardTest {
    @Test
    public void yierdisDbMustNotOwnExtractedStringTtlAndKeyspaceMethods() {
        Assert.assertNull(findDeclaredMethod(
                YierdisDb.class,
                "setString",
                byte[].class,
                byte[].class,
                SetMode.class,
                ExpireOption.class
        ));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "getStringBytes", byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "expire", BytesView.class, long.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "keys", byte[].class, int.class, long.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "hset", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "hget", byte[].class, byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "lpush", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "lrange", byte[].class, int.class, int.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "sadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "smembers", byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "zadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "zrange", byte[].class, long.class, long.class, boolean.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "pfadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "pfcount", java.util.List.class));
    }

    @Test
    public void yierdisDbInternalsMustNotExposeRawContainersOrMemoryRuntime() {
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "store"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "expires"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "offHeapAllocator"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "memoryRuntime"));
    }

    private static Method findDeclaredMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        }
    }
}
