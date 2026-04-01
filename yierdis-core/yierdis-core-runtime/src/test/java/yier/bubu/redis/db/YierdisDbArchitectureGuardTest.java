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
    }

    private static Method findDeclaredMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        }
    }
}
