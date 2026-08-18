package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.HashOps;
import yier.bubu.redis.storage.api.HllOps;
import yier.bubu.redis.storage.api.KeyspaceOps;
import yier.bubu.redis.storage.api.ListOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetOps;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.TtlOps;
import yier.bubu.redis.storage.api.ZSetOps;
import yier.bubu.redis.bytes.BytesView;

import java.lang.reflect.Method;

public class DbEngineReadWriteBoundaryTest {
    @Test
    public void dbEngineExposesPublicReadAndWriteContracts() throws Exception {
        Assert.assertEquals(StringOps.class, DbEngine.class.getMethod("strings").getReturnType());
        Assert.assertEquals(HashOps.class, DbEngine.class.getMethod("hashes").getReturnType());
        Assert.assertEquals(ListOps.class, DbEngine.class.getMethod("lists").getReturnType());
        Assert.assertEquals(SetOps.class, DbEngine.class.getMethod("sets").getReturnType());
        Assert.assertEquals(ZSetOps.class, DbEngine.class.getMethod("zsets").getReturnType());
        Assert.assertEquals(HllOps.class, DbEngine.class.getMethod("hll").getReturnType());
        Assert.assertEquals(KeyspaceOps.class, DbEngine.class.getMethod("keyspace").getReturnType());
        Assert.assertEquals(TtlOps.class, DbEngine.class.getMethod("ttl").getReturnType());
        Assert.assertNull(findMethod(DbEngine.class, "values"));
        Assert.assertNull(findMethod(DbEngine.class, "eviction"));
        Assert.assertNull(findMethod(DbEngine.class, "enforceMaxmemoryMaintenance"));
    }

    @Test
    public void dbTypeContractsContainReadAndWriteMethods() throws Exception {
        Assert.assertNotNull(StringOps.class.getMethod("getStringValue", BytesView.class));
        Assert.assertNotNull(StringOps.class.getMethod("setString", byte[].class, byte[].class,
                yier.bubu.redis.storage.api.SetMode.class,
                yier.bubu.redis.storage.api.ExpireOption.class));
        Assert.assertNotNull(HashOps.class.getMethod("hget", byte[].class, byte[].class));
        Assert.assertNotNull(HashOps.class.getMethod("hset", byte[].class, java.util.List.class));
        Assert.assertNotNull(ListOps.class.getMethod("lrange", byte[].class, int.class, int.class));
        Assert.assertNotNull(ListOps.class.getMethod("rpush", byte[].class, java.util.List.class));
        Assert.assertNotNull(SetOps.class.getMethod("smembers", byte[].class));
        Assert.assertNotNull(SetOps.class.getMethod("sadd", byte[].class, java.util.List.class));
        Assert.assertNotNull(ZSetOps.class.getMethod("zrange", byte[].class, long.class, long.class, boolean.class));
        Assert.assertNotNull(ZSetOps.class.getMethod("zadd", byte[].class, java.util.List.class));
        Assert.assertNotNull(HllOps.class.getMethod("pfcount", java.util.List.class));
        Assert.assertNotNull(HllOps.class.getMethod("pfadd", byte[].class, java.util.List.class));
        Assert.assertNotNull(KeyspaceOps.class.getMethod("existsKey", yier.bubu.redis.bytes.BytesView.class));
        Assert.assertNotNull(KeyspaceOps.class.getMethod("del", java.util.Collection.class));
        Assert.assertNotNull(TtlOps.class.getMethod("ttlMillis", yier.bubu.redis.bytes.BytesView.class));
        Assert.assertNotNull(TtlOps.class.getMethod("persist", yier.bubu.redis.bytes.BytesView.class));
    }

    @Test
    public void runtimeDbEngineExposesRuntimeOnlyMaintenanceHook() throws Exception {
        Method maintenance = RuntimeDbEngine.class.getMethod("runMaintenance");

        Assert.assertEquals(void.class, maintenance.getReturnType());
        Assert.assertNull(findMethod(DbEngine.class, "runMaintenance"));
        Assert.assertNull(findMethod(DbEngine.class, "enforceMaxmemoryMaintenance"));
    }

    private static Method findMethod(Class<?> type, String name) {
        for (Method method : type.getMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        return null;
    }
}
