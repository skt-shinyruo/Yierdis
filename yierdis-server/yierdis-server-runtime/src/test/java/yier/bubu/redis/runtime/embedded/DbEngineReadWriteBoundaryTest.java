package yier.bubu.redis.runtime.embedded;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.HashReadOps;
import yier.bubu.redis.storage.api.HashWriteOps;
import yier.bubu.redis.storage.api.HllReadOps;
import yier.bubu.redis.storage.api.HllWriteOps;
import yier.bubu.redis.storage.api.KeyspaceReadOps;
import yier.bubu.redis.storage.api.KeyspaceWriteOps;
import yier.bubu.redis.storage.api.ListReadOps;
import yier.bubu.redis.storage.api.ListWriteOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.StringReadOps;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.TtlReadOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;

import java.lang.reflect.Method;

public class DbEngineReadWriteBoundaryTest {
    @Test
    public void dbEngineExposesPublicReadAndWriteContracts() throws Exception {
        Method reads = DbEngine.class.getMethod("reads");
        Method writes = DbEngine.class.getMethod("writes");

        Assert.assertEquals(DbReads.class, reads.getReturnType());
        Assert.assertEquals(DbWrites.class, writes.getReturnType());
        Assert.assertNull(findMethod(DbEngine.class, "values"));
        Assert.assertNull(findMethod(DbEngine.class, "eviction"));
        Assert.assertNull(findMethod(DbEngine.class, "enforceMaxmemoryMaintenance"));
    }

    @Test
    public void dbReadAndWriteContractsAreSplitByCapabilityFamily() throws Exception {
        Assert.assertEquals(StringReadOps.class, DbReads.class.getMethod("strings").getReturnType());
        Assert.assertEquals(HashReadOps.class, DbReads.class.getMethod("hashes").getReturnType());
        Assert.assertEquals(ListReadOps.class, DbReads.class.getMethod("lists").getReturnType());
        Assert.assertEquals(SetReadOps.class, DbReads.class.getMethod("sets").getReturnType());
        Assert.assertEquals(ZSetReadOps.class, DbReads.class.getMethod("zsets").getReturnType());
        Assert.assertEquals(HllReadOps.class, DbReads.class.getMethod("hll").getReturnType());
        Assert.assertEquals(KeyspaceReadOps.class, DbReads.class.getMethod("keyspace").getReturnType());
        Assert.assertEquals(TtlReadOps.class, DbReads.class.getMethod("ttl").getReturnType());

        Assert.assertEquals(StringWriteOps.class, DbWrites.class.getMethod("strings").getReturnType());
        Assert.assertEquals(HashWriteOps.class, DbWrites.class.getMethod("hashes").getReturnType());
        Assert.assertEquals(ListWriteOps.class, DbWrites.class.getMethod("lists").getReturnType());
        Assert.assertEquals(SetWriteOps.class, DbWrites.class.getMethod("sets").getReturnType());
        Assert.assertEquals(ZSetWriteOps.class, DbWrites.class.getMethod("zsets").getReturnType());
        Assert.assertEquals(HllWriteOps.class, DbWrites.class.getMethod("hll").getReturnType());
        Assert.assertEquals(KeyspaceWriteOps.class, DbWrites.class.getMethod("keyspace").getReturnType());
        Assert.assertEquals(TtlWriteOps.class, DbWrites.class.getMethod("ttl").getReturnType());
    }

    @Test
    public void runtimeDbEngineExposesRuntimeOnlyMaxmemoryMaintenanceHook() throws Exception {
        Method maintenance = RuntimeDbEngine.class.getMethod("enforceMaxmemoryMaintenance");

        Assert.assertEquals(void.class, maintenance.getReturnType());
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
