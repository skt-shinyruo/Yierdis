package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CollectionRootTest {
    @Test
    public void hashSetAndZsetRootsRoundTripMembers() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("collection-root");
             HashRoot hash = new HashRoot(runtime);
             SetRoot set = new SetRoot(runtime);
             ZSetRoot zset = new ZSetRoot(runtime)) {
            ValueHandle hashHandle = hash.create();
            ValueHandle setHandle = set.create();
            ValueHandle zsetHandle = zset.create();

            hash.hset(hashHandle, b("field"), b("value"));
            set.sadd(setHandle, List.of(b("alpha"), b("beta")));
            zset.zadd(zsetHandle, List.of(b("1"), b("m1"), b("2"), b("m2")));

            Assert.assertArrayEquals(b("value"), hash.hget(hashHandle, b("field")));
            Assert.assertEquals(2, set.size(setHandle));
            Assert.assertEquals(2, zset.size(zsetHandle));
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
