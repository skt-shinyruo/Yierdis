package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;

public class StableMemoryBackendCompositionTest {
    @Test
    public void factoryConstructsAndMutatesDatabaseWithHeapBackend() {
        AtomicReference<HeapStableMemoryBackend> created = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                (name, maxSlots, owner) -> {
                    HeapStableMemoryBackend backend = new HeapStableMemoryBackend(name, maxSlots, owner);
                    created.set(backend);
                    return backend;
                },
                4096
        );
        RuntimeDbEngine engine = factory.create(config());
        byte[] key = "heap-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "heap-value".getBytes(StandardCharsets.UTF_8);

        try {
            engine.bindToCurrentThread();
            Assert.assertTrue(engine.strings().setString(key, value, SetMode.NORMAL, null).value());
            Assert.assertArrayEquals(value, OwnedReplyValueAssertions.stringValue(engine.strings(), key));
            Assert.assertNotNull(created.get());
        } finally {
            engine.shutdown();
        }
    }

    static DbEngineConfig config() {
        return new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        );
    }
}
