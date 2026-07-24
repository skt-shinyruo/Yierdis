package yier.bubu.redis.storage.memory;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

public class DbOwnerBindingTest {
    @Test
    public void factoryPassesTheDatabaseOwnerObjectToTheBackend() {
        AtomicReference<MemoryOwner> received = new AtomicReference<>();
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                (name, slots, owner) -> {
                    received.set(owner);
                    return new HeapStableMemoryBackend(name, slots, owner);
                },
                new YierdisDbBackendConfig(64)
        );
        YierdisDb engine = (YierdisDb) factory.create(StableMemoryBackendCompositionTest.config());

        try {
            Assert.assertSame(engine.memoryOwnerForTesting(), received.get());
            engine.bindToCurrentThread();
        } finally {
            engine.shutdown();
        }
    }
}
