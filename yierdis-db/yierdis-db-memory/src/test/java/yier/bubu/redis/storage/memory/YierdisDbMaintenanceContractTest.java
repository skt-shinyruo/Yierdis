package yier.bubu.redis.storage.memory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

public class YierdisDbMaintenanceContractTest {
    @Test
    public void maintenanceCleansExpiryAndAdvancesHashWorkWithoutGlobalCoordination() {
        YierdisDb db = TestDbSupport.open(new DbEngineConfig(
                0,
                1L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        try {
            RecordingCoordinator coordinator = new RecordingCoordinator();
            db.attachMaxmemoryCoordinator(coordinator);

            byte[] expired = bytes("expired");
            byte[] live = bytes("live");
            Assert.assertTrue(db.writes().strings().setString(
                    expired,
                    bytes("value"),
                    SetMode.NORMAL,
                    null
            ).value());
            db.setExpireAtMillis(expired, System.currentTimeMillis() - 1L);
            for (int index = 0; index < 13; index++) {
                byte[] key = index == 0 ? live : bytes("live-" + index);
                Assert.assertTrue(db.writes().strings().setString(
                        key,
                        bytes("value"),
                        SetMode.NORMAL,
                        null
                ).value());
            }

            int pendingBefore = db.memory().memoryStats().pendingHashTableCount();
            Assert.assertTrue("setup must create hash maintenance debt", pendingBefore > 0);
            coordinator.reset();

            db.runMaintenance();

            Assert.assertEquals(0, coordinator.prepareWriteCalls);
            Assert.assertEquals(13, db.size());
            Assert.assertTrue(db.reads().keyspace().existsKey(view(live)));
            Assert.assertTrue(
                    "maintenance must advance registered hash-table work",
                    db.memory().memoryStats().pendingHashTableCount() < pendingBefore
            );
        } finally {
            db.shutdown();
        }
    }

    @Test
    public void maintenanceLeavesEnabledDefragmentationToExplicitDefragCycle() {
        AtomicInteger defragCycleCalls = new AtomicInteger();
        YierdisDb db = TestDbSupport.openWithFactory(
                (name, maxSlots, owner) -> recordingDefragBackend(
                        name,
                        maxSlots,
                        owner,
                        defragCycleCalls
                ),
                64,
                new DbEngineConfig(
                        0,
                        0L,
                        MaxmemoryPolicy.NOEVICTION,
                        5,
                        5L,
                        5L,
                        new DbDefragConfig(true, 64L, 64L, 1L)
                )
        );
        try {
            db.runMaintenance();

            Assert.assertEquals(0, defragCycleCalls.get());

            db.defragMaintenance();

            Assert.assertEquals(1, defragCycleCalls.get());
        } finally {
            db.shutdown();
        }
    }

    private static StableMemoryBackend recordingDefragBackend(
            String name,
            int maxSlots,
            MemoryOwner owner,
            AtomicInteger defragCycleCalls
    ) {
        StableMemoryBackend delegate = new HeapStableMemoryBackend(name, maxSlots, owner);
        return (StableMemoryBackend) Proxy.newProxyInstance(
                StableMemoryBackend.class.getClassLoader(),
                new Class<?>[]{StableMemoryBackend.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("defragCycle")) {
                        defragCycleCalls.incrementAndGet();
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static BytesView view(byte[] bytes) {
        return new BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }

    private static final class RecordingCoordinator implements MaxmemoryCoordinator {
        private int prepareWriteCalls;

        @Override
        public void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes) {
            prepareWriteCalls++;
        }

        @Override
        public long nextLruClock() {
            return 1L;
        }

        private void reset() {
            prepareWriteCalls = 0;
        }
    }
}
