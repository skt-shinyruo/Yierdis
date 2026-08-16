package yier.bubu.redis.storage.memory;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryReservation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

public class FfmPageTrimIntegrationTest {
    @Test
    public void emptyFfmPageIsTrimmedBeforeNoevictionAdmission() {
        try (StableMemoryBackend backend = new YierdisFfmStableMemoryBackend(
                "maxmemory-admission-trim",
                128,
                new DbThreadGuard()
        )) {
            backend.bindToCurrentThread();
            NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 24 * 1024);
            backend.free(handle);

            long physicalBefore = backend.memoryUsage().effectiveBytesForMaxmemory();
            long dataCommittedBefore = backend.memoryUsage().nativeDataCommittedBytes();
            Assert.assertTrue(backend.stats().freePages() > 0L);

            YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                    physicalBefore - 1L,
                    MaxmemoryPolicy.NOEVICTION,
                    () -> {
                    },
                    ignored -> backend.trimEmptyPages(MemoryPressureBudget.unlimited()),
                    () -> backend.memoryUsage().effectiveBytesForMaxmemory(),
                    () -> null,
                    () -> null
            );

            MemoryReservation reservation = ledger.reserve(1L);

            Assert.assertEquals(1L, reservation.reservedBytes());
            Assert.assertEquals(0L, backend.stats().freePages());
            Assert.assertTrue(backend.memoryUsage().nativeDataCommittedBytes() < dataCommittedBefore);
            Assert.assertTrue(
                    backend.memoryUsage().effectiveBytesForMaxmemory() + reservation.reservedBytes()
                            <= ledger.limitBytes()
            );
            ledger.rollback(reservation);
            Assert.assertEquals(0L, ledger.reservedBytes());
        }
    }
}
