package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryReservation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MaxmemoryPageTrimTest {
    @Test
    public void localAdmissionRunsTrimBeforeNoevictionOom() {
        AtomicLong physicalUsedBytes = new AtomicLong(110);
        AtomicLong requestedLimit = new AtomicLong(-1);
        AtomicBoolean trimRan = new AtomicBoolean(false);
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                100,
                MaxmemoryPolicy.NOEVICTION,
                () -> {
                },
                limit -> {
                    trimRan.set(true);
                    requestedLimit.set(limit);
                    physicalUsedBytes.set(80);
                },
                physicalUsedBytes::get,
                () -> null
        );

        MemoryReservation reservation = ledger.reserve(10);

        Assert.assertTrue("local admission must run reclaim before rejecting growth", trimRan.get());
        Assert.assertEquals(90L, requestedLimit.get());
        Assert.assertEquals(10L, reservation.reservedBytes());
        Assert.assertEquals(10L, ledger.reservedBytes());

        ledger.rollback(reservation);
        Assert.assertEquals(0L, ledger.reservedBytes());
    }

    @Test
    public void localAdmissionThrowsOomWhenTrimMakesNoPhysicalProgress() {
        AtomicLong physicalUsedBytes = new AtomicLong(110);
        AtomicBoolean trimRan = new AtomicBoolean(false);
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                100,
                MaxmemoryPolicy.NOEVICTION,
                () -> {
                },
                limit -> trimRan.set(true),
                physicalUsedBytes::get,
                () -> null
        );

        try {
            ledger.reserve(10);
            Assert.fail("expected OOM when trim leaves physical usage above the admitted limit");
        } catch (MemoryLedgerOutOfMemoryException expected) {
            // expected
        }

        Assert.assertTrue("trim should run immediately before OOM", trimRan.get());
        Assert.assertEquals(0L, ledger.reservedBytes());
    }

    @Test
    public void realAllocatorEmptyPageIsTrimmedBeforeNoevictionAdmission() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("maxmemory-admission-trim");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            allocator.bindToCurrentThread();
            long handle = allocator.allocateRaw(NativeObjectKind.STRING_BYTES, 24 * 1024);
            allocator.freeRaw(handle);

            long physicalBefore = allocator.memoryUsage().effectiveBytesForMaxmemory();
            long dataCommittedBefore = allocator.memoryUsage().nativeDataCommittedBytes();
            Assert.assertTrue(allocator.stats().freePages() > 0L);

            YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                    physicalBefore - 1L,
                    MaxmemoryPolicy.NOEVICTION,
                    () -> {
                    },
                    ignored -> allocator.trimEmptyPages(MemoryPressureBudget.unlimited()),
                    () -> allocator.memoryUsage().effectiveBytesForMaxmemory(),
                    () -> null
            );

            MemoryReservation reservation = ledger.reserve(1L);

            Assert.assertEquals(1L, reservation.reservedBytes());
            Assert.assertEquals(0L, allocator.stats().freePages());
            Assert.assertTrue(allocator.memoryUsage().nativeDataCommittedBytes() < dataCommittedBefore);
            Assert.assertTrue(
                    allocator.memoryUsage().effectiveBytesForMaxmemory() + reservation.reservedBytes()
                            <= ledger.limitBytes()
            );
            ledger.rollback(reservation);
            Assert.assertEquals(0L, ledger.reservedBytes());
        }
    }
}
