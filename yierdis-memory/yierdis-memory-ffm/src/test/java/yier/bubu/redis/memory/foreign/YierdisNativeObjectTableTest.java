package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public class YierdisNativeObjectTableTest {
    @Test
    public void allocatesGenerationBearingHandlesAndStoresMetadata() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-basic");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 4, 77)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 32, 48, 1234L, 3, 9L);
            YierdisNativeObjectMeta meta = table.resolve(handle);

            Assert.assertEquals(NativeObjectKind.STRING_BYTES.domain(), handle.domain());
            Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());
            Assert.assertEquals(1L, handle.slotId());
            Assert.assertEquals(1, handle.generation());
            Assert.assertEquals(32, meta.size());
            Assert.assertEquals(48, meta.capacity());
            Assert.assertEquals(1234L, meta.address());
            Assert.assertEquals(3, meta.pageClass());
            Assert.assertEquals(77, meta.ownerShardId());
            Assert.assertEquals(9L, meta.allocEpoch());
            Assert.assertEquals(YierdisNativeObjectTable.STATE_ALLOCATED, meta.state());
        }
    }

    @Test
    public void freeIncrementsGenerationBeforeReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-reuse");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 1, 7)) {

            NativeHandle first = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 11L, 1, 1L);
            table.free(first, 2L);

            try {
                table.resolve(first);
                Assert.fail("expected stale handle");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            NativeHandle second = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 22L, 1, 3L);
            Assert.assertEquals(first.slotId(), second.slotId());
            Assert.assertEquals(first.generation() + 1, second.generation());
        }
    }

    @Test
    public void generationWrapRetiresSlot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-wrap");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 1, 7)) {

            for (int generation = 1; generation <= 0x0fff; generation++) {
                NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, generation, 1, generation);
                Assert.assertEquals(generation, handle.generation());
                table.free(handle, generation);
            }

            try {
                table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 1L, 1, 1L);
                Assert.fail("expected retired slot exhaustion");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }
        }
    }

    @Test
    public void pinnedFreeQuarantinesUntilUnpin() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-quarantine");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 1, 7)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 11L, 1, 1L);
            table.pin(handle);
            table.free(handle, 2L);

            YierdisNativeObjectMeta quarantined = table.snapshot(handle, true);
            Assert.assertEquals(YierdisNativeObjectTable.STATE_FREED_QUARANTINED, quarantined.state());
            Assert.assertEquals(1, quarantined.pinCount());
            Assert.assertEquals(2L, quarantined.freeEpoch());

            try {
                table.free(handle, 3L);
                Assert.fail("expected quarantined double-free stale rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                table.resolve(handle);
                Assert.fail("expected quarantined handle rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("quarantined"));
            }

            try {
                table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 22L, 1, 3L);
                Assert.fail("expected slot limit while quarantined");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("slot limit"));
            }

            table.unpin(handle);

            NativeHandle second = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 22L, 1, 3L);
            Assert.assertEquals(handle.slotId(), second.slotId());
            Assert.assertEquals(handle.generation() + 1, second.generation());
        }
    }

    @Test
    public void rejectsWrongKindAndDomainAndDoubleFree() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-invalid");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 2, 7)) {

            NativeHandle handle = table.allocate(NativeObjectKind.STRING_BYTES, 16, 16, 11L, 1, 1L);
            NativeHandle wrongKind = NativeHandle.of(handle.domain(), NativeObjectKind.GENERIC, handle.slotId(), handle.generation(), 0);
            NativeHandle wrongDomain = NativeHandle.of(NativeObjectKind.ENTRY_RECORD.domain(), NativeObjectKind.ENTRY_RECORD, handle.slotId(), handle.generation(), 0);

            try {
                table.resolve(wrongKind);
                Assert.fail("expected wrong-kind rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("kind/domain"));
            }

            try {
                table.resolve(wrongDomain);
                Assert.fail("expected wrong-domain rejection");
            } catch (NativeMemoryException expected) {
                Assert.assertTrue(expected.getMessage().contains("kind/domain"));
            }

            table.free(handle, 2L);

            try {
                table.free(handle, 3L);
                Assert.fail("expected double-free stale rejection");
            } catch (StaleNativeHandleException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
        }
    }

    @Test
    public void metadataIsBackedByNativeRuntimeMemory() {
        YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("object-table-native");
        YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 4, 7);
        try {
            Assert.assertTrue(runtime.usedBytes() >= YierdisNativeObjectTable.META_BYTES * 4L);
        } finally {
            table.close();
            runtime.close();
        }
    }
}
