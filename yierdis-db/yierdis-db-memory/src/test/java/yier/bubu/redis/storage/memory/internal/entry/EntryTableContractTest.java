package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmSlabAllocator;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.api.ValueType;

public class EntryTableContractTest {
    @Test
    public void entryRecordCarriesNativeMetadata() {
        EntryRecord record = new EntryRecord(
                11L,
                valueHandle(22L),
                33,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                7,
                99L,
                123L,
                456L
        );

        Assert.assertEquals(11L, record.keyHandle());
        Assert.assertEquals(22L, record.valueHandle().nativeHandle().slotId());
        Assert.assertEquals(ValueType.STRING, record.type());
        Assert.assertEquals(ValueEncoding.STRING_RAW, record.encoding());
    }

    @Test
    public void entryTableAllocatesAndReleasesHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-test")) {
            EntryTable table = new EntryTable(runtime, 64);
            EntryHandle handle = table.allocate(new EntryRecord(
                    1L, valueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            Assert.assertEquals(NativeObjectKind.ENTRY_RECORD.domain(), handle.nativeHandle().domain());
            Assert.assertEquals(NativeObjectKind.ENTRY_RECORD.code(), handle.nativeHandle().kindCode());
            Assert.assertNotNull(table.get(handle));
            Assert.assertTrue(runtime.usedBytes() > 0L);
            table.release(handle);
            try {
                table.get(handle);
                Assert.fail("expected stale entry handle");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void entryTableReplacesRecordsInPlaceAndUsesStableNativeHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-replace-test")) {
            EntryTable table = new EntryTable(runtime, 64);
            EntryHandle handle = table.allocate(new EntryRecord(
                    1L, valueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            long bytesAfterAllocate = runtime.usedBytes();
            EntryRecord previous = table.replace(handle, new EntryRecord(
                    11L, valueHandle(22L), 33, ValueType.STRING, ValueEncoding.STRING_EMBSTR, 7, 99L, 123L, 456L
            ));

            Assert.assertEquals(1L, previous.keyHandle());
            Assert.assertEquals(bytesAfterAllocate, runtime.usedBytes());
            EntryRecord current = table.get(handle);
            Assert.assertEquals(11L, current.keyHandle());
            Assert.assertEquals(22L, current.valueHandle().nativeHandle().slotId());
            Assert.assertEquals(ValueEncoding.STRING_EMBSTR, current.encoding());

            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void entryTableRejectsStaleReleasedHandleAfterSlotReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-slab-idle-test")) {
            EntryTable table = new EntryTable(runtime, new YierdisFfmSlabAllocator(runtime), 64);

            EntryHandle first = table.allocate(new EntryRecord(
                    1L, valueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            Assert.assertTrue(runtime.usedBytes() > 0L);
            table.release(first);

            try {
                table.get(first);
                Assert.fail("expected released entry handle rejection");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            EntryHandle second = table.allocate(new EntryRecord(
                    11L, valueHandle(22L), 33, ValueType.STRING, ValueEncoding.STRING_EMBSTR, 0, -1L, 0L, 0L
            ));
            Assert.assertNotEquals(first.raw(), second.raw());

            try {
                table.get(first);
                Assert.fail("expected stale entry handle rejection");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }

            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static ValueHandle valueHandle(long slotId) {
        return ValueHandle.fromNativeHandle(NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                slotId,
                1,
                0
        ));
    }
}
