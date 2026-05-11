package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmSlabAllocator;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.api.ValueType;

public class EntryTableContractTest {
    @Test
    public void entryRecordCarriesNativeMetadata() {
        EntryRecord record = new EntryRecord(
                11L,
                new ValueHandle(22L),
                33,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                7,
                99L,
                123L,
                456L
        );

        Assert.assertEquals(11L, record.keyHandle());
        Assert.assertEquals(22L, record.valueHandle().raw());
        Assert.assertEquals(ValueType.STRING, record.type());
        Assert.assertEquals(ValueEncoding.STRING_RAW, record.encoding());
    }

    @Test
    public void entryTableAllocatesAndReleasesHandles() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-test")) {
            EntryTable table = new EntryTable(runtime, 64);
            EntryHandle handle = table.allocate(new EntryRecord(
                    1L, new ValueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            Assert.assertNotNull(table.get(handle));
            Assert.assertTrue(runtime.usedBytes() > 0L);
            table.release(handle);
            Assert.assertNull(table.get(handle));
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void entryTableReplacesRecordsInPlaceAndClosesNativeSlots() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-replace-test")) {
            EntryTable table = new EntryTable(runtime, 64);
            EntryHandle handle = table.allocate(new EntryRecord(
                    1L, new ValueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            long bytesAfterAllocate = runtime.usedBytes();
            EntryRecord previous = table.replace(handle, new EntryRecord(
                    11L, new ValueHandle(22L), 33, ValueType.STRING, ValueEncoding.STRING_EMBSTR, 7, 99L, 123L, 456L
            ));

            Assert.assertEquals(1L, previous.keyHandle());
            Assert.assertEquals(bytesAfterAllocate, runtime.usedBytes());
            EntryRecord current = table.get(handle);
            Assert.assertEquals(11L, current.keyHandle());
            Assert.assertEquals(22L, current.valueHandle().raw());
            Assert.assertEquals(ValueEncoding.STRING_EMBSTR, current.encoding());

            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void ownedSlabEntryTableReleasesIdleSlabsAndCanAllocateAgain() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-slab-idle-test")) {
            EntryTable table = new EntryTable(runtime, new YierdisFfmSlabAllocator(runtime), 64);

            EntryHandle first = table.allocate(new EntryRecord(
                    1L, new ValueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            ));
            Assert.assertTrue(runtime.usedBytes() > 0L);
            table.release(first);
            Assert.assertEquals(0L, runtime.usedBytes());

            EntryHandle second = table.allocate(new EntryRecord(
                    11L, new ValueHandle(22L), 33, ValueType.STRING, ValueEncoding.STRING_EMBSTR, 0, -1L, 0L, 0L
            ));
            Assert.assertTrue(runtime.usedBytes() > 0L);
            table.release(second);
            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }
}
