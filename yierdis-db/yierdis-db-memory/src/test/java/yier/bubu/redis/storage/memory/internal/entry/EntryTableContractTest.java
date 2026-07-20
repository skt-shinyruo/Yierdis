package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Arrays;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.api.ValueType;

public class EntryTableContractTest {
    @Test
    public void entryTableDoesNotMirrorEveryLiveHandle() {
        Assert.assertFalse(Arrays.stream(EntryTable.class.getDeclaredFields())
                .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
        Assert.assertFalse(Arrays.stream(EntryTable.class.getDeclaredFields())
                .anyMatch(field -> NativeHandle.class.isAssignableFrom(field.getType())));
    }

    @Test
    public void entryTableUsesRawAllocatorPathForRecordLifecycle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-raw-path");
             RawPathRecordingAllocator allocator = new RawPathRecordingAllocator(
                     new YierdisStableNativeAllocator(runtime, 64)
             );
             EntryTable table = new EntryTable(runtime, allocator)) {
            EntryRecord initial = new EntryRecord(
                    1L, valueHandle(2L), 3, ValueType.STRING, ValueEncoding.STRING_RAW, 0, -1L, 0L, 0L
            );
            EntryHandle allocated = table.allocate(initial);
            EntryHandle reserved = table.reserve();
            table.writeReserved(reserved, initial);

            Assert.assertEquals(initial, table.get(allocated));
            Assert.assertEquals(initial, table.replace(reserved, new EntryRecord(
                    11L, valueHandle(22L), 33, ValueType.STRING, ValueEncoding.STRING_EMBSTR, 1, 2L, 3L, 4L
            )));

            table.release(allocated);
            table.release(reserved);
            Assert.assertEquals(2, allocator.allocateRawCalls());
            Assert.assertEquals(2, allocator.freeRawCalls());
            Assert.assertEquals(5, allocator.resolveRawCalls());
        }
    }

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

            table.release(handle);
            table.close();
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void entryTableRejectsStaleReleasedHandleAfterSlotReuse() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("entry-slab-idle-test")) {
            EntryTable table = new EntryTable(runtime, 64);

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

            table.release(second);
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
