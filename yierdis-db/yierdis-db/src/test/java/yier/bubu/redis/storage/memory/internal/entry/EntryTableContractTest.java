package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Arrays;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public class EntryTableContractTest {
    @Test
    public void entryTableDoesNotMirrorEveryLiveHandle() {
        Assert.assertFalse(Arrays.stream(EntryTable.class.getDeclaredFields())
                .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
    }

    @Test
    public void entryTableStoresCompleteNestedHandleIdentity() {
        try (TestBackend runtime = TestBackend.open("entry-test")) {
            EntryTable table = new EntryTable(runtime.backend());
            EntryRecord initial = record(11L, 22L);
            EntryHandle handle = table.allocate(initial);
            try {
                Assert.assertEquals(initial, table.get(handle));
                Assert.assertEquals(initial.valueHandle().nativeHandle(), table.get(handle).valueHandle().nativeHandle());
                EntryRecord replacement = record(33L, 44L);
                Assert.assertEquals(initial, table.replace(handle, replacement));
                Assert.assertEquals(replacement, table.get(handle));
            } finally {
                table.release(handle);
                table.close();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void entryTableRejectsReleasedHandle() {
        try (TestBackend runtime = TestBackend.open("entry-stale")) {
            EntryTable table = new EntryTable(runtime.backend());
            EntryHandle first = table.allocate(record(1L, 2L));
            table.release(first);
            try {
                Assert.assertThrows(StaleNativeHandleException.class, () -> table.get(first));
            } finally {
                table.close();
            }
        }
    }

    private static EntryRecord record(long keyRaw, long valueRaw) {
        return new EntryRecord(
                new NativeHandle(17L, keyRaw),
                new ValueHandle(new NativeHandle(18L, valueRaw)),
                3,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        );
    }
}
