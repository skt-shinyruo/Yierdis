package yier.bubu.redis.storage.memory.internal.keyspace;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.nio.charset.StandardCharsets;

public class NativeKeyDirectoryTest {
    @Test
    public void nativeKeyDirectoryMapsKeysToStableHandlesAndReleasesThem() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("directory-test")) {
            NativeKeyDirectory directory = new NativeKeyDirectory(runtime);
            EntryTable entryTable = new EntryTable(runtime, 64);
            try {
                EntryHandle first = entryTable.allocate(entryRecord(1L));
                EntryHandle second = entryTable.allocate(entryRecord(2L));

                directory.compute(bytes("k1"), (key, old) -> first);
                directory.compute(bytes("k2"), (key, old) -> second);

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                for (int i = 0; i < 200; i++) {
                    EntryHandle next = entryTable.allocate(entryRecord(3L + i));
                    directory.compute(bytes("k" + (i + 3)), (key, old) -> next);
                }

                Assert.assertEquals(first, directory.get(bytes("k1")));
                Assert.assertEquals(second, directory.get(bytes("k2")));

                Assert.assertTrue(directory.remove(bytes("k1"), first));
                Assert.assertNull(directory.get(bytes("k1")));
            } finally {
                directory.close();
                entryTable.close();
            }

            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static EntryRecord entryRecord(long keyHandle) {
        return new EntryRecord(
                keyHandle,
                new ValueHandle(0L),
                1,
                ValueType.STRING,
                ValueEncoding.STRING_RAW,
                0,
                -1L,
                0L,
                0L
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
