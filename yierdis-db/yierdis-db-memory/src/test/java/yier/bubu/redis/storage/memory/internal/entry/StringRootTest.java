package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public class StringRootTest {
    @Test
    public void stringRootOverwritesWithoutReintroducingHeapPayloads() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root");
             StringRoot root = new StringRoot(runtime)) {
            ValueHandle handle = root.store(new byte[] { 'h', 'e', 'l', 'l', 'o' });
            Assert.assertEquals(ValueEncoding.STRING_RAW, root.encoding(handle));

            BytesSlice slice = root.slice(handle);
            byte[] copy = new byte[slice.length()];
            slice.getBytes(0, copy, 0, copy.length);
            Assert.assertArrayEquals(new byte[] { 'h', 'e', 'l', 'l', 'o' }, copy);

            root.overwrite(handle, new byte[] { 'w', 'o', 'r', 'l', 'd' });
            Assert.assertArrayEquals(new byte[] { 'w', 'o', 'r', 'l', 'd' }, root.copy(handle));
        }
    }
}
