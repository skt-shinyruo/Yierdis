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

    @Test
    public void stringRootStoresIntegerLikeBytesAsRawNativeBytes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-int-like");
             StringRoot root = new StringRoot(runtime)) {
            ValueHandle handle = root.store(new byte[] { '1', '2', '3', '4' });

            Assert.assertEquals(ValueEncoding.STRING_RAW, root.encoding(handle));
            Assert.assertEquals(4, root.length(handle));
            Assert.assertEquals('1', root.byteAt(handle, 0));
            Assert.assertArrayEquals(new byte[] { '1', '2', '3', '4' }, root.copy(handle));
        }
    }

    @Test
    public void stringRootOverwriteReusesSpareCapacityForShorterValue() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-spare-capacity");
             StringRoot root = new StringRoot(runtime)) {
            ValueHandle handle = root.store(new byte[32]);
            long allocatedBytes = root.estimatedBytes(handle);

            root.overwrite(handle, new byte[] { 'o', 'k' });

            Assert.assertEquals(2, root.length(handle));
            Assert.assertEquals(allocatedBytes, root.estimatedBytes(handle));
            Assert.assertArrayEquals(new byte[] { 'o', 'k' }, root.copy(handle));
        }
    }

    @Test
    public void stringRootEnsureLengthSupportsBitmapStyleGrowthWithZeroFill() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-bitmap-growth");
             StringRoot root = new StringRoot(runtime)) {
            ValueHandle handle = root.store(new byte[] { (byte) 0x80 });

            root.ensureLength(handle, 4);
            root.setByteAt(handle, 3, (byte) 0x01);

            Assert.assertEquals(4, root.length(handle));
            Assert.assertArrayEquals(new byte[] { (byte) 0x80, 0, 0, 1 }, root.copy(handle));
            Assert.assertTrue(root.estimatedBytes(handle) >= 4L);
        }
    }
}
