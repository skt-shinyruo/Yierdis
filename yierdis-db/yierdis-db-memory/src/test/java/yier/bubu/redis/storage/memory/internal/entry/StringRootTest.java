package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Arrays;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public class StringRootTest {
    @Test
    public void stringRootDoesNotMirrorEveryLiveHandle() {
        Assert.assertFalse(Arrays.stream(StringRoot.class.getDeclaredFields())
                .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
        Assert.assertFalse(Arrays.stream(StringRoot.class.getDeclaredFields())
                .anyMatch(field -> NativeHandle.class.isAssignableFrom(field.getType())));
    }

    @Test
    public void stringRootUsesRawAllocatorPathForValueLifecycle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-raw-path");
             RawPathRecordingAllocator allocator = new RawPathRecordingAllocator(
                     new YierdisStableNativeAllocator(runtime, 32)
             );
             StringRoot root = new StringRoot(allocator)) {
            ValueHandle handle = root.store(new byte[] { 'a' });
            root.overwrite(handle, new byte[] { 'b', 'c' });
            Assert.assertEquals(3, root.append(handle, new byte[] { 'd' }));
            Assert.assertArrayEquals(new byte[] { 'b', 'c', 'd' }, root.copy(handle));

            try (BulkStringValue ignored = root.retainedValue(handle)) {
                Assert.assertEquals(3, ignored.payloadLength());
            }
            root.release(handle);

            Assert.assertEquals(1, allocator.allocateRawCalls());
            Assert.assertEquals(2, allocator.reallocRawCalls());
            Assert.assertEquals(1, allocator.freeRawCalls());
            Assert.assertEquals(1, allocator.pinRawCalls());
            Assert.assertEquals(1, allocator.unpinRawCalls());
            Assert.assertTrue(allocator.resolveRawCalls() >= 6);
        }
    }

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
            root.release(handle);
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
            root.release(handle);
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
            root.release(handle);
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
            root.release(handle);
        }
    }

    @Test
    public void appendPreservesStableValueHandleWhenReallocMovesObject() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-stable-append");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             StringRoot root = new StringRoot(allocator)) {
            ValueHandle handle = root.store(new byte[] { 'a' });
            long raw = handle.raw();

            byte[] suffix = new byte[64 * 1024];
            for (int i = 0; i < suffix.length; i++) {
                suffix[i] = 'b';
            }

            Assert.assertEquals(1 + suffix.length, root.append(handle, suffix));
            Assert.assertEquals(raw, handle.raw());
            Assert.assertEquals(1 + suffix.length, root.length(handle));
            Assert.assertEquals('a', root.byteAt(handle, 0));
            Assert.assertEquals('b', root.byteAt(handle, suffix.length));
            Assert.assertTrue(allocator.stats().reallocMovedCount() > 0L);
            root.release(handle);
        }
    }

    @Test
    public void releasedStringHandleFailsThroughAllocatorStaleHandleDetection() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-stale");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             StringRoot root = new StringRoot(allocator)) {
            ValueHandle handle = root.store(new byte[] { 'x' });
            root.release(handle);

            try {
                root.copy(handle);
                Assert.fail("expected stale string handle");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));
        }
    }

    @Test
    public void reusedStringSlotDoesNotReviveOldHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-generation");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             StringRoot root = new StringRoot(allocator)) {
            ValueHandle first = root.store(new byte[] { 'a' });
            root.release(first);

            ValueHandle second = root.store(new byte[] { 'b' });
            Assert.assertNotEquals(first.raw(), second.raw());
            Assert.assertArrayEquals(new byte[] { 'b' }, root.copy(second));

            try {
                root.copy(first);
                Assert.fail("expected old handle to remain stale");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("stale native handle"));
            }
            root.release(second);
        }
    }

    @Test
    public void allocatorRecognizesLiveStringHandleAcrossRootAdapters() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-foreign-handle");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             StringRoot firstRoot = new StringRoot(allocator);
             StringRoot secondRoot = new StringRoot(allocator)) {
            ValueHandle secondHandle = secondRoot.store(new byte[] { 'b' });

            Assert.assertArrayEquals(new byte[] { 'b' }, firstRoot.copy(secondHandle));
            firstRoot.release(secondHandle);
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));
        }
    }

    @Test
    public void emptyStringHandleCanGrowAndShrinkWithoutChangingHandle() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("string-root-empty");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32);
             StringRoot root = new StringRoot(allocator)) {
            ValueHandle handle = root.store(new byte[0]);
            long raw = handle.raw();

            Assert.assertEquals(0, root.length(handle));
            Assert.assertArrayEquals(new byte[0], root.copy(handle));

            Assert.assertEquals(3, root.append(handle, new byte[] { 'a', 'b', 'c' }));
            Assert.assertEquals(raw, handle.raw());
            Assert.assertArrayEquals(new byte[] { 'a', 'b', 'c' }, root.copy(handle));

            root.overwrite(handle, new byte[0]);
            Assert.assertEquals(raw, handle.raw());
            Assert.assertEquals(0, root.length(handle));
            Assert.assertArrayEquals(new byte[0], root.copy(handle));
            root.release(handle);
        }
    }
}
