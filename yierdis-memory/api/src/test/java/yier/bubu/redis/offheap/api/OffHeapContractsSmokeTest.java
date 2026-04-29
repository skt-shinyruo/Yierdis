package yier.bubu.redis.offheap.api;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesSource;

public class OffHeapContractsSmokeTest {
    @Test
    public void offHeapSliceExtendsNeutralBytesSlice() {
        Assert.assertTrue(BytesSlice.class.isAssignableFrom(OffHeapSlice.class));
    }

    @Test
    public void allocatorAllocatesBufAndCanSlice() {
        OffHeapAllocator allocator = new OffHeapAllocator() {
            private long usedBytes;

            @Override
            public OffHeapBuf allocate(int capacity) {
                if (capacity <= 0) {
                    throw new IllegalArgumentException("capacity must be > 0");
                }
                usedBytes += capacity;
                return new InMemoryBuf(capacity);
            }

            @Override
            public long usedBytes() {
                return usedBytes;
            }

            @Override
            public long maxBytes() {
                return 0;
            }

            @Override
            public void close() {
            }
        };

        OffHeapBuf buf = allocator.allocate(8);
        Assert.assertNotNull(buf);
        Assert.assertEquals(8, buf.capacity());

        OffHeapSlice slice = buf.slice(0, 4);
        Assert.assertNotNull(slice);
        Assert.assertEquals(4, slice.length());
    }

    private static final class InMemoryBuf implements OffHeapBuf {
        private final byte[] data;

        private InMemoryBuf(int capacity) {
            this.data = new byte[Math.max(0, capacity)];
        }

        @Override
        public int capacity() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void setByte(int index, byte value) {
            data[index] = value;
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            System.arraycopy(src, srcOff, data, index, len);
        }

        @Override
        public void setBytes(int index, BytesSource src, int srcIndex, int len) {
            if (src == null) {
                throw new IllegalArgumentException("src must not be null");
            }
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            for (int i = 0; i < len; i++) {
                data[index + i] = src.getByte(srcIndex + i);
            }
        }

        @Override
        public OffHeapSlice slice(int index, int len) {
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            return new InMemorySlice(data, index, len);
        }

        @Override
        public void close() {
        }
    }

    private static final class InMemorySlice implements OffHeapSlice {
        private final byte[] data;
        private final int offset;
        private final int length;

        private InMemorySlice(byte[] data, int offset, int length) {
            if (data == null) {
                throw new IllegalArgumentException("data must not be null");
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be >= 0");
            }
            this.data = data;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte getByte(int index) {
            return data[offset + index];
        }

        @Override
        public void writeTo(BytesSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            out.writeBytes(data, offset, length);
        }
    }
}

