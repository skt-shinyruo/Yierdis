package yier.bubu.redis.db.memory.offheap;

import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.result.BulkStringSink;

/**
 * A minimal listpack-like container backed by {@link YierdisUnsafeOffHeapString}.
 * <p>
 * Encoding is intentionally NOT compatible with Redis listpack; it is an internal format:
 * <ul>
 *   <li>{@code varint(len+1)} followed by {@code len} bytes</li>
 *   <li>{@code 0} represents a {@code null} entry</li>
 * </ul>
 */
public final class YierdisUnsafeOffHeapListpack implements AutoCloseable {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final OffHeapAddressAllocator allocator;
    private YierdisUnsafeOffHeapString data;
    private int size;
    private int rawBytes;

    public YierdisUnsafeOffHeapListpack(OffHeapAddressAllocator allocator) {
        this.allocator = allocator;
        this.data = new YierdisUnsafeOffHeapString(allocator, 0);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int encodedBytes() {
        return data.length();
    }

    public int allocatedBytes() {
        return data.capacity();
    }

    public int rawBytesSize() {
        return rawBytes;
    }

    public void clear() {
        data.clear();
        size = 0;
        rawBytes = 0;
    }

    public void addLast(byte[] value) {
        insertAt(size, value);
    }

    public void addFirst(byte[] value) {
        insertAt(0, value);
    }

    public void insertAt(int index, byte[] value) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException();
        }

        int len = value == null ? -1 : value.length;
        int rawLen = Math.max(0, len);
        int headerValue = len < 0 ? 0 : len + 1;
        int headerBytes = varIntSize(headerValue);
        int entryBytes = headerBytes + rawLen;

        int usedBytes = data.length();
        int insertOffset = index == size ? usedBytes : offsetOfIndex(index);
        ensureCapacity(usedBytes + entryBytes);

        int move = usedBytes - insertOffset;
        if (move > 0) {
            memmove(insertOffset, insertOffset + entryBytes, move);
        }

        int p = writeVarInt(insertOffset, headerValue);
        if (rawLen > 0) {
            dataOverwrite(p, value, 0, rawLen);
        }

        data.setLength(usedBytes + entryBytes);
        rawBytes += rawLen;
        size++;
    }

    public byte[] removeFirst() {
        return removeAt(0);
    }

    public byte[] removeLast() {
        return removeAt(size - 1);
    }

    public byte[] removeAt(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }

        int usedBytes = data.length();
        int off = offsetOfIndex(index);
        Header h = readHeader(off, usedBytes);
        byte[] out = null;
        if (h.len >= 0) {
            out = new byte[h.len];
            data.getBytes(h.dataOffset, out, 0, h.len);
        }

        int entryBytes = h.totalBytes;
        int tailOff = off + entryBytes;
        int move = usedBytes - tailOff;
        if (move > 0) {
            memmove(tailOff, off, move);
        }
        data.setLength(usedBytes - entryBytes);
        rawBytes -= Math.max(0, h.len);
        size--;
        return out;
    }

    public byte[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int usedBytes = data.length();
        Header h = readHeader(offsetOfIndex(index), usedBytes);
        if (h.len < 0) {
            return null;
        }
        byte[] out = new byte[h.len];
        data.getBytes(h.dataOffset, out, 0, h.len);
        return out;
    }

    public boolean equalsAt(int index, byte[] other) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }

        int usedBytes = data.length();
        Header h = readHeader(offsetOfIndex(index), usedBytes);
        if (h.len < 0) {
            return other == null;
        }
        if (other == null || other.length != h.len) {
            return false;
        }
        for (int i = 0; i < h.len; i++) {
            if (data.getByte(h.dataOffset + i) != other[i]) {
                return false;
            }
        }
        return true;
    }

    public void set(int index, byte[] value) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int usedBytes = data.length();
        int off = offsetOfIndex(index);
        Header old = readHeader(off, usedBytes);

        int newLen = value == null ? -1 : value.length;
        int oldRawLen = Math.max(0, old.len);
        int newRawLen = Math.max(0, newLen);
        int newHeaderValue = newLen < 0 ? 0 : newLen + 1;
        int newHeaderBytes = varIntSize(newHeaderValue);
        int newEntryBytes = newHeaderBytes + newRawLen;

        if (newEntryBytes == old.totalBytes) {
            int p = writeVarInt(off, newHeaderValue);
            if (newRawLen > 0) {
                dataOverwrite(p, value, 0, newRawLen);
            }
            rawBytes += newRawLen - oldRawLen;
            return;
        }

        int delta = newEntryBytes - old.totalBytes;
        ensureCapacity(usedBytes + delta);

        int tailOff = off + old.totalBytes;
        int move = usedBytes - tailOff;
        if (move > 0) {
            memmove(tailOff, tailOff + delta, move);
        }

        int p = writeVarInt(off, newHeaderValue);
        if (newRawLen > 0) {
            dataOverwrite(p, value, 0, newRawLen);
        }
        data.setLength(usedBytes + delta);
        rawBytes += newRawLen - oldRawLen;
    }

    public int indexOf(byte[] needle) {
        if (size == 0) {
            return -1;
        }
        int idx = 0;
        int off = 0;
        int usedBytes = data.length();
        while (idx < size) {
            Header h = readHeader(off, usedBytes);
            if (h.len < 0) {
                if (needle == null) {
                    return idx;
                }
            } else if (needle != null && needle.length == h.len) {
                boolean eq = true;
                for (int i = 0; i < h.len; i++) {
                    if (data.getByte(h.dataOffset + i) != needle[i]) {
                        eq = false;
                        break;
                    }
                }
                if (eq) {
                    return idx;
                }
            }
            off += h.totalBytes;
            idx++;
        }
        return -1;
    }

    public Cursor cursor() {
        return new Cursor(this);
    }

    @Override
    public void close() {
        if (data != null) {
            data.close();
            data = null;
        }
    }

    private void ensureCapacity(int desired) {
        if (desired <= data.capacity()) {
            return;
        }
        data.ensureCapacity(desired);
    }

    private void dataOverwrite(int dstOff, byte[] src, int srcOff, int len) {
        if (len == 0) {
            return;
        }
        long dstAddr = data.dataAddress() + dstOff;
        allocator.copyMemory(src, srcOff, dstAddr, len);
    }

    private void memmove(int srcOff, int dstOff, int len) {
        if (len <= 0 || srcOff == dstOff) {
            return;
        }
        long base = data.dataAddress();
        long srcAddr = base + srcOff;
        long dstAddr = base + dstOff;

        byte[] scratch = TL_COPY_BUF.get();
        if (dstOff > srcOff) {
            int remaining = len;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                long from = srcAddr + (remaining - chunk);
                long to = dstAddr + (remaining - chunk);
                allocator.copyMemory(from, scratch, 0, chunk);
                allocator.copyMemory(scratch, 0, to, chunk);
                remaining -= chunk;
            }
            return;
        }

        int remaining = len;
        long from = srcAddr;
        long to = dstAddr;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            allocator.copyMemory(from, scratch, 0, chunk);
            allocator.copyMemory(scratch, 0, to, chunk);
            from += chunk;
            to += chunk;
            remaining -= chunk;
        }
    }

    private int offsetOfIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int idx = 0;
        int off = 0;
        int usedBytes = data.length();
        while (idx < index) {
            Header h = readHeader(off, usedBytes);
            off += h.totalBytes;
            idx++;
        }
        return off;
    }

    private int writeVarInt(int offset, int value) {
        int v = value;
        int p = offset;
        while ((v & ~0x7F) != 0) {
            dataOverwriteByte(p++, (byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        dataOverwriteByte(p++, (byte) (v & 0x7F));
        return p;
    }

    private void dataOverwriteByte(int index, byte value) {
        long addr = data.dataAddress() + index;
        allocator.putByte(addr, value);
    }

    private int readVarInt(int offset, int limit) {
        int result = 0;
        int shift = 0;
        int p = offset;
        while (p < limit) {
            int b = data.getByte(p++) & 0xff;
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalStateException("varint too long");
            }
        }
        throw new IllegalStateException("unterminated varint");
    }

    private static int varIntSize(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        int bytes = 1;
        int v = value;
        while ((v & ~0x7f) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }

    private Header readHeader(int offset, int limit) {
        int headerValue = readVarInt(offset, limit);
        int headerBytes = varIntSize(headerValue);
        if (headerValue == 0) {
            return new Header(-1, offset + headerBytes, headerBytes);
        }
        int len = headerValue - 1;
        return new Header(len, offset + headerBytes, headerBytes + len);
    }

    private static final class Header {
        final int len; // -1 for null
        final int dataOffset;
        final int totalBytes;

        private Header(int len, int dataOffset, int totalBytes) {
            this.len = len;
            this.dataOffset = dataOffset;
            this.totalBytes = totalBytes;
        }
    }

    public static final class Cursor {
        private final YierdisUnsafeOffHeapListpack owner;
        private int index;
        private int offset;
        private Header current;

        Cursor(YierdisUnsafeOffHeapListpack owner) {
            this.owner = owner;
        }

        public boolean next() {
            if (index >= owner.size) {
                current = null;
                return false;
            }
            int used = owner.data.length();
            Header h = owner.readHeader(offset, used);
            current = h;
            offset += h.totalBytes;
            index++;
            return true;
        }

        public boolean isNull() {
            Header h = current;
            return h != null && h.len < 0;
        }

        public int length() {
            Header h = current;
            if (h == null) {
                return 0;
            }
            return Math.max(0, h.len);
        }

        public boolean equalsBytes(byte[] other) {
            Header h = current;
            if (h == null) {
                return false;
            }
            if (h.len < 0) {
                return other == null;
            }
            if (other == null || other.length != h.len) {
                return false;
            }
            for (int i = 0; i < h.len; i++) {
                if (owner.data.getByte(h.dataOffset + i) != other[i]) {
                    return false;
                }
            }
            return true;
        }

        public byte[] toByteArray() {
            Header h = current;
            if (h == null) {
                return null;
            }
            if (h.len < 0) {
                return null;
            }
            byte[] out = new byte[h.len];
            owner.data.getBytes(h.dataOffset, out, 0, h.len);
            return out;
        }

        public void writeTo(BulkStringSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            Header h = current;
            if (h == null) {
                return;
            }
            if (h.len < 0) {
                out.bulkStringNull();
                return;
            }
            OffHeapSlice slice = owner.data.slice(h.dataOffset, h.len);
            out.bulkString(slice);
        }
    }
}
