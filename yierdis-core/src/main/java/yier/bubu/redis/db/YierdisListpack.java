package yier.bubu.redis.db;

import yier.bubu.redis.protocol.ReplySink;

import java.util.Arrays;

/**
 * A minimal listpack-like container: entries are stored in a single contiguous byte buffer.
 * <p>
 * This is a Java approximation of Redis' listpack/quicklist-node idea. The exact encoding is internal and not
 * intended to be compatible with Redis' listpack binary format.
 * <p>
 * Entry encoding:
 * <ul>
 *   <li>{@code varint(len+1)} followed by {@code len} bytes</li>
 *   <li>{@code 0} represents a {@code null} entry (to preserve null vs empty)</li>
 * </ul>
 */
final class YierdisListpack {
    private byte[] data = new byte[0];
    private int usedBytes = 0;
    private int size = 0;
    private int rawBytes = 0;

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    int encodedBytes() {
        return usedBytes;
    }

    int allocatedBytes() {
        return data.length;
    }

    int rawBytesSize() {
        return rawBytes;
    }

    void clear() {
        data = new byte[0];
        usedBytes = 0;
        size = 0;
        rawBytes = 0;
    }

    void addLast(byte[] value) {
        insertAt(size, value);
    }

    void addFirst(byte[] value) {
        insertAt(0, value);
    }

    void addLastSlice(byte[] src, int srcOff, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (srcOff < 0 || srcOff + len > src.length) {
            throw new IndexOutOfBoundsException();
        }

        int headerValue = len + 1;
        int headerBytes = varIntSize(headerValue);
        int entryBytes = headerBytes + len;
        ensureCapacity(usedBytes + entryBytes);

        int p = writeVarInt(data, usedBytes, headerValue);
        if (len > 0) {
            System.arraycopy(src, srcOff, data, p, len);
        }
        usedBytes += entryBytes;
        rawBytes += len;
        size++;
    }

    void insertAt(int index, byte[] value) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException();
        }

        int len = value == null ? -1 : value.length;
        int rawLen = Math.max(0, len);
        int headerValue = len < 0 ? 0 : len + 1;
        int headerBytes = varIntSize(headerValue);
        int entryBytes = headerBytes + rawLen;

        int insertOffset = index == size ? usedBytes : offsetOfIndex(index);
        ensureCapacity(usedBytes + entryBytes);

        int move = usedBytes - insertOffset;
        if (move > 0) {
            System.arraycopy(data, insertOffset, data, insertOffset + entryBytes, move);
        }

        int p = writeVarInt(data, insertOffset, headerValue);
        if (rawLen > 0) {
            System.arraycopy(value, 0, data, p, len);
        }

        usedBytes += entryBytes;
        rawBytes += rawLen;
        size++;
    }

    byte[] removeFirst() {
        return removeAt(0);
    }

    byte[] removeLast() {
        return removeAt(size - 1);
    }

    byte[] removeAt(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }

        int off = offsetOfIndex(index);
        Header h = readHeader(off);
        byte[] out = h.len < 0 ? null : Arrays.copyOfRange(data, h.dataOffset, h.dataOffset + h.len);

        int entryBytes = h.totalBytes;
        int tailOff = off + entryBytes;
        int move = usedBytes - tailOff;
        if (move > 0) {
            System.arraycopy(data, tailOff, data, off, move);
        }
        usedBytes -= entryBytes;
        rawBytes -= Math.max(0, h.len);
        size--;
        return out;
    }

    byte[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        Header h = readHeader(offsetOfIndex(index));
        if (h.len < 0) {
            return null;
        }
        return Arrays.copyOfRange(data, h.dataOffset, h.dataOffset + h.len);
    }

    boolean equalsAt(int index, byte[] other) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        Header h = readHeader(offsetOfIndex(index));
        if (h.len < 0) {
            return other == null;
        }
        if (other == null || other.length != h.len) {
            return false;
        }
        for (int i = 0; i < h.len; i++) {
            if (data[h.dataOffset + i] != other[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Replace the entry at {@code index} with {@code value}.
     */
    void set(int index, byte[] value) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int off = offsetOfIndex(index);
        Header old = readHeader(off);

        int newLen = value == null ? -1 : value.length;
        int oldRawLen = Math.max(0, old.len);
        int newRawLen = Math.max(0, newLen);
        int newHeaderValue = newLen < 0 ? 0 : newLen + 1;
        int newHeaderBytes = varIntSize(newHeaderValue);
        int newEntryBytes = newHeaderBytes + newRawLen;

        if (newEntryBytes == old.totalBytes) {
            int p = writeVarInt(data, off, newHeaderValue);
            if (newRawLen > 0) {
                System.arraycopy(value, 0, data, p, newLen);
            }
            rawBytes += newRawLen - oldRawLen;
            return;
        }

        int delta = newEntryBytes - old.totalBytes;
        ensureCapacity(usedBytes + delta);

        int tailOff = off + old.totalBytes;
        int move = usedBytes - tailOff;
        if (move > 0) {
            System.arraycopy(data, tailOff, data, tailOff + delta, move);
        }

        int p = writeVarInt(data, off, newHeaderValue);
        if (newRawLen > 0) {
            System.arraycopy(value, 0, data, p, newLen);
        }
        usedBytes += delta;
        rawBytes += newRawLen - oldRawLen;
    }

    /**
     * Returns the index of the first entry equal to {@code needle}, or -1 if not found.
     */
    int indexOf(byte[] needle) {
        if (size == 0) {
            return -1;
        }
        int idx = 0;
        int off = 0;
        while (idx < size) {
            Header h = readHeader(off);
            if (h.len < 0) {
                if (needle == null) {
                    return idx;
                }
            } else if (needle != null && needle.length == h.len) {
                boolean eq = true;
                for (int i = 0; i < h.len; i++) {
                    if (data[h.dataOffset + i] != needle[i]) {
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

    Cursor cursor() {
        return new Cursor(this);
    }

    private int offsetOfIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        int idx = 0;
        int off = 0;
        while (idx < index) {
            Header h = readHeader(off);
            off += h.totalBytes;
            idx++;
        }
        return off;
    }

    private Header readHeader(int offset) {
        int p = offset;
        int headerValue = readVarInt(data, p, usedBytes);
        int headerBytes = varIntSize(headerValue);
        if (headerBytes <= 0 || p + headerBytes > usedBytes) {
            throw new IllegalStateException("corrupt listpack header");
        }

        int len = headerValue == 0 ? -1 : headerValue - 1;
        int dataOff = offset + headerBytes;
        int total = headerBytes + Math.max(0, len);
        if (dataOff + Math.max(0, len) > usedBytes) {
            throw new IllegalStateException("corrupt listpack entry");
        }
        return new Header(len, dataOff, total);
    }

    private void ensureCapacity(int desired) {
        if (desired <= data.length) {
            return;
        }
        int next = Math.max(32, data.length);
        while (next < desired) {
            int n = next < 1024 * 1024 ? (next << 1) : (next + 1024 * 1024);
            if (n <= next) {
                next = desired;
                break;
            }
            next = n;
        }
        data = Arrays.copyOf(data, next);
    }

    private static int varIntSize(int value) {
        // unsigned base-128 varint for non-negative ints.
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }

    private static int writeVarInt(byte[] dst, int offset, int value) {
        int v = value;
        int p = offset;
        while ((v & ~0x7F) != 0) {
            dst[p++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        dst[p++] = (byte) (v & 0x7F);
        return p;
    }

    private static int readVarInt(byte[] src, int offset, int limit) {
        int result = 0;
        int shift = 0;
        int p = offset;
        while (p < limit) {
            int b = src[p++] & 0xFF;
            result |= (b & 0x7F) << shift;
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

    static final class Cursor {
        private final YierdisListpack owner;
        private int index;
        private int offset;
        private Header current;

        Cursor(YierdisListpack owner) {
            this.owner = owner;
        }

        boolean next() {
            if (index >= owner.size) {
                current = null;
                return false;
            }
            current = owner.readHeader(offset);
            offset += current.totalBytes;
            index++;
            return true;
        }

        int index() {
            return index - 1;
        }

        boolean isNull() {
            return current != null && current.len < 0;
        }

        int length() {
            return current == null ? 0 : current.len;
        }

        byte[] toByteArray() {
            if (current == null || current.len < 0) {
                return null;
            }
            return Arrays.copyOfRange(owner.data, current.dataOffset, current.dataOffset + current.len);
        }

        void writeTo(ReplySink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            if (current == null) {
                throw new IllegalStateException("cursor not positioned");
            }
            if (current.len < 0) {
                out.bulkStringNull();
                return;
            }
            out.bulkString(owner.data, current.dataOffset, current.len);
        }

        void appendTo(YierdisListpack dst) {
            if (dst == null) {
                throw new IllegalArgumentException("dst must not be null");
            }
            if (current == null) {
                throw new IllegalStateException("cursor not positioned");
            }
            if (current.len < 0) {
                dst.addLast(null);
                return;
            }
            dst.addLastSlice(owner.data, current.dataOffset, current.len);
        }

        boolean equalsBytes(byte[] other) {
            if (current == null) {
                throw new IllegalStateException("cursor not positioned");
            }
            if (current.len < 0) {
                return other == null;
            }
            if (other == null || other.length != current.len) {
                return false;
            }
            for (int i = 0; i < current.len; i++) {
                if (owner.data[current.dataOffset + i] != other[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
