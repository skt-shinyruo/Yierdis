package yier.bubu.redis.db.key;

// OffHeapKeyHandle：基于 off-heap (address+len) 的 KeyHandle 实现，提供稳定的 bytes view 与 equality 语义。

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;

import java.util.Objects;

final class OffHeapKeyHandle implements KeyHandle {
    private final YierdisOffHeapAddressAllocator allocator;
    private final long address;
    private final int len;
    private final int dictHash;
    private final int contentHash;

    OffHeapKeyHandle(YierdisOffHeapAddressAllocator allocator, long address, int len, int dictHash) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (len > 0 && address == 0) {
            throw new IllegalArgumentException("address must be != 0 when len > 0");
        }
        this.address = address;
        this.len = len;
        this.dictHash = dictHash;
        this.contentHash = hashBytesView(this, len);
    }

    @Override
    public int dictHash() {
        return dictHash;
    }

    @Override
    public int len() {
        return len;
    }

    @Override
    public byte byteAt(int index) {
        if (index < 0 || index >= len) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + len);
        }
        return allocator.getByte(address + index);
    }

    @Override
    public int hashCode() {
        return contentHash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KeyHandle other)) {
            return false;
        }
        if (other.len() != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (byteAt(i) != other.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static int hashBytesView(KeyHandle view, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + view.byteAt(i);
        }
        return h;
    }

    YierdisOffHeapAddressAllocator allocatorUnsafe() {
        return allocator;
    }

    long addressUnsafe() {
        return address;
    }
}
