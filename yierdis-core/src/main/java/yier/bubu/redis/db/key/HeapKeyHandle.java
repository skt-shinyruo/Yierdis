package yier.bubu.redis.db.key;

// HeapKeyHandle：基于 heap byte[] 的 KeyHandle 实现（不复制 bytes；调用方需保证 bytes 不被修改）。

import java.util.Arrays;
import java.util.Objects;

final class HeapKeyHandle implements KeyHandle {
    private final byte[] keyBytes;
    private final int dictHash;
    private final int contentHash;

    HeapKeyHandle(byte[] keyBytes, int dictHash) {
        this.keyBytes = Objects.requireNonNull(keyBytes, "keyBytes");
        this.dictHash = dictHash;
        this.contentHash = Arrays.hashCode(keyBytes);
    }

    @Override
    public int dictHash() {
        return dictHash;
    }

    @Override
    public int len() {
        return keyBytes.length;
    }

    @Override
    public byte byteAt(int index) {
        if (index < 0 || index >= keyBytes.length) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + keyBytes.length);
        }
        return keyBytes[index];
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
        int len = keyBytes.length;
        if (other.len() != len) {
            return false;
        }
        if (obj instanceof HeapKeyHandle otherHeap) {
            return Arrays.equals(this.keyBytes, otherHeap.keyBytes);
        }
        for (int i = 0; i < len; i++) {
            if (keyBytes[i] != other.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    byte[] bytesUnsafe() {
        return keyBytes;
    }
}
