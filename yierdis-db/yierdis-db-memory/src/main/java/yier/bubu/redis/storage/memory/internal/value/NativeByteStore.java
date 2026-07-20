package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;

import java.util.Objects;

public final class NativeByteStore {
    private final NativeAllocator allocator;
    private final NativeObjectKind defaultKind;
    private long nativeBytes;

    public NativeByteStore(NativeAllocator allocator, NativeObjectKind defaultKind) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.defaultKind = Objects.requireNonNull(defaultKind, "defaultKind");
    }

    public NativeHandle store(byte[] bytes) {
        return store(bytes, defaultKind);
    }

    public long storeRaw(byte[] bytes) {
        return storeRaw(bytes, defaultKind);
    }

    public NativeHandle store(byte[] bytes, NativeObjectKind kind) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(kind, "kind");
        NativeHandle handle = allocator.allocate(kind, bytes.length);
        boolean written = false;
        try {
            if (bytes.length > 0) {
                try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
                    view.setBytes(0, bytes, 0, bytes.length);
                }
            }
            nativeBytes += allocatedBytes(handle);
            written = true;
            return handle;
        } finally {
            if (!written) {
                allocator.free(handle);
            }
        }
    }

    public long storeRaw(byte[] bytes, NativeObjectKind kind) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(kind, "kind");
        long rawHandle = allocator.allocateRaw(kind, bytes.length);
        boolean written = false;
        try {
            if (bytes.length > 0) {
                try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
                    view.setBytes(0, bytes, 0, bytes.length);
                }
            }
            nativeBytes += allocatedBytesRaw(rawHandle);
            written = true;
            return rawHandle;
        } finally {
            if (!written) {
                allocator.freeRaw(rawHandle);
            }
        }
    }

    /**
     * 分配由调用方自行编码的连续 native block，并把实际 capacity 计入当前 store。
     */
    long allocateRawBlock(NativeObjectKind kind, int size) {
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        long rawHandle = allocator.allocateRaw(kind, size);
        boolean accounted = false;
        try {
            nativeBytes += allocatedBytesRaw(rawHandle);
            accounted = true;
            return rawHandle;
        } finally {
            if (!accounted) {
                allocator.freeRaw(rawHandle);
            }
        }
    }

    /**
     * 调整连续 block 的逻辑大小；稳定句柄不转移 ownership，store 只修正 capacity 计量。
     */
    long reallocRawBlock(long rawHandle, int newSize, NativeReallocPolicy policy) {
        NativeHandle.requireValidRaw(rawHandle);
        if (NativeHandle.isNull(rawHandle)) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        Objects.requireNonNull(policy, "policy");
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0");
        }
        int oldAllocatedBytes = allocatedBytesRaw(rawHandle);
        long resizedRawHandle = allocator.reallocRaw(rawHandle, newSize, policy);
        if (resizedRawHandle != rawHandle) {
            throw new IllegalStateException("native realloc changed stable handle");
        }
        int nextAllocatedBytes = allocatedBytesRaw(rawHandle);
        nativeBytes += (long) nextAllocatedBytes - oldAllocatedBytes;
        return resizedRawHandle;
    }

    public void release(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        releaseRaw(handle.raw());
    }

    public void releaseRaw(long rawHandle) {
        int len = allocatedBytesRaw(rawHandle);
        allocator.freeRaw(rawHandle);
        nativeBytes -= len;
    }

    void forget(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        forgetRaw(handle.raw());
    }

    void forgetRaw(long rawHandle) {
        nativeBytes -= allocatedBytesRaw(rawHandle);
    }

    void adopt(NativeHandle handle, int retainedBytes) {
        Objects.requireNonNull(handle, "handle");
        adoptRaw(handle.raw(), retainedBytes);
    }

    void adoptRaw(long rawHandle, int retainedBytes) {
        NativeHandle.requireValidRaw(rawHandle);
        if (NativeHandle.isNull(rawHandle)) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        nativeBytes += retainedBytes;
    }

    public byte[] toByteArray(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return toByteArrayRaw(handle.raw());
    }

    public byte[] toByteArrayRaw(long rawHandle) {
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            if (len == 0) {
                return new byte[0];
            }
            byte[] out = new byte[len];
            view.getBytes(0, out, 0, len);
            return out;
        }
    }

    byte[] toByteArrayRaw(long rawHandle, int offset, int length) {
        validateSlice(rawHandle, offset, length);
        if (length == 0) {
            return new byte[0];
        }
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            byte[] out = new byte[length];
            view.getBytes(offset, out, 0, length);
            return out;
        }
    }

    void copyRawToArray(long rawHandle, int offset, byte[] target, int targetOffset, int length) {
        Objects.requireNonNull(target, "target");
        if (targetOffset < 0 || length < 0 || targetOffset > target.length - length) {
            throw new IndexOutOfBoundsException();
        }
        validateSlice(rawHandle, offset, length);
        if (length == 0) {
            return;
        }
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            view.getBytes(offset, target, targetOffset, length);
        }
    }

    public boolean equalsBytes(NativeHandle handle, byte[] bytes) {
        Objects.requireNonNull(handle, "handle");
        return equalsBytesRaw(handle.raw(), bytes);
    }

    public boolean equalsBytesRaw(long rawHandle, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            return len == bytes.length && view.contentEquals(0, bytes, 0, len);
        }
    }

    public boolean equalsBytes(NativeHandle handle, BytesView view) {
        Objects.requireNonNull(handle, "handle");
        return equalsBytesRaw(handle.raw(), view);
    }

    public boolean equalsBytesRaw(long rawHandle, BytesView view) {
        Objects.requireNonNull(view, "view");
        try (NativeObjectView nativeView = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int len = nativeView.size();
            if (len != view.length()) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (nativeView.getByte(i) != view.getByte(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    public int length(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return lengthRaw(handle.raw());
    }

    public int lengthRaw(long rawHandle) {
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    public int hashBytes(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return hashBytesRaw(handle.raw());
    }

    public int hashBytesRaw(long rawHandle) {
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int hash = 1;
            int len = view.size();
            for (int i = 0; i < len; i++) {
                hash = 31 * hash + (view.getByte(i) & 0xFF);
            }
            return hash;
        }
    }

    int sipHashRaw(long rawHandle, HashSeed seed) {
        Objects.requireNonNull(seed, "seed");
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return SipHash24.foldToInt(SipHash24.hash(seed, view));
        }
    }

    public int compareLex(NativeHandle left, byte[] right) {
        Objects.requireNonNull(left, "left");
        return compareLexRaw(left.raw(), right);
    }

    public int compareLexRaw(long leftRawHandle, byte[] right) {
        Objects.requireNonNull(right, "right");
        try (NativeObjectView leftView = allocator.resolveRaw(leftRawHandle, NativeAccessMode.READ_ONLY)) {
            return compareLex(leftView, right);
        }
    }

    public int compareLex(NativeHandle left, NativeHandle right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        return compareLexRaw(left.raw(), right.raw());
    }

    public int compareLexRaw(long leftRawHandle, long rightRawHandle) {
        try (NativeObjectView leftView = allocator.resolveRaw(leftRawHandle, NativeAccessMode.READ_ONLY);
             NativeObjectView rightView = allocator.resolveRaw(rightRawHandle, NativeAccessMode.READ_ONLY)) {
            int min = Math.min(leftView.size(), rightView.size());
            for (int i = 0; i < min; i++) {
                int av = leftView.getByte(i) & 0xFF;
                int bv = rightView.getByte(i) & 0xFF;
                if (av != bv) {
                    return Integer.compare(av, bv);
                }
            }
            return Integer.compare(leftView.size(), rightView.size());
        }
    }

    public int allocatedBytes(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return allocatedBytesRaw(handle.raw());
    }

    public int allocatedBytesRaw(long rawHandle) {
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    public BytesSlice slice(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return new NativeBytesSlice(allocator, handle, 0, length(handle));
    }

    BytesSlice sliceRaw(long rawHandle, int offset, int length) {
        validateSlice(rawHandle, offset, length);
        return new NativeBytesSlice(allocator, NativeHandle.fromRaw(rawHandle), offset, length);
    }

    public BulkStringValue retainedValue(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return retainedValueRaw(handle.raw());
    }

    BulkStringValue retainedValue(NativeHandle handle, int offset, int length) {
        Objects.requireNonNull(handle, "handle");
        return retainedValueRaw(handle.raw(), offset, length);
    }

    public BulkStringValue retainedValueRaw(long rawHandle) {
        return retainedValueRaw(rawHandle, 0, lengthRaw(rawHandle));
    }

    BulkStringValue retainedValueRaw(long rawHandle, int offset, int length) {
        validateSlice(rawHandle, offset, length);
        long retainedBytes = allocatedBytesRaw(rawHandle);
        allocator.pinRaw(rawHandle);
        boolean transferred = false;
        try {
            NativeHandle handle = NativeHandle.fromRaw(rawHandle);
            BulkStringValue value = BulkStringValue.owned(
                    NativeBytesSlice.retained(allocator, handle, offset, length),
                    length,
                    retainedBytes,
                    () -> allocator.unpinRaw(rawHandle)
            );
            transferred = true;
            return value;
        } finally {
            if (!transferred) {
                allocator.unpinRaw(rawHandle);
            }
        }
    }

    public long nativeBytes() {
        return nativeBytes;
    }

    NativeAllocator allocator() {
        return allocator;
    }

    private static int compareLex(NativeObjectView leftView, byte[] right) {
        int min = Math.min(leftView.size(), right.length);
        for (int i = 0; i < min; i++) {
            int av = leftView.getByte(i) & 0xFF;
            int bv = right[i] & 0xFF;
            if (av != bv) {
                return Integer.compare(av, bv);
            }
        }
        return Integer.compare(leftView.size(), right.length);
    }

    private void validateSlice(long rawHandle, int offset, int length) {
        NativeHandle.requireValidRaw(rawHandle);
        if (NativeHandle.isNull(rawHandle)) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("slice offset and length must be >= 0");
        }
        int size = lengthRaw(rawHandle);
        if (offset > size || length > size - offset) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + ", len=" + length + ", size=" + size
            );
        }
    }
}
