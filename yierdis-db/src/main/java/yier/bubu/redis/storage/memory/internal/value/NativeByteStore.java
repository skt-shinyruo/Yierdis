package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;

public final class NativeByteStore {
    private final StableMemoryBackend backend;
    private final NativeObjectKind defaultKind;
    private long nativeBytes;

    public NativeByteStore(StableMemoryBackend backend, NativeObjectKind defaultKind) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.defaultKind = Objects.requireNonNull(defaultKind, "defaultKind");
    }

    public NativeHandle store(byte[] bytes) {
        return store(bytes, defaultKind);
    }

    public NativeHandle store(byte[] bytes, NativeObjectKind kind) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(kind, "kind");
        NativeHandle handle = backend.allocate(kind, bytes.length);
        boolean written = false;
        try {
            if (bytes.length > 0) {
                try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE)) {
                    view.setBytes(0, bytes, 0, bytes.length);
                }
            }
            nativeBytes += allocatedBytes(handle);
            written = true;
            return handle;
        } finally {
            if (!written) {
                backend.free(handle);
            }
        }
    }

    NativeHandle allocateBlock(NativeObjectKind kind, int size) {
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        NativeHandle handle = backend.allocate(kind, size);
        boolean accounted = false;
        try {
            nativeBytes += allocatedBytes(handle);
            accounted = true;
            return handle;
        } finally {
            if (!accounted) {
                backend.free(handle);
            }
        }
    }

    NativeHandle reallocateBlock(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        requireLiveHandle(handle);
        Objects.requireNonNull(policy, "policy");
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0");
        }
        int oldAllocatedBytes = allocatedBytes(handle);
        NativeHandle resized = backend.reallocate(handle, newSize, policy);
        if (!handle.equals(resized)) {
            throw new IllegalStateException("native realloc changed stable handle");
        }
        nativeBytes += (long) allocatedBytes(handle) - oldAllocatedBytes;
        return handle;
    }

    public void release(NativeHandle handle) {
        requireLiveHandle(handle);
        int length = allocatedBytes(handle);
        backend.free(handle);
        nativeBytes -= length;
    }

    void forget(NativeHandle handle) {
        requireLiveHandle(handle);
        nativeBytes -= allocatedBytes(handle);
    }

    void adopt(NativeHandle handle, int retainedBytes) {
        requireLiveHandle(handle);
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        nativeBytes += retainedBytes;
    }

    public byte[] toByteArray(NativeHandle handle) {
        return toByteArray(handle, 0, length(handle));
    }

    byte[] toByteArray(NativeHandle handle, int offset, int length) {
        validateSlice(handle, offset, length);
        byte[] out = new byte[length];
        if (length == 0) {
            return out;
        }
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_ONLY)) {
            view.getBytes(offset, out, 0, length);
        }
        return out;
    }

    void copyToArray(NativeHandle handle, int offset, byte[] target, int targetOffset, int length) {
        Objects.requireNonNull(target, "target");
        if (targetOffset < 0 || length < 0 || targetOffset > target.length - length) {
            throw new IndexOutOfBoundsException();
        }
        validateSlice(handle, offset, length);
        if (length == 0) {
            return;
        }
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_ONLY)) {
            view.getBytes(offset, target, targetOffset, length);
        }
    }

    public boolean equalsBytes(NativeHandle handle, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            return view.size() == bytes.length && view.contentEquals(0, bytes, 0, bytes.length);
        }
    }

    public boolean equalsBytes(NativeHandle handle, BytesView bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            if (view.size() != bytes.length()) {
                return false;
            }
            for (int index = 0; index < view.size(); index++) {
                if (view.getByte(index) != bytes.getByte(index)) {
                    return false;
                }
            }
            return true;
        }
    }

    public int length(NativeHandle handle) {
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    public int hashBytes(NativeHandle handle) {
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            int hash = 1;
            for (int index = 0; index < view.size(); index++) {
                hash = 31 * hash + (view.getByte(index) & 0xff);
            }
            return hash;
        }
    }

    int sipHash(NativeHandle handle, HashSeed seed) {
        Objects.requireNonNull(seed, "seed");
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            return SipHash24.foldToInt(SipHash24.hash(seed, view));
        }
    }

    public int compareLex(NativeHandle left, byte[] right) {
        Objects.requireNonNull(right, "right");
        try (NativeObjectView view = backend.resolve(requireLiveHandle(left), NativeAccessMode.READ_ONLY)) {
            return compareLex(view, right);
        }
    }

    public int compareLex(NativeHandle left, NativeHandle right) {
        try (NativeObjectView leftView = backend.resolve(requireLiveHandle(left), NativeAccessMode.READ_ONLY);
             NativeObjectView rightView = backend.resolve(requireLiveHandle(right), NativeAccessMode.READ_ONLY)) {
            int limit = Math.min(leftView.size(), rightView.size());
            for (int index = 0; index < limit; index++) {
                int leftByte = leftView.getByte(index) & 0xff;
                int rightByte = rightView.getByte(index) & 0xff;
                if (leftByte != rightByte) {
                    return Integer.compare(leftByte, rightByte);
                }
            }
            return Integer.compare(leftView.size(), rightView.size());
        }
    }

    public int allocatedBytes(NativeHandle handle) {
        try (NativeObjectView view = backend.resolve(requireLiveHandle(handle), NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    public BytesSlice slice(NativeHandle handle) {
        return slice(handle, 0, length(handle));
    }

    BytesSlice slice(NativeHandle handle, int offset, int length) {
        validateSlice(handle, offset, length);
        return new NativeBytesSlice(backend, handle, offset, length);
    }

    public ByteValue retainedValue(NativeHandle handle) {
        return retainedValue(handle, 0, length(handle));
    }

    ByteValue retainedValue(NativeHandle handle, int offset, int length) {
        validateSlice(handle, offset, length);
        long retainedBytes = allocatedBytes(handle);
        backend.pin(handle);
        boolean transferred = false;
        try {
            ByteValue value = ByteValue.owned(
                    NativeBytesSlice.retained(backend, handle, offset, length),
                    length,
                    retainedBytes,
                    () -> backend.unpin(handle)
            );
            transferred = true;
            return value;
        } finally {
            if (!transferred) {
                backend.unpin(handle);
            }
        }
    }

    public long nativeBytes() {
        return nativeBytes;
    }

    StableMemoryBackend backend() {
        return backend;
    }

    private static int compareLex(NativeObjectView leftView, byte[] right) {
        int limit = Math.min(leftView.size(), right.length);
        for (int index = 0; index < limit; index++) {
            int leftByte = leftView.getByte(index) & 0xff;
            int rightByte = right[index] & 0xff;
            if (leftByte != rightByte) {
                return Integer.compare(leftByte, rightByte);
            }
        }
        return Integer.compare(leftView.size(), right.length);
    }

    private NativeHandle requireLiveHandle(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        if (handle.isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        return handle;
    }

    private void validateSlice(NativeHandle handle, int offset, int length) {
        requireLiveHandle(handle);
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("slice offset and length must be >= 0");
        }
        int size = length(handle);
        if (offset > size || length > size - offset) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + ", len=" + length + ", size=" + size
            );
        }
    }
}
