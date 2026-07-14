package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.result.BulkStringValue;

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

    public void release(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        int len = allocatedBytes(handle);
        allocator.free(handle);
        nativeBytes -= len;
    }

    void forget(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        nativeBytes -= allocatedBytes(handle);
    }

    public byte[] toByteArray(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            if (len == 0) {
                return new byte[0];
            }
            byte[] out = new byte[len];
            view.getBytes(0, out, 0, len);
            return out;
        }
    }

    public boolean equalsBytes(NativeHandle handle, byte[] bytes) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(bytes, "bytes");
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            if (len != bytes.length) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (view.getByte(i) != bytes[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    public boolean equalsBytes(NativeHandle handle, BytesView view) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(view, "view");
        try (NativeObjectView nativeView = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
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
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    public int hashBytes(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            int hash = 1;
            int len = view.size();
            for (int i = 0; i < len; i++) {
                hash = 31 * hash + (view.getByte(i) & 0xFF);
            }
            return hash;
        }
    }

    public int compareLex(NativeHandle left, byte[] right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        try (NativeObjectView leftView = allocator.resolve(left, NativeAccessMode.READ_ONLY)) {
            return compareLex(leftView, right);
        }
    }

    public int compareLex(NativeHandle left, NativeHandle right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        try (NativeObjectView leftView = allocator.resolve(left, NativeAccessMode.READ_ONLY);
             NativeObjectView rightView = allocator.resolve(right, NativeAccessMode.READ_ONLY)) {
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
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    public BytesSlice slice(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        return new NativeBytesSlice(allocator, handle, 0, length(handle));
    }

    public BulkStringValue retainedValue(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        int length = length(handle);
        long retainedBytes = allocatedBytes(handle);
        allocator.pin(handle);
        boolean transferred = false;
        try {
            BulkStringValue value = BulkStringValue.owned(
                    NativeBytesSlice.retained(allocator, handle, 0, length),
                    length,
                    retainedBytes,
                    () -> allocator.unpin(handle)
            );
            transferred = true;
            return value;
        } finally {
            if (!transferred) {
                allocator.unpin(handle);
            }
        }
    }

    public long nativeBytes() {
        return nativeBytes;
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
}
