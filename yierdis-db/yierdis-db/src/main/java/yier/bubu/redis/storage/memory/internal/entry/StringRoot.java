package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.memory.internal.value.NativeBytesSlice;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Arrays;
import java.util.Objects;

public final class StringRoot implements AutoCloseable {
    private static final int COPY_BUFFER_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_BYTES]);
    private static final BytesSlice EMPTY_SLICE = new HeapBackedBytesSlice(new byte[0]);

    private final StableMemoryBackend allocator;
    private boolean closed;

    public StringRoot(StableMemoryBackend allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    StableMemoryBackend allocator() {
        return allocator;
    }

    public ValueEncoding encoding(ValueHandle handle) {
        NativeHandle nativeHandle = requireStringHandle(handle);
        try (NativeObjectView ignored = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            return ValueEncoding.STRING_RAW;
        }
    }

    public boolean contains(ValueHandle handle) {
        ensureOpen();
        if (handle == null || handle.isNull()) {
            return false;
        }
        NativeHandle nativeHandle;
        try {
            nativeHandle = requireStringHandle(handle);
        } catch (IllegalArgumentException e) {
            return false;
        }
        try (NativeObjectView ignored = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            return true;
        } catch (StaleNativeHandleException e) {
            return false;
        }
    }

    public ValueHandle store(byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        NativeHandle nativeHandle = allocator.allocate(NativeObjectKind.STRING_BYTES, len);
        boolean ok = false;
        try {
            if (len > 0) {
                try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
                    view.setBytes(0, value, 0, len);
                }
            }
            ok = true;
            return new ValueHandle(nativeHandle);
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public ValueHandle store(BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        NativeHandle nativeHandle = allocator.allocate(NativeObjectKind.STRING_BYTES, len);
        boolean ok = false;
        try {
            if (len > 0) {
                try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
                    setBytes(view, 0, value, len);
                }
            }
            ok = true;
            return new ValueHandle(nativeHandle);
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public void overwrite(ValueHandle handle, byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        NativeHandle nativeHandle = requireStringHandle(handle);
        resizePreservingHandle(nativeHandle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, value, 0, len);
            }
        }
    }

    public void overwrite(ValueHandle handle, BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        NativeHandle nativeHandle = requireStringHandle(handle);
        resizePreservingHandle(nativeHandle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
                setBytes(view, 0, value, len);
            }
        }
    }

    public int append(ValueHandle handle, byte[] suffix) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        if (suffix == null || suffix.length == 0) {
            return length(nativeHandle);
        }
        int oldLen = length(nativeHandle);
        int nextLen = Math.addExact(oldLen, suffix.length);
        resizePreservingHandle(nativeHandle, nextLen);
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
            view.setBytes(oldLen, suffix, 0, suffix.length);
        }
        return nextLen;
    }

    public int append(ValueHandle handle, BytesSlice suffix) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        if (suffix == null || suffix.length() == 0) {
            return length(nativeHandle);
        }
        int oldLen = length(nativeHandle);
        int suffixLen = suffix.length();
        int nextLen = Math.addExact(oldLen, suffixLen);
        resizePreservingHandle(nativeHandle, nextLen);
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
            setBytes(view, oldLen, suffix, suffixLen);
        }
        return nextLen;
    }

    public byte byteAt(ValueHandle handle, int index) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            return view.getByte(index);
        }
    }

    public void setByteAt(ValueHandle handle, int index, byte value) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_WRITE)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            view.setByte(index, value);
        }
    }

    public BytesSlice slice(ValueHandle handle) {
        ensureOpen();
        byte[] copy = copy(requireStringHandle(handle));
        if (copy.length == 0) {
            return EMPTY_SLICE;
        }
        return new HeapBackedBytesSlice(copy);
    }

    public ByteValue retainedValue(ValueHandle handle) {
        return retainedValueWithCloseHook(handle, null);
    }

    public ByteValue retainedValueWithCloseHook(ValueHandle handle, Runnable closeHook) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        int payloadLength;
        long retainedBytes;
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            payloadLength = view.size();
            retainedBytes = view.capacity();
        }

        allocator.pin(nativeHandle);
        boolean ownershipTransferred = false;
        try {
            ByteValue value = ByteValue.owned(
                    NativeBytesSlice.retained(allocator, nativeHandle, 0, payloadLength),
                    payloadLength,
                    retainedBytes,
                    () -> {
                        allocator.unpin(nativeHandle);
                        if (closeHook != null) {
                            closeHook.run();
                        }
                    }
            );
            ownershipTransferred = true;
            return value;
        } finally {
            if (!ownershipTransferred) {
                allocator.unpin(nativeHandle);
            }
        }
    }

    public byte[] copy(ValueHandle handle) {
        ensureOpen();
        return copy(requireStringHandle(handle));
    }

    public int length(ValueHandle handle) {
        ensureOpen();
        return length(requireStringHandle(handle));
    }

    public long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        NativeHandle nativeHandle = requireStringHandle(handle);
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    public void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        allocator.free(requireStringHandle(handle));
    }

    public void clear() {
        ensureOpen();
        // StringRoot 不拥有共享 allocator 中 STRING_BYTES 的实例集合；DB 生命周期必须沿 entry graph 逐项释放。
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        RuntimeException failure = null;
        try {
            clear();
        } catch (RuntimeException e) {
            failure = e;
        }
        closed = true;
        if (failure != null) {
            throw failure;
        }
    }

    private NativeHandle requireStringHandle(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        NativeHandle nativeHandle = handle.nativeHandle();
        if (nativeHandle.isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        return nativeHandle;
    }

    private void resizePreservingHandle(NativeHandle nativeHandle, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        NativeHandle resizedHandle = allocator.reallocate(
                nativeHandle,
                len,
                NativeReallocPolicy.PRESERVE_PREFIX
        );
        if (!resizedHandle.equals(nativeHandle)) {
            throw new IllegalStateException("string realloc changed stable handle");
        }
    }

    private byte[] copy(NativeHandle nativeHandle) {
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            if (len == 0) {
                return new byte[0];
            }
            byte[] out = new byte[len];
            view.getBytes(0, out, 0, len);
            return out;
        }
    }

    private int length(NativeHandle nativeHandle) {
        try (NativeObjectView view = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    private static void setBytes(NativeObjectView view, int index, BytesSlice value, int len) {
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int srcOffset = 0;
        int dstOffset = index;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            value.getBytes(srcOffset, scratch, 0, chunk);
            view.setBytes(dstOffset, scratch, 0, chunk);
            srcOffset += chunk;
            dstOffset += chunk;
            remaining -= chunk;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("string root is closed");
        }
    }

    private static final class HeapBackedBytesSlice implements BytesSlice {
        private final byte[] bytes;

        private HeapBackedBytesSlice(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes").length == 0 ? bytes : Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            if (index < 0 || index >= bytes.length) {
                throw new IndexOutOfBoundsException();
            }
            return bytes[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            Objects.requireNonNull(dst, "dst");
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            if (index < 0 || dstOff < 0 || index + len > bytes.length || dstOff + len > dst.length) {
                throw new IndexOutOfBoundsException();
            }
            if (len > 0) {
                System.arraycopy(bytes, index, dst, dstOff, len);
            }
        }

        @Override
        public void writeTo(BytesSink out) {
            Objects.requireNonNull(out, "out");
            out.writeBytes(bytes, 0, bytes.length);
        }
    }
}
