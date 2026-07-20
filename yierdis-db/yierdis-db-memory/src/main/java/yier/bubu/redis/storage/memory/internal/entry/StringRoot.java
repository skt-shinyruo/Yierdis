package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.value.NativeBytesSlice;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Arrays;
import java.util.Objects;

public final class StringRoot implements TypeRoot {
    private static final int COPY_BUFFER_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_BYTES]);
    private static final byte[] ZERO_BUF = new byte[COPY_BUFFER_BYTES];
    private static final BytesSlice EMPTY_SLICE = new HeapBackedBytesSlice(new byte[0]);

    private final NativeAllocator allocator;
    private final boolean ownsAllocator;
    private boolean closed;

    public StringRoot(YierdisFfmMemoryRuntime runtime) {
        this(new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
    }

    public StringRoot(NativeAllocator allocator) {
        this(allocator, false);
    }

    private StringRoot(NativeAllocator allocator, boolean ownsAllocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
    }

    NativeAllocator allocator() {
        return allocator;
    }

    @Override
    public ValueType type() {
        return ValueType.STRING;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.STRING_RAW;
    }

    public ValueEncoding encoding(ValueHandle handle) {
        long rawHandle = requireStringRaw(handle);
        try (NativeObjectView ignored = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return ValueEncoding.STRING_RAW;
        }
    }

    public boolean contains(ValueHandle handle) {
        ensureOpen();
        if (handle == null || handle.isNull()) {
            return false;
        }
        long rawHandle;
        try {
            rawHandle = requireStringRaw(handle);
        } catch (IllegalArgumentException e) {
            return false;
        }
        try (NativeObjectView ignored = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return true;
        } catch (StaleNativeHandleException e) {
            return false;
        }
    }

    public ValueHandle store(byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        long rawHandle = allocator.allocateRaw(NativeObjectKind.STRING_BYTES, len);
        boolean ok = false;
        try {
            if (len > 0) {
                try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
                    view.setBytes(0, value, 0, len);
                }
            }
            ok = true;
            return ValueHandle.fromRaw(rawHandle);
        } finally {
            if (!ok) {
                allocator.freeRaw(rawHandle);
            }
        }
    }

    public ValueHandle store(BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        long rawHandle = allocator.allocateRaw(NativeObjectKind.STRING_BYTES, len);
        boolean ok = false;
        try {
            if (len > 0) {
                try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
                    setBytes(view, 0, value, len);
                }
            }
            ok = true;
            return ValueHandle.fromRaw(rawHandle);
        } finally {
            if (!ok) {
                allocator.freeRaw(rawHandle);
            }
        }
    }

    public void overwrite(ValueHandle handle, byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        long rawHandle = requireStringRaw(handle);
        resizePreservingHandle(rawHandle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, value, 0, len);
            }
        }
    }

    public void overwrite(ValueHandle handle, BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        long rawHandle = requireStringRaw(handle);
        resizePreservingHandle(rawHandle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
                setBytes(view, 0, value, len);
            }
        }
    }

    public int append(ValueHandle handle, byte[] suffix) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        if (suffix == null || suffix.length == 0) {
            return lengthRaw(rawHandle);
        }
        int oldLen = lengthRaw(rawHandle);
        int nextLen = Math.addExact(oldLen, suffix.length);
        resizePreservingHandle(rawHandle, nextLen);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
            view.setBytes(oldLen, suffix, 0, suffix.length);
        }
        return nextLen;
    }

    public int append(ValueHandle handle, BytesSlice suffix) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        if (suffix == null || suffix.length() == 0) {
            return lengthRaw(rawHandle);
        }
        int oldLen = lengthRaw(rawHandle);
        int suffixLen = suffix.length();
        int nextLen = Math.addExact(oldLen, suffixLen);
        resizePreservingHandle(rawHandle, nextLen);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
            setBytes(view, oldLen, suffix, suffixLen);
        }
        return nextLen;
    }

    public void ensureLength(ValueHandle handle, int requiredLen) {
        ensureOpen();
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        long rawHandle = requireStringRaw(handle);
        int oldLen = lengthRaw(rawHandle);
        if (requiredLen <= oldLen) {
            return;
        }
        resizePreservingHandle(rawHandle, requiredLen);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
            zeroFill(view, oldLen, requiredLen);
        }
    }

    public byte byteAt(ValueHandle handle, int index) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            return view.getByte(index);
        }
    }

    public void setByteAt(ValueHandle handle, int index, byte value) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_WRITE)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            view.setByte(index, value);
        }
    }

    public BytesSlice slice(ValueHandle handle) {
        ensureOpen();
        byte[] copy = copyRaw(requireStringRaw(handle));
        if (copy.length == 0) {
            return EMPTY_SLICE;
        }
        return new HeapBackedBytesSlice(copy);
    }

    public BulkStringValue retainedValue(ValueHandle handle) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        int payloadLength;
        long retainedBytes;
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            payloadLength = view.size();
            retainedBytes = view.capacity();
        }

        allocator.pinRaw(rawHandle);
        boolean ownershipTransferred = false;
        try {
            BulkStringValue value = BulkStringValue.owned(
                    NativeBytesSlice.retained(allocator, NativeHandle.fromRaw(rawHandle), 0, payloadLength),
                    payloadLength,
                    retainedBytes,
                    () -> allocator.unpinRaw(rawHandle)
            );
            ownershipTransferred = true;
            return value;
        } finally {
            if (!ownershipTransferred) {
                allocator.unpinRaw(rawHandle);
            }
        }
    }

    public byte[] copy(ValueHandle handle) {
        ensureOpen();
        return copyRaw(requireStringRaw(handle));
    }

    public int length(ValueHandle handle) {
        ensureOpen();
        return lengthRaw(requireStringRaw(handle));
    }

    @Override
    public long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        long rawHandle = requireStringRaw(handle);
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    @Override
    public void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        allocator.freeRaw(requireStringRaw(handle));
    }

    @Override
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
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        closed = true;
        if (failure != null) {
            throw failure;
        }
    }

    private long requireStringRaw(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        long rawHandle = handle.raw();
        NativeHandle.requireValidRaw(rawHandle);
        if (NativeHandle.isNull(rawHandle)) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        NativeObjectKind kind = NativeObjectKind.STRING_BYTES;
        if (NativeHandle.domainCode(rawHandle) != kind.domain().code()
                || NativeHandle.kindCode(rawHandle) != kind.code()) {
            throw new IllegalArgumentException("value handle is not string bytes: " + rawHandle);
        }
        return rawHandle;
    }

    private void resizePreservingHandle(long rawHandle, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        long resizedRawHandle = allocator.reallocRaw(rawHandle, len, NativeReallocPolicy.PRESERVE_PREFIX);
        if (resizedRawHandle != rawHandle) {
            throw new IllegalStateException("string realloc changed stable handle");
        }
    }

    private byte[] copyRaw(long rawHandle) {
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

    private int lengthRaw(long rawHandle) {
        try (NativeObjectView view = allocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
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

    private static void zeroFill(NativeObjectView view, int from, int toExclusive) {
        int remaining = toExclusive - from;
        int offset = from;
        while (remaining > 0) {
            int chunk = Math.min(remaining, ZERO_BUF.length);
            view.setBytes(offset, ZERO_BUF, 0, chunk);
            offset += chunk;
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
