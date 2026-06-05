package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

public final class StringRoot implements TypeRoot {
    private static final int COPY_BUFFER_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_BYTES]);
    private static final byte[] ZERO_BUF = new byte[COPY_BUFFER_BYTES];
    private static final BytesSlice EMPTY_SLICE = new HeapBackedBytesSlice(new byte[0]);

    private final NativeAllocator allocator;
    private final boolean ownsAllocator;
    private final HashSet<Long> liveHandles = new HashSet<>();
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

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        try (NativeObjectView ignored = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_ONLY)) {
            return ValueEncoding.STRING_RAW;
        }
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return handle != null && liveHandles.contains(handle.raw());
    }

    public synchronized ValueHandle store(byte[] value) {
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
            ValueHandle handle = ValueHandle.fromNativeHandle(nativeHandle);
            liveHandles.add(handle.raw());
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public synchronized ValueHandle store(BytesSlice value) {
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
            ValueHandle handle = ValueHandle.fromNativeHandle(nativeHandle);
            liveHandles.add(handle.raw());
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public synchronized void overwrite(ValueHandle handle, byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        resizePreservingHandle(handle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
                view.setBytes(0, value, 0, len);
            }
        }
    }

    public synchronized void overwrite(ValueHandle handle, BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        resizePreservingHandle(handle, len);
        if (len > 0) {
            try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
                setBytes(view, 0, value, len);
            }
        }
    }

    public synchronized int append(ValueHandle handle, byte[] suffix) {
        ensureOpen();
        if (suffix == null || suffix.length == 0) {
            return length(handle);
        }
        int oldLen = length(handle);
        int nextLen = Math.addExact(oldLen, suffix.length);
        resizePreservingHandle(handle, nextLen);
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
            view.setBytes(oldLen, suffix, 0, suffix.length);
        }
        return nextLen;
    }

    public synchronized int append(ValueHandle handle, BytesSlice suffix) {
        ensureOpen();
        if (suffix == null || suffix.length() == 0) {
            return length(handle);
        }
        int oldLen = length(handle);
        int suffixLen = suffix.length();
        int nextLen = Math.addExact(oldLen, suffixLen);
        resizePreservingHandle(handle, nextLen);
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
            setBytes(view, oldLen, suffix, suffixLen);
        }
        return nextLen;
    }

    public synchronized void ensureLength(ValueHandle handle, int requiredLen) {
        ensureOpen();
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        int oldLen = length(handle);
        if (requiredLen <= oldLen) {
            return;
        }
        resizePreservingHandle(handle, requiredLen);
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
            zeroFill(view, oldLen, requiredLen);
        }
    }

    public synchronized byte byteAt(ValueHandle handle, int index) {
        ensureOpen();
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_ONLY)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            return view.getByte(index);
        }
    }

    public synchronized void setByteAt(ValueHandle handle, int index, byte value) {
        ensureOpen();
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_WRITE)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException();
            }
            view.setByte(index, value);
        }
    }

    public synchronized BytesSlice slice(ValueHandle handle) {
        ensureOpen();
        byte[] copy = copy(handle);
        if (copy.length == 0) {
            return EMPTY_SLICE;
        }
        return new HeapBackedBytesSlice(copy);
    }

    public synchronized byte[] copy(ValueHandle handle) {
        ensureOpen();
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_ONLY)) {
            int len = view.size();
            if (len == 0) {
                return new byte[0];
            }
            byte[] out = new byte[len];
            view.getBytes(0, out, 0, len);
            return out;
        }
    }

    public synchronized int length(ValueHandle handle) {
        ensureOpen();
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        try (NativeObjectView view = allocator.resolve(requireOwnedStringHandle(handle), NativeAccessMode.READ_ONLY)) {
            return view.capacity();
        }
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        NativeHandle nativeHandle = requireOwnedStringHandle(handle);
        allocator.free(nativeHandle);
        liveHandles.remove(handle.raw());
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        Long[] handles = liveHandles.toArray(Long[]::new);
        for (Long raw : handles) {
            try {
                allocator.free(NativeHandle.fromRaw(raw));
                liveHandles.remove(raw);
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
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

    private NativeHandle requireStringHandle(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        NativeHandle nativeHandle = handle.nativeHandle();
        nativeHandle.requireNonNull();
        NativeObjectKind kind = NativeObjectKind.STRING_BYTES;
        if (nativeHandle.domain() != kind.domain() || nativeHandle.kindCode() != kind.code()) {
            throw new IllegalArgumentException("value handle is not string bytes: " + handle.raw());
        }
        return nativeHandle;
    }

    private NativeHandle requireOwnedStringHandle(ValueHandle handle) {
        NativeHandle nativeHandle = requireStringHandle(handle);
        if (liveHandles.contains(handle.raw())) {
            return nativeHandle;
        }
        try (NativeObjectView ignored = allocator.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
            throw new IllegalArgumentException("unknown string value handle: " + handle.raw());
        }
    }

    private void resizePreservingHandle(ValueHandle handle, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        NativeHandle nativeHandle = requireOwnedStringHandle(handle);
        NativeHandle resized = allocator.realloc(nativeHandle, len, NativeReallocPolicy.PRESERVE_PREFIX);
        if (resized.raw() != nativeHandle.raw()) {
            throw new IllegalStateException("string realloc changed stable handle");
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
