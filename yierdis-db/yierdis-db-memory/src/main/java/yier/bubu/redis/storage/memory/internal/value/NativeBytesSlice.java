package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;

import java.util.Objects;

public final class NativeBytesSlice implements BytesSlice {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final NativeAllocator allocator;
    private final NativeHandle handle;
    private final int offset;
    private final int length;
    private final boolean retainedPin;

    public NativeBytesSlice(NativeAllocator allocator, NativeHandle handle, int offset, int length) {
        this(allocator, handle, offset, length, false);
    }

    public static NativeBytesSlice retained(NativeAllocator allocator, NativeHandle handle, int offset, int length) {
        return new NativeBytesSlice(allocator, handle, offset, length, true);
    }

    private NativeBytesSlice(NativeAllocator allocator, NativeHandle handle, int offset, int length, boolean retainedPin) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.handle = Objects.requireNonNull(handle, "handle");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        this.offset = offset;
        this.length = length;
        this.retainedPin = retainedPin;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte getByte(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + length);
        }
        try (NativeObjectView view = readView()) {
            checkSliceBounds(view);
            return view.getByte(offset + index);
        }
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        Objects.requireNonNull(dst, "dst");
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (index < 0 || dstOff < 0 || index > length || dstOff > dst.length
                || len > length - index || len > dst.length - dstOff) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        try (NativeObjectView view = readView()) {
            checkSliceBounds(view);
            view.getBytes(offset + index, dst, dstOff, len);
        }
    }

    @Override
    public void writeTo(BytesSink out) {
        Objects.requireNonNull(out, "out");
        try (NativeObjectView view = readView()) {
            checkSliceBounds(view);
            byte[] scratch = TL_COPY_BUF.get();
            int remaining = length;
            int sourceOffset = offset;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                view.getBytes(sourceOffset, scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                sourceOffset += chunk;
                remaining -= chunk;
            }
        }
    }

    private NativeObjectView readView() {
        return retainedPin
                ? allocator.resolvePinned(handle, NativeAccessMode.READ_ONLY)
                : allocator.resolve(handle, NativeAccessMode.READ_ONLY);
    }

    private void checkSliceBounds(NativeObjectView view) {
        int size = view.size();
        if (offset > size || length > size - offset) {
            throw new IndexOutOfBoundsException("offset=" + offset + ", len=" + length + ", size=" + size);
        }
    }
}
