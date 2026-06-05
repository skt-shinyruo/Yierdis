package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.ArrayList;
import java.util.Objects;

public final class NativeListpack implements AutoCloseable {
    private final NativeByteStore byteStore;
    private final NativeObjectKind valueKind;
    private final ArrayList<NativeHandle> entries = new ArrayList<>();

    private int encodedBytes;
    private int allocatedBytes;
    private int rawBytes;

    public NativeListpack(NativeByteStore byteStore, NativeObjectKind valueKind) {
        this.byteStore = Objects.requireNonNull(byteStore, "byteStore");
        this.valueKind = Objects.requireNonNull(valueKind, "valueKind");
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int encodedBytes() {
        return encodedBytes;
    }

    public int allocatedBytes() {
        return allocatedBytes;
    }

    public long estimatedBytes() {
        return allocatedBytes;
    }

    public int rawBytesSize() {
        return rawBytes;
    }

    public void clear() {
        for (NativeHandle handle : entries) {
            release(handle);
        }
        entries.clear();
        encodedBytes = 0;
        allocatedBytes = 0;
        rawBytes = 0;
    }

    public void addLast(byte[] value) {
        insertAt(entries.size(), value);
    }

    public void addFirst(byte[] value) {
        insertAt(0, value);
    }

    public void insertAt(int index, byte[] value) {
        if (index < 0 || index > entries.size()) {
            throw new IndexOutOfBoundsException();
        }
        NativeHandle handle = store(value);
        entries.add(index, handle);
        encodedBytes += entryEncodedBytes(value == null ? -1 : value.length);
        if (handle != null) {
            allocatedBytes += byteStore.allocatedBytes(handle);
            rawBytes += value.length;
        }
    }

    public byte[] removeFirst() {
        return removeAt(0);
    }

    public byte[] removeLast() {
        return removeAt(entries.size() - 1);
    }

    public byte[] removeAt(int index) {
        NativeHandle handle = entries.remove(index);
        encodedBytes -= entryEncodedBytes(handle == null ? -1 : byteStore.length(handle));
        if (handle == null) {
            return null;
        }
        int allocatedLen = byteStore.allocatedBytes(handle);
        byte[] out = byteStore.toByteArray(handle);
        allocatedBytes -= allocatedLen;
        rawBytes -= out.length;
        byteStore.release(handle);
        return out;
    }

    public byte[] get(int index) {
        NativeHandle handle = entries.get(index);
        return handle == null ? null : byteStore.toByteArray(handle);
    }

    public boolean equalsAt(int index, byte[] other) {
        NativeHandle handle = entries.get(index);
        if (handle == null) {
            return other == null;
        }
        return other != null && byteStore.equalsBytes(handle, other);
    }

    public void set(int index, byte[] value) {
        NativeHandle old = entries.get(index);
        NativeHandle next = store(value);
        entries.set(index, next);
        encodedBytes += entryEncodedBytes(value == null ? -1 : value.length)
                - entryEncodedBytes(old == null ? -1 : byteStore.length(old));
        if (old != null) {
            int oldLen = byteStore.length(old);
            allocatedBytes -= byteStore.allocatedBytes(old);
            rawBytes -= oldLen;
            byteStore.release(old);
        }
        if (next != null) {
            allocatedBytes += byteStore.allocatedBytes(next);
            rawBytes += value.length;
        }
    }

    public int indexOf(byte[] needle) {
        for (int i = 0; i < entries.size(); i++) {
            if (equalsAt(i, needle)) {
                return i;
            }
        }
        return -1;
    }

    public Cursor cursor() {
        return new Cursor(this);
    }

    @Override
    public void close() {
        clear();
    }

    private NativeHandle store(byte[] value) {
        if (value == null) {
            return null;
        }
        return byteStore.store(value, valueKind);
    }

    private void release(NativeHandle handle) {
        if (handle != null) {
            byteStore.release(handle);
        }
    }

    private static int entryEncodedBytes(int len) {
        int headerValue = len < 0 ? 0 : len + 1;
        return varIntSize(headerValue) + Math.max(0, len);
    }

    private static int varIntSize(int value) {
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

    public static final class Cursor {
        private final NativeListpack owner;
        private int index = -1;

        private Cursor(NativeListpack owner) {
            this.owner = owner;
        }

        public boolean next() {
            if (index + 1 >= owner.entries.size()) {
                return false;
            }
            index++;
            return true;
        }

        public boolean isNull() {
            currentHandle();
            return owner.entries.get(index) == null;
        }

        public int length() {
            NativeHandle handle = currentHandle();
            return handle == null ? 0 : owner.byteStore.length(handle);
        }

        public boolean equalsBytes(byte[] other) {
            currentHandle();
            NativeHandle handle = owner.entries.get(index);
            if (handle == null) {
                return other == null;
            }
            return other != null && owner.byteStore.equalsBytes(handle, other);
        }

        public byte[] toByteArray() {
            currentHandle();
            NativeHandle handle = owner.entries.get(index);
            return handle == null ? null : owner.byteStore.toByteArray(handle);
        }

        public void writeTo(BulkStringSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            currentHandle();
            NativeHandle handle = owner.entries.get(index);
            if (handle == null) {
                out.bulkStringNull();
                return;
            }
            out.bulkString(owner.byteStore.slice(handle));
        }

        public void appendTo(NativeListpack other) {
            if (other == null) {
                throw new IllegalArgumentException("other must not be null");
            }
            other.addLast(toByteArray());
        }

        private NativeHandle currentHandle() {
            if (index < 0 || index >= owner.entries.size()) {
                throw new IllegalStateException("cursor is not positioned");
            }
            return owner.entries.get(index);
        }
    }
}
