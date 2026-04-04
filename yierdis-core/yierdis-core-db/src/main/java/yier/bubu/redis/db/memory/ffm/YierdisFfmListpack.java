package yier.bubu.redis.db.memory.ffm;

import yier.bubu.redis.ops.result.BulkStringSink;

import java.util.ArrayList;

public final class YierdisFfmListpack implements AutoCloseable {
    private final YierdisFfmBlobStore blobStore;
    private final ArrayList<YierdisFfmBytesRef> entries = new ArrayList<>();

    private int encodedBytes;
    private int allocatedBytes;
    private int rawBytes;

    public YierdisFfmListpack(YierdisFfmBlobStore blobStore) {
        if (blobStore == null) {
            throw new IllegalArgumentException("blobStore must not be null");
        }
        this.blobStore = blobStore;
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

    public int rawBytesSize() {
        return rawBytes;
    }

    public void clear() {
        for (YierdisFfmBytesRef ref : entries) {
            release(ref);
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
        YierdisFfmBytesRef ref = store(value);
        entries.add(index, ref);
        encodedBytes += entryEncodedBytes(value == null ? -1 : value.length);
        if (ref != null) {
            allocatedBytes += ref.region().size();
            rawBytes += ref.length();
        }
    }

    public byte[] removeFirst() {
        return removeAt(0);
    }

    public byte[] removeLast() {
        return removeAt(entries.size() - 1);
    }

    public byte[] removeAt(int index) {
        YierdisFfmBytesRef ref = entries.remove(index);
        encodedBytes -= entryEncodedBytes(ref == null ? -1 : ref.length());
        if (ref == null) {
            return null;
        }
        byte[] out = blobStore.toByteArray(ref);
        allocatedBytes -= ref.region().size();
        rawBytes -= ref.length();
        blobStore.release(ref);
        return out;
    }

    public byte[] get(int index) {
        YierdisFfmBytesRef ref = entries.get(index);
        if (ref == null) {
            return null;
        }
        return blobStore.toByteArray(ref);
    }

    public boolean equalsAt(int index, byte[] other) {
        YierdisFfmBytesRef ref = entries.get(index);
        if (ref == null) {
            return other == null;
        }
        return other != null && blobStore.equalsBytes(ref, other);
    }

    public void set(int index, byte[] value) {
        YierdisFfmBytesRef old = entries.get(index);
        YierdisFfmBytesRef next = store(value);
        entries.set(index, next);
        encodedBytes += entryEncodedBytes(value == null ? -1 : value.length)
                - entryEncodedBytes(old == null ? -1 : old.length());
        if (old != null) {
            allocatedBytes -= old.region().size();
            rawBytes -= old.length();
            blobStore.release(old);
        }
        if (next != null) {
            allocatedBytes += next.region().size();
            rawBytes += next.length();
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

    private YierdisFfmBytesRef store(byte[] value) {
        if (value == null) {
            return null;
        }
        return blobStore.store(value);
    }

    private void release(YierdisFfmBytesRef ref) {
        if (ref != null) {
            blobStore.release(ref);
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
        private final YierdisFfmListpack owner;
        private int index = -1;

        private Cursor(YierdisFfmListpack owner) {
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
            currentRef();
            return owner.entries.get(index) == null;
        }

        public int length() {
            YierdisFfmBytesRef ref = currentRef();
            return ref == null ? 0 : ref.length();
        }

        public boolean equalsBytes(byte[] other) {
            currentRef();
            YierdisFfmBytesRef ref = owner.entries.get(index);
            if (ref == null) {
                return other == null;
            }
            return other != null && owner.blobStore.equalsBytes(ref, other);
        }

        public byte[] toByteArray() {
            currentRef();
            YierdisFfmBytesRef ref = owner.entries.get(index);
            return ref == null ? null : owner.blobStore.toByteArray(ref);
        }

        public void writeTo(BulkStringSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            currentRef();
            YierdisFfmBytesRef ref = owner.entries.get(index);
            if (ref == null) {
                out.bulkStringNull();
                return;
            }
            out.bulkString(new YierdisFfmBytesRefSlice(ref));
        }

        public void appendTo(YierdisFfmListpack other) {
            if (other == null) {
                throw new IllegalArgumentException("other must not be null");
            }
            other.addLast(toByteArray());
        }

        private YierdisFfmBytesRef currentRef() {
            if (index < 0 || index >= owner.entries.size()) {
                throw new IllegalStateException("cursor is not positioned");
            }
            return owner.entries.get(index);
        }
    }
}
