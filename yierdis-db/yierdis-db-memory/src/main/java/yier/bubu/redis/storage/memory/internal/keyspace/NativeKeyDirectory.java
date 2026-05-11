package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBytesRef;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

public final class NativeKeyDirectory implements AutoCloseable {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final YierdisFfmBlobStore blobStore;
    private YierdisFfmBytesRef[] keyRefs;
    private EntryHandle[] handles;
    private int[] hashes;
    private byte[] states;
    private int size;
    private int tombstones;
    private boolean closed;

    public NativeKeyDirectory(YierdisFfmMemoryRuntime runtime) {
        this(new YierdisFfmBlobStore(Objects.requireNonNull(runtime, "runtime"), "native-key"));
    }

    public NativeKeyDirectory(EntryTable entryTable) {
        this(new YierdisFfmBlobStore(Objects.requireNonNull(entryTable, "entryTable").runtime(), "native-key"));
    }

    public NativeKeyDirectory(YierdisFfmBlobStore blobStore) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        int capacity = MIN_CAPACITY;
        this.keyRefs = new YierdisFfmBytesRef[capacity];
        this.handles = new EntryHandle[capacity];
        this.hashes = new int[capacity];
        this.states = new byte[capacity];
    }

    public synchronized int size() {
        return size;
    }

    public synchronized long nativeBytes() {
        ensureOpen();
        return blobStore.liveBytes();
    }

    public synchronized EntryHandle get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        return index < 0 ? null : handles[index];
    }

    public synchronized KeyHandle getKeyHandle(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        return index < 0 ? null : keyHandleAt(index);
    }

    public synchronized KeyHandle randomKeyHandle() {
        ensureOpen();
        if (size == 0) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(keyRefs.length);
        for (int step = 0; step < keyRefs.length; step++) {
            int index = (start + step) & (keyRefs.length - 1);
            if (states[index] == STATE_FILLED) {
                return keyHandleAt(index);
            }
        }
        return null;
    }

    public synchronized void forEachEntry(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        for (int i = 0; i < keyRefs.length; i++) {
            if (states[i] == STATE_FILLED) {
                consumer.accept(keyHandleAt(i), handles[i]);
            }
        }
    }

    public synchronized ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer consumer) {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        if (maxSteps <= 0 || size == 0) {
            return ScanCursorV2.start();
        }

        int index = Math.toIntExact(Math.min(cursor.position(), keyRefs.length));
        int visited = 0;
        while (index < keyRefs.length && visited < maxSteps) {
            if (states[index] == STATE_FILLED) {
                visited++;
                if (!consumer.accept(keyHandleAt(index), handles[index])) {
                    return nextCursor(index + 1);
                }
            }
            index++;
        }
        return nextCursor(index);
    }

    public synchronized EntryHandle compute(
            byte[] keyBytes,
            BiFunction<? super byte[], ? super EntryHandle, ? extends EntryHandle> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        ensureOpen();

        int hash = hash(keyBytes);
        int index = findIndex(keyBytes, hash);
        if (index >= 0) {
            EntryHandle oldHandle = handles[index];
            EntryHandle newHandle = remappingFunction.apply(keyBytes, oldHandle);
            if (newHandle == null) {
                removeAt(index);
                return null;
            }
            handles[index] = newHandle;
            return newHandle;
        }

        EntryHandle newHandle = remappingFunction.apply(keyBytes, null);
        if (newHandle == null) {
            return null;
        }

        ensureCapacityForInsert();
        int insertIndex = findInsertIndex(keyBytes, hash);
        if (states[insertIndex] == STATE_TOMBSTONE) {
            tombstones--;
        }
        keyRefs[insertIndex] = blobStore.store(keyBytes);
        handles[insertIndex] = newHandle;
        hashes[insertIndex] = hash;
        states[insertIndex] = STATE_FILLED;
        size++;
        return newHandle;
    }

    public synchronized EntryHandle remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        if (index < 0) {
            return null;
        }
        EntryHandle removed = handles[index];
        removeAt(index);
        return removed;
    }

    public synchronized boolean remove(byte[] keyBytes, EntryHandle expectedHandle) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(expectedHandle, "expectedHandle");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        if (index < 0 || !expectedHandle.equals(handles[index])) {
            return false;
        }
        removeAt(index);
        return true;
    }

    public synchronized boolean remove(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        int index = findHandleIndex(handle);
        if (index < 0) {
            return false;
        }
        removeAt(index);
        return true;
    }

    public synchronized void clear() {
        ensureOpen();
        clearInternal();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        clearInternal();
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native key directory is closed");
        }
    }

    private KeyHandle keyHandleAt(int index) {
        return KeyHandle.forFfm(keyRefs[index], hashes[index]);
    }

    private ScanCursorV2 nextCursor(int nextIndex) {
        return nextIndex >= keyRefs.length ? ScanCursorV2.start() : ScanCursorV2.of(nextIndex);
    }

    private void clearInternal() {
        RuntimeException failure = null;
        for (int i = 0; i < keyRefs.length; i++) {
            if (states[i] == STATE_FILLED) {
                try {
                    blobStore.release(keyRefs[i]);
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                    continue;
                }
            }
            keyRefs[i] = null;
            handles[i] = null;
            hashes[i] = 0;
            states[i] = STATE_EMPTY;
        }
        size = 0;
        tombstones = 0;
        if (failure != null) {
            recomputeCounts();
            throw failure;
        }
    }

    private void ensureCapacityForInsert() {
        if (size + tombstones + 1 <= threshold()) {
            return;
        }
        rehash(keyRefs.length << 1);
    }

    private void rehash(int newCapacity) {
        YierdisFfmBytesRef[] oldKeyRefs = keyRefs;
        EntryHandle[] oldHandles = handles;
        int[] oldHashes = hashes;
        byte[] oldStates = states;

        keyRefs = new YierdisFfmBytesRef[newCapacity];
        handles = new EntryHandle[newCapacity];
        hashes = new int[newCapacity];
        states = new byte[newCapacity];
        tombstones = 0;

        for (int i = 0; i < oldKeyRefs.length; i++) {
            if (oldStates[i] != STATE_FILLED) {
                continue;
            }
            YierdisFfmBytesRef keyRef = oldKeyRefs[i];
            EntryHandle handle = oldHandles[i];
            int hash = oldHashes[i];
            int index = findInsertIndex(keyRef, hash);
            keyRefs[index] = keyRef;
            handles[index] = handle;
            hashes[index] = hash;
            states[index] = STATE_FILLED;
        }
    }

    private int threshold() {
        return Math.max(1, (int) (keyRefs.length * LOAD_FACTOR));
    }

    private int findIndex(byte[] keyBytes, int hash) {
        int mask = keyRefs.length - 1;
        int index = hash & mask;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[index] == hash && blobStore.equalsBytes(keyRefs[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findHandleIndex(EntryHandle handle) {
        for (int i = 0; i < handles.length; i++) {
            if (states[i] == STATE_FILLED && handle.equals(handles[i])) {
                return i;
            }
        }
        return -1;
    }

    private int findInsertIndex(byte[] keyBytes, int hash) {
        int mask = keyRefs.length - 1;
        int index = hash & mask;
        int firstTombstone = -1;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return firstTombstone >= 0 ? firstTombstone : index;
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = index;
                }
            } else if (hashes[index] == hash && blobStore.equalsBytes(keyRefs[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findInsertIndex(YierdisFfmBytesRef keyRef, int hash) {
        int mask = keyRefs.length - 1;
        int index = hash & mask;
        while (true) {
            if (states[index] == STATE_EMPTY) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void removeAt(int index) {
        if (states[index] != STATE_FILLED) {
            return;
        }
        blobStore.release(keyRefs[index]);
        keyRefs[index] = null;
        handles[index] = null;
        hashes[index] = 0;
        states[index] = STATE_TOMBSTONE;
        size--;
        tombstones++;
    }

    private void recomputeCounts() {
        int nextSize = 0;
        int nextTombstones = 0;
        for (byte state : states) {
            if (state == STATE_FILLED) {
                nextSize++;
            } else if (state == STATE_TOMBSTONE) {
                nextTombstones++;
            }
        }
        size = nextSize;
        tombstones = nextTombstones;
    }

    private static int hash(byte[] keyBytes) {
        int h = 1;
        for (byte keyByte : keyBytes) {
            h = 31 * h + keyByte;
        }
        return h;
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(KeyHandle keyHandle, EntryHandle entryHandle);
    }

    @FunctionalInterface
    public interface ScanConsumer {
        boolean accept(KeyHandle keyHandle, EntryHandle entryHandle);
    }
}
