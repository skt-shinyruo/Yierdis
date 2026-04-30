package yier.bubu.redis.db.memory.ffm;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class YierdisFfmByteMap<V> implements AutoCloseable {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;

    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final YierdisFfmBlobStore blobStore;
    private final int seed;

    private byte[] states;
    private int[] hashes;
    private YierdisFfmBytesRef[] keys;
    private Object[] values;
    private int size;
    private int used;
    private int threshold;

    public YierdisFfmByteMap(YierdisFfmBlobStore blobStore) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        this.seed = ThreadLocalRandom.current().nextInt();
    }

    public int size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    public V get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int index = findIndex(keyBytes);
        return index < 0 ? null : (V) values[index];
    }

    @SuppressWarnings("unchecked")
    public V put(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureTable();
        if (used >= threshold) {
            rehash(keys.length << 1);
        }

        int hash = hash(keyBytes);
        int insertAt = findInsertIndex(keyBytes, hash);
        if (insertAt < 0) {
            int existing = -insertAt - 1;
            V old = (V) values[existing];
            values[existing] = value;
            return old;
        }

        YierdisFfmBytesRef ref = blobStore.store(keyBytes);
        byte oldState = states[insertAt];
        states[insertAt] = STATE_FILLED;
        hashes[insertAt] = hash;
        keys[insertAt] = ref;
        values[insertAt] = value;
        size++;
        if (oldState == STATE_EMPTY) {
            used++;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public V remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int index = findIndex(keyBytes);
        if (index < 0) {
            return null;
        }
        V old = (V) values[index];
        blobStore.release(keys[index]);
        states[index] = STATE_TOMBSTONE;
        keys[index] = null;
        values[index] = null;
        size--;
        return old;
    }

    public void forEach(EntryConsumer<V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (states == null || size == 0) {
            return;
        }
        for (int i = 0; i < states.length; i++) {
            if (states[i] != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V value = (V) values[i];
            consumer.accept(keys[i], value);
        }
    }

    public void clear() {
        if (states == null) {
            return;
        }
        for (int i = 0; i < states.length; i++) {
            if (states[i] == STATE_FILLED) {
                blobStore.release(keys[i]);
            }
        }
        states = null;
        hashes = null;
        keys = null;
        values = null;
        size = 0;
        used = 0;
        threshold = 0;
    }

    @Override
    public void close() {
        clear();
    }

    private void ensureTable() {
        if (states != null) {
            return;
        }
        int capacity = MIN_CAPACITY;
        states = new byte[capacity];
        hashes = new int[capacity];
        keys = new YierdisFfmBytesRef[capacity];
        values = new Object[capacity];
        threshold = Math.max(1, (int) (capacity * LOAD_FACTOR));
    }

    private void rehash(int capacity) {
        byte[] oldStates = states;
        int[] oldHashes = hashes;
        YierdisFfmBytesRef[] oldKeys = keys;
        Object[] oldValues = values;

        states = new byte[capacity];
        hashes = new int[capacity];
        keys = new YierdisFfmBytesRef[capacity];
        values = new Object[capacity];
        size = 0;
        used = 0;
        threshold = Math.max(1, (int) (capacity * LOAD_FACTOR));

        for (int i = 0; i < oldStates.length; i++) {
            if (oldStates[i] != STATE_FILLED) {
                continue;
            }
            int index = findInsertIndexForRehash(oldKeys[i], oldHashes[i]);
            states[index] = STATE_FILLED;
            hashes[index] = oldHashes[i];
            keys[index] = oldKeys[i];
            values[index] = oldValues[i];
            size++;
            used++;
        }
    }

    private int findIndex(byte[] keyBytes) {
        if (states == null || size == 0) {
            return -1;
        }
        int mask = states.length - 1;
        int hash = hash(keyBytes);
        int index = hash & mask;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[index] == hash && blobStore.equalsBytes(keys[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findInsertIndex(byte[] keyBytes, int hash) {
        int mask = states.length - 1;
        int index = hash & mask;
        int tombstone = -1;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return tombstone >= 0 ? tombstone : index;
            }
            if (state == STATE_TOMBSTONE) {
                if (tombstone < 0) {
                    tombstone = index;
                }
            } else if (hashes[index] == hash && blobStore.equalsBytes(keys[index], keyBytes)) {
                return -index - 1;
            }
            index = (index + 1) & mask;
        }
    }

    private int findInsertIndexForRehash(YierdisFfmBytesRef ref, int hash) {
        int mask = states.length - 1;
        int index = hash & mask;
        while (states[index] == STATE_FILLED) {
            index = (index + 1) & mask;
        }
        return index;
    }

    private int hash(byte[] keyBytes) {
        int h = seed;
        for (byte b : keyBytes) {
            h = 31 * h + (b & 0xFF);
        }
        h ^= (h >>> 16);
        return h;
    }

    @FunctionalInterface
    public interface EntryConsumer<V> {
        void accept(YierdisFfmBytesRef keyRef, V value);
    }
}
