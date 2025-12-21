package yier.bubu.redis.db;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A tiny open-addressing hash set of {@code byte[]}.
 * <p>
 * This is intentionally minimal and optimized for low object overhead.
 * It is <b>not</b> thread-safe.
 */
final class ByteArrayHashSet {
    private static final Object PRESENT = new Object();

    private final ByteArrayHashMap<Object> map;

    ByteArrayHashSet() {
        this.map = new ByteArrayHashMap<>();
    }

    ByteArrayHashSet(int expectedSize) {
        this.map = new ByteArrayHashMap<>(expectedSize);
    }

    int size() {
        return map.size();
    }

    boolean add(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (map.containsKey(key)) {
            return false;
        }
        map.put(key, PRESENT);
        return true;
    }

    boolean remove(byte[] key) {
        Objects.requireNonNull(key, "key");
        return map.removeKey(key);
    }

    boolean contains(byte[] key) {
        Objects.requireNonNull(key, "key");
        return map.containsKey(key);
    }

    void forEach(Consumer<byte[]> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        map.forEach((k, v) -> consumer.accept(k));
    }
}

