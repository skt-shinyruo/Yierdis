package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A tiny open-addressing hash set of {@code byte[]}.
 * <p>
 * This is intentionally minimal and optimized for low object overhead.
 * It is <b>not</b> thread-safe.
 */
public final class ByteArrayHashSet {
    private static final Object PRESENT = new Object();

    private final ByteArrayHashMap<Object> map;

    public ByteArrayHashSet() {
        this.map = new ByteArrayHashMap<>();
    }

    public ByteArrayHashSet(int expectedSize) {
        this.map = new ByteArrayHashMap<>(expectedSize);
    }

    public int size() {
        return map.size();
    }

    public long estimatedBytes() {
        return map.estimatedBytes();
    }

    public boolean add(byte[] key) {
        Objects.requireNonNull(key, "key");
        return map.put(key, PRESENT) == null;
    }

    public boolean remove(byte[] key) {
        Objects.requireNonNull(key, "key");
        return map.removeKey(key);
    }

    public boolean contains(byte[] key) {
        Objects.requireNonNull(key, "key");
        return map.containsKey(key);
    }

    public void forEach(Consumer<byte[]> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        map.forEach((k, v) -> consumer.accept(k));
    }
}
