package yier.bubu.redis.db;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * A tiny dict-like hash table with incremental rehashing (Redis-style).
 * <p>
 * This is intentionally minimal and optimized for predictability and learning.
 * It is <b>not</b> thread-safe.
 */
final class YierdisDict<V> {
    private static final int INITIAL_SIZE = 4;

    @SuppressWarnings("unchecked")
    private Node<V>[] table0 = (Node<V>[]) new Node[INITIAL_SIZE];
    private int rehashIndex = -1;

    @SuppressWarnings("unchecked")
    private Node<V>[] table1;

    private int size = 0;

    V get(ByteArrayKey key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        NodeLocation<V> loc = findNodeLocation(key);
        return loc == null ? null : loc.node.value;
    }

    V compute(ByteArrayKey key, BiFunction<? super ByteArrayKey, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();

        NodeLocation<V> loc = findNodeLocation(key);
        V oldValue = loc == null ? null : loc.node.value;
        V newValue = remappingFunction.apply(key, oldValue);

        if (newValue == null) {
            if (loc != null) {
                removeAt(loc);
            }
            return null;
        }

        if (loc != null) {
            loc.node.value = newValue;
            return newValue;
        }

        insertNew(key, newValue);
        return newValue;
    }

    V computeIfPresent(ByteArrayKey key, BiFunction<? super ByteArrayKey, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();

        NodeLocation<V> loc = findNodeLocation(key);
        if (loc == null) {
            return null;
        }

        V newValue = remappingFunction.apply(key, loc.node.value);
        if (newValue == null) {
            removeAt(loc);
            return null;
        }
        loc.node.value = newValue;
        return newValue;
    }

    boolean remove(ByteArrayKey key, V expectedValue) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        NodeLocation<V> loc = findNodeLocation(key);
        if (loc == null || loc.node.value != expectedValue) {
            return false;
        }
        removeAt(loc);
        return true;
    }

    void clear() {
        @SuppressWarnings("unchecked")
        Node<V>[] fresh = (Node<V>[]) new Node[INITIAL_SIZE];
        table0 = fresh;
        table1 = null;
        rehashIndex = -1;
        size = 0;
    }

    int size() {
        return size;
    }

    void forEach(BiConsumer<ByteArrayKey, V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        forEachTable(table0, consumer);
        if (table1 != null) {
            forEachTable(table1, consumer);
        }
    }

    private void forEachTable(Node<V>[] table, BiConsumer<ByteArrayKey, V> consumer) {
        for (int i = 0; i < table.length; i++) {
            for (Node<V> n = table[i]; n != null; n = n.next) {
                consumer.accept(n.key, n.value);
            }
        }
    }

    private boolean isRehashing() {
        return table1 != null;
    }

    private void rehashStep() {
        if (!isRehashing()) {
            return;
        }

        while (rehashIndex < table0.length && table0[rehashIndex] == null) {
            rehashIndex++;
        }
        if (rehashIndex >= table0.length) {
            finishRehash();
            return;
        }

        Node<V> node = table0[rehashIndex];
        table0[rehashIndex] = null;
        while (node != null) {
            Node<V> next = node.next;
            int idx = index(node.key, table1);
            node.next = table1[idx];
            table1[idx] = node;
            node = next;
        }
        rehashIndex++;

        if (rehashIndex >= table0.length) {
            finishRehash();
        }
    }

    private void finishRehash() {
        table0 = table1;
        table1 = null;
        rehashIndex = -1;
    }

    private void maybeStartRehash() {
        if (isRehashing()) {
            return;
        }
        if (size <= table0.length) {
            return;
        }

        int newSize = table0.length;
        while (newSize < size * 2) {
            newSize <<= 1;
        }

        @SuppressWarnings("unchecked")
        Node<V>[] next = (Node<V>[]) new Node[newSize];
        table1 = next;
        rehashIndex = 0;
    }

    private void insertNew(ByteArrayKey key, V value) {
        Node<V>[] table = isRehashing() ? table1 : table0;
        int idx = index(key, table);
        table[idx] = new Node<>(key, value, table[idx]);
        size++;
        maybeStartRehash();
    }

    private static int index(ByteArrayKey key, Node<?>[] table) {
        return key.hashCode() & (table.length - 1);
    }

    private NodeLocation<V> findNodeLocation(ByteArrayKey key) {
        NodeLocation<V> loc = findInTable(table0, key);
        if (loc != null) {
            return loc;
        }
        if (table1 != null) {
            return findInTable(table1, key);
        }
        return null;
    }

    private NodeLocation<V> findInTable(Node<V>[] table, ByteArrayKey key) {
        int idx = index(key, table);
        Node<V> prev = null;
        Node<V> cur = table[idx];
        while (cur != null) {
            if (cur.key.equals(key)) {
                return new NodeLocation<>(table, idx, prev, cur);
            }
            prev = cur;
            cur = cur.next;
        }
        return null;
    }

    private void removeAt(NodeLocation<V> loc) {
        if (loc.prev == null) {
            loc.table[loc.tableIndex] = loc.node.next;
        } else {
            loc.prev.next = loc.node.next;
        }
        size--;
    }

    private static final class Node<V> {
        final ByteArrayKey key;
        V value;
        Node<V> next;

        private Node(ByteArrayKey key, V value, Node<V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    private static final class NodeLocation<V> {
        final Node<V>[] table;
        final int tableIndex;
        final Node<V> prev;
        final Node<V> node;

        private NodeLocation(Node<V>[] table, int tableIndex, Node<V> prev, Node<V> node) {
            this.table = table;
            this.tableIndex = tableIndex;
            this.prev = prev;
            this.node = node;
        }
    }
}

