package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayDeque;

final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using an ArrayList for small lists (listpack-like),
    // and upgrading to ArrayDeque for larger ones.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private List<byte[]> listpack = new ArrayList<>();
    private ArrayDeque<byte[]> deque;

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    int size() {
        if (deque != null) {
            return deque.size();
        }
        return listpack.size();
    }

    void lpushAll(List<byte[]> values) {
        if (deque != null) {
            for (byte[] v : values) {
                deque.addFirst(v);
            }
            return;
        }

        if (shouldConvert(values)) {
            convertToDeque();
            lpushAll(values);
            return;
        }

        for (byte[] v : values) {
            listpack.add(0, v);
        }
        if (listpack.size() > LISTPACK_MAX_ENTRIES) {
            convertToDeque();
        }
    }

    void rpushAll(List<byte[]> values) {
        if (deque != null) {
            for (byte[] v : values) {
                deque.addLast(v);
            }
            return;
        }

        if (shouldConvert(values)) {
            convertToDeque();
            rpushAll(values);
            return;
        }

        listpack.addAll(values);
        if (listpack.size() > LISTPACK_MAX_ENTRIES) {
            convertToDeque();
        }
    }

    List<byte[]> lpop(int count) {
        List<byte[]> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            byte[] v = deque != null ? deque.pollFirst() : pollListpackFirst();
            if (v == null) {
                break;
            }
            out.add(v);
        }
        return out;
    }

    List<byte[]> rpop(int count) {
        List<byte[]> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            byte[] v = deque != null ? deque.pollLast() : pollListpackLast();
            if (v == null) {
                break;
            }
            out.add(v);
        }
        return out;
    }

    List<byte[]> range(int start, int stop) {
        int size = size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int normalizedStart = normalizeIndex(start, size);
        int normalizedStop = normalizeIndex(stop, size);

        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return new ArrayList<>();
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1;
        }
        if (normalizedStart > normalizedStop) {
            return new ArrayList<>();
        }

        List<byte[]> out = new ArrayList<>(normalizedStop - normalizedStart + 1);
        int idx = 0;
        Iterable<byte[]> it = deque != null ? deque : listpack;
        for (byte[] v : it) {
            if (idx > normalizedStop) {
                break;
            }
            if (idx >= normalizedStart) {
                out.add(v);
            }
            idx++;
        }
        return out;
    }

    private static int normalizeIndex(int idx, int size) {
        if (idx >= 0) {
            return idx;
        }
        return size + idx;
    }

    private boolean shouldConvert(List<byte[]> incoming) {
        if (listpack.size() + incoming.size() > LISTPACK_MAX_ENTRIES) {
            return true;
        }
        for (byte[] s : incoming) {
            if (s != null && s.length > LISTPACK_MAX_ELEMENT_BYTES) {
                return true;
            }
        }
        return false;
    }

    private void convertToDeque() {
        if (deque != null) {
            return;
        }
        ArrayDeque<byte[]> out = new ArrayDeque<>(Math.max(16, listpack.size() * 2));
        out.addAll(listpack);
        this.deque = out;
        this.listpack = null;
    }

    private byte[] pollListpackFirst() {
        if (listpack.isEmpty()) {
            return null;
        }
        return listpack.remove(0);
    }

    private byte[] pollListpackLast() {
        int size = listpack.size();
        if (size == 0) {
            return null;
        }
        return listpack.remove(size - 1);
    }
}
