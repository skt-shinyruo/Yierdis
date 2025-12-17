package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayDeque;

final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using an ArrayList for small lists (listpack-like),
    // and upgrading to ArrayDeque for larger ones.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_CHARS = 64;

    private List<String> listpack = new ArrayList<>();
    private ArrayDeque<String> deque;

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

    void lpushAll(List<String> values) {
        if (deque != null) {
            for (String v : values) {
                deque.addFirst(v);
            }
            return;
        }

        if (shouldConvert(values)) {
            convertToDeque();
            lpushAll(values);
            return;
        }

        for (String v : values) {
            listpack.add(0, v);
        }
        if (listpack.size() > LISTPACK_MAX_ENTRIES) {
            convertToDeque();
        }
    }

    void rpushAll(List<String> values) {
        if (deque != null) {
            for (String v : values) {
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

    List<String> lpop(int count) {
        List<String> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            String v = deque != null ? deque.pollFirst() : pollListpackFirst();
            if (v == null) {
                break;
            }
            out.add(v);
        }
        return out;
    }

    List<String> rpop(int count) {
        List<String> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            String v = deque != null ? deque.pollLast() : pollListpackLast();
            if (v == null) {
                break;
            }
            out.add(v);
        }
        return out;
    }

    List<String> range(int start, int stop) {
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

        List<String> out = new ArrayList<>(normalizedStop - normalizedStart + 1);
        int idx = 0;
        Iterable<String> it = deque != null ? deque : listpack;
        for (String v : it) {
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

    private boolean shouldConvert(List<String> incoming) {
        if (listpack.size() + incoming.size() > LISTPACK_MAX_ENTRIES) {
            return true;
        }
        for (String s : incoming) {
            if (s != null && s.length() > LISTPACK_MAX_ELEMENT_CHARS) {
                return true;
            }
        }
        return false;
    }

    private void convertToDeque() {
        if (deque != null) {
            return;
        }
        ArrayDeque<String> out = new ArrayDeque<>(Math.max(16, listpack.size() * 2));
        out.addAll(listpack);
        this.deque = out;
        this.listpack = null;
    }

    private String pollListpackFirst() {
        if (listpack.isEmpty()) {
            return null;
        }
        return listpack.remove(0);
    }

    private String pollListpackLast() {
        int size = listpack.size();
        if (size == 0) {
            return null;
        }
        return listpack.remove(size - 1);
    }
}
