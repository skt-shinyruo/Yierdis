package yier.bubu.redis.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

final class ListValue implements YierdisValue {
    private final Deque<String> list = new ArrayDeque<>();

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    int size() {
        return list.size();
    }

    void lpushAll(List<String> values) {
        for (String v : values) {
            list.addFirst(v);
        }
    }

    void rpushAll(List<String> values) {
        for (String v : values) {
            list.addLast(v);
        }
    }

    List<String> lpop(int count) {
        List<String> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            String v = list.pollFirst();
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
            String v = list.pollLast();
            if (v == null) {
                break;
            }
            out.add(v);
        }
        return out;
    }

    List<String> range(int start, int stop) {
        int size = list.size();
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
        for (String v : list) {
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
}
