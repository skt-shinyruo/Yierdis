package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayDeque;

final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using an ArrayList for small lists (listpack-like),
    // and upgrading to a quicklist-like "deque of listpacks" for larger ones.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;
    private static final int QUICKLIST_NODE_MAX_ENTRIES = 64;

    private List<byte[]> listpack = new ArrayList<>();
    private ArrayDeque<ListPackNode> quicklist;
    private int totalSize = 0;

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    int size() {
        return totalSize;
    }

    void lpushAll(List<byte[]> values) {
        if (quicklist != null) {
            for (byte[] v : values) {
                qlAddFirst(v);
            }
            return;
        }

        if (shouldConvert(values)) {
            convertToQuickList();
            lpushAll(values);
            return;
        }

        for (byte[] v : values) {
            listpack.add(0, v);
            totalSize++;
        }
        if (listpack.size() > LISTPACK_MAX_ENTRIES) {
            convertToQuickList();
        }
    }

    void rpushAll(List<byte[]> values) {
        if (quicklist != null) {
            for (byte[] v : values) {
                qlAddLast(v);
            }
            return;
        }

        if (shouldConvert(values)) {
            convertToQuickList();
            rpushAll(values);
            return;
        }

        listpack.addAll(values);
        totalSize += values.size();
        if (listpack.size() > LISTPACK_MAX_ENTRIES) {
            convertToQuickList();
        }
    }

    List<byte[]> lpop(int count) {
        List<byte[]> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            byte[] v = quicklist != null ? qlPollFirst() : pollListpackFirst();
            if (v == null) {
                break;
            }
            totalSize--;
            out.add(v);
        }
        return out;
    }

    List<byte[]> rpop(int count) {
        List<byte[]> out = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            byte[] v = quicklist != null ? qlPollLast() : pollListpackLast();
            if (v == null) {
                break;
            }
            totalSize--;
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
        if (quicklist != null) {
            outer:
            for (ListPackNode n : quicklist) {
                for (int i = 0; i < n.size(); i++) {
                    if (idx > normalizedStop) {
                        break outer;
                    }
                    if (idx >= normalizedStart) {
                        out.add(n.get(i));
                    }
                    idx++;
                }
            }
        } else {
            for (byte[] v : listpack) {
                if (idx > normalizedStop) {
                    break;
                }
                if (idx >= normalizedStart) {
                    out.add(v);
                }
                idx++;
            }
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
        if (totalSize + incoming.size() > LISTPACK_MAX_ENTRIES) {
            return true;
        }
        for (byte[] s : incoming) {
            if (s != null && s.length > LISTPACK_MAX_ELEMENT_BYTES) {
                return true;
            }
        }
        return false;
    }

    private void convertToQuickList() {
        if (quicklist != null) {
            return;
        }

        ArrayDeque<ListPackNode> out = new ArrayDeque<>();
        ListPackNode node = new ListPackNode();
        for (byte[] v : listpack) {
            if (node.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                out.addLast(node);
                node = new ListPackNode();
            }
            node.addLast(v);
        }
        if (!node.isEmpty()) {
            out.addLast(node);
        }

        this.quicklist = out;
        this.listpack = null;
    }

    private void qlAddFirst(byte[] v) {
        if (quicklist.isEmpty() || quicklist.peekFirst().isFull()) {
            quicklist.addFirst(new ListPackNode());
        }
        quicklist.peekFirst().addFirst(v);
        totalSize++;
    }

    private void qlAddLast(byte[] v) {
        if (quicklist.isEmpty() || quicklist.peekLast().isFull()) {
            quicklist.addLast(new ListPackNode());
        }
        quicklist.peekLast().addLast(v);
        totalSize++;
    }

    private byte[] qlPollFirst() {
        if (quicklist.isEmpty()) {
            return null;
        }
        ListPackNode n = quicklist.peekFirst();
        byte[] v = n.removeFirst();
        if (n.isEmpty()) {
            quicklist.removeFirst();
        }
        return v;
    }

    private byte[] qlPollLast() {
        if (quicklist.isEmpty()) {
            return null;
        }
        ListPackNode n = quicklist.peekLast();
        byte[] v = n.removeLast();
        if (n.isEmpty()) {
            quicklist.removeLast();
        }
        return v;
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

    private static final class ListPackNode {
        private final ArrayList<byte[]> values = new ArrayList<>();

        boolean isFull() {
            return values.size() >= QUICKLIST_NODE_MAX_ENTRIES;
        }

        boolean isEmpty() {
            return values.isEmpty();
        }

        int size() {
            return values.size();
        }

        byte[] get(int i) {
            return values.get(i);
        }

        void addFirst(byte[] v) {
            values.add(0, v);
        }

        void addLast(byte[] v) {
            values.add(v);
        }

        byte[] removeFirst() {
            if (values.isEmpty()) {
                return null;
            }
            return values.remove(0);
        }

        byte[] removeLast() {
            int s = values.size();
            if (s == 0) {
                return null;
            }
            return values.remove(s - 1);
        }
    }
}
