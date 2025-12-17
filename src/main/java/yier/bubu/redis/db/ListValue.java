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
    private static final int QUICKLIST_NODE_MAX_BYTES = 8 * 1024;

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
        if (count <= 0) {
            return new ArrayList<>();
        }
        int expected = Math.min(count, totalSize);
        List<byte[]> out = new ArrayList<>(expected);
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
        if (count <= 0) {
            return new ArrayList<>();
        }
        int expected = Math.min(count, totalSize);
        List<byte[]> out = new ArrayList<>(expected);
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
            if (!node.canAdd(v)) {
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
        if (quicklist.isEmpty() || !quicklist.peekFirst().canAdd(v)) {
            quicklist.addFirst(new ListPackNode());
        }
        quicklist.peekFirst().addFirst(v);
        totalSize++;
    }

    private void qlAddLast(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekLast().canAdd(v)) {
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
        maybeMergeFirstTwo();
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
        maybeMergeLastTwo();
        return v;
    }

    private void maybeMergeFirstTwo() {
        if (quicklist.size() < 2) {
            return;
        }
        ListPackNode first = quicklist.pollFirst();
        ListPackNode second = quicklist.pollFirst();
        if (first == null || second == null) {
            if (second != null) {
                quicklist.addFirst(second);
            }
            if (first != null) {
                quicklist.addFirst(first);
            }
            return;
        }

        if (first.canAppendAll(second)) {
            first.appendAll(second);
            quicklist.addFirst(first);
            return;
        }

        // Restore original order: first, second
        quicklist.addFirst(second);
        quicklist.addFirst(first);
    }

    private void maybeMergeLastTwo() {
        if (quicklist.size() < 2) {
            return;
        }
        ListPackNode last = quicklist.pollLast();
        ListPackNode prev = quicklist.pollLast();
        if (last == null || prev == null) {
            if (prev != null) {
                quicklist.addLast(prev);
            }
            if (last != null) {
                quicklist.addLast(last);
            }
            return;
        }

        if (prev.canAppendAll(last)) {
            prev.appendAll(last);
            quicklist.addLast(prev);
            return;
        }

        // Restore original order: prev, last
        quicklist.addLast(prev);
        quicklist.addLast(last);
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
        private int bytesSize = 0;

        boolean canAdd(byte[] v) {
            int len = v == null ? 0 : v.length;
            if (values.isEmpty()) {
                return true;
            }
            if (values.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return bytesSize + len <= QUICKLIST_NODE_MAX_BYTES;
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
            bytesSize += v == null ? 0 : v.length;
        }

        void addLast(byte[] v) {
            values.add(v);
            bytesSize += v == null ? 0 : v.length;
        }

        byte[] removeFirst() {
            if (values.isEmpty()) {
                return null;
            }
            byte[] v = values.remove(0);
            bytesSize -= v == null ? 0 : v.length;
            return v;
        }

        byte[] removeLast() {
            int s = values.size();
            if (s == 0) {
                return null;
            }
            byte[] v = values.remove(s - 1);
            bytesSize -= v == null ? 0 : v.length;
            return v;
        }

        boolean canAppendAll(ListPackNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            if (this.isEmpty()) {
                return true;
            }
            if (this.values.size() + other.values.size() > QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return this.bytesSize + other.bytesSize <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(ListPackNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            for (byte[] v : other.values) {
                values.add(v);
            }
            bytesSize += other.bytesSize;
        }
    }
}
