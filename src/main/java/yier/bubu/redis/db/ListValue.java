package yier.bubu.redis.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using a small ring-buffer for packed lists and upgrading
    // to a quicklist-like deque of nodes once size/element thresholds are crossed.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;
    private static final int QUICKLIST_NODE_MAX_ENTRIES = 64;
    private static final int QUICKLIST_NODE_MAX_BYTES = 8 * 1024;

    private PackedList listpack = new PackedList();
    private ArrayDeque<ListNode> quicklist;
    private int totalSize = 0;

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public ValueEncoding encoding() {
        return quicklist != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
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
            listpack.addFirst(v);
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

        for (byte[] v : values) {
            listpack.addLast(v);
        }
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
            if (quicklist != null) {
                if (quicklist.isEmpty()) {
                    break;
                }
                byte[] v = qlPollFirst();
                totalSize--;
                out.add(v);
                continue;
            }

            if (listpack.isEmpty()) {
                break;
            }
            byte[] v = listpack.removeFirst();
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
            if (quicklist != null) {
                if (quicklist.isEmpty()) {
                    break;
                }
                byte[] v = qlPollLast();
                totalSize--;
                out.add(v);
                continue;
            }

            if (listpack.isEmpty()) {
                break;
            }
            byte[] v = listpack.removeLast();
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
            for (ListNode n : quicklist) {
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
            for (int i = normalizedStart; i <= normalizedStop; i++) {
                out.add(listpack.get(i));
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

        ArrayDeque<ListNode> out = new ArrayDeque<>();
        ListNode node = new ListNode();
        for (int i = 0; i < listpack.size(); i++) {
            byte[] v = listpack.get(i);
            if (!node.canAdd(v)) {
                out.addLast(node);
                node = new ListNode();
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
            quicklist.addFirst(new ListNode());
        }
        quicklist.peekFirst().addFirst(v);
        totalSize++;
    }

    private void qlAddLast(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekLast().canAdd(v)) {
            quicklist.addLast(new ListNode());
        }
        quicklist.peekLast().addLast(v);
        totalSize++;
    }

    private byte[] qlPollFirst() {
        if (quicklist.isEmpty()) {
            return null;
        }
        ListNode n = quicklist.peekFirst();
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
        ListNode n = quicklist.peekLast();
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
        ListNode first = quicklist.pollFirst();
        ListNode second = quicklist.pollFirst();
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
        ListNode last = quicklist.pollLast();
        ListNode prev = quicklist.pollLast();
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

    private static final class PackedList {
        private byte[][] elements = new byte[0][];
        private int head = 0;
        private int size = 0;
        private int rawBytesSize = 0;

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        byte[] get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            return elements[physicalIndex(index)];
        }

        void addFirst(byte[] v) {
            ensureCapacity(size + 1);
            head = dec(head, elements.length);
            elements[head] = v;
            size++;
            rawBytesSize += rawLen(v);
        }

        void addLast(byte[] v) {
            ensureCapacity(size + 1);
            elements[physicalIndex(size)] = v;
            size++;
            rawBytesSize += rawLen(v);
        }

        byte[] removeFirst() {
            if (size == 0) {
                return null;
            }
            byte[] v = elements[head];
            elements[head] = null;
            head = inc(head, elements.length);
            size--;
            rawBytesSize -= rawLen(v);
            return v;
        }

        byte[] removeLast() {
            if (size == 0) {
                return null;
            }
            int idx = physicalIndex(size - 1);
            byte[] v = elements[idx];
            elements[idx] = null;
            size--;
            rawBytesSize -= rawLen(v);
            return v;
        }

        int rawBytesSize() {
            return rawBytesSize;
        }

        private int physicalIndex(int logicalIndex) {
            int cap = elements.length;
            if (cap == 0) {
                return 0;
            }
            int idx = head + logicalIndex;
            return idx >= cap ? idx - cap : idx;
        }

        private void ensureCapacity(int desired) {
            int cap = elements.length;
            if (cap >= desired) {
                return;
            }
            int next = Math.max(8, cap);
            while (next < desired) {
                next <<= 1;
            }
            byte[][] out = new byte[next][];
            for (int i = 0; i < size; i++) {
                out[i] = elements[physicalIndex(i)];
            }
            elements = out;
            head = 0;
        }

        private static int rawLen(byte[] v) {
            return v == null ? 0 : v.length;
        }

        private static int inc(int i, int cap) {
            i++;
            return i == cap ? 0 : i;
        }

        private static int dec(int i, int cap) {
            i--;
            return i < 0 ? cap - 1 : i;
        }
    }

    private static final class ListNode {
        private final PackedList list = new PackedList();

        boolean isEmpty() {
            return list.isEmpty();
        }

        int size() {
            return list.size();
        }

        byte[] get(int index) {
            return list.get(index);
        }

        boolean canAdd(byte[] v) {
            if (list.isEmpty()) {
                return true;
            }
            if (list.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            int len = v == null ? 0 : v.length;
            return list.rawBytesSize() + len <= QUICKLIST_NODE_MAX_BYTES;
        }

        void addFirst(byte[] v) {
            list.addFirst(v);
        }

        void addLast(byte[] v) {
            list.addLast(v);
        }

        byte[] removeFirst() {
            return list.removeFirst();
        }

        byte[] removeLast() {
            return list.removeLast();
        }

        boolean canAppendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            if (this.size() + other.size() > QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return this.list.rawBytesSize() + other.list.rawBytesSize() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            for (int i = 0; i < other.size(); i++) {
                this.addLast(other.get(i));
            }
        }
    }
}
