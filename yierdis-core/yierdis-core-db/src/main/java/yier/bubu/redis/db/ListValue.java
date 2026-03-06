package yier.bubu.redis.db;

import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapListpack;
import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.ops.result.BulkStringSink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using a small ring-buffer for packed lists and upgrading
    // to a quicklist-like deque of nodes once size/element thresholds are crossed.
    private static final int QUICKLIST_NODE_MAX_BYTES = YierdisEncodingThresholds.LIST_MAX_LISTPACK_BYTES;

    private final OffHeapAddressAllocator offHeapAllocator;

    private YierdisListpack listpack;
    private ArrayDeque<ListNode> quicklist;

    private YierdisUnsafeOffHeapListpack listpackOffHeap;
    private ArrayDeque<OffHeapListNode> quicklistOffHeap;

    private int totalSize = 0;
    private long allocatedBytes = 0;

    ListValue() {
        this.offHeapAllocator = null;
        this.listpack = new YierdisListpack();
    }

    ListValue(OffHeapAddressAllocator allocator) {
        this.offHeapAllocator = allocator;
        this.listpackOffHeap = new YierdisUnsafeOffHeapListpack(allocator);
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public ValueEncoding encoding() {
        if (offHeapAllocator != null) {
            return quicklistOffHeap != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
        }
        return quicklist != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
    }

    int size() {
        return totalSize;
    }

    long estimatedBytes() {
        if (offHeapAllocator != null) {
            return 0;
        }
        return allocatedBytes;
    }

    void lpushAll(List<byte[]> values) {
        if (offHeapAllocator != null) {
            if (quicklistOffHeap != null) {
                for (byte[] v : values) {
                    qlAddFirstOffHeap(v);
                }
                return;
            }

            if (wouldExceedPackedBytesOffHeap(values)) {
                convertToQuickListOffHeap();
                lpushAll(values);
                return;
            }

            for (byte[] v : values) {
                listpackOffHeap.addFirst(v);
                totalSize++;
            }
            if (quicklistOffHeap == null && listpackOffHeap.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
                convertToQuickListOffHeap();
            }
            return;
        }

        if (quicklist != null) {
            for (byte[] v : values) {
                qlAddFirst(v);
            }
            return;
        }

        if (wouldExceedPackedBytes(values)) {
            convertToQuickList();
            lpushAll(values);
            return;
        }

        int beforeAlloc = listpack.allocatedBytes();
        for (byte[] v : values) {
            listpack.addFirst(v);
            totalSize++;
        }
        // Keep bytes in sync if we stayed in packed mode.
        if (quicklist == null) {
            allocatedBytes += listpack.allocatedBytes() - beforeAlloc;
        }
        if (listpack.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
            convertToQuickList();
        }
    }

    void rpushAll(List<byte[]> values) {
        if (offHeapAllocator != null) {
            if (quicklistOffHeap != null) {
                for (byte[] v : values) {
                    qlAddLastOffHeap(v);
                }
                return;
            }

            if (wouldExceedPackedBytesOffHeap(values)) {
                convertToQuickListOffHeap();
                rpushAll(values);
                return;
            }

            for (byte[] v : values) {
                listpackOffHeap.addLast(v);
            }
            totalSize += values.size();
            if (quicklistOffHeap == null && listpackOffHeap.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
                convertToQuickListOffHeap();
            }
            return;
        }

        if (quicklist != null) {
            for (byte[] v : values) {
                qlAddLast(v);
            }
            return;
        }

        if (wouldExceedPackedBytes(values)) {
            convertToQuickList();
            rpushAll(values);
            return;
        }

        int beforeAlloc = listpack.allocatedBytes();
        for (byte[] v : values) {
            listpack.addLast(v);
        }
        totalSize += values.size();
        // Keep bytes in sync if we stayed in packed mode.
        if (quicklist == null) {
            allocatedBytes += listpack.allocatedBytes() - beforeAlloc;
        }
        if (listpack.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
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
            if (offHeapAllocator != null) {
                if (quicklistOffHeap != null) {
                    if (quicklistOffHeap.isEmpty()) {
                        break;
                    }
                    byte[] v = qlPollFirstOffHeap();
                    totalSize--;
                    out.add(v);
                    continue;
                }

                if (listpackOffHeap.isEmpty()) {
                    break;
                }
                byte[] v = listpackOffHeap.removeFirst();
                totalSize--;
                out.add(v);
                continue;
            }

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
            if (offHeapAllocator != null) {
                if (quicklistOffHeap != null) {
                    if (quicklistOffHeap.isEmpty()) {
                        break;
                    }
                    byte[] v = qlPollLastOffHeap();
                    totalSize--;
                    out.add(v);
                    continue;
                }

                if (listpackOffHeap.isEmpty()) {
                    break;
                }
                byte[] v = listpackOffHeap.removeLast();
                totalSize--;
                out.add(v);
                continue;
            }

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
        if (offHeapAllocator != null) {
            if (quicklistOffHeap != null) {
                outer:
                for (OffHeapListNode n : quicklistOffHeap) {
                    YierdisUnsafeOffHeapListpack.Cursor c = n.cursor();
                    while (c.next()) {
                        if (idx > normalizedStop) {
                            break outer;
                        }
                        if (idx >= normalizedStart) {
                            out.add(c.toByteArray());
                        }
                        idx++;
                    }
                }
                return out;
            }

            YierdisUnsafeOffHeapListpack.Cursor c = listpackOffHeap.cursor();
            while (c.next()) {
                if (idx > normalizedStop) {
                    break;
                }
                if (idx >= normalizedStart) {
                    out.add(c.toByteArray());
                }
                idx++;
            }
            return out;
        }

        if (quicklist != null) {
            outer:
            for (ListNode n : quicklist) {
                YierdisListpack.Cursor c = n.cursor();
                while (c.next()) {
                    if (idx > normalizedStop) {
                        break outer;
                    }
                    if (idx >= normalizedStart) {
                        out.add(c.toByteArray());
                    }
                    idx++;
                }
            }
        } else {
            YierdisListpack.Cursor c = listpack.cursor();
            while (c.next()) {
                if (idx > normalizedStop) {
                    break;
                }
                if (idx >= normalizedStart) {
                    out.add(c.toByteArray());
                }
                idx++;
            }
        }
        return out;
    }

    int rangeCount(int start, int stop) {
        int size = size();
        if (size == 0) {
            return 0;
        }

        int normalizedStart = normalizeIndex(start, size);
        int normalizedStop = normalizeIndex(stop, size);

        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return 0;
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1;
        }
        if (normalizedStart > normalizedStop) {
            return 0;
        }
        return normalizedStop - normalizedStart + 1;
    }

    void rangeInto(int start, int stop, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        int size = size();
        if (size == 0) {
            return;
        }

        int normalizedStart = normalizeIndex(start, size);
        int normalizedStop = normalizeIndex(stop, size);

        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return;
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1;
        }
        if (normalizedStart > normalizedStop) {
            return;
        }

        int idx = 0;
        if (offHeapAllocator != null) {
            if (quicklistOffHeap != null) {
                outer:
                for (OffHeapListNode n : quicklistOffHeap) {
                    YierdisUnsafeOffHeapListpack.Cursor c = n.cursor();
                    while (c.next()) {
                        if (idx > normalizedStop) {
                            break outer;
                        }
                        if (idx >= normalizedStart) {
                            c.writeTo(out);
                        }
                        idx++;
                    }
                }
                return;
            }

            YierdisUnsafeOffHeapListpack.Cursor c = listpackOffHeap.cursor();
            while (c.next()) {
                if (idx > normalizedStop) {
                    break;
                }
                if (idx >= normalizedStart) {
                    c.writeTo(out);
                }
                idx++;
            }
            return;
        }

        if (quicklist != null) {
            outer:
            for (ListNode n : quicklist) {
                YierdisListpack.Cursor c = n.cursor();
                while (c.next()) {
                    if (idx > normalizedStop) {
                        break outer;
                    }
                    if (idx >= normalizedStart) {
                        c.writeTo(out);
                    }
                    idx++;
                }
            }
            return;
        }

        YierdisListpack.Cursor c = listpack.cursor();
        while (c.next()) {
            if (idx > normalizedStop) {
                break;
            }
            if (idx >= normalizedStart) {
                c.writeTo(out);
            }
            idx++;
        }
    }

    private static int normalizeIndex(int idx, int size) {
        if (idx >= 0) {
            return idx;
        }
        return size + idx;
    }

    private boolean wouldExceedPackedBytes(List<byte[]> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return false;
        }
        int predicted = listpack.encodedBytes();
        for (byte[] v : incoming) {
            predicted += entryEncodedBytes(v);
            if (predicted > QUICKLIST_NODE_MAX_BYTES) {
                return true;
            }
        }
        return predicted > QUICKLIST_NODE_MAX_BYTES;
    }

    private boolean wouldExceedPackedBytesOffHeap(List<byte[]> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return false;
        }
        int predicted = listpackOffHeap.encodedBytes();
        for (byte[] v : incoming) {
            predicted += entryEncodedBytes(v);
            if (predicted > QUICKLIST_NODE_MAX_BYTES) {
                return true;
            }
        }
        return predicted > QUICKLIST_NODE_MAX_BYTES;
    }

    private void convertToQuickList() {
        if (quicklist != null) {
            return;
        }

        allocatedBytes = 0;
        ArrayDeque<ListNode> out = new ArrayDeque<>();
        ListNode node = new ListNode();
        YierdisListpack.Cursor c = listpack.cursor();
        while (c.next()) {
            int entryBytes = entryEncodedBytes(c);
            if (!node.canAddEntry(entryBytes)) {
                allocatedBytes += node.allocatedBytes();
                out.addLast(node);
                node = new ListNode();
            }
            node.addLast(c);
        }
        if (!node.isEmpty()) {
            allocatedBytes += node.allocatedBytes();
            out.addLast(node);
        }

        this.quicklist = out;
        this.listpack = null;
    }

    private void convertToQuickListOffHeap() {
        if (quicklistOffHeap != null) {
            return;
        }

        ArrayDeque<OffHeapListNode> out = new ArrayDeque<>();
        OffHeapListNode node = new OffHeapListNode(offHeapAllocator);
        YierdisUnsafeOffHeapListpack.Cursor c = listpackOffHeap.cursor();
        while (c.next()) {
            int entryBytes = entryEncodedBytes(c.isNull() ? -1 : c.length());
            if (!node.canAddEntry(entryBytes)) {
                out.addLast(node);
                node = new OffHeapListNode(offHeapAllocator);
            }
            node.addLast(c.toByteArray());
        }
        if (!node.isEmpty()) {
            out.addLast(node);
        }

        listpackOffHeap.close();
        listpackOffHeap = null;
        this.quicklistOffHeap = out;
    }

    private void qlAddFirst(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekFirst().canAdd(v)) {
            quicklist.addFirst(new ListNode());
        }
        ListNode n = quicklist.peekFirst();
        int before = n.allocatedBytes();
        n.addFirst(v);
        allocatedBytes += n.allocatedBytes() - before;
        totalSize++;
    }

    private void qlAddLast(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekLast().canAdd(v)) {
            quicklist.addLast(new ListNode());
        }
        ListNode n = quicklist.peekLast();
        int before = n.allocatedBytes();
        n.addLast(v);
        allocatedBytes += n.allocatedBytes() - before;
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
            allocatedBytes -= n.allocatedBytes();
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
            allocatedBytes -= n.allocatedBytes();
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
            long before = (long) first.allocatedBytes() + second.allocatedBytes();
            first.appendAll(second);
            allocatedBytes += (long) first.allocatedBytes() - before;
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
            long before = (long) prev.allocatedBytes() + last.allocatedBytes();
            prev.appendAll(last);
            allocatedBytes += (long) prev.allocatedBytes() - before;
            quicklist.addLast(prev);
            return;
        }

        // Restore original order: prev, last
        quicklist.addLast(prev);
        quicklist.addLast(last);
    }

    private void qlAddFirstOffHeap(byte[] v) {
        if (quicklistOffHeap.isEmpty() || !quicklistOffHeap.peekFirst().canAdd(v)) {
            quicklistOffHeap.addFirst(new OffHeapListNode(offHeapAllocator));
        }
        OffHeapListNode n = quicklistOffHeap.peekFirst();
        n.addFirst(v);
        totalSize++;
    }

    private void qlAddLastOffHeap(byte[] v) {
        if (quicklistOffHeap.isEmpty() || !quicklistOffHeap.peekLast().canAdd(v)) {
            quicklistOffHeap.addLast(new OffHeapListNode(offHeapAllocator));
        }
        OffHeapListNode n = quicklistOffHeap.peekLast();
        n.addLast(v);
        totalSize++;
    }

    private byte[] qlPollFirstOffHeap() {
        if (quicklistOffHeap.isEmpty()) {
            return null;
        }
        OffHeapListNode n = quicklistOffHeap.peekFirst();
        byte[] v = n.removeFirst();
        if (n.isEmpty()) {
            quicklistOffHeap.removeFirst();
            n.close();
        }
        maybeMergeFirstTwoOffHeap();
        return v;
    }

    private byte[] qlPollLastOffHeap() {
        if (quicklistOffHeap.isEmpty()) {
            return null;
        }
        OffHeapListNode n = quicklistOffHeap.peekLast();
        byte[] v = n.removeLast();
        if (n.isEmpty()) {
            quicklistOffHeap.removeLast();
            n.close();
        }
        maybeMergeLastTwoOffHeap();
        return v;
    }

    private void maybeMergeFirstTwoOffHeap() {
        if (quicklistOffHeap.size() < 2) {
            return;
        }
        OffHeapListNode first = quicklistOffHeap.pollFirst();
        OffHeapListNode second = quicklistOffHeap.pollFirst();
        if (first == null || second == null) {
            if (second != null) {
                quicklistOffHeap.addFirst(second);
            }
            if (first != null) {
                quicklistOffHeap.addFirst(first);
            }
            return;
        }

        if (first.canAppendAll(second)) {
            first.appendAll(second);
            second.close();
            quicklistOffHeap.addFirst(first);
            return;
        }

        quicklistOffHeap.addFirst(second);
        quicklistOffHeap.addFirst(first);
    }

    private void maybeMergeLastTwoOffHeap() {
        if (quicklistOffHeap.size() < 2) {
            return;
        }
        OffHeapListNode last = quicklistOffHeap.pollLast();
        OffHeapListNode prev = quicklistOffHeap.pollLast();
        if (last == null || prev == null) {
            if (prev != null) {
                quicklistOffHeap.addLast(prev);
            }
            if (last != null) {
                quicklistOffHeap.addLast(last);
            }
            return;
        }

        if (prev.canAppendAll(last)) {
            prev.appendAll(last);
            last.close();
            quicklistOffHeap.addLast(prev);
            return;
        }

        quicklistOffHeap.addLast(prev);
        quicklistOffHeap.addLast(last);
    }

    private static final class ListNode {
        private final YierdisListpack listpack = new YierdisListpack();

        int allocatedBytes() {
            return listpack.allocatedBytes();
        }

        boolean isEmpty() {
            return listpack.isEmpty();
        }

        int size() {
            return listpack.size();
        }

        YierdisListpack.Cursor cursor() {
            return listpack.cursor();
        }

        boolean canAdd(byte[] v) {
            return canAddEntry(entryEncodedBytes(v));
        }

        boolean canAddEntry(int entryBytes) {
            if (entryBytes < 0) {
                throw new IllegalArgumentException("entryBytes must be >= 0");
            }
            if (listpack.isEmpty()) {
                return true;
            }
            return listpack.encodedBytes() + entryBytes <= QUICKLIST_NODE_MAX_BYTES;
        }

        void addFirst(byte[] v) {
            listpack.addFirst(v);
        }

        void addLast(byte[] v) {
            listpack.addLast(v);
        }

        void addLast(YierdisListpack.Cursor c) {
            c.appendTo(listpack);
        }

        byte[] removeFirst() {
            return listpack.removeFirst();
        }

        byte[] removeLast() {
            return listpack.removeLast();
        }

        boolean canAppendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            return this.listpack.encodedBytes() + other.listpack.encodedBytes() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            YierdisListpack.Cursor c = other.listpack.cursor();
            while (c.next()) {
                c.appendTo(this.listpack);
            }
        }
    }

    private static final class OffHeapListNode implements AutoCloseable {
        private final YierdisUnsafeOffHeapListpack listpack;

        private OffHeapListNode(OffHeapAddressAllocator allocator) {
            this.listpack = new YierdisUnsafeOffHeapListpack(allocator);
        }

        boolean isEmpty() {
            return listpack.isEmpty();
        }

        YierdisUnsafeOffHeapListpack.Cursor cursor() {
            return listpack.cursor();
        }

        boolean canAdd(byte[] v) {
            return canAddEntry(entryEncodedBytes(v));
        }

        boolean canAddEntry(int entryBytes) {
            if (entryBytes < 0) {
                throw new IllegalArgumentException("entryBytes must be >= 0");
            }
            if (listpack.isEmpty()) {
                return true;
            }
            return listpack.encodedBytes() + entryBytes <= QUICKLIST_NODE_MAX_BYTES;
        }

        void addFirst(byte[] v) {
            listpack.addFirst(v);
        }

        void addLast(byte[] v) {
            listpack.addLast(v);
        }

        byte[] removeFirst() {
            return listpack.removeFirst();
        }

        byte[] removeLast() {
            return listpack.removeLast();
        }

        boolean canAppendAll(OffHeapListNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            return this.listpack.encodedBytes() + other.listpack.encodedBytes() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(OffHeapListNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            YierdisUnsafeOffHeapListpack.Cursor c = other.listpack.cursor();
            while (c.next()) {
                listpack.addLast(c.toByteArray());
            }
        }

        @Override
        public void close() {
            listpack.close();
        }
    }

    private static int entryEncodedBytes(YierdisListpack.Cursor c) {
        if (c == null) {
            throw new IllegalArgumentException("cursor must not be null");
        }
        int len = c.isNull() ? -1 : c.length();
        int headerValue = len < 0 ? 0 : len + 1;
        return varIntSize(headerValue) + Math.max(0, len);
    }

    private static int entryEncodedBytes(byte[] v) {
        int len = v == null ? -1 : v.length;
        int headerValue = len < 0 ? 0 : len + 1;
        return varIntSize(headerValue) + Math.max(0, len);
    }

    private static int entryEncodedBytes(int len) {
        int headerValue = len < 0 ? 0 : len + 1;
        return varIntSize(headerValue) + Math.max(0, len);
    }

    @Override
    public void close() {
        if (offHeapAllocator == null) {
            return;
        }
        if (listpackOffHeap != null) {
            listpackOffHeap.close();
            listpackOffHeap = null;
        }
        if (quicklistOffHeap != null) {
            for (OffHeapListNode n : quicklistOffHeap) {
                n.close();
            }
            quicklistOffHeap.clear();
            quicklistOffHeap = null;
        }
    }

    private static int varIntSize(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }
}
