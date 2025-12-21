package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Arrays;
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

    private PackedListPack listpack = new PackedListPack();
    private ArrayDeque<ListPackNode> quicklist;
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
            byte[] v = pollListpackFirst();
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
            byte[] v = pollListpackLast();
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

        ArrayDeque<ListPackNode> out = new ArrayDeque<>();
        ListPackNode node = new ListPackNode();
        for (int i = 0; i < listpack.size(); i++) {
            if (!node.canAddFrom(listpack, i)) {
                out.addLast(node);
                node = new ListPackNode();
            }
            node.addLastFrom(listpack, i);
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
        return listpack.removeFirst();
    }

    private byte[] pollListpackLast() {
        int size = listpack.size();
        if (size == 0) {
            return null;
        }
        return listpack.removeLast();
    }

    private static final class PackedListPack {
        private byte[] blob = new byte[0];
        private int[] offsets = new int[0];
        private int size = 0;
        private int rawBytesSize = 0;

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        int rawBytesSize() {
            return rawBytesSize;
        }

        byte[] get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            int p = offsets[index];
            long r = readVarint(blob, p);
            int storedLen = (int) r;
            p = (int) (r >>> 32);
            if (storedLen == 0) {
                return null;
            }
            int len = storedLen - 1;
            return Arrays.copyOfRange(blob, p, p + len);
        }

        int elementRawLen(int index) {
            int p = offsets[index];
            long r = readVarint(blob, p);
            int storedLen = (int) r;
            return storedLen == 0 ? 0 : storedLen - 1;
        }

        void appendElementTo(int index, PackedListPack dest) {
            int p = offsets[index];
            long r = readVarint(blob, p);
            int storedLen = (int) r;
            p = (int) (r >>> 32);
            if (storedLen == 0) {
                dest.addLastNull();
                return;
            }
            int len = storedLen - 1;
            dest.addLastFrom(blob, p, len);
        }

        void addFirst(byte[] v) {
            int rawLen = v == null ? 0 : v.length;
            int storedLen = v == null ? 0 : rawLen + 1;
            int encodedLen = varintLength(storedLen) + rawLen;

            byte[] next = new byte[blob.length + encodedLen];
            int w = 0;
            w = writeVarint(next, w, storedLen);
            if (rawLen > 0) {
                System.arraycopy(v, 0, next, w, rawLen);
            }
            System.arraycopy(blob, 0, next, encodedLen, blob.length);

            int[] nextOffsets = new int[size + 1];
            nextOffsets[0] = 0;
            for (int i = 0; i < size; i++) {
                nextOffsets[i + 1] = offsets[i] + encodedLen;
            }

            blob = next;
            offsets = nextOffsets;
            size++;
            rawBytesSize += rawLen;
        }

        void addLast(byte[] v) {
            if (v == null) {
                addLastNull();
                return;
            }
            addLastFrom(v, 0, v.length);
        }

        void addLastNull() {
            int pairStart = blob.length;
            byte[] next = Arrays.copyOf(blob, pairStart + 1);
            next[pairStart] = 0; // storedLen=0 => null

            blob = next;
            offsets = Arrays.copyOf(offsets, size + 1);
            offsets[size] = pairStart;
            size++;
        }

        void addLastFrom(byte[] src, int off, int len) {
            int storedLen = len + 1;
            int encodedLen = varintLength(storedLen) + len;

            int elemStart = blob.length;
            byte[] next = new byte[elemStart + encodedLen];
            System.arraycopy(blob, 0, next, 0, blob.length);

            int w = elemStart;
            w = writeVarint(next, w, storedLen);
            if (len > 0) {
                System.arraycopy(src, off, next, w, len);
            }

            blob = next;
            offsets = Arrays.copyOf(offsets, size + 1);
            offsets[size] = elemStart;
            size++;
            rawBytesSize += len;
        }

        byte[] removeFirst() {
            if (size == 0) {
                return null;
            }
            int start = offsets[0];
            long r = readVarint(blob, start);
            int storedLen = (int) r;
            int p = (int) (r >>> 32);
            int len = storedLen == 0 ? 0 : storedLen - 1;
            int end = p + len;

            byte[] value = storedLen == 0 ? null : Arrays.copyOfRange(blob, p, end);

            byte[] next = new byte[blob.length - end];
            System.arraycopy(blob, end, next, 0, blob.length - end);

            int[] nextOffsets = new int[size - 1];
            for (int i = 1; i < size; i++) {
                nextOffsets[i - 1] = offsets[i] - end;
            }

            blob = next;
            offsets = nextOffsets;
            size--;
            rawBytesSize -= len;
            return value;
        }

        byte[] removeLast() {
            int s = size;
            if (s == 0) {
                return null;
            }
            int start = offsets[s - 1];
            long r = readVarint(blob, start);
            int storedLen = (int) r;
            int p = (int) (r >>> 32);
            int len = storedLen == 0 ? 0 : storedLen - 1;
            int end = p + len;

            byte[] value = storedLen == 0 ? null : Arrays.copyOfRange(blob, p, end);

            blob = Arrays.copyOf(blob, start);
            offsets = Arrays.copyOf(offsets, s - 1);
            size--;
            rawBytesSize -= len;
            return value;
        }

        void appendAll(PackedListPack other) {
            if (other == null || other.size == 0) {
                return;
            }
            if (this.size == 0) {
                this.blob = Arrays.copyOf(other.blob, other.blob.length);
                this.offsets = Arrays.copyOf(other.offsets, other.offsets.length);
                this.size = other.size;
                this.rawBytesSize = other.rawBytesSize;
                return;
            }

            int base = this.blob.length;
            byte[] nextBlob = new byte[this.blob.length + other.blob.length];
            System.arraycopy(this.blob, 0, nextBlob, 0, this.blob.length);
            System.arraycopy(other.blob, 0, nextBlob, this.blob.length, other.blob.length);

            int[] nextOffsets = Arrays.copyOf(this.offsets, this.size + other.size);
            for (int i = 0; i < other.size; i++) {
                nextOffsets[this.size + i] = base + other.offsets[i];
            }

            this.blob = nextBlob;
            this.offsets = nextOffsets;
            this.size += other.size;
            this.rawBytesSize += other.rawBytesSize;
        }

        private static int varintLength(int v) {
            int len = 1;
            while ((v & ~0x7F) != 0) {
                v >>>= 7;
                len++;
            }
            return len;
        }

        private static int writeVarint(byte[] out, int pos, int v) {
            while ((v & ~0x7F) != 0) {
                out[pos++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            out[pos++] = (byte) v;
            return pos;
        }

        private static long readVarint(byte[] buf, int pos) {
            int result = 0;
            int shift = 0;
            while (true) {
                int b = buf[pos++] & 0xFF;
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
                if (shift > 28) {
                    throw new IllegalStateException("varint too long");
                }
            }
            return (((long) pos) << 32) | (result & 0xffffffffL);
        }
    }

    private static final class ListPackNode {
        private final PackedListPack pack = new PackedListPack();

        boolean canAdd(byte[] v) {
            int len = v == null ? 0 : v.length;
            if (pack.isEmpty()) {
                return true;
            }
            if (pack.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return pack.rawBytesSize() + len <= QUICKLIST_NODE_MAX_BYTES;
        }

        boolean canAddFrom(PackedListPack src, int elementIndex) {
            int len = src.elementRawLen(elementIndex);
            if (pack.isEmpty()) {
                return true;
            }
            if (pack.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return pack.rawBytesSize() + len <= QUICKLIST_NODE_MAX_BYTES;
        }

        boolean isEmpty() {
            return pack.isEmpty();
        }

        int size() {
            return pack.size();
        }

        byte[] get(int i) {
            return pack.get(i);
        }

        void addFirst(byte[] v) {
            pack.addFirst(v);
        }

        void addLast(byte[] v) {
            pack.addLast(v);
        }

        void addLastFrom(PackedListPack src, int elementIndex) {
            src.appendElementTo(elementIndex, pack);
        }

        byte[] removeFirst() {
            return pack.removeFirst();
        }

        byte[] removeLast() {
            return pack.removeLast();
        }

        boolean canAppendAll(ListPackNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            if (this.isEmpty()) {
                return true;
            }
            if (this.pack.size() + other.pack.size() > QUICKLIST_NODE_MAX_ENTRIES) {
                return false;
            }
            return this.pack.rawBytesSize() + other.pack.rawBytesSize() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(ListPackNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            pack.appendAll(other.pack);
        }
    }
}
