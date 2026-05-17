package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmListpack;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ListValue implements YierdisValue {
    // Redis stores small lists in a compact encoding and upgrades to quicklist as needed.
    // We approximate that behavior by using a small ring-buffer for packed lists and upgrading
    // to a quicklist-like deque of nodes once size/element thresholds are crossed.
    private static final int QUICKLIST_NODE_MAX_BYTES = YierdisEncodingThresholds.LIST_MAX_LISTPACK_BYTES;
    private static final int QUICKLIST_NODE_RECORD_BYTES = 48;
    private static final int QUICKLIST_NODE_OWNER_ROOT_OFFSET = 0;
    private static final int QUICKLIST_NODE_PREV_OFFSET = 8;
    private static final int QUICKLIST_NODE_NEXT_OFFSET = 16;
    private static final int QUICKLIST_NODE_PAYLOAD_REF_OFFSET = 24;
    private static final int QUICKLIST_NODE_ENTRY_COUNT_OFFSET = 32;
    private static final int QUICKLIST_NODE_ENCODED_BYTES_OFFSET = 36;
    private static final int QUICKLIST_NODE_FLAGS_OFFSET = 40;
    private static final int QUICKLIST_NODE_RESERVED_OFFSET = 44;

    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final YierdisFfmBlobStore ffmBlobStore;
    private final NativeAllocator nativeAllocator;
    private final NativeHandle rootHandle;

    private YierdisListpack listpack;
    private ArrayDeque<ListNode> quicklist;

    private YierdisFfmListpack listpackFfm;
    private ArrayDeque<FfmListNode> quicklistFfm;

    private int totalSize = 0;
    private long allocatedBytes = 0;

    public ListValue() {
        this.memoryRuntime = null;
        this.ffmBlobStore = null;
        this.nativeAllocator = null;
        this.rootHandle = null;
        this.listpack = new YierdisListpack();
    }

    public ListValue(YierdisFfmMemoryRuntime memoryRuntime) {
        throw new UnsupportedOperationException("FFM ListValue requires ListRoot native allocator ownership");
    }

    public ListValue(YierdisFfmMemoryRuntime memoryRuntime, NativeAllocator nativeAllocator, NativeHandle rootHandle) {
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.ffmBlobStore = new YierdisFfmBlobStore(memoryRuntime, "list");
        this.nativeAllocator = Objects.requireNonNull(nativeAllocator, "nativeAllocator");
        this.rootHandle = Objects.requireNonNull(rootHandle, "rootHandle");
        if (rootHandle.domain() != NativeObjectKind.LIST_NODE.domain()
                || rootHandle.kindCode() != NativeObjectKind.LIST_NODE.code()) {
            throw new IllegalArgumentException("rootHandle must be a LIST_NODE handle: " + rootHandle.raw());
        }
        this.listpackFfm = new YierdisFfmListpack(ffmBlobStore);
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public ValueEncoding encoding() {
        if (memoryRuntime != null) {
            return quicklistFfm != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
        }
        return quicklist != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
    }

    static int quicklistNodeMaxBytesForTesting() {
        return QUICKLIST_NODE_MAX_BYTES;
    }

    int quicklistNodeCountForTesting() {
        return quicklistFfm == null ? 0 : quicklistFfm.size();
    }

    public int size() {
        return totalSize;
    }

    public long estimatedBytes() {
        if (memoryRuntime != null) {
            return ffmBlobStore.liveBytes();
        }
        return allocatedBytes;
    }

    public void lpushAll(List<byte[]> values) {
        if (memoryRuntime != null) {
            if (quicklistFfm != null) {
                for (byte[] v : values) {
                    qlAddFirstFfm(v);
                }
                return;
            }

            if (wouldExceedPackedBytesFfm(values)) {
                convertToQuickListFfm();
                lpushAll(values);
                return;
            }

            for (byte[] v : values) {
                listpackFfm.addFirst(v);
                totalSize++;
            }
            if (quicklistFfm == null && listpackFfm.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
                convertToQuickListFfm();
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

    public void rpushAll(List<byte[]> values) {
        if (memoryRuntime != null) {
            if (quicklistFfm != null) {
                for (byte[] v : values) {
                    qlAddLastFfm(v);
                }
                return;
            }

            if (wouldExceedPackedBytesFfm(values)) {
                convertToQuickListFfm();
                rpushAll(values);
                return;
            }

            for (byte[] v : values) {
                listpackFfm.addLast(v);
            }
            totalSize += values.size();
            if (quicklistFfm == null && listpackFfm.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
                convertToQuickListFfm();
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

    public List<byte[]> lpop(int count) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        int expected = Math.min(count, totalSize);
        List<byte[]> out = new ArrayList<>(expected);
        for (int i = 0; i < count; i++) {
            if (memoryRuntime != null) {
                if (quicklistFfm != null) {
                    if (quicklistFfm.isEmpty()) {
                        break;
                    }
                    byte[] v = qlPollFirstFfm();
                    out.add(v);
                    continue;
                }

                if (listpackFfm.isEmpty()) {
                    break;
                }
                byte[] v = listpackFfm.removeFirst();
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

    public List<byte[]> rpop(int count) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        int expected = Math.min(count, totalSize);
        List<byte[]> out = new ArrayList<>(expected);
        for (int i = 0; i < count; i++) {
            if (memoryRuntime != null) {
                if (quicklistFfm != null) {
                    if (quicklistFfm.isEmpty()) {
                        break;
                    }
                    byte[] v = qlPollLastFfm();
                    out.add(v);
                    continue;
                }

                if (listpackFfm.isEmpty()) {
                    break;
                }
                byte[] v = listpackFfm.removeLast();
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

    public List<byte[]> range(int start, int stop) {
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
        if (memoryRuntime != null) {
            if (quicklistFfm != null) {
                outer:
                for (FfmListNode n : quicklistFfm) {
                    YierdisFfmListpack.Cursor c = n.cursor();
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

            YierdisFfmListpack.Cursor c = listpackFfm.cursor();
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

    public int rangeCount(int start, int stop) {
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

    public void rangeInto(int start, int stop, BulkStringSink out) {
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
        if (memoryRuntime != null) {
            if (quicklistFfm != null) {
                outer:
                for (FfmListNode n : quicklistFfm) {
                    YierdisFfmListpack.Cursor c = n.cursor();
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

            YierdisFfmListpack.Cursor c = listpackFfm.cursor();
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

    private boolean wouldExceedPackedBytesFfm(List<byte[]> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return false;
        }
        int predicted = listpackFfm.encodedBytes();
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

    private void convertToQuickListFfm() {
        if (quicklistFfm != null) {
            return;
        }

        ArrayDeque<FfmListNode> out = new ArrayDeque<>();
        FfmListNode node = null;
        try {
            node = newFfmListNode();
            YierdisFfmListpack.Cursor c = listpackFfm.cursor();
            while (c.next()) {
                int entryBytes = entryEncodedBytes(c.isNull() ? -1 : c.length());
                if (!node.canAddEntry(entryBytes)) {
                    out.addLast(node);
                    node = newFfmListNode();
                }
                node.addLast(c.toByteArray());
            }
            if (!node.isEmpty()) {
                out.addLast(node);
            } else {
                try {
                    node.close();
                } finally {
                    node = null;
                }
            }

            refreshFfmNodeMetadataLinks(out);
            YierdisFfmListpack packed = listpackFfm;
            packed.close();
            listpackFfm = null;
            quicklistFfm = out;
        } catch (RuntimeException | Error e) {
            closeFfmNodes(out, e);
            if (node != null) {
                closeFfmNode(node, e);
            }
            throw e;
        }
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

    private void qlAddFirstFfm(byte[] v) {
        if (quicklistFfm.isEmpty() || !quicklistFfm.peekFirst().canAdd(v)) {
            quicklistFfm.addFirst(newFfmListNode());
        }
        FfmListNode n = quicklistFfm.peekFirst();
        n.addFirst(v);
        totalSize++;
        refreshFfmNodeMetadataLinks();
    }

    private void qlAddLastFfm(byte[] v) {
        if (quicklistFfm.isEmpty() || !quicklistFfm.peekLast().canAdd(v)) {
            quicklistFfm.addLast(newFfmListNode());
        }
        FfmListNode n = quicklistFfm.peekLast();
        n.addLast(v);
        totalSize++;
        refreshFfmNodeMetadataLinks();
    }

    private byte[] qlPollFirstFfm() {
        if (quicklistFfm.isEmpty()) {
            return null;
        }
        FfmListNode n = quicklistFfm.peekFirst();
        byte[] v = n.removeFirst();
        totalSize--;
        FfmListNode removedNode = null;
        Throwable failure = null;
        if (n.isEmpty()) {
            removedNode = quicklistFfm.removeFirst();
        }
        try {
            if (removedNode != null) {
                refreshFfmNodeMetadataLinks();
            }
            maybeMergeFirstTwoFfm();
            refreshFfmNodeMetadataLinks();
            return v;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            if (removedNode != null) {
                closeRemovedFfmNode(removedNode, failure);
            }
        }
    }

    private byte[] qlPollLastFfm() {
        if (quicklistFfm.isEmpty()) {
            return null;
        }
        FfmListNode n = quicklistFfm.peekLast();
        byte[] v = n.removeLast();
        totalSize--;
        FfmListNode removedNode = null;
        Throwable failure = null;
        if (n.isEmpty()) {
            removedNode = quicklistFfm.removeLast();
        }
        try {
            if (removedNode != null) {
                refreshFfmNodeMetadataLinks();
            }
            maybeMergeLastTwoFfm();
            refreshFfmNodeMetadataLinks();
            return v;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            if (removedNode != null) {
                closeRemovedFfmNode(removedNode, failure);
            }
        }
    }

    private void maybeMergeFirstTwoFfm() {
        if (quicklistFfm.size() < 2) {
            return;
        }
        FfmListNode first = quicklistFfm.peekFirst();
        FfmListNode second = secondFfmNodeFromFirst();
        if (first.canAppendAll(second)) {
            first.appendAll(second);
            quicklistFfm.remove(second);
            Throwable failure = null;
            try {
                refreshFfmNodeMetadataLinks();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                closeRemovedFfmNode(second, failure);
            }
            return;
        }
        refreshFfmNodeMetadataLinks();
    }

    private void maybeMergeLastTwoFfm() {
        if (quicklistFfm.size() < 2) {
            return;
        }
        FfmListNode last = quicklistFfm.peekLast();
        FfmListNode prev = secondFfmNodeFromLast();
        if (prev.canAppendAll(last)) {
            prev.appendAll(last);
            quicklistFfm.remove(last);
            Throwable failure = null;
            try {
                refreshFfmNodeMetadataLinks();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                closeRemovedFfmNode(last, failure);
            }
            return;
        }
        refreshFfmNodeMetadataLinks();
    }

    private FfmListNode newFfmListNode() {
        return new FfmListNode(ffmBlobStore, nativeAllocator, rootHandle);
    }

    private void refreshFfmNodeMetadataLinks() {
        refreshFfmNodeMetadataLinks(quicklistFfm);
    }

    private void refreshFfmNodeMetadataLinks(ArrayDeque<FfmListNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        FfmListNode prev = null;
        for (FfmListNode node : nodes) {
            if (prev != null) {
                prev.writeMetadata(prev.prevRawDuringRefresh, node.rawHandle());
            }
            node.prevRawDuringRefresh = prev == null ? 0L : prev.rawHandle();
            prev = node;
        }
        if (prev != null) {
            prev.writeMetadata(prev.prevRawDuringRefresh, 0L);
        }
    }

    private FfmListNode secondFfmNodeFromFirst() {
        java.util.Iterator<FfmListNode> iterator = quicklistFfm.iterator();
        iterator.next();
        return iterator.next();
    }

    private FfmListNode secondFfmNodeFromLast() {
        java.util.Iterator<FfmListNode> iterator = quicklistFfm.descendingIterator();
        iterator.next();
        return iterator.next();
    }

    private static void closeFfmNodes(ArrayDeque<FfmListNode> nodes, Throwable failure) {
        for (FfmListNode node : nodes) {
            closeFfmNode(node, failure);
        }
        nodes.clear();
    }

    private static void closeFfmNode(FfmListNode node, Throwable failure) {
        try {
            node.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void closeRemovedFfmNode(FfmListNode node, Throwable failure) {
        try {
            node.close();
        } catch (RuntimeException | Error closeFailure) {
            if (failure != null) {
                failure.addSuppressed(closeFailure);
                return;
            }
            throw closeFailure;
        }
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
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

    private static final class FfmListNode implements AutoCloseable {
        private final NativeAllocator allocator;
        private final NativeHandle rootHandle;
        private NativeHandle nodeHandle;
        private YierdisFfmListpack listpack;
        private long prevRawDuringRefresh;
        private boolean payloadClosed;
        private boolean nodeFreed;

        private FfmListNode(YierdisFfmBlobStore blobStore, NativeAllocator allocator, NativeHandle rootHandle) {
            this.allocator = Objects.requireNonNull(allocator, "allocator");
            this.rootHandle = Objects.requireNonNull(rootHandle, "rootHandle");
            NativeHandle allocated = this.allocator.allocate(
                    NativeObjectKind.LIST_QUICKLIST_NODE,
                    QUICKLIST_NODE_RECORD_BYTES
            );
            try {
                this.listpack = new YierdisFfmListpack(blobStore);
            } catch (RuntimeException | Error e) {
                try {
                    this.allocator.free(allocated);
                } catch (RuntimeException freeFailure) {
                    e.addSuppressed(freeFailure);
                }
                throw e;
            }
            this.nodeHandle = allocated;
            this.nodeFreed = false;
            try {
                writeMetadata(0L, 0L);
            } catch (RuntimeException | Error e) {
                try {
                    this.listpack.close();
                    this.payloadClosed = true;
                } catch (RuntimeException | Error closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                try {
                    this.allocator.free(allocated);
                    this.nodeFreed = true;
                } catch (RuntimeException freeFailure) {
                    e.addSuppressed(freeFailure);
                }
                throw e;
            }
        }

        boolean isEmpty() {
            return liveListpack().isEmpty();
        }

        YierdisFfmListpack.Cursor cursor() {
            return liveListpack().cursor();
        }

        boolean canAdd(byte[] v) {
            return canAddEntry(entryEncodedBytes(v));
        }

        boolean canAddEntry(int entryBytes) {
            if (entryBytes < 0) {
                throw new IllegalArgumentException("entryBytes must be >= 0");
            }
            YierdisFfmListpack current = liveListpack();
            if (current.isEmpty()) {
                return true;
            }
            return current.encodedBytes() + entryBytes <= QUICKLIST_NODE_MAX_BYTES;
        }

        void addFirst(byte[] v) {
            liveListpack().addFirst(v);
        }

        void addLast(byte[] v) {
            liveListpack().addLast(v);
        }

        byte[] removeFirst() {
            return liveListpack().removeFirst();
        }

        byte[] removeLast() {
            return liveListpack().removeLast();
        }

        boolean canAppendAll(FfmListNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            return this.liveListpack().encodedBytes() + other.liveListpack().encodedBytes() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(FfmListNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            int appended = 0;
            YierdisFfmListpack current = liveListpack();
            YierdisFfmListpack.Cursor c = other.cursor();
            try {
                while (c.next()) {
                    current.addLast(c.toByteArray());
                    appended++;
                }
            } catch (RuntimeException | Error e) {
                while (appended > 0) {
                    try {
                        current.removeLast();
                    } catch (RuntimeException rollbackFailure) {
                        e.addSuppressed(rollbackFailure);
                        break;
                    }
                    appended--;
                }
                throw e;
            }
        }

        long rawHandle() {
            return nodeHandle == null ? 0L : nodeHandle.raw();
        }

        void writeMetadata(long prevRaw, long nextRaw) {
            validateOwnerRoot();
            YierdisFfmListpack current = liveListpack();
            try (NativeObjectView view = allocator.resolve(nodeHandle, NativeAccessMode.READ_WRITE)) {
                setLong(view, QUICKLIST_NODE_OWNER_ROOT_OFFSET, rootHandle.raw());
                setLong(view, QUICKLIST_NODE_PREV_OFFSET, prevRaw);
                setLong(view, QUICKLIST_NODE_NEXT_OFFSET, nextRaw);
                setLong(view, QUICKLIST_NODE_PAYLOAD_REF_OFFSET, 0L);
                setInt(view, QUICKLIST_NODE_ENTRY_COUNT_OFFSET, current.size());
                setInt(view, QUICKLIST_NODE_ENCODED_BYTES_OFFSET, current.encodedBytes());
                setInt(view, QUICKLIST_NODE_FLAGS_OFFSET, 0);
                setInt(view, QUICKLIST_NODE_RESERVED_OFFSET, 0);
            }
        }

        private YierdisFfmListpack liveListpack() {
            validateLiveNode();
            return listpack;
        }

        private void validateLiveNode() {
            validateNodeHandleKind();
            try (NativeObjectView ignored = allocator.resolve(nodeHandle, NativeAccessMode.READ_ONLY)) {
                // Allocator resolution validates quicklist node handle liveness and generation.
            }
        }

        private void validateNodeHandleKind() {
            if (nodeHandle != null
                    && (nodeHandle.domain() != NativeObjectKind.LIST_QUICKLIST_NODE.domain()
                    || nodeHandle.kindCode() != NativeObjectKind.LIST_QUICKLIST_NODE.code())) {
                throw new NativeMemoryException("LIST_QUICKLIST_NODE handle expected: "
                        + nodeHandle.raw());
            }
        }

        private void validateOwnerRoot() {
            try (NativeObjectView ignored = allocator.resolve(rootHandle, NativeAccessMode.READ_ONLY)) {
                // Allocator resolution validates root handle liveness; constructor validates LIST_NODE kind.
            }
        }

        @Override
        public void close() {
            if (payloadClosed && nodeFreed) {
                return;
            }
            RuntimeException failure = null;
            if (!payloadClosed) {
                try {
                    listpack.close();
                    payloadClosed = true;
                    listpack = null;
                } catch (RuntimeException e) {
                    failure = e;
                }
            }
            if (allocator != null && nodeHandle != null && !nodeFreed) {
                try {
                    allocator.free(nodeHandle);
                    nodeFreed = true;
                    nodeHandle = null;
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static void setLong(NativeObjectView view, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            view.setByte(offset + i, (byte) (value >>> (i * 8)));
        }
    }

    private static void setInt(NativeObjectView view, int offset, int value) {
        for (int i = 0; i < Integer.BYTES; i++) {
            view.setByte(offset + i, (byte) (value >>> (i * 8)));
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
        if (memoryRuntime != null) {
            if (listpackFfm != null) {
                listpackFfm.close();
                listpackFfm = null;
            }
            if (quicklistFfm != null) {
                RuntimeException failure = null;
                java.util.Iterator<FfmListNode> iterator = quicklistFfm.iterator();
                while (iterator.hasNext()) {
                    FfmListNode n = iterator.next();
                    try {
                        n.close();
                        iterator.remove();
                    } catch (RuntimeException e) {
                        failure = addFailure(failure, e);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
                quicklistFfm = null;
            }
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
