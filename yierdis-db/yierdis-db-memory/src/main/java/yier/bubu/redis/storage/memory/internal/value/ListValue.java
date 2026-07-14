package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ListValue implements YierdisValue, NativeHandleOwner, HeapTrackedValue {
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
    private static final long FIXED_HEAP_BYTES = 88L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long ARRAY_DEQUE_HEAP_BYTES = 40L;
    private static final int INITIAL_QUICKLIST_DEQUE_CAPACITY = 16;

    private final NativeAllocator nativeAllocator;
    private final NativeByteStore byteStore;
    private final NativeHandle rootHandle;

    private NativeListpack listpack;
    private ArrayDeque<ListNode> quicklist;
    private int quicklistDequeCapacity;
    private long quicklistNodeHeapBytes;
    private int totalSize;
    private Runnable heapChangeListener = () -> {
    };

    public ListValue(NativeAllocator nativeAllocator, NativeHandle rootHandle) {
        this.nativeAllocator = Objects.requireNonNull(nativeAllocator, "nativeAllocator");
        this.rootHandle = Objects.requireNonNull(rootHandle, "rootHandle");
        if (rootHandle.domain() != NativeObjectKind.LIST_ROOT.domain()
                || rootHandle.kindCode() != NativeObjectKind.LIST_ROOT.code()) {
            throw new IllegalArgumentException("rootHandle must be a LIST_ROOT handle: " + rootHandle.raw());
        }
        this.byteStore = new NativeByteStore(nativeAllocator, NativeObjectKind.LISTPACK_BYTES);
        this.listpack = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public ValueEncoding encoding() {
        return quicklist != null ? ValueEncoding.LIST_QUICKLIST : ValueEncoding.LIST_PACKED;
    }

    static int quicklistNodeMaxBytesForTesting() {
        return QUICKLIST_NODE_MAX_BYTES;
    }

    int quicklistNodeCountForTesting() {
        return quicklist == null ? 0 : quicklist.size();
    }

    public int size() {
        return totalSize;
    }

    public long preparedCopyHeapUpperBound(List<byte[]> values) {
        return heapUpperBoundForElementCount(addSaturating(totalSize, valueCount(values)));
    }

    public static long preparedHeapUpperBoundForElementCount(long expectedElements) {
        return heapUpperBoundForElementCount(expectedElements);
    }

    public static long preparedNewHeapUpperBound(List<byte[]> values) {
        return heapUpperBoundForElementCount(valueCount(values));
    }

    public long estimatedBytes() {
        long nodeBytes = quicklist == null ? 0L : (long) quicklist.size() * QUICKLIST_NODE_RECORD_BYTES;
        return byteStore.nativeBytes() + nodeBytes;
    }

    @Override
    public long heapEstimatedBytes() {
        if (quicklist == null) {
            return FIXED_HEAP_BYTES + (listpack == null ? 0L : listpack.heapEstimatedBytes());
        }
        return FIXED_HEAP_BYTES
                + ARRAY_DEQUE_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) quicklistDequeCapacity * REFERENCE_BYTES
                + quicklistNodeHeapBytes;
    }

    @Override
    public void setHeapChangeListener(Runnable listener) {
        heapChangeListener = Objects.requireNonNull(listener, "listener");
    }

    public int[] nativePayloadSizes() {
        int[] sizes = new int[nativePayloadCount()];
        int next = copyNativePayloadSizes(sizes, 0);
        if (next != sizes.length) {
            throw new IllegalStateException("native list payload size count changed during collection");
        }
        return sizes;
    }

    @Override
    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (quicklist != null) {
            for (ListNode node : quicklist) {
                node.forEachNativeHandle(consumer);
            }
            return;
        }
        listpack.forEachNativeHandle(consumer);
    }

    public void lpushAll(List<byte[]> values) {
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

        for (byte[] v : values) {
            listpack.addFirst(v);
            totalSize++;
        }
        if (listpack.encodedBytes() > QUICKLIST_NODE_MAX_BYTES) {
            convertToQuickList();
        }
    }

    public void rpushAll(List<byte[]> values) {
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

        for (byte[] v : values) {
            listpack.addLast(v);
        }
        totalSize += values.size();
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
            if (quicklist != null) {
                if (quicklist.isEmpty()) {
                    break;
                }
                out.add(qlPollFirst());
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

    public NativeListEntryRef[] popEntries(int count, boolean left) {
        int remaining = Math.min(Math.max(0, count), totalSize);
        if (remaining == 0) {
            return new NativeListEntryRef[0];
        }
        NativeListEntryRef[] out = new NativeListEntryRef[remaining];
        if (quicklist == null) {
            if (left) {
                for (int i = 0; i < remaining; i++) {
                    out[i] = listpack.entryRefAt(i);
                }
            } else {
                int start = totalSize - remaining;
                for (int i = totalSize - 1, next = 0; i >= start; i--, next++) {
                    out[next] = listpack.entryRefAt(i);
                }
            }
            return out;
        }

        if (left) {
            int next = 0;
            for (ListNode node : quicklist) {
                for (int i = 0; i < node.size() && next < remaining; i++) {
                    out[next++] = node.entryRefAt(i);
                }
                if (next == remaining) {
                    return out;
                }
            }
            return out;
        }

        int next = 0;
        java.util.Iterator<ListNode> iterator = quicklist.descendingIterator();
        while (iterator.hasNext() && next < remaining) {
            ListNode node = iterator.next();
            for (int i = node.size() - 1; i >= 0 && next < remaining; i--) {
                out[next++] = node.entryRefAt(i);
            }
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
            if (quicklist != null) {
                if (quicklist.isEmpty()) {
                    break;
                }
                out.add(qlPollLast());
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

        RangeBounds bounds = bounds(start, stop, size);
        if (bounds == null) {
            return new ArrayList<>();
        }

        List<byte[]> out = new ArrayList<>(bounds.stop - bounds.start + 1);
        int idx = 0;
        if (quicklist != null) {
            outer:
            for (ListNode n : quicklist) {
                NativeListpack.Cursor c = n.cursor();
                while (c.next()) {
                    if (idx > bounds.stop) {
                        break outer;
                    }
                    if (idx >= bounds.start) {
                        out.add(c.toByteArray());
                    }
                    idx++;
                }
            }
            return out;
        }

        NativeListpack.Cursor c = listpack.cursor();
        while (c.next()) {
            if (idx > bounds.stop) {
                break;
            }
            if (idx >= bounds.start) {
                out.add(c.toByteArray());
            }
            idx++;
        }
        return out;
    }

    public int rangeCount(int start, int stop) {
        RangeBounds bounds = bounds(start, stop, size());
        return bounds == null ? 0 : bounds.stop - bounds.start + 1;
    }

    public void rangeInto(int start, int stop, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        RangeBounds bounds = bounds(start, stop, size());
        if (bounds == null) {
            return;
        }

        int idx = 0;
        if (quicklist != null) {
            outer:
            for (ListNode n : quicklist) {
                NativeListpack.Cursor c = n.cursor();
                while (c.next()) {
                    if (idx > bounds.stop) {
                        break outer;
                    }
                    if (idx >= bounds.start) {
                        c.writeTo(out);
                    }
                    idx++;
                }
            }
            return;
        }

        NativeListpack.Cursor c = listpack.cursor();
        while (c.next()) {
            if (idx > bounds.stop) {
                break;
            }
            if (idx >= bounds.start) {
                c.writeTo(out);
            }
            idx++;
        }
    }

    public void emitPopRange(int count, boolean left, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        int remaining = Math.min(Math.max(0, count), totalSize);
        if (remaining == 0) {
            return;
        }
        if (quicklist == null) {
            if (left) {
                for (int i = 0; i < remaining; i++) {
                    listpack.writeAt(i, out);
                }
            } else {
                int start = totalSize - remaining;
                for (int i = totalSize - 1; i >= start; i--) {
                    listpack.writeAt(i, out);
                }
            }
            return;
        }

        if (left) {
            for (ListNode node : quicklist) {
                for (int i = 0; i < node.size() && remaining > 0; i++) {
                    node.writeAt(i, out);
                    remaining--;
                }
                if (remaining == 0) {
                    return;
                }
            }
            return;
        }

        java.util.Iterator<ListNode> iterator = quicklist.descendingIterator();
        while (iterator.hasNext() && remaining > 0) {
            ListNode node = iterator.next();
            for (int i = node.size() - 1; i >= 0 && remaining > 0; i--) {
                node.writeAt(i, out);
                remaining--;
            }
        }
    }

    public long encodedPopElementBytes(int count, boolean left) {
        int remaining = Math.min(Math.max(0, count), totalSize);
        if (remaining == 0) {
            return 0L;
        }
        long total = 0L;
        if (quicklist == null) {
            if (left) {
                for (int i = 0; i < remaining; i++) {
                    total = addSaturating(total, listpack.encodedElementBytesAt(i));
                }
            } else {
                int start = totalSize - remaining;
                for (int i = totalSize - 1; i >= start; i--) {
                    total = addSaturating(total, listpack.encodedElementBytesAt(i));
                }
            }
            return total;
        }

        if (left) {
            for (ListNode node : quicklist) {
                for (int i = 0; i < node.size() && remaining > 0; i++) {
                    total = addSaturating(total, node.encodedElementBytesAt(i));
                    remaining--;
                }
                if (remaining == 0) {
                    return total;
                }
            }
            return total;
        }

        java.util.Iterator<ListNode> iterator = quicklist.descendingIterator();
        while (iterator.hasNext() && remaining > 0) {
            ListNode node = iterator.next();
            for (int i = node.size() - 1; i >= 0 && remaining > 0; i--) {
                    total = addSaturating(total, node.encodedElementBytesAt(i));
                remaining--;
            }
        }
        return total;
    }

    public void releaseExcept(NativeHandle[] retained) {
        RuntimeException failure = null;
        if (listpack != null) {
            try {
                listpack.closeExcept(retained);
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                listpack = null;
            }
        }
        if (quicklist != null) {
            java.util.Iterator<ListNode> iterator = quicklist.iterator();
            while (iterator.hasNext()) {
                ListNode n = iterator.next();
                try {
                    n.closeExcept(retained);
                    iterator.remove();
                } catch (RuntimeException e) {
                    failure = addFailure(failure, e);
                }
            }
            quicklist = null;
            quicklistDequeCapacity = 0;
            quicklistNodeHeapBytes = 0L;
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        if (listpack != null) {
            try {
                listpack.close();
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                listpack = null;
            }
        }
        if (quicklist != null) {
            java.util.Iterator<ListNode> iterator = quicklist.iterator();
            while (iterator.hasNext()) {
                ListNode n = iterator.next();
                try {
                    n.close();
                    iterator.remove();
                } catch (RuntimeException e) {
                    failure = addFailure(failure, e);
                }
            }
            quicklist = null;
            quicklistDequeCapacity = 0;
            quicklistNodeHeapBytes = 0L;
        }
        if (failure != null) {
            throw failure;
        }
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

    private int nativePayloadCount() {
        if (quicklist == null) {
            return listpack.nativePayloadCount();
        }
        int count = 0;
        for (ListNode node : quicklist) {
            count += node.nativePayloadCount();
        }
        return count;
    }

    private int copyNativePayloadSizes(int[] target, int offset) {
        if (quicklist == null) {
            return listpack.copyNativePayloadSizes(target, offset);
        }
        int next = offset;
        for (ListNode node : quicklist) {
            next = node.copyNativePayloadSizes(target, next);
        }
        return next;
    }

    private void convertToQuickList() {
        if (quicklist != null) {
            return;
        }

        ArrayDeque<ListNode> out = new ArrayDeque<>(INITIAL_QUICKLIST_DEQUE_CAPACITY);
        int outDequeCapacity = INITIAL_QUICKLIST_DEQUE_CAPACITY + 1;
        long outNodeHeapBytes = 0L;
        ListNode node = null;
        try {
            node = newListNode();
            NativeListpack.Cursor c = listpack.cursor();
            while (c.next()) {
                int entryBytes = entryEncodedBytes(c.isNull() ? -1 : c.length());
                if (!node.canAddEntry(entryBytes)) {
                    if (out.size() + 1 >= outDequeCapacity) {
                        outDequeCapacity = nextArrayDequeCapacity(outDequeCapacity);
                    }
                    out.addLast(node);
                    outNodeHeapBytes += node.heapEstimatedBytes();
                    node = newListNode();
                }
                node.addLast(c.toByteArray());
            }
            if (!node.isEmpty()) {
                if (out.size() + 1 >= outDequeCapacity) {
                    outDequeCapacity = nextArrayDequeCapacity(outDequeCapacity);
                }
                out.addLast(node);
                outNodeHeapBytes += node.heapEstimatedBytes();
            } else {
                try {
                    node.close();
                } finally {
                    node = null;
                }
            }

            refreshNodeMetadataLinks(out);
            NativeListpack packed = listpack;
            packed.close();
            listpack = null;
            quicklist = out;
            quicklistDequeCapacity = outDequeCapacity;
            quicklistNodeHeapBytes = outNodeHeapBytes;
        } catch (RuntimeException | Error e) {
            closeNodes(out, e);
            if (node != null) {
                closeNode(node, e);
            }
            throw e;
        }
    }

    private void qlAddFirst(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekFirst().canAdd(v)) {
            addQuicklistFirst(newListNode());
        }
        ListNode n = quicklist.peekFirst();
        long before = n.heapEstimatedBytes();
        n.addFirst(v);
        refreshQuicklistNodeHeap(before, n);
        totalSize++;
        refreshNodeMetadataLinks();
    }

    private void qlAddLast(byte[] v) {
        if (quicklist.isEmpty() || !quicklist.peekLast().canAdd(v)) {
            addQuicklistLast(newListNode());
        }
        ListNode n = quicklist.peekLast();
        long before = n.heapEstimatedBytes();
        n.addLast(v);
        refreshQuicklistNodeHeap(before, n);
        totalSize++;
        refreshNodeMetadataLinks();
    }

    private byte[] qlPollFirst() {
        if (quicklist.isEmpty()) {
            return null;
        }
        ListNode n = quicklist.peekFirst();
        byte[] v = n.removeFirst();
        totalSize--;
        ListNode removedNode = null;
        Throwable failure = null;
        if (n.isEmpty()) {
            removedNode = quicklist.removeFirst();
            quicklistNodeHeapBytes -= removedNode.heapEstimatedBytes();
        }
        try {
            if (removedNode != null) {
                refreshNodeMetadataLinks();
            }
            maybeMergeFirstTwo();
            refreshNodeMetadataLinks();
            return v;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            if (removedNode != null) {
                closeRemovedNode(removedNode, failure);
            }
        }
    }

    private byte[] qlPollLast() {
        if (quicklist.isEmpty()) {
            return null;
        }
        ListNode n = quicklist.peekLast();
        byte[] v = n.removeLast();
        totalSize--;
        ListNode removedNode = null;
        Throwable failure = null;
        if (n.isEmpty()) {
            removedNode = quicklist.removeLast();
            quicklistNodeHeapBytes -= removedNode.heapEstimatedBytes();
        }
        try {
            if (removedNode != null) {
                refreshNodeMetadataLinks();
            }
            maybeMergeLastTwo();
            refreshNodeMetadataLinks();
            return v;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            if (removedNode != null) {
                closeRemovedNode(removedNode, failure);
            }
        }
    }

    private void maybeMergeFirstTwo() {
        if (quicklist.size() < 2) {
            return;
        }
        ListNode first = quicklist.peekFirst();
        ListNode second = secondNodeFromFirst();
        if (first.canAppendAll(second)) {
            long firstHeapBytes = first.heapEstimatedBytes();
            first.appendAll(second);
            quicklist.remove(second);
            refreshQuicklistNodeHeap(firstHeapBytes, first);
            quicklistNodeHeapBytes -= second.heapEstimatedBytes();
            Throwable failure = null;
            try {
                refreshNodeMetadataLinks();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                closeRemovedNode(second, failure);
            }
            return;
        }
        refreshNodeMetadataLinks();
    }

    private void maybeMergeLastTwo() {
        if (quicklist.size() < 2) {
            return;
        }
        ListNode last = quicklist.peekLast();
        ListNode prev = secondNodeFromLast();
        if (prev.canAppendAll(last)) {
            long previousHeapBytes = prev.heapEstimatedBytes();
            prev.appendAll(last);
            quicklist.remove(last);
            refreshQuicklistNodeHeap(previousHeapBytes, prev);
            quicklistNodeHeapBytes -= last.heapEstimatedBytes();
            Throwable failure = null;
            try {
                refreshNodeMetadataLinks();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                closeRemovedNode(last, failure);
            }
            return;
        }
        refreshNodeMetadataLinks();
    }

    private ListNode newListNode() {
        return new ListNode(byteStore, nativeAllocator, rootHandle);
    }

    private void addQuicklistFirst(ListNode node) {
        ensureQuicklistDequeCapacityForAdd();
        quicklist.addFirst(node);
        quicklistNodeHeapBytes += node.heapEstimatedBytes();
    }

    private void addQuicklistLast(ListNode node) {
        ensureQuicklistDequeCapacityForAdd();
        quicklist.addLast(node);
        quicklistNodeHeapBytes += node.heapEstimatedBytes();
    }

    private void refreshQuicklistNodeHeap(long previousHeapBytes, ListNode node) {
        quicklistNodeHeapBytes += node.heapEstimatedBytes() - previousHeapBytes;
    }

    private void ensureQuicklistDequeCapacityForAdd() {
        if (quicklist.size() + 1 < quicklistDequeCapacity) {
            return;
        }
        quicklistDequeCapacity = nextArrayDequeCapacity(quicklistDequeCapacity);
    }

    private static int nextArrayDequeCapacity(int capacity) {
        int increment = capacity < 64 ? capacity + 2 : capacity >>> 1;
        return Math.addExact(capacity, increment);
    }

    private void refreshNodeMetadataLinks() {
        refreshNodeMetadataLinks(quicklist);
    }

    private void refreshNodeMetadataLinks(ArrayDeque<ListNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }
        ListNode prev = null;
        for (ListNode node : nodes) {
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

    private ListNode secondNodeFromFirst() {
        java.util.Iterator<ListNode> iterator = quicklist.iterator();
        iterator.next();
        return iterator.next();
    }

    private ListNode secondNodeFromLast() {
        java.util.Iterator<ListNode> iterator = quicklist.descendingIterator();
        iterator.next();
        return iterator.next();
    }

    private static void closeNodes(ArrayDeque<ListNode> nodes, Throwable failure) {
        for (ListNode node : nodes) {
            closeNode(node, failure);
        }
        nodes.clear();
    }

    private static void closeNode(ListNode node, Throwable failure) {
        try {
            node.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void closeRemovedNode(ListNode node, Throwable failure) {
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

    private static RangeBounds bounds(int start, int stop, int size) {
        if (size == 0) {
            return null;
        }
        int normalizedStart = normalizeIndex(start, size);
        int normalizedStop = normalizeIndex(stop, size);
        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return null;
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1;
        }
        if (normalizedStart > normalizedStop) {
            return null;
        }
        return new RangeBounds(normalizedStart, normalizedStop);
    }

    private static int normalizeIndex(int idx, int size) {
        return idx >= 0 ? idx : size + idx;
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

    private static int entryEncodedBytes(byte[] v) {
        int len = v == null ? -1 : v.length;
        return entryEncodedBytes(len);
    }

    private static int entryEncodedBytes(int len) {
        int headerValue = len < 0 ? 0 : len + 1;
        return varIntSize(headerValue) + Math.max(0, len);
    }

    private static long heapUpperBoundForElementCount(long expectedElements) {
        if (expectedElements < 0L || expectedElements > Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long packedBytes = addSaturating(
                FIXED_HEAP_BYTES,
                NativeListpack.heapUpperBoundForEntries(expectedElements)
        );
        if (expectedElements == 0L) {
            return packedBytes;
        }
        long nodeBytes = addSaturating(80L, NativeListpack.heapUpperBoundForEntries(1L));
        long nodesBytes = multiplySaturating(expectedElements, nodeBytes);
        long quicklistBytes = addSaturating(
                FIXED_HEAP_BYTES + ARRAY_DEQUE_HEAP_BYTES + ARRAY_HEADER_BYTES,
                multiplySaturating(arrayDequeCapacityForElements(expectedElements), REFERENCE_BYTES)
        );
        quicklistBytes = addSaturating(quicklistBytes, nodesBytes);
        return Math.max(packedBytes, quicklistBytes);
    }

    private static long arrayDequeCapacityForElements(long elements) {
        long capacity = INITIAL_QUICKLIST_DEQUE_CAPACITY + 1L;
        while (capacity <= elements) {
            long increment = capacity < 64L ? capacity + 2L : capacity >>> 1;
            if (capacity > Integer.MAX_VALUE - increment) {
                return Integer.MAX_VALUE;
            }
            capacity += increment;
        }
        return capacity;
    }

    private static long valueCount(List<byte[]> values) {
        return values == null ? 0L : values.size();
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

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private record RangeBounds(int start, int stop) {
    }

    private static final class ListNode implements AutoCloseable {
        private final NativeAllocator allocator;
        private final NativeHandle rootHandle;
        private final NativeListpack listpack;
        private NativeHandle nodeHandle;
        private long prevRawDuringRefresh;
        private boolean payloadClosed;
        private boolean nodeFreed;

        private ListNode(NativeByteStore byteStore, NativeAllocator allocator, NativeHandle rootHandle) {
            this.allocator = Objects.requireNonNull(allocator, "allocator");
            this.rootHandle = Objects.requireNonNull(rootHandle, "rootHandle");
            NativeHandle allocated = this.allocator.allocate(
                    NativeObjectKind.LIST_NODE,
                    QUICKLIST_NODE_RECORD_BYTES
            );
            try {
                this.listpack = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
            } catch (RuntimeException | Error e) {
                try {
                    this.allocator.free(allocated);
                } catch (RuntimeException freeFailure) {
                    e.addSuppressed(freeFailure);
                }
                throw e;
            }
            this.nodeHandle = allocated;
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

        long heapEstimatedBytes() {
            return 80L + listpack.heapEstimatedBytes();
        }

        NativeListpack.Cursor cursor() {
            return liveListpack().cursor();
        }

        boolean canAdd(byte[] v) {
            return canAddEntry(entryEncodedBytes(v));
        }

        boolean canAddEntry(int entryBytes) {
            if (entryBytes < 0) {
                throw new IllegalArgumentException("entryBytes must be >= 0");
            }
            NativeListpack current = liveListpack();
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

        int size() {
            return liveListpack().size();
        }

        void writeAt(int index, BulkStringSink out) {
            liveListpack().writeAt(index, out);
        }

        long encodedElementBytesAt(int index) {
            return liveListpack().encodedElementBytesAt(index);
        }

        int nativePayloadCount() {
            return liveListpack().nativePayloadCount();
        }

        int copyNativePayloadSizes(int[] target, int offset) {
            return liveListpack().copyNativePayloadSizes(target, offset);
        }

        NativeListEntryRef entryRefAt(int index) {
            return liveListpack().entryRefAt(index);
        }

        byte[] removeFirst() {
            return liveListpack().removeFirst();
        }

        byte[] removeLast() {
            return liveListpack().removeLast();
        }

        boolean canAppendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return true;
            }
            return this.liveListpack().encodedBytes() + other.liveListpack().encodedBytes() <= QUICKLIST_NODE_MAX_BYTES;
        }

        void appendAll(ListNode other) {
            if (other == null || other.isEmpty()) {
                return;
            }
            int appended = 0;
            NativeListpack current = liveListpack();
            NativeListpack.Cursor c = other.cursor();
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

        void closeExcept(NativeHandle[] retained) {
            if (payloadClosed && nodeFreed) {
                return;
            }
            RuntimeException failure = null;
            if (!payloadClosed) {
                try {
                    listpack.closeExcept(retained);
                    payloadClosed = true;
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

        long rawHandle() {
            return nodeHandle == null ? 0L : nodeHandle.raw();
        }

        void forEachNativeHandle(Consumer<NativeHandle> consumer) {
            Objects.requireNonNull(consumer, "consumer");
            validateLiveNode();
            consumer.accept(nodeHandle);
            listpack.forEachNativeHandle(consumer);
        }

        void writeMetadata(long prevRaw, long nextRaw) {
            validateOwnerRoot();
            NativeListpack current = liveListpack();
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

        private NativeListpack liveListpack() {
            validateLiveNode();
            return listpack;
        }

        private void validateLiveNode() {
            validateNodeHandleKind();
            try (NativeObjectView ignored = allocator.resolve(nodeHandle, NativeAccessMode.READ_ONLY)) {
                // Allocator resolution validates node liveness and generation.
            }
        }

        private void validateNodeHandleKind() {
            if (nodeHandle != null
                    && (nodeHandle.domain() != NativeObjectKind.LIST_NODE.domain()
                    || nodeHandle.kindCode() != NativeObjectKind.LIST_NODE.code())) {
                throw new NativeMemoryException("LIST_NODE handle expected: " + nodeHandle.raw());
            }
        }

        private void validateOwnerRoot() {
            try (NativeObjectView ignored = allocator.resolve(rootHandle, NativeAccessMode.READ_ONLY)) {
                // Allocator resolution validates root handle liveness.
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
}
