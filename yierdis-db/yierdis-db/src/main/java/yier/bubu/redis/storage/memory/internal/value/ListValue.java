package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ListValue implements YierdisValue {
    private static final int QUICKLIST_NODE_MAX_BYTES = YierdisEncodingThresholds.LIST_MAX_LISTPACK_BYTES;
    private static final int QUICKLIST_NODE_RECORD_BYTES = 80;
    private static final int QUICKLIST_NODE_OWNER_ROOT_OFFSET = 0;
    private static final int QUICKLIST_NODE_PREV_OFFSET = 16;
    private static final int QUICKLIST_NODE_NEXT_OFFSET = 32;
    private static final int QUICKLIST_NODE_PAYLOAD_REF_OFFSET = 48;
    private static final int QUICKLIST_NODE_ENTRY_COUNT_OFFSET = 64;
    private static final int QUICKLIST_NODE_ENCODED_BYTES_OFFSET = 68;
    private static final int QUICKLIST_NODE_FLAGS_OFFSET = 72;
    private static final int QUICKLIST_NODE_RESERVED_OFFSET = 76;
    private static final long FIXED_HEAP_BYTES = 88L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long ARRAY_DEQUE_HEAP_BYTES = 40L;
    private static final int INITIAL_QUICKLIST_DEQUE_CAPACITY = 16;

    private final StableMemoryBackend stableMemoryBackend;
    private final NativeByteStore byteStore;
    private final NativeHandle rootHandle;

    private NativeListpack listpack;
    private ArrayDeque<ListNode> quicklist;
    private int quicklistDequeCapacity;
    private long quicklistNodeHeapBytes;
    private int totalSize;
    private Runnable heapChangeListener = () -> {
    };

    public ListValue(StableMemoryBackend stableMemoryBackend, NativeHandle rootHandle) {
        this.stableMemoryBackend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        this.rootHandle = Objects.requireNonNull(rootHandle, "rootHandle");
        if (rootHandle.isNull()) {
            throw new IllegalArgumentException("rootHandle must not be null");
        }
        this.byteStore = new NativeByteStore(stableMemoryBackend, NativeObjectKind.LISTPACK_BYTES);
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

    public int size() {
        return totalSize;
    }

    public long preparedPushHeapUpperBound(List<byte[]> values, boolean left) {
        long upperBound = preparedMutationHeapUpperBound(
                addSaturating(valueCount(values), quicklist == null ? totalSize : edgeSize(left)),
                preparedPushNativeAllocationSizes(values, left).length
        );
        if (quicklist == null) {
            return upperBound;
        }
        QuicklistPushPlan plan = planQuicklistPush(values, left);
        long finalNodeCount = addSaturating(quicklist.size(), plan.nodeCount());
        if (finalNodeCount < quicklistDequeCapacity) {
            return upperBound;
        }
        long topologyBytes = addSaturating(
                ARRAY_DEQUE_HEAP_BYTES + ARRAY_HEADER_BYTES,
                multiplySaturating(addSaturating(finalNodeCount, 1L), REFERENCE_BYTES)
        );
        return addSaturating(upperBound, topologyBytes);
    }

    public long preparedPopHeapUpperBound(int count, boolean left) {
        int popCount = Math.min(Math.max(0, count), totalSize);
        return preparedMutationHeapUpperBound(
                addSaturating(popCount, quicklist == null ? totalSize : edgeSize(left)),
                preparedPopNativeAllocationSizes(count, left).length
        );
    }

    public static long preparedHeapUpperBoundForElementCount(long expectedElements) {
        return heapUpperBoundForElementCount(expectedElements);
    }

    public static long preparedNewHeapUpperBound(List<byte[]> values) {
        return heapUpperBoundForElementCount(valueCount(values));
    }

    public int[] preparedPushNativeAllocationSizes(List<byte[]> values, boolean left) {
        Objects.requireNonNull(values, "values");
        if (quicklist != null) {
            return planQuicklistPush(values, left).nativeAllocationSizes();
        }
        return planPackedPush(values, left).nativeAllocationSizes();
    }

    private BuildPlan planPackedPush(List<byte[]> values, boolean left) {
        int[] current = encodedEntrySizes();
        int incomingCount = values == null ? 0 : values.size();
        int[] combined = new int[Math.addExact(current.length, incomingCount)];
        if (left) {
            for (int index = 0; index < incomingCount; index++) {
                combined[index] = NativeListpack.entryEncodedBytes(values.get(incomingCount - index - 1));
            }
            System.arraycopy(current, 0, combined, incomingCount, current.length);
        } else {
            System.arraycopy(current, 0, combined, 0, current.length);
            for (int index = 0; index < incomingCount; index++) {
                combined[current.length + index] = NativeListpack.entryEncodedBytes(values.get(index));
            }
        }
        return buildPlan(combined);
    }

    public static int[] preparedNewNativeAllocationSizes(List<byte[]> values, boolean left) {
        int incomingCount = values == null ? 0 : values.size();
        int[] encoded = new int[incomingCount];
        for (int index = 0; index < incomingCount; index++) {
            int sourceIndex = left ? incomingCount - index - 1 : index;
            encoded[index] = NativeListpack.entryEncodedBytes(values.get(sourceIndex));
        }
        return buildPlan(encoded).nativeAllocationSizes();
    }

    public int[] preparedPopNativeAllocationSizes(int count, boolean left) {
        int popCount = Math.min(Math.max(0, count), totalSize);
        int remaining = totalSize - popCount;
        if (remaining == 0) {
            return new int[0];
        }
        if (quicklist == null) {
            int sourceOffset = left ? popCount : 0;
            return new int[]{listpack.encodedBytesInRange(sourceOffset, remaining)};
        }

        int pending = popCount;
        java.util.Iterator<ListNode> iterator = left ? quicklist.iterator() : quicklist.descendingIterator();
        while (iterator.hasNext()) {
            ListNode node = iterator.next();
            if (pending >= node.size()) {
                pending -= node.size();
                continue;
            }
            if (pending == 0) {
                return new int[0];
            }
            int retainedCount = node.size() - pending;
            int retainedFrom = left ? pending : 0;
            return new int[]{node.encodedBytesInRange(retainedFrom, retainedCount)};
        }
        throw new IllegalStateException("quicklist pop planning exceeded list size");
    }

    private int edgeSize(boolean left) {
        ListNode edge = left ? quicklist.peekFirst() : quicklist.peekLast();
        return edge == null ? 0 : edge.size();
    }

    private QuicklistPushPlan planQuicklistPush(List<byte[]> values, boolean left) {
        ListNode edge = left ? quicklist.peekFirst() : quicklist.peekLast();
        int edgeEntryCount = 0;
        int edgeEncodedBytes = edge == null ? 0 : edge.encodedBytes();
        while (edge != null && edgeEntryCount < values.size()) {
            int entryBytes = entryEncodedBytes(values.get(edgeEntryCount));
            if ((long) edgeEncodedBytes + entryBytes > QUICKLIST_NODE_MAX_BYTES) {
                break;
            }
            edgeEncodedBytes = Math.addExact(edgeEncodedBytes, entryBytes);
            edgeEntryCount++;
        }

        int remaining = values.size() - edgeEntryCount;
        int[] nodeEntryCounts = new int[remaining];
        int[] nodeEncodedBytes = new int[remaining];
        int nodeCount = 0;
        int currentEntries = 0;
        int currentBytes = 0;
        for (int index = edgeEntryCount; index < values.size(); index++) {
            int entryBytes = entryEncodedBytes(values.get(index));
            if (currentEntries > 0 && (long) currentBytes + entryBytes > QUICKLIST_NODE_MAX_BYTES) {
                nodeEntryCounts[nodeCount] = currentEntries;
                nodeEncodedBytes[nodeCount] = currentBytes;
                nodeCount++;
                currentEntries = 0;
                currentBytes = 0;
            }
            currentEntries++;
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (currentEntries > 0) {
            nodeEntryCounts[nodeCount] = currentEntries;
            nodeEncodedBytes[nodeCount] = currentBytes;
            nodeCount++;
        }
        return new QuicklistPushPlan(
                edgeEntryCount,
                edgeEncodedBytes,
                java.util.Arrays.copyOf(nodeEntryCounts, nodeCount),
                java.util.Arrays.copyOf(nodeEncodedBytes, nodeCount)
        );
    }

    public void loadForBuild(List<byte[]> orderedValues) {
        Objects.requireNonNull(orderedValues, "orderedValues");
        if (totalSize != 0 || quicklist != null || !listpack.isEmpty()) {
            throw new IllegalStateException("staged list build requires an empty value");
        }
        int[] encodedEntries = encodedEntrySizes(orderedValues);
        BuildPlan plan = buildPlan(encodedEntries);
        if (!plan.quicklist()) {
            int encodedBytes = plan.blockCount() == 0 ? 0 : plan.blockEncodedBytes()[0];
            listpack.reserveForBuild(orderedValues.size(), encodedBytes);
            for (byte[] value : orderedValues) {
                listpack.addLast(value);
            }
            totalSize = orderedValues.size();
            return;
        }

        ArrayDeque<ListNode> out = new ArrayDeque<>(INITIAL_QUICKLIST_DEQUE_CAPACITY);
        int outDequeCapacity = INITIAL_QUICKLIST_DEQUE_CAPACITY + 1;
        long outNodeHeapBytes = 0L;
        int valueIndex = 0;
        ListNode currentNode = null;
        try {
            for (int blockIndex = 0; blockIndex < plan.blockCount(); blockIndex++) {
                currentNode = newListNode();
                int entryCount = plan.blockEntryCounts()[blockIndex];
                currentNode.reserveForBuild(entryCount, plan.blockEncodedBytes()[blockIndex]);
                for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                    currentNode.addLast(orderedValues.get(valueIndex++));
                }
                if (out.size() + 1 >= outDequeCapacity) {
                    outDequeCapacity = nextArrayDequeCapacity(outDequeCapacity);
                }
                out.addLast(currentNode);
                outNodeHeapBytes += currentNode.heapEstimatedBytes();
                currentNode = null;
            }
            refreshNodeMetadataLinks(out);
        } catch (RuntimeException | Error failure) {
            closeNodes(out, failure);
            if (currentNode != null) {
                closeNode(currentNode, failure);
            }
            throw failure;
        }

        listpack.close();
        listpack = null;
        quicklist = out;
        quicklistDequeCapacity = outDequeCapacity;
        quicklistNodeHeapBytes = outNodeHeapBytes;
        totalSize = orderedValues.size();
    }

    public PreparedMutation preparePush(List<byte[]> values, boolean left) {
        Objects.requireNonNull(values, "values");
        if (values.isEmpty()) {
            return PreparedMutation.unchanged(this);
        }
        return quicklist == null ? preparePackedPush(values, left) : prepareQuicklistPush(values, left);
    }

    public PreparedMutation preparePop(int count, boolean left) {
        int popCount = Math.min(Math.max(0, count), totalSize);
        if (popCount == 0) {
            return PreparedMutation.unchanged(this);
        }
        if (popCount == totalSize) {
            throw new IllegalArgumentException("full list deletion is owned by the entry mutation");
        }
        return quicklist == null ? preparePackedPop(popCount, left) : prepareQuicklistPop(popCount, left);
    }

    private PreparedMutation preparePackedPush(List<byte[]> values, boolean left) {
        BuildPlan plan = planPackedPush(values, left);
        int nextSize = Math.addExact(totalSize, values.size());
        if (!plan.quicklist()) {
            NativeListpack replacement = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
            boolean success = false;
            try {
                replacement.reserveForBuild(nextSize, plan.blockEncodedBytes()[0]);
                appendPackedPush(replacement, values, left);
                PreparedMutation prepared = PreparedMutation.packedReplacement(this, replacement, nextSize);
                success = true;
                return prepared;
            } finally {
                if (!success) {
                    replacement.close();
                }
            }
        }

        ArrayDeque<ListNode> nodes = new ArrayDeque<>(plan.blockCount());
        boolean success = false;
        int logicalOffset = 0;
        ListNode current = null;
        try {
            for (int blockIndex = 0; blockIndex < plan.blockCount(); blockIndex++) {
                current = newListNode();
                int entryCount = plan.blockEntryCounts()[blockIndex];
                current.reserveForBuild(entryCount, plan.blockEncodedBytes()[blockIndex]);
                appendPackedPushRange(current, values, left, logicalOffset, entryCount);
                nodes.addLast(current);
                current = null;
                logicalOffset += entryCount;
            }
            PreparedMutation prepared = PreparedMutation.packedToQuicklist(this, nodes, nextSize);
            success = true;
            return prepared;
        } finally {
            if (!success) {
                RuntimeException failure = null;
                if (current != null) {
                    try {
                        current.close();
                    } catch (RuntimeException closeFailure) {
                        failure = closeFailure;
                    }
                }
                for (ListNode node : nodes) {
                    try {
                        node.close();
                    } catch (RuntimeException closeFailure) {
                        failure = addFailure(failure, closeFailure);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        }
    }

    private PreparedMutation preparePackedPop(int popCount, boolean left) {
        int retainedCount = totalSize - popCount;
        int retainedFrom = left ? popCount : 0;
        int retainedBytes = listpack.encodedBytesInRange(retainedFrom, retainedCount);
        NativeListpack replacement = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
        boolean success = false;
        try {
            replacement.reserveForBuild(retainedCount, retainedBytes);
            replacement.appendRangeFrom(listpack, retainedFrom, retainedCount);
            PreparedMutation prepared = PreparedMutation.packedReplacement(this, replacement, retainedCount);
            success = true;
            return prepared;
        } finally {
            if (!success) {
                replacement.close();
            }
        }
    }

    private PreparedMutation prepareQuicklistPush(List<byte[]> values, boolean left) {
        QuicklistPushPlan plan = planQuicklistPush(values, left);
        ListNode edge = left ? quicklist.peekFirst() : quicklist.peekLast();
        NativeListpack edgeReplacement = null;
        ArrayDeque<ListNode> addedNodes = new ArrayDeque<>(plan.nodeCount());
        ListNode current = null;
        boolean success = false;
        try {
            if (plan.edgeEntryCount() > 0) {
                edgeReplacement = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
                edgeReplacement.reserveForBuild(
                        Math.addExact(edge.size(), plan.edgeEntryCount()),
                        plan.edgeEncodedBytes()
                );
                if (left) {
                    for (int index = plan.edgeEntryCount() - 1; index >= 0; index--) {
                        edgeReplacement.addLast(values.get(index));
                    }
                    edgeReplacement.appendRangeFrom(edge.listpack, 0, edge.size());
                } else {
                    edgeReplacement.appendRangeFrom(edge.listpack, 0, edge.size());
                    for (int index = 0; index < plan.edgeEntryCount(); index++) {
                        edgeReplacement.addLast(values.get(index));
                    }
                }
            }

            int valueIndex = plan.edgeEntryCount();
            for (int nodeIndex = 0; nodeIndex < plan.nodeCount(); nodeIndex++) {
                current = newListNode();
                int entryCount = plan.nodeEntryCounts()[nodeIndex];
                current.reserveForBuild(entryCount, plan.nodeEncodedBytes()[nodeIndex]);
                if (left) {
                    for (int index = valueIndex + entryCount - 1; index >= valueIndex; index--) {
                        current.addLast(values.get(index));
                    }
                    addedNodes.addFirst(current);
                } else {
                    for (int index = valueIndex; index < valueIndex + entryCount; index++) {
                        current.addLast(values.get(index));
                    }
                    addedNodes.addLast(current);
                }
                current = null;
                valueIndex += entryCount;
            }

            ArrayDeque<ListNode> replacementTopology = null;
            int finalNodeCount = Math.addExact(quicklist.size(), addedNodes.size());
            if (finalNodeCount >= quicklistDequeCapacity) {
                replacementTopology = new ArrayDeque<>(finalNodeCount);
                if (left) {
                    replacementTopology.addAll(addedNodes);
                    replacementTopology.addAll(quicklist);
                } else {
                    replacementTopology.addAll(quicklist);
                    replacementTopology.addAll(addedNodes);
                }
            }
            PreparedMutation prepared = PreparedMutation.quicklistPush(
                    this,
                    left,
                    edge,
                    edgeReplacement,
                    addedNodes,
                    replacementTopology,
                    Math.addExact(totalSize, values.size())
            );
            success = true;
            return prepared;
        } finally {
            if (!success) {
                RuntimeException failure = null;
                if (edgeReplacement != null) {
                    try {
                        edgeReplacement.close();
                    } catch (RuntimeException closeFailure) {
                        failure = closeFailure;
                    }
                }
                if (current != null) {
                    try {
                        current.close();
                    } catch (RuntimeException closeFailure) {
                        failure = addFailure(failure, closeFailure);
                    }
                }
                for (ListNode node : addedNodes) {
                    try {
                        node.close();
                    } catch (RuntimeException closeFailure) {
                        failure = addFailure(failure, closeFailure);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        }
    }

    private PreparedMutation prepareQuicklistPop(int popCount, boolean left) {
        ArrayList<ListNode> removed = new ArrayList<>();
        java.util.Iterator<ListNode> iterator = left ? quicklist.iterator() : quicklist.descendingIterator();
        int pending = popCount;
        ListNode edge = null;
        NativeListpack edgeReplacement = null;
        while (iterator.hasNext() && pending > 0) {
            ListNode node = iterator.next();
            if (pending >= node.size()) {
                pending -= node.size();
                removed.add(node);
                continue;
            }
            edge = node;
            int retainedCount = node.size() - pending;
            int retainedFrom = left ? pending : 0;
            int retainedBytes = node.encodedBytesInRange(retainedFrom, retainedCount);
            edgeReplacement = new NativeListpack(byteStore, NativeObjectKind.LISTPACK_BYTES);
            boolean copied = false;
            try {
                edgeReplacement.reserveForBuild(retainedCount, retainedBytes);
                edgeReplacement.appendRangeFrom(node.listpack, retainedFrom, retainedCount);
                copied = true;
            } finally {
                if (!copied) {
                    edgeReplacement.close();
                }
            }
            pending = 0;
        }
        if (pending != 0) {
            throw new IllegalStateException("quicklist pop staging exceeded list size");
        }
        if (edge == null) {
            java.util.Iterator<ListNode> retained = left
                    ? quicklist.iterator()
                    : quicklist.descendingIterator();
            for (int index = 0; index < removed.size(); index++) {
                retained.next();
            }
            edge = retained.next();
        }
        return PreparedMutation.quicklistPop(
                this,
                left,
                edge,
                edgeReplacement,
                removed.toArray(ListNode[]::new),
                totalSize - popCount
        );
    }

    private void appendPackedPush(NativeListpack target, List<byte[]> values, boolean left) {
        if (left) {
            for (int index = values.size() - 1; index >= 0; index--) {
                target.addLast(values.get(index));
            }
            target.appendRangeFrom(listpack, 0, listpack.size());
            return;
        }
        target.appendRangeFrom(listpack, 0, listpack.size());
        for (byte[] value : values) {
            target.addLast(value);
        }
    }

    private void appendPackedPushRange(
            ListNode target,
            List<byte[]> values,
            boolean left,
            int logicalOffset,
            int entryCount
    ) {
        int incomingCount = values.size();
        int next = logicalOffset;
        int remaining = entryCount;
        while (remaining > 0) {
            if (left && next < incomingCount) {
                int run = Math.min(remaining, incomingCount - next);
                for (int index = 0; index < run; index++) {
                    target.addLast(values.get(incomingCount - next - index - 1));
                }
                next += run;
                remaining -= run;
                continue;
            }
            if (!left && next < listpack.size()) {
                int run = Math.min(remaining, listpack.size() - next);
                target.appendRangeFrom(listpack, next, run);
                next += run;
                remaining -= run;
                continue;
            }
            if (left) {
                int sourceIndex = next - incomingCount;
                int run = Math.min(remaining, listpack.size() - sourceIndex);
                target.appendRangeFrom(listpack, sourceIndex, run);
                next += run;
                remaining -= run;
                continue;
            }
            int valueIndex = next - listpack.size();
            int run = Math.min(remaining, incomingCount - valueIndex);
            for (int index = 0; index < run; index++) {
                target.addLast(values.get(valueIndex + index));
            }
            next += run;
            remaining -= run;
        }
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

    public void rangeInto(int start, int stop, ByteValueSink out) {
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

    public void emitPopRange(int count, boolean left, ByteValueSink out) {
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

    public void releaseExcept(PreparedPoppedValueSequence retained) {
        Objects.requireNonNull(retained, "retained");
        RuntimeException failure = null;
        if (listpack != null) {
            try {
                listpack.closeExcept(retained::retainsHandle);
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
            predicted = Math.addExact(predicted, NativeListpack.entryEncodedBytes(v));
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
        return new ListNode(byteStore, stableMemoryBackend, rootHandle);
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
                prev.writeMetadata(prev.prevHandleDuringRefresh, node.handle());
            }
            node.prevHandleDuringRefresh = prev == null ? NativeHandle.NULL : prev.handle();
            prev = node;
        }
        if (prev != null) {
            prev.writeMetadata(prev.prevHandleDuringRefresh, NativeHandle.NULL);
        }
    }

    private void refreshEdgeMetadata(boolean left, int nodeCount) {
        if (quicklist == null || quicklist.isEmpty() || nodeCount <= 0) {
            return;
        }
        java.util.Iterator<ListNode> iterator = left ? quicklist.iterator() : quicklist.descendingIterator();
        if (left) {
            NativeHandle previousHandle = NativeHandle.NULL;
            ListNode current = iterator.next();
            for (int written = 0; written < nodeCount; written++) {
                ListNode next = iterator.hasNext() ? iterator.next() : null;
                current.writeMetadata(
                        previousHandle,
                        next == null ? NativeHandle.NULL : next.handle()
                );
                previousHandle = current.handle();
                if (next == null) {
                    break;
                }
                current = next;
            }
            return;
        }

        NativeHandle nextHandle = NativeHandle.NULL;
        ListNode current = iterator.next();
        for (int written = 0; written < nodeCount; written++) {
            ListNode previous = iterator.hasNext() ? iterator.next() : null;
            current.writeMetadata(
                    previous == null ? NativeHandle.NULL : previous.handle(),
                    nextHandle
            );
            nextHandle = current.handle();
            if (previous == null) {
                break;
            }
            current = previous;
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

    private static int entryEncodedBytes(byte[] v) {
        return NativeListpack.entryEncodedBytes(v);
    }

    private static int entryEncodedBytes(int len) {
        return NativeListpack.entryEncodedBytes(len);
    }

    private int[] encodedEntrySizes() {
        int[] encoded = new int[totalSize];
        int next = 0;
        if (quicklist == null) {
            for (int index = 0; index < listpack.size(); index++) {
                encoded[next++] = listpack.encodedEntryBytesAt(index);
            }
        } else {
            for (ListNode node : quicklist) {
                for (int index = 0; index < node.size(); index++) {
                    encoded[next++] = node.encodedEntryBytesAt(index);
                }
            }
        }
        if (next != encoded.length) {
            throw new IllegalStateException("list entry count changed during allocation planning");
        }
        return encoded;
    }

    private static int[] encodedEntrySizes(List<byte[]> values) {
        int[] encoded = new int[values.size()];
        for (int index = 0; index < values.size(); index++) {
            encoded[index] = NativeListpack.entryEncodedBytes(values.get(index));
        }
        return encoded;
    }

    private static BuildPlan buildPlan(int[] encodedEntries) {
        long totalEncodedBytes = 0L;
        for (int entryBytes : encodedEntries) {
            totalEncodedBytes += entryBytes;
            if (totalEncodedBytes > QUICKLIST_NODE_MAX_BYTES) {
                break;
            }
        }
        if (totalEncodedBytes <= QUICKLIST_NODE_MAX_BYTES) {
            return encodedEntries.length == 0
                    ? BuildPlan.emptyPacked()
                    : new BuildPlan(false, new int[]{encodedEntries.length}, new int[]{(int) totalEncodedBytes});
        }

        int[] entryCounts = new int[encodedEntries.length];
        int[] blockBytes = new int[encodedEntries.length];
        int blockCount = 0;
        int currentEntries = 0;
        int currentBytes = 0;
        for (int entryBytes : encodedEntries) {
            if (currentEntries > 0 && (long) currentBytes + entryBytes > QUICKLIST_NODE_MAX_BYTES) {
                entryCounts[blockCount] = currentEntries;
                blockBytes[blockCount] = currentBytes;
                blockCount++;
                currentEntries = 0;
                currentBytes = 0;
            }
            currentEntries++;
            currentBytes = Math.addExact(currentBytes, entryBytes);
        }
        if (currentEntries > 0) {
            entryCounts[blockCount] = currentEntries;
            blockBytes[blockCount] = currentBytes;
            blockCount++;
        }
        return new BuildPlan(
                true,
                java.util.Arrays.copyOf(entryCounts, blockCount),
                java.util.Arrays.copyOf(blockBytes, blockCount)
        );
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

    private static long preparedMutationHeapUpperBound(long affectedEntries, int nativeAllocationCount) {
        long arrays = multiplySaturating(Math.max(0L, affectedEntries), 16L);
        long nativeAdapters = multiplySaturating(Math.max(0L, nativeAllocationCount), 256L);
        return addSaturating(512L, addSaturating(arrays, nativeAdapters));
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

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private record RangeBounds(int start, int stop) {
    }

    private record BuildPlan(boolean quicklist, int[] blockEntryCounts, int[] blockEncodedBytes) {
        private static BuildPlan emptyPacked() {
            return new BuildPlan(false, new int[0], new int[0]);
        }

        private int blockCount() {
            return blockEncodedBytes.length;
        }

        private int[] nativeAllocationSizes() {
            if (!quicklist) {
                return blockEncodedBytes.clone();
            }
            int[] sizes = new int[Math.multiplyExact(blockCount(), 2)];
            for (int blockIndex = 0; blockIndex < blockCount(); blockIndex++) {
                sizes[blockIndex * 2] = QUICKLIST_NODE_RECORD_BYTES;
                sizes[blockIndex * 2 + 1] = blockEncodedBytes[blockIndex];
            }
            return sizes;
        }
    }

    private record QuicklistPushPlan(
            int edgeEntryCount,
            int edgeEncodedBytes,
            int[] nodeEntryCounts,
            int[] nodeEncodedBytes
    ) {
        private int nodeCount() {
            return nodeEncodedBytes.length;
        }

        private int[] nativeAllocationSizes() {
            int edgeAllocations = edgeEntryCount == 0 ? 0 : 1;
            int[] sizes = new int[Math.addExact(edgeAllocations, Math.multiplyExact(nodeCount(), 2))];
            int next = 0;
            if (edgeEntryCount > 0) {
                sizes[next++] = edgeEncodedBytes;
            }
            for (int index = 0; index < nodeCount(); index++) {
                sizes[next++] = QUICKLIST_NODE_RECORD_BYTES;
                sizes[next++] = nodeEncodedBytes[index];
            }
            return sizes;
        }
    }

    public static sealed abstract class PreparedMutation implements AutoCloseable
            permits UnchangedPreparedMutation, PackedReplacementPreparedMutation,
            PackedToQuicklistPreparedMutation, QuicklistPushPreparedMutation,
            QuicklistPopPreparedMutation {
        enum Operation {
            UNCHANGED,
            PACKED_REPLACEMENT,
            PACKED_TO_QUICKLIST,
            QUICKLIST_PUSH,
            QUICKLIST_POP
        }

        static final long BASE_STAGED_HEAP_BYTES = 256L + ARRAY_HEADER_BYTES;

        private final ListValue owner;
        private final int sourceSize;
        private final int finalSize;
        private final ValueEncoding finalEncoding;
        private final long stagedHeapBytes;

        private boolean committed;
        private boolean released;
        private boolean closed;

        private PreparedMutation(
                ListValue owner,
                int finalSize,
                ValueEncoding finalEncoding,
                long stagedHeapBytes
        ) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.sourceSize = owner.totalSize;
            this.finalSize = finalSize;
            this.finalEncoding = Objects.requireNonNull(finalEncoding, "finalEncoding");
            this.stagedHeapBytes = stagedHeapBytes;
        }

        private static PreparedMutation unchanged(ListValue owner) {
            return new UnchangedPreparedMutation(owner);
        }

        private static PreparedMutation packedReplacement(
                ListValue owner,
                NativeListpack replacement,
                int finalSize
        ) {
            return new PackedReplacementPreparedMutation(owner, replacement, finalSize);
        }

        private static PreparedMutation packedToQuicklist(
                ListValue owner,
                ArrayDeque<ListNode> nodes,
                int finalSize
        ) {
            return new PackedToQuicklistPreparedMutation(owner, nodes, finalSize);
        }

        private static PreparedMutation quicklistPush(
                ListValue owner,
                boolean left,
                ListNode edge,
                NativeListpack edgeReplacement,
                ArrayDeque<ListNode> addedNodes,
                ArrayDeque<ListNode> replacementTopology,
                int finalSize
        ) {
            return new QuicklistPushPreparedMutation(
                    owner,
                    ListEnd.from(left),
                    edge,
                    edgeReplacement,
                    addedNodes,
                    replacementTopology,
                    finalSize
            );
        }

        private static PreparedMutation quicklistPop(
                ListValue owner,
                boolean left,
                ListNode edge,
                NativeListpack edgeReplacement,
                ListNode[] removedNodes,
                int finalSize
        ) {
            return new QuicklistPopPreparedMutation(
                    owner,
                    ListEnd.from(left),
                    edge,
                    edgeReplacement,
                    removedNodes,
                    finalSize
            );
        }

        abstract Operation operation();

        public final int size() {
            return finalSize;
        }

        public final ValueEncoding encoding() {
            return finalEncoding;
        }

        public final long stagedHeapBytes() {
            return stagedHeapBytes;
        }

        public final void commit() {
            if (committed) {
                throw new IllegalStateException("prepared list mutation is already committed");
            }
            if (closed) {
                throw new IllegalStateException("prepared list mutation is closed");
            }
            validateSourceTopology();
            commitPrepared();
            committed = true;
        }

        protected abstract void validateSourceTopology();

        protected abstract void commitPrepared();

        public final void releaseSuperseded() {
            releaseSuperseded(null);
        }

        public final void releaseSuperseded(PreparedPoppedValueSequence retained) {
            if (!committed) {
                throw new IllegalStateException("prepared list mutation is not committed");
            }
            if (released) {
                return;
            }
            releaseSupersededPrepared(retained);
            released = true;
        }

        protected abstract void releaseSupersededPrepared(PreparedPoppedValueSequence retained);

        @Override
        public final void close() {
            if (committed || closed) {
                return;
            }
            closed = true;
            closePrepared();
        }

        protected abstract void closePrepared();

        protected final ListValue owner() {
            return owner;
        }

        protected final void validateSourceEncoding(ValueEncoding sourceEncoding) {
            if (owner.totalSize != sourceSize || owner.encoding() != sourceEncoding) {
                throw new IllegalStateException("list changed after mutation preparation");
            }
        }

        static long packedStagedHeapBytes(NativeListpack packed) {
            return addSaturating(BASE_STAGED_HEAP_BYTES, packed.heapEstimatedBytes());
        }

        static long packedToQuicklistStagedHeapBytes(ArrayDeque<ListNode> nodes) {
            return addSaturating(BASE_STAGED_HEAP_BYTES, stagedNodesHeapBytes(nodes));
        }

        static long quicklistPushStagedHeapBytes(
                NativeListpack edgeReplacement,
                ArrayDeque<ListNode> addedNodes,
                ArrayDeque<ListNode> replacementTopology,
                int replacementTopologyCapacity
        ) {
            long bytes = BASE_STAGED_HEAP_BYTES;
            if (edgeReplacement != null) {
                bytes = addSaturating(bytes, edgeReplacement.heapEstimatedBytes());
            }
            bytes = addSaturating(bytes, stagedNodesHeapBytes(addedNodes));
            if (replacementTopology != null) {
                bytes = addSaturating(
                        bytes,
                        ARRAY_DEQUE_HEAP_BYTES + ARRAY_HEADER_BYTES
                                + (long) replacementTopologyCapacity * REFERENCE_BYTES
                );
            }
            return bytes;
        }

        static long quicklistPopStagedHeapBytes(
                NativeListpack edgeReplacement,
                int removedNodeCount
        ) {
            long bytes = addSaturating(
                    BASE_STAGED_HEAP_BYTES,
                    (long) removedNodeCount * REFERENCE_BYTES
            );
            return edgeReplacement == null
                    ? bytes
                    : addSaturating(bytes, edgeReplacement.heapEstimatedBytes());
        }

        static long stagedNodesHeapBytes(ArrayDeque<ListNode> nodes) {
            long bytes = ARRAY_DEQUE_HEAP_BYTES + ARRAY_HEADER_BYTES
                    + (long) (nodes.size() + 1) * REFERENCE_BYTES;
            return addSaturating(bytes, nodeHeapBytes(nodes));
        }

        static long nodeHeapBytes(ArrayDeque<ListNode> nodes) {
            long bytes = 0L;
            for (ListNode node : nodes) {
                bytes = addSaturating(bytes, node.heapEstimatedBytes());
            }
            return bytes;
        }

        static void closeNodes(ArrayDeque<ListNode> nodes) {
            Throwable failure = null;
            for (ListNode node : nodes) {
                try {
                    node.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            nodes.clear();
            if (failure != null) {
                rethrow(failure);
            }
        }

        static void closePacked(
                NativeListpack packed,
                PreparedPoppedValueSequence retained
        ) {
            if (retained == null) {
                packed.close();
            } else {
                packed.closeExcept(retained::retainsHandle);
            }
        }

        static Throwable addFailure(Throwable failure, Throwable next) {
            if (failure == null) {
                return next;
            }
            failure.addSuppressed(next);
            return failure;
        }

        static void rethrow(Throwable failure) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("unexpected prepared list cleanup failure", failure);
        }
    }

    private static final class UnchangedPreparedMutation extends PreparedMutation {
        private final ValueEncoding sourceEncoding;
        private final Object sourceTopology;

        private UnchangedPreparedMutation(ListValue owner) {
            super(owner, owner.totalSize, owner.encoding(), BASE_STAGED_HEAP_BYTES);
            sourceEncoding = owner.encoding();
            sourceTopology = owner.quicklist == null ? owner.listpack : owner.quicklist;
        }

        @Override
        Operation operation() {
            return Operation.UNCHANGED;
        }

        @Override
        protected void validateSourceTopology() {
            validateSourceEncoding(sourceEncoding);
            Object currentTopology = sourceEncoding == ValueEncoding.LIST_PACKED
                    ? owner().listpack
                    : owner().quicklist;
            if (currentTopology != sourceTopology) {
                throw new IllegalStateException("list changed after mutation preparation");
            }
        }

        @Override
        protected void commitPrepared() {
        }

        @Override
        protected void releaseSupersededPrepared(PreparedPoppedValueSequence retained) {
        }

        @Override
        protected void closePrepared() {
        }
    }

    private static final class PackedReplacementPreparedMutation extends PreparedMutation {
        private final NativeListpack source;

        private NativeListpack replacement;
        private NativeListpack superseded;
        private boolean accountingRefreshed;

        private PackedReplacementPreparedMutation(
                ListValue owner,
                NativeListpack replacement,
                int finalSize
        ) {
            super(
                    owner,
                    finalSize,
                    ValueEncoding.LIST_PACKED,
                    packedStagedHeapBytes(Objects.requireNonNull(replacement, "replacement"))
            );
            source = owner.listpack;
            this.replacement = replacement;
        }

        @Override
        Operation operation() {
            return Operation.PACKED_REPLACEMENT;
        }

        @Override
        protected void validateSourceTopology() {
            validateSourceEncoding(ValueEncoding.LIST_PACKED);
            if (owner().listpack != source) {
                throw new IllegalStateException("packed list changed after mutation preparation");
            }
        }

        @Override
        protected void commitPrepared() {
            superseded = owner().listpack;
            owner().listpack = replacement;
            replacement = null;
            owner().totalSize = size();
        }

        @Override
        protected void releaseSupersededPrepared(PreparedPoppedValueSequence retained) {
            Throwable failure = null;
            if (!accountingRefreshed) {
                try {
                    owner().heapChangeListener.run();
                    accountingRefreshed = true;
                } catch (RuntimeException | Error refreshFailure) {
                    failure = refreshFailure;
                }
            }
            if (superseded != null) {
                try {
                    closePacked(superseded, retained);
                    superseded = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            if (failure != null) {
                rethrow(failure);
            }
        }

        @Override
        protected void closePrepared() {
            replacement.close();
            replacement = null;
        }
    }

    private static final class PackedToQuicklistPreparedMutation extends PreparedMutation {
        private final NativeListpack source;
        private final int topologyCapacity;

        private ArrayDeque<ListNode> nodes;
        private NativeListpack superseded;
        private boolean metadataRefreshed;

        private PackedToQuicklistPreparedMutation(
                ListValue owner,
                ArrayDeque<ListNode> nodes,
                int finalSize
        ) {
            super(
                    owner,
                    finalSize,
                    ValueEncoding.LIST_QUICKLIST,
                    packedToQuicklistStagedHeapBytes(Objects.requireNonNull(nodes, "nodes"))
            );
            source = owner.listpack;
            topologyCapacity = Math.addExact(nodes.size(), 1);
            this.nodes = nodes;
        }

        @Override
        Operation operation() {
            return Operation.PACKED_TO_QUICKLIST;
        }

        @Override
        protected void validateSourceTopology() {
            validateSourceEncoding(ValueEncoding.LIST_PACKED);
            if (owner().listpack != source) {
                throw new IllegalStateException("packed list changed after mutation preparation");
            }
        }

        @Override
        protected void commitPrepared() {
            superseded = owner().listpack;
            owner().listpack = null;
            owner().quicklist = nodes;
            owner().quicklistDequeCapacity = topologyCapacity;
            owner().quicklistNodeHeapBytes = nodeHeapBytes(nodes);
            nodes = null;
            owner().totalSize = size();
        }

        @Override
        protected void releaseSupersededPrepared(PreparedPoppedValueSequence retained) {
            Throwable failure = null;
            if (!metadataRefreshed) {
                try {
                    owner().refreshNodeMetadataLinks();
                    owner().heapChangeListener.run();
                    metadataRefreshed = true;
                } catch (RuntimeException | Error refreshFailure) {
                    failure = refreshFailure;
                }
            }
            if (superseded != null) {
                try {
                    closePacked(superseded, retained);
                    superseded = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            if (failure != null) {
                rethrow(failure);
            }
        }

        @Override
        protected void closePrepared() {
            ArrayDeque<ListNode> staged = nodes;
            nodes = null;
            closeNodes(staged);
        }
    }

    private enum ListEnd {
        LEFT,
        RIGHT;

        private static ListEnd from(boolean left) {
            return left ? LEFT : RIGHT;
        }

        private java.util.Iterator<ListNode> iterator(ArrayDeque<ListNode> nodes) {
            return this == LEFT ? nodes.iterator() : nodes.descendingIterator();
        }

        private ListNode removeEdge(ArrayDeque<ListNode> nodes) {
            return this == LEFT ? nodes.removeFirst() : nodes.removeLast();
        }

        private void addNodes(ArrayDeque<ListNode> target, ArrayDeque<ListNode> added) {
            if (this == RIGHT) {
                target.addAll(added);
                return;
            }
            java.util.Iterator<ListNode> iterator = added.descendingIterator();
            while (iterator.hasNext()) {
                target.addFirst(iterator.next());
            }
        }
    }

    private static final class QuicklistPushPreparedMutation extends PreparedMutation {
        private final ArrayDeque<ListNode> source;
        private final ListEnd end;
        private final ListNode edge;
        private final int affectedMetadataNodes;
        private final int replacementTopologyCapacity;

        private NativeListpack edgeReplacement;
        private ArrayDeque<ListNode> addedNodes;
        private ArrayDeque<ListNode> replacementTopology;
        private NativeListpack supersededEdge;
        private boolean metadataRefreshed;

        private QuicklistPushPreparedMutation(
                ListValue owner,
                ListEnd end,
                ListNode edge,
                NativeListpack edgeReplacement,
                ArrayDeque<ListNode> addedNodes,
                ArrayDeque<ListNode> replacementTopology,
                int finalSize
        ) {
            super(
                    owner,
                    finalSize,
                    ValueEncoding.LIST_QUICKLIST,
                    quicklistPushStagedHeapBytes(
                            edgeReplacement,
                            Objects.requireNonNull(addedNodes, "addedNodes"),
                            replacementTopology,
                            replacementTopology == null
                                    ? 0
                                    : Math.addExact(replacementTopology.size(), 1)
                    )
            );
            source = owner.quicklist;
            this.end = Objects.requireNonNull(end, "end");
            this.edge = Objects.requireNonNull(edge, "edge");
            this.edgeReplacement = edgeReplacement;
            this.addedNodes = addedNodes;
            this.replacementTopology = replacementTopology;
            affectedMetadataNodes = Math.addExact(addedNodes.size(), 1);
            replacementTopologyCapacity = replacementTopology == null
                    ? 0
                    : Math.addExact(replacementTopology.size(), 1);
        }

        @Override
        Operation operation() {
            return Operation.QUICKLIST_PUSH;
        }

        @Override
        protected void validateSourceTopology() {
            validateSourceEncoding(ValueEncoding.LIST_QUICKLIST);
            if (owner().quicklist != source) {
                throw new IllegalStateException("quicklist topology changed after mutation preparation");
            }
            java.util.Iterator<ListNode> iterator = end.iterator(owner().quicklist);
            if (!iterator.hasNext() || iterator.next() != edge) {
                throw new IllegalStateException("quicklist edge changed after mutation preparation");
            }
        }

        @Override
        protected void commitPrepared() {
            long nextNodeHeapBytes = owner().quicklistNodeHeapBytes;
            if (edgeReplacement != null) {
                long previousHeapBytes = edge.heapEstimatedBytes();
                supersededEdge = edge.replaceListpack(edgeReplacement);
                edgeReplacement = null;
                nextNodeHeapBytes += edge.heapEstimatedBytes() - previousHeapBytes;
            }

            long addedHeapBytes = nodeHeapBytes(addedNodes);
            if (replacementTopology != null) {
                owner().quicklist = replacementTopology;
                owner().quicklistDequeCapacity = replacementTopologyCapacity;
                replacementTopology = null;
            } else {
                end.addNodes(owner().quicklist, addedNodes);
            }
            nextNodeHeapBytes += addedHeapBytes;
            addedNodes = null;
            owner().quicklistNodeHeapBytes = nextNodeHeapBytes;
            owner().totalSize = size();
        }

        @Override
        protected void releaseSupersededPrepared(PreparedPoppedValueSequence retained) {
            Throwable failure = null;
            if (!metadataRefreshed) {
                try {
                    owner().refreshEdgeMetadata(end == ListEnd.LEFT, affectedMetadataNodes);
                    owner().heapChangeListener.run();
                    metadataRefreshed = true;
                } catch (RuntimeException | Error refreshFailure) {
                    failure = refreshFailure;
                }
            }
            if (supersededEdge != null) {
                try {
                    closePacked(supersededEdge, retained);
                    supersededEdge = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            if (failure != null) {
                rethrow(failure);
            }
        }

        @Override
        protected void closePrepared() {
            Throwable failure = null;
            if (edgeReplacement != null) {
                try {
                    edgeReplacement.close();
                    edgeReplacement = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = closeFailure;
                }
            }
            ArrayDeque<ListNode> staged = addedNodes;
            addedNodes = null;
            replacementTopology = null;
            try {
                closeNodes(staged);
            } catch (RuntimeException | Error closeFailure) {
                failure = addFailure(failure, closeFailure);
            }
            if (failure != null) {
                rethrow(failure);
            }
        }
    }

    private static final class QuicklistPopPreparedMutation extends PreparedMutation {
        private final ArrayDeque<ListNode> source;
        private final ListEnd end;
        private final ListNode edge;
        private final ListNode[] removedNodes;

        private NativeListpack edgeReplacement;
        private NativeListpack supersededEdge;
        private boolean metadataRefreshed;

        private QuicklistPopPreparedMutation(
                ListValue owner,
                ListEnd end,
                ListNode edge,
                NativeListpack edgeReplacement,
                ListNode[] removedNodes,
                int finalSize
        ) {
            super(
                    owner,
                    finalSize,
                    ValueEncoding.LIST_QUICKLIST,
                    quicklistPopStagedHeapBytes(
                            edgeReplacement,
                            Objects.requireNonNull(removedNodes, "removedNodes").length
                    )
            );
            source = owner.quicklist;
            this.end = Objects.requireNonNull(end, "end");
            this.edge = Objects.requireNonNull(edge, "edge");
            this.edgeReplacement = edgeReplacement;
            this.removedNodes = removedNodes;
        }

        @Override
        Operation operation() {
            return Operation.QUICKLIST_POP;
        }

        @Override
        protected void validateSourceTopology() {
            validateSourceEncoding(ValueEncoding.LIST_QUICKLIST);
            if (owner().quicklist != source) {
                throw new IllegalStateException("quicklist topology changed after mutation preparation");
            }
            java.util.Iterator<ListNode> iterator = end.iterator(owner().quicklist);
            for (ListNode removed : removedNodes) {
                if (!iterator.hasNext() || iterator.next() != removed) {
                    throw new IllegalStateException("quicklist edge changed after mutation preparation");
                }
            }
            if (!iterator.hasNext() || iterator.next() != edge) {
                throw new IllegalStateException("quicklist edge changed after mutation preparation");
            }
        }

        @Override
        protected void commitPrepared() {
            long nextNodeHeapBytes = owner().quicklistNodeHeapBytes;
            if (edgeReplacement != null) {
                long previousHeapBytes = edge.heapEstimatedBytes();
                supersededEdge = edge.replaceListpack(edgeReplacement);
                edgeReplacement = null;
                nextNodeHeapBytes += edge.heapEstimatedBytes() - previousHeapBytes;
            }
            for (ListNode removed : removedNodes) {
                ListNode actual = end.removeEdge(owner().quicklist);
                if (actual != removed) {
                    throw new IllegalStateException("quicklist edge changed after pop preparation");
                }
                nextNodeHeapBytes -= removed.heapEstimatedBytes();
            }
            owner().quicklistNodeHeapBytes = nextNodeHeapBytes;
            owner().totalSize = size();
        }

        @Override
        protected void releaseSupersededPrepared(PreparedPoppedValueSequence retained) {
            Throwable failure = null;
            if (!metadataRefreshed) {
                try {
                    owner().refreshEdgeMetadata(end == ListEnd.LEFT, 1);
                    owner().heapChangeListener.run();
                    metadataRefreshed = true;
                } catch (RuntimeException | Error refreshFailure) {
                    failure = refreshFailure;
                }
            }
            if (supersededEdge != null) {
                try {
                    closePacked(supersededEdge, retained);
                    supersededEdge = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            for (int index = 0; index < removedNodes.length; index++) {
                ListNode removed = removedNodes[index];
                if (removed == null) {
                    continue;
                }
                try {
                    if (retained == null) {
                        removed.close();
                    } else {
                        removed.closeExcept(retained);
                    }
                    removedNodes[index] = null;
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            if (failure != null) {
                rethrow(failure);
            }
        }

        @Override
        protected void closePrepared() {
            if (edgeReplacement != null) {
                edgeReplacement.close();
                edgeReplacement = null;
            }
        }
    }
    private static void writeHandle(NativeObjectView view, int offset, NativeHandle handle) {
        NativeHandle value = Objects.requireNonNull(handle, "handle");
        view.setLongLittleEndian(offset, value.allocatorId());
        view.setLongLittleEndian(offset + Long.BYTES, value.localRaw());
    }

    private static final class ListNode implements AutoCloseable {
        private final StableMemoryBackend allocator;
        private final NativeHandle rootHandle;
        private NativeListpack listpack;
        private NativeHandle nodeHandle;
        private NativeHandle prevHandleDuringRefresh = NativeHandle.NULL;
        private boolean payloadClosed;
        private boolean nodeFreed;

        private ListNode(NativeByteStore byteStore, StableMemoryBackend allocator, NativeHandle rootHandle) {
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
                writeMetadata(NativeHandle.NULL, NativeHandle.NULL);
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

        void reserveForBuild(int entryCount, int encodedBytes) {
            liveListpack().reserveForBuild(entryCount, encodedBytes);
        }

        int size() {
            return liveListpack().size();
        }

        void writeAt(int index, ByteValueSink out) {
            liveListpack().writeAt(index, out);
        }

        int encodedEntryBytesAt(int index) {
            return liveListpack().encodedEntryBytesAt(index);
        }

        int encodedBytes() {
            return liveListpack().encodedBytes();
        }

        int encodedBytesInRange(int fromIndex, int entryCount) {
            return liveListpack().encodedBytesInRange(fromIndex, entryCount);
        }

        void appendRangeFrom(NativeListpack source, int fromIndex, int entryCount) {
            liveListpack().appendRangeFrom(source, fromIndex, entryCount);
        }

        NativeListpack replaceListpack(NativeListpack replacement) {
            Objects.requireNonNull(replacement, "replacement");
            validateLiveNode();
            NativeListpack previous = listpack;
            listpack = replacement;
            payloadClosed = false;
            return previous;
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

        void closeExcept(PreparedPoppedValueSequence retained) {
            if (payloadClosed && nodeFreed) {
                return;
            }
            RuntimeException failure = null;
            if (!payloadClosed) {
                try {
                    listpack.closeExcept(retained::retainsHandle);
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

        NativeHandle handle() {
            if (nodeHandle == null) {
                throw new IllegalStateException("quicklist node is closed");
            }
            return nodeHandle;
        }

        void forEachNativeHandle(Consumer<NativeHandle> consumer) {
            Objects.requireNonNull(consumer, "consumer");
            validateLiveNode();
            consumer.accept(nodeHandle);
            listpack.forEachNativeHandle(consumer);
        }

        void writeMetadata(NativeHandle previousHandle, NativeHandle nextHandle) {
            validateOwnerRoot();
            NativeListpack current = liveListpack();
            try (NativeObjectView view = allocator.resolve(nodeHandle, NativeAccessMode.READ_WRITE)) {
                writeHandle(view, QUICKLIST_NODE_OWNER_ROOT_OFFSET, rootHandle);
                writeHandle(view, QUICKLIST_NODE_PREV_OFFSET, previousHandle);
                writeHandle(view, QUICKLIST_NODE_NEXT_OFFSET, nextHandle);
                writeHandle(view, QUICKLIST_NODE_PAYLOAD_REF_OFFSET, NativeHandle.NULL);
                view.setIntLittleEndian(QUICKLIST_NODE_ENTRY_COUNT_OFFSET, current.size());
                view.setIntLittleEndian(QUICKLIST_NODE_ENCODED_BYTES_OFFSET, current.encodedBytes());
                view.setIntLittleEndian(QUICKLIST_NODE_FLAGS_OFFSET, 0);
                view.setIntLittleEndian(QUICKLIST_NODE_RESERVED_OFFSET, 0);
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
            if (nodeHandle == null) {
                throw new IllegalStateException("quicklist node is closed");
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
