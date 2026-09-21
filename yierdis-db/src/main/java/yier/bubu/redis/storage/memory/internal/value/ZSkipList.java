package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.memory.api.NativeHandle;

import java.util.Objects;

public final class ZSkipList {
    // Mirrors Redis defaults:
    // https://github.com/redis/redis/blob/unstable/src/server.h
    private static final int MAX_LEVEL = 32;
    private static final double P = 0.25d;
    private static final long FIXED_HEAP_BYTES = 72L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long NODE_FIXED_HEAP_BYTES = 48L;
    private static final long PREPARED_INSERT_FIXED_HEAP_BYTES = 64L;
    private static final long PREPARED_DELETE_FIXED_HEAP_BYTES = 64L;

    private final NativeByteStore memberStore;
    private final Node header = new Node(MAX_LEVEL, null, 0);
    private Node tail;
    private int level = 1;
    private int length = 0;
    private long nodeLevelCount;

    public ZSkipList(NativeByteStore memberStore) {
        this.memberStore = Objects.requireNonNull(memberStore, "memberStore");
    }

    public Node first() {
        return header.forward[0];
    }

    public Node last() {
        return tail;
    }

    /**
     * Returns the first node whose score is within the given lower bound.
     */
    public Node findFirstByScore(double score, boolean exclusive) {
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && (exclusive ? x.forward[i].score <= score : x.forward[i].score < score)) {
                x = x.forward[i];
            }
        }
        return x.forward[0];
    }

    /**
     * Returns the last node whose score is within the given upper bound.
     */
    public Node findLastByScore(double score, boolean exclusive) {
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && (exclusive ? x.forward[i].score < score : x.forward[i].score <= score)) {
                x = x.forward[i];
            }
        }
        return x == header ? null : x;
    }

    public Node insert(double score, NativeHandle member) {
        return insertPrepared(prepareInsert(score, member));
    }

    PreparedInsert prepareInsert(double score, NativeHandle member) {
        if (member == null) {
            throw new IllegalArgumentException("member must not be null");
        }
        return new PreparedInsert(
                new Node(levelFor(score, member), member, canonicalScore(score)),
                new Node[MAX_LEVEL],
                new int[MAX_LEVEL]
        );
    }

    Node insertPrepared(PreparedInsert prepared) {
        Objects.requireNonNull(prepared, "prepared");
        prepared.ensurePending();
        Node newNode = prepared.node;
        double score = newNode.score;
        NativeHandle member = newNode.member;

        Node[] update = prepared.update;
        int[] rank = prepared.rank;

        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            while (x.forward[i] != null && lessThan(x.forward[i], score, member)) {
                rank[i] += x.span[i];
                x = x.forward[i];
            }
            update[i] = x;
        }

        int newLevel = newNode.forward.length;
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                rank[i] = 0;
                update[i] = header;
                update[i].span[i] = length;
            }
            level = newLevel;
        }

        for (int i = 0; i < newLevel; i++) {
            newNode.forward[i] = update[i].forward[i];
            update[i].forward[i] = newNode;

            newNode.span[i] = update[i].span[i] - (rank[0] - rank[i]);
            update[i].span[i] = (rank[0] - rank[i]) + 1;
        }

        for (int i = newLevel; i < level; i++) {
            update[i].span[i]++;
        }

        newNode.backward = update[0] == header ? null : update[0];
        if (newNode.forward[0] != null) {
            newNode.forward[0].backward = newNode;
        } else {
            tail = newNode;
        }

        length++;
        nodeLevelCount += newLevel;
        prepared.published = true;
        return newNode;
    }

    public boolean delete(double score, NativeHandle member) {
        if (member == null) {
            return false;
        }

        return deletePrepared(new PreparedDelete(null, canonicalScore(score), member, new Node[MAX_LEVEL]));
    }

    PreparedDelete prepareDelete(Node expected) {
        Objects.requireNonNull(expected, "expected");
        return new PreparedDelete(
                expected,
                expected.score,
                expected.member,
                new Node[MAX_LEVEL]
        );
    }

    void validatePreparedDelete(PreparedDelete prepared) {
        Objects.requireNonNull(prepared, "prepared");
        prepared.ensurePending();
        Node candidate = findPreparedDeleteCandidate(prepared);
        if (!matchesPreparedDelete(candidate, prepared)) {
            throw new IllegalStateException("prepared skiplist delete source node changed");
        }
    }

    boolean deletePrepared(PreparedDelete prepared) {
        Objects.requireNonNull(prepared, "prepared");
        prepared.ensurePending();
        double score = prepared.score;
        NativeHandle member = prepared.member;

        Node[] update = prepared.update;
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && lessThan(x.forward[i], score, member)) {
                x = x.forward[i];
            }
            update[i] = x;
        }

        x = x.forward[0];
        if (!matchesPreparedDelete(x, prepared)) {
            return false;
        }

        for (int i = 0; i < level; i++) {
            if (update[i].forward[i] == x) {
                update[i].span[i] += x.span[i] - 1;
                update[i].forward[i] = x.forward[i];
            } else {
                update[i].span[i] -= 1;
            }
        }

        if (x.forward[0] != null) {
            x.forward[0].backward = x.backward;
        } else {
            tail = x.backward;
        }

        while (level > 1 && header.forward[level - 1] == null) {
            level--;
        }

        length--;
        nodeLevelCount -= x.forward.length;
        prepared.published = true;
        return true;
    }

    private Node findPreparedDeleteCandidate(PreparedDelete prepared) {
        Node current = header;
        for (int index = level - 1; index >= 0; index--) {
            while (current.forward[index] != null
                    && lessThan(current.forward[index], prepared.score, prepared.member)) {
                current = current.forward[index];
            }
        }
        return current.forward[0];
    }

    private boolean matchesPreparedDelete(Node candidate, PreparedDelete prepared) {
        return candidate != null
                && scoresEqual(candidate.score, prepared.score)
                && memberStore.compareLex(candidate.member, prepared.member) == 0
                && (prepared.expected == null || candidate == prepared.expected);
    }

    public Node getElementByRank(int rank) {
        if (rank <= 0 || rank > length) {
            return null;
        }

        Node x = header;
        int traversed = 0;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && traversed + x.span[i] <= rank) {
                traversed += x.span[i];
                x = x.forward[i];
            }
            if (traversed == rank) {
                return x;
            }
        }
        return null;
    }

    public long heapEstimatedBytes() {
        long headerBytes = NODE_FIXED_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Long.BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Integer.BYTES;
        long nodes = (long) length * (NODE_FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES * 2L);
        return FIXED_HEAP_BYTES + headerBytes + nodes + nodeLevelCount * (Long.BYTES + Integer.BYTES);
    }

    long heapEstimatedBytesAfterPreparedChanges(int addedNodes, long levelDelta) {
        if (addedNodes < 0) {
            throw new IllegalArgumentException("addedNodes must be >= 0");
        }
        long targetLength = (long) length + addedNodes;
        long targetLevelCount = nodeLevelCount + levelDelta;
        if (targetLevelCount < 0L) {
            throw new IllegalArgumentException("targetLevelCount must be >= 0");
        }
        long headerBytes = NODE_FIXED_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Long.BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Integer.BYTES;
        long nodes = targetLength * (NODE_FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES * 2L);
        return FIXED_HEAP_BYTES + headerBytes + nodes
                + targetLevelCount * (Long.BYTES + Integer.BYTES);
    }

    static long heapUpperBoundForNodes(long nodeCount) {
        if (nodeCount < 0L) {
            return Long.MAX_VALUE;
        }
        long headerBytes = NODE_FIXED_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Long.BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Integer.BYTES;
        long perNodeBytes = NODE_FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES * 2L
                + (long) MAX_LEVEL * (Long.BYTES + Integer.BYTES);
        long nodes = multiplySaturating(nodeCount, perNodeBytes);
        return addSaturating(FIXED_HEAP_BYTES + headerBytes, nodes);
    }

    static long preparedInsertWorkspaceHeapUpperBound(long insertCount) {
        if (insertCount < 0L) {
            return Long.MAX_VALUE;
        }
        long perInsert = PREPARED_INSERT_FIXED_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Long.BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Integer.BYTES;
        return multiplySaturating(insertCount, perInsert);
    }

    static long preparedMutationHeapUpperBound(long insertCount, long deleteCount) {
        if (insertCount < 0L || deleteCount < 0L || deleteCount > insertCount) {
            return Long.MAX_VALUE;
        }
        long nodeBytes = NODE_FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES * 2L
                + (long) MAX_LEVEL * (Long.BYTES + Integer.BYTES);
        long insertBytes = addSaturating(
                multiplySaturating(insertCount, nodeBytes),
                preparedInsertWorkspaceHeapUpperBound(insertCount)
        );
        long perDelete = PREPARED_DELETE_FIXED_HEAP_BYTES
                + ARRAY_HEADER_BYTES + (long) MAX_LEVEL * Long.BYTES;
        return addSaturating(insertBytes, multiplySaturating(deleteCount, perDelete));
    }

    private static long multiplySaturating(long left, long right) {
        return left == 0L || right == 0L || left <= Long.MAX_VALUE / right
                ? left * right
                : Long.MAX_VALUE;
    }

    private boolean lessThan(Node node, double score, NativeHandle member) {
        if (node.score < score) {
            return true;
        }
        if (node.score > score) {
            return false;
        }
        return memberStore.compareLex(node.member, member) < 0;
    }

    private static double canonicalScore(double score) {
        return score == 0.0d ? 0.0d : score;
    }

    private static boolean scoresEqual(double left, double right) {
        return left == right;
    }

    private int levelFor(double score, NativeHandle member) {
        int lvl = 1;
        long state = mix64(Double.doubleToLongBits(score) ^ memberStore.hashBytes(member));
        while (lvl < MAX_LEVEL && (state & 0x3L) == 0L) {
            lvl++;
            state = mix64(state + 0x9E3779B97F4A7C15L);
        }
        return lvl;
    }

    private static long mix64(long value) {
        long z = value;
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    static final class Node {
        final NativeHandle member;
        final double score;
        final Node[] forward;
        final int[] span;
        Node backward;

        Node(int level, NativeHandle member, double score) {
            this.member = member;
            this.score = score;
            this.forward = new Node[level];
            this.span = new int[level];
        }
    }

    static final class PreparedInsert {
        private final Node node;
        private final Node[] update;
        private final int[] rank;
        private boolean published;

        private PreparedInsert(Node node, Node[] update, int[] rank) {
            this.node = node;
            this.update = update;
            this.rank = rank;
        }

        Node node() {
            return node;
        }

        private void ensurePending() {
            if (published) {
                throw new IllegalStateException("prepared skiplist insert is already published");
            }
        }
    }

    static final class PreparedDelete {
        private final Node expected;
        private final double score;
        private final NativeHandle member;
        private final Node[] update;
        private boolean published;

        private PreparedDelete(Node expected, double score, NativeHandle member, Node[] update) {
            this.expected = expected;
            this.score = score;
            this.member = member;
            this.update = update;
        }

        private void ensurePending() {
            if (published) {
                throw new IllegalStateException("prepared skiplist delete is already published");
            }
        }
    }
}
