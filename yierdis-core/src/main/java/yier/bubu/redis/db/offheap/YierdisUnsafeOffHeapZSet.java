package yier.bubu.redis.db.offheap;

import yier.bubu.redis.db.YierdisBulkStringOutput;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class YierdisUnsafeOffHeapZSet implements AutoCloseable {
    private final YierdisOffHeapAddressAllocator allocator;
    private final YierdisUnsafeOffHeapDictLong byMember;
    private final UnsafeZSkipList byScore;

    public YierdisUnsafeOffHeapZSet(YierdisOffHeapAddressAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.byMember = new YierdisUnsafeOffHeapDictLong(allocator);
        this.byScore = new UnsafeZSkipList(allocator);
    }

    public int size() {
        return byMember.size();
    }

    public int zaddMany(List<byte[]> scoreMemberPairs) {
        int added = 0;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            byte[] memberBytes = scoreMemberPairs.get(i + 1);
            added += zaddOne(score, memberBytes);
        }
        return added;
    }

    public int zrem(List<byte[]> members) {
        int removed = 0;
        for (byte[] m : members) {
            removed += zremOne(m);
        }
        return removed;
    }

    public List<byte[]> zrange(long start, long stop, boolean withScores) {
        int count = zrangeReplyCount(start, stop, withScores);
        if (count == 0) {
            return new ArrayList<>();
        }

        List<byte[]> out = new ArrayList<>(count);
        zrangeReplyInto(start, stop, withScores, new ListBulkOutput(out));
        return out;
    }

    public void zrangeReplyInto(long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        Objects.requireNonNull(out, "out");
        int size = size();
        if (size == 0) {
            return;
        }
        IndexRange r = normalizeRange(start, stop, size);
        if (r == null) {
            return;
        }

        int rank = r.start + 1; // 1-indexed
        long node = byScore.getElementByRank(rank);
        int remaining = r.stop - r.start + 1;
        while (node != 0 && remaining-- > 0) {
            writeNodeTo(out, node, withScores);
            node = byScore.next(node);
        }
    }

    public int zrangeReplyCount(long start, long stop, boolean withScores) {
        int size = size();
        if (size == 0) {
            return 0;
        }
        IndexRange r = normalizeRange(start, stop, size);
        if (r == null) {
            return 0;
        }
        int items = r.stop - r.start + 1;
        return withScores ? items * 2 : items;
    }

    public List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        int count = zrevrangeReplyCount(start, stop, withScores);
        if (count == 0) {
            return new ArrayList<>();
        }

        List<byte[]> out = new ArrayList<>(count);
        zrevrangeReplyInto(start, stop, withScores, new ListBulkOutput(out));
        return out;
    }

    public void zrevrangeReplyInto(long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        Objects.requireNonNull(out, "out");
        int size = size();
        if (size == 0) {
            return;
        }
        IndexRange r = normalizeRange(start, stop, size);
        if (r == null) {
            return;
        }

        int startRankFromHead = size - r.start; // 1-indexed
        long node = byScore.getElementByRank(startRankFromHead);
        int remaining = r.stop - r.start + 1;
        while (node != 0 && remaining-- > 0) {
            writeNodeTo(out, node, withScores);
            node = byScore.prev(node);
        }
    }

    public int zrevrangeReplyCount(long start, long stop, boolean withScores) {
        int size = size();
        if (size == 0) {
            return 0;
        }
        IndexRange r = normalizeRange(start, stop, size);
        if (r == null) {
            return 0;
        }
        int items = r.stop - r.start + 1;
        return withScores ? items * 2 : items;
    }

    public List<byte[]> zrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int replyCount = zrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
        if (replyCount == 0) {
            return new ArrayList<>();
        }
        List<byte[]> out = new ArrayList<>(replyCount);
        zrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, new ListBulkOutput(out));
        return out;
    }

    public int zrangeByScoreReplyCount(double min,
                                      boolean minExclusive,
                                      double max,
                                      boolean maxExclusive,
                                      boolean withScores,
                                      long offset,
                                      long count) {
        if (count <= 0) {
            return 0;
        }
        if (size() == 0) {
            return 0;
        }
        if (offset < 0) {
            offset = 0;
        }

        long node = byScore.findFirstByScore(min, minExclusive);
        if (node == 0) {
            return 0;
        }
        int emitted = 0;
        long remainingOffset = offset;
        long remainingCount = count;
        while (node != 0) {
            double score = byScore.score(node);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                break;
            }
            if (remainingOffset > 0) {
                remainingOffset--;
                node = byScore.next(node);
                continue;
            }
            emitted++;
            if (--remainingCount <= 0) {
                break;
            }
            node = byScore.next(node);
        }
        return withScores ? emitted * 2 : emitted;
    }

    public void zrangeByScoreReplyInto(double min,
                                       boolean minExclusive,
                                       double max,
                                       boolean maxExclusive,
                                       boolean withScores,
                                       long offset,
                                       long count,
                                       YierdisBulkStringOutput out) {
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            return;
        }
        if (size() == 0) {
            return;
        }
        if (offset < 0) {
            offset = 0;
        }

        long node = byScore.findFirstByScore(min, minExclusive);
        if (node == 0) {
            return;
        }

        long remainingOffset = offset;
        long remainingCount = count;
        while (node != 0 && remainingCount > 0) {
            double score = byScore.score(node);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                break;
            }

            if (remainingOffset > 0) {
                remainingOffset--;
                node = byScore.next(node);
                continue;
            }

            writeNodeTo(out, node, withScores);
            remainingCount--;
            node = byScore.next(node);
        }
    }

    public List<byte[]> zrevrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int replyCount = zrevrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
        if (replyCount == 0) {
            return new ArrayList<>();
        }
        List<byte[]> out = new ArrayList<>(replyCount);
        zrevrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, new ListBulkOutput(out));
        return out;
    }

    public int zrevrangeByScoreReplyCount(double min,
                                         boolean minExclusive,
                                         double max,
                                         boolean maxExclusive,
                                         boolean withScores,
                                         long offset,
                                         long count) {
        if (count <= 0) {
            return 0;
        }
        if (size() == 0) {
            return 0;
        }
        if (offset < 0) {
            offset = 0;
        }

        long node = byScore.findLastByScore(max, maxExclusive);
        if (node == 0) {
            return 0;
        }

        int emitted = 0;
        long remainingOffset = offset;
        long remainingCount = count;
        while (node != 0) {
            double score = byScore.score(node);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                break;
            }

            if (remainingOffset > 0) {
                remainingOffset--;
                node = byScore.prev(node);
                continue;
            }
            emitted++;
            if (--remainingCount <= 0) {
                break;
            }
            node = byScore.prev(node);
        }
        return withScores ? emitted * 2 : emitted;
    }

    public void zrevrangeByScoreReplyInto(double min,
                                          boolean minExclusive,
                                          double max,
                                          boolean maxExclusive,
                                          boolean withScores,
                                          long offset,
                                          long count,
                                          YierdisBulkStringOutput out) {
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            return;
        }
        if (size() == 0) {
            return;
        }
        if (offset < 0) {
            offset = 0;
        }

        long node = byScore.findLastByScore(max, maxExclusive);
        if (node == 0) {
            return;
        }

        long remainingOffset = offset;
        long remainingCount = count;
        while (node != 0 && remainingCount > 0) {
            double score = byScore.score(node);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                break;
            }

            if (remainingOffset > 0) {
                remainingOffset--;
                node = byScore.prev(node);
                continue;
            }

            writeNodeTo(out, node, withScores);
            remainingCount--;
            node = byScore.prev(node);
        }
    }

    public int zremrangeByRank(long start, long stop) {
        int size = size();
        if (size == 0) {
            return 0;
        }
        IndexRange r = normalizeRange(start, stop, size);
        if (r == null) {
            return 0;
        }

        int rank = r.start + 1; // 1-indexed
        long node = byScore.getElementByRank(rank);
        int remaining = r.stop - r.start + 1;
        int removed = 0;
        while (node != 0 && remaining-- > 0) {
            long next = byScore.next(node);
            removeNode(node);
            removed++;
            node = next;
        }
        return removed;
    }

    public int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (size() == 0) {
            return 0;
        }
        long node = byScore.findFirstByScore(min, minExclusive);
        if (node == 0) {
            return 0;
        }

        int removed = 0;
        while (node != 0) {
            double score = byScore.score(node);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                break;
            }
            long next = byScore.next(node);
            removeNode(node);
            removed++;
            node = next;
        }
        return removed;
    }

    @Override
    public void close() {
        long node = byScore.first();
        while (node != 0) {
            long next = byScore.next(node);
            byScore.freeNode(node);
            node = next;
        }
        byScore.close();
        byMember.close();
    }

    private int zaddOne(double score, byte[] memberBytes) {
        if (memberBytes == null) {
            throw new IllegalArgumentException("member must not be null");
        }

        long existing = byMember.get(memberBytes);
        if (existing != 0L) {
            double oldScore = byScore.score(existing);
            if (Double.compare(oldScore, score) == 0) {
                return 0;
            }
            long memberPtr = byScore.memberPtr(existing);
            int memberLen = byScore.memberLen(existing);
            int memberHash = byScore.memberHash(existing);

            long newNode = byScore.insert(score, memberPtr, memberLen, memberHash);
            try {
                byMember.put(memberBytes, newNode);
            } catch (RuntimeException e) {
                long removed = byScore.delete(score, memberPtr, memberLen);
                if (removed != 0L) {
                    byScore.freeNode(removed);
                }
                throw e;
            }

            long removed = byScore.delete(oldScore, memberPtr, memberLen);
            if (removed == 0L) {
                throw new IllegalStateException("failed to delete existing zset node");
            }
            byScore.freeNode(removed);
            return 0;
        }

        // First insertion: let the dict own key bytes; the node references the canonical dict key pointer.
        long old = byMember.put(memberBytes, 1L);
        if (old != 0L) {
            // Should not happen; we just checked via get().
            return 0;
        }

        YierdisUnsafeOffHeapDictLong.KeyHandle handle = byMember.keyHandle(memberBytes);
        if (handle == null) {
            byMember.remove(memberBytes);
            throw new IllegalStateException("failed to resolve dict key handle for zset member");
        }

        long node;
        try {
            node = byScore.insert(score, handle.keyPtr, handle.keyLen, handle.hash);
        } catch (RuntimeException e) {
            byMember.remove(memberBytes);
            throw e;
        }

        try {
            byMember.put(memberBytes, node);
        } catch (RuntimeException e) {
            long removed = byScore.delete(score, handle.keyPtr, handle.keyLen);
            if (removed != 0L) {
                byScore.freeNode(removed);
            }
            byMember.remove(memberBytes);
            throw e;
        }

        return 1;
    }

    private int zremOne(byte[] memberBytes) {
        if (memberBytes == null) {
            throw new IllegalArgumentException("member must not be null");
        }
        long node = byMember.get(memberBytes);
        if (node == 0L) {
            return 0;
        }

        long memberPtr = byScore.memberPtr(node);
        int memberLen = byScore.memberLen(node);
        int memberHash = byScore.memberHash(node);
        double score = byScore.score(node);

        long removed = byScore.delete(score, memberPtr, memberLen);
        if (removed == 0L) {
            return 0;
        }
        byScore.freeNode(removed);
        byMember.removeByPtr(memberPtr, memberLen, memberHash);
        return 1;
    }

    private void removeNode(long node) {
        long memberPtr = byScore.memberPtr(node);
        int memberLen = byScore.memberLen(node);
        int memberHash = byScore.memberHash(node);
        double score = byScore.score(node);

        long removed = byScore.delete(score, memberPtr, memberLen);
        if (removed == 0L) {
            return;
        }
        byScore.freeNode(removed);
        byMember.removeByPtr(memberPtr, memberLen, memberHash);
    }

    private void writeNodeTo(YierdisBulkStringOutput out, long node, boolean withScores) {
        out.bulkString(new YierdisUnsafeOffHeapRawSlice(allocator, byScore.memberPtr(node), byScore.memberLen(node)));
        if (!withScores) {
            return;
        }
        writeScoreTo(out, byScore.score(node));
    }

    private static IndexRange normalizeRange(long start, long stop, int size) {
        long s = start < 0 ? (long) size + start : start;
        long e = stop < 0 ? (long) size + stop : stop;

        if (s < 0) {
            s = 0;
        }
        if (e < 0) {
            return null;
        }
        if (s >= size) {
            return null;
        }
        if (e >= size) {
            e = size - 1L;
        }
        if (s > e) {
            return null;
        }
        return new IndexRange((int) s, (int) e);
    }

    private static boolean scoreInRange(double score, double min, boolean minExclusive, double max, boolean maxExclusive) {
        boolean aboveMin = minExclusive ? score > min : score >= min;
        boolean belowMax = maxExclusive ? score < max : score <= max;
        return aboveMin && belowMax;
    }

    private static double parseScore(byte[] s) {
        double v;
        try {
            v = Double.parseDouble(new String(s, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new YierdisDb.YierdisCommandException("ERR value is not a valid float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisDb.YierdisCommandException("ERR value is not a valid float");
        }
        return v;
    }

    private static void writeScoreTo(YierdisBulkStringOutput out, double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            out.bulkStringLongAscii((long) score);
            return;
        }
        byte[] encoded = Double.toString(score).getBytes(StandardCharsets.US_ASCII);
        out.bulkString(encoded, 0, encoded.length);
    }

    private static final class IndexRange {
        final int start;
        final int stop;

        IndexRange(int start, int stop) {
            this.start = start;
            this.stop = stop;
        }
    }

    private static final class ListBulkOutput implements YierdisBulkStringOutput {
        private final List<byte[]> out;

        private ListBulkOutput(List<byte[]> out) {
            this.out = out;
        }

        @Override
        public void bulkString(byte[] buf, int off, int len) {
            if (buf == null) {
                out.add(null);
                return;
            }
            byte[] copy = new byte[len];
            System.arraycopy(buf, off, copy, 0, len);
            out.add(copy);
        }

        @Override
        public void bulkString(yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice slice) {
            if (slice == null) {
                out.add(null);
                return;
            }
            byte[] copy = new byte[slice.length()];
            slice.getBytes(0, copy, 0, copy.length);
            out.add(copy);
        }

        @Override
        public void bulkStringNull() {
            out.add(null);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            out.add(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static final class UnsafeZSkipList implements AutoCloseable {
        private static final int MAX_LEVEL = 32;
        private static final double P = 0.25d;

        private static final int MEMBER_PTR_OFFSET = 0;
        private static final int MEMBER_LEN_OFFSET = 8;
        private static final int MEMBER_HASH_OFFSET = 12;
        private static final int SCORE_BITS_OFFSET = 16;
        private static final int BACKWARD_PTR_OFFSET = 24;
        private static final int LEVEL_OFFSET = 32;
        private static final int HEADER_BYTES = 40;

        private final YierdisOffHeapAddressAllocator allocator;
        private final long header;

        private long tail;
        private int level = 1;
        private int length = 0;

        UnsafeZSkipList(YierdisOffHeapAddressAllocator allocator) {
            this.allocator = allocator;
            this.header = allocateNode(MAX_LEVEL, 0L, 0, 0, 0L);
        }

        long first() {
            return forward(header, 0);
        }

        long getElementByRank(int rank) {
            if (rank <= 0 || rank > length) {
                return 0L;
            }

            long x = header;
            int traversed = 0;
            for (int i = level - 1; i >= 0; i--) {
                while (forward(x, i) != 0L && traversed + span(x, i) <= rank) {
                    traversed += span(x, i);
                    x = forward(x, i);
                }
                if (traversed == rank) {
                    return x;
                }
            }
            return 0L;
        }

        long findFirstByScore(double score, boolean exclusive) {
            long x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (forward(x, i) != 0L && (exclusive ? this.score(forward(x, i)) <= score : this.score(forward(x, i)) < score)) {
                    x = forward(x, i);
                }
            }
            return forward(x, 0);
        }

        long findLastByScore(double score, boolean exclusive) {
            long x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (forward(x, i) != 0L && (exclusive ? this.score(forward(x, i)) < score : this.score(forward(x, i)) <= score)) {
                    x = forward(x, i);
                }
            }
            return x == header ? 0L : x;
        }

        long next(long node) {
            return forward(node, 0);
        }

        long prev(long node) {
            return backward(node);
        }

        double score(long node) {
            return Double.longBitsToDouble(readLong(node + SCORE_BITS_OFFSET));
        }

        long memberPtr(long node) {
            return readLong(node + MEMBER_PTR_OFFSET);
        }

        int memberLen(long node) {
            return readInt(node + MEMBER_LEN_OFFSET);
        }

        int memberHash(long node) {
            return readInt(node + MEMBER_HASH_OFFSET);
        }

        long insert(double score, long memberPtr, int memberLen, int memberHash) {
            if (memberLen < 0) {
                throw new IllegalArgumentException("memberLen must be >= 0");
            }

            long[] update = new long[MAX_LEVEL];
            int[] rank = new int[MAX_LEVEL];

            long x = header;
            for (int i = level - 1; i >= 0; i--) {
                rank[i] = i == level - 1 ? 0 : rank[i + 1];
                while (forward(x, i) != 0L && lessThan(forward(x, i), score, memberPtr, memberLen)) {
                    rank[i] += span(x, i);
                    x = forward(x, i);
                }
                update[i] = x;
            }

            int newLevel = randomLevel();
            if (newLevel > level) {
                for (int i = level; i < newLevel; i++) {
                    rank[i] = 0;
                    update[i] = header;
                    setSpan(update[i], i, length);
                }
                level = newLevel;
            }

            long node = allocateNode(newLevel, memberPtr, memberLen, memberHash, Double.doubleToLongBits(score));
            for (int i = 0; i < newLevel; i++) {
                setForward(node, i, forward(update[i], i));
                setForward(update[i], i, node);

                setSpan(node, i, span(update[i], i) - (rank[0] - rank[i]));
                setSpan(update[i], i, (rank[0] - rank[i]) + 1);
            }
            for (int i = newLevel; i < level; i++) {
                setSpan(update[i], i, span(update[i], i) + 1);
            }

            setBackward(node, update[0] == header ? 0L : update[0]);
            long next = forward(node, 0);
            if (next != 0L) {
                setBackward(next, node);
            } else {
                tail = node;
            }

            length++;
            return node;
        }

        long delete(double score, long memberPtr, int memberLen) {
            long[] update = new long[MAX_LEVEL];
            long x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (forward(x, i) != 0L && lessThan(forward(x, i), score, memberPtr, memberLen)) {
                    x = forward(x, i);
                }
                update[i] = x;
            }

            x = forward(x, 0);
            if (x == 0L) {
                return 0L;
            }
            if (Double.compare(this.score(x), score) != 0) {
                return 0L;
            }
            if (compareLex(memberPtr(x), memberLen(x), memberPtr, memberLen) != 0) {
                return 0L;
            }

            for (int i = 0; i < level; i++) {
                if (forward(update[i], i) == x) {
                    setSpan(update[i], i, span(update[i], i) + span(x, i) - 1);
                    setForward(update[i], i, forward(x, i));
                } else {
                    setSpan(update[i], i, span(update[i], i) - 1);
                }
            }

            long next = forward(x, 0);
            if (next != 0L) {
                setBackward(next, backward(x));
            } else {
                tail = backward(x);
            }

            while (level > 1 && forward(header, level - 1) == 0L) {
                level--;
            }
            length--;
            return x;
        }

        void freeNode(long node) {
            int lvl = readInt(node + LEVEL_OFFSET);
            allocator.freeAddress(node, nodeBytesForLevel(lvl));
        }

        @Override
        public void close() {
            allocator.freeAddress(header, nodeBytesForLevel(MAX_LEVEL));
        }

        private static int nodeBytesForLevel(int level) {
            int raw = HEADER_BYTES + level * Long.BYTES + level * Integer.BYTES;
            int x = raw + 7;
            return x & ~7;
        }

        private long allocateNode(int level, long memberPtr, int memberLen, int memberHash, long scoreBits) {
            int bytes = nodeBytesForLevel(level);
            long addr = allocator.allocateAddress(bytes);
            writeLong(addr + MEMBER_PTR_OFFSET, memberPtr);
            writeInt(addr + MEMBER_LEN_OFFSET, memberLen);
            writeInt(addr + MEMBER_HASH_OFFSET, memberHash);
            writeLong(addr + SCORE_BITS_OFFSET, scoreBits);
            writeLong(addr + BACKWARD_PTR_OFFSET, 0L);
            writeInt(addr + LEVEL_OFFSET, level);
            for (int i = 0; i < level; i++) {
                writeLong(addr + forwardOffset(i), 0L);
                writeInt(addr + spanOffset(i, level), 0);
            }
            return addr;
        }

        private boolean lessThan(long node, double score, long memberPtr, int memberLen) {
            double ns = this.score(node);
            int cmp = Double.compare(ns, score);
            if (cmp < 0) {
                return true;
            }
            if (cmp > 0) {
                return false;
            }
            return compareLex(memberPtr(node), memberLen(node), memberPtr, memberLen) < 0;
        }

        private int compareLex(long aPtr, int aLen, long bPtr, int bLen) {
            int min = Math.min(aLen, bLen);
            for (int i = 0; i < min; i++) {
                int av = allocator.getByte(aPtr + i) & 0xFF;
                int bv = allocator.getByte(bPtr + i) & 0xFF;
                if (av != bv) {
                    return Integer.compare(av, bv);
                }
            }
            return Integer.compare(aLen, bLen);
        }

        private static int randomLevel() {
            int lvl = 1;
            while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < P) {
                lvl++;
            }
            return lvl;
        }

        private long forward(long node, int level) {
            return readLong(node + forwardOffset(level));
        }

        private void setForward(long node, int level, long next) {
            writeLong(node + forwardOffset(level), next);
        }

        private int span(long node, int level) {
            return readInt(node + spanOffset(level, readInt(node + LEVEL_OFFSET)));
        }

        private void setSpan(long node, int level, int value) {
            writeInt(node + spanOffset(level, readInt(node + LEVEL_OFFSET)), value);
        }

        private long backward(long node) {
            return readLong(node + BACKWARD_PTR_OFFSET);
        }

        private void setBackward(long node, long prev) {
            writeLong(node + BACKWARD_PTR_OFFSET, prev);
        }

        private static int forwardOffset(int level) {
            return HEADER_BYTES + level * Long.BYTES;
        }

        private static int spanOffset(int level, int nodeLevel) {
            return HEADER_BYTES + nodeLevel * Long.BYTES + level * Integer.BYTES;
        }

        private int readInt(long addr) {
            int b0 = allocator.getByte(addr) & 0xff;
            int b1 = allocator.getByte(addr + 1) & 0xff;
            int b2 = allocator.getByte(addr + 2) & 0xff;
            int b3 = allocator.getByte(addr + 3) & 0xff;
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }

        private void writeInt(long addr, int value) {
            allocator.putByte(addr, (byte) value);
            allocator.putByte(addr + 1, (byte) (value >>> 8));
            allocator.putByte(addr + 2, (byte) (value >>> 16));
            allocator.putByte(addr + 3, (byte) (value >>> 24));
        }

        private long readLong(long addr) {
            long b0 = allocator.getByte(addr) & 0xffL;
            long b1 = allocator.getByte(addr + 1) & 0xffL;
            long b2 = allocator.getByte(addr + 2) & 0xffL;
            long b3 = allocator.getByte(addr + 3) & 0xffL;
            long b4 = allocator.getByte(addr + 4) & 0xffL;
            long b5 = allocator.getByte(addr + 5) & 0xffL;
            long b6 = allocator.getByte(addr + 6) & 0xffL;
            long b7 = allocator.getByte(addr + 7) & 0xffL;
            return b0
                    | (b1 << 8)
                    | (b2 << 16)
                    | (b3 << 24)
                    | (b4 << 32)
                    | (b5 << 40)
                    | (b6 << 48)
                    | (b7 << 56);
        }

        private void writeLong(long addr, long value) {
            allocator.putByte(addr, (byte) value);
            allocator.putByte(addr + 1, (byte) (value >>> 8));
            allocator.putByte(addr + 2, (byte) (value >>> 16));
            allocator.putByte(addr + 3, (byte) (value >>> 24));
            allocator.putByte(addr + 4, (byte) (value >>> 32));
            allocator.putByte(addr + 5, (byte) (value >>> 40));
            allocator.putByte(addr + 6, (byte) (value >>> 48));
            allocator.putByte(addr + 7, (byte) (value >>> 56));
        }
    }
}
