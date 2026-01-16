package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapZSet;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.charset.StandardCharsets;

final class ZSetValue implements YierdisValue {
    private static final int REF_BYTES = 8;

    private final YierdisOffHeapAddressAllocator offHeapAllocator;
    private final YierdisUnsafeOffHeapZSet offHeap;

    // Redis uses listpack for small ZSETs and upgrades to dict+skiplist as needed.
    // We approximate that behavior with an in-Java "listpack-like" sorted array and upgrade to
    // HashMap+skiplist once size/element thresholds are crossed.
    private PackedZSet listpack;
    private ByteArrayHashMap<ZSkipList.Node> byMember;
    private ZSkipList byScore;
    private long rawBytes;
    private long skiplistLevels;

    ZSetValue() {
        this.offHeapAllocator = null;
        this.offHeap = null;
        this.listpack = new PackedZSet();
    }

    ZSetValue(YierdisOffHeapAddressAllocator allocator) {
        this.offHeapAllocator = allocator;
        this.offHeap = new YierdisUnsafeOffHeapZSet(allocator);
        this.listpack = null;
    }

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    @Override
    public ValueEncoding encoding() {
        if (offHeapAllocator != null) {
            return ValueEncoding.ZSET_SKIPLIST;
        }
        return listpack != null ? ValueEncoding.ZSET_PACKED : ValueEncoding.ZSET_SKIPLIST;
    }

    int size() {
        if (offHeapAllocator != null) {
            return offHeap.size();
        }
        if (listpack != null) {
            return listpack.size();
        }
        return byMember.size();
    }

    long estimatedBytes() {
        if (offHeapAllocator != null) {
            return 0;
        }
        if (listpack != null) {
            return listpack.estimatedBytes();
        }
        // dict (hash map) + raw member bytes + skiplist forward/span arrays (approximate by level count).
        return byMember.estimatedBytes()
                + rawBytes
                + skiplistLevels * (REF_BYTES + Integer.BYTES);
    }

    int zaddMany(List<byte[]> scoreMemberPairs) {
        if (offHeapAllocator != null) {
            return offHeap.zaddMany(scoreMemberPairs);
        }
        int added = 0;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            byte[] memberBytes = scoreMemberPairs.get(i + 1);

            if (listpack != null) {
                if (memberBytes != null && memberBytes.length > YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES) {
                    convertToSkipList();
                }
                if (listpack != null) {
                    added += listpackZadd(score, memberBytes);
                    continue;
                }
            }

            added += skiplistZadd(score, memberBytes);
        }
        return added;
    }

    int zrem(List<byte[]> members) {
        if (offHeapAllocator != null) {
            return offHeap.zrem(members);
        }
        if (listpack != null) {
            int removed = 0;
            for (byte[] m : members) {
                removed += listpackZrem(m);
            }
            return removed;
        }

        int removed = 0;
        for (byte[] m : members) {
            ZSkipList.Node old = byMember.remove(m);
            if (old == null) {
                continue;
            }
            rawBytes -= old.member.length;
            skiplistLevels -= old.forward.length;
            byScore.delete(old.score, old.member);
            removed++;
        }
        return removed;
    }

    List<byte[]> zrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (offHeapAllocator != null) {
            return offHeap.zrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    List<byte[]> zrevrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (offHeapAllocator != null) {
            return offHeap.zrevrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrevrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrevrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (offHeapAllocator != null) {
            return offHeap.zremrangeByScore(min, minExclusive, max, maxExclusive);
        }
        if (listpack != null) {
            int removed = 0;
            for (int i = listpack.size() - 1; i >= 0; i--) {
                double s = listpack.scoreAt(i);
                if (scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                    listpack.removeAt(i);
                    removed++;
                }
            }
            return removed;
        }

        int removed = 0;
        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        while (node != null && scoreInRange(node.score, min, minExclusive, max, maxExclusive)) {
            ZSkipList.Node next = node.forward[0];
            rawBytes -= node.member.length;
            skiplistLevels -= node.forward.length;
            byMember.remove(node.member);
            byScore.delete(node.score, node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    int zremrangeByRank(long start, long stop) {
        if (offHeapAllocator != null) {
            return offHeap.zremrangeByRank(start, stop);
        }
        int size = size();
        if (size == 0) {
            return 0;
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        if (listpack != null) {
            int removed = 0;
            for (long i = normalizedStop; i >= normalizedStart; i--) {
                listpack.removeAt((int) i);
                removed++;
            }
            return removed;
        }

        int startRank = (int) normalizedStart + 1;
        int stopRank = (int) normalizedStop + 1;
        int remaining = stopRank - startRank + 1;
        int removed = 0;

        ZSkipList.Node node = byScore.getElementByRank(startRank);
        for (int i = 0; i < remaining && node != null; i++) {
            ZSkipList.Node next = node.forward[0];
            rawBytes -= node.member.length;
            skiplistLevels -= node.forward.length;
            byMember.remove(node.member);
            byScore.delete(node.score, node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    List<byte[]> zrange(long start, long stop, boolean withScores) {
        if (offHeapAllocator != null) {
            return offHeap.zrange(start, stop, withScores);
        }
        return rangeByIndex(start, stop, withScores, false);
    }

    List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        if (offHeapAllocator != null) {
            return offHeap.zrevrange(start, stop, withScores);
        }
        return rangeByIndex(start, stop, withScores, true);
    }

    int zrangeReplyCount(long start, long stop, boolean withScores) {
        if (offHeapAllocator != null) {
            return offHeap.zrangeReplyCount(start, stop, withScores);
        }
        return rangeByIndexReplyCount(start, stop, withScores, false);
    }

    void zrangeReplyInto(long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        if (offHeapAllocator != null) {
            offHeap.zrangeReplyInto(start, stop, withScores, out);
            return;
        }
        rangeByIndexReplyInto(start, stop, withScores, false, out);
    }

    int zrevrangeReplyCount(long start, long stop, boolean withScores) {
        if (offHeapAllocator != null) {
            return offHeap.zrevrangeReplyCount(start, stop, withScores);
        }
        return rangeByIndexReplyCount(start, stop, withScores, true);
    }

    void zrevrangeReplyInto(long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        if (offHeapAllocator != null) {
            offHeap.zrevrangeReplyInto(start, stop, withScores, out);
            return;
        }
        rangeByIndexReplyInto(start, stop, withScores, true, out);
    }

    int zrangeByScoreReplyCount(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (offHeapAllocator != null) {
            return offHeap.zrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        if (count <= 0) {
            return 0;
        }
        if (listpack != null) {
            return zrangeByScoreReplyCountListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrangeByScoreReplyCountSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    void zrangeByScoreReplyInto(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        if (offHeapAllocator != null) {
            offHeap.zrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (count <= 0) {
            return;
        }
        if (listpack != null) {
            zrangeByScoreReplyIntoListpack(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        zrangeByScoreReplyIntoSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    int zrevrangeByScoreReplyCount(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (offHeapAllocator != null) {
            return offHeap.zrevrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        if (count <= 0) {
            return 0;
        }
        if (listpack != null) {
            return zrevrangeByScoreReplyCountListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrevrangeByScoreReplyCountSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    void zrevrangeByScoreReplyInto(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        if (offHeapAllocator != null) {
            offHeap.zrevrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (count <= 0) {
            return;
        }
        if (listpack != null) {
            zrevrangeByScoreReplyIntoListpack(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        zrevrangeByScoreReplyIntoSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    private List<byte[]> rangeByIndex(long start, long stop, boolean withScores, boolean reverse) {
        if (listpack != null) {
            return rangeByIndexListpack(start, stop, withScores, reverse);
        }

        int size = byMember.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        int remaining;
        ZSkipList.Node node;
        boolean stepBackwards = reverse;
        if (!reverse) {
            int startRank = (int) normalizedStart + 1;
            int stopRank = (int) normalizedStop + 1;
            remaining = stopRank - startRank + 1;
            node = byScore.getElementByRank(startRank);
        } else {
            int startRank = size - (int) normalizedStart;
            int stopRank = size - (int) normalizedStop;
            remaining = startRank - stopRank + 1;
            node = byScore.getElementByRank(startRank);
        }

        if (!withScores) {
            List<byte[]> out = new ArrayList<>(remaining);
            for (int i = 0; i < remaining && node != null; i++) {
                out.add(node.member);
                node = stepBackwards ? node.backward : node.forward[0];
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (int i = 0; i < remaining && node != null; i++) {
            out.add(node.member);
            out.add(formatScoreBytes(node.score));
            node = stepBackwards ? node.backward : node.forward[0];
        }
        return out;
    }

    private int rangeByIndexReplyCount(long start, long stop, boolean withScores, boolean reverse) {
        int size = size();
        if (size == 0) {
            return 0;
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        int remaining = (int) (normalizedStop - normalizedStart + 1);
        if (remaining <= 0) {
            return 0;
        }
        if (!withScores) {
            return remaining;
        }

        long elementCount = (long) remaining * 2;
        if (elementCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("response is too large");
        }
        return (int) elementCount;
    }

    private void rangeByIndexReplyInto(long start, long stop, boolean withScores, boolean reverse, YierdisBulkStringOutput out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (listpack != null) {
            rangeByIndexReplyIntoListpack(start, stop, withScores, reverse, out);
            return;
        }
        rangeByIndexReplyIntoSkipList(start, stop, withScores, reverse, out);
    }

    private void rangeByIndexReplyIntoListpack(long start, long stop, boolean withScores, boolean reverse, YierdisBulkStringOutput out) {
        int size = listpack.size();
        if (size == 0) {
            return;
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        for (long i = normalizedStart; i <= normalizedStop; i++) {
            int idx = !reverse ? (int) i : (size - 1 - (int) i);
            listpack.memberWriteTo(idx, out);
            if (withScores) {
                writeScoreTo(out, listpack.scoreAt(idx));
            }
        }
    }

    private void rangeByIndexReplyIntoSkipList(long start, long stop, boolean withScores, boolean reverse, YierdisBulkStringOutput out) {
        int size = byMember.size();
        if (size == 0) {
            return;
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        int remaining = (int) (normalizedStop - normalizedStart + 1);
        boolean stepBackwards = reverse;

        int startRank = !reverse ? (int) normalizedStart + 1 : size - (int) normalizedStart;
        ZSkipList.Node node = byScore.getElementByRank(startRank);

        for (int i = 0; i < remaining && node != null; i++) {
            if (node.member == null) {
                out.bulkStringNull();
            } else {
                out.bulkString(node.member, 0, node.member.length);
            }
            if (withScores) {
                writeScoreTo(out, node.score);
            }
            node = stepBackwards ? node.backward : node.forward[0];
        }
    }

    private List<byte[]> zrangeByScoreListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int startIdx;
        if (min == Double.NEGATIVE_INFINITY) {
            startIdx = 0;
        } else {
            startIdx = firstIndexForMin(min, minExclusive);
            if (startIdx >= size) {
                return new ArrayList<>();
            }
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int expected = (int) Math.min(size, remaining);
        List<byte[]> out = new ArrayList<>(withScores ? expected * 2 : expected);

        for (int i = startIdx; i < size && remaining > 0; i++) {
            double score = listpack.scoreAt(i);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                if (max == Double.POSITIVE_INFINITY) {
                    continue;
                }
                if (score > max || (maxExclusive && Double.compare(score, max) == 0)) {
                    break;
                }
                continue;
            }
            if (skipped > 0) {
                skipped--;
                continue;
            }

            out.add(listpack.memberAt(i));
            if (withScores) {
                out.add(formatScoreBytes(score));
            }
            remaining--;
        }
        return out;
    }

    private int zrangeByScoreReplyCountListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return 0;
        }

        int startIdx;
        if (min == Double.NEGATIVE_INFINITY) {
            startIdx = 0;
        } else {
            startIdx = firstIndexForMin(min, minExclusive);
            if (startIdx >= size) {
                return 0;
            }
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int outCount = 0;

        for (int i = startIdx; i < size && remaining > 0; i++) {
            double score = listpack.scoreAt(i);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                if (max == Double.POSITIVE_INFINITY) {
                    continue;
                }
                if (score > max || (maxExclusive && Double.compare(score, max) == 0)) {
                    break;
                }
                continue;
            }
            if (skipped > 0) {
                skipped--;
                continue;
            }

            int add = withScores ? 2 : 1;
            if (outCount > Integer.MAX_VALUE - add) {
                throw new IllegalArgumentException("response is too large");
            }
            outCount += add;
            remaining--;
        }
        return outCount;
    }

    private void zrangeByScoreReplyIntoListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        int size = listpack.size();
        if (size == 0) {
            return;
        }

        int startIdx;
        if (min == Double.NEGATIVE_INFINITY) {
            startIdx = 0;
        } else {
            startIdx = firstIndexForMin(min, minExclusive);
            if (startIdx >= size) {
                return;
            }
        }

        long skipped = Math.max(0, offset);
        long remaining = count;

        for (int i = startIdx; i < size && remaining > 0; i++) {
            double score = listpack.scoreAt(i);
            if (!scoreInRange(score, min, minExclusive, max, maxExclusive)) {
                if (max == Double.POSITIVE_INFINITY) {
                    continue;
                }
                if (score > max || (maxExclusive && Double.compare(score, max) == 0)) {
                    break;
                }
                continue;
            }
            if (skipped > 0) {
                skipped--;
                continue;
            }

            listpack.memberWriteTo(i, out);
            if (withScores) {
                writeScoreTo(out, score);
            }
            remaining--;
        }
    }

    private List<byte[]> zrevrangeByScoreListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int startIdx = min == Double.NEGATIVE_INFINITY ? 0 : firstIndexForMin(min, minExclusive);
        int endIdx = max == Double.POSITIVE_INFINITY ? size - 1 : lastIndexForMax(max, maxExclusive);
        if (endIdx < startIdx) {
            return new ArrayList<>();
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int expected = (int) Math.min(size, remaining);
        List<byte[]> out = new ArrayList<>(withScores ? expected * 2 : expected);

        for (int i = endIdx; i >= startIdx && remaining > 0; i--) {
            if (skipped > 0) {
                skipped--;
                continue;
            }
            out.add(listpack.memberAt(i));
            if (withScores) {
                out.add(formatScoreBytes(listpack.scoreAt(i)));
            }
            remaining--;
        }
        return out;
    }

    private int zrevrangeByScoreReplyCountListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return 0;
        }

        int startIdx = min == Double.NEGATIVE_INFINITY ? 0 : firstIndexForMin(min, minExclusive);
        int endIdx = max == Double.POSITIVE_INFINITY ? size - 1 : lastIndexForMax(max, maxExclusive);
        if (endIdx < startIdx) {
            return 0;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int outCount = 0;
        for (int i = endIdx; i >= startIdx && remaining > 0; i--) {
            if (skipped > 0) {
                skipped--;
                continue;
            }
            int add = withScores ? 2 : 1;
            if (outCount > Integer.MAX_VALUE - add) {
                throw new IllegalArgumentException("response is too large");
            }
            outCount += add;
            remaining--;
        }
        return outCount;
    }

    private void zrevrangeByScoreReplyIntoListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        int size = listpack.size();
        if (size == 0) {
            return;
        }

        int startIdx = min == Double.NEGATIVE_INFINITY ? 0 : firstIndexForMin(min, minExclusive);
        int endIdx = max == Double.POSITIVE_INFINITY ? size - 1 : lastIndexForMax(max, maxExclusive);
        if (endIdx < startIdx) {
            return;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        for (int i = endIdx; i >= startIdx && remaining > 0; i--) {
            if (skipped > 0) {
                skipped--;
                continue;
            }
            listpack.memberWriteTo(i, out);
            if (withScores) {
                writeScoreTo(out, listpack.scoreAt(i));
            }
            remaining--;
        }
    }

    private int firstIndexForMin(double min, boolean exclusive) {
        int size = listpack.size();
        for (int i = 0; i < size; i++) {
            double s = listpack.scoreAt(i);
            if (Double.compare(s, min) > 0) {
                return i;
            }
            if (!exclusive && Double.compare(s, min) == 0) {
                return i;
            }
        }
        return size;
    }

    private int lastIndexForMax(double max, boolean exclusive) {
        int size = listpack.size();
        for (int i = size - 1; i >= 0; i--) {
            double s = listpack.scoreAt(i);
            if (Double.compare(s, max) < 0) {
                return i;
            }
            if (!exclusive && Double.compare(s, max) == 0) {
                return i;
            }
        }
        return -1;
    }

    private List<byte[]> zrangeByScoreSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = byMember.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        if (node == null) {
            return new ArrayList<>();
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int expected = (int) Math.min(size, remaining);
        List<byte[]> out = new ArrayList<>(withScores ? expected * 2 : expected);

        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.forward[0];
                continue;
            }

            out.add(node.member);
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.forward[0];
        }
        return out;
    }

    private int zrangeByScoreReplyCountSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = byMember.size();
        if (size == 0) {
            return 0;
        }

        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        if (node == null) {
            return 0;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int outCount = 0;

        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.forward[0];
                continue;
            }

            int add = withScores ? 2 : 1;
            if (outCount > Integer.MAX_VALUE - add) {
                throw new IllegalArgumentException("response is too large");
            }
            outCount += add;
            remaining--;
            node = node.forward[0];
        }
        return outCount;
    }

    private void zrangeByScoreReplyIntoSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        if (node == null) {
            return;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.forward[0];
                continue;
            }

            if (node.member == null) {
                out.bulkStringNull();
            } else {
                out.bulkString(node.member, 0, node.member.length);
            }
            if (withScores) {
                writeScoreTo(out, s);
            }
            remaining--;
            node = node.forward[0];
        }
    }

    private List<byte[]> zrevrangeByScoreSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = byMember.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        ZSkipList.Node node = lastNodeForMax(max, maxExclusive);
        if (node == null) {
            return new ArrayList<>();
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int expected = (int) Math.min(size, remaining);
        List<byte[]> out = new ArrayList<>(withScores ? expected * 2 : expected);

        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreAtOrAboveMin(s, min, minExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.backward;
                continue;
            }

            out.add(node.member);
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.backward;
        }
        return out;
    }

    private int zrevrangeByScoreReplyCountSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = byMember.size();
        if (size == 0) {
            return 0;
        }

        ZSkipList.Node node = lastNodeForMax(max, maxExclusive);
        if (node == null) {
            return 0;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        int outCount = 0;
        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreAtOrAboveMin(s, min, minExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.backward;
                continue;
            }

            int add = withScores ? 2 : 1;
            if (outCount > Integer.MAX_VALUE - add) {
                throw new IllegalArgumentException("response is too large");
            }
            outCount += add;
            remaining--;
            node = node.backward;
        }
        return outCount;
    }

    private void zrevrangeByScoreReplyIntoSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        ZSkipList.Node node = lastNodeForMax(max, maxExclusive);
        if (node == null) {
            return;
        }

        long skipped = Math.max(0, offset);
        long remaining = count;
        while (node != null && remaining > 0) {
            double s = node.score;
            if (!scoreAtOrAboveMin(s, min, minExclusive)) {
                break;
            }
            if (skipped > 0) {
                skipped--;
                node = node.backward;
                continue;
            }

            if (node.member == null) {
                out.bulkStringNull();
            } else {
                out.bulkString(node.member, 0, node.member.length);
            }
            if (withScores) {
                writeScoreTo(out, s);
            }
            remaining--;
            node = node.backward;
        }
    }

    private ZSkipList.Node firstNodeForMin(double min, boolean minExclusive) {
        if (min == Double.NEGATIVE_INFINITY) {
            return byScore.first();
        }
        return byScore.findFirstByScore(min, minExclusive);
    }

    private ZSkipList.Node lastNodeForMax(double max, boolean maxExclusive) {
        if (max == Double.POSITIVE_INFINITY) {
            return byScore.last();
        }
        return byScore.findLastByScore(max, maxExclusive);
    }

    private static boolean scoreAtOrAboveMin(double s, double min, boolean minExclusive) {
        if (Double.compare(s, min) < 0) {
            return false;
        }
        return !minExclusive || Double.compare(s, min) != 0;
    }

    private static boolean scoreInRange(double s, double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (Double.isNaN(s)) {
            return false;
        }
        if (Double.compare(s, min) < 0) {
            return false;
        }
        if (minExclusive && Double.compare(s, min) == 0) {
            return false;
        }
        if (Double.compare(s, max) > 0) {
            return false;
        }
        if (maxExclusive && Double.compare(s, max) == 0) {
            return false;
        }
        return true;
    }

    private List<byte[]> rangeByIndexListpack(long start, long stop, boolean withScores, boolean reverse) {
        int size = listpack.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);

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

        int remaining = (int) (normalizedStop - normalizedStart + 1);
        if (!withScores) {
            List<byte[]> out = new ArrayList<>(remaining);
            for (long i = normalizedStart; i <= normalizedStop; i++) {
                int idx = !reverse ? (int) i : (size - 1 - (int) i);
                out.add(listpack.memberAt(idx));
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (long i = normalizedStart; i <= normalizedStop; i++) {
            int idx = !reverse ? (int) i : (size - 1 - (int) i);
            out.add(listpack.memberAt(idx));
            out.add(formatScoreBytes(listpack.scoreAt(idx)));
        }
        return out;
    }

    private static long normalizeIndex(long idx, int size) {
        if (idx >= 0) {
            return idx;
        }
        return (long) size + idx;
    }

    private static int compareLex(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int av = a[i] & 0xFF;
            int bv = b[i] & 0xFF;
            if (av != bv) {
                return Integer.compare(av, bv);
            }
        }
        return Integer.compare(a.length, b.length);
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

    private static byte[] formatScoreBytes(double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            return Long.toString((long) score).getBytes(StandardCharsets.US_ASCII);
        }
        return Double.toString(score).getBytes(StandardCharsets.US_ASCII);
    }

    private static void writeScoreTo(YierdisBulkStringOutput out, double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            out.bulkStringLongAscii((long) score);
            return;
        }
        byte[] encoded = Double.toString(score).getBytes(StandardCharsets.US_ASCII);
        out.bulkString(encoded, 0, encoded.length);
    }

    private int skiplistZadd(double score, byte[] memberBytes) {
        if (byMember == null) {
            byMember = new ByteArrayHashMap<>();
            byScore = new ZSkipList();
        }

        ZSkipList.Node old = byMember.get(memberBytes);
        byte[] member = memberBytes;
        if (old != null) {
            member = old.member;
            if (Double.compare(old.score, score) == 0) {
                return 0;
            }
            skiplistLevels -= old.forward.length;
            byScore.delete(old.score, member);
        } else {
            rawBytes += memberBytes.length;
        }

        ZSkipList.Node next = byScore.insert(score, member);
        skiplistLevels += next.forward.length;
        byMember.put(member, next);
        return old == null ? 1 : 0;
    }

    private int listpackZadd(double score, byte[] memberBytes) {
        int idx = indexOfMember(memberBytes);
        if (idx >= 0) {
            double oldScore = listpack.scoreAt(idx);
            if (Double.compare(oldScore, score) == 0) {
                return 0;
            }
            byte[] member = listpack.memberAt(idx);
            listpack.removeAt(idx);
            insertSorted(member, score);
            return 0;
        }

        if (listpack.size() >= YierdisEncodingThresholds.ZSET_MAX_LISTPACK_ENTRIES) {
            convertToSkipList();
            return skiplistZadd(score, memberBytes);
        }

        insertSorted(memberBytes, score);
        return 1;
    }

    private int listpackZrem(byte[] memberBytes) {
        int idx = indexOfMember(memberBytes);
        if (idx < 0) {
            return 0;
        }
        listpack.removeAt(idx);
        return 1;
    }

    private int indexOfMember(byte[] memberBytes) {
        return listpack.indexOfMember(memberBytes);
    }

    private void insertSorted(byte[] member, double score) {
        int insertAt = 0;
        int size = listpack.size();
        for (; insertAt < size; insertAt++) {
            double nodeScore = listpack.scoreAt(insertAt);
            int cmp = Double.compare(nodeScore, score);
            if (cmp > 0) {
                break;
            }
            if (cmp < 0) {
                continue;
            }
            if (listpack.compareMemberAt(insertAt, member) >= 0) {
                break;
            }
        }
        listpack.insertAt(insertAt, score, member);
    }

    private void convertToSkipList() {
        if (listpack == null) {
            return;
        }
        int size = listpack.size();
        ByteArrayHashMap<ZSkipList.Node> outByMember = new ByteArrayHashMap<>(Math.max(16, size));
        ZSkipList outByScore = new ZSkipList();
        rawBytes = 0;
        skiplistLevels = 0;
        for (int i = 0; i < size; i++) {
            double score = listpack.scoreAt(i);
            byte[] member = listpack.memberAt(i);
            ZSkipList.Node n = outByScore.insert(score, member);
            outByMember.put(member, n);
            rawBytes += member.length;
            skiplistLevels += n.forward.length;
        }
        this.byMember = outByMember;
        this.byScore = outByScore;
        this.listpack = null;
    }

    @Override
    public void close() {
        if (offHeapAllocator != null) {
            offHeap.close();
        }
    }

    private static final class PackedZSet {
        private static final int SCORE_BYTES = Double.BYTES;

        private byte[] data = new byte[0];
        private int usedBytes = 0;
        private int size = 0;

        private int modCount = 0;
        private int offsetsModCount = -1;
        private int[] offsets = new int[0];

        int size() {
            return size;
        }

        long estimatedBytes() {
            return (long) data.length + (long) offsets.length * Integer.BYTES;
        }

        double scoreAt(int index) {
            Entry e = readEntry(offsetOfIndex(index));
            return Double.longBitsToDouble(e.scoreBits);
        }

        byte[] memberAt(int index) {
            Entry e = readEntry(offsetOfIndex(index));
            if (e.len < 0) {
                return null;
            }
            return Arrays.copyOfRange(data, e.dataOffset, e.dataOffset + e.len);
        }

        void memberWriteTo(int index, YierdisBulkStringOutput out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            Entry e = readEntry(offsetOfIndex(index));
            if (e.len < 0) {
                out.bulkStringNull();
                return;
            }
            out.bulkString(data, e.dataOffset, e.len);
        }

        int indexOfMember(byte[] memberBytes) {
            int idx = 0;
            int off = 0;
            while (idx < size) {
                Entry e = readEntry(off);
                if (e.equalsMember(data, memberBytes)) {
                    return idx;
                }
                off += e.totalBytes;
                idx++;
            }
            return -1;
        }

        int compareMemberAt(int index, byte[] other) {
            Entry e = readEntry(offsetOfIndex(index));
            return e.compareMemberLex(data, other);
        }

        void insertAt(int index, double score, byte[] memberBytes) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            }

            int len = memberBytes == null ? -1 : memberBytes.length;
            int rawLen = Math.max(0, len);
            int headerValue = len < 0 ? 0 : len + 1;
            int headerBytes = varIntSize(headerValue);
            int entryBytes = SCORE_BYTES + headerBytes + rawLen;

            int insertOffset = index == size ? usedBytes : offsetOfIndex(index);
            ensureCapacity(usedBytes + entryBytes);

            int move = usedBytes - insertOffset;
            if (move > 0) {
                System.arraycopy(data, insertOffset, data, insertOffset + entryBytes, move);
            }

            long bits = Double.doubleToLongBits(score);
            writeLongBE(data, insertOffset, bits);
            int p = writeVarInt(data, insertOffset + SCORE_BYTES, headerValue);
            if (rawLen > 0) {
                System.arraycopy(memberBytes, 0, data, p, rawLen);
            }

            usedBytes += entryBytes;
            size++;
            modCount++;
        }

        void removeAt(int index) {
            int off = offsetOfIndex(index);
            Entry e = readEntry(off);

            int tailOff = off + e.totalBytes;
            int move = usedBytes - tailOff;
            if (move > 0) {
                System.arraycopy(data, tailOff, data, off, move);
            }
            usedBytes -= e.totalBytes;
            size--;
            modCount++;
        }

        private int offsetOfIndex(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            ensureOffsets();
            return offsets[index];
        }

        private void ensureOffsets() {
            if (offsetsModCount == modCount && offsets.length >= size) {
                return;
            }
            if (size <= 0) {
                offsetsModCount = modCount;
                return;
            }
            if (offsets.length < size) {
                int next = Math.max(16, offsets.length);
                while (next < size) {
                    next <<= 1;
                }
                offsets = new int[next];
            }
            int off = 0;
            for (int i = 0; i < size; i++) {
                offsets[i] = off;
                Entry e = readEntry(off);
                off += e.totalBytes;
            }
            offsetsModCount = modCount;
        }

        private Entry readEntry(int offset) {
            if (offset < 0 || offset + SCORE_BYTES > usedBytes) {
                throw new IllegalStateException("corrupt packed zset");
            }
            long scoreBits = readLongBE(data, offset);
            int headerOffset = offset + SCORE_BYTES;
            int headerValue = readVarInt(data, headerOffset, usedBytes);
            int headerBytes = varIntSize(headerValue);
            int len = headerValue == 0 ? -1 : headerValue - 1;
            int dataOffset = headerOffset + headerBytes;
            int totalBytes = SCORE_BYTES + headerBytes + Math.max(0, len);
            if (dataOffset + Math.max(0, len) > usedBytes) {
                throw new IllegalStateException("corrupt packed zset");
            }
            return new Entry(scoreBits, len, dataOffset, totalBytes);
        }

        private void ensureCapacity(int desired) {
            if (desired <= data.length) {
                return;
            }
            int next = Math.max(32, data.length);
            while (next < desired) {
                int n = next < 1024 * 1024 ? (next << 1) : (next + 1024 * 1024);
                if (n <= next) {
                    next = desired;
                    break;
                }
                next = n;
            }
            data = Arrays.copyOf(data, next);
        }

        private static void writeLongBE(byte[] dst, int offset, long v) {
            dst[offset] = (byte) (v >>> 56);
            dst[offset + 1] = (byte) (v >>> 48);
            dst[offset + 2] = (byte) (v >>> 40);
            dst[offset + 3] = (byte) (v >>> 32);
            dst[offset + 4] = (byte) (v >>> 24);
            dst[offset + 5] = (byte) (v >>> 16);
            dst[offset + 6] = (byte) (v >>> 8);
            dst[offset + 7] = (byte) (v);
        }

        private static long readLongBE(byte[] src, int offset) {
            return ((long) (src[offset] & 0xFF) << 56)
                    | ((long) (src[offset + 1] & 0xFF) << 48)
                    | ((long) (src[offset + 2] & 0xFF) << 40)
                    | ((long) (src[offset + 3] & 0xFF) << 32)
                    | ((long) (src[offset + 4] & 0xFF) << 24)
                    | ((long) (src[offset + 5] & 0xFF) << 16)
                    | ((long) (src[offset + 6] & 0xFF) << 8)
                    | ((long) (src[offset + 7] & 0xFF));
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

        private static int writeVarInt(byte[] dst, int offset, int value) {
            int v = value;
            int p = offset;
            while ((v & ~0x7F) != 0) {
                dst[p++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            dst[p++] = (byte) (v & 0x7F);
            return p;
        }

        private static int readVarInt(byte[] src, int offset, int limit) {
            int result = 0;
            int shift = 0;
            int p = offset;
            while (p < limit) {
                int b = src[p++] & 0xFF;
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
                if (shift > 28) {
                    throw new IllegalStateException("varint too long");
                }
            }
            throw new IllegalStateException("unterminated varint");
        }

        private static final class Entry {
            final long scoreBits;
            final int len; // -1 for null
            final int dataOffset;
            final int totalBytes;

            private Entry(long scoreBits, int len, int dataOffset, int totalBytes) {
                this.scoreBits = scoreBits;
                this.len = len;
                this.dataOffset = dataOffset;
                this.totalBytes = totalBytes;
            }

            boolean equalsMember(byte[] buf, byte[] other) {
                if (len < 0) {
                    return other == null;
                }
                if (other == null || other.length != len) {
                    return false;
                }
                for (int i = 0; i < len; i++) {
                    if (buf[dataOffset + i] != other[i]) {
                        return false;
                    }
                }
                return true;
            }

            int compareMemberLex(byte[] buf, byte[] other) {
                if (len < 0) {
                    return other == null ? 0 : -1;
                }
                if (other == null) {
                    return 1;
                }

                int min = Math.min(len, other.length);
                for (int i = 0; i < min; i++) {
                    int av = buf[dataOffset + i] & 0xFF;
                    int bv = other[i] & 0xFF;
                    if (av != bv) {
                        return Integer.compare(av, bv);
                    }
                }
                return Integer.compare(len, other.length);
            }
        }
    }

}
