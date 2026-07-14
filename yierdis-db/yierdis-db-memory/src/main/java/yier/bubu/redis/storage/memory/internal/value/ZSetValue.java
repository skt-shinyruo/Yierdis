package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.nio.charset.StandardCharsets;

public final class ZSetValue implements YierdisValue, NativeHandleOwner, HeapTrackedValue {
    public record ZAddResult(int added, boolean changedAny) {
    }

    private static final int REF_BYTES = 8;
    private static final long FIXED_HEAP_BYTES = 96L;

    private final NativeByteStore memberStore;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;

    // Redis uses listpack for small ZSETs and upgrades to dict+skiplist as needed.
    // We approximate that behavior with an in-Java "listpack-like" sorted array and upgrade to
    // HashMap+skiplist once size/element thresholds are crossed.
    private NativePackedZSet listpack;
    private NativeByteMap<ZSkipList.Node> byMember;
    private ZSkipList byScore;
    private long skiplistLevels;
    private Runnable heapChangeListener = () -> {
    };

    public ZSetValue(NativeAllocator allocator) {
        this(allocator, HashSeed.random());
    }

    public ZSetValue(NativeAllocator allocator, HashSeed hashSeed) {
        this(allocator, hashSeed, null);
    }

    public ZSetValue(
            NativeAllocator allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        NativeAllocator nativeAllocator = Objects.requireNonNull(allocator, "allocator");
        this.memberStore = new NativeByteStore(nativeAllocator, NativeObjectKind.ZSET_MEMBER_BYTES);
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.listpack = new NativePackedZSet(memberStore);
    }

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    @Override
    public ValueEncoding encoding() {
        return listpack != null ? ValueEncoding.ZSET_PACKED : ValueEncoding.ZSET_SKIPLIST;
    }

    public int size() {
        if (listpack != null) {
            return listpack.size();
        }
        return byMember.size();
    }

    public long preparedCopyHeapUpperBound(List<byte[]> scoreMemberPairs) {
        long expectedMembers = size();
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                if (scoreMemberPairs.get(i) != null) {
                    expectedMembers = addSaturating(expectedMembers, 1L);
                }
            }
        }
        return heapUpperBoundForMemberCount(expectedMembers);
    }

    public static long preparedNewHeapUpperBound(List<byte[]> scoreMemberPairs) {
        long expectedMembers = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                if (scoreMemberPairs.get(i) != null) {
                    expectedMembers = addSaturating(expectedMembers, 1L);
                }
            }
        }
        return heapUpperBoundForMemberCount(expectedMembers);
    }

    public HashTableMetrics memberTableMetrics() {
        return byMember == null ? null : byMember.metrics();
    }

    public boolean hasMemberTableMaintenanceDebt() {
        return byMember != null && byMember.hasMaintenanceDebt();
    }

    public long estimatedBytes() {
        if (listpack != null) {
            return listpack.estimatedBytes();
        }
        // dict (hash map) + raw member bytes + skiplist forward/span arrays (approximate by level count).
        return memberStore.nativeBytes() + skiplistLevels * (REF_BYTES + Integer.BYTES);
    }

    @Override
    public long heapEstimatedBytes() {
        if (listpack != null) {
            return FIXED_HEAP_BYTES + listpack.heapEstimatedBytes();
        }
        long memberTableBytes = byMember == null ? 0L : byMember.heapEstimatedBytes();
        long skipListBytes = byScore == null ? 0L : byScore.heapEstimatedBytes();
        return FIXED_HEAP_BYTES + memberTableBytes + skipListBytes;
    }

    @Override
    public void setHeapChangeListener(Runnable listener) {
        heapChangeListener = Objects.requireNonNull(listener, "listener");
    }

    public int[] nativePayloadSizes() {
        List<byte[]> memberScorePairs = zrange(0, -1, true);
        int[] sizes = new int[memberScorePairs.size() / 2];
        int next = 0;
        for (int i = 0; i + 1 < memberScorePairs.size(); i += 2) {
            byte[] member = memberScorePairs.get(i);
            sizes[next++] = member == null ? 0 : member.length;
        }
        return sizes;
    }

    public ZAddResult prepareAdd(List<byte[]> scoreMemberPairs) {
        return prepareAddInternal(scoreMemberPairs);
    }

    @Override
    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (listpack != null) {
            listpack.forEachNativeHandle(consumer);
            return;
        }
        if (byMember != null) {
            byMember.forEach((memberRef, node) -> {
                consumer.accept(memberRef);
                consumer.accept(node.member);
            });
        }
    }

    public int zaddMany(List<byte[]> scoreMemberPairs) {
        return prepareAdd(scoreMemberPairs).added();
    }

    public ZAddResult zaddManyResult(List<byte[]> scoreMemberPairs) {
        return prepareAddInternal(scoreMemberPairs);
    }

    private ZAddResult prepareAddInternal(List<byte[]> scoreMemberPairs) {
        int added = 0;
        boolean changedAny = false;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            byte[] memberBytes = scoreMemberPairs.get(i + 1);

            int outcome;
            if (listpack != null) {
                if (memberBytes != null && memberBytes.length > YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES) {
                    convertToSkipList();
                }
                if (listpack != null) {
                    outcome = listpackZadd(score, memberBytes);
                } else {
                    outcome = skiplistZadd(score, memberBytes);
                }
            } else {
                outcome = skiplistZadd(score, memberBytes);
            }

            if (outcome != 0) {
                changedAny = true;
                if (outcome > 0) {
                    added++;
                }
            }
        }
        return new ZAddResult(added, changedAny);
    }

    public int zrem(List<byte[]> members) {
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
            skiplistLevels -= old.forward.length;
            byScore.delete(old.score, old.member);
            memberStore.release(old.member);
            removed++;
        }
        return removed;
    }

    public int countExistingMembers(List<byte[]> members) {
        Objects.requireNonNull(members, "members");
        int removed = 0;
        for (int index = 0; index < members.size(); index++) {
            byte[] member = members.get(index);
            if (appearedEarlier(members, index, member)) {
                continue;
            }
            boolean exists = listpack != null
                    ? indexOfMember(member) >= 0
                    : byMember.get(member) != null;
            if (exists) {
                removed++;
            }
        }
        return removed;
    }

    public List<byte[]> zrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public List<byte[]> zrevrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrevrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrevrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (listpack != null) {
            int removed = 0;
            for (int i = listpack.size() - 1; i >= 0; i--) {
                double s = listpack.scoreAt(i);
                if (scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                    listpack.removeAtDiscard(i);
                    removed++;
                }
            }
            return removed;
        }

        int removed = 0;
        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        while (node != null && scoreInRange(node.score, min, minExclusive, max, maxExclusive)) {
            ZSkipList.Node next = node.forward[0];
            skiplistLevels -= node.forward.length;
            ZSkipList.Node removedNode = byMember.remove(node.member);
            if (removedNode != node) {
                throw new IllegalStateException("zset member index is missing a score-range node");
            }
            byScore.delete(node.score, node.member);
            memberStore.release(node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    public int zremrangeByRank(long start, long stop) {
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
                listpack.removeAtDiscard((int) i);
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
            skiplistLevels -= node.forward.length;
            ZSkipList.Node removedNode = byMember.remove(node.member);
            if (removedNode != node) {
                throw new IllegalStateException("zset member index is missing a rank-range node");
            }
            byScore.delete(node.score, node.member);
            memberStore.release(node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    public int countRemovalsByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (listpack != null) {
            int removed = 0;
            for (int index = 0; index < listpack.size(); index++) {
                if (scoreInRange(listpack.scoreAt(index), min, minExclusive, max, maxExclusive)) {
                    removed++;
                }
            }
            return removed;
        }

        int removed = 0;
        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        while (node != null && scoreInRange(node.score, min, minExclusive, max, maxExclusive)) {
            removed++;
            node = node.forward[0];
        }
        return removed;
    }

    public int countRemovalsByRank(long start, long stop) {
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
        return Math.toIntExact(normalizedStop - normalizedStart + 1L);
    }

    public List<byte[]> zrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, false);
    }

    public List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, true);
    }

    public int zrangeCount(long start, long stop, boolean withScores) {
        return rangeByIndexCount(start, stop, withScores, false);
    }

    public void zrangeWriteTo(long start, long stop, boolean withScores, BulkStringSink out) {
        rangeByIndexWriteTo(start, stop, withScores, false, out);
    }

    public int zrevrangeCount(long start, long stop, boolean withScores) {
        return rangeByIndexCount(start, stop, withScores, true);
    }

    public void zrevrangeWriteTo(long start, long stop, boolean withScores, BulkStringSink out) {
        rangeByIndexWriteTo(start, stop, withScores, true, out);
    }

    public int zrangeByScoreCount(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return 0;
        }
        if (listpack != null) {
            return zrangeByScoreCountListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrangeByScoreCountSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrangeByScoreWriteTo(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (count <= 0) {
            return;
        }
        if (listpack != null) {
            zrangeByScoreWriteToListpack(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        zrangeByScoreWriteToSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public int zrevrangeByScoreCount(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return 0;
        }
        if (listpack != null) {
            return zrevrangeByScoreCountListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrevrangeByScoreCountSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrevrangeByScoreWriteTo(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (count <= 0) {
            return;
        }
        if (listpack != null) {
            zrevrangeByScoreWriteToListpack(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
            return;
        }
        zrevrangeByScoreWriteToSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
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
                out.add(memberStore.toByteArray(node.member));
                node = stepBackwards ? node.backward : node.forward[0];
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (int i = 0; i < remaining && node != null; i++) {
            out.add(memberStore.toByteArray(node.member));
            out.add(formatScoreBytes(node.score));
            node = stepBackwards ? node.backward : node.forward[0];
        }
        return out;
    }

    private int rangeByIndexCount(long start, long stop, boolean withScores, boolean reverse) {
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

    private void rangeByIndexWriteTo(long start, long stop, boolean withScores, boolean reverse, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (listpack != null) {
            rangeByIndexWriteToListpack(start, stop, withScores, reverse, out);
            return;
        }
        rangeByIndexWriteToSkipList(start, stop, withScores, reverse, out);
    }

    private void rangeByIndexWriteToListpack(long start, long stop, boolean withScores, boolean reverse, BulkStringSink out) {
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

    private void rangeByIndexWriteToSkipList(long start, long stop, boolean withScores, boolean reverse, BulkStringSink out) {
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
                out.bulkString(memberStore.slice(node.member));
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

    private int zrangeByScoreCountListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
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

    private void zrangeByScoreWriteToListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
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

    private int zrevrangeByScoreCountListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
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

    private void zrevrangeByScoreWriteToListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
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

            out.add(memberStore.toByteArray(node.member));
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.forward[0];
        }
        return out;
    }

    private int zrangeByScoreCountSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
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

    private void zrangeByScoreWriteToSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
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
                out.bulkString(memberStore.slice(node.member));
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

            out.add(memberStore.toByteArray(node.member));
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.backward;
        }
        return out;
    }

    private int zrevrangeByScoreCountSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
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

    private void zrevrangeByScoreWriteToSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
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
                out.bulkString(memberStore.slice(node.member));
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

    private static boolean appearedEarlier(List<byte[]> values, int limit, byte[] candidate) {
        for (int index = 0; index < limit; index++) {
            if (Arrays.equals(values.get(index), candidate)) {
                return true;
            }
        }
        return false;
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
            throw new YierdisCommandException("ERR value is not a valid float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisCommandException("ERR value is not a valid float");
        }
        return v;
    }

    private static byte[] formatScoreBytes(double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            return Long.toString((long) score).getBytes(StandardCharsets.US_ASCII);
        }
        return Double.toString(score).getBytes(StandardCharsets.US_ASCII);
    }

    private static void writeScoreTo(BulkStringSink out, double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            out.bulkStringLongAscii((long) score);
            return;
        }
        byte[] encoded = Double.toString(score).getBytes(StandardCharsets.US_ASCII);
        out.bulkString(encoded, 0, encoded.length);
    }

    private int skiplistZadd(double score, byte[] memberBytes) {
        if (byMember == null) {
            byMember = new NativeByteMap<>(
                    memberStore,
                    NativeObjectKind.ZSET_MEMBER_BYTES,
                    hashSeed,
                    maintenanceRegistry,
                    this::notifyHeapChanged
            );
            byScore = new ZSkipList(memberStore);
        }

        ZSkipList.Node old = byMember.get(memberBytes);
        if (old != null) {
            if (Double.compare(old.score, score) == 0) {
                return 0;
            }
        }

        NativeHandle member = memberStore.store(memberBytes);
        boolean linked = false;
        ZSkipList.Node next = null;
        try {
            next = byScore.insert(score, member);
            linked = true;
            skiplistLevels += next.forward.length;
            if (old == null) {
                byMember.put(memberBytes, next);
                return 1;
            }
        } catch (RuntimeException | Error failure) {
            if (linked && next != null) {
                try {
                    if (byScore.delete(score, member)) {
                        skiplistLevels -= next.forward.length;
                    }
                } catch (RuntimeException | Error deleteFailure) {
                    failure.addSuppressed(deleteFailure);
                }
            }
            try {
                memberStore.release(member);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
        ZSkipList.Node replaced = byMember.replace(memberBytes, next);
        if (replaced != old) {
            if (byScore.delete(score, member)) {
                skiplistLevels -= next.forward.length;
            }
            memberStore.release(member);
            throw new IllegalStateException("zset member index changed during score update");
        }
        if (!byScore.delete(old.score, old.member)) {
            byMember.replace(memberBytes, old);
            if (byScore.delete(score, member)) {
                skiplistLevels -= next.forward.length;
            }
            memberStore.release(member);
            throw new IllegalStateException("zset score index missing old member");
        }
        skiplistLevels -= old.forward.length;
        memberStore.release(old.member);
        return -1;
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
            return -1;
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
        NativeByteMap<ZSkipList.Node> outByMember = new NativeByteMap<>(
                memberStore,
                NativeObjectKind.ZSET_MEMBER_BYTES,
                hashSeed,
                maintenanceRegistry,
                this::notifyHeapChanged
        );
        ZSkipList outByScore = new ZSkipList(memberStore);
        skiplistLevels = 0;
        for (int i = 0; i < size; i++) {
            double score = listpack.scoreAt(i);
            byte[] member = listpack.memberAt(i);
            NativeHandle memberHandle = memberStore.store(member);
            ZSkipList.Node n = outByScore.insert(score, memberHandle);
            outByMember.put(member, n);
            skiplistLevels += n.forward.length;
        }
        this.byMember = outByMember;
        this.byScore = outByScore;
        this.listpack.close();
        this.listpack = null;
        notifyHeapChanged();
    }

    @Override
    public void close() {
        if (byMember != null) {
            byMember.forEach((memberRef, node) -> memberStore.release(node.member));
            byMember.close();
            byMember = null;
            byScore = null;
        }
        if (listpack != null) {
            listpack.close();
            listpack = null;
        }
    }

    private static final class NativePackedZSet implements AutoCloseable {
        private static final long FIXED_HEAP_BYTES = 56L;
        private static final long ARRAY_HEADER_BYTES = 16L;
        private final NativeListpack members;
        private double[] scores = new double[0];
        private int size;

        private NativePackedZSet(NativeByteStore memberStore) {
            this.members = new NativeListpack(memberStore, NativeObjectKind.ZSET_MEMBER_BYTES);
        }

        int size() {
            return size;
        }

        long estimatedBytes() {
            return members.estimatedBytes() + (long) scores.length * Double.BYTES;
        }

        long heapEstimatedBytes() {
            return FIXED_HEAP_BYTES
                    + members.heapEstimatedBytes()
                    + ARRAY_HEADER_BYTES + (long) scores.length * Double.BYTES;
        }

        double scoreAt(int index) {
            checkIndex(index);
            return scores[index];
        }

        byte[] memberAt(int index) {
            checkIndex(index);
            return members.get(index);
        }

        void memberWriteTo(int index, BulkStringSink out) {
            checkIndex(index);
            members.writeAt(index, out);
        }

        void forEachNativeHandle(Consumer<NativeHandle> consumer) {
            members.forEachNativeHandle(consumer);
        }

        int indexOfMember(byte[] memberBytes) {
            return members.indexOf(memberBytes);
        }

        int compareMemberAt(int index, byte[] other) {
            checkIndex(index);
            byte[] member = members.get(index);
            return compareLex(member, other);
        }

        void insertAt(int index, double score, byte[] memberBytes) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            }
            ensureCapacity(size + 1);
            if (size - index > 0) {
                System.arraycopy(scores, index, scores, index + 1, size - index);
            }
            scores[index] = score;
            members.insertAt(index, memberBytes, NativeObjectKind.ZSET_MEMBER_BYTES);
            size++;
        }

        void removeAt(int index) {
            checkIndex(index);
            members.removeAt(index);
            if (size - index - 1 > 0) {
                System.arraycopy(scores, index + 1, scores, index, size - index - 1);
            }
            size--;
        }

        void removeAtDiscard(int index) {
            checkIndex(index);
            members.removeAtDiscard(index);
            if (size - index - 1 > 0) {
                System.arraycopy(scores, index + 1, scores, index, size - index - 1);
            }
            size--;
        }

        @Override
        public void close() {
            members.close();
            scores = new double[0];
            size = 0;
        }

        private void ensureCapacity(int desired) {
            if (scores.length >= desired) {
                return;
            }
            int next = Math.max(16, scores.length);
            while (next < desired) {
                next <<= 1;
            }
            scores = Arrays.copyOf(scores, next);
        }

        private void checkIndex(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    private void notifyHeapChanged() {
        heapChangeListener.run();
    }

    private static long heapUpperBoundForMemberCount(long memberCount) {
        return addSaturating(
                FIXED_HEAP_BYTES,
                addSaturating(
                        NativeByteMap.heapUpperBoundForEntries(memberCount),
                        ZSkipList.heapUpperBoundForNodes(memberCount)
                )
        );
    }

    private static long addSaturating(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
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

        void memberWriteTo(int index, BulkStringSink out) {
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
