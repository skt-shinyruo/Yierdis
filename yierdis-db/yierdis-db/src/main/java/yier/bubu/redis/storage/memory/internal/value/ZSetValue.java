package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.MaterializedCollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisGlobMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.nio.charset.StandardCharsets;

public final class ZSetValue implements YierdisValue {
    public record ZAddResult(int added, boolean changedAny) {
    }

    private static final int REF_BYTES = 8;
    private static final long FIXED_HEAP_BYTES = 96L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long PREPARED_EXISTING_ADD_HEAP_BYTES = 256L;
    private static final long STAGED_SKIPLIST_HEAP_BYTES = 128L;
    private static final long NATIVE_BYTE_MAP_HEAP_BYTES = 256L;
    private static final long ARRAY_LIST_HEAP_BYTES = 32L;
    private static final long STAGED_PUT_HEAP_BYTES = 64L;

    private final NativeByteStore memberStore;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;

    // 小 ZSET 使用 packed 编码；超过阈值后由 NativeByteMap 负责 member 定位，skiplist 负责 score 顺序。
    private NativePackedZSet listpack;
    private NativeByteMap<ZSkipList.Node> byMember;
    private ZSkipList byScore;
    private long skiplistLevels;
    private Runnable heapChangeListener = () -> {
    };

    public ZSetValue(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        StableMemoryBackend stableMemoryBackend = Objects.requireNonNull(allocator, "allocator");
        this.memberStore = new NativeByteStore(stableMemoryBackend, NativeObjectKind.ZSET_MEMBER_BYTES);
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

    public static PackedBuildPlan preparedNewPackedBuildPlan(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
        return packedBuildPlan(null, scoreMemberPairs);
    }

    public PackedBuildPlan preparedPackedBuildPlan(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
        return listpack == null ? null : packedBuildPlan(listpack, scoreMemberPairs);
    }

    public void reservePackedForBuild(PackedBuildPlan plan) {
        Objects.requireNonNull(plan, "plan");
        if (listpack == null || listpack.size() != 0) {
            throw new IllegalStateException("packed zset build reservation requires an empty value");
        }
        listpack.reserveForBuild(plan.memberCount(), plan.encodedBytes());
    }

    public void prepareSkiplistForBuild() {
        if (listpack == null || listpack.size() != 0) {
            throw new IllegalStateException("skiplist build preparation requires an empty value");
        }
        convertToSkipList();
    }

    public HashTableMetrics memberTableMetrics() {
        return byMember == null ? null : byMember.metrics();
    }

    public boolean usesSkiplistEncoding() {
        return byMember != null;
    }

    public ZAddResult previewAdd(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
        int added = 0;
        boolean changedAny = false;
        for (int pairIndex = 0; pairIndex < scoreMemberPairs.size(); pairIndex += 2) {
            double score = parseScore(scoreMemberPairs.get(pairIndex));
            byte[] member = scoreMemberPairs.get(pairIndex + 1);
            boolean present = false;
            double previousScore = 0.0d;
            for (int previousIndex = pairIndex - 2; previousIndex >= 0; previousIndex -= 2) {
                if (Arrays.equals(scoreMemberPairs.get(previousIndex + 1), member)) {
                    previousScore = parseScore(scoreMemberPairs.get(previousIndex));
                    present = true;
                    break;
                }
            }
            if (!present && listpack != null) {
                int memberIndex = listpack.indexOfMember(member);
                if (memberIndex >= 0) {
                    previousScore = listpack.scoreAt(memberIndex);
                    present = true;
                }
            } else if (!present && byMember != null) {
                ZSkipList.Node node = byMember.get(member);
                if (node != null) {
                    previousScore = node.score;
                    present = true;
                }
            }

            if (!present) {
                added++;
                changedAny = true;
            } else if (!scoresEqual(previousScore, score)) {
                changedAny = true;
            }
        }
        return new ZAddResult(added, changedAny);
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

    public ZAddResult add(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
        if (size() != 0) {
            try (PreparedExistingAdd prepared = prepareExistingAdd(planExistingAdd(scoreMemberPairs))) {
                ZAddResult result = prepared.result();
                if (!prepared.changedAny()) {
                    return result;
                }
                prepared.commit();
                prepared.releaseSuperseded();
                return result;
            }
        }
        return addDirectly(scoreMemberPairs);
    }

    public ZAddPlan planExistingAdd(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
        ArrayList<ScoreMemberInput> canonical = canonicalScoreMemberInputs(scoreMemberPairs);
        ZAddPlanEntry[] entries = new ZAddPlanEntry[canonical.size()];
        int added = 0;
        int changed = 0;
        for (int index = 0; index < canonical.size(); index++) {
            ScoreMemberInput input = canonical.get(index);
            ZSkipList.Node previousNode = null;
            boolean present;
            double previousScore;
            if (listpack != null) {
                int memberIndex = listpack.indexOfMember(input.member);
                present = memberIndex >= 0;
                previousScore = present ? listpack.scoreAt(memberIndex) : 0.0d;
            } else {
                previousNode = byMember.get(input.member);
                present = previousNode != null;
                previousScore = present ? previousNode.score : 0.0d;
            }
            boolean scoreChanged = !present || !scoresEqual(previousScore, input.score);
            entries[index] = new ZAddPlanEntry(
                    input.member,
                    input.score,
                    present,
                    previousScore,
                    previousNode,
                    scoreChanged
            );
            if (!present) {
                added++;
            }
            if (scoreChanged) {
                changed++;
            }
        }

        if (changed == 0) {
            return new ZAddPlan(
                    this,
                    listpack,
                    byMember,
                    byScore,
                    entries,
                    listpack == null ? ZAddPath.SKIPLIST_DELTA : ZAddPath.PACKED_REPLACEMENT,
                    List.of(),
                    0,
                    0,
                    new int[0],
                    0L
            );
        }

        ZAddPath path;
        List<FinalMember> finalMembers = null;
        if (listpack == null) {
            path = ZAddPath.SKIPLIST_DELTA;
        } else {
            int finalSize = listpack.size() + added;
            boolean packed = finalSize <= YierdisEncodingThresholds.ZSET_MAX_LISTPACK_ENTRIES;
            for (ZAddPlanEntry entry : entries) {
                if (entry.member.length > YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES) {
                    packed = false;
                    break;
                }
            }
            finalMembers = finalMembers(entries);
            path = packed ? ZAddPath.PACKED_REPLACEMENT : ZAddPath.PACKED_TO_SKIPLIST;
        }

        int[] allocationSizes = allocationSizes(path, entries, finalMembers);
        long stagedHeapBytes = stagedHeapUpperBound(path, changed, added, finalMembers);
        return new ZAddPlan(
                this,
                listpack,
                byMember,
                byScore,
                entries,
                path,
                finalMembers,
                added,
                changed,
                allocationSizes,
                stagedHeapBytes
        );
    }

    public PreparedExistingAdd prepareExistingAdd(ZAddPlan plan) {
        Objects.requireNonNull(plan, "plan");
        plan.validateFor(this);
        if (!plan.changedAny()) {
            return PreparedExistingAdd.noop(this, plan);
        }
        return switch (plan.path) {
            case PACKED_REPLACEMENT -> preparePackedReplacement(plan);
            case PACKED_TO_SKIPLIST -> preparePackedToSkiplist(plan);
            case SKIPLIST_DELTA -> prepareSkiplistDelta(plan);
        };
    }

    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (listpack != null) {
            listpack.forEachNativeHandle(consumer);
            return;
        }
        if (byMember != null) {
            byMember.forEach((ignored, node) -> consumer.accept(node.member));
        }
    }

    public int zaddMany(List<byte[]> scoreMemberPairs) {
        return add(scoreMemberPairs).added();
    }

    public ZAddResult zaddManyResult(List<byte[]> scoreMemberPairs) {
        return add(scoreMemberPairs);
    }

    private ZAddResult addDirectly(List<byte[]> scoreMemberPairs) {
        validateScoreMemberPairs(scoreMemberPairs);
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

    public CollectionScanWindow zscan(ScanCursorV2 cursor, byte[] globPattern, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        ScanCursorV2 current = cursor == null ? ScanCursorV2.start() : cursor;
        if (byMember != null) {
            int boundedCount = NativeCollectionScanWindow.boundedMatchCount(count);
            int[] matched = {0};
            try (NativeCollectionScanWindow.Builder builder =
                         NativeCollectionScanWindow.builder(memberStore.backend(), boundedCount * 2)) {
                NativeByteMap.ScanResult result = byMember.scanWithWork(
                        current,
                        NativeCollectionScanWindow.slotBudget(boundedCount),
                        (memberRef, node) -> {
                            var memberSlice = memberStore.slice(memberRef);
                            if (globPattern != null && !YierdisGlobMatcher.matches(globPattern, memberSlice)) {
                                return true;
                            }
                            builder.addNative(memberRef, memberSlice.length());
                            addScoreElement(builder, node.score);
                            matched[0]++;
                            return matched[0] < boundedCount;
                        }
                );
                return builder.build(result.nextCursor());
            }
        }

        // packed ZSET 按 score 排序；一次返回完整编码，避免改分重排让稳定 member 落到旧游标之前。
        if (current.value() != 0L) {
            return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
        }
        int packedSize = listpack.size();
        List<byte[]> out = new ArrayList<>(packedSize * 2);
        for (int index = 0; index < packedSize; index++) {
            byte[] member = listpack.memberAt(index);
            if (globPattern != null && !YierdisGlobMatcher.matches(globPattern, member)) {
                continue;
            }
            out.add(member);
            out.add(formatScoreBytes(listpack.scoreAt(index)));
        }
        return new MaterializedCollectionScanWindow(ScanCursorV2.start(), out);
    }

    public List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, true);
    }

    public int zrangeCount(long start, long stop, boolean withScores) {
        return rangeByIndexCount(start, stop, withScores, false);
    }

    public void zrangeWriteTo(long start, long stop, boolean withScores, ByteValueSink out) {
        rangeByIndexWriteTo(start, stop, withScores, false, out);
    }

    public int zrevrangeCount(long start, long stop, boolean withScores) {
        return rangeByIndexCount(start, stop, withScores, true);
    }

    public void zrevrangeWriteTo(long start, long stop, boolean withScores, ByteValueSink out) {
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

    public void zrangeByScoreWriteTo(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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

    public void zrevrangeByScoreWriteTo(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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

    private void rangeByIndexWriteTo(long start, long stop, boolean withScores, boolean reverse, ByteValueSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        if (listpack != null) {
            rangeByIndexWriteToListpack(start, stop, withScores, reverse, out);
            return;
        }
        rangeByIndexWriteToSkipList(start, stop, withScores, reverse, out);
    }

    private void rangeByIndexWriteToListpack(long start, long stop, boolean withScores, boolean reverse, ByteValueSink out) {
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

    private void rangeByIndexWriteToSkipList(long start, long stop, boolean withScores, boolean reverse, ByteValueSink out) {
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
                out.nullValue();
            } else {
                out.value(memberStore.slice(node.member));
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
                if (score > max || (maxExclusive && compareScores(score, max) == 0)) {
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
                if (score > max || (maxExclusive && compareScores(score, max) == 0)) {
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

    private void zrangeByScoreWriteToListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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
                if (score > max || (maxExclusive && compareScores(score, max) == 0)) {
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

    private void zrevrangeByScoreWriteToListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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
            if (compareScores(s, min) > 0) {
                return i;
            }
            if (!exclusive && compareScores(s, min) == 0) {
                return i;
            }
        }
        return size;
    }

    private int lastIndexForMax(double max, boolean exclusive) {
        int size = listpack.size();
        for (int i = size - 1; i >= 0; i--) {
            double s = listpack.scoreAt(i);
            if (compareScores(s, max) < 0) {
                return i;
            }
            if (!exclusive && compareScores(s, max) == 0) {
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

    private void zrangeByScoreWriteToSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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
                out.nullValue();
            } else {
                out.value(memberStore.slice(node.member));
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

    private void zrevrangeByScoreWriteToSkipList(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, ByteValueSink out) {
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
                out.nullValue();
            } else {
                out.value(memberStore.slice(node.member));
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
        if (compareScores(s, min) < 0) {
            return false;
        }
        return !minExclusive || compareScores(s, min) != 0;
    }

    private static boolean scoreInRange(double s, double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (Double.isNaN(s)) {
            return false;
        }
        if (compareScores(s, min) < 0) {
            return false;
        }
        if (minExclusive && compareScores(s, min) == 0) {
            return false;
        }
        if (compareScores(s, max) > 0) {
            return false;
        }
        if (maxExclusive && compareScores(s, max) == 0) {
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

    private static PackedBuildPlan packedBuildPlan(
            NativePackedZSet current,
            List<byte[]> scoreMemberPairs
    ) {
        int memberCount = current == null ? 0 : current.size();
        int encodedBytes = current == null ? 0 : current.encodedBytes();
        for (int memberIndex = 1; memberIndex < scoreMemberPairs.size(); memberIndex += 2) {
            byte[] member = scoreMemberPairs.get(memberIndex);
            if (member != null && member.length > YierdisEncodingThresholds.ZSET_MAX_LISTPACK_VALUE_BYTES) {
                return null;
            }
            boolean alreadyPresent = current != null && current.indexOfMember(member) >= 0;
            if (alreadyPresent || memberAppearedEarlier(scoreMemberPairs, memberIndex, member)) {
                continue;
            }
            memberCount = Math.addExact(memberCount, 1);
            if (memberCount > YierdisEncodingThresholds.ZSET_MAX_LISTPACK_ENTRIES) {
                return null;
            }
            encodedBytes = Math.addExact(encodedBytes, NativeListpack.entryEncodedBytes(member));
        }
        return new PackedBuildPlan(memberCount, encodedBytes);
    }

    private static boolean memberAppearedEarlier(
            List<byte[]> scoreMemberPairs,
            int memberIndex,
            byte[] candidate
    ) {
        for (int index = 1; index < memberIndex; index += 2) {
            if (Arrays.equals(scoreMemberPairs.get(index), candidate)) {
                return true;
            }
        }
        return false;
    }

    private static void validateScoreMemberPairs(List<byte[]> scoreMemberPairs) {
        Objects.requireNonNull(scoreMemberPairs, "scoreMemberPairs");
        if ((scoreMemberPairs.size() & 1) != 0) {
            throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
        }
        for (int index = 0; index < scoreMemberPairs.size(); index += 2) {
            parseScore(Objects.requireNonNull(scoreMemberPairs.get(index), "zset score"));
            Objects.requireNonNull(scoreMemberPairs.get(index + 1), "zset member");
        }
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
        return v == 0.0d ? 0.0d : v;
    }

    private static boolean scoresEqual(double left, double right) {
        return left == right;
    }

    private static int compareScores(double left, double right) {
        return left < right ? -1 : left > right ? 1 : 0;
    }

    private static byte[] formatScoreBytes(double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            return Long.toString((long) score).getBytes(StandardCharsets.US_ASCII);
        }
        return Double.toString(score).getBytes(StandardCharsets.US_ASCII);
    }

    private static void addScoreElement(NativeCollectionScanWindow.Builder builder, double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            builder.addLong((long) score);
            return;
        }
        builder.addBytes(Double.toString(score).getBytes(StandardCharsets.US_ASCII));
    }

    private static void writeScoreTo(ByteValueSink out, double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            out.longAscii((long) score);
            return;
        }
        byte[] encoded = Double.toString(score).getBytes(StandardCharsets.US_ASCII);
        out.value(encoded, 0, encoded.length);
    }

    private ArrayList<ScoreMemberInput> canonicalScoreMemberInputs(List<byte[]> scoreMemberPairs) {
        ArrayList<ScoreMemberInput> canonical = new ArrayList<>(scoreMemberPairs.size() / 2);
        ScoreMemberIndex index = new ScoreMemberIndex(
                canonical,
                scoreMemberPairs.size() / 2,
                hashSeed
        );
        for (int pairIndex = 0; pairIndex < scoreMemberPairs.size(); pairIndex += 2) {
            double score = parseScore(scoreMemberPairs.get(pairIndex));
            byte[] member = scoreMemberPairs.get(pairIndex + 1);
            int existingIndex = index.find(member);
            if (existingIndex >= 0) {
                canonical.set(existingIndex, new ScoreMemberInput(member, score));
                continue;
            }
            canonical.add(new ScoreMemberInput(member, score));
            index.add(canonical.size() - 1);
        }
        return canonical;
    }

    private List<FinalMember> finalMembers(ZAddPlanEntry[] entries) {
        ArrayList<FinalMember> members = new ArrayList<>(listpack.size() + entries.length);
        for (int index = 0; index < listpack.size(); index++) {
            members.add(new FinalMember(listpack.memberAt(index), listpack.scoreAt(index)));
        }
        for (ZAddPlanEntry entry : entries) {
            int existingIndex = -1;
            for (int index = 0; index < members.size(); index++) {
                if (Arrays.equals(members.get(index).member, entry.member)) {
                    existingIndex = index;
                    break;
                }
            }
            FinalMember replacement = new FinalMember(entry.member, entry.score);
            if (existingIndex >= 0) {
                members.set(existingIndex, replacement);
            } else {
                members.add(replacement);
            }
        }
        members.sort((left, right) -> {
            if (left.score < right.score) {
                return -1;
            }
            if (left.score > right.score) {
                return 1;
            }
            return compareLex(left.member, right.member);
        });
        return members;
    }

    private static int[] allocationSizes(
            ZAddPath path,
            ZAddPlanEntry[] entries,
            List<FinalMember> finalMembers
    ) {
        if (path == ZAddPath.PACKED_REPLACEMENT) {
            int encodedBytes = 0;
            for (FinalMember member : finalMembers) {
                encodedBytes = Math.addExact(encodedBytes, NativeListpack.entryEncodedBytes(member.member));
            }
            return encodedBytes == 0 ? new int[0] : new int[]{encodedBytes};
        }
        if (path == ZAddPath.PACKED_TO_SKIPLIST) {
            int[] sizes = new int[finalMembers.size()];
            for (int index = 0; index < finalMembers.size(); index++) {
                sizes[index] = Math.max(1, finalMembers.get(index).member.length);
            }
            return sizes;
        }
        int added = 0;
        for (ZAddPlanEntry entry : entries) {
            if (entry.changed && !entry.present) {
                added++;
            }
        }
        int[] sizes = new int[added];
        int next = 0;
        for (ZAddPlanEntry entry : entries) {
            if (entry.changed && !entry.present) {
                sizes[next++] = Math.max(1, entry.member.length);
            }
        }
        return sizes;
    }

    private long stagedHeapUpperBound(
            ZAddPath path,
            int changed,
            int added,
            List<FinalMember> finalMembers
    ) {
        if (changed == 0) {
            return 0L;
        }
        if (path == ZAddPath.PACKED_REPLACEMENT) {
            return addSaturating(
                    PREPARED_EXISTING_ADD_HEAP_BYTES,
                    NativePackedZSet.heapUpperBoundForEntries(finalMembers.size())
            );
        }
        if (path == ZAddPath.PACKED_TO_SKIPLIST) {
            long memberCount = finalMembers.size();
            long bytes = PREPARED_EXISTING_ADD_HEAP_BYTES + STAGED_SKIPLIST_HEAP_BYTES;
            bytes = addSaturating(bytes, NATIVE_BYTE_MAP_HEAP_BYTES);
            bytes = addSaturating(bytes, referenceArrayHeapBytes(memberCount));
            bytes = addSaturating(bytes, NativeByteMap.heapUpperBoundForEntries(memberCount));
            bytes = addSaturating(bytes, ZSkipList.heapUpperBoundForNodes(memberCount));
            return addSaturating(
                    bytes,
                    ZSkipList.preparedInsertWorkspaceHeapUpperBound(memberCount)
            );
        }
        long changedCount = changed;
        long bytes = PREPARED_EXISTING_ADD_HEAP_BYTES;
        bytes = addSaturating(bytes, multiplySaturating(3L, referenceArrayHeapBytes(changedCount)));
        bytes = addSaturating(bytes, ARRAY_LIST_HEAP_BYTES);
        bytes = addSaturating(bytes, referenceArrayHeapBytes(changedCount));
        bytes = addSaturating(bytes, multiplySaturating(changedCount, STAGED_PUT_HEAP_BYTES));
        bytes = addSaturating(bytes, byMember.estimatedPreparedPutHeapGrowthBytes(changed, added));
        return addSaturating(
                bytes,
                ZSkipList.preparedMutationHeapUpperBound(changedCount, changedCount - added)
        );
    }

    private PreparedExistingAdd preparePackedReplacement(ZAddPlan plan) {
        NativePackedZSet replacement = new NativePackedZSet(memberStore);
        boolean prepared = false;
        try {
            int encodedBytes = plan.nativeAllocationSizes.length == 0
                    ? 0
                    : plan.nativeAllocationSizes[0];
            replacement.reserveForBuild(plan.finalMembers.size(), encodedBytes);
            for (FinalMember member : plan.finalMembers) {
                replacement.insertAt(replacement.size(), member.score, member.member);
            }
            prepared = true;
            return PreparedExistingAdd.packed(this, plan, replacement);
        } finally {
            if (!prepared) {
                replacement.close();
            }
        }
    }

    private PreparedExistingAdd preparePackedToSkiplist(ZAddPlan plan) {
        StagedSkiplist replacement = buildStagedSkiplist(plan.finalMembers);
        return PreparedExistingAdd.converted(this, plan, replacement);
    }

    private PreparedExistingAdd prepareSkiplistDelta(ZAddPlan plan) {
        int changedCount = plan.changedCount;
        ZSkipList.PreparedInsert[] inserts = new ZSkipList.PreparedInsert[changedCount];
        ZSkipList.PreparedDelete[] deletes = new ZSkipList.PreparedDelete[changedCount];
        NativeHandle[] newMembers = new NativeHandle[changedCount];
        ArrayList<NativeByteMap.StagedPut<ZSkipList.Node>> puts = new ArrayList<>(changedCount);
        NativeByteMap.PreparedMutation<ZSkipList.Node> preparedMap = null;
        int next = 0;
        try {
            for (ZAddPlanEntry entry : plan.entries) {
                if (!entry.changed) {
                    continue;
                }
                NativeHandle member = entry.present
                        ? entry.previousNode.member
                        : memberStore.store(entry.member);
                if (!entry.present) {
                    newMembers[next] = member;
                }
                ZSkipList.PreparedInsert insert = byScore.prepareInsert(entry.score, member);
                inserts[next] = insert;
                deletes[next] = entry.present ? byScore.prepareDelete(entry.previousNode) : null;
                puts.add(NativeByteMap.StagedPut.borrowed(
                        entry.member,
                        member,
                        insert.node(),
                        entry.present
                ));
                next++;
            }
            preparedMap = byMember.preparePuts(puts);
            if (preparedMap.addedCount() != plan.added) {
                throw new IllegalStateException("prepared zset member-map added count changed");
            }
            return PreparedExistingAdd.skiplist(
                    this,
                    plan,
                    preparedMap,
                    inserts,
                    deletes,
                    newMembers
            );
        } catch (RuntimeException | Error failure) {
            if (preparedMap != null) {
                try {
                    preparedMap.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            releaseMemberHandles(newMembers, failure);
            throw failure;
        }
    }

    private StagedSkiplist buildStagedSkiplist(List<FinalMember> members) {
        NativeByteMap<ZSkipList.Node> stagedByMember = NativeByteMap.borrowedKeys(
                memberStore,
                NativeObjectKind.ZSET_MEMBER_BYTES,
                hashSeed,
                maintenanceRegistry,
                this::notifyHeapChanged
        );
        ZSkipList stagedByScore = new ZSkipList(memberStore);
        NativeHandle[] ownedMembers = new NativeHandle[members.size()];
        long levels = 0L;
        try {
            for (int index = 0; index < members.size(); index++) {
                FinalMember member = members.get(index);
                NativeHandle handle = memberStore.store(member.member);
                ownedMembers[index] = handle;
                ZSkipList.Node node = stagedByScore.insert(member.score, handle);
                if (stagedByMember.putBorrowed(handle, node) != null) {
                    throw new IllegalStateException("staged zset contains duplicate members");
                }
                levels += node.forward.length;
            }
            return new StagedSkiplist(stagedByMember, stagedByScore, ownedMembers, levels);
        } catch (RuntimeException | Error failure) {
            releaseMemberHandles(ownedMembers, failure);
            try {
                stagedByMember.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void releaseMemberHandles(NativeHandle[] handles, Throwable failure) {
        for (int index = 0; index < handles.length; index++) {
            NativeHandle handle = handles[index];
            if (handle == null) {
                continue;
            }
            try {
                memberStore.release(handle);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            } finally {
                handles[index] = null;
            }
        }
    }

    private int skiplistZadd(double score, byte[] memberBytes) {
        if (byMember == null) {
            byMember = NativeByteMap.borrowedKeys(
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
            if (scoresEqual(old.score, score)) {
                return 0;
            }
        }

        NativeHandle member = old == null ? memberStore.store(memberBytes) : old.member;
        boolean releaseMemberOnFailure = old == null;
        boolean linked = false;
        ZSkipList.Node next = null;
        try {
            next = byScore.insert(score, member);
            linked = true;
            skiplistLevels += next.forward.length;
            if (old == null) {
                byMember.putBorrowed(member, next);
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
            if (releaseMemberOnFailure) {
                try {
                    memberStore.release(member);
                } catch (RuntimeException | Error releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
        ZSkipList.Node replaced = byMember.replace(memberBytes, next);
        if (replaced != old) {
            if (byScore.delete(score, member)) {
                skiplistLevels -= next.forward.length;
            }
            throw new IllegalStateException("zset member index changed during score update");
        }
        if (!byScore.delete(old.score, old.member)) {
            byMember.replace(memberBytes, old);
            if (byScore.delete(score, member)) {
                skiplistLevels -= next.forward.length;
            }
            throw new IllegalStateException("zset score index missing old member");
        }
        skiplistLevels -= old.forward.length;
        return -1;
    }

    private int listpackZadd(double score, byte[] memberBytes) {
        int idx = indexOfMember(memberBytes);
        if (idx >= 0) {
            double oldScore = listpack.scoreAt(idx);
            if (scoresEqual(oldScore, score)) {
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
            int cmp = compareScores(nodeScore, score);
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
        NativeByteMap<ZSkipList.Node> outByMember = NativeByteMap.borrowedKeys(
                memberStore,
                NativeObjectKind.ZSET_MEMBER_BYTES,
                hashSeed,
                maintenanceRegistry,
                this::notifyHeapChanged
        );
        ZSkipList outByScore = new ZSkipList(memberStore);
        long outSkiplistLevels = 0L;
        try {
            for (int i = 0; i < size; i++) {
                double score = listpack.scoreAt(i);
                byte[] member = listpack.memberAt(i);
                NativeHandle memberHandle = memberStore.store(member);
                ZSkipList.Node node = null;
                boolean indexed = false;
                try {
                    node = outByScore.insert(score, memberHandle);
                    ZSkipList.Node previous = outByMember.putBorrowed(memberHandle, node);
                    if (previous != null) {
                        throw new IllegalStateException("packed zset contains duplicate members");
                    }
                    indexed = true;
                    outSkiplistLevels += node.forward.length;
                } finally {
                    if (!indexed) {
                        if (node != null) {
                            outByScore.delete(score, memberHandle);
                        }
                        memberStore.release(memberHandle);
                    }
                }
            }
        } catch (RuntimeException | Error failure) {
            closeFailedSkiplistBuild(outByMember, failure);
            throw failure;
        }

        NativePackedZSet previous = listpack;
        try {
            previous.close();
        } catch (RuntimeException | Error failure) {
            closeFailedSkiplistBuild(outByMember, failure);
            throw failure;
        }
        this.byMember = outByMember;
        this.byScore = outByScore;
        this.skiplistLevels = outSkiplistLevels;
        this.listpack = null;
        notifyHeapChanged();
    }

    private void closeFailedSkiplistBuild(
            NativeByteMap<ZSkipList.Node> stagedByMember,
            Throwable failure
    ) {
        stagedByMember.forEach((ignored, node) -> {
            try {
                memberStore.release(node.member);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        });
        try {
            stagedByMember.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
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
        private long generation;

        private NativePackedZSet(NativeByteStore memberStore) {
            this.members = new NativeListpack(memberStore, NativeObjectKind.ZSET_MEMBER_BYTES);
        }

        int size() {
            return size;
        }

        long generation() {
            return generation;
        }

        long estimatedBytes() {
            return members.estimatedBytes() + (long) scores.length * Double.BYTES;
        }

        int encodedBytes() {
            return members.encodedBytes();
        }

        void reserveForBuild(int finalMemberCount, int finalEncodedBytes) {
            if (size != 0 || !members.isEmpty()) {
                throw new IllegalStateException("packed zset build reservation requires an empty value");
            }
            ensureCapacity(finalMemberCount);
            if (finalMemberCount > 0) {
                members.reserveForBuild(finalMemberCount, finalEncodedBytes);
            }
        }

        long heapEstimatedBytes() {
            return FIXED_HEAP_BYTES
                    + members.heapEstimatedBytes()
                    + ARRAY_HEADER_BYTES + (long) scores.length * Double.BYTES;
        }

        static long heapUpperBoundForEntries(long entryCount) {
            if (entryCount < 0L || entryCount > Integer.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long scoreCapacity = 0L;
            if (entryCount > 0L) {
                scoreCapacity = 16L;
                while (scoreCapacity < entryCount) {
                    scoreCapacity <<= 1;
                }
            }
            long bytes = addSaturating(
                    FIXED_HEAP_BYTES,
                    NativeListpack.heapUpperBoundForEntries(entryCount)
            );
            return addSaturating(
                    bytes,
                    addSaturating(ARRAY_HEADER_BYTES, multiplySaturating(scoreCapacity, Double.BYTES))
            );
        }

        double scoreAt(int index) {
            checkIndex(index);
            return scores[index];
        }

        byte[] memberAt(int index) {
            checkIndex(index);
            return members.get(index);
        }

        void memberWriteTo(int index, ByteValueSink out) {
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
            // native 插入仍可能因扩容失败；先完成它，避免把可见 score 数组移成半提交状态。
            members.insertAt(index, memberBytes, NativeObjectKind.ZSET_MEMBER_BYTES);
            if (size - index > 0) {
                System.arraycopy(scores, index, scores, index + 1, size - index);
            }
            scores[index] = score;
            size++;
            generation++;
        }

        void removeAt(int index) {
            checkIndex(index);
            members.removeAt(index);
            if (size - index - 1 > 0) {
                System.arraycopy(scores, index + 1, scores, index, size - index - 1);
            }
            size--;
            generation++;
        }

        void removeAtDiscard(int index) {
            checkIndex(index);
            members.removeAtDiscard(index);
            if (size - index - 1 > 0) {
                System.arraycopy(scores, index + 1, scores, index, size - index - 1);
            }
            size--;
            generation++;
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

    private static long multiplySaturating(long left, long right) {
        if (left < 0L || right < 0L) {
            return Long.MAX_VALUE;
        }
        return left == 0L || right == 0L || left <= Long.MAX_VALUE / right
                ? left * right
                : Long.MAX_VALUE;
    }

    private static long referenceArrayHeapBytes(long length) {
        return addSaturating(ARRAY_HEADER_BYTES, multiplySaturating(length, REF_BYTES));
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException(failure);
    }

    public static final class ZAddPlan {
        private final ZSetValue owner;
        private final NativePackedZSet sourcePacked;
        private final long sourcePackedGeneration;
        private final NativeByteMap<ZSkipList.Node> sourceByMember;
        private final ZSkipList sourceByScore;
        private final ZAddPlanEntry[] entries;
        private final ZAddPath path;
        private final List<FinalMember> finalMembers;
        private final int added;
        private final int changedCount;
        private final int[] nativeAllocationSizes;
        private final long stagedHeapBytes;

        private ZAddPlan(
                ZSetValue owner,
                NativePackedZSet sourcePacked,
                NativeByteMap<ZSkipList.Node> sourceByMember,
                ZSkipList sourceByScore,
                ZAddPlanEntry[] entries,
                ZAddPath path,
                List<FinalMember> finalMembers,
                int added,
                int changedCount,
                int[] nativeAllocationSizes,
                long stagedHeapBytes
        ) {
            this.owner = owner;
            this.sourcePacked = sourcePacked;
            this.sourcePackedGeneration = sourcePacked == null ? 0L : sourcePacked.generation();
            this.sourceByMember = sourceByMember;
            this.sourceByScore = sourceByScore;
            this.entries = entries;
            this.path = path;
            this.finalMembers = finalMembers;
            this.added = added;
            this.changedCount = changedCount;
            this.nativeAllocationSizes = nativeAllocationSizes;
            this.stagedHeapBytes = stagedHeapBytes;
        }

        public int added() {
            return added;
        }

        public boolean changedAny() {
            return changedCount != 0;
        }

        public int[] nativeAllocationSizes() {
            return nativeAllocationSizes.clone();
        }

        public long stagedHeapBytes() {
            return stagedHeapBytes;
        }

        private void validateFor(ZSetValue expectedOwner) {
            if (owner != expectedOwner
                    || owner.listpack != sourcePacked
                    || owner.byMember != sourceByMember
                    || owner.byScore != sourceByScore
                    || (sourcePacked != null && sourcePacked.generation() != sourcePackedGeneration)) {
                throw new IllegalStateException("prepared ZADD source representation changed");
            }
            for (ZAddPlanEntry entry : entries) {
                if (sourcePacked != null) {
                    int index = sourcePacked.indexOfMember(entry.member);
                    boolean present = index >= 0;
                    if (present != entry.present
                            || (present && !scoresEqual(sourcePacked.scoreAt(index), entry.previousScore))) {
                        throw new IllegalStateException("prepared packed ZADD source entry changed");
                    }
                } else {
                    ZSkipList.Node current = sourceByMember.get(entry.member);
                    if ((current != null) != entry.present || current != entry.previousNode) {
                        throw new IllegalStateException("prepared skiplist ZADD source entry changed");
                    }
                }
            }
        }
    }

    public static final class PreparedExistingAdd implements AutoCloseable {
        private final ZSetValue owner;
        private final ZAddPlan plan;
        private NativePackedZSet packedReplacement;
        private StagedSkiplist convertedReplacement;
        private NativeByteMap.PreparedMutation<ZSkipList.Node> preparedMap;
        private ZSkipList.PreparedInsert[] inserts;
        private ZSkipList.PreparedDelete[] deletes;
        private NativeHandle[] newMembers;
        private NativePackedZSet supersededPacked;
        private boolean committed;
        private boolean released;
        private boolean metadataRefreshed;

        private PreparedExistingAdd(ZSetValue owner, ZAddPlan plan) {
            this.owner = owner;
            this.plan = plan;
        }

        private static PreparedExistingAdd noop(ZSetValue owner, ZAddPlan plan) {
            return new PreparedExistingAdd(owner, plan);
        }

        private static PreparedExistingAdd packed(
                ZSetValue owner,
                ZAddPlan plan,
                NativePackedZSet replacement
        ) {
            PreparedExistingAdd prepared = new PreparedExistingAdd(owner, plan);
            prepared.packedReplacement = replacement;
            return prepared;
        }

        private static PreparedExistingAdd converted(
                ZSetValue owner,
                ZAddPlan plan,
                StagedSkiplist replacement
        ) {
            PreparedExistingAdd prepared = new PreparedExistingAdd(owner, plan);
            prepared.convertedReplacement = replacement;
            return prepared;
        }

        private static PreparedExistingAdd skiplist(
                ZSetValue owner,
                ZAddPlan plan,
                NativeByteMap.PreparedMutation<ZSkipList.Node> preparedMap,
                ZSkipList.PreparedInsert[] inserts,
                ZSkipList.PreparedDelete[] deletes,
                NativeHandle[] newMembers
        ) {
            PreparedExistingAdd prepared = new PreparedExistingAdd(owner, plan);
            prepared.preparedMap = preparedMap;
            prepared.inserts = inserts;
            prepared.deletes = deletes;
            prepared.newMembers = newMembers;
            return prepared;
        }

        public ZAddResult result() {
            return new ZAddResult(plan.added(), plan.changedAny());
        }

        public boolean changedAny() {
            return plan.changedAny();
        }

        public long stagedHeapBytes() {
            return plan.stagedHeapBytes();
        }

        public ValueEncoding targetEncoding() {
            if (!plan.changedAny()) {
                return owner.encoding();
            }
            return switch (plan.path) {
                case PACKED_REPLACEMENT -> ValueEncoding.ZSET_PACKED;
                case PACKED_TO_SKIPLIST, SKIPLIST_DELTA -> ValueEncoding.ZSET_SKIPLIST;
            };
        }

        public long targetHeapEstimatedBytes() {
            if (!plan.changedAny()) {
                return owner.heapEstimatedBytes();
            }
            return switch (plan.path) {
                case PACKED_REPLACEMENT -> addSaturating(
                        FIXED_HEAP_BYTES,
                        packedReplacement.heapEstimatedBytes()
                );
                case PACKED_TO_SKIPLIST -> addSaturating(
                        FIXED_HEAP_BYTES,
                        addSaturating(
                                convertedReplacement.byMember.heapEstimatedBytes(),
                                convertedReplacement.byScore.heapEstimatedBytes()
                        )
                );
                case SKIPLIST_DELTA -> targetSkiplistHeapEstimatedBytes();
            };
        }

        private long targetSkiplistHeapEstimatedBytes() {
            int addedNodes = 0;
            long levelDelta = 0L;
            int changedIndex = 0;
            for (ZAddPlanEntry entry : plan.entries) {
                if (!entry.changed) {
                    continue;
                }
                ZSkipList.Node inserted = inserts[changedIndex++].node();
                int previousLevels = entry.previousNode == null ? 0 : entry.previousNode.forward.length;
                if (entry.previousNode == null) {
                    addedNodes++;
                }
                levelDelta += inserted.forward.length - previousLevels;
            }
            return addSaturating(
                    FIXED_HEAP_BYTES,
                    addSaturating(
                            preparedMap.targetHeapBytes(),
                            owner.byScore.heapEstimatedBytesAfterPreparedChanges(addedNodes, levelDelta)
                    )
            );
        }

        public void commit() {
            if (committed || released) {
                throw new IllegalStateException("prepared ZADD is closed");
            }
            plan.validateFor(owner);
            if (!plan.changedAny()) {
                committed = true;
                return;
            }
            if (plan.path == ZAddPath.SKIPLIST_DELTA) {
                preparedMap.validateForCommit();
                for (ZSkipList.PreparedDelete delete : deletes) {
                    if (delete != null) {
                        owner.byScore.validatePreparedDelete(delete);
                    }
                }
            }
            committed = true;
            switch (plan.path) {
                case PACKED_REPLACEMENT -> commitPacked();
                case PACKED_TO_SKIPLIST -> commitConversion();
                case SKIPLIST_DELTA -> commitSkiplistDelta();
            }
        }

        private void commitPacked() {
            supersededPacked = owner.listpack;
            owner.listpack = packedReplacement;
            packedReplacement = null;
        }

        private void commitConversion() {
            supersededPacked = owner.listpack;
            owner.listpack = null;
            owner.byMember = convertedReplacement.byMember;
            owner.byScore = convertedReplacement.byScore;
            owner.skiplistLevels = convertedReplacement.levels;
            convertedReplacement.transferred = true;
            convertedReplacement = null;
        }

        private void commitSkiplistDelta() {
            int changedIndex = 0;
            for (ZAddPlanEntry entry : plan.entries) {
                if (!entry.changed) {
                    continue;
                }
                if (deletes[changedIndex] != null
                        && !owner.byScore.deletePrepared(deletes[changedIndex])) {
                    throw new IllegalStateException("prepared ZADD old score node is missing");
                }
                ZSkipList.Node inserted = owner.byScore.insertPrepared(inserts[changedIndex]);
                int previousLevels = entry.previousNode == null ? 0 : entry.previousNode.forward.length;
                owner.skiplistLevels += inserted.forward.length - previousLevels;
                changedIndex++;
            }
            preparedMap.commitValidated();
        }

        public void releaseSuperseded() {
            if (!committed) {
                throw new IllegalStateException("prepared ZADD is not committed");
            }
            if (released) {
                return;
            }
            Throwable failure = null;
            if (!metadataRefreshed) {
                try {
                    if (preparedMap != null) {
                        preparedMap.releaseSuperseded();
                        preparedMap = null;
                    } else {
                        owner.notifyHeapChanged();
                    }
                    metadataRefreshed = true;
                } catch (RuntimeException | Error refreshFailure) {
                    failure = refreshFailure;
                }
            }
            if (supersededPacked != null) {
                NativePackedZSet previous = supersededPacked;
                supersededPacked = null;
                try {
                    previous.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            inserts = null;
            deletes = null;
            newMembers = null;
            if (failure != null) {
                rethrow(failure);
            }
            released = true;
        }

        @Override
        public void close() {
            if (committed || released) {
                return;
            }
            released = true;
            RuntimeException cleanupFailure = new RuntimeException("prepared ZADD abort failed");
            if (packedReplacement != null) {
                try {
                    packedReplacement.close();
                } catch (RuntimeException | Error closeFailure) {
                    cleanupFailure.addSuppressed(closeFailure);
                } finally {
                    packedReplacement = null;
                }
            }
            if (convertedReplacement != null) {
                try {
                    convertedReplacement.close();
                } catch (RuntimeException | Error closeFailure) {
                    cleanupFailure.addSuppressed(closeFailure);
                } finally {
                    convertedReplacement = null;
                }
            }
            if (preparedMap != null) {
                try {
                    preparedMap.close();
                } catch (RuntimeException | Error closeFailure) {
                    cleanupFailure.addSuppressed(closeFailure);
                } finally {
                    preparedMap = null;
                }
            }
            if (newMembers != null) {
                owner.releaseMemberHandles(newMembers, cleanupFailure);
                newMembers = null;
            }
            if (cleanupFailure.getSuppressed().length != 0) {
                throw cleanupFailure;
            }
        }
    }

    private final class StagedSkiplist implements AutoCloseable {
        private final NativeByteMap<ZSkipList.Node> byMember;
        private final ZSkipList byScore;
        private final NativeHandle[] members;
        private final long levels;
        private boolean transferred;

        private StagedSkiplist(
                NativeByteMap<ZSkipList.Node> byMember,
                ZSkipList byScore,
                NativeHandle[] members,
                long levels
        ) {
            this.byMember = byMember;
            this.byScore = byScore;
            this.members = members;
            this.levels = levels;
        }

        @Override
        public void close() {
            if (transferred) {
                return;
            }
            RuntimeException cleanupFailure = new RuntimeException("staged skiplist cleanup failed");
            releaseMemberHandles(members, cleanupFailure);
            try {
                byMember.close();
            } catch (RuntimeException | Error closeFailure) {
                cleanupFailure.addSuppressed(closeFailure);
            }
            if (cleanupFailure.getSuppressed().length != 0) {
                throw cleanupFailure;
            }
        }
    }

    private enum ZAddPath {
        PACKED_REPLACEMENT,
        PACKED_TO_SKIPLIST,
        SKIPLIST_DELTA
    }

    private record ScoreMemberInput(byte[] member, double score) {
    }

    private record FinalMember(byte[] member, double score) {
    }

    private static final class ZAddPlanEntry {
        private final byte[] member;
        private final double score;
        private final boolean present;
        private final double previousScore;
        private final ZSkipList.Node previousNode;
        private final boolean changed;

        private ZAddPlanEntry(
                byte[] member,
                double score,
                boolean present,
                double previousScore,
                ZSkipList.Node previousNode,
                boolean changed
        ) {
            this.member = member;
            this.score = score;
            this.present = present;
            this.previousScore = previousScore;
            this.previousNode = previousNode;
            this.changed = changed;
        }
    }

    private static final class ScoreMemberIndex {
        private static final int MAX_CAPACITY = 1 << 30;

        private final List<ScoreMemberInput> entries;
        private final HashSeed hashSeed;
        private final int[] indexes;

        private ScoreMemberIndex(List<ScoreMemberInput> entries, int expected, HashSeed hashSeed) {
            this.entries = entries;
            this.hashSeed = hashSeed;
            this.indexes = new int[indexCapacity(expected)];
        }

        private int find(byte[] member) {
            int slot = slot(member);
            int mask = indexes.length - 1;
            while (true) {
                int encoded = indexes[slot];
                if (encoded == 0) {
                    return -1;
                }
                int index = encoded - 1;
                if (Arrays.equals(entries.get(index).member, member)) {
                    return index;
                }
                slot = (slot + 1) & mask;
            }
        }

        private void add(int index) {
            int slot = slot(entries.get(index).member);
            int mask = indexes.length - 1;
            while (indexes[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            indexes[slot] = index + 1;
        }

        private int slot(byte[] member) {
            int hash = SipHash24.foldToInt(SipHash24.hash(hashSeed, member));
            return (hash ^ (hash >>> 16)) & (indexes.length - 1);
        }

        private static int indexCapacity(int expected) {
            long required = Math.max(16L, (long) expected * 2L);
            if (required > MAX_CAPACITY) {
                throw new IllegalArgumentException("too many ZADD members to plan");
            }
            int capacity = 16;
            while (capacity < required) {
                capacity <<= 1;
            }
            return capacity;
        }
    }

    public record PackedBuildPlan(int memberCount, int encodedBytes) {
        public PackedBuildPlan {
            if (memberCount < 0 || encodedBytes < 0) {
                throw new IllegalArgumentException("packed build sizes must be >= 0");
            }
            if ((memberCount == 0) != (encodedBytes == 0)) {
                throw new IllegalArgumentException("empty member and encoded byte counts must agree");
            }
        }
    }

}
