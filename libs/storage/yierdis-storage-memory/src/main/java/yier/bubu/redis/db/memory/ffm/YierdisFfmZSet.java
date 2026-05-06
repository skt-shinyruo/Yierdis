package yier.bubu.redis.db.memory.ffm;

import yier.bubu.redis.db.ValueEncoding;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class YierdisFfmZSet implements AutoCloseable {
    private static final int ZSET_MAX_LISTPACK_ENTRIES = 128;
    private static final int ZSET_MAX_LISTPACK_VALUE_BYTES = 64;

    private final YierdisFfmBlobStore blobStore;
    private final ArrayList<Entry> ordered = new ArrayList<>();
    private YierdisFfmByteMap<Entry> byMember;

    public YierdisFfmZSet(YierdisFfmBlobStore blobStore) {
        if (blobStore == null) {
            throw new IllegalArgumentException("blobStore must not be null");
        }
        this.blobStore = blobStore;
    }

    public ValueEncoding encoding() {
        return byMember == null ? ValueEncoding.ZSET_PACKED : ValueEncoding.ZSET_SKIPLIST;
    }

    public int size() {
        return ordered.size();
    }

    public int zaddMany(List<byte[]> scoreMemberPairs) {
        return zaddMany(scoreMemberPairs, null);
    }

    public int zaddMany(List<byte[]> scoreMemberPairs, boolean[] changedRef) {
        int added = 0;
        boolean changedAny = false;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            byte[] member = scoreMemberPairs.get(i + 1);
            int outcome = zadd(score, member);
            if (outcome != 0) {
                changedAny = true;
                if (outcome > 0) {
                    added++;
                }
            }
        }
        if (changedRef != null && changedRef.length > 0 && changedAny) {
            changedRef[0] = true;
        }
        return added;
    }

    public int zrem(List<byte[]> members) {
        int removed = 0;
        for (byte[] member : members) {
            Entry entry = findEntry(member);
            if (entry == null) {
                continue;
            }
            removeEntry(entry, member);
            removed++;
        }
        return removed;
    }

    public int zremrangeByRank(long start, long stop) {
        Range range = normalizeRange(start, stop);
        if (range == null) {
            return 0;
        }
        int removed = 0;
        for (int i = range.stop; i >= range.start; i--) {
            Entry entry = ordered.get(i);
            removeEntry(entry, blobStore.toByteArray(entry.memberRef));
            removed++;
        }
        return removed;
    }

    public int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        int removed = 0;
        for (int i = ordered.size() - 1; i >= 0; i--) {
            Entry entry = ordered.get(i);
            if (!scoreInRange(entry.score, min, minExclusive, max, maxExclusive)) {
                continue;
            }
            removeEntry(entry, blobStore.toByteArray(entry.memberRef));
            removed++;
        }
        return removed;
    }

    public List<byte[]> zrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, false);
    }

    public List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, true);
    }

    public int zrangeCount(long start, long stop, boolean withScores) {
        Range range = normalizeRange(start, stop);
        if (range == null) {
            return 0;
        }
        int count = range.stop - range.start + 1;
        return withScores ? count * 2 : count;
    }

    public void zrangeWriteTo(long start, long stop, boolean withScores, BulkStringSink out) {
        rangeByIndexWriteTo(start, stop, withScores, false, out);
    }

    public int zrevrangeCount(long start, long stop, boolean withScores) {
        return zrangeCount(start, stop, withScores);
    }

    public void zrevrangeWriteTo(long start, long stop, boolean withScores, BulkStringSink out) {
        rangeByIndexWriteTo(start, stop, withScores, true, out);
    }

    public List<byte[]> zrangeByScore(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return rangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count, false);
    }

    public List<byte[]> zrevrangeByScore(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return rangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count, true);
    }

    public int zrangeByScoreCount(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return rangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count, false);
    }

    public void zrangeByScoreWriteTo(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            BulkStringSink out
    ) {
        rangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, false, out);
    }

    public int zrevrangeByScoreCount(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return rangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count, true);
    }

    public void zrevrangeByScoreWriteTo(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            BulkStringSink out
    ) {
        rangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, true, out);
    }

    @Override
    public void close() {
        if (byMember != null) {
            byMember.close();
            byMember = null;
        }
        for (Entry entry : ordered) {
            blobStore.release(entry.memberRef);
        }
        ordered.clear();
    }

    private int zadd(double score, byte[] memberBytes) {
        Entry entry = findEntry(memberBytes);
        if (entry != null) {
            if (Double.compare(entry.score, score) == 0) {
                return 0;
            }
            ordered.remove(entry);
            entry.score = score;
            insertOrdered(entry);
            return -1;
        }

        if (byMember == null
                && (ordered.size() >= ZSET_MAX_LISTPACK_ENTRIES
                || memberBytes.length > ZSET_MAX_LISTPACK_VALUE_BYTES)) {
            convertToSkiplistMode();
        }

        Entry next = new Entry(blobStore.store(memberBytes), score);
        insertOrdered(next);
        if (byMember != null) {
            byMember.put(memberBytes, next);
        }
        return 1;
    }

    private void convertToSkiplistMode() {
        if (byMember != null) {
            return;
        }
        byMember = new YierdisFfmByteMap<>(blobStore);
        for (Entry entry : ordered) {
            byMember.put(blobStore.toByteArray(entry.memberRef), entry);
        }
    }

    private Entry findEntry(byte[] memberBytes) {
        if (byMember != null) {
            return byMember.get(memberBytes);
        }
        for (Entry entry : ordered) {
            if (blobStore.equalsBytes(entry.memberRef, memberBytes)) {
                return entry;
            }
        }
        return null;
    }

    private void removeEntry(Entry entry, byte[] memberBytes) {
        ordered.remove(entry);
        if (byMember != null) {
            byMember.remove(memberBytes);
        }
        blobStore.release(entry.memberRef);
    }

    private void insertOrdered(Entry entry) {
        int insertAt = 0;
        for (; insertAt < ordered.size(); insertAt++) {
            Entry candidate = ordered.get(insertAt);
            int cmp = compareEntries(candidate, entry);
            if (cmp >= 0) {
                break;
            }
        }
        ordered.add(insertAt, entry);
    }

    private List<byte[]> rangeByIndex(long start, long stop, boolean withScores, boolean reverse) {
        Range range = normalizeRange(start, stop);
        if (range == null) {
            return new ArrayList<>();
        }
        int count = range.stop - range.start + 1;
        List<byte[]> out = new ArrayList<>(withScores ? count * 2 : count);
        for (int i = range.start; i <= range.stop; i++) {
            Entry entry = entryAt(range, i, reverse);
            out.add(blobStore.toByteArray(entry.memberRef));
            if (withScores) {
                out.add(formatScoreBytes(entry.score));
            }
        }
        return out;
    }

    private void rangeByIndexWriteTo(long start, long stop, boolean withScores, boolean reverse, BulkStringSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        Range range = normalizeRange(start, stop);
        if (range == null) {
            return;
        }
        for (int i = range.start; i <= range.stop; i++) {
            Entry entry = entryAt(range, i, reverse);
            out.bulkString(new YierdisFfmBytesRefSlice(entry.memberRef));
            if (withScores) {
                writeScoreTo(out, entry.score);
            }
        }
    }

    private List<byte[]> rangeByScore(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            boolean reverse
    ) {
        List<Entry> matches = entriesByScore(min, minExclusive, max, maxExclusive, offset, count, reverse);
        List<byte[]> out = new ArrayList<>(withScores ? matches.size() * 2 : matches.size());
        for (Entry entry : matches) {
            out.add(blobStore.toByteArray(entry.memberRef));
            if (withScores) {
                out.add(formatScoreBytes(entry.score));
            }
        }
        return out;
    }

    private int rangeByScoreCount(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            boolean reverse
    ) {
        int matches = entriesByScore(min, minExclusive, max, maxExclusive, offset, count, reverse).size();
        return withScores ? matches * 2 : matches;
    }

    private void rangeByScoreWriteTo(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            boolean reverse,
            BulkStringSink out
    ) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        for (Entry entry : entriesByScore(min, minExclusive, max, maxExclusive, offset, count, reverse)) {
            out.bulkString(new YierdisFfmBytesRefSlice(entry.memberRef));
            if (withScores) {
                writeScoreTo(out, entry.score);
            }
        }
    }

    private List<Entry> entriesByScore(
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            long offset,
            long count,
            boolean reverse
    ) {
        if (count <= 0 || ordered.isEmpty()) {
            return List.of();
        }
        ArrayList<Entry> out = new ArrayList<>();
        long skipped = Math.max(0, offset);
        if (!reverse) {
            for (Entry entry : ordered) {
                if (!scoreInRange(entry.score, min, minExclusive, max, maxExclusive)) {
                    continue;
                }
                if (skipped > 0) {
                    skipped--;
                    continue;
                }
                out.add(entry);
                if (out.size() >= count) {
                    break;
                }
            }
            return out;
        }
        for (int i = ordered.size() - 1; i >= 0; i--) {
            Entry entry = ordered.get(i);
            if (!scoreInRange(entry.score, min, minExclusive, max, maxExclusive)) {
                continue;
            }
            if (skipped > 0) {
                skipped--;
                continue;
            }
            out.add(entry);
            if (out.size() >= count) {
                break;
            }
        }
        return out;
    }

    private Range normalizeRange(long start, long stop) {
        int size = ordered.size();
        if (size == 0) {
            return null;
        }
        long normalizedStart = normalizeIndex(start, size);
        long normalizedStop = normalizeIndex(stop, size);
        if (normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (normalizedStop < 0) {
            return null;
        }
        if (normalizedStop >= size) {
            normalizedStop = size - 1L;
        }
        if (normalizedStart > normalizedStop) {
            return null;
        }
        return new Range((int) normalizedStart, (int) normalizedStop);
    }

    private Entry entryAt(Range range, int index, boolean reverse) {
        if (!reverse) {
            return ordered.get(index);
        }
        return ordered.get(ordered.size() - 1 - index);
    }

    private static long normalizeIndex(long index, int size) {
        return index >= 0 ? index : (long) size + index;
    }

    private int compareEntries(Entry left, Entry right) {
        int cmp = Double.compare(left.score, right.score);
        if (cmp != 0) {
            return cmp;
        }
        return compareLex(left.memberRef, right.memberRef);
    }

    private int compareLex(YierdisFfmBytesRef left, YierdisFfmBytesRef right) {
        int min = Math.min(left.length(), right.length());
        for (int i = 0; i < min; i++) {
            int lv = left.byteAt(i) & 0xFF;
            int rv = right.byteAt(i) & 0xFF;
            if (lv != rv) {
                return Integer.compare(lv, rv);
            }
        }
        return Integer.compare(left.length(), right.length());
    }

    private static boolean scoreInRange(double score, double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (Double.isNaN(score)) {
            return false;
        }
        if (Double.compare(score, min) < 0) {
            return false;
        }
        if (minExclusive && Double.compare(score, min) == 0) {
            return false;
        }
        if (Double.compare(score, max) > 0) {
            return false;
        }
        return !maxExclusive || Double.compare(score, max) != 0;
    }

    private static double parseScore(byte[] bytes) {
        double value;
        try {
            value = Double.parseDouble(new String(bytes, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new YierdisCommandException("ERR value is not a valid float");
        }
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new YierdisCommandException("ERR value is not a valid float");
        }
        return value;
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

    private static final class Entry {
        private final YierdisFfmBytesRef memberRef;
        private double score;

        private Entry(YierdisFfmBytesRef memberRef, double score) {
            this.memberRef = memberRef;
            this.score = score;
        }
    }

    private record Range(int start, int stop) {
    }
}
