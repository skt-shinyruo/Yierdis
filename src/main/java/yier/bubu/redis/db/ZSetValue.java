package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.charset.StandardCharsets;

final class ZSetValue implements YierdisValue {
    // Redis uses listpack for small ZSETs and upgrades to dict+skiplist as needed.
    // We approximate that behavior with an in-Java "listpack-like" sorted array and upgrade to
    // HashMap+skiplist once size/element thresholds are crossed.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private PackedZSet listpack = new PackedZSet();
    private ByteArrayHashMap<ZSkipList.Node> byMember;
    private ZSkipList byScore;

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    @Override
    public ValueEncoding encoding() {
        return listpack != null ? ValueEncoding.ZSET_PACKED : ValueEncoding.ZSET_SKIPLIST;
    }

    int size() {
        if (listpack != null) {
            return listpack.size();
        }
        return byMember.size();
    }

    int zaddMany(List<byte[]> scoreMemberPairs) {
        int added = 0;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            byte[] memberBytes = scoreMemberPairs.get(i + 1);

            if (listpack != null) {
                if (memberBytes != null && memberBytes.length > LISTPACK_MAX_ELEMENT_BYTES) {
                    convertToSkipList();
                }
                if (listpack != null) {
                    added += listpackZadd(score, memberBytes);
                    continue;
                }
            }

            ByteArrayKey member = new ByteArrayKey(memberBytes);
            added += skiplistZadd(score, member);
        }
        return added;
    }

    int zrem(List<byte[]> members) {
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
            byScore.delete(old.score, old.member);
            removed++;
        }
        return removed;
    }

    List<byte[]> zrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    List<byte[]> zrevrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        if (listpack != null) {
            return zrevrangeByScoreListpack(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
        return zrevrangeByScoreSkipList(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
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
            byMember.remove(node.member.bytes());
            byScore.delete(node.score, node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    int zremrangeByRank(long start, long stop) {
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
            byMember.remove(node.member.bytes());
            byScore.delete(node.score, node.member);
            removed++;
            node = next;
        }
        return removed;
    }

    List<byte[]> zrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, false);
    }

    List<byte[]> zrevrange(long start, long stop, boolean withScores) {
        return rangeByIndex(start, stop, withScores, true);
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
                out.add(node.member.bytes());
                node = stepBackwards ? node.backward : node.forward[0];
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (int i = 0; i < remaining && node != null; i++) {
            out.add(node.member.bytes());
            out.add(formatScoreBytes(node.score));
            node = stepBackwards ? node.backward : node.forward[0];
        }
        return out;
    }

    private List<byte[]> zrangeByScoreListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int startIdx = 0;
        if (min != Double.NEGATIVE_INFINITY) {
            startIdx = lowerBoundByScore(min, minExclusive);
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

            out.add(listpack.memberBytesAt(i));
            if (withScores) {
                out.add(formatScoreBytes(score));
            }
            remaining--;
        }
        return out;
    }

    private List<byte[]> zrevrangeByScoreListpack(double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        int size = listpack.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int startIdx = 0;
        if (min != Double.NEGATIVE_INFINITY) {
            startIdx = lowerBoundByScore(min, minExclusive);
        }

        int endExclusive = size;
        if (max != Double.POSITIVE_INFINITY) {
            endExclusive = upperBoundByScore(max, maxExclusive);
        }
        int endIdx = endExclusive - 1;
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
            out.add(listpack.memberBytesAt(i));
            if (withScores) {
                out.add(formatScoreBytes(listpack.scoreAt(i)));
            }
            remaining--;
        }
        return out;
    }

    private int lowerBoundByScore(double score, boolean exclusive) {
        int low = 0;
        int high = listpack.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            double s = listpack.scoreAt(mid);
            if (s < score || (exclusive && Double.compare(s, score) == 0)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int upperBoundByScore(double score, boolean exclusive) {
        int low = 0;
        int high = listpack.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            double s = listpack.scoreAt(mid);
            if (Double.compare(s, score) > 0 || (exclusive && Double.compare(s, score) == 0)) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
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

            out.add(node.member.bytes());
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.forward[0];
        }
        return out;
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

            out.add(node.member.bytes());
            if (withScores) {
                out.add(formatScoreBytes(s));
            }
            remaining--;
            node = node.backward;
        }
        return out;
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
                out.add(listpack.memberBytesAt(idx));
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (long i = normalizedStart; i <= normalizedStop; i++) {
            int idx = !reverse ? (int) i : (size - 1 - (int) i);
            out.add(listpack.memberBytesAt(idx));
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

    private int skiplistZadd(double score, ByteArrayKey member) {
        if (byMember == null) {
            byMember = new ByteArrayHashMap<>();
            byScore = new ZSkipList();
        }

        ZSkipList.Node old = byMember.get(member.bytes());
        if (old != null) {
            if (Double.compare(old.score, score) == 0) {
                return 0;
            }
            byScore.delete(old.score, old.member);
        }

        ZSkipList.Node next = byScore.insert(score, member);
        byMember.put(member.bytes(), next);
        return old == null ? 1 : 0;
    }

    private int listpackZadd(double score, byte[] memberBytes) {
        ByteArrayKey member = new ByteArrayKey(memberBytes);
        int idx = indexOfMember(member);
        if (idx >= 0) {
            double oldScore = listpack.scoreAt(idx);
            if (Double.compare(oldScore, score) == 0) {
                return 0;
            }
            listpack.removeAt(idx);
            insertSorted(member, score);
            return 0;
        }

        if (listpack.size() >= LISTPACK_MAX_ENTRIES) {
            convertToSkipList();
            return skiplistZadd(score, member);
        }

        insertSorted(member, score);
        return 1;
    }

    private int listpackZrem(byte[] memberBytes) {
        ByteArrayKey member = new ByteArrayKey(memberBytes);
        int idx = indexOfMember(member);
        if (idx < 0) {
            return 0;
        }
        listpack.removeAt(idx);
        return 1;
    }

    private int indexOfMember(ByteArrayKey member) {
        return listpack.indexOfMember(member);
    }

    private void insertSorted(ByteArrayKey member, double score) {
        int low = 0;
        int high = listpack.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (lessThan(mid, score, member)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        listpack.insertAt(low, score, member.bytes());
    }

    private boolean lessThan(int entryIndex, double score, ByteArrayKey member) {
        double nodeScore = listpack.scoreAt(entryIndex);
        if (Double.compare(nodeScore, score) < 0) {
            return true;
        }
        if (Double.compare(nodeScore, score) > 0) {
            return false;
        }
        return listpack.compareMemberAt(entryIndex, member) < 0;
    }

    private void convertToSkipList() {
        if (listpack == null) {
            return;
        }
        int size = listpack.size();
        ByteArrayHashMap<ZSkipList.Node> outByMember = new ByteArrayHashMap<>(Math.max(16, size));
        ZSkipList outByScore = new ZSkipList();
        for (int i = 0; i < size; i++) {
            double score = listpack.scoreAt(i);
            ByteArrayKey member = new ByteArrayKey(listpack.memberBytesAt(i));
            ZSkipList.Node n = outByScore.insert(score, member);
            outByMember.put(member.bytes(), n);
        }
        this.byMember = outByMember;
        this.byScore = outByScore;
        this.listpack = null;
    }

    private static final class PackedZSet {
        private byte[] blob = new byte[0];
        private int[] offsets = new int[0];
        private int size = 0;

        int size() {
            return size;
        }

        double scoreAt(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            int entryStart = offsets[index];
            long bits = readLongBE(blob, entryStart);
            return Double.longBitsToDouble(bits);
        }

        byte[] memberBytesAt(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            int entryStart = offsets[index];
            int p = entryStart + Long.BYTES;
            long r = readVarint(blob, p);
            int memberLen = (int) r;
            p = (int) (r >>> 32);
            return Arrays.copyOfRange(blob, p, p + memberLen);
        }

        int compareMemberAt(int index, ByteArrayKey member) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            int entryStart = offsets[index];
            int p = entryStart + Long.BYTES;
            long r = readVarint(blob, p);
            int memberLen = (int) r;
            p = (int) (r >>> 32);
            return compareLex(blob, p, memberLen, member.bytes());
        }

        int indexOfMember(ByteArrayKey member) {
            byte[] target = member.bytes();
            for (int i = 0; i < size; i++) {
                int entryStart = offsets[i];
                int p = entryStart + Long.BYTES;
                long r = readVarint(blob, p);
                int memberLen = (int) r;
                p = (int) (r >>> 32);
                if (memberLen != target.length) {
                    continue;
                }
                if (bytesEqual(blob, p, memberLen, target)) {
                    return i;
                }
            }
            return -1;
        }

        void insertAt(int index, double score, byte[] memberBytes) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException();
            }
            int memberLen = memberBytes.length;
            int encodedLen = Long.BYTES + varintLength(memberLen) + memberLen;

            int insertPos = index == size ? blob.length : offsets[index];
            byte[] next = new byte[blob.length + encodedLen];
            System.arraycopy(blob, 0, next, 0, insertPos);

            int w = insertPos;
            writeLongBE(next, w, Double.doubleToLongBits(score));
            w += Long.BYTES;
            w = writeVarint(next, w, memberLen);
            System.arraycopy(memberBytes, 0, next, w, memberLen);

            System.arraycopy(blob, insertPos, next, insertPos + encodedLen, blob.length - insertPos);

            int[] nextOffsets = new int[size + 1];
            System.arraycopy(offsets, 0, nextOffsets, 0, index);
            nextOffsets[index] = insertPos;
            for (int i = index; i < size; i++) {
                nextOffsets[i + 1] = offsets[i] + encodedLen;
            }

            blob = next;
            offsets = nextOffsets;
            size++;
        }

        void removeAt(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            int start = offsets[index];
            int end = index + 1 < size ? offsets[index + 1] : blob.length;
            int removedLen = end - start;

            byte[] next = new byte[blob.length - removedLen];
            System.arraycopy(blob, 0, next, 0, start);
            System.arraycopy(blob, end, next, start, blob.length - end);

            int[] nextOffsets = new int[size - 1];
            System.arraycopy(offsets, 0, nextOffsets, 0, index);
            for (int i = index + 1; i < size; i++) {
                nextOffsets[i - 1] = offsets[i] - removedLen;
            }

            blob = next;
            offsets = nextOffsets;
            size--;
        }

        private static boolean bytesEqual(byte[] buf, int bufOff, int len, byte[] other) {
            for (int i = 0; i < len; i++) {
                if (buf[bufOff + i] != other[i]) {
                    return false;
                }
            }
            return true;
        }

        private static int compareLex(byte[] buf, int bufOff, int len, byte[] other) {
            int min = Math.min(len, other.length);
            for (int i = 0; i < min; i++) {
                int av = buf[bufOff + i] & 0xFF;
                int bv = other[i] & 0xFF;
                if (av != bv) {
                    return Integer.compare(av, bv);
                }
            }
            return Integer.compare(len, other.length);
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

        private static long readLongBE(byte[] buf, int pos) {
            return ((long) (buf[pos] & 0xFF) << 56)
                    | ((long) (buf[pos + 1] & 0xFF) << 48)
                    | ((long) (buf[pos + 2] & 0xFF) << 40)
                    | ((long) (buf[pos + 3] & 0xFF) << 32)
                    | ((long) (buf[pos + 4] & 0xFF) << 24)
                    | ((long) (buf[pos + 5] & 0xFF) << 16)
                    | ((long) (buf[pos + 6] & 0xFF) << 8)
                    | ((long) (buf[pos + 7] & 0xFF));
        }

        private static void writeLongBE(byte[] out, int pos, long v) {
            out[pos] = (byte) (v >>> 56);
            out[pos + 1] = (byte) (v >>> 48);
            out[pos + 2] = (byte) (v >>> 40);
            out[pos + 3] = (byte) (v >>> 32);
            out[pos + 4] = (byte) (v >>> 24);
            out[pos + 5] = (byte) (v >>> 16);
            out[pos + 6] = (byte) (v >>> 8);
            out[pos + 7] = (byte) v;
        }
    }

}
