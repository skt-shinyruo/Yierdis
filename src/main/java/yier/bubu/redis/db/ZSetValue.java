package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

final class ZSetValue implements YierdisValue {
    // Redis uses listpack for small ZSETs and upgrades to dict+skiplist as needed.
    // We approximate that behavior with an in-Java "listpack-like" sorted array and upgrade to
    // HashMap+skiplist once size/element thresholds are crossed.
    private static final int LISTPACK_MAX_ENTRIES = 128;
    private static final int LISTPACK_MAX_ELEMENT_BYTES = 64;

    private List<ListPackEntry> listpack = new ArrayList<>();
    private Map<ByteArrayKey, ZSkipList.Node> byMember;
    private ZSkipList byScore;

    @Override
    public ValueType type() {
        return ValueType.ZSET;
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
            ZSkipList.Node old = byMember.remove(new ByteArrayKey(m));
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

    int zremrangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive) {
        if (listpack != null) {
            int removed = 0;
            for (int i = listpack.size() - 1; i >= 0; i--) {
                double s = listpack.get(i).score;
                if (scoreInRange(s, min, minExclusive, max, maxExclusive)) {
                    listpack.remove(i);
                    removed++;
                }
            }
            return removed;
        }

        int removed = 0;
        ZSkipList.Node node = firstNodeForMin(min, minExclusive);
        while (node != null && scoreInRange(node.score, min, minExclusive, max, maxExclusive)) {
            ZSkipList.Node next = node.forward[0];
            byMember.remove(node.member);
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
            ListPackEntry e = listpack.get(i);
            if (!scoreInRange(e.score, min, minExclusive, max, maxExclusive)) {
                if (max == Double.POSITIVE_INFINITY) {
                    continue;
                }
                if (e.score > max || (maxExclusive && Double.compare(e.score, max) == 0)) {
                    break;
                }
                continue;
            }
            if (skipped > 0) {
                skipped--;
                continue;
            }

            out.add(e.member.bytes());
            if (withScores) {
                out.add(formatScoreBytes(e.score));
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
            double s = listpack.get(mid).score;
            if (s < score || (exclusive && Double.compare(s, score) == 0)) {
                low = mid + 1;
            } else {
                high = mid;
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

    private ZSkipList.Node firstNodeForMin(double min, boolean minExclusive) {
        if (min == Double.NEGATIVE_INFINITY) {
            return byScore.first();
        }
        return byScore.findFirstByScore(min, minExclusive);
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
                out.add(listpack.get(idx).member.bytes());
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (long i = normalizedStart; i <= normalizedStop; i++) {
            int idx = !reverse ? (int) i : (size - 1 - (int) i);
            ListPackEntry e = listpack.get(idx);
            out.add(e.member.bytes());
            out.add(formatScoreBytes(e.score));
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
            byMember = new HashMap<>();
            byScore = new ZSkipList();
        }

        ZSkipList.Node old = byMember.get(member);
        if (old != null) {
            if (Double.compare(old.score, score) == 0) {
                return 0;
            }
            byScore.delete(old.score, old.member);
        }

        ZSkipList.Node next = byScore.insert(score, member);
        byMember.put(member, next);
        return old == null ? 1 : 0;
    }

    private int listpackZadd(double score, byte[] memberBytes) {
        ByteArrayKey member = new ByteArrayKey(memberBytes);
        int idx = indexOfMember(member);
        if (idx >= 0) {
            ListPackEntry old = listpack.get(idx);
            if (Double.compare(old.score, score) == 0) {
                return 0;
            }
            listpack.remove(idx);
            insertSorted(new ListPackEntry(member, score));
            return 0;
        }

        if (listpack.size() >= LISTPACK_MAX_ENTRIES) {
            convertToSkipList();
            return skiplistZadd(score, member);
        }

        insertSorted(new ListPackEntry(member, score));
        return 1;
    }

    private int listpackZrem(byte[] memberBytes) {
        ByteArrayKey member = new ByteArrayKey(memberBytes);
        int idx = indexOfMember(member);
        if (idx < 0) {
            return 0;
        }
        listpack.remove(idx);
        return 1;
    }

    private int indexOfMember(ByteArrayKey member) {
        for (int i = 0; i < listpack.size(); i++) {
            if (listpack.get(i).member.equals(member)) {
                return i;
            }
        }
        return -1;
    }

    private void insertSorted(ListPackEntry entry) {
        int low = 0;
        int high = listpack.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            ListPackEntry cur = listpack.get(mid);
            if (lessThan(cur, entry.score, entry.member)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        listpack.add(low, entry);
    }

    private static boolean lessThan(ListPackEntry node, double score, ByteArrayKey member) {
        if (Double.compare(node.score, score) < 0) {
            return true;
        }
        if (Double.compare(node.score, score) > 0) {
            return false;
        }
        return node.member.compareTo(member) < 0;
    }

    private void convertToSkipList() {
        if (listpack == null) {
            return;
        }
        Map<ByteArrayKey, ZSkipList.Node> outByMember = new HashMap<>(Math.max(16, listpack.size() * 2));
        ZSkipList outByScore = new ZSkipList();
        for (ListPackEntry e : listpack) {
            ZSkipList.Node n = outByScore.insert(e.score, e.member);
            outByMember.put(e.member, n);
        }
        this.byMember = outByMember;
        this.byScore = outByScore;
        this.listpack = null;
    }

    private static final class ListPackEntry {
        final ByteArrayKey member;
        final double score;

        private ListPackEntry(ByteArrayKey member, double score) {
            this.member = member;
            this.score = score;
        }
    }

}
