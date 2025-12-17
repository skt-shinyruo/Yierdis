package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

final class ZSetValue implements YierdisValue {
    private final Map<ByteArrayKey, ZSkipList.Node> byMember = new HashMap<>();
    private final ZSkipList byScore = new ZSkipList();

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    int size() {
        return byMember.size();
    }

    int zaddMany(List<byte[]> scoreMemberPairs) {
        int added = 0;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            ByteArrayKey member = new ByteArrayKey(scoreMemberPairs.get(i + 1));

            ZSkipList.Node old = byMember.get(member);
            if (old != null) {
                if (Double.compare(old.score, score) == 0) {
                    continue;
                }
                byScore.delete(old.score, old.member);
            } else {
                added++;
            }

            ZSkipList.Node next = byScore.insert(score, member);
            byMember.put(member, next);
        }
        return added;
    }

    int zrem(List<byte[]> members) {
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

    List<byte[]> zrange(int start, int stop, boolean withScores) {
        int size = byMember.size();
        if (size == 0) {
            return new ArrayList<>();
        }

        int normalizedStart = normalizeIndex(start, size);
        int normalizedStop = normalizeIndex(stop, size);

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

        int startRank = normalizedStart + 1;
        int stopRank = normalizedStop + 1;
        ZSkipList.Node node = byScore.getElementByRank(startRank);
        int remaining = stopRank - startRank + 1;

        if (!withScores) {
            List<byte[]> out = new ArrayList<>(remaining);
            for (int i = 0; i < remaining && node != null; i++) {
                out.add(node.member.bytes());
                node = node.forward[0];
            }
            return out;
        }

        List<byte[]> out = new ArrayList<>(remaining * 2);
        for (int i = 0; i < remaining && node != null; i++) {
            out.add(node.member.bytes());
            out.add(formatScoreBytes(node.score));
            node = node.forward[0];
        }
        return out;
    }

    private static int normalizeIndex(int idx, int size) {
        if (idx >= 0) {
            return idx;
        }
        return size + idx;
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
}
