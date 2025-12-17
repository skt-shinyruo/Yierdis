package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

final class ZSetValue implements YierdisValue {
    private final Map<String, ZSkipList.Node> byMember = new HashMap<>();
    private final ZSkipList byScore = new ZSkipList();

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    int size() {
        return byMember.size();
    }

    int zaddMany(List<String> scoreMemberPairs) {
        int added = 0;
        for (int i = 0; i < scoreMemberPairs.size(); i += 2) {
            double score = parseScore(scoreMemberPairs.get(i));
            String member = scoreMemberPairs.get(i + 1);

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

    int zrem(List<String> members) {
        int removed = 0;
        for (String m : members) {
            ZSkipList.Node old = byMember.remove(m);
            if (old == null) {
                continue;
            }
            byScore.delete(old.score, old.member);
            removed++;
        }
        return removed;
    }

    List<String> zrange(int start, int stop, boolean withScores) {
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
            List<String> out = new ArrayList<>(remaining);
            for (int i = 0; i < remaining && node != null; i++) {
                out.add(node.member);
                node = node.forward[0];
            }
            return out;
        }

        List<String> out = new ArrayList<>(remaining * 2);
        for (int i = 0; i < remaining && node != null; i++) {
            out.add(node.member);
            out.add(formatScore(node.score));
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

    private static double parseScore(String s) {
        double v;
        try {
            v = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new YierdisDb.YierdisCommandException("ERR value is not a valid float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisDb.YierdisCommandException("ERR value is not a valid float");
        }
        return v;
    }

    private static String formatScore(double score) {
        if (score == Math.rint(score) && score >= Long.MIN_VALUE && score <= Long.MAX_VALUE) {
            return Long.toString((long) score);
        }
        return Double.toString(score);
    }

    private static final class ZSkipList {
        // Mirrors Redis defaults:
        // https://github.com/redis/redis/blob/unstable/src/server.h
        private static final int MAX_LEVEL = 32;
        private static final double P = 0.25d;

        private final Node header = new Node(MAX_LEVEL, null, 0);
        private Node tail;
        private int level = 1;
        private int length = 0;

        Node insert(double score, String member) {
            Node[] update = new Node[MAX_LEVEL];
            int[] rank = new int[MAX_LEVEL];

            Node x = header;
            for (int i = level - 1; i >= 0; i--) {
                rank[i] = i == level - 1 ? 0 : rank[i + 1];
                while (x.forward[i] != null && lessThan(x.forward[i], score, member)) {
                    rank[i] += x.span[i];
                    x = x.forward[i];
                }
                update[i] = x;
            }

            int newLevel = randomLevel();
            if (newLevel > level) {
                for (int i = level; i < newLevel; i++) {
                    rank[i] = 0;
                    update[i] = header;
                    update[i].span[i] = length;
                }
                level = newLevel;
            }

            Node newNode = new Node(newLevel, member, score);
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
            return newNode;
        }

        boolean delete(double score, String member) {
            Node[] update = new Node[MAX_LEVEL];
            Node x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (x.forward[i] != null && lessThan(x.forward[i], score, member)) {
                    x = x.forward[i];
                }
                update[i] = x;
            }

            x = x.forward[0];
            if (x == null || Double.compare(x.score, score) != 0 || !x.member.equals(member)) {
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
            return true;
        }

        Node getElementByRank(int rank) {
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

        private static boolean lessThan(Node node, double score, String member) {
            if (Double.compare(node.score, score) < 0) {
                return true;
            }
            if (Double.compare(node.score, score) > 0) {
                return false;
            }
            return node.member.compareTo(member) < 0;
        }

        private static int randomLevel() {
            int lvl = 1;
            while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < P) {
                lvl++;
            }
            return lvl;
        }

        static final class Node {
            final String member;
            final double score;
            final Node[] forward;
            final int[] span;
            Node backward;

            Node(int level, String member, double score) {
                this.member = member;
                this.score = score;
                this.forward = new Node[level];
                this.span = new int[level];
            }
        }
    }
}
