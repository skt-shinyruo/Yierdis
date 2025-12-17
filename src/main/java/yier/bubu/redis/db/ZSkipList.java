package yier.bubu.redis.db;

import java.util.concurrent.ThreadLocalRandom;

final class ZSkipList {
    // Mirrors Redis defaults:
    // https://github.com/redis/redis/blob/unstable/src/server.h
    private static final int MAX_LEVEL = 32;
    private static final double P = 0.25d;

    private final Node header = new Node(MAX_LEVEL, null, 0);
    private Node tail;
    private int level = 1;
    private int length = 0;

    Node insert(double score, ByteArrayKey member) {
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

    boolean delete(double score, ByteArrayKey member) {
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

    private static boolean lessThan(Node node, double score, ByteArrayKey member) {
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
        final ByteArrayKey member;
        final double score;
        final Node[] forward;
        final int[] span;
        Node backward;

        Node(int level, ByteArrayKey member, double score) {
            this.member = member;
            this.score = score;
            this.forward = new Node[level];
            this.span = new int[level];
        }
    }
}

