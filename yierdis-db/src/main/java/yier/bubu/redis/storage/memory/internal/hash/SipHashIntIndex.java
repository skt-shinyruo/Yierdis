package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * 暂存期去重索引的共享实现：由 keyAt 从条目下标提取 byte[] key，SipHash24 → fold hash ^ hash >>> 16，
 * 2 的幂表长、0 哨兵线性探测，容量 max(16, 2 × expectedEntries) 向上取 2 的幂。
 * 相等性为字节内容相等（{@link Arrays#equals(byte[], byte[])}）。
 * 容量需按最大写入条目数给足——负载因子超过 1/2 时探测可能找不到空槽。
 */
public final class SipHashIntIndex {
    private final IntFunction<byte[]> keyAt;
    private final HashSeed hashSeed;
    private final int[] entryIndexes;

    public SipHashIntIndex(
            IntFunction<byte[]> keyAt,
            long expectedEntries,
            HashSeed hashSeed,
            String capacityExceededMessage
    ) {
        this.keyAt = Objects.requireNonNull(keyAt, "keyAt");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        Objects.requireNonNull(capacityExceededMessage, "capacityExceededMessage");
        long required = Math.max((long) HashCapacityPolicy.MIN_CAPACITY, expectedEntries * 2L);
        if (required > HashCapacityPolicy.MAX_CAPACITY) {
            throw new IllegalArgumentException(capacityExceededMessage);
        }
        int capacity = HashCapacityPolicy.MIN_CAPACITY;
        while (capacity < required) {
            capacity <<= 1;
        }
        this.entryIndexes = new int[capacity];
    }

    /** 返回 key 对应的条目下标，不存在返回 -1。 */
    public int find(byte[] key) {
        int mask = entryIndexes.length - 1;
        int slot = slot(key);
        while (true) {
            int encoded = entryIndexes[slot];
            if (encoded == 0) {
                return -1;
            }
            int entryIndex = encoded - 1;
            if (Arrays.equals(keyAt.apply(entryIndex), key)) {
                return entryIndex;
            }
            slot = (slot + 1) & mask;
        }
    }

    /** 写入调用方已知不在索引中的条目下标。 */
    public void add(int entryIndex) {
        int mask = entryIndexes.length - 1;
        int slot = slot(keyAt.apply(entryIndex));
        while (entryIndexes[slot] != 0) {
            slot = (slot + 1) & mask;
        }
        entryIndexes[slot] = entryIndex + 1;
    }

    /** 条目 key 不在索引中时写入并返回 true，已存在同 key 条目时返回 false。 */
    public boolean addIfAbsent(int entryIndex) {
        byte[] key = keyAt.apply(entryIndex);
        int mask = entryIndexes.length - 1;
        int slot = slot(key);
        while (entryIndexes[slot] != 0) {
            int existingIndex = entryIndexes[slot] - 1;
            if (Arrays.equals(keyAt.apply(existingIndex), key)) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
        entryIndexes[slot] = entryIndex + 1;
        return true;
    }

    public void clear() {
        Arrays.fill(entryIndexes, 0);
    }

    private int slot(byte[] key) {
        int hash = SipHash24.foldToInt(SipHash24.hash(hashSeed, key));
        return (hash ^ (hash >>> 16)) & (entryIndexes.length - 1);
    }
}
