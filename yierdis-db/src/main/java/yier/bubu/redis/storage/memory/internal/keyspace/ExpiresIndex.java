package yier.bubu.redis.storage.memory.internal.keyspace;

import java.util.Objects;
import java.util.PriorityQueue;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;

/**
 * 按过期时间有序的 expires 索引，只在 owner 线程上维护与消费，不引入任何同步原语。
 * <p>
 * 索引允许 stale 项滞留：re-SET 改 TTL、PERSIST、overwrite、key 删除重建后都不主动清除旧项，
 * 消费方必须按 entry 的真实 {@code expireAtMillis} 与 key identity 惰性校验，判 stale 后丢弃。
 * 因此索引规模可以超过存活 TTL key 数，但只在 TTL 值实际变化时增长，touch 等
 * expireAtMillis 不变的替换不会写入新项。
 * <p>
 * 该结构是纯堆内存派生 bookkeeping，不计入 ledger 逐 mutation 账，也不计入
 * componentRetainedHeapBytes 物理账；两套账与 TTL 派生状态的关系保持与引入索引前一致。
 */
public final class ExpiresIndex {
    private final PriorityQueue<Entry> queue = new PriorityQueue<>();
    private long nextSequence;

    public void add(long expireAtMillis, AllocatorKeyHandle keyHandle) {
        if (expireAtMillis < 0L) {
            throw new IllegalArgumentException("expireAtMillis must be >= 0");
        }
        queue.offer(new Entry(expireAtMillis, nextSequence++, Objects.requireNonNull(keyHandle, "keyHandle")));
    }

    /**
     * 最早到期的索引项；队列按 (expireAtMillis, sequence) 排序，与 key 内容无关。
     */
    public Entry peek() {
        return queue.peek();
    }

    public Entry poll() {
        return queue.poll();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public void clear() {
        queue.clear();
    }

    /**
     * sequence 是单调插入序号，只用于打破同一时间戳的次序，避免与 keyHandle 比较耦合。
     */
    public record Entry(long expireAtMillis, long sequence, AllocatorKeyHandle keyHandle)
            implements Comparable<Entry> {
        @Override
        public int compareTo(Entry other) {
            int byTime = Long.compare(expireAtMillis, other.expireAtMillis);
            return byTime != 0 ? byTime : Long.compare(sequence, other.sequence);
        }
    }
}
