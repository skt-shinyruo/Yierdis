package yier.bubu.redis.memory.api;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 分配进程范围内的稳定内存后端身份。
 * 返回值始终为正且从不复用；可用正值耗尽后，当前进程中的后续分配永久失败。
 */
public final class StableMemoryBackendIds {
    private static final AtomicLong PROCESS_IDS = new AtomicLong(1L);

    private StableMemoryBackendIds() {
    }

    /** 返回新的进程唯一身份；身份空间耗尽时抛出 {@link IllegalStateException}。 */
    public static long nextId() {
        long id = PROCESS_IDS.getAndUpdate(StableMemoryBackendIds::advance);
        if (id <= 0L) {
            throw new IllegalStateException("stable memory backend IDs are exhausted");
        }
        return id;
    }

    static long advance(long current) {
        return current <= 0L || current == Long.MAX_VALUE ? 0L : current + 1L;
    }
}
