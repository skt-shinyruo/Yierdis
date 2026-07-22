package yier.bubu.redis.memory.api;

/**
 * 分配进程范围内的稳定内存后端身份。
 * 返回值始终为正且从不复用；可用正值耗尽后，当前进程中的后续分配永久失败。
 */
public final class StableMemoryBackendIds {
    private static final StableMemoryBackendIdSequence PROCESS_IDS =
            new StableMemoryBackendIdSequence(1L);

    private StableMemoryBackendIds() {
    }

    /** 返回新的进程唯一身份；身份空间耗尽时抛出 {@link IllegalStateException}。 */
    public static long nextId() {
        return PROCESS_IDS.nextId();
    }
}
