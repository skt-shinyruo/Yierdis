package yier.bubu.redis.memory.api;

/**
 * 表示句柄所属后端与接收操作的后端不一致。
 * expected/actual 身份作为结构化契约公开，调用方无需解析异常消息。
 */
public final class NativeHandleOwnershipException extends NativeMemoryException {
    private final long expectedAllocatorId;
    private final long actualAllocatorId;

    public NativeHandleOwnershipException(long expected, long actual) {
        super("native handle belongs to allocator " + actual + ", expected " + expected);
        this.expectedAllocatorId = expected;
        this.actualAllocatorId = actual;
    }

    /** 返回接收本次操作的后端身份。 */
    public long expectedAllocatorId() {
        return expectedAllocatorId;
    }

    /** 返回句柄携带的所属后端身份。 */
    public long actualAllocatorId() {
        return actualAllocatorId;
    }
}
