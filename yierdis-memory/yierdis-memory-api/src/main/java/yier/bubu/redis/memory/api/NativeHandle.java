package yier.bubu.redis.memory.api;

/**
 * 标识由某个 {@link StableMemoryBackend} 拥有的稳定内存对象。
 * {@code allocatorId} 是后端身份，{@code localRaw} 是只能由所属后端解释的不透明局部值；
 * 调用方不得解码该值，也不得把句柄交给其他后端。重分配和整理搬迁不改变句柄身份，
 * 释放对象后句柄失效；仅当两个分量都为 {@code 0} 时表示空句柄。
 */
public record NativeHandle(long allocatorId, long localRaw) {
    public static final NativeHandle NULL = new NativeHandle(0L, 0L);

    public boolean isNull() {
        return allocatorId == 0L && localRaw == 0L;
    }
}
