package yier.bubu.redis.storage.memory.internal.ledger;

public interface PreparedDbMutation<T> extends PreparedMutation<T> {
    /**
     * 提交并释放旧 native 对象后，是否应尝试回收 allocator 空页。
     * 逻辑账本缩小时默认尝试回收；零 delta 的物理缩容由具体 mutation 显式声明。
     */
    default boolean shouldTrimNativePagesAfterCommit() {
        return actualDeltaBytes() < 0L;
    }
}
