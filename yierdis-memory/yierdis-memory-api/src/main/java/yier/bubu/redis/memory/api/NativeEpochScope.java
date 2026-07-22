package yier.bubu.redis.memory.api;

/**
 * 表示后端回收屏障中的活动 epoch。
 * 关闭前，该 epoch 保护的退役存储不得回收；关闭只撤销屏障，不保证立即回收。
 * scope 沿用后端 owner，必须在后端前关闭，重复关闭无效果。
 */
public interface NativeEpochScope extends AutoCloseable {
    NativeEpochKind kind();

    long epoch();

    @Override
    void close();
}
