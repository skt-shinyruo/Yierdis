package yier.bubu.redis.storage.api;

/**
 * ZADD 的条件标志（Redis 3.0.2/6.2 语义）。{@code incr} 表示 INCR 增量模式；
 * CH 只影响回复计数形状，不改变存储行为，因此不在这里出现。
 */
public record ZAddOptions(boolean nx, boolean xx, boolean gt, boolean lt, boolean incr) {
    public static ZAddOptions plain() {
        return new ZAddOptions(false, false, false, false, false);
    }
}
