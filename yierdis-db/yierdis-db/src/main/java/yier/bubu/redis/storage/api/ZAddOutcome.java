package yier.bubu.redis.storage.api;

/**
 * ZADD 的存储层结果。{@code changed} 是 Redis CH 回复计数（added + updated）；
 * {@code newScore} 只在 INCR 模式且该对未被 NX/XX/GT/LT 阻挡时携带按 Redis
 * 双精度格式渲染的新分数，其余情况为 null。
 */
public record ZAddOutcome(long added, long changed, byte[] newScore) {
    public static ZAddOutcome blocked() {
        return new ZAddOutcome(0L, 0L, null);
    }
}
