package yier.bubu.redis.execution.api;

/**
 * 回复编码和保留来源在写入前必须取得的上界额度。
 *
 * <p>{@link #protocolVersion()} 是 sizing（prepare/预留）时刻捕获的 RESP 协议版本 wire value：
 * 容量按它计算，writer 渲染同一条回复时必须使用这同一份捕获值，不允许在写出时再次读取版本。</p>
 */
public record ReplyPlan(
        long encodedUpperBoundBytes,
        long retainedSourceBytes,
        boolean reserveMaximum,
        int protocolVersion
) {
    public ReplyPlan {
        if (encodedUpperBoundBytes < 0L) {
            throw new IllegalArgumentException("encodedUpperBoundBytes must be non-negative");
        }
        if (retainedSourceBytes < 0L) {
            throw new IllegalArgumentException("retainedSourceBytes must be non-negative");
        }
    }

    public static ReplyPlan exact(long encodedUpperBoundBytes, long retainedSourceBytes, int protocolVersion) {
        return new ReplyPlan(encodedUpperBoundBytes, retainedSourceBytes, false, protocolVersion);
    }

    public static ReplyPlan maximum(int protocolVersion) {
        return new ReplyPlan(0L, 0L, true, protocolVersion);
    }

    public long totalUpperBoundBytes() {
        return saturatedAdd(encodedUpperBoundBytes, retainedSourceBytes);
    }

    static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
