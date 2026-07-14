package yier.bubu.redis.execution.api;

/**
 * 回复编码和保留来源在写入前必须取得的上界额度。
 */
public record ReplyPlan(
        long encodedUpperBoundBytes,
        long retainedSourceBytes,
        boolean reserveMaximum
) {
    public ReplyPlan {
        if (encodedUpperBoundBytes < 0L) {
            throw new IllegalArgumentException("encodedUpperBoundBytes must be non-negative");
        }
        if (retainedSourceBytes < 0L) {
            throw new IllegalArgumentException("retainedSourceBytes must be non-negative");
        }
    }

    public static ReplyPlan exact(long encodedUpperBoundBytes, long retainedSourceBytes) {
        return new ReplyPlan(encodedUpperBoundBytes, retainedSourceBytes, false);
    }

    public static ReplyPlan maximum() {
        return new ReplyPlan(0L, 0L, true);
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
