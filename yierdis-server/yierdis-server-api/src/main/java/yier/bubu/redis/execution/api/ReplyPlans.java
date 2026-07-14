package yier.bubu.redis.execution.api;

/**
 * RESP2 形状的常量时间回复额度计算器。
 */
public final class ReplyPlans {
    private ReplyPlans() {
    }

    public static ReplyPlan bulkString(int payloadLength, long retainedSourceBytes) {
        if (payloadLength < -1) {
            throw new IllegalArgumentException("payloadLength must be >= -1");
        }
        requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        if (payloadLength == -1) {
            return ReplyPlan.exact(5L, retainedSourceBytes);
        }
        long headerBytes = saturatedAdd(1L, decimalDigits(payloadLength));
        headerBytes = saturatedAdd(headerBytes, 2L);
        long encoded = saturatedAdd(headerBytes, payloadLength);
        encoded = saturatedAdd(encoded, 2L);
        return ReplyPlan.exact(encoded, retainedSourceBytes);
    }

    public static ReplyPlan bulkStringArray(
            int count,
            long encodedElementBytes,
            long retainedSourceBytes
    ) {
        if (count < -1) {
            throw new IllegalArgumentException("count must be >= -1");
        }
        requireNonNegative(encodedElementBytes, "encodedElementBytes");
        requireNonNegative(retainedSourceBytes, "retainedSourceBytes");
        if (count == -1) {
            return ReplyPlan.exact(5L, retainedSourceBytes);
        }
        long headerBytes = saturatedAdd(1L, decimalDigits(count));
        headerBytes = saturatedAdd(headerBytes, 2L);
        return ReplyPlan.exact(saturatedAdd(headerBytes, encodedElementBytes), retainedSourceBytes);
    }

    public static ReplyPlan raw(long encodedUpperBoundBytes, long retainedSourceBytes) {
        return ReplyPlan.exact(encodedUpperBoundBytes, retainedSourceBytes);
    }

    private static int decimalDigits(int value) {
        if (value < 10) {
            return 1;
        }
        if (value < 100) {
            return 2;
        }
        if (value < 1_000) {
            return 3;
        }
        if (value < 10_000) {
            return 4;
        }
        if (value < 100_000) {
            return 5;
        }
        if (value < 1_000_000) {
            return 6;
        }
        if (value < 10_000_000) {
            return 7;
        }
        if (value < 100_000_000) {
            return 8;
        }
        if (value < 1_000_000_000) {
            return 9;
        }
        return 10;
    }

    private static long saturatedAdd(long left, long right) {
        return ReplyPlan.saturatedAdd(left, right);
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
