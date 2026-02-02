package yier.bubu.redis.db;

import java.nio.charset.StandardCharsets;

/**
 * Redis SCAN 游标的最小抽象（best-effort）。
 * <p>
 * Redis 生态约定：游标以“数字字符串”的 bulk string 形式返回；当返回值为 {@code 0} 时表示扫描结束。
 * <p>
 * 注意：该游标仅用于“增量遍历 keyspace”的交互语义，不保证在并发写入/过期清理等情况下提供强一致结果。
 */
public final class ScanCursor {
    private static final byte[] ZERO_ASCII = "0".getBytes(StandardCharsets.US_ASCII);

    private final long value;

    private ScanCursor(long value) {
        this.value = value;
    }

    public static ScanCursor start() {
        return new ScanCursor(0L);
    }

    public static ScanCursor of(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("cursor must be >= 0");
        }
        return value == 0L ? start() : new ScanCursor(value);
    }

    public long value() {
        return value;
    }

    public byte[] toBulkStringAscii() {
        if (value == 0L) {
            return ZERO_ASCII;
        }
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }
}
