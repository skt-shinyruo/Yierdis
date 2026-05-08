package yier.bubu.redis.storage.api;

import java.nio.charset.StandardCharsets;

/**
 * Redis SCAN 游标 v2（rehash-aware）。
 * <p>
 * 兼容 Redis 生态约定：游标仍以“数字字符串”的 bulk string 形式传输；当返回值为 {@code 0} 时表示扫描结束。
 * <p>
 * v2 语义：
 * <ul>
 *   <li>在 keyspace rehash 的“双表期”，游标会携带 table phase（table0/table1）与 slot position。</li>
 *   <li>该游标用于 best-effort 增量遍历，不提供强一致保证；目标是“可推进、可终止”。</li>
 * </ul>
 */
public final class ScanCursorV2 {
    private static final byte[] ZERO_ASCII = "0".getBytes(StandardCharsets.US_ASCII);

    // [phase:2 bits][pos:62 bits]
    static final int PHASE_SHIFT = 62;
    static final long POS_MASK = (1L << PHASE_SHIFT) - 1L;

    private final long value;

    private ScanCursorV2(long value) {
        this.value = value;
    }

    public static ScanCursorV2 start() {
        return new ScanCursorV2(0L);
    }

    public static ScanCursorV2 of(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("cursor must be >= 0");
        }
        return value == 0L ? start() : new ScanCursorV2(value);
    }

    public static ScanCursorV2 ofPhaseAndPosition(int phase, long position) {
        int p = phase & 0b11;
        long pos = position < 0 ? 0 : position;
        if (pos > POS_MASK) {
            pos = POS_MASK;
        }
        long v = (((long) p) << PHASE_SHIFT) | pos;
        return of(v);
    }

    public long value() {
        return value;
    }

    public int phase() {
        return (int) ((value >>> PHASE_SHIFT) & 0b11);
    }

    public long position() {
        return value & POS_MASK;
    }

    public byte[] toBulkStringAscii() {
        if (value == 0L) {
            return ZERO_ASCII;
        }
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }
}
