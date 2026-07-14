package yier.bubu.redis.storage.api;

import java.nio.charset.StandardCharsets;

/**
 * Redis SCAN 游标 v2（rehash-aware）。
 * <p>
 * 兼容 Redis 生态约定：游标仍以“数字字符串”的 bulk string 形式传输；当返回值为 {@code 0} 时表示扫描结束。
 * <p>
 * v2 语义：
 * <ul>
 *   <li>在 keyspace rehash 的“双表期”，游标会携带 table generation、phase 与 slot position。</li>
 *   <li>该游标用于 best-effort 增量遍历，不提供强一致保证；目标是“可推进、可终止”。</li>
 * </ul>
 *
 * <p>29 位 generation 有限：只有完整迭代跨越的结构代数少于 {@code 2^29} 时，才保证不会遗漏
 * 全程存在的 key。该 token 不是可跨数据库生命周期保存的书签。</p>
 */
public final class ScanCursorV2 {
    private static final byte[] ZERO_ASCII = "0".getBytes(StandardCharsets.US_ASCII);

    private static final int POSITION_BITS = 32;
    private static final int PHASE_BITS = 2;
    private static final int GENERATION_BITS = 29;
    private static final int PHASE_SHIFT = POSITION_BITS;
    private static final int GENERATION_SHIFT = PHASE_SHIFT + PHASE_BITS;
    private static final long POSITION_MASK = (1L << POSITION_BITS) - 1L;
    private static final int PHASE_MASK = (1 << PHASE_BITS) - 1;
    private static final int GENERATION_MASK = (1 << GENERATION_BITS) - 1;

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
        int phase = (int) ((value >>> PHASE_SHIFT) & PHASE_MASK);
        if (phase > 1) {
            throw new IllegalArgumentException("cursor phase must be 0 or 1");
        }
        return value == 0L ? start() : new ScanCursorV2(value);
    }

    public static ScanCursorV2 of(int generation, int phase, long position) {
        if (generation < 0 || generation > GENERATION_MASK) {
            throw new IllegalArgumentException("cursor generation must be between 0 and " + GENERATION_MASK);
        }
        if (phase < 0 || phase > 1) {
            throw new IllegalArgumentException("cursor phase must be 0 or 1");
        }
        if (position < 0L || position > POSITION_MASK) {
            throw new IllegalArgumentException("cursor position must be between 0 and " + POSITION_MASK);
        }
        long value = ((long) generation << GENERATION_SHIFT) | ((long) phase << PHASE_SHIFT) | position;
        return value == 0L ? start() : new ScanCursorV2(value);
    }

    public static ScanCursorV2 ofPhaseAndPosition(int phase, long position) {
        return of(0, phase, position);
    }

    public long value() {
        return value;
    }

    public int phase() {
        return (int) ((value >>> PHASE_SHIFT) & PHASE_MASK);
    }

    public long position() {
        return value & POSITION_MASK;
    }

    public int generation() {
        return (int) ((value >>> GENERATION_SHIFT) & GENERATION_MASK);
    }

    public byte[] toBulkStringAscii() {
        if (value == 0L) {
            return ZERO_ASCII;
        }
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }
}
