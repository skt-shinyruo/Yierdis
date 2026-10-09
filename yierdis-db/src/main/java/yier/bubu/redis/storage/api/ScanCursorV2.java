package yier.bubu.redis.storage.api;

import java.nio.charset.StandardCharsets;

/**
 * Redis SCAN 系列命令的游标。
 * <p>
 * 兼容 Redis 生态约定：游标仍以“数字字符串”的 bulk string 形式传输；当返回值为 {@code 0} 时表示扫描结束。
 * <p>
 * 线上语义：游标是哈希空间中的不透明位置（与 Redis {@code dictScan} 的反向二进制递增游标同族），
 * 在表扩容、缩容或 rehash 完成后仍可映射到新表对应位置继续扫描。目标是“可推进、可终止”：
 * 完整迭代在有限步内回到 {@code 0}；全程存在的元素至少返回一次。
 *
 * <p>cursor 对客户端是不透明非负整数：{@link #of(long)} 接受任意非负值，绝不因客户端输入抛出异常；
 * 无法映射到当前哈希空间的 cursor（例如超出 32 位）由存储层按“从头重启迭代”处理（允许重复，结束仍回 0）。</p>
 *
 * <p>{@link #of(int, int, long)} / {@link #generation()} / {@link #phase()} / {@link #position()}
 * 保留对历史“代数 + 阶段 + 物理槽位”打包格式的编解码能力，便于测试与兼容旧 token；
 * 当前 SCAN 实现不再把 generation/phase 写入线上游标。</p>
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

    public byte[] toAsciiBytes() {
        if (value == 0L) {
            return ZERO_ASCII;
        }
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }
}
