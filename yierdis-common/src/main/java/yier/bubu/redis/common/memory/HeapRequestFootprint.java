package yier.bubu.redis.common.memory;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.Objects;

/**
 * heap-backed request snapshot 的 retained bytes 统一估算口径。
 * <p>
 * executor queued bytes、连接 pending bytes 与事务 queue bytes 都消费 {@code ExecutionRequest#retainedBytes()}，
 * 因此 RESP array path、inline path 与 {@code ByteArrayExecutionRequest} 各工厂方法必须共享这里的估算逻辑，
 * 而不是各自按 payload 长度求和。估算覆盖：请求对象本身、外层 {@code byte[][]} 与其引用槽位、
 * 每个非空 {@code byte[]} 的数组头与按 8 对齐的 payload。
 */
public final class HeapRequestFootprint {
    private static final int REQUEST_FIXED_BYTES = 32;
    private static final int OUTER_ARGV_BYTES = 16;
    private static final int REFERENCE_BYTES = 8;
    private static final int ARRAY_HEADER_BYTES = 16;

    private HeapRequestFootprint() {
    }

    /** 整棵 argv 对象图的估算字节数；long 域饱和，供准入计费使用。 */
    public static long estimateBytes(byte[][] argv) {
        Objects.requireNonNull(argv, "argv");
        long total = addSaturating(OUTER_ARGV_BYTES + REQUEST_FIXED_BYTES, (long) argv.length * REFERENCE_BYTES);
        for (byte[] arg : argv) {
            if (arg != null) {
                total = addSaturating(total, ARRAY_HEADER_BYTES + align8(arg.length));
            }
        }
        return total;
    }

    /** {@code ExecutionRequest#retainedBytes()} 口径的 int 域饱和估算。 */
    public static int estimateRetainedBytes(byte[][] argv) {
        long estimate = estimateBytes(argv);
        return estimate >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estimate;
    }

    /** 请求对象自身的固定开销；decoder 准入计费与 retainedBytes 必须共用这里的常量，避免两套模型漂移。 */
    public static long requestFixedBytes() {
        return REQUEST_FIXED_BYTES;
    }

    /** 外层 argv 数组头与全部引用槽位的开销；long 域饱和，供准入计费使用。 */
    public static long outerArgvBytes(int argc) {
        return addSaturating(OUTER_ARGV_BYTES, saturatedMultiply(Math.max(0, argc), REFERENCE_BYTES));
    }

    /** 单个非空参数的数组头与 8 对齐 payload；long 域饱和，供准入计费使用。 */
    public static long argumentBytes(int argLength) {
        return addSaturating(ARRAY_HEADER_BYTES, align8(Math.max(0, argLength)));
    }

    /** 流式解析在拿到 argc 时的起始 retained bytes：请求对象 + 外层数组头 + 全部引用槽位。 */
    public static int baseRetainedBytes(int argc) {
        long base = (long) OUTER_ARGV_BYTES + REQUEST_FIXED_BYTES + (long) Math.max(0, argc) * REFERENCE_BYTES;
        return base >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) base;
    }

    /** 单个非空参数的 retained bytes：数组头 + 8 对齐后的 payload。 */
    public static int argumentRetainedBytes(int argLength) {
        long bytes = ARRAY_HEADER_BYTES + align8(Math.max(0, argLength));
        return bytes >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) bytes;
    }

    /** int 域饱和累加；解析路径逐参数累计时保持非负且不回绕。 */
    public static int addRetainedBytes(int current, int addend) {
        long next = (long) Math.max(0, current) + Math.max(0, addend);
        return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
    }

    private static long align8(long length) {
        if (length <= 0L) {
            return 0L;
        }
        return length > Long.MAX_VALUE - 7L ? Long.MAX_VALUE : (length + 7L) & ~7L;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }
}
