package yier.bubu.redis.bytes;

/**
 * Redis {@code string2ll} 方言的严格整数解析：只接受规范十进制表示，拒绝 {@code '+'} 前缀、
 * 前导零（{@code "0"} 本身除外，{@code "-0"} 同样拒绝）、空白与溢出。
 * 命令整数参数与已存 string 的 INCR 解析共用此实现，保证整个服务只有一种整数方言。
 */
public final class String2ll {
    private String2ll() {
    }

    /**
     * 按 Redis {@code string2ll} 规则解析整数。
     *
     * @throws NumberFormatException 输入为 null、为空或不是规范十进制整数（含溢出）时抛出
     */
    public static long parse(byte[] value) {
        if (value == null || value.length == 0) {
            throw notAnInteger();
        }

        int index = 0;
        boolean negative = false;
        if (value[0] == '-') {
            negative = true;
            index = 1;
            if (index == value.length) {
                throw notAnInteger();
            }
        }
        // 除单独的 "0" 外不允许前导零，"-0" 也在此拒绝；'+' 等非数字字符由下面的逐位数字校验拒绝。
        if (value[index] == '0' && (negative || value.length - index > 1)) {
            throw notAnInteger();
        }

        // 以负数累加并逐位检查溢出，使 Long.MIN_VALUE 可表示而 Long.MAX_VALUE + 1 被拒绝。
        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;
        while (index < value.length) {
            int digit = value[index++] - '0';
            if (digit < 0 || digit > 9) {
                throw notAnInteger();
            }
            if (result < multMin) {
                throw notAnInteger();
            }
            result *= 10;
            if (result < limit + digit) {
                throw notAnInteger();
            }
            result -= digit;
        }
        return negative ? result : -result;
    }

    private static NumberFormatException notAnInteger() {
        return new NumberFormatException("not a canonical string2ll integer");
    }
}
