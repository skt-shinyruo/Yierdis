package yier.bubu.redis.command.defaults;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.OptionalLong;
import java.util.Set;
import yier.bubu.redis.bytes.String2ll;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;

/**
 * SCAN、HSCAN、SSCAN、ZSCAN 的 cursor 与 option。四条命令共用 Redis {@code scanGenericCommand}
 * 的同一套规则和错误文案，只在 {@code TYPE}、{@code NOVALUES} 能用在哪条命令上有区别。
 */
public record ScanArguments(
        ScanCursorV2 cursor,
        byte[] match,
        int count,
        Set<ValueType> types,
        boolean noValues
) {
    private static final int DEFAULT_COUNT = 10;
    private static final long U64_MAX_DIV_10 = Long.divideUnsigned(-1L, 10L);
    private static final int U64_MAX_LAST_DIGIT = (int) Long.remainderUnsigned(-1L, 10L);
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INVALID_CURSOR = "ERR invalid cursor";
    private static final String NOVALUES_ERROR = "ERR NOVALUES option can only be used in HSCAN";

    public enum Target {
        KEYSPACE,
        HASH,
        MEMBERS
    }

    public static ScanArguments parse(CommandArgs args, int cursorIndex, Target target) {
        // 和 Redis 一样先校验 cursor：cursor 非法时后面的 option 错误不会被报告。
        ScanCursorV2 cursor = cursor(args, cursorIndex);
        byte[] match = null;
        int count = DEFAULT_COUNT;
        Set<ValueType> types = EnumSet.allOf(ValueType.class);
        boolean noValues = false;
        // 逐个 option 从左到右解析，第一个失败的 option 决定错误文案。带值的 option 缺值时
        // 不单独报错，而是落到最后的 syntax error，与 Redis 的 j >= 2 判断一致。
        int index = cursorIndex + 1;
        while (index < args.argc()) {
            boolean hasValue = index + 1 < args.argc();
            if (hasValue && args.is(index, "COUNT")) {
                long parsed = args.longAt(index + 1);
                if (parsed < 1) {
                    throw syntaxFailure();
                }
                // Redis 接受到 LONG_MAX；COUNT 只是工作量 hint，存储层本来就有上限，钳到 int 不改变结果。
                count = (int) Math.min(parsed, Integer.MAX_VALUE);
                index += 2;
            } else if (hasValue && args.is(index, "MATCH")) {
                match = args.bytes(index + 1);
                index += 2;
            } else if (hasValue && target == Target.KEYSPACE && args.is(index, "TYPE")) {
                types = typesNamed(args, index + 1);
                index += 2;
            } else if (args.is(index, "NOVALUES")) {
                if (target != Target.HASH) {
                    throw new CommandParseException(NOVALUES_ERROR);
                }
                noValues = true;
                index++;
            } else {
                throw syntaxFailure();
            }
        }
        return new ScanArguments(cursor, match, count, types, noValues);
    }

    // Redis 8 对未知类型名不报错，只是让这次 SCAN 不匹配任何 key；stream 这类 Yierdis 不存储的
    // Redis 类型也落在这里。类型名比较不分大小写，与 TYPE 命令回复的小写名字一一对应。
    private static Set<ValueType> typesNamed(CommandArgs args, int index) {
        for (ValueType type : ValueType.values()) {
            if (args.is(index, type.name())) {
                return EnumSet.of(type);
            }
        }
        return EnumSet.noneOf(ValueType.class);
    }

    /**
     * 按 Redis {@code parseScanCursorOrReply} 解析 cursor：参数按 C 字符串处理（第一个 NUL 之后不算），
     * 先用 {@code string2ll}，能解析出的负数直接拒绝；解析不了再退回 {@code strtoull}。
     */
    private static ScanCursorV2 cursor(CommandArgs args, int index) {
        byte[] text = untilFirstNul(args.bytes(index));
        long unsigned;
        OptionalLong signed = String2ll.tryParse(text);
        if (signed.isPresent()) {
            if (signed.getAsLong() < 0L) {
                throw invalidCursor();
            }
            unsigned = signed.getAsLong();
        } else {
            unsigned = strtoull(text);
        }
        // Yierdis 发出的 cursor 只占低 63 位，超过 Long.MAX_VALUE 的 u64 不可能是本实例发出的。
        // 钳到 Long.MAX_VALUE 后 phase 位是 3（发出的只有 0/1），存储层会按无法映射的 cursor 从头重启。
        return ScanCursorV2.of(unsigned < 0L ? Long.MAX_VALUE : unsigned);
    }

    // glibc strtoull(base 10) 的子集：跳过前导空白，接受一个 '+' 或 '-'，其后到结尾都必须是数字；
    // 超出 u64 视为 ERANGE。'-' 按 C 语义对 2^64 取模，所以 "-1" 这类写法在这一步会回绕成大数。
    private static long strtoull(byte[] text) {
        int index = 0;
        while (index < text.length && isCSpace(text[index])) {
            index++;
        }
        boolean negative = false;
        if (index < text.length && (text[index] == '+' || text[index] == '-')) {
            negative = text[index] == '-';
            index++;
        }
        if (index == text.length) {
            throw invalidCursor();
        }
        long value = 0L;
        for (; index < text.length; index++) {
            int digit = text[index] - '0';
            if (digit < 0 || digit > 9) {
                throw invalidCursor();
            }
            int compared = Long.compareUnsigned(value, U64_MAX_DIV_10);
            if (compared > 0 || (compared == 0 && digit > U64_MAX_LAST_DIGIT)) {
                throw invalidCursor();
            }
            value = value * 10L + digit;
        }
        return negative ? -value : value;
    }

    private static boolean isCSpace(byte value) {
        return value == ' ' || (value >= '\t' && value <= '\r');
    }

    private static byte[] untilFirstNul(byte[] value) {
        if (value == null) {
            throw invalidCursor();
        }
        for (int index = 0; index < value.length; index++) {
            if (value[index] == 0) {
                return Arrays.copyOf(value, index);
            }
        }
        return value;
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }

    private static CommandParseException invalidCursor() {
        return new CommandParseException(INVALID_CURSOR);
    }
}
