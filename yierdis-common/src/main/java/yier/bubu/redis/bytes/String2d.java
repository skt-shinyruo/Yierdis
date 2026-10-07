package yier.bubu.redis.bytes;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Redis 8.0.2 {@code string2d} 里 ZADD 分数用到的那一部分：拒绝首尾空白、{@code d}/{@code f}
 * 后缀、十进制字面量上溢成无穷、下溢成 0，同时接受任意大小写的 {@code inf}/{@code infinity}
 * 和不带二进制指数的十六进制（{@code 0x10}、{@code 0x1.8}）。
 * <p>
 * {@code ZRANGEBYSCORE} 的 min/max 不走这里。边界拒绝全部十六进制，上溢则收成无穷。
 */
public final class String2d {
    private String2d() {
    }

    /**
     * 按 ZADD 分数的 {@code string2d} 规则解析。
     *
     * @throws NumberFormatException 输入为 null、为空，或不是该方言接受的分数时抛出
     */
    public static double parse(byte[] raw) {
        if (raw == null || raw.length == 0 || hasSurroundingCSpace(raw)) {
            throw invalid();
        }
        String text = new String(raw, StandardCharsets.US_ASCII);
        // parseDouble 只认 "Infinity"。inf/infinity 的其余拼写要先认出来，不能和 1e309 的上溢混在一起。
        if (isInfinitySpelling(text)) {
            return text.toLowerCase(Locale.ROOT).startsWith("-")
                    ? Double.NEGATIVE_INFINITY
                    : Double.POSITIVE_INFINITY;
        }
        if (isHexLiteral(text)) {
            return parseHex(text);
        }
        // JDK 的 parseDouble 把 1d/1f 当成 Java 浮点后缀。string2d 没有这个后缀。
        if (hasJavaFloatSuffix(text)) {
            throw invalid();
        }
        double value;
        try {
            value = Double.parseDouble(text);
        } catch (NumberFormatException failure) {
            throw invalid();
        }
        // JDK 25 的 parseDouble 把 1e309 收成无穷、把 1e-400 收成 0，且都不抛异常。
        // string2d 只在 strtod 设了 ERANGE 时拒绝这两类结果；次正规数（如 1e-323）结果非 0，仍然合法。
        if (!Double.isFinite(value) || decimalLiteralUnderflowedToZero(text, value)) {
            throw invalid();
        }
        return value;
    }

    // C isspace：空格、TAB、LF、VT、FF、CR。JDK 25 的 parseDouble 会剥掉首尾这些字符。
    private static boolean hasSurroundingCSpace(byte[] raw) {
        return isCSpace(raw[0]) || isCSpace(raw[raw.length - 1]);
    }

    private static boolean isCSpace(byte value) {
        return value == ' ' || value == '\t' || value == '\n' || value == '\r' || value == '\f' || value == 0x0b;
    }

    private static boolean isHexLiteral(String text) {
        int index = (text.charAt(0) == '+' || text.charAt(0) == '-') ? 1 : 0;
        return text.length() >= index + 2
                && text.charAt(index) == '0'
                && (text.charAt(index + 1) == 'x' || text.charAt(index + 1) == 'X');
    }

    // parseDouble 要求十六进制带 p 指数。C strtod 把缺省指数当成 0，所以 0x10 是 16 而不是错误。
    private static double parseHex(String text) {
        String normalized = hasBinaryExponent(text) ? text : text + "p0";
        double value;
        try {
            value = Double.parseDouble(normalized);
        } catch (NumberFormatException failure) {
            throw invalid();
        }
        if (!Double.isFinite(value) || hexLiteralUnderflowedToZero(text, value)) {
            throw invalid();
        }
        return value;
    }

    private static boolean hasBinaryExponent(String text) {
        int index = 0;
        if (text.charAt(0) == '+' || text.charAt(0) == '-') {
            index++;
        }
        index += 2;
        for (int cursor = index; cursor < text.length(); cursor++) {
            char current = text.charAt(cursor);
            if (current == 'p' || current == 'P') {
                return true;
            }
        }
        return false;
    }

    private static boolean hexLiteralUnderflowedToZero(String text, double value) {
        if (value != 0.0d) {
            return false;
        }
        int index = (text.charAt(0) == '+' || text.charAt(0) == '-') ? 1 : 0;
        index += 2;
        boolean nonzero = false;
        for (; index < text.length(); index++) {
            char current = text.charAt(index);
            if (current == 'p' || current == 'P') {
                break;
            }
            if (current == '.') {
                continue;
            }
            int digit = Character.digit(current, 16);
            if (digit < 0) {
                return false;
            }
            if (digit != 0) {
                nonzero = true;
            }
        }
        return nonzero;
    }

    private static boolean hasJavaFloatSuffix(String text) {
        char last = text.charAt(text.length() - 1);
        return last == 'd' || last == 'D' || last == 'f' || last == 'F';
    }

    private static boolean isInfinitySpelling(String text) {
        String lowered = text.toLowerCase(Locale.ROOT);
        boolean negative = lowered.startsWith("-");
        String body = (lowered.startsWith("+") || negative) ? lowered.substring(1) : lowered;
        return body.equals("inf") || body.equals("infinity");
    }

    // 精确零（0、-0、0e-400）的有效数字全是 0，strtod 不设 ERANGE。有非零有效数字却变成 ±0 才是下溢。
    // 扫不到完整十进制字面量时返回 false，避免把 0x0p0 这类 parseDouble 能接受的零误判成下溢。
    private static boolean decimalLiteralUnderflowedToZero(String text, double value) {
        if (value != 0.0d) {
            return false;
        }
        int index = 0;
        int length = text.length();
        if (index < length && (text.charAt(index) == '+' || text.charAt(index) == '-')) {
            index++;
        }
        boolean sawDigit = false;
        boolean nonzero = false;
        boolean sawDot = false;
        while (index < length) {
            char current = text.charAt(index);
            if (current == '.') {
                if (sawDot) {
                    return false;
                }
                sawDot = true;
                index++;
                continue;
            }
            if (current == 'e' || current == 'E') {
                break;
            }
            if (current < '0' || current > '9') {
                return false;
            }
            sawDigit = true;
            if (current != '0') {
                nonzero = true;
            }
            index++;
        }
        if (!sawDigit) {
            return false;
        }
        if (index < length) {
            index++;
            if (index < length && (text.charAt(index) == '+' || text.charAt(index) == '-')) {
                index++;
            }
            int exponentStart = index;
            while (index < length && text.charAt(index) >= '0' && text.charAt(index) <= '9') {
                index++;
            }
            if (index == exponentStart || index != length) {
                return false;
            }
        }
        return nonzero;
    }

    private static NumberFormatException invalid() {
        return new NumberFormatException("not a string2d float");
    }
}
