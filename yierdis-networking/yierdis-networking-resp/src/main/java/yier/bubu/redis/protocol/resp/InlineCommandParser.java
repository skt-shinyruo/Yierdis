package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesView;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Redis sdssplitargs-style inline command parser.
 */
public final class InlineCommandParser {
    private InlineCommandParser() {
    }

    static byte[][] parse(byte[] input, int off, int len, int maxArgs) {
        if (maxArgs <= 0) {
            throw new IllegalArgumentException("maxArgs must be > 0");
        }
        Parsed parsed = requireArguments(parseResult(input, off, len));
        if (parsed.argc() > maxArgs) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }
        return parsed.takeArgs();
    }

    public static byte[][] parseUnlimited(byte[] input, int off, int len) {
        return requireArguments(parseResult(input, off, len)).takeArgs();
    }

    /** 校验并统计 inline 命令；空行表示为 argc=0，且在调用 {@link Parsed#takeArgs()} 前不分配 argv。 */
    public static Parsed parseResult(byte[] input, int off, int len) {
        return parseInternal(input, off, len);
    }

    public static boolean isBlank(BytesView input) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        for (int i = 0; i < input.length(); i++) {
            if (!isSpace(input.getByte(i))) {
                return false;
            }
        }
        return true;
    }

    private static Parsed requireArguments(Parsed parsed) {
        if (parsed.argc() == 0) {
            throw new IllegalArgumentException("Protocol error: empty inline command");
        }
        return parsed;
    }

    private static Parsed parseInternal(byte[] input, int off, int len) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (off < 0 || len < 0 || off > input.length - len) {
            throw new IndexOutOfBoundsException();
        }
        byte[] ownedInput = Arrays.copyOfRange(input, off, off + len);
        return parseSyntax(ownedInput, 0, ownedInput.length, null, null);
    }

    private static Parsed parseSyntax(byte[] input, int off, int len, byte[] decoded, byte[][] args) {
        int argc = 0;
        int end = off + len;
        int outPos = 0;

        int p = off;
        while (true) {
            while (p < end && isSpace(input[p])) {
                p++;
            }
            if (p >= end) {
                break;
            }

            boolean inq = false;
            boolean insq = false;
            int tokenStart = outPos;

            while (p < end) {
                byte b = input[p];
                if (inq) {
                    if (b == '\\'
                            && p + 3 < end
                            && input[p + 1] == 'x'
                            && isHexDigit(input[p + 2])
                            && isHexDigit(input[p + 3])) {
                        if (decoded != null) {
                            decoded[outPos] = (byte) ((hexDigitToInt(input[p + 2]) << 4)
                                    | hexDigitToInt(input[p + 3]));
                        }
                        outPos++;
                        p += 4;
                        continue;
                    }
                    if (b == '\\' && p + 1 < end) {
                        p++;
                        if (decoded != null) {
                            decoded[outPos] = decodeEscapedChar(input[p]);
                        }
                        outPos++;
                        p++;
                        continue;
                    }
                    if (b == '"') {
                        if (p + 1 < end && !isSpace(input[p + 1])) {
                            throw new IllegalArgumentException("Protocol error: invalid inline command");
                        }
                        inq = false;
                        p++;
                        break;
                    }
                    if (decoded != null) {
                        decoded[outPos] = b;
                    }
                    outPos++;
                    p++;
                    continue;
                }

                if (insq) {
                    if (b == '\\' && p + 1 < end && input[p + 1] == '\'') {
                        if (decoded != null) {
                            decoded[outPos] = (byte) '\'';
                        }
                        outPos++;
                        p += 2;
                        continue;
                    }
                    if (b == '\'') {
                        if (p + 1 < end && !isSpace(input[p + 1])) {
                            throw new IllegalArgumentException("Protocol error: invalid inline command");
                        }
                        insq = false;
                        p++;
                        break;
                    }
                    if (decoded != null) {
                        decoded[outPos] = b;
                    }
                    outPos++;
                    p++;
                    continue;
                }

                if (isSpace(b)) {
                    break;
                }
                if (b == '"') {
                    inq = true;
                    p++;
                    continue;
                }
                if (b == '\'') {
                    insq = true;
                    p++;
                    continue;
                }
                if (decoded != null) {
                    decoded[outPos] = b;
                }
                outPos++;
                p++;
            }

            if (inq || insq) {
                throw new IllegalArgumentException("Protocol error: unbalanced quotes in inline command");
            }

            int tokenLen = outPos - tokenStart;
            if (args != null) {
                args[argc] = new byte[tokenLen];
                System.arraycopy(decoded, tokenStart, args[argc], 0, tokenLen);
            }
            argc++;
        }

        return new Parsed(input, off, len, argc, outPos);
    }

    public static List<byte[]> splitUtf8(String line, int maxArgs) {
        if (line == null) {
            throw new IllegalArgumentException("line must not be null");
        }
        byte[] input = line.getBytes(StandardCharsets.UTF_8);
        return new ArrayList<>(Arrays.asList(parse(input, 0, input.length, maxArgs)));
    }

    /** 已完成语法校验并持有输入快照的 inline 命令；调用方完成限制检查后再物化 argv。 */
    public static final class Parsed {
        private byte[] input;
        private final int off;
        private final int len;
        private final int argc;
        private final int retainedBytes;

        private Parsed(byte[] input, int off, int len, int argc, int retainedBytes) {
            this.input = input;
            this.off = off;
            this.len = len;
            this.argc = argc;
            this.retainedBytes = retainedBytes;
        }

        public int argc() {
            return argc;
        }

        public int retainedBytes() {
            return retainedBytes;
        }

        /** 将输入快照转换为 argv 并转移所有权；每个解析结果只能调用一次。 */
        public byte[][] takeArgs() {
            if (input == null) {
                throw new IllegalStateException("inline arguments already taken");
            }
            byte[][] args = new byte[argc][];
            parseSyntax(input, off, len, input, args);
            input = null;
            return args;
        }
    }

    private static boolean isSpace(byte b) {
        return b == ' ' || b == '\t';
    }

    private static byte decodeEscapedChar(byte b) {
        return switch (b) {
            case 'n' -> (byte) '\n';
            case 'r' -> (byte) '\r';
            case 't' -> (byte) '\t';
            case 'b' -> (byte) '\b';
            case 'a' -> 7;
            default -> b;
        };
    }

    private static boolean isHexDigit(byte b) {
        return (b >= '0' && b <= '9')
                || (b >= 'a' && b <= 'f')
                || (b >= 'A' && b <= 'F');
    }

    private static int hexDigitToInt(byte b) {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        if (b >= 'a' && b <= 'f') {
            return 10 + (b - 'a');
        }
        if (b >= 'A' && b <= 'F') {
            return 10 + (b - 'A');
        }
        return 0;
    }
}
