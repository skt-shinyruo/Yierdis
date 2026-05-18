package yier.bubu.redis.protocol.resp;

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

    public static Decoded parse(byte[] input, int off, int len, int maxArgs) {
        if (maxArgs <= 0) {
            throw new IllegalArgumentException("maxArgs must be > 0");
        }
        return parseInternal(input, off, len, maxArgs);
    }

    public static Decoded parseUnlimited(byte[] input, int off, int len) {
        return parseInternal(input, off, len, 0);
    }

    private static Decoded parseInternal(byte[] input, int off, int len, int maxArgs) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (off < 0 || len < 0 || off + len > input.length) {
            throw new IndexOutOfBoundsException();
        }

        int[] offsets = new int[16];
        int[] lengths = new int[16];
        int argc = 0;

        int end = off + len;
        byte[] decoded = new byte[len];
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
                        decoded[outPos++] = (byte) ((hexDigitToInt(input[p + 2]) << 4) | hexDigitToInt(input[p + 3]));
                        p += 4;
                        continue;
                    }
                    if (b == '\\' && p + 1 < end) {
                        p++;
                        decoded[outPos++] = decodeEscapedChar(input[p]);
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
                    decoded[outPos++] = b;
                    p++;
                    continue;
                }

                if (insq) {
                    if (b == '\\' && p + 1 < end && input[p + 1] == '\'') {
                        decoded[outPos++] = (byte) '\'';
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
                    decoded[outPos++] = b;
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
                decoded[outPos++] = b;
                p++;
            }

            if (inq || insq) {
                throw new IllegalArgumentException("Protocol error: unbalanced quotes in inline command");
            }

            int tokenLen = outPos - tokenStart;
            if (maxArgs > 0 && argc >= maxArgs) {
                throw new IllegalArgumentException("Protocol error: array length too large");
            }
            if (argc == offsets.length) {
                int next = offsets.length << 1;
                offsets = Arrays.copyOf(offsets, next);
                lengths = Arrays.copyOf(lengths, next);
            }
            offsets[argc] = tokenStart;
            lengths[argc] = tokenLen;
            argc++;
        }

        if (argc == 0) {
            throw new IllegalArgumentException("Protocol error: empty inline command");
        }
        return new Decoded(decoded, outPos, argc, offsets, lengths);
    }

    public static List<byte[]> splitUtf8(String line, int maxArgs) {
        if (line == null) {
            throw new IllegalArgumentException("line must not be null");
        }
        byte[] input = line.getBytes(StandardCharsets.UTF_8);
        Decoded decoded = parse(input, 0, input.length, maxArgs);
        List<byte[]> out = new ArrayList<>(decoded.argc());
        for (int i = 0; i < decoded.argc(); i++) {
            out.add(decoded.copyArg(i));
        }
        return out;
    }

    public static final class Decoded {
        private final byte[] decoded;
        private final int decodedLen;
        private final int argc;
        private final int[] offsets;
        private final int[] lengths;

        private Decoded(byte[] decoded, int decodedLen, int argc, int[] offsets, int[] lengths) {
            this.decoded = decoded;
            this.decodedLen = decodedLen;
            this.argc = argc;
            this.offsets = offsets;
            this.lengths = lengths;
        }

        public byte[] decoded() {
            return decoded;
        }

        public int decodedLen() {
            return decodedLen;
        }

        public int argc() {
            return argc;
        }

        public int offset(int arg) {
            checkArg(arg);
            return offsets[arg];
        }

        public int length(int arg) {
            checkArg(arg);
            return lengths[arg];
        }

        public int retainedBytes() {
            return decodedLen;
        }

        public byte[] copyArg(int arg) {
            checkArg(arg);
            int start = offsets[arg];
            return Arrays.copyOfRange(decoded, start, start + lengths[arg]);
        }

        public byte[][] copyArgs() {
            byte[][] out = new byte[argc][];
            for (int i = 0; i < argc; i++) {
                out[i] = copyArg(i);
            }
            return out;
        }

        private void checkArg(int arg) {
            if (arg < 0 || arg >= argc) {
                throw new IndexOutOfBoundsException();
            }
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
