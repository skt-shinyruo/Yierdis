package yier.bubu.redis.protocol.resp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class RespClientCodec {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final byte[] NULL_BULK_STRING = new byte[]{'$', '-', '1', '\r', '\n'};
    private static final ThreadLocal<byte[]> INT_BUF = ThreadLocal.withInitial(() -> new byte[20]);

    private RespClientCodec() {
    }

    public static byte[] encodeCommand(List<byte[]> args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            writeCommand(out, args);
        } catch (IOException e) {
            throw new IllegalStateException("ByteArrayOutputStream should not fail", e);
        }
        return out.toByteArray();
    }

    public static void writeCommand(OutputStream out, List<byte[]> args) throws IOException {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(args, "args");
        out.write('*');
        writeNonNegativeInt(out, args.size());
        out.write(CRLF);
        for (int i = 0; i < args.size(); i++) {
            byte[] arg = args.get(i);
            if (arg == null) {
                out.write(NULL_BULK_STRING);
                continue;
            }
            out.write('$');
            writeNonNegativeInt(out, arg.length);
            out.write(CRLF);
            out.write(arg);
            out.write(CRLF);
        }
    }

    public static RespReply readReply(InputStream in, int maxBulkBytes) throws IOException {
        return readReply(in, maxBulkBytes, false);
    }

    /**
     * 读完整帧，simple string 和 error 不解码为 {@code text}，正文保存在 {@link RespReply#bytes()} 中。
     * 调用方可在帧读完后严格解码 UTF-8，解码失败不会留下尚未读取的数组元素。
     */
    public static RespReply readReplyWithRawText(InputStream in, int maxBulkBytes) throws IOException {
        return readReply(in, maxBulkBytes, true);
    }

    private static RespReply readReply(InputStream in, int maxBulkBytes, boolean rawText) throws IOException {
        Objects.requireNonNull(in, "in");
        if (maxBulkBytes < 0) {
            throw new IllegalArgumentException("maxBulkBytes must be >= 0");
        }
        int type = in.read();
        if (type < 0) {
            throw new IOException("unexpected EOF before RESP reply");
        }
        return switch (type) {
            case '+' -> readText(in, maxBulkBytes, RespReply.Kind.SIMPLE_STRING, rawText);
            case '-' -> readText(in, maxBulkBytes, RespReply.Kind.ERROR, rawText);
            case ':' -> new RespReply(RespReply.Kind.INTEGER, null, null, readLongLine(in, "integer"), null);
            case '$' -> readBulkString(in, maxBulkBytes);
            case '*' -> readAggregate(in, maxBulkBytes, RespReply.Kind.ARRAY, "array", false, rawText);
            case '%' -> readAggregate(in, maxBulkBytes, RespReply.Kind.MAP, "map", true, rawText);
            case '~' -> readAggregate(in, maxBulkBytes, RespReply.Kind.SET, "set", false, rawText);
            case '_' -> {
                expectEmptyLine(in);
                // `_` 不是 RESP2 的 $-1 / *-1。单独成类，调用方才能关掉连接，而不是把它当成 null。
                yield new RespReply(RespReply.Kind.NULL_TYPE, null, null, null, null);
            }
            default -> throw new IOException("unexpected RESP reply type: " + (char) type);
        };
    }

    private static RespReply readBulkString(InputStream in, int maxBulkBytes) throws IOException {
        int len = readLengthLine(in, "bulk string");
        if (len < 0) {
            return new RespReply(RespReply.Kind.NULL, null, null, null, null);
        }
        if (len > maxBulkBytes) {
            throw new IOException("RESP bulk string exceeds limit: " + len);
        }
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new IOException("unexpected EOF in RESP bulk string");
        }
        expectCrlf(in);
        return new RespReply(RespReply.Kind.BULK_STRING, null, bytes, null, null);
    }

    private static RespReply readAggregate(
            InputStream in,
            int maxBulkBytes,
            RespReply.Kind kind,
            String type,
            boolean map,
            boolean rawText
    ) throws IOException {
        int count = readLengthLine(in, type);
        if (count < 0) {
            return new RespReply(RespReply.Kind.NULL, null, null, null, null);
        }
        long valueCount = map ? Math.multiplyExact((long) count, 2L) : count;
        if (valueCount > Integer.MAX_VALUE || valueCount > maxBulkBytes) {
            throw new IOException("invalid RESP " + type + " length: " + count);
        }
        // 声明的元素个数只用来循环读取，初始容量封顶，避免按超长声明直接分配巨型数组。
        List<RespReply> values = new ArrayList<>((int) Math.min(valueCount, 16L));
        for (int i = 0; i < valueCount; i++) {
            values.add(readReply(in, maxBulkBytes, rawText));
        }
        return new RespReply(kind, null, null, null, values);
    }

    private static int readLengthLine(InputStream in, String type) throws IOException {
        long value = readLongLine(in, type + " length");
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IOException("invalid RESP " + type + " length: " + value);
        }
        return (int) value;
    }

    private static RespReply readText(
            InputStream in, int maxBulkBytes, RespReply.Kind kind, boolean rawText
    ) throws IOException {
        byte[] bytes = readLineBytes(in, maxBulkBytes);
        return new RespReply(kind, rawText ? null : new String(bytes, StandardCharsets.UTF_8), rawText ? bytes : null, null, null);
    }

    private static byte[] readLineBytes(InputStream in, int maxBulkBytes) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (prev == '\r' && b == '\n') {
                byte[] bytes = buf.toByteArray();
                return java.util.Arrays.copyOf(bytes, bytes.length - 1);
            }
            buf.write(b);
            // 多留 1 字节给尚未配对的 CR，正好等于上限的正文仍能遇到 LF 后成功返回。
            if (buf.size() > maxBulkBytes + 1) {
                throw new IOException("RESP line exceeds limit");
            }
            prev = b;
        }
    }

    private static long readLongLine(InputStream in, String type) throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new IOException("unexpected EOF before RESP " + type);
        }

        boolean negative = false;
        if (b == '-') {
            negative = true;
            b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP " + type);
            }
        }

        if (b < '0' || b > '9') {
            throw new IOException("invalid RESP " + type);
        }

        // 用负数累积才能表示 Long.MIN_VALUE；每次运算前检查，避免溢出后绕回合法正数。
        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long value = 0;
        while (true) {
            int digit = b - '0';
            if (value < limit / 10) {
                throw new IOException("invalid RESP " + type);
            }
            value *= 10;
            if (value < limit + digit) {
                throw new IOException("invalid RESP " + type);
            }
            value -= digit;

            b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (b == '\r') {
                int lf = in.read();
                if (lf != '\n') {
                    throw new IOException("expected RESP CRLF");
                }
                return negative ? value : -value;
            }
            if (b < '0' || b > '9') {
                throw new IOException("invalid RESP " + type);
            }
        }
    }

    private static void expectEmptyLine(InputStream in) throws IOException {
        expectCrlf(in);
    }

    private static void expectCrlf(InputStream in) throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("expected RESP CRLF");
        }
    }

    private static void writeNonNegativeInt(OutputStream out, int value) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        if (value == 0) {
            out.write('0');
            return;
        }

        byte[] buf = INT_BUF.get();
        int pos = buf.length;
        int v = value;
        while (v > 0) {
            buf[--pos] = (byte) ('0' + (v % 10));
            v /= 10;
        }
        out.write(buf, pos, buf.length - pos);
    }

    public record RespReply(Kind kind, String text, byte[] bytes, Long integer, List<RespReply> values) {
        public RespReply {
            Objects.requireNonNull(kind, "kind");
            bytes = bytes == null ? null : bytes.clone();
            values = values == null ? null : List.copyOf(values);
        }

        public enum Kind {
            SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, NULL, ARRAY, MAP, SET, NULL_TYPE
        }

        /**
         * RESP2 null bulk（{@code $-1}）和 null array（{@code *-1}）为 true。
         * RESP3 {@code _} 是 {@link Kind#NULL_TYPE}，这里为 false。
         */
        public boolean isNull() {
            return kind == Kind.NULL;
        }

        public byte[] bytes() {
            return bytes == null ? null : bytes.clone();
        }
    }
}
