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
    private static final byte[] EMPTY_BYTES = new byte[0];
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
            byte[] value = arg == null ? EMPTY_BYTES : arg;
            out.write('$');
            writeNonNegativeInt(out, value.length);
            out.write(CRLF);
            out.write(value);
            out.write(CRLF);
        }
    }

    public static RespReply readReply(InputStream in, int maxBulkBytes) throws IOException {
        Objects.requireNonNull(in, "in");
        if (maxBulkBytes < 0) {
            throw new IllegalArgumentException("maxBulkBytes must be >= 0");
        }
        int type = in.read();
        if (type < 0) {
            throw new IOException("unexpected EOF before RESP reply");
        }
        return switch (type) {
            case '+' -> new RespReply(RespReply.Kind.SIMPLE_STRING, readStringLine(in), null, null, null);
            case '-' -> new RespReply(RespReply.Kind.ERROR, readStringLine(in), null, null, null);
            case ':' -> new RespReply(RespReply.Kind.INTEGER, null, null, readLongLine(in, "integer"), null);
            case '$' -> readBulkString(in, maxBulkBytes);
            case '*' -> readAggregate(in, maxBulkBytes, RespReply.Kind.ARRAY, "array");
            case '%' -> readAggregate(in, maxBulkBytes, RespReply.Kind.MAP, "map", true);
            case '~' -> readAggregate(in, maxBulkBytes, RespReply.Kind.SET, "set");
            case '_' -> {
                expectEmptyLine(in);
                yield new RespReply(RespReply.Kind.NULL, null, null, null, null);
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
            String type
    ) throws IOException {
        return readAggregate(in, maxBulkBytes, kind, type, false);
    }

    private static RespReply readAggregate(
            InputStream in,
            int maxBulkBytes,
            RespReply.Kind kind,
            String type,
            boolean map
    ) throws IOException {
        int count = readLengthLine(in, type);
        if (count < 0) {
            return new RespReply(RespReply.Kind.NULL, null, null, null, null);
        }
        long valueCount = map ? Math.multiplyExact((long) count, 2L) : count;
        if (valueCount > Integer.MAX_VALUE) {
            throw new IOException("invalid RESP " + type + " length: " + count);
        }
        List<RespReply> values = new ArrayList<>((int) valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.add(readReply(in, maxBulkBytes));
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

    private static String readStringLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (prev == '\r' && b == '\n') {
                byte[] bytes = buf.toByteArray();
                return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
            }
            buf.write(b);
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

        long value = 0;
        while (true) {
            value = value * 10 + (b - '0');
            if (value < 0) {
                throw new IOException("invalid RESP " + type);
            }

            b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (b == '\r') {
                int lf = in.read();
                if (lf != '\n') {
                    throw new IOException("expected RESP CRLF");
                }
                return negative ? -value : value;
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
            SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, NULL, ARRAY, MAP, SET
        }

        public boolean isNull() {
            return kind == Kind.NULL;
        }

        public byte[] bytes() {
            return bytes == null ? null : bytes.clone();
        }
    }
}
