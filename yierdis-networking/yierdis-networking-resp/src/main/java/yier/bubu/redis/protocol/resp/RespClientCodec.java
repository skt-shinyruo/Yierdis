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
        writeAscii(out, "*" + args.size() + "\r\n");
        for (byte[] arg : args) {
            byte[] value = arg == null ? EMPTY_BYTES : arg;
            writeAscii(out, "$" + value.length + "\r\n");
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
            case '+' -> new RespReply(RespReply.Kind.SIMPLE_STRING, readLine(in), null, null, null);
            case '-' -> new RespReply(RespReply.Kind.ERROR, readLine(in), null, null, null);
            case ':' -> new RespReply(RespReply.Kind.INTEGER, null, null, Long.parseLong(readLine(in)), null);
            case '$' -> readBulkString(in, maxBulkBytes);
            case '*' -> readArray(in, maxBulkBytes);
            case '_' -> {
                expectEmptyLine(in);
                yield new RespReply(RespReply.Kind.NULL, null, null, null, null);
            }
            default -> throw new IOException("unexpected RESP reply type: " + (char) type);
        };
    }

    private static RespReply readBulkString(InputStream in, int maxBulkBytes) throws IOException {
        int len = parseLength(readLine(in), "bulk string");
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

    private static RespReply readArray(InputStream in, int maxBulkBytes) throws IOException {
        int count = parseLength(readLine(in), "array");
        if (count < 0) {
            return new RespReply(RespReply.Kind.NULL, null, null, null, null);
        }
        List<RespReply> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(readReply(in, maxBulkBytes));
        }
        return new RespReply(RespReply.Kind.ARRAY, null, null, null, values);
    }

    private static int parseLength(String text, String type) throws IOException {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new IOException("invalid RESP " + type + " length: " + text, e);
        }
    }

    private static String readLine(InputStream in) throws IOException {
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

    private static void writeAscii(OutputStream out, String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.US_ASCII));
    }

    public record RespReply(Kind kind, String text, byte[] bytes, Long integer, List<RespReply> values) {
        public RespReply {
            Objects.requireNonNull(kind, "kind");
            bytes = bytes == null ? null : bytes.clone();
            values = values == null ? null : List.copyOf(values);
        }

        public enum Kind {
            SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, NULL, ARRAY
        }

        public boolean isNull() {
            return kind == Kind.NULL;
        }

        public boolean isSimpleString(String expected) {
            return kind == Kind.SIMPLE_STRING && Objects.equals(text, expected);
        }

        public byte[] bytes() {
            return bytes == null ? null : bytes.clone();
        }
    }
}
