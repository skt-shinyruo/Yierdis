package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes RESP2 responses directly into a {@link ByteBuf}.
 * <p>
 * This is the server fast-path: it avoids building {@link RespObject} trees.
 */
public final class RespWriter {
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte[] CRLF = new byte[]{CR, LF};

    private static final ThreadLocal<byte[]> TL_NUM_BUF = ThreadLocal.withInitial(() -> new byte[32]);

    private final ByteBuf out;

    public RespWriter(ByteBuf out) {
        this.out = out;
    }

    public void simpleString(String value) {
        out.writeByte('+');
        out.writeCharSequence(value, StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }

    public void error(String message) {
        out.writeByte('-');
        out.writeCharSequence(message, StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }

    public void integer(long value) {
        out.writeByte(':');
        writeLongAscii(out, value);
        out.writeBytes(CRLF);
    }

    public void bulkString(byte[] data) {
        out.writeByte('$');
        if (data == null) {
            out.writeByte('-');
            out.writeByte('1');
            out.writeBytes(CRLF);
            return;
        }
        writeLongAscii(out, data.length);
        out.writeBytes(CRLF);
        out.writeBytes(data);
        out.writeBytes(CRLF);
    }

    public void nullArray() {
        out.writeByte('*');
        out.writeByte('-');
        out.writeByte('1');
        out.writeBytes(CRLF);
    }

    public void arrayHeader(int count) {
        out.writeByte('*');
        writeLongAscii(out, count);
        out.writeBytes(CRLF);
    }

    public void bulkStringArray(List<byte[]> values) {
        if (values == null) {
            nullArray();
            return;
        }

        arrayHeader(values.size());
        for (int i = 0; i < values.size(); i++) {
            bulkString(values.get(i));
        }
    }

    public void emptyArray() {
        arrayHeader(0);
    }

    static void writeLongAscii(ByteBuf out, long value) {
        if (value == 0) {
            out.writeByte('0');
            return;
        }
        if (value == Long.MIN_VALUE) {
            out.writeCharSequence("-9223372036854775808", StandardCharsets.US_ASCII);
            return;
        }

        byte[] buf = TL_NUM_BUF.get();
        int pos = buf.length;

        long x = value;
        boolean negative = x < 0;
        if (negative) {
            x = -x;
        }

        while (x != 0) {
            long q = x / 10;
            int digit = (int) (x - q * 10);
            buf[--pos] = (byte) ('0' + digit);
            x = q;
        }
        if (negative) {
            buf[--pos] = '-';
        }

        out.writeBytes(buf, pos, buf.length - pos);
    }
}
