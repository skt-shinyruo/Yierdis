package yier.bubu.redis.protocol;

import yier.bubu.redis.db.offheap.api.YierdisBytesSink;
import yier.bubu.redis.db.offheap.api.YierdisDirectBytesSink;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Writes RESP2/RESP3 responses into a {@link YierdisBytesSink}.
 * <p>
 * This is the server fast-path: it avoids building {@link RespObject} trees.
 */
public final class RespWriter {
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte[] CRLF = new byte[]{CR, LF};
    private static final byte[] PLUS = new byte[]{'+'};
    private static final byte[] MINUS = new byte[]{'-'};
    private static final byte[] COLON = new byte[]{':'};
    private static final byte[] DOLLAR = new byte[]{'$'};
    private static final byte[] STAR = new byte[]{'*'};
    private static final byte[] PERCENT = new byte[]{'%'};
    private static final byte[] RESP2_NULL_BULK = new byte[]{'$', '-', '1', CR, LF};
    private static final byte[] RESP2_NULL_ARRAY = new byte[]{'*', '-', '1', CR, LF};
    private static final byte[] RESP3_NULL = new byte[]{'_', CR, LF};

    private static final int MAX_ERROR_MESSAGE_CHARS = 256;

    private static final ThreadLocal<byte[]> TL_NUM_BUF = ThreadLocal.withInitial(() -> new byte[32]);

    private final YierdisBytesSink out;
    private final RespSession session;
    private RespProtocol protocol;

    public RespWriter(YierdisBytesSink out) {
        this(out, null);
    }

    public RespWriter(YierdisBytesSink out, RespSession session) {
        this.out = Objects.requireNonNull(out, "out");
        this.session = session;
        this.protocol = session == null ? RespProtocol.RESP2 : normalizeProtocol(session.protocol());
    }

    public RespProtocol protocol() {
        return protocol;
    }

    public void setProtocol(RespProtocol protocol) {
        RespProtocol next = normalizeProtocol(protocol);
        this.protocol = next;
        if (session != null) {
            session.setProtocol(next);
        }
    }

    public void simpleString(String value) {
        out.writeBytes(PLUS, 0, 1);
        if (value != null && !value.isEmpty()) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeBytes(bytes, 0, bytes.length);
        }
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void error(String message) {
        String msg = message;
        if (msg == null || msg.isBlank()) {
            msg = "ERR internal error";
        }
        // 安全净化：防止 CRLF 注入导致 RESP response splitting。
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (msg.length() > MAX_ERROR_MESSAGE_CHARS) {
            msg = msg.substring(0, MAX_ERROR_MESSAGE_CHARS);
        }
        out.writeBytes(MINUS, 0, 1);
        byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
        out.writeBytes(bytes, 0, bytes.length);
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void integer(long value) {
        out.writeBytes(COLON, 0, 1);
        writeLongAscii(out, value);
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void bulkString(byte[] data) {
        if (data == null) {
            nullValue();
            return;
        }
        bulkString(data, 0, data.length);
    }

    public void bulkString(byte[] data, int off, int len) {
        if (data == null) {
            nullValue();
            return;
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (off < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        out.writeBytes(DOLLAR, 0, 1);
        writeLongAscii(out, len);
        out.writeBytes(CRLF, 0, CRLF.length);
        if (len > 0) {
            out.writeBytes(data, off, len);
        }
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void bulkString(YierdisOffHeapSlice slice) {
        if (slice == null) {
            nullValue();
            return;
        }
        out.writeBytes(DOLLAR, 0, 1);
        int len = slice.length();
        if (len < 0) {
            throw new IllegalStateException("slice length must be >= 0");
        }
        writeLongAscii(out, len);
        out.writeBytes(CRLF, 0, CRLF.length);
        slice.writeTo(out);
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void bulkStringLongAscii(long value) {
        out.writeBytes(DOLLAR, 0, 1);
        int len = longAsciiLength(value);
        writeLongAscii(out, len);
        out.writeBytes(CRLF, 0, CRLF.length);
        writeLongAscii(out, value);
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void nullArray() {
        if (protocol == RespProtocol.RESP3) {
            nullValue();
            return;
        }
        out.writeBytes(RESP2_NULL_ARRAY, 0, RESP2_NULL_ARRAY.length);
    }

    public void arrayHeader(int count) {
        out.writeBytes(STAR, 0, 1);
        writeLongAscii(out, count);
        out.writeBytes(CRLF, 0, CRLF.length);
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

    public void mapHeader(int pairs) {
        if (protocol != RespProtocol.RESP3) {
            throw new IllegalStateException("RESP3 map requires RESP3 protocol");
        }
        out.writeBytes(PERCENT, 0, 1);
        writeLongAscii(out, pairs);
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    public void nullValue() {
        if (protocol == RespProtocol.RESP3) {
            out.writeBytes(RESP3_NULL, 0, RESP3_NULL.length);
            return;
        }
        out.writeBytes(RESP2_NULL_BULK, 0, RESP2_NULL_BULK.length);
    }

    static void writeLongAscii(YierdisBytesSink out, long value) {
        if (value == 0) {
            out.writeBytes(new byte[]{'0'}, 0, 1);
            return;
        }
        if (value == Long.MIN_VALUE) {
            byte[] min = "-9223372036854775808".getBytes(StandardCharsets.US_ASCII);
            out.writeBytes(min, 0, min.length);
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

    private static int longAsciiLength(long v) {
        if (v == Long.MIN_VALUE) {
            return 20;
        }
        if (v == 0) {
            return 1;
        }
        long x = v < 0 ? -v : v;
        int digits = 0;
        while (x != 0) {
            x /= 10;
            digits++;
        }
        return v < 0 ? digits + 1 : digits;
    }

    private static RespProtocol normalizeProtocol(RespProtocol protocol) {
        return protocol == null ? RespProtocol.RESP2 : protocol;
    }
}
