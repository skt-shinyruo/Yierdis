package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import yier.bubu.redis.db.offheap.api.YierdisDirectBytesSink;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Writes RESP2 responses directly into a {@link ByteBuf}.
 * <p>
 * This is the server fast-path: it avoids building {@link RespObject} trees.
 */
public final class RespWriter {
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte[] CRLF = new byte[]{CR, LF};

    private static final int MAX_ERROR_MESSAGE_CHARS = 256;

    private static final ThreadLocal<byte[]> TL_NUM_BUF = ThreadLocal.withInitial(() -> new byte[32]);

    private final ByteBuf out;
    private final Channel channel;
    private final ByteBufSink outSink;
    private RespProtocol protocol;

    public RespWriter(ByteBuf out) {
        this(out, null);
    }

    public RespWriter(ByteBuf out, Channel channel) {
        this.out = Objects.requireNonNull(out, "out");
        this.channel = channel;
        this.outSink = new ByteBufSink(out);
        this.protocol = RespProtocol.get(channel);
    }

    public RespProtocol protocol() {
        return protocol;
    }

    public void setProtocol(RespProtocol protocol) {
        RespProtocol next = protocol == null ? RespProtocol.RESP2 : protocol;
        this.protocol = next;
        RespProtocol.set(channel, next);
    }

    public void simpleString(String value) {
        out.writeByte('+');
        out.writeCharSequence(value, StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
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
        out.writeByte('-');
        out.writeCharSequence(msg, StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }

    public void integer(long value) {
        out.writeByte(':');
        writeLongAscii(out, value);
        out.writeBytes(CRLF);
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
        out.writeByte('$');
        writeLongAscii(out, len);
        out.writeBytes(CRLF);
        if (len > 0) {
            out.writeBytes(data, off, len);
        }
        out.writeBytes(CRLF);
    }

    public void bulkString(YierdisOffHeapSlice slice) {
        if (slice == null) {
            nullValue();
            return;
        }
        out.writeByte('$');
        int len = slice.length();
        if (len < 0) {
            throw new IllegalStateException("slice length must be >= 0");
        }
        writeLongAscii(out, len);
        out.writeBytes(CRLF);
        slice.writeTo(outSink);
        out.writeBytes(CRLF);
    }

    public void bulkStringLongAscii(long value) {
        out.writeByte('$');
        int len = longAsciiLength(value);
        writeLongAscii(out, len);
        out.writeBytes(CRLF);
        writeLongAscii(out, value);
        out.writeBytes(CRLF);
    }

    public void nullArray() {
        if (protocol == RespProtocol.RESP3) {
            nullValue();
            return;
        }
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

    public void mapHeader(int pairs) {
        if (protocol != RespProtocol.RESP3) {
            throw new IllegalStateException("RESP3 map requires RESP3 protocol");
        }
        out.writeByte('%');
        writeLongAscii(out, pairs);
        out.writeBytes(CRLF);
    }

    public void nullValue() {
        if (protocol == RespProtocol.RESP3) {
            out.writeByte('_');
            out.writeBytes(CRLF);
            return;
        }
        out.writeByte('$');
        out.writeByte('-');
        out.writeByte('1');
        out.writeBytes(CRLF);
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

    private static final class ByteBufSink implements YierdisDirectBytesSink {
        private final ByteBuf out;

        private ByteBufSink(ByteBuf out) {
            this.out = out;
        }

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            out.writeBytes(src, srcIndex, len);
        }

        @Override
        public void ensureWritable(int len) {
            out.ensureWritable(len);
        }

        @Override
        public int writerIndex() {
            return out.writerIndex();
        }

        @Override
        public void writerIndex(int writerIndex) {
            out.writerIndex(writerIndex);
        }

        @Override
        public boolean hasMemoryAddress() {
            return out.hasMemoryAddress();
        }

        @Override
        public long memoryAddress() {
            if (!out.hasMemoryAddress()) {
                throw new UnsupportedOperationException("ByteBuf has no memory address");
            }
            return out.memoryAddress();
        }
    }
}
