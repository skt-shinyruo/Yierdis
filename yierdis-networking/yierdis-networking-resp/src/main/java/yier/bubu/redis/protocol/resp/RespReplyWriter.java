package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyShapes;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.IntSupplier;

public final class RespReplyWriter implements RedisReplyWriter {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private final BytesSink out;
    private final IntSupplier versionSupplier;

    public RespReplyWriter(CommandSession session, BytesSink out) {
        this(out, Objects.requireNonNull(session, "session")::respVersion);
    }

    RespReplyWriter(BytesSink out, IntSupplier versionSupplier) {
        this.out = Objects.requireNonNull(out, "out");
        this.versionSupplier = Objects.requireNonNull(versionSupplier, "versionSupplier");
    }

    @Override
    public void simpleString(String value) {
        writeAsciiLine('+', ReplyShapes.sanitizeSimple(value));
    }

    @Override
    public void error(String message) {
        writeAsciiLine('-', ReplyShapes.normalizeError(message));
    }

    @Override
    public void controlError(String message) {
        if (out instanceof ReplyReservationSink reservationSink) {
            reservationSink.useControlReservation();
        }
        error(message);
    }

    @Override
    public void integer(long value) {
        writeAsciiLine(':', Long.toString(value));
    }

    @Override
    public void bulkString(byte[] data) {
        if (data == null) {
            nullValue();
            return;
        }
        bulkString(data, 0, data.length);
    }

    @Override
    public void bulkString(byte[] data, int off, int len) {
        if (data == null) {
            nullValue();
            return;
        }
        Objects.checkFromIndexSize(off, len, data.length);
        writeAscii("$" + len + "\r\n");
        out.writeBytes(data, off, len);
        writeCrlf();
    }

    @Override
    public void bulkString(BytesSlice slice) {
        if (slice == null) {
            nullValue();
            return;
        }
        int len = slice.length();
        if (len < 0) {
            throw new IllegalArgumentException("slice length must be >= 0");
        }
        writeAscii("$" + len + "\r\n");
        slice.writeTo(out);
        writeCrlf();
    }

    @Override
    public void bulkStringLongAscii(long value) {
        bulkString(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void nullValue() {
        if (version() == RespProtocolVersion.RESP3) {
            writeAscii("_\r\n");
        } else {
            writeAscii("$-1\r\n");
        }
    }

    @Override
    public void nullArray() {
        if (version() == RespProtocolVersion.RESP3) {
            writeAscii("_\r\n");
        } else {
            writeAscii("*-1\r\n");
        }
    }

    @Override
    public void arrayHeader(int count) {
        writeAsciiLine('*', Integer.toString(Math.max(0, count)));
    }

    @Override
    public void mapHeader(int pairs) {
        if (version() == RespProtocolVersion.RESP3) {
            writeAsciiLine('%', Integer.toString(Math.max(0, pairs)));
        } else {
            writeAsciiLine('*', Integer.toString(Math.max(0, pairs) * 2));
        }
    }

    @Override
    public void setHeader(int count) {
        if (version() == RespProtocolVersion.RESP3) {
            writeAsciiLine('~', Integer.toString(Math.max(0, count)));
        } else {
            arrayHeader(count);
        }
    }

    private RespProtocolVersion version() {
        return RespProtocolVersion.fromWireValue(versionSupplier.getAsInt());
    }

    private void writeAsciiLine(char prefix, String value) {
        writeAscii(prefix + value + "\r\n");
    }

    private void writeAscii(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        out.writeBytes(bytes, 0, bytes.length);
    }

    private void writeCrlf() {
        out.writeBytes(CRLF, 0, CRLF.length);
    }
}
