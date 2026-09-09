package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyReservationSink;
import yier.bubu.redis.execution.api.ReplyShapes;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class RespReplyWriter implements RedisReplyWriter {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private final BytesSink out;
    private final RespProtocolVersion version;

    // 协议版本在构造时固定，渲染期间不回读 session。预留回复路径上 executor 传入的是
    // ReplyPlan 在 prepare 时刻捕获的值，保证写出字节与预留容量按同一版本计算。
    public RespReplyWriter(int protocolVersion, BytesSink out) {
        this.out = Objects.requireNonNull(out, "out");
        this.version = RespProtocolVersion.fromWireValue(protocolVersion);
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
        writeBytes(version.nullValueEncoding());
    }

    @Override
    public void nullArray() {
        writeBytes(version.nullArrayEncoding());
    }

    @Override
    public void arrayHeader(int count) {
        writeAsciiLine('*', Integer.toString(Math.max(0, count)));
    }

    @Override
    public void mapHeader(int pairs) {
        writeAsciiLine(version.mapPrefix(), Long.toString(version.mapHeaderCount(Math.max(0, pairs))));
    }

    @Override
    public void setHeader(int count) {
        writeAsciiLine(version.setPrefix(), Integer.toString(Math.max(0, count)));
    }

    private void writeBytes(byte[] encoding) {
        out.writeBytes(encoding, 0, encoding.length);
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
