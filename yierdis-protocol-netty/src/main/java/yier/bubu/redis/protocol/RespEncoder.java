package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class RespEncoder extends MessageToByteEncoder<RespObject> {
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte[] CRLF = new byte[]{CR, LF};

    private static final ThreadLocal<byte[]> TL_NUM_BUF = ThreadLocal.withInitial(() -> new byte[32]);

    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) {
        writeObject(out, msg);
    }

    private void writeObject(ByteBuf out, RespObject obj) {
        if (obj == null || obj instanceof RespNull) {
            out.writeByte('$');
            out.writeByte('-');
            out.writeByte('1');
            out.writeBytes(CRLF);
            return;
        }

        switch (obj.type()) {
            case SIMPLE_STRING:
                writeSimpleString(out, (RespSimpleString) obj);
                return;
            case ERROR:
                writeError(out, (RespError) obj);
                return;
            case INTEGER:
                writeInteger(out, (RespInteger) obj);
                return;
            case BULK_STRING:
                writeBulkString(out, (RespBulkString) obj);
                return;
            case ARRAY:
                writeArray(out, (RespArray) obj);
                return;
            case NULL:
            default:
                out.writeByte('$');
                out.writeByte('-');
                out.writeByte('1');
                out.writeBytes(CRLF);
        }
    }

    private void writeSimpleString(ByteBuf out, RespSimpleString s) {
        out.writeByte('+');
        out.writeCharSequence(s.value(), StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }

    private void writeError(ByteBuf out, RespError e) {
        out.writeByte('-');
        out.writeCharSequence(e.message(), StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }

    private void writeInteger(ByteBuf out, RespInteger i) {
        out.writeByte(':');
        writeLongAscii(out, i.value());
        out.writeBytes(CRLF);
    }

    private void writeBulkString(ByteBuf out, RespBulkString b) {
        out.writeByte('$');
        if (b.isNull()) {
            out.writeByte('-');
            out.writeByte('1');
            out.writeBytes(CRLF);
            return;
        }

        byte[] data = b.data();
        writeLongAscii(out, data.length);
        out.writeBytes(CRLF);
        out.writeBytes(data);
        out.writeBytes(CRLF);
    }

    private void writeArray(ByteBuf out, RespArray array) {
        out.writeByte('*');
        if (array.isNull()) {
            out.writeByte('-');
            out.writeByte('1');
            out.writeBytes(CRLF);
            return;
        }

        List<RespObject> values = array.values();
        writeLongAscii(out, values.size());
        out.writeBytes(CRLF);
        for (RespObject v : values) {
            writeObject(out, v);
        }
    }

    private static void writeLongAscii(ByteBuf out, long value) {
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

