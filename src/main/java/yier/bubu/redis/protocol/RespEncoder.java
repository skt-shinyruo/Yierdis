package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class RespEncoder extends MessageToByteEncoder<RespObject> {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) {
        writeObject(out, msg);
    }

    private void writeObject(ByteBuf out, RespObject obj) {
        if (obj == null || obj instanceof RespNull) {
            out.writeByte('$');
            out.writeBytes("-1".getBytes(StandardCharsets.US_ASCII));
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
                out.writeBytes("-1".getBytes(StandardCharsets.US_ASCII));
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
        out.writeCharSequence(Long.toString(i.value()), StandardCharsets.US_ASCII);
        out.writeBytes(CRLF);
    }

    private void writeBulkString(ByteBuf out, RespBulkString b) {
        out.writeByte('$');
        if (b.isNull()) {
            out.writeCharSequence("-1", StandardCharsets.US_ASCII);
            out.writeBytes(CRLF);
            return;
        }

        byte[] data = b.data();
        out.writeCharSequence(Integer.toString(data.length), StandardCharsets.US_ASCII);
        out.writeBytes(CRLF);
        out.writeBytes(data);
        out.writeBytes(CRLF);
    }

    private void writeArray(ByteBuf out, RespArray array) {
        out.writeByte('*');
        if (array.isNull()) {
            out.writeCharSequence("-1", StandardCharsets.US_ASCII);
            out.writeBytes(CRLF);
            return;
        }

        List<RespObject> values = array.values();
        out.writeCharSequence(Integer.toString(values.size()), StandardCharsets.US_ASCII);
        out.writeBytes(CRLF);
        for (RespObject v : values) {
            writeObject(out, v);
        }
    }
}
