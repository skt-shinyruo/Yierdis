package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespBigNumber;
import yier.bubu.redis.protocol.RespBlobError;
import yier.bubu.redis.protocol.RespBoolean;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespDouble;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespPush;
import yier.bubu.redis.protocol.RespSet;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.RespVerbatimString;
import java.util.List;

public final class RespEncoder extends MessageToByteEncoder<RespObject> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RespObject msg, ByteBuf out) {
        RespWriterAdapter.write(out, ConnectionContext.getOrCreate(ctx.channel()), msg);
    }

    /**
     * 协议 Netty adapter：将 {@link RespObject} 写入 {@link ByteBuf} 时统一使用 {@link yier.bubu.redis.protocol.RespWriter} 的语义，
     * 避免 codec 与 server fast-path 出现行为漂移。
     */
    private static final class RespWriterAdapter {
        private RespWriterAdapter() {
        }

        static void write(ByteBuf out, yier.bubu.redis.protocol.RespSession session, RespObject obj) {
            yier.bubu.redis.protocol.RespWriter writer =
                    new yier.bubu.redis.protocol.RespWriter(new NettyByteBufSink(out), session);
            writeObject(writer, obj);
        }

        private static void writeObject(yier.bubu.redis.protocol.RespWriter writer, RespObject obj) {
            if (obj == null || obj instanceof RespNull) {
                writer.nullValue();
                return;
            }

            switch (obj.type()) {
                case SIMPLE_STRING:
                    writer.simpleString(((RespSimpleString) obj).value());
                    return;
                case ERROR:
                    writer.error(((RespError) obj).message());
                    return;
                case INTEGER:
                    writer.integer(((RespInteger) obj).value());
                    return;
                case BOOLEAN:
                    writer.booleanValue(((RespBoolean) obj).value());
                    return;
                case DOUBLE:
                    writer.doubleValue(((RespDouble) obj).value());
                    return;
                case BIG_NUMBER:
                    writer.bigNumberAscii(((RespBigNumber) obj).value());
                    return;
                case BULK_STRING:
                    writer.bulkString(((RespBulkString) obj).data());
                    return;
                case VERBATIM_STRING: {
                    RespVerbatimString v = (RespVerbatimString) obj;
                    writer.verbatimString(v.format(), v.data());
                    return;
                }
                case BLOB_ERROR: {
                    RespBlobError e = (RespBlobError) obj;
                    // RESP2 fallback behavior is handled by RespWriter.blobError(...).
                    writer.blobError(e.asString());
                    return;
                }
                case ARRAY:
                    writeArray(writer, (RespArray) obj);
                    return;
                case MAP:
                    writeMap(writer, (RespMap) obj);
                    return;
                case SET:
                    writeSet(writer, (RespSet) obj);
                    return;
                case PUSH:
                    writePush(writer, (RespPush) obj);
                    return;
                case ATTRIBUTE:
                    writeAttribute(writer, (RespAttribute) obj);
                    return;
                case NULL:
                default:
                    writer.nullValue();
            }
        }

        private static void writeArray(yier.bubu.redis.protocol.RespWriter writer, RespArray array) {
            if (array.isNull()) {
                writer.nullArray();
                return;
            }
            List<RespObject> values = array.values();
            if (values == null) {
                writer.nullArray();
                return;
            }
            writer.arrayHeader(values.size());
            for (RespObject v : values) {
                writeObject(writer, v);
            }
        }

        private static void writeMap(yier.bubu.redis.protocol.RespWriter writer, RespMap map) {
            if (map == null) {
                writer.nullValue();
                return;
            }
            List<RespMap.Entry> entries = map.entries();
            if (entries == null) {
                writer.nullValue();
                return;
            }
            writer.mapHeader(entries.size());
            for (RespMap.Entry e : entries) {
                writeObject(writer, e.key());
                writeObject(writer, e.value());
            }
        }

        private static void writeSet(yier.bubu.redis.protocol.RespWriter writer, RespSet set) {
            if (set == null) {
                writer.nullValue();
                return;
            }
            List<RespObject> values = set.values();
            if (values == null) {
                writer.nullValue();
                return;
            }
            writer.setHeader(values.size());
            for (RespObject v : values) {
                writeObject(writer, v);
            }
        }

        private static void writePush(yier.bubu.redis.protocol.RespWriter writer, RespPush push) {
            if (push == null) {
                writer.nullValue();
                return;
            }
            List<RespObject> values = push.values();
            if (values == null) {
                writer.nullValue();
                return;
            }
            writer.pushHeader(values.size());
            for (RespObject v : values) {
                writeObject(writer, v);
            }
        }

        private static void writeAttribute(yier.bubu.redis.protocol.RespWriter writer, RespAttribute attr) {
            if (attr == null) {
                writer.nullValue();
                return;
            }
            RespMap attrs = attr.attributes();
            RespObject value = attr.value();
            if (attrs == null || value == null) {
                writer.nullValue();
                return;
            }
            List<RespMap.Entry> entries = attrs.entries();
            if (entries == null) {
                writer.nullValue();
                return;
            }
            writer.attributeHeader(entries.size());
            for (RespMap.Entry e : entries) {
                writeObject(writer, e.key());
                writeObject(writer, e.value());
            }
            writeObject(writer, value);
        }
    }
}
