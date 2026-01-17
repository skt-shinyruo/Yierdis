package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespMap;
import yier.bubu.redis.protocol.RespNull;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
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
                case BULK_STRING:
                    writer.bulkString(((RespBulkString) obj).data());
                    return;
                case ARRAY:
                    writeArray(writer, (RespArray) obj);
                    return;
                case MAP:
                    writeMap(writer, (RespMap) obj);
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
    }
}
