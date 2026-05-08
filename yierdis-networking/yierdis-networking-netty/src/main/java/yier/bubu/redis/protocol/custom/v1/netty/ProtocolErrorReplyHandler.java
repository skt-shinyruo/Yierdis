package yier.bubu.redis.protocol.custom.v1.netty;

// decoder 协议错误处理器：接收 ProtocolError 事件并统一编码回包（NDJSON）。

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.protocol.custom.v1.execution.JsonLineReplyWriterFactory;

import java.util.Objects;

public final class ProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final JsonLineReplyWriterFactory replyWriterFactory;

    public ProtocolErrorReplyHandler(JsonLineReplyWriterFactory replyWriterFactory) {
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ProtocolError e) {
            writeProtocolError(ctx, e.message());
            return;
        }
        super.channelRead(ctx, msg);
    }

    private void writeProtocolError(ChannelHandlerContext ctx, String message) {
        if (ctx == null) {
            return;
        }
        ByteBuf out = ctx.alloc().buffer();
        try {
            var writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
            writer.protocolError(message);
            ctx.writeAndFlush(out);
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }
}
