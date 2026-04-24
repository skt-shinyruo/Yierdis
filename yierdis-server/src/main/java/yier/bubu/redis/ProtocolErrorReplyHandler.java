package yier.bubu.redis;

// decoder 协议错误处理器：接收 ProtocolError 事件并统一编码回包（NDJSON）。

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;
import yier.bubu.redis.protocol.netty.ProtocolError;

import java.util.Objects;

final class ProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final NettyCommandExecutor executor;
    private final ReplyWriterFactory replyWriterFactory;

    ProtocolErrorReplyHandler(NettyCommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = null;
    }

    ProtocolErrorReplyHandler(ReplyWriterFactory replyWriterFactory) {
        this.executor = null;
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
            ReplyWriter writer = newReplyWriter(out, ctx);
            writer.protocolError(message);
            ctx.writeAndFlush(out);
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private ReplyWriter newReplyWriter(ByteBuf out, ChannelHandlerContext ctx) {
        if (executor != null) {
            return executor.newReplyWriter(out, ctx.channel());
        }
        return replyWriterFactory.newWriter(new NettyByteBufSink(out));
    }
}
