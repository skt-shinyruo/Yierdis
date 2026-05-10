package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;

import java.util.Objects;

public final class RespProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final ReplyWriterFactory replyWriterFactory;

    public RespProtocolErrorReplyHandler(ReplyWriterFactory replyWriterFactory) {
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RespProtocolError error)) {
            super.channelRead(ctx, msg);
            return;
        }
        ByteBuf out = ctx.alloc().buffer();
        try {
            ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
            writer.protocolError(error.message());
            if (error.closeAfterReply()) {
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.writeAndFlush(out);
            }
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }
}
