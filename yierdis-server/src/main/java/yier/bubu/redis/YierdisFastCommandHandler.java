package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<RespCommand> {
    private final YierdisFastCommandProcessor commandProcessor;

    YierdisFastCommandHandler(YierdisFastCommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespCommand msg) {
        ByteBuf out = ctx.alloc().buffer();
        try {
            RespWriter writer = new RespWriter(out);
            if (msg.argc() == 1 && isQuit(msg)) {
                writer.simpleString("OK");
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            } else {
                commandProcessor.execute(msg, writer);
                ctx.write(out);
            }
            out = null;
        } finally {
            msg.recycle();
            if (out != null) {
                out.release();
            }
        }
    }

    private static boolean isQuit(RespCommand cmd) {
        if (cmd.isNull(0) || cmd.len(0) != 4) {
            return false;
        }
        byte b0 = cmd.byteAt(0, 0);
        byte b1 = cmd.byteAt(0, 1);
        byte b2 = cmd.byteAt(0, 2);
        byte b3 = cmd.byteAt(0, 3);
        return (b0 == 'Q' || b0 == 'q')
                && (b1 == 'U' || b1 == 'u')
                && (b2 == 'I' || b2 == 'i')
                && (b3 == 'T' || b3 == 't');
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Best-effort: close the connection on protocol errors.
        ctx.close();
    }
}
