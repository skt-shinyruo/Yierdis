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
            if (msg.argc() == 1 && isQuit(msg.arg(0))) {
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

    private static boolean isQuit(byte[] cmd) {
        if (cmd == null || cmd.length != 4) {
            return false;
        }
        return (cmd[0] == 'Q' || cmd[0] == 'q')
                && (cmd[1] == 'U' || cmd[1] == 'u')
                && (cmd[2] == 'I' || cmd[2] == 'i')
                && (cmd[3] == 'T' || cmd[3] == 't');
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
