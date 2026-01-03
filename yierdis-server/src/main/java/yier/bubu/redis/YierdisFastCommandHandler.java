package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
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
        // Best-effort: return a RESP error and close the connection.
        // This covers protocol decode errors (e.g. invalid RESP frames) and unexpected runtime errors.
        if (ctx == null) {
            return;
        }
        Throwable root = unwrapDecoderException(cause);
        String message = safeErrorMessage(root);
        String err = message.startsWith("Protocol error")
                ? "ERR " + message
                : "ERR internal error";

        ByteBuf out = ctx.alloc().buffer();
        try {
            new RespWriter(out).error(err);
            ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private static Throwable unwrapDecoderException(Throwable cause) {
        if (cause instanceof DecoderException && cause.getCause() != null) {
            return cause.getCause();
        }
        return cause;
    }

    private static String safeErrorMessage(Throwable cause) {
        if (cause == null) {
            return "internal error";
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isBlank()) {
            msg = cause.getClass().getSimpleName();
        }
        // Prevent response splitting via CRLF injection.
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (msg.length() > 256) {
            msg = msg.substring(0, 256);
        }
        return msg;
    }
}
