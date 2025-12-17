package yier.bubu.redis;

import yier.bubu.redis.command.CommandProcessor;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

final class YierdisCommandHandler extends SimpleChannelInboundHandler<RespObject> {
    private static final Logger log = LoggerFactory.getLogger(YierdisCommandHandler.class);

    private final CommandProcessor commandProcessor;

    YierdisCommandHandler(CommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespObject msg) {
        RespObject response;
        try {
            List<byte[]> args = asCommandArgs(msg);
            if (args.isEmpty()) {
                response = RespError.of("ERR empty command");
            } else if (isQuit(args.get(0))) {
                ctx.writeAndFlush(RespSimpleString.of("OK")).addListener(ChannelFutureListener.CLOSE);
                return;
            } else {
                response = commandProcessor.execute(args);
            }
        } catch (Exception e) {
            log.debug("Command handling error", e);
            response = RespError.of("ERR " + e.getMessage());
        }
        ctx.writeAndFlush(response);
    }

    private static List<byte[]> asCommandArgs(RespObject msg) {
        if (!(msg instanceof RespArray)) {
            throw new IllegalArgumentException("Protocol error: expected array");
        }
        RespArray array = (RespArray) msg;
        List<RespObject> items = array.values();
        List<byte[]> args = new ArrayList<>(items.size());
        for (RespObject item : items) {
            if (item instanceof RespBulkString) {
                args.add(((RespBulkString) item).data());
                continue;
            }
            throw new IllegalArgumentException("Protocol error: expected bulk string array");
        }
        return args;
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("Connection error", cause);
        ctx.close();
    }
}
