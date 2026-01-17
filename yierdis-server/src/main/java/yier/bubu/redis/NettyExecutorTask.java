package yier.bubu.redis;

import io.netty.channel.ChannelHandlerContext;
import yier.bubu.redis.protocol.RespCommand;

/**
 * 执行器内部任务载体：持有 {@link RespCommand} 所需的上下文以及 bytes 预算口径。
 * <p>
 * 该对象只在 server 模块内部使用（executor/调度队列），用于避免在多个组件之间传递松散参数。
 */
final class NettyExecutorTask {
    final ChannelHandlerContext ctx;
    final RespCommand cmd;
    final int retainedBytes;

    private NettyExecutorTask(ChannelHandlerContext ctx, RespCommand cmd, int retainedBytes) {
        this.ctx = ctx;
        this.cmd = cmd;
        this.retainedBytes = retainedBytes;
    }

    static NettyExecutorTask command(ChannelHandlerContext ctx, RespCommand cmd, int retainedBytes) {
        return new NettyExecutorTask(ctx, cmd, retainedBytes);
    }
}

