package yier.bubu.redis;

// 协议请求到执行命令的适配器：保持 Custom Protocol v1 的 UTF-8/null argv 语义，同时把协议层与 core-contract 解耦。

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;

final class ProtocolCommandAdapter extends SimpleChannelInboundHandler<CustomProtocolV1ArgvRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CustomProtocolV1ArgvRequest msg) {
        if (ctx == null || msg == null) {
            return;
        }
        byte[][] argv = new byte[msg.argc()][];
        for (int i = 0; i < argv.length; i++) {
            argv[i] = msg.readOnlyArg(i);
        }
        ctx.fireChannelRead(ByteArrayExecutionRequest.wrapReadOnly(argv, msg.retainedBytes()));
    }
}
