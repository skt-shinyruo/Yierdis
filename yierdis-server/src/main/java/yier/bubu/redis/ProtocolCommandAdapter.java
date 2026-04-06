package yier.bubu.redis;

// 协议请求到执行命令的适配器：保持 Custom Protocol v1 的 UTF-8/null argv 语义，同时把协议层与 core-contract 解耦。

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

final class ProtocolCommandAdapter extends SimpleChannelInboundHandler<CustomProtocolV1Request> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CustomProtocolV1Request msg) {
        if (ctx == null || msg == null) {
            return;
        }
        ctx.fireChannelRead(ByteArrayExecutionRequest.fromUtf8(msg.cmd(), msg.args()));
    }
}
