package yier.bubu.redis;

// 协议请求到执行命令的适配器：保持 Custom Protocol v1 的 UTF-8/null argv 语义，同时把协议层与 core-contract 解耦。

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

final class ProtocolCommandAdapter extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (ctx == null || msg == null) {
            return;
        }
        if (msg instanceof CustomProtocolV1ArgvRequest argvRequest) {
            byte[][] argv = new byte[argvRequest.argc()][];
            for (int i = 0; i < argv.length; i++) {
                argv[i] = argvRequest.readOnlyArg(i);
            }
            ctx.fireChannelRead(ByteArrayExecutionRequest.wrapReadOnly(argv, argvRequest.retainedBytes()));
            return;
        }
        if (msg instanceof CustomProtocolV1Request request) {
            ctx.fireChannelRead(ByteArrayExecutionRequest.fromUtf8(request.cmd(), request.args()));
            return;
        }
        super.channelRead(ctx, msg);
    }
}
