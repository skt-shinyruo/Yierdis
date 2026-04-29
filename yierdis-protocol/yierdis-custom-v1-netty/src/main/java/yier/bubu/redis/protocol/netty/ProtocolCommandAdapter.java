package yier.bubu.redis.protocol.netty;

// 协议请求到执行命令的适配器：保持 Custom Protocol v1 的 UTF-8/null argv 语义，同时把协议层与 core-contract 解耦。

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ExecutionAdapter;
import yier.bubu.redis.protocol.v1.CustomProtocolV1Request;

public final class ProtocolCommandAdapter extends ChannelInboundHandlerAdapter {
    private final CustomProtocolV1ExecutionAdapter adapter;

    public ProtocolCommandAdapter() {
        this(CustomProtocolV1ExecutionAdapter.DEFAULT);
    }

    public ProtocolCommandAdapter(CustomProtocolV1ExecutionAdapter adapter) {
        this.adapter = adapter == null ? CustomProtocolV1ExecutionAdapter.DEFAULT : adapter;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (ctx == null || msg == null) {
            return;
        }
        if (msg instanceof CustomProtocolV1ArgvRequest argvRequest) {
            ctx.fireChannelRead(adapter.toExecutionRequest(argvRequest));
            return;
        }
        if (msg instanceof CustomProtocolV1Request request) {
            ctx.fireChannelRead(adapter.toExecutionRequest(request));
            return;
        }
        super.channelRead(ctx, msg);
    }
}
