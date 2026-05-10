package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import yier.bubu.redis.protocol.resp.RespCommandRequest;
import yier.bubu.redis.protocol.resp.RespExecutionAdapter;

public final class RespCommandAdapter extends ChannelInboundHandlerAdapter {
    private final RespExecutionAdapter adapter;

    public RespCommandAdapter() {
        this(RespExecutionAdapter.DEFAULT);
    }

    public RespCommandAdapter(RespExecutionAdapter adapter) {
        this.adapter = adapter == null ? RespExecutionAdapter.DEFAULT : adapter;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (ctx == null || msg == null) {
            return;
        }
        if (msg instanceof RespCommandRequest request) {
            ctx.fireChannelRead(adapter.toExecutionRequest(request));
            return;
        }
        super.channelRead(ctx, msg);
    }
}
