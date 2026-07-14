package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Objects;

/**
 * 将已授予的 receive credit 变为随实际 ByteBuf 生命周期释放的 component lease。
 */
public final class InboundByteAccountingHandler extends ChannelInboundHandlerAdapter {
    private final InboundReadCreditHandler readCredits;

    public InboundByteAccountingHandler(InboundReadCreditHandler readCredits) {
        this.readCredits = Objects.requireNonNull(readCredits, "readCredits");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf input)) {
            super.channelRead(ctx, msg);
            return;
        }

        InboundBufferLease lease = readCredits.takeInputLease(input);
        if (lease == null) {
            readCredits.rejectInbound(input);
            ctx.fireChannelRead(new RespProtocolError("ERR request exceeds configured memory limit", true));
            return;
        }
        ctx.fireChannelRead(new AccountedInboundBuffer(input, lease));
    }
}
