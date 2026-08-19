package yier.bubu.redis.protocol.resp.netty;

import io.netty.channel.ChannelHandlerContext;

/**
 * 在 RESP 解码完成但尚未向下游传播前执行的连接本地准入边界。
 * 返回 {@link Admission#ADMITTED} 时，实现必须已经接管解码结果的所有权：结果已通过
 * Netty 消息边界转交，或者在转交失败后完成终结；其余结果不得转交。
 */
public interface RespDecodedMessageGate {
    RespDecodedMessageGate PASS_THROUGH = (ctx, decoded, resumeOnEventLoop) -> {
        ctx.fireChannelRead(decoded);
        return Admission.ADMITTED;
    };

    Admission tryAdmit(ChannelHandlerContext ctx, RespDecodedMessage decoded, Runnable resumeOnEventLoop);

    enum Admission {
        ADMITTED,
        WAITING,
        CLOSED
    }
}
