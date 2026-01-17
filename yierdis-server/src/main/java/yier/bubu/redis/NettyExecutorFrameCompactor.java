package yier.bubu.redis;

// 执行器提交阶段的 frame compaction：将“长度小但 retained 大”的派生 ByteBuf 拷贝为精确大小，降低驻留与 bytes 预算扭曲风险。

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.netty.NettyRespFrame;

final class NettyExecutorFrameCompactor {
    private final long thresholdBytes;
    private final double ratio;
    private final int maxCopyBytes;

    NettyExecutorFrameCompactor(long thresholdBytes, double ratio, int maxCopyBytes) {
        this.thresholdBytes = thresholdBytes;
        this.ratio = ratio;
        this.maxCopyBytes = maxCopyBytes;
    }

    void tryCompact(ChannelHandlerContext ctx, RespCommand cmd) {
        if (thresholdBytes <= 0) {
            return;
        }
        if (ctx == null || cmd == null) {
            return;
        }

        RespFrame frame = cmd.frame();
        if (!(frame instanceof NettyRespFrame nettyFrame)) {
            return;
        }

        int length = nettyFrame.length();
        if (length <= 0 || length > maxCopyBytes) {
            return;
        }

        int retained = nettyFrame.retainedBytes();
        if (retained <= length) {
            return;
        }
        if ((long) retained < thresholdBytes) {
            return;
        }
        if ((double) retained < (double) length * ratio) {
            return;
        }

        ByteBuf src = nettyFrame.unwrap();
        if (src == null) {
            return;
        }

        ByteBuf copied = ctx.alloc().buffer(length, length);
        boolean ok = false;
        try {
            copied.writeBytes(src, 0, length);
            RespCommandBuilder.replaceFrame(cmd, new NettyRespFrame(copied));
            ok = true;
        } finally {
            if (!ok) {
                copied.release();
            }
        }
    }
}

