package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.Objects;

/**
 * 输入处理器向解码器转交 ByteBuf 所有权及其对应的预算额度。
 */
final class AccountedInboundBuffer implements AutoCloseable {
    private final ByteBuf buffer;
    private final InboundBufferLease lease;

    AccountedInboundBuffer(ByteBuf buffer, InboundBufferLease lease) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.lease = Objects.requireNonNull(lease, "lease");
    }

    ByteBuf takeBuffer() {
        return buffer;
    }

    InboundBufferLease takeLease() {
        return lease;
    }

    @Override
    public void close() {
        ReferenceCountUtil.safeRelease(buffer);
        lease.close();
    }
}
