package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;

import yier.bubu.redis.protocol.RespFrame;

public final class NettyRespFrame implements RespFrame {
    private ByteBuf buf;
    private final int length;
    private final int retainedBytes;

    public NettyRespFrame(ByteBuf buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        this.buf = buf;
        this.length = buf.readableBytes();
        this.retainedBytes = estimateRetainedBytes(buf, this.length);
    }

    public ByteBuf unwrap() {
        return buf;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int retainedBytes() {
        return retainedBytes;
    }

    @Override
    public byte getByte(int index) {
        return buf.getByte(index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        buf.getBytes(index, dst, dstOff, len);
    }

    @Override
    public boolean hasMemoryAddress() {
        return buf.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        if (!buf.hasMemoryAddress()) {
            return RespFrame.super.memoryAddress();
        }
        return buf.memoryAddress();
    }

    @Override
    public void close() {
        if (buf != null) {
            buf.release();
            buf = null;
        }
    }

    private static int estimateRetainedBytes(ByteBuf buf, int length) {
        if (buf == null) {
            return Math.max(0, length);
        }
        int len = Math.max(0, length);
        ByteBuf root = unwrapRoot(buf);
        long cap = Math.max(0, (long) root.capacity());
        int refCnt;
        try {
            refCnt = root.refCnt();
        } catch (Throwable ignored) {
            refCnt = 1;
        }
        if (refCnt <= 0) {
            refCnt = 1;
        }

        long share = cap / refCnt;
        long estimated = Math.max((long) len, share);
        if (cap > 0) {
            estimated = Math.min(cap, estimated);
        }
        if (estimated > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) estimated;
    }

    private static ByteBuf unwrapRoot(ByteBuf buf) {
        ByteBuf cur = buf;
        for (; ; ) {
            ByteBuf next;
            try {
                next = cur.unwrap();
            } catch (Throwable ignored) {
                next = null;
            }
            if (next == null || next == cur) {
                return cur;
            }
            cur = next;
        }
    }
}
