package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.protocol.RespLimits;
import yier.bubu.redis.protocol.RespWireSkipper;

import java.util.List;

/**
 * RESP reply decoder（frame/zero-copy 取向）。
 * <p>
 * 该 decoder 只负责“切帧”：从 ByteBuf 中定位一个完整的 RESP reply，并输出 {@link NettyRespFrame}。
 * 语义解析（例如将 reply 转成 String/long/对象树）由上层按需完成，以减少分配与拷贝。
 */
public final class RespDecoder extends ByteToMessageDecoder {
    private final int maxBulkBytes;
    private final int maxArrayLen;
    private final int maxNestingDepth;
    private final int maxLineBytes;

    public RespDecoder() {
        this(RespLimits.DEFAULT_MAX_BULK_BYTES, RespLimits.DEFAULT_MAX_ARRAY_LEN, RespLimits.DEFAULT_MAX_NESTING_DEPTH, RespLimits.DEFAULT_MAX_LINE_BYTES);
    }

    public RespDecoder(int maxBulkBytes, int maxArrayLen, int maxNestingDepth, int maxLineBytes) {
        this.maxBulkBytes = RespDecodingSupport.requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArrayLen = RespDecodingSupport.requirePositive(maxArrayLen, "maxArrayLen");
        this.maxNestingDepth = RespDecodingSupport.requirePositive(maxNestingDepth, "maxNestingDepth");
        this.maxLineBytes = RespDecodingSupport.requirePositive(maxLineBytes, "maxLineBytes");
    }

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            int available = in.writerIndex() - startIdx;
            if (available <= 0) {
                return;
            }

            int endOffset = RespWireSkipper.trySkipOne(
                    new NettyBytesSource(in, startIdx),
                    0,
                    available,
                    maxBulkBytes,
                    maxArrayLen,
                    maxNestingDepth,
                    maxLineBytes
            );
            if (endOffset < 0) {
                return;
            }
            if (endOffset == 0) {
                throw new IllegalStateException("skipper must make progress");
            }

            ByteBuf frameBuf = in.retainedSlice(startIdx, endOffset);
            in.readerIndex(startIdx + endOffset);
            out.add(new NettyRespFrame(frameBuf));
        }
    }
}
