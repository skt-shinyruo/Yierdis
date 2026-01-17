package yier.bubu.redis.protocol.netty;

// RESP 请求解码器：支持 RESP2 array-of-bulk-strings 与 inline command，并显式拒绝 reply 前缀以避免误解析。

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespInlineCommandParser;
import yier.bubu.redis.protocol.RespLimits;

import java.util.List;

/**
 * Fast path decoder for Redis client requests.
 * <p>
 * Supports the common command form: an array of bulk strings (RESP2).
 * It avoids building generic {@link RespObject} trees.
 */
public final class RespCommandDecoder extends ByteToMessageDecoder {
    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxLineBytes;

    public RespCommandDecoder() {
        this(RespLimits.DEFAULT_MAX_BULK_BYTES, RespLimits.DEFAULT_MAX_ARGS, RespLimits.DEFAULT_MAX_LINE_BYTES);
    }

    public RespCommandDecoder(int maxBulkBytes, int maxArgs, int maxLineBytes) {
        this.maxBulkBytes = RespDecodingSupport.requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArgs = RespDecodingSupport.requirePositive(maxArgs, "maxArgs");
        this.maxLineBytes = RespDecodingSupport.requirePositive(maxLineBytes, "maxLineBytes");
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            RespCommand cmd = tryDecodeOne(in);
            if (cmd == null) {
                in.readerIndex(startIdx);
                return;
            }
            out.add(cmd);
        }
    }

    private RespCommand tryDecodeOne(ByteBuf in) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return null;
        }

        byte first = in.getByte(startIdx);
        if (first == '*') {
            return decodeArrayOfBulkStrings(in, startIdx);
        }
        // 严格区分 request 与 reply：禁止将 RESP reply（含 RESP3 类型前缀）误判为 inline command。
        // 允许的 request：
        // 1) RESP2 array-of-bulk-strings（以 '*' 开头）
        // 2) inline command（用于调试；sdssplitargs 风格）
        if (isRespReplyPrefix(first)) {
            throw new IllegalArgumentException("Protocol error: expected array");
        }
        if (isInvalidRequestPrefix(first)) {
            throw new IllegalArgumentException("Protocol error: invalid request");
        }
        return decodeInlineCommand(in, startIdx);
    }

    private static boolean isRespReplyPrefix(byte b) {
        // RESP2 reply:
        // + simple string, - error, : integer, $ bulk string, * array
        // RESP3 types we may see from buggy clients:
        // _ null, % map, # boolean, , double, ( big number, ~ set, > push, = verbatim, ! blob error, | attribute
        // 说明：这里不需要覆盖全部 RESP3 类型，但覆盖常见前缀可以避免误路由为 inline command。
        switch (b) {
            case '+':
            case '-':
            case ':':
            case '$':
            case '*':
            case '_':
            case '%':
            case '#':
            case ',':
            case '(':
            case '~':
            case '>':
            case '=':
            case '!':
            case '|':
                return true;
            default:
                return false;
        }
    }

    private static boolean isInvalidRequestPrefix(byte b) {
        // 允许空格/制表符用于 inline command 的前导空白，其余控制字符直接判为协议错误。
        int x = b & 0xFF;
        if (x == ' ' || x == '\t') {
            return false;
        }
        return x < 0x20 || x == 0x7F;
    }

    private RespCommand decodeArrayOfBulkStrings(ByteBuf in, int startIdx) {
        in.readByte(); // consume '*'

        int argcLineStart = in.readerIndex();
        int argcLineEnd = RespDecodingSupport.indexOfCrlf(in, argcLineStart, maxLineBytes);
        if (argcLineEnd < 0) {
            if (in.writerIndex() - argcLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        int argc = RespDecodingSupport.parseIntAscii(in, argcLineStart, argcLineEnd);
        if (argc < 0) {
            throw new IllegalArgumentException("Protocol error: invalid array length");
        }
        if (argc > maxArgs) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }

        in.readerIndex(argcLineEnd + 2); // consume CRLF

        RespCommand cmd = RespCommandBuilder.acquire(argc);
        try {
            for (int i = 0; i < argc; i++) {
                if (!in.isReadable()) {
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                byte bulkPrefix = in.readByte();
                if (bulkPrefix != '$') {
                    throw new IllegalArgumentException("Protocol error: expected bulk string");
                }

                int lenLineStart = in.readerIndex();
                int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
                if (lenLineEnd < 0) {
                    if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len == -1) {
                    // Null bulk string.
                    in.readerIndex(lenLineEnd + 2);
                    RespCommandBuilder.setArgNull(cmd, i);
                    continue;
                }
                if (len < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid bulk length");
                }
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
                }

                int dataStart = lenLineEnd + 2;
                int dataEnd = dataStart + len;
                int end = dataEnd + 2;
                if (in.writerIndex() < end) {
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                RespCommandBuilder.setArgSlice(cmd, i, dataStart - startIdx, len);
                in.readerIndex(end);
            }

            int endIdx = in.readerIndex();
            ByteBuf frameBuf = in.retainedSlice(startIdx, endIdx - startIdx);
            NettyRespFrame frame = new NettyRespFrame(frameBuf);
            boolean ok = false;
            try {
                RespCommandBuilder.setFrame(cmd, frame);
                ok = true;
            } finally {
                if (!ok) {
                    frame.close();
                }
            }
            return cmd;
        } catch (RuntimeException e) {
            cmd.close();
            throw e;
        }
    }

    private RespCommand decodeInlineCommand(ByteBuf in, int startIdx) {
        int lineEnd = RespDecodingSupport.indexOfCrlf(in, startIdx, maxLineBytes);
        if (lineEnd < 0) {
            if (in.writerIndex() - startIdx > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        // Inline command parsing follows Redis-like rules (sdssplitargs) and is shared with CLI parsing.
        // Since escapes require decoding, inline commands are materialized into a small heap buffer.
        int inputLen = lineEnd - startIdx;
        byte[] input = new byte[inputLen];
        in.getBytes(startIdx, input);
        RespInlineCommandParser.Decoded decoded = RespInlineCommandParser.parse(input, 0, inputLen, maxArgs);

        // Consume the line (+ CRLF) before we slice; this keeps the control flow aligned with the RESP path.
        in.readerIndex(lineEnd + 2);

        RespCommand cmd = RespCommandBuilder.acquire(decoded.argc());
        for (int arg = 0; arg < decoded.argc(); arg++) {
            RespCommandBuilder.setArgSlice(cmd, arg, decoded.offset(arg), decoded.length(arg));
        }

        ByteBuf frameBuf = Unpooled.wrappedBuffer(decoded.decoded(), 0, decoded.decodedLen());
        NettyRespFrame frame = new NettyRespFrame(frameBuf);
        boolean ok = false;
        try {
            RespCommandBuilder.setFrame(cmd, frame);
            ok = true;
        } finally {
            if (!ok) {
                frame.close();
            }
        }
        return cmd;
    }

    // 兼容旧代码：将参数校验逻辑委托到公共工具类，避免 request/response 双轨漂移。
    private static int requirePositive(int value, String name) {
        return RespDecodingSupport.requirePositive(value, name);
    }
}
