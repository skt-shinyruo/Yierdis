package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.protocol.json.JsonParseException;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ArgvRequest;
import yier.bubu.redis.protocol.v1.CustomProtocolV1RequestPayloadParser;

import java.util.List;

/**
 * Custom protocol v1 request decoder.
 * <p>
 * Wire framing: {@code <len>:<json-payload>\n}, where {@code <len>} is the UTF-8 byte length of the JSON payload.
 * <p>
 * Protocol errors are best-effort recoverable: the decoder emits {@link ProtocolError} events and resyncs by
 * discarding until the next {@code '\n'} when needed.
 */
public final class CustomRequestDecoder extends ByteToMessageDecoder {
    private static final byte COLON = (byte) ':';
    private static final byte LF = (byte) '\n';

    private enum State {
        READ_HEADER,
        READ_PAYLOAD,
        DISCARD_TO_LF
    }

    private final int maxPayloadBytes;
    private final int maxArgs;
    private final int maxHeaderBytes;
    private final int maxDiscardBytes;

    private State state = State.READ_HEADER;
    private int expectedPayloadLen;
    private int discardedBytes;

    public CustomRequestDecoder(int maxPayloadBytes, int maxArgs, int maxHeaderBytes) {
        this(maxPayloadBytes, maxArgs, maxHeaderBytes, Math.max(1024, maxPayloadBytes + maxHeaderBytes));
    }

    public CustomRequestDecoder(int maxPayloadBytes, int maxArgs, int maxHeaderBytes, int maxDiscardBytes) {
        this.maxPayloadBytes = Math.max(0, maxPayloadBytes);
        this.maxArgs = Math.max(0, maxArgs);
        this.maxHeaderBytes = Math.max(0, maxHeaderBytes);
        this.maxDiscardBytes = Math.max(0, maxDiscardBytes);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (ctx == null || in == null || out == null) {
            return;
        }
        for (; ; ) {
            if (state == State.DISCARD_TO_LF) {
                if (!discardToLf(ctx, in)) {
                    return;
                }
                state = State.READ_HEADER;
                continue;
            }

            if (state == State.READ_HEADER) {
                Integer len = tryReadLengthHeader(out, in);
                if (len == null) {
                    if (state == State.DISCARD_TO_LF) {
                        continue;
                    }
                    return;
                }
                expectedPayloadLen = len;
                state = State.READ_PAYLOAD;
            }

            if (state == State.READ_PAYLOAD) {
                if (in.readableBytes() < expectedPayloadLen + 1) {
                    return;
                }
                ByteBuf payload = in.readSlice(expectedPayloadLen);
                byte term = in.readByte();
                if (term != LF) {
                    enterDiscard(out, "Protocol error: missing frame terminator");
                    state = State.DISCARD_TO_LF;
                    continue;
                }

                // Enforce "single line payload": reject raw CR/LF bytes inside payload to keep resync predictable.
                if (containsCrLf(payload)) {
                    enterDiscard(out, "Protocol error: payload must be a single line");
                    // We already consumed the full length-prefixed payload and its '\n' terminator,
                    // so we are positioned at the next frame boundary. Discarding to the next LF would
                    // incorrectly drop subsequent frames.
                    expectedPayloadLen = 0;
                    state = State.READ_HEADER;
                    continue;
                }

                try {
                    CustomProtocolV1ArgvRequest request = parseCommandPayload(payload);
                    out.add(request);
                } catch (JsonParseException e) {
                    enterDiscard(out, "Protocol error: invalid JSON");
                } catch (IllegalArgumentException e) {
                    enterDiscard(out, "Protocol error: invalid request schema");
                } catch (Throwable t) {
                    enterDiscard(out, "Protocol error: decode failed");
                } finally {
                    expectedPayloadLen = 0;
                    state = State.READ_HEADER;
                }
            }
        }
    }

    private Integer tryReadLengthHeader(List<Object> out, ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        if (start >= end) {
            return null;
        }

        int scanLimit = end;
        if (maxHeaderBytes > 0) {
            scanLimit = Math.min(end, start + maxHeaderBytes);
        }

        int colonIdx = -1;
        for (int i = start; i < scanLimit; i++) {
            byte b = in.getByte(i);
            if (b == COLON) {
                colonIdx = i;
                break;
            }
            if (b < '0' || b > '9') {
                enterDiscard(out, "Protocol error: invalid length header");
                state = State.DISCARD_TO_LF;
                return null;
            }
        }

        if (colonIdx < 0) {
            if (scanLimit < end) {
                enterDiscard(out, "Protocol error: length header too long");
                state = State.DISCARD_TO_LF;
                return null;
            }
            return null;
        }

        int digits = colonIdx - start;
        if (digits <= 0) {
            enterDiscard(out, "Protocol error: empty length header");
            state = State.DISCARD_TO_LF;
            return null;
        }

        long len = 0L;
        for (int i = start; i < colonIdx; i++) {
            int d = (in.getByte(i) & 0xFF) - '0';
            len = len * 10L + (long) d;
            if (len > (long) Integer.MAX_VALUE) {
                enterDiscard(out, "Protocol error: length too large");
                state = State.DISCARD_TO_LF;
                return null;
            }
            if (maxPayloadBytes > 0 && len > (long) maxPayloadBytes) {
                enterDiscard(out, "Protocol error: payload too large");
                state = State.DISCARD_TO_LF;
                return null;
            }
        }

        in.readerIndex(colonIdx + 1);
        return (int) len;
    }

    private boolean discardToLf(ChannelHandlerContext ctx, ByteBuf in) {
        int start = in.readerIndex();
        int end = in.writerIndex();
        for (int i = start; i < end; i++) {
            if (in.getByte(i) == LF) {
                int toDiscard = i - start + 1;
                in.readerIndex(i + 1);
                discardedBytes = 0;
                return true;
            }
        }

        int readable = in.readableBytes();
        if (readable > 0) {
            in.readerIndex(end);
            discardedBytes += readable;
            if (maxDiscardBytes > 0 && discardedBytes > maxDiscardBytes) {
                // DoS safety: if we can't find a frame boundary for too long, close.
                ctx.close();
                return false;
            }
        }
        return false;
    }

    private void enterDiscard(List<Object> out, String message) {
        if (out == null) {
            return;
        }
        // decoder 只输出协议错误事件；具体 NDJSON 编码由上层 handler 统一处理（避免重复与漂移）。
        out.add(new ProtocolError(message));
    }

    private CustomProtocolV1ArgvRequest parseCommandPayload(ByteBuf payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        int off = payload.readerIndex();
        int len = payload.readableBytes();
        if (len <= 0) {
            throw new JsonParseException("Invalid JSON");
        }

        if (payload.hasArray()) {
            byte[] arr = payload.array();
            int base = payload.arrayOffset() + off;
            return CustomProtocolV1RequestPayloadParser.parse(arr, base, len, maxArgs);
        }

        // Fallback: some composite buffers can't expose a single ByteBuffer view for the payload range.
        byte[] copy = new byte[len];
        payload.getBytes(off, copy);
        return CustomProtocolV1RequestPayloadParser.parse(copy, 0, copy.length, maxArgs);
    }

    private static boolean containsCrLf(ByteBuf payload) {
        if (payload == null || !payload.isReadable()) {
            return false;
        }
        int start = payload.readerIndex();
        int end = payload.writerIndex();
        for (int i = start; i < end; i++) {
            byte b = payload.getByte(i);
            if (b == '\n' || b == '\r') {
                return true;
            }
        }
        return false;
    }
}
