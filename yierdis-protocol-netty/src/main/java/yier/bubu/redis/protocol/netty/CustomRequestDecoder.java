package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.protocol.json.JsonArray;
import yier.bubu.redis.protocol.json.JsonLimits;
import yier.bubu.redis.protocol.json.JsonNull;
import yier.bubu.redis.protocol.json.JsonObject;
import yier.bubu.redis.protocol.json.JsonParseException;
import yier.bubu.redis.protocol.json.JsonParser;
import yier.bubu.redis.protocol.json.JsonString;
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.json.JsonWriter;
import yier.bubu.redis.protocol.v1.CustomCommand;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Custom protocol v1 request decoder.
 * <p>
 * Wire framing: {@code <len>:<json-payload>\n}, where {@code <len>} is the UTF-8 byte length of the JSON payload.
 * <p>
 * Protocol errors are best-effort recoverable: the decoder writes an error reply and resyncs by discarding until the
 * next {@code '\n'}.
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
                Integer len = tryReadLengthHeader(ctx, in);
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
                    enterDiscard(ctx, "Protocol error: missing frame terminator");
                    state = State.DISCARD_TO_LF;
                    continue;
                }

                // Enforce "single line payload": reject raw CR/LF bytes inside payload to keep resync predictable.
                if (containsCrLf(payload)) {
                    enterDiscard(ctx, "Protocol error: payload must be a single line");
                    state = State.DISCARD_TO_LF;
                    continue;
                }

                try {
                    CustomCommand cmd = parseCommandPayload(payload);
                    out.add(cmd);
                } catch (JsonParseException e) {
                    enterDiscard(ctx, "Protocol error: invalid JSON");
                } catch (IllegalArgumentException e) {
                    enterDiscard(ctx, "Protocol error: invalid request schema");
                } catch (Throwable t) {
                    enterDiscard(ctx, "Protocol error: decode failed");
                } finally {
                    expectedPayloadLen = 0;
                    state = State.READ_HEADER;
                }
            }
        }
    }

    private Integer tryReadLengthHeader(ChannelHandlerContext ctx, ByteBuf in) {
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
                enterDiscard(ctx, "Protocol error: invalid length header");
                state = State.DISCARD_TO_LF;
                return null;
            }
        }

        if (colonIdx < 0) {
            if (scanLimit < end) {
                enterDiscard(ctx, "Protocol error: length header too long");
                state = State.DISCARD_TO_LF;
                return null;
            }
            return null;
        }

        int digits = colonIdx - start;
        if (digits <= 0) {
            enterDiscard(ctx, "Protocol error: empty length header");
            state = State.DISCARD_TO_LF;
            return null;
        }

        long len = 0L;
        for (int i = start; i < colonIdx; i++) {
            int d = (in.getByte(i) & 0xFF) - '0';
            len = len * 10L + (long) d;
            if (len > (long) Integer.MAX_VALUE) {
                enterDiscard(ctx, "Protocol error: length too large");
                state = State.DISCARD_TO_LF;
                return null;
            }
            if (maxPayloadBytes > 0 && len > (long) maxPayloadBytes) {
                enterDiscard(ctx, "Protocol error: payload too large");
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

    private void enterDiscard(ChannelHandlerContext ctx, String message) {
        writeProtocolError(ctx, message);
    }

    private void writeProtocolError(ChannelHandlerContext ctx, String message) {
        if (ctx == null) {
            return;
        }
        String msg = safeErrorMessage(message);

        ByteBuf out = ctx.alloc().buffer();
        boolean ok = false;
        try {
            ByteBuf buf = out;
            BytesSink sink = (src, srcIndex, len) -> buf.writeBytes(src, srcIndex, len);

            // {"ok":false,"error":{"kind":"protocol","message":"..."} }\n
            sink.writeBytes("{\"ok\":false,\"error\":{\"kind\":\"protocol\",\"message\":".getBytes(StandardCharsets.US_ASCII));
            JsonWriter.writeString(sink, msg);
            sink.writeBytes("}}\n".getBytes(StandardCharsets.US_ASCII));

            ctx.writeAndFlush(out);
            ok = true;
        } finally {
            if (!ok) {
                out.release();
            }
        }
    }

    private static String safeErrorMessage(String message) {
        String msg = message;
        if (msg == null || msg.isBlank()) {
            msg = "Protocol error";
        }
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (msg.length() > 256) {
            msg = msg.substring(0, 256);
        }
        return msg;
    }

    private CustomCommand parseCommandPayload(ByteBuf payload) {
        JsonValue v = parsePayloadJson(payload);
        if (!(v instanceof JsonObject obj)) {
            throw new IllegalArgumentException("request must be a JSON object");
        }

        Map<String, JsonValue> map = obj.values();
        JsonValue cmdVal = map.get("cmd");
        if (!(cmdVal instanceof JsonString)) {
            throw new IllegalArgumentException("cmd must be a string");
        }
        String cmd = ((JsonString) cmdVal).value();

        JsonValue argsVal = map.get("args");
        ArrayList<String> args = new ArrayList<>();
        if (argsVal == null || argsVal instanceof JsonNull) {
            // no args
        } else if (argsVal instanceof JsonArray arr) {
            List<JsonValue> values = arr.values();
            if (values != null && !values.isEmpty()) {
                for (JsonValue a : values) {
                    if (a == null || a instanceof JsonNull) {
                        args.add(null);
                        continue;
                    }
                    if (a instanceof JsonString s) {
                        args.add(s.value());
                        continue;
                    }
                    throw new IllegalArgumentException("args elements must be string|null");
                }
            }
        } else {
            throw new IllegalArgumentException("args must be an array");
        }

        int argc = 1 + args.size();
        if (maxArgs > 0 && argc > maxArgs) {
            throw new IllegalArgumentException("too many args");
        }
        return new CustomCommand(cmd, args);
    }

    private static JsonValue parsePayloadJson(ByteBuf payload) {
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
            return JsonParser.parseStrictUtf8(arr, base, len, JsonLimits.DEFAULT);
        }

        if (payload.nioBufferCount() == 1) {
            ByteBuffer buf = payload.nioBuffer(off, len);
            return JsonParser.parseStrictUtf8(buf, JsonLimits.DEFAULT);
        }

        // Fallback: some composite buffers can't expose a single ByteBuffer view for the payload range.
        byte[] copy = new byte[len];
        payload.getBytes(off, copy);
        return JsonParser.parseStrictUtf8(copy, 0, copy.length, JsonLimits.DEFAULT);
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
