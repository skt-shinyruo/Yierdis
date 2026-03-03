package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.protocol.json.JsonWriter;
import yier.bubu.redis.protocol.reply.ReplyErrorKind;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * Custom protocol v1 reply writer: NDJSON (one JSON object per reply, terminated by {@code '\n'}).
 * <p>
 * Success envelope: {@code {"ok":true,"result":...}\n}
 * Error envelope: {@code {"ok":false,"error":{"kind":"...","message":"..."}}\n}
 */
public final class JsonLineReplyWriter implements ReplyWriter {
    private static final byte[] OK_PREFIX = "{\"ok\":true,\"result\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OK_SUFFIX = "}\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] LBRACKET = new byte[]{'['};
    private static final byte[] RBRACKET = new byte[]{']'};
    private static final byte[] COMMA = new byte[]{','};

    private final BytesSink out;

    private boolean closeAfterReplyRequested;

    private boolean envelopeStarted;
    private boolean finished;

    private final Deque<Container> stack = new ArrayDeque<>(8);

    private enum ContainerType {
        ARRAY,
        MAP
    }

    private static final class Container {
        final ContainerType type;
        int remaining;
        boolean first = true;
        boolean expectingKey;

        private Container(ContainerType type, int remaining) {
            this.type = type;
            this.remaining = remaining;
            this.expectingKey = type == ContainerType.MAP;
        }
    }

    private static final ThreadLocal<CharsetDecoder> TL_UTF8_DECODER = ThreadLocal.withInitial(() ->
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
    );

    public JsonLineReplyWriter(BytesSink out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    @Override
    public void requestCloseAfterReply() {
        this.closeAfterReplyRequested = true;
    }

    @Override
    public boolean closeAfterReplyRequested() {
        return closeAfterReplyRequested;
    }

    // --- Scalars ---

    @Override
    public void simpleString(String value) {
        writeStringValue(value);
    }

    @Override
    public void error(String message) {
        writeErrorValue(ReplyErrorKind.COMMAND, message);
    }

    @Override
    public void protocolError(String message) {
        writeErrorValue(ReplyErrorKind.PROTOCOL, message);
    }

    @Override
    public void internalError(String message) {
        writeErrorValue(ReplyErrorKind.INTERNAL, message);
    }

    @Override
    public void integer(long value) {
        writeNumberValue(Long.toString(value));
    }

    @Override
    public void booleanValue(boolean value) {
        beginValueOrKey();
        out.writeBytes(value ? TRUE : FALSE);
        finishValue();
    }

    @Override
    public void doubleValue(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("double must be finite");
        }
        writeNumberValue(Double.toString(value));
    }

    @Override
    public void bigNumberAscii(String value) {
        // Best-effort: represent as a JSON string to avoid integer overflow / precision loss.
        writeStringValue(value == null ? null : value.trim());
    }

    @Override
    public void verbatimString(String format, byte[] data) {
        String prefix = format == null ? "" : format.trim();
        String s = strictUtf8ToStringOrNull(data, 0, data == null ? 0 : data.length);
        if (s == null) {
            String b64 = data == null ? "" : Base64.getEncoder().encodeToString(data);
            s = "b64:" + b64;
        }
        writeStringValue(prefix.isEmpty() ? s : (prefix + ":" + s));
    }

    @Override
    public void blobError(String message) {
        writeErrorValue(ReplyErrorKind.COMMAND, message);
    }

    // --- Bulk / bytes ---

    @Override
    public void bulkString(byte[] data) {
        if (data == null) {
            nullValue();
            return;
        }
        bulkString(data, 0, data.length);
    }

    @Override
    public void bulkString(byte[] data, int off, int len) {
        if (data == null) {
            nullValue();
            return;
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        writeBytesValue(data, off, len);
    }

    @Override
    public void bulkString(BytesSlice slice) {
        if (slice == null) {
            nullValue();
            return;
        }
        int len = Math.max(0, slice.length());
        if (len == 0) {
            writeStringValue("");
            return;
        }
        writeBytesValue(slice);
    }

    @Override
    public void bulkStringLongAscii(long value) {
        // Bulk-string semantics: represent the long as an ASCII decimal string (encoded as JSON string).
        writeStringValue(Long.toString(value));
    }

    // --- Aggregates ---

    @Override
    public void nullValue() {
        beginValueOrKey();
        out.writeBytes(NULL);
        finishValue();
    }

    @Override
    public void nullArray() {
        nullValue();
    }

    @Override
    public void arrayHeader(int count) {
        beginValueOrKey();

        int n = Math.max(0, count);
        out.writeBytes(LBRACKET);
        if (n == 0) {
            out.writeBytes(RBRACKET);
            finishValue();
            return;
        }
        stack.push(new Container(ContainerType.ARRAY, n));
    }

    @Override
    public void bulkStringArray(List<byte[]> values) {
        if (values == null) {
            nullArray();
            return;
        }
        arrayHeader(values.size());
        for (int i = 0; i < values.size(); i++) {
            bulkString(values.get(i));
        }
    }

    @Override
    public void emptyArray() {
        arrayHeader(0);
    }

    @Override
    public void mapHeader(int pairs) {
        beginValueOrKey();

        int n = Math.max(0, pairs);
        CustomProtocolV1NdjsonEncoder.writeMapPrefix(out);
        if (n == 0) {
            CustomProtocolV1NdjsonEncoder.writeMapSuffix(out);
            finishValue();
            return;
        }
        stack.push(new Container(ContainerType.MAP, n));
    }

    @Override
    public void setHeader(int count) {
        arrayHeader(count);
    }

    @Override
    public void pushHeader(int count) {
        arrayHeader(count);
    }

    @Override
    public void attributeHeader(int pairs) {
        mapHeader(pairs);
    }

    // --- Internals ---

    private void writeErrorValue(ReplyErrorKind kind, String message) {
        if (finished) {
            return;
        }

        if (stack.isEmpty() && !envelopeStarted) {
            // Top-level error: write the error envelope and finish the reply.
            CustomProtocolV1NdjsonEncoder.writeErrorEnvelope(out, kind, message);
            finished = true;
            return;
        }

        // Nested error value (e.g., EXEC array element): encode as a tagged JSON object value.
        beginValueOrKey();
        CustomProtocolV1NdjsonEncoder.writeNestedErrorValue(out, kind, message);
        finishValue();
    }

    private void writeStringValue(String value) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        JsonWriter.writeString(out, value);
        finishValue();
    }

    private void writeNumberValue(String asciiNumber) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        byte[] bytes = (asciiNumber == null ? "0" : asciiNumber).getBytes(StandardCharsets.US_ASCII);
        out.writeBytes(bytes, 0, bytes.length);
        finishValue();
    }

    private void writeBytesValue(byte[] data, int off, int len) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(out, data, off, len);
        finishValue();
    }

    private void writeBytesValue(BytesSlice slice) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        CustomProtocolV1NdjsonEncoder.writeBytesValue(out, slice);
        finishValue();
    }

    private void beginValueOrKey() {
        if (finished) {
            return;
        }
        if (stack.isEmpty() && !envelopeStarted) {
            out.writeBytes(OK_PREFIX);
            envelopeStarted = true;
            return;
        }

        if (stack.isEmpty()) {
            // Envelope started and no container: the next value write would be a duplicate top-level result.
            return;
        }

        Container c = stack.peek();
        if (c == null) {
            return;
        }

        if (c.type == ContainerType.ARRAY) {
            if (!c.first) {
                out.writeBytes(COMMA);
            }
            c.first = false;
            return;
        }

        if (c.type == ContainerType.MAP) {
            if (c.expectingKey) {
                if (!c.first) {
                    out.writeBytes(COMMA);
                }
                c.first = false;
                out.writeBytes(LBRACKET);
                return;
            }
            out.writeBytes(COMMA);
        }
    }

    private void finishValue() {
        if (finished) {
            return;
        }

        if (stack.isEmpty()) {
            // Top-level success value finished.
            if (envelopeStarted) {
                out.writeBytes(OK_SUFFIX);
                finished = true;
            }
            return;
        }

        Container c = stack.peek();
        if (c == null) {
            return;
        }

        if (c.type == ContainerType.ARRAY) {
            c.remaining--;
            if (c.remaining <= 0) {
                out.writeBytes(RBRACKET);
                stack.pop();
                finishValue();
            }
            return;
        }

        if (c.type == ContainerType.MAP) {
            if (c.expectingKey) {
                // Finishing a key: next value will be the pair's value.
                c.expectingKey = false;
                return;
            }

            // Finishing a value completes one pair.
            out.writeBytes(RBRACKET);
            c.remaining--;
            c.expectingKey = true;
            if (c.remaining <= 0) {
                CustomProtocolV1NdjsonEncoder.writeMapSuffix(out);
                stack.pop();
                finishValue();
            }
        }
    }

    private static String strictUtf8ToStringOrNull(byte[] data, int off, int len) {
        if (data == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        CharsetDecoder dec = TL_UTF8_DECODER.get();
        dec.reset();
        try {
            CharBuffer cb = dec.decode(ByteBuffer.wrap(data, off, len));
            return cb.toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
