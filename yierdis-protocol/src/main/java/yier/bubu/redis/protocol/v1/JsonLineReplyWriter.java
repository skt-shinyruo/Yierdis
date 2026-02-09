package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.Session;
import yier.bubu.redis.protocol.json.JsonWriter;

import java.nio.charset.StandardCharsets;
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

    private static final byte[] ERR_PREFIX_PROTOCOL = "{\"ok\":false,\"error\":{\"kind\":\"protocol\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_PREFIX_COMMAND = "{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_PREFIX_INTERNAL = "{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_SUFFIX = "}}\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] VALUE_ERR_PREFIX_PROTOCOL = "{\"error\":{\"kind\":\"protocol\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_PREFIX_COMMAND = "{\"error\":{\"kind\":\"command\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_PREFIX_INTERNAL = "{\"error\":{\"kind\":\"internal\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_SUFFIX = "}}".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] LBRACKET = new byte[]{'['};
    private static final byte[] RBRACKET = new byte[]{']'};
    private static final byte[] LBRACE = new byte[]{'{'};
    private static final byte[] RBRACE = new byte[]{'}'};
    private static final byte[] COMMA = new byte[]{','};
    private static final byte[] COLON = new byte[]{':'};

    private static final int MAX_ERROR_MESSAGE_CHARS = 256;

    private final BytesSink out;
    private final Session session;

    private boolean closeAfterReplyRequested;

    private boolean envelopeStarted;
    private boolean finished;

    private final Deque<Container> stack = new ArrayDeque<>(8);

    private enum ContainerType {
        ARRAY,
        OBJECT
    }

    private static final class Container {
        final ContainerType type;
        int remaining;
        boolean first = true;
        boolean expectingKey;

        private Container(ContainerType type, int remaining) {
            this.type = type;
            this.remaining = remaining;
            this.expectingKey = type == ContainerType.OBJECT;
        }
    }

    public JsonLineReplyWriter(BytesSink out) {
        this(out, null);
    }

    public JsonLineReplyWriter(BytesSink out, Session session) {
        this.out = Objects.requireNonNull(out, "out");
        this.session = session;
    }

    @Override
    public Session session() {
        return session;
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
        writeErrorValue(ErrorKind.COMMAND, message);
    }

    @Override
    public void protocolError(String message) {
        writeErrorValue(ErrorKind.PROTOCOL, message);
    }

    @Override
    public void internalError(String message) {
        writeErrorValue(ErrorKind.INTERNAL, message);
    }

    @Override
    public void integer(long value) {
        writeNumberValue(Long.toString(value));
    }

    @Override
    public void booleanValue(boolean value) {
        beginValueOrKey();
        if (isExpectingKey()) {
            writeObjectKey(value ? "true" : "false");
            return;
        }
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
        String s = decodeUtf8(data, 0, data == null ? 0 : data.length);
        writeStringValue(prefix.isEmpty() ? s : (prefix + ":" + s));
    }

    @Override
    public void blobError(String message) {
        writeErrorValue(ErrorKind.COMMAND, message);
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
        writeStringValue(decodeUtf8(data, off, len));
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
        byte[] data = new byte[len];
        slice.getBytes(0, data, 0, len);
        writeStringValue(decodeUtf8(data, 0, len));
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
        if (isExpectingKey()) {
            writeObjectKey("null");
            return;
        }
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
        if (isExpectingKey()) {
            // Keys must be strings; fall back to a placeholder.
            writeObjectKey("array");
        }

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
        if (isExpectingKey()) {
            // Keys must be strings; fall back to a placeholder.
            writeObjectKey("map");
        }

        int n = Math.max(0, pairs);
        out.writeBytes(LBRACE);
        if (n == 0) {
            out.writeBytes(RBRACE);
            finishValue();
            return;
        }
        stack.push(new Container(ContainerType.OBJECT, n));
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

    private enum ErrorKind {
        PROTOCOL,
        COMMAND,
        INTERNAL
    }

    private void writeErrorValue(ErrorKind kind, String message) {
        if (finished) {
            return;
        }

        String msg = safeErrorMessage(message);
        if (stack.isEmpty() && !envelopeStarted) {
            // Top-level error: write the error envelope and finish the reply.
            out.writeBytes(errorEnvelopePrefix(kind));
            JsonWriter.writeString(out, msg);
            out.writeBytes(ERR_SUFFIX);
            finished = true;
            return;
        }

        // Nested error value (e.g., EXEC array element): encode as a JSON object value.
        beginValueOrKey();
        if (isExpectingKey()) {
            writeObjectKey("error");
        }
        out.writeBytes(errorValuePrefix(kind));
        JsonWriter.writeString(out, msg);
        out.writeBytes(VALUE_ERR_SUFFIX);
        finishValue();
    }

    private byte[] errorEnvelopePrefix(ErrorKind kind) {
        return switch (kind) {
            case PROTOCOL -> ERR_PREFIX_PROTOCOL;
            case COMMAND -> ERR_PREFIX_COMMAND;
            case INTERNAL -> ERR_PREFIX_INTERNAL;
        };
    }

    private byte[] errorValuePrefix(ErrorKind kind) {
        return switch (kind) {
            case PROTOCOL -> VALUE_ERR_PREFIX_PROTOCOL;
            case COMMAND -> VALUE_ERR_PREFIX_COMMAND;
            case INTERNAL -> VALUE_ERR_PREFIX_INTERNAL;
        };
    }

    private void writeStringValue(String value) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        if (isExpectingKey()) {
            writeObjectKey(value == null ? "null" : value);
            return;
        }
        JsonWriter.writeString(out, value);
        finishValue();
    }

    private void writeNumberValue(String asciiNumber) {
        if (finished) {
            return;
        }
        beginValueOrKey();
        if (isExpectingKey()) {
            writeObjectKey(asciiNumber == null ? "0" : asciiNumber);
            return;
        }
        byte[] bytes = (asciiNumber == null ? "0" : asciiNumber).getBytes(StandardCharsets.US_ASCII);
        out.writeBytes(bytes, 0, bytes.length);
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

        if (c.type == ContainerType.OBJECT) {
            if (c.expectingKey) {
                if (!c.first) {
                    out.writeBytes(COMMA);
                }
                c.first = false;
            }
            // For values, ':' is written by writeObjectKey().
        }
    }

    private boolean isExpectingKey() {
        if (stack.isEmpty()) {
            return false;
        }
        Container c = stack.peek();
        return c != null && c.type == ContainerType.OBJECT && c.expectingKey;
    }

    private void writeObjectKey(String key) {
        Container c = stack.peek();
        if (c == null || c.type != ContainerType.OBJECT || !c.expectingKey) {
            // Not in key position: write as a value.
            JsonWriter.writeString(out, key);
            finishValue();
            return;
        }
        JsonWriter.writeString(out, key == null ? "null" : key);
        out.writeBytes(COLON);
        c.expectingKey = false;
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

        if (c.type == ContainerType.OBJECT) {
            // Finishing a value completes one pair.
            c.remaining--;
            c.expectingKey = true;
            if (c.remaining <= 0) {
                out.writeBytes(RBRACE);
                stack.pop();
                finishValue();
            }
        }
    }

    private static String safeErrorMessage(String message) {
        String msg = message;
        if (msg == null || msg.isBlank()) {
            msg = "ERR error";
        }
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (msg.length() > MAX_ERROR_MESSAGE_CHARS) {
            msg = msg.substring(0, MAX_ERROR_MESSAGE_CHARS);
        }
        return msg;
    }

    private static String decodeUtf8(byte[] data, int off, int len) {
        if (data == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(data, off, len, StandardCharsets.UTF_8);
    }
}
