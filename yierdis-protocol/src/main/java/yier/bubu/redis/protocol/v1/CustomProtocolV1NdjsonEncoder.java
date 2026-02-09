package yier.bubu.redis.protocol.v1;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.protocol.json.JsonWriter;
import yier.bubu.redis.protocol.reply.ReplyErrorKind;
import yier.bubu.redis.protocol.reply.ReplyErrorSanitizer;
import yier.bubu.redis.protocol.reply.ReplyValue;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * Custom Protocol v1 的 NDJSON 编码器（SSOT）。
 * <p>
 * 说明：
 * - reply framing：每条 reply 为一个 JSON object，并以 {@code '\n'} 结尾
 * - bytes/map/nested error 采用显式 tagged value，避免 JSON 语义限制导致的 best-effort 漂移
 */
public final class CustomProtocolV1NdjsonEncoder {
    public static final String TAG_B64 = "$b64";
    public static final String TAG_MAP = "$map";
    public static final String TAG_ERROR = "$error";

    private static final byte[] LBRACKET = new byte[]{'['};
    private static final byte[] RBRACKET = new byte[]{']'};
    private static final byte[] COMMA = new byte[]{','};

    private static final byte[] OK_PREFIX = "{\"ok\":true,\"result\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OK_SUFFIX = "}\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ERR_PREFIX_PROTOCOL = "{\"ok\":false,\"error\":{\"kind\":\"protocol\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_PREFIX_COMMAND = "{\"ok\":false,\"error\":{\"kind\":\"command\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_PREFIX_INTERNAL = "{\"ok\":false,\"error\":{\"kind\":\"internal\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ERR_SUFFIX = "}}\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] MAP_PREFIX = "{\"$map\":[".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAP_SUFFIX = "]}".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] B64_PREFIX = "{\"$b64\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] B64_SUFFIX = "}".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] VALUE_ERR_PREFIX_PROTOCOL = "{\"$error\":{\"kind\":\"protocol\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_PREFIX_COMMAND = "{\"$error\":{\"kind\":\"command\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_PREFIX_INTERNAL = "{\"$error\":{\"kind\":\"internal\",\"message\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VALUE_ERR_SUFFIX = "}}".getBytes(StandardCharsets.US_ASCII);

    private static final ThreadLocal<CharsetDecoder> TL_UTF8_DECODER = ThreadLocal.withInitial(() ->
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
    );

    private CustomProtocolV1NdjsonEncoder() {
    }

    public static void writeOkEnvelope(BytesSink out, ReplyValue value) {
        Objects.requireNonNull(out, "out");
        out.writeBytes(OK_PREFIX);
        writeValue(out, value);
        out.writeBytes(OK_SUFFIX);
    }

    public static void writeErrorEnvelope(BytesSink out, ReplyErrorKind kind, String message) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(kind, "kind");
        String msg = ReplyErrorSanitizer.sanitize(kind, message);
        out.writeBytes(errorEnvelopePrefix(kind));
        JsonWriter.writeString(out, msg);
        out.writeBytes(ERR_SUFFIX);
    }

    public static void writeNestedErrorValue(BytesSink out, ReplyErrorKind kind, String message) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(kind, "kind");
        String msg = ReplyErrorSanitizer.sanitize(kind, message);
        out.writeBytes(nestedErrorPrefix(kind));
        JsonWriter.writeString(out, msg);
        out.writeBytes(VALUE_ERR_SUFFIX);
    }

    public static void writeMapPrefix(BytesSink out) {
        Objects.requireNonNull(out, "out");
        out.writeBytes(MAP_PREFIX);
    }

    public static void writeMapSuffix(BytesSink out) {
        Objects.requireNonNull(out, "out");
        out.writeBytes(MAP_SUFFIX);
    }

    public static void writeBytesValue(BytesSink out, byte[] data, int off, int len) {
        Objects.requireNonNull(out, "out");
        if (data == null) {
            out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
            return;
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            JsonWriter.writeString(out, "");
            return;
        }

        String utf8 = strictUtf8ToStringOrNull(data, off, len);
        if (utf8 != null) {
            // 语义保真：对“有效 UTF-8 字节序列”，UTF-8 decode/encode 是可逆的。
            JsonWriter.writeString(out, utf8);
            return;
        }

        // 非 UTF-8 的 bytes：使用 tagged base64 表示，避免 best-effort 与信息丢失。
        out.writeBytes(B64_PREFIX);
        String b64 = Base64.getEncoder().encodeToString(copyRange(data, off, len));
        JsonWriter.writeString(out, b64);
        out.writeBytes(B64_SUFFIX);
    }

    public static void writeValue(BytesSink out, ReplyValue value) {
        Objects.requireNonNull(out, "out");
        if (value == null) {
            out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
            return;
        }

        if (value instanceof yier.bubu.redis.protocol.reply.ReplyNull) {
            out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyBoolean b) {
            out.writeBytes((b.value() ? "true" : "false").getBytes(StandardCharsets.US_ASCII));
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyLong l) {
            byte[] bytes = Long.toString(l.value()).getBytes(StandardCharsets.US_ASCII);
            out.writeBytes(bytes, 0, bytes.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyDouble d) {
            byte[] bytes = Double.toString(d.value()).getBytes(StandardCharsets.US_ASCII);
            out.writeBytes(bytes, 0, bytes.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyString s) {
            JsonWriter.writeString(out, s.value());
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyBytes b) {
            byte[] data = b.data();
            writeBytesValue(out, data, 0, data.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyArray a) {
            List<ReplyValue> values = a.values();
            out.writeBytes(LBRACKET);
            if (values != null && !values.isEmpty()) {
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0) {
                        out.writeBytes(COMMA);
                    }
                    writeValue(out, values.get(i));
                }
            }
            out.writeBytes(RBRACKET);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyMap m) {
            out.writeBytes(MAP_PREFIX);
            List<yier.bubu.redis.protocol.reply.ReplyMap.Entry> entries = m.entries();
            if (entries != null && !entries.isEmpty()) {
                for (int i = 0; i < entries.size(); i++) {
                    if (i > 0) {
                        out.writeBytes(COMMA);
                    }
                    yier.bubu.redis.protocol.reply.ReplyMap.Entry e = entries.get(i);
                    out.writeBytes(LBRACKET);
                    writeValue(out, e.key());
                    out.writeBytes(COMMA);
                    writeValue(out, e.value());
                    out.writeBytes(RBRACKET);
                }
            }
            out.writeBytes(MAP_SUFFIX);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.reply.ReplyError e) {
            writeNestedErrorValue(out, e.kind(), e.message());
            return;
        }

        // 未覆盖类型：按 null 输出（避免 encoder 在生产路径抛异常影响可用性）。
        out.writeBytes("null".getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] errorEnvelopePrefix(ReplyErrorKind kind) {
        return switch (kind) {
            case PROTOCOL -> ERR_PREFIX_PROTOCOL;
            case COMMAND -> ERR_PREFIX_COMMAND;
            case INTERNAL -> ERR_PREFIX_INTERNAL;
        };
    }

    private static byte[] nestedErrorPrefix(ReplyErrorKind kind) {
        return switch (kind) {
            case PROTOCOL -> VALUE_ERR_PREFIX_PROTOCOL;
            case COMMAND -> VALUE_ERR_PREFIX_COMMAND;
            case INTERNAL -> VALUE_ERR_PREFIX_INTERNAL;
        };
    }

    private static String strictUtf8ToStringOrNull(byte[] data, int off, int len) {
        CharsetDecoder dec = TL_UTF8_DECODER.get();
        dec.reset();
        try {
            CharBuffer cb = dec.decode(ByteBuffer.wrap(data, off, len));
            return cb.toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static byte[] copyRange(byte[] data, int off, int len) {
        byte[] out = new byte[len];
        System.arraycopy(data, off, out, 0, len);
        return out;
    }
}
