package yier.bubu.redis.protocol.custom.v1.wire;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.protocol.custom.v1.json.JsonWriter;
import yier.bubu.redis.protocol.custom.v1.reply.ReplyErrorKind;
import yier.bubu.redis.protocol.custom.v1.reply.ReplyErrorSanitizer;
import yier.bubu.redis.protocol.custom.v1.reply.ReplyValue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Custom Protocol v1 的 NDJSON 协议编码器/辅助类。
 * <p>
 * 说明：
 * - reply framing：每条 reply 为一个 JSON object，并以 {@code '\n'} 结尾
 * - bytes/map/nested error 采用显式 tagged value，避免 JSON 语义限制导致的 best-effort 漂移
 * - server 主写回路径的语义 owner 仍是 {@code ReplyWriter}；这里提供协议侧编码辅助能力
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

    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] QUOTE = new byte[]{'"'};

    // JSON string escaping（与 JsonWriter.writeString(String) 语义对齐）
    private static final byte[] ESC_QUOTE = new byte[]{'\\', '"'};
    private static final byte[] ESC_BACKSLASH = new byte[]{'\\', '\\'};
    private static final byte[] ESC_B = new byte[]{'\\', 'b'};
    private static final byte[] ESC_F = new byte[]{'\\', 'f'};
    private static final byte[] ESC_N = new byte[]{'\\', 'n'};
    private static final byte[] ESC_R = new byte[]{'\\', 'r'};
    private static final byte[] ESC_T = new byte[]{'\\', 't'};

    private static final byte[] HEX_UPPER = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private static final byte[] BASE64_TABLE =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".getBytes(StandardCharsets.US_ASCII);
    private static final byte BASE64_PAD = (byte) '=';
    // 4 的倍数，便于批量 flush
    private static final int BASE64_OUT_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_BASE64_OUT_BUF =
            ThreadLocal.withInitial(() -> new byte[BASE64_OUT_CHUNK_BYTES]);
    private static final ThreadLocal<byte[]> TL_U00_ESCAPE_BUF =
            ThreadLocal.withInitial(() -> new byte[]{'\\', 'u', '0', '0', '0', '0'});

    private CustomProtocolV1NdjsonEncoder() {
    }

    /**
     * Writes an ok envelope for protocol-side callers that already operate on {@link ReplyValue}.
     */
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
            out.writeBytes(NULL);
            return;
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            out.writeBytes(QUOTE);
            out.writeBytes(QUOTE);
            return;
        }

        Utf8ScanResult scan = scanUtf8(data, off, len);
        if (!scan.valid) {
            // 非 UTF-8 的 bytes：使用 tagged base64 表示，避免 best-effort 与信息丢失。
            writeB64TaggedValue(out, data, off, len);
            return;
        }

        // strict UTF-8：按 JSON string 输出。对无需 escape 的场景走 raw fast-path。
        if (!scan.needsEscape) {
            writeQuotedRaw(out, data, off, len);
            return;
        }
        writeQuotedEscaped(out, data, off, len);
    }

    /**
     * bytes value 编码（slice 版本）：避免按 value 大小分配 heap byte[]，并尽可能利用 slice.writeTo(out) fast-path。
     */
    public static void writeBytesValue(BytesSink out, BytesSlice slice) {
        Objects.requireNonNull(out, "out");
        if (slice == null) {
            out.writeBytes(NULL);
            return;
        }
        int len = Math.max(0, slice.length());
        if (len == 0) {
            out.writeBytes(QUOTE);
            out.writeBytes(QUOTE);
            return;
        }

        Utf8ScanResult scan = scanUtf8(slice, 0, len);
        if (!scan.valid) {
            writeB64TaggedValue(out, slice, 0, len);
            return;
        }

        if (!scan.needsEscape) {
            out.writeBytes(QUOTE);
            // 贯通 fast-path：NettyByteBufSink / DirectBytesSink 可由 slice 实现自行识别与优化。
            slice.writeTo(out);
            out.writeBytes(QUOTE);
            return;
        }

        writeQuotedEscaped(out, slice, 0, len);
    }

    public static void writeValue(BytesSink out, ReplyValue value) {
        Objects.requireNonNull(out, "out");
        if (value == null) {
            out.writeBytes(NULL);
            return;
        }

        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyNull) {
            out.writeBytes(NULL);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyBoolean b) {
            out.writeBytes((b.value() ? "true" : "false").getBytes(StandardCharsets.US_ASCII));
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyLong l) {
            byte[] bytes = Long.toString(l.value()).getBytes(StandardCharsets.US_ASCII);
            out.writeBytes(bytes, 0, bytes.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyDouble d) {
            byte[] bytes = Double.toString(d.value()).getBytes(StandardCharsets.US_ASCII);
            out.writeBytes(bytes, 0, bytes.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyString s) {
            JsonWriter.writeString(out, s.value());
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyBytes b) {
            byte[] data = b.data();
            writeBytesValue(out, data, 0, data.length);
            return;
        }
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyArray a) {
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
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyMap m) {
            out.writeBytes(MAP_PREFIX);
            List<yier.bubu.redis.protocol.custom.v1.reply.ReplyMap.Entry> entries = m.entries();
            if (entries != null && !entries.isEmpty()) {
                for (int i = 0; i < entries.size(); i++) {
                    if (i > 0) {
                        out.writeBytes(COMMA);
                    }
                    yier.bubu.redis.protocol.custom.v1.reply.ReplyMap.Entry e = entries.get(i);
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
        if (value instanceof yier.bubu.redis.protocol.custom.v1.reply.ReplyError e) {
            writeNestedErrorValue(out, e.kind(), e.message());
            return;
        }

        // 未覆盖类型：按 null 输出（避免 encoder 在生产路径抛异常影响可用性）。
        out.writeBytes(NULL);
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

    private static void writeB64TaggedValue(BytesSink out, byte[] data, int off, int len) {
        out.writeBytes(B64_PREFIX);
        out.writeBytes(QUOTE);
        writeBase64(out, data, off, len);
        out.writeBytes(QUOTE);
        out.writeBytes(B64_SUFFIX);
    }

    private static void writeB64TaggedValue(BytesSink out, BytesSource src, int srcIndex, int len) {
        out.writeBytes(B64_PREFIX);
        out.writeBytes(QUOTE);
        writeBase64(out, src, srcIndex, len);
        out.writeBytes(QUOTE);
        out.writeBytes(B64_SUFFIX);
    }

    private static void writeQuotedRaw(BytesSink out, byte[] data, int off, int len) {
        out.writeBytes(QUOTE);
        out.writeBytes(data, off, len);
        out.writeBytes(QUOTE);
    }

    /**
     * 输出 JSON string（已加引号），并对 ASCII 控制字符/引号/反斜杠执行 escape。
     * <p>
     * 前置条件：输入 bytes 为 strict UTF-8（仅需在 ASCII 范围做 escape）。
     */
    private static void writeQuotedEscaped(BytesSink out, byte[] data, int off, int len) {
        out.writeBytes(QUOTE);
        writeEscapedUtf8Bytes(out, data, off, len);
        out.writeBytes(QUOTE);
    }

    private static void writeQuotedEscaped(BytesSink out, BytesSource src, int srcIndex, int len) {
        out.writeBytes(QUOTE);
        writeEscapedUtf8Bytes(out, src, srcIndex, len);
        out.writeBytes(QUOTE);
    }

    private static void writeEscapedUtf8Bytes(BytesSink out, byte[] data, int off, int len) {
        int end = off + len;
        int runStart = off;
        for (int i = off; i < end; i++) {
            int b = data[i] & 0xFF;
            if (b >= 0x20 && b != '"' && b != '\\') {
                continue;
            }

            if (i > runStart) {
                out.writeBytes(data, runStart, i - runStart);
            }
            writeAsciiEscape(out, b);
            runStart = i + 1;
        }
        if (end > runStart) {
            out.writeBytes(data, runStart, end - runStart);
        }
    }

    private static void writeEscapedUtf8Bytes(BytesSink out, BytesSource src, int srcIndex, int len) {
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int index = srcIndex;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(index, scratch, 0, chunk);
            writeEscapedUtf8BytesChunk(out, scratch, 0, chunk);
            index += chunk;
            remaining -= chunk;
        }
    }

    private static void writeEscapedUtf8BytesChunk(BytesSink out, byte[] data, int off, int len) {
        int end = off + len;
        int runStart = off;
        for (int i = off; i < end; i++) {
            int b = data[i] & 0xFF;
            if (b >= 0x20 && b != '"' && b != '\\') {
                continue;
            }

            if (i > runStart) {
                out.writeBytes(data, runStart, i - runStart);
            }
            writeAsciiEscape(out, b);
            runStart = i + 1;
        }
        if (end > runStart) {
            out.writeBytes(data, runStart, end - runStart);
        }
    }

    private static void writeAsciiEscape(BytesSink out, int b) {
        switch (b) {
            case '"':
                out.writeBytes(ESC_QUOTE);
                return;
            case '\\':
                out.writeBytes(ESC_BACKSLASH);
                return;
            case '\b':
                out.writeBytes(ESC_B);
                return;
            case '\f':
                out.writeBytes(ESC_F);
                return;
            case '\n':
                out.writeBytes(ESC_N);
                return;
            case '\r':
                out.writeBytes(ESC_R);
                return;
            case '\t':
                out.writeBytes(ESC_T);
                return;
            default:
                break;
        }
        if (b <= 0x1F) {
            byte[] tmp = TL_U00_ESCAPE_BUF.get();
            tmp[4] = HEX_UPPER[(b >>> 4) & 0x0F];
            tmp[5] = HEX_UPPER[b & 0x0F];
            out.writeBytes(tmp, 0, 6);
            return;
        }
        // strict UTF-8 前置条件下，这里只会遇到 ASCII 或 >= 0x80；>= 0x80 不需要 escape。
        out.writeBytes(new byte[]{(byte) b}, 0, 1);
    }

    private static void writeBase64(BytesSink out, byte[] data, int off, int len) {
        if (len <= 0) {
            return;
        }
        byte[] outBuf = TL_BASE64_OUT_BUF.get();
        int outPos = 0;

        int i = off;
        int end = off + len;
        while (i + 3 <= end) {
            int b0 = data[i++] & 0xFF;
            int b1 = data[i++] & 0xFF;
            int b2 = data[i++] & 0xFF;
            outPos = appendBase64Quad(out, outBuf, outPos, b0, b1, b2);
        }

        int rem = end - i;
        if (rem == 1) {
            int b0 = data[i] & 0xFF;
            int v0 = b0 >>> 2;
            int v1 = (b0 & 0x03) << 4;
            outBuf[outPos++] = BASE64_TABLE[v0];
            outBuf[outPos++] = BASE64_TABLE[v1];
            outBuf[outPos++] = BASE64_PAD;
            outBuf[outPos++] = BASE64_PAD;
        } else if (rem == 2) {
            int b0 = data[i++] & 0xFF;
            int b1 = data[i] & 0xFF;
            int v0 = b0 >>> 2;
            int v1 = ((b0 & 0x03) << 4) | (b1 >>> 4);
            int v2 = (b1 & 0x0F) << 2;
            outBuf[outPos++] = BASE64_TABLE[v0];
            outBuf[outPos++] = BASE64_TABLE[v1];
            outBuf[outPos++] = BASE64_TABLE[v2];
            outBuf[outPos++] = BASE64_PAD;
        }

        if (outPos > 0) {
            out.writeBytes(outBuf, 0, outPos);
        }
    }

    private static void writeBase64(BytesSink out, BytesSource src, int srcIndex, int len) {
        if (len <= 0) {
            return;
        }
        byte[] scratch = TL_COPY_BUF.get();
        byte[] outBuf = TL_BASE64_OUT_BUF.get();
        int outPos = 0;

        int carryLen = 0;
        int carry0 = 0;
        int carry1 = 0;

        int remaining = len;
        int index = srcIndex;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(index, scratch, 0, chunk);
            index += chunk;
            remaining -= chunk;

            int i = 0;
            if (carryLen == 1) {
                if (chunk - i >= 2) {
                    int b0 = carry0;
                    int b1 = scratch[i++] & 0xFF;
                    int b2 = scratch[i++] & 0xFF;
                    outPos = appendBase64Quad(out, outBuf, outPos, b0, b1, b2);
                    carryLen = 0;
                } else {
                    if (chunk - i == 1) {
                        carry1 = scratch[i] & 0xFF;
                        carryLen = 2;
                    }
                    continue;
                }
            } else if (carryLen == 2) {
                if (chunk - i >= 1) {
                    int b0 = carry0;
                    int b1 = carry1;
                    int b2 = scratch[i++] & 0xFF;
                    outPos = appendBase64Quad(out, outBuf, outPos, b0, b1, b2);
                    carryLen = 0;
                } else {
                    continue;
                }
            }

            while (i + 3 <= chunk) {
                int b0 = scratch[i++] & 0xFF;
                int b1 = scratch[i++] & 0xFF;
                int b2 = scratch[i++] & 0xFF;
                outPos = appendBase64Quad(out, outBuf, outPos, b0, b1, b2);
            }

            int rem = chunk - i;
            if (rem == 1) {
                carry0 = scratch[i] & 0xFF;
                carryLen = 1;
            } else if (rem == 2) {
                carry0 = scratch[i++] & 0xFF;
                carry1 = scratch[i] & 0xFF;
                carryLen = 2;
            }
        }

        if (carryLen == 1) {
            int b0 = carry0;
            int v0 = b0 >>> 2;
            int v1 = (b0 & 0x03) << 4;
            outBuf[outPos++] = BASE64_TABLE[v0];
            outBuf[outPos++] = BASE64_TABLE[v1];
            outBuf[outPos++] = BASE64_PAD;
            outBuf[outPos++] = BASE64_PAD;
        } else if (carryLen == 2) {
            int b0 = carry0;
            int b1 = carry1;
            int v0 = b0 >>> 2;
            int v1 = ((b0 & 0x03) << 4) | (b1 >>> 4);
            int v2 = (b1 & 0x0F) << 2;
            outBuf[outPos++] = BASE64_TABLE[v0];
            outBuf[outPos++] = BASE64_TABLE[v1];
            outBuf[outPos++] = BASE64_TABLE[v2];
            outBuf[outPos++] = BASE64_PAD;
        }

        if (outPos > 0) {
            out.writeBytes(outBuf, 0, outPos);
        }
    }

    private static int appendBase64Quad(BytesSink out, byte[] outBuf, int outPos, int b0, int b1, int b2) {
        int v0 = b0 >>> 2;
        int v1 = ((b0 & 0x03) << 4) | (b1 >>> 4);
        int v2 = ((b1 & 0x0F) << 2) | (b2 >>> 6);
        int v3 = b2 & 0x3F;

        outBuf[outPos++] = BASE64_TABLE[v0];
        outBuf[outPos++] = BASE64_TABLE[v1];
        outBuf[outPos++] = BASE64_TABLE[v2];
        outBuf[outPos++] = BASE64_TABLE[v3];

        if (outPos == outBuf.length) {
            out.writeBytes(outBuf, 0, outPos);
            return 0;
        }
        return outPos;
    }

    private static Utf8ScanResult scanUtf8(byte[] data, int off, int len) {
        Utf8Scanner scanner = new Utf8Scanner();
        scanner.process(data, off, len);
        return new Utf8ScanResult(scanner.isValid(), scanner.needsEscape);
    }

    private static Utf8ScanResult scanUtf8(BytesSource src, int srcIndex, int len) {
        Utf8Scanner scanner = new Utf8Scanner();
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int index = srcIndex;
        while (remaining > 0 && scanner.valid) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(index, scratch, 0, chunk);
            scanner.process(scratch, 0, chunk);
            index += chunk;
            remaining -= chunk;
        }
        return new Utf8ScanResult(scanner.isValid(), scanner.needsEscape);
    }

    private record Utf8ScanResult(boolean valid, boolean needsEscape) {
    }

    /**
     * streaming UTF-8 校验器：仅用于判定 strict UTF-8 与“是否需要 JSON escape”。
     * <p>
     * 约束：只接受 Unicode scalar value（拒绝 surrogate）、拒绝 overlong、拒绝 > U+10FFFF。
     */
    private static final class Utf8Scanner {
        private boolean valid = true;
        private boolean needsEscape = false;

        // continuation bytes 状态
        private int remaining = 0;
        private boolean firstContinuation = false;
        private int firstMin = 0x80;
        private int firstMax = 0xBF;

        void process(byte[] data, int off, int len) {
            int end = off + len;
            for (int i = off; i < end && valid; i++) {
                int b = data[i] & 0xFF;

                if (b < 0x80) {
                    if (remaining != 0) {
                        valid = false;
                        break;
                    }
                    if (b < 0x20 || b == '"' || b == '\\') {
                        needsEscape = true;
                    }
                    continue;
                }

                if (remaining == 0) {
                    // lead byte
                    if (b >= 0xC2 && b <= 0xDF) {
                        startSeq(1, 0x80, 0xBF);
                    } else if (b == 0xE0) {
                        startSeq(2, 0xA0, 0xBF);
                    } else if (b >= 0xE1 && b <= 0xEC) {
                        startSeq(2, 0x80, 0xBF);
                    } else if (b == 0xED) {
                        // ED A0..BF 80..BF -> surrogate range，禁止
                        startSeq(2, 0x80, 0x9F);
                    } else if (b >= 0xEE && b <= 0xEF) {
                        startSeq(2, 0x80, 0xBF);
                    } else if (b == 0xF0) {
                        startSeq(3, 0x90, 0xBF);
                    } else if (b >= 0xF1 && b <= 0xF3) {
                        startSeq(3, 0x80, 0xBF);
                    } else if (b == 0xF4) {
                        startSeq(3, 0x80, 0x8F);
                    } else {
                        // 包含：0x80..0xBF（裸 continuation）、0xC0..0xC1（overlong）、0xF5..0xFF（超范围）
                        valid = false;
                    }
                    continue;
                }

                // continuation bytes
                if (firstContinuation) {
                    if (b < firstMin || b > firstMax) {
                        valid = false;
                        break;
                    }
                    firstContinuation = false;
                } else {
                    if (b < 0x80 || b > 0xBF) {
                        valid = false;
                        break;
                    }
                }

                remaining--;
                if (remaining == 0) {
                    firstContinuation = false;
                }
            }
        }

        private void startSeq(int contBytes, int min1, int max1) {
            remaining = contBytes;
            firstContinuation = true;
            firstMin = min1;
            firstMax = max1;
        }

        boolean isValid() {
            return valid && remaining == 0;
        }
    }
}
