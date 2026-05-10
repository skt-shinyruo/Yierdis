package yier.bubu.redis.protocol.resp;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ReplyWriter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class RespReplyWriter implements ReplyWriter {
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final String[] REDIS_ERROR_PREFIXES = {
            "ERR",
            "WRONGTYPE",
            "NOPROTO",
            "NOAUTH",
            "BUSY",
            "OOM",
            "EXECABORT",
            "LOADING",
            "NOSCRIPT",
            "READONLY",
            "MOVED",
            "ASK",
            "TRYAGAIN",
            "CLUSTERDOWN",
            "MASTERDOWN",
            "WRONGPASS",
            "NOREPLICAS",
            "DENIED"
    };
    private final BytesSink out;
    private final RespProtocolVersion version;
    private boolean closeAfterReplyRequested;

    public RespReplyWriter(BytesSink out, RespProtocolVersion version) {
        this.out = Objects.requireNonNull(out, "out");
        this.version = version == null ? RespProtocolVersion.RESP2 : version;
    }

    @Override
    public void requestCloseAfterReply() {
        closeAfterReplyRequested = true;
    }

    @Override
    public boolean closeAfterReplyRequested() {
        return closeAfterReplyRequested;
    }

    @Override
    public void simpleString(String value) {
        writeAsciiLine('+', sanitizeSimple(value));
    }

    @Override
    public void error(String message) {
        writeAsciiLine('-', normalizeError(message));
    }

    @Override
    public void protocolError(String message) {
        error(message == null ? "ERR Protocol error" : message);
        requestCloseAfterReply();
    }

    @Override
    public void internalError(String message) {
        error(message == null ? "ERR internal error" : message);
    }

    @Override
    public void integer(long value) {
        writeAsciiLine(':', Long.toString(value));
    }

    @Override
    public void booleanValue(boolean value) {
        if (version == RespProtocolVersion.RESP3) {
            writeAscii(value ? "#t\r\n" : "#f\r\n");
        } else {
            integer(value ? 1L : 0L);
        }
    }

    @Override
    public void doubleValue(double value) {
        if (version == RespProtocolVersion.RESP3) {
            if (Double.isNaN(value)) {
                writeAscii(",nan\r\n");
                return;
            }
            if (value == Double.POSITIVE_INFINITY) {
                writeAscii(",inf\r\n");
                return;
            }
            if (value == Double.NEGATIVE_INFINITY) {
                writeAscii(",-inf\r\n");
                return;
            }
            writeAsciiLine(',', Double.toString(value));
        } else {
            if (Double.isNaN(value)) {
                bulkString("nan".getBytes(StandardCharsets.US_ASCII));
                return;
            }
            if (value == Double.POSITIVE_INFINITY) {
                bulkString("inf".getBytes(StandardCharsets.US_ASCII));
                return;
            }
            if (value == Double.NEGATIVE_INFINITY) {
                bulkString("-inf".getBytes(StandardCharsets.US_ASCII));
                return;
            }
            bulkString(Double.toString(value).getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Override
    public void bigNumberAscii(String value) {
        String normalized = sanitizeSimple(value == null ? "" : value.trim());
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('(', normalized);
        } else {
            bulkString(normalized.getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Override
    public void verbatimString(String format, byte[] data) {
        byte[] body = data == null ? new byte[0] : data;
        if (version == RespProtocolVersion.RESP3) {
            String f = sanitizeVerbatimFormat(format);
            writeAscii("=" + (f.length() + 1 + body.length) + "\r\n" + f + ":");
            out.writeBytes(body, 0, body.length);
            writeCrlf();
        } else {
            bulkString(body);
        }
    }

    @Override
    public void blobError(String message) {
        String normalized = normalizeError(message);
        if (version == RespProtocolVersion.RESP3) {
            byte[] bytes = normalized.getBytes(StandardCharsets.UTF_8);
            writeAscii("!" + bytes.length + "\r\n");
            out.writeBytes(bytes, 0, bytes.length);
            writeCrlf();
        } else {
            error(normalized);
        }
    }

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
        Objects.checkFromIndexSize(off, len, data.length);
        writeAscii("$" + len + "\r\n");
        out.writeBytes(data, off, len);
        writeCrlf();
    }

    @Override
    public void bulkString(BytesSlice slice) {
        if (slice == null) {
            nullValue();
            return;
        }
        int len = slice.length();
        if (len < 0) {
            throw new IllegalArgumentException("slice length must be >= 0");
        }
        writeAscii("$" + len + "\r\n");
        slice.writeTo(out);
        writeCrlf();
    }

    @Override
    public void bulkStringLongAscii(long value) {
        bulkString(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void nullValue() {
        if (version == RespProtocolVersion.RESP3) {
            writeAscii("_\r\n");
        } else {
            writeAscii("$-1\r\n");
        }
    }

    @Override
    public void nullArray() {
        if (version == RespProtocolVersion.RESP3) {
            writeAscii("_\r\n");
        } else {
            writeAscii("*-1\r\n");
        }
    }

    @Override
    public void arrayHeader(int count) {
        writeAsciiLine('*', Integer.toString(Math.max(0, count)));
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
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('%', Integer.toString(Math.max(0, pairs)));
        } else {
            writeAsciiLine('*', Integer.toString(Math.max(0, pairs) * 2));
        }
    }

    @Override
    public void setHeader(int count) {
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('~', Integer.toString(Math.max(0, count)));
        } else {
            arrayHeader(count);
        }
    }

    @Override
    public void pushHeader(int count) {
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('>', Integer.toString(Math.max(0, count)));
        } else {
            arrayHeader(count);
        }
    }

    @Override
    public void attributeHeader(int pairs) {
        if (version == RespProtocolVersion.RESP3) {
            writeAsciiLine('|', Integer.toString(Math.max(0, pairs)));
        } else {
            mapHeader(pairs);
        }
    }

    private void writeAsciiLine(char prefix, String value) {
        writeAscii(prefix + value + "\r\n");
    }

    private void writeAscii(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        out.writeBytes(bytes, 0, bytes.length);
    }

    private void writeCrlf() {
        out.writeBytes(CRLF, 0, CRLF.length);
    }

    private static String sanitizeSimple(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private static String normalizeError(String message) {
        String value = sanitizeSimple(message == null ? "ERR error" : message);
        if (!hasRedisErrorPrefix(value)) {
            value = "ERR " + value;
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 512) {
            return value;
        }

        int end = 0;
        int used = 0;
        while (end < value.length()) {
            int cp = value.codePointAt(end);
            int cpBytes = utf8Length(cp);
            if (used + cpBytes > 512) {
                break;
            }
            used += cpBytes;
            end += Character.charCount(cp);
        }
        return value.substring(0, end);
    }

    private static int utf8Length(int codePoint) {
        if (codePoint <= 0x7F) {
            return 1;
        }
        if (codePoint <= 0x7FF) {
            return 2;
        }
        if (codePoint <= 0xFFFF) {
            return 3;
        }
        return 4;
    }

    private static boolean hasRedisErrorPrefix(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (String prefix : REDIS_ERROR_PREFIXES) {
            if (value.equals(prefix)) {
                return true;
            }
            if (value.startsWith(prefix) && value.length() > prefix.length()) {
                char separator = value.charAt(prefix.length());
                if (Character.isWhitespace(separator)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String sanitizeVerbatimFormat(String format) {
        String value = sanitizeSimple(format == null ? "txt" : format.trim());
        if (value.length() < 3) {
            return "txt";
        }
        return value.substring(0, 3);
    }
}
