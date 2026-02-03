package yier.bubu.redis.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 将完整的 RESP frame 解析为 {@link RespObject}（用于 CLI/测试/调试）。
 * <p>
 * 注意：该解析器以“易用/正确性”为主，会分配对象与 byte[]；性能敏感路径应优先使用 frame/zero-copy 视图。
 */
public final class RespObjectParser {
    private final RespFrame frame;
    private final int maxBulkBytes;
    private final int maxArrayLen;
    private final int maxNestingDepth;
    private final int maxLineBytes;

    private int index;

    private RespObjectParser(RespFrame frame, int maxBulkBytes, int maxArrayLen, int maxNestingDepth, int maxLineBytes) {
        this.frame = frame;
        this.maxBulkBytes = maxBulkBytes;
        this.maxArrayLen = maxArrayLen;
        this.maxNestingDepth = maxNestingDepth;
        this.maxLineBytes = maxLineBytes;
    }

    public static RespObject parse(RespFrame frame) {
        return parse(frame,
                RespLimits.DEFAULT_MAX_BULK_BYTES,
                RespLimits.DEFAULT_MAX_ARRAY_LEN,
                RespLimits.DEFAULT_MAX_NESTING_DEPTH,
                RespLimits.DEFAULT_MAX_LINE_BYTES
        );
    }

    public static RespObject parse(RespFrame frame, int maxBulkBytes, int maxArrayLen, int maxNestingDepth, int maxLineBytes) {
        if (frame == null) {
            throw new IllegalArgumentException("frame must not be null");
        }
        if (maxBulkBytes <= 0) {
            throw new IllegalArgumentException("maxBulkBytes must be > 0");
        }
        if (maxArrayLen <= 0) {
            throw new IllegalArgumentException("maxArrayLen must be > 0");
        }
        if (maxNestingDepth <= 0) {
            throw new IllegalArgumentException("maxNestingDepth must be > 0");
        }
        if (maxLineBytes <= 0) {
            throw new IllegalArgumentException("maxLineBytes must be > 0");
        }
        RespObjectParser p = new RespObjectParser(frame, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes);
        RespObject obj = p.parseOne(0);
        if (p.index != frame.length()) {
            // frame 应该只包含一个完整的 reply；多余内容通常意味着 framing/使用方式错误。
            throw new IllegalArgumentException("Protocol error: trailing bytes in frame");
        }
        return obj;
    }

    private RespObject parseOne(int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }
        if (index >= frame.length()) {
            throw new IllegalArgumentException("Protocol error: truncated frame");
        }

        byte prefix = frame.getByte(index++);
        switch (prefix) {
            case '+': {
                String line = readLineUtf8();
                return RespSimpleString.of(line);
            }
            case '-': {
                String line = readLineUtf8();
                return RespError.of(line);
            }
            case ':': {
                String line = readLineAscii();
                return RespInteger.of(Long.parseLong(line.trim()));
            }
            case '$': {
                String line = readLineAscii();
                String s = line.trim();
                if ("?".equals(s)) {
                    return parseStreamedBlobString();
                }
                int len = Integer.parseInt(s);
                if (len == -1) {
                    return RespBulkString.nullString();
                }
                if (len < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid bulk length");
                }
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
                }
                if (index + len + 2 > frame.length()) {
                    throw new IllegalArgumentException("Protocol error: truncated bulk string");
                }
                byte[] data = new byte[len];
                if (len > 0) {
                    frame.getBytes(index, data, 0, len);
                }
                index += len;
                expectCrlf();
                return RespBulkString.ofBytes(data);
            }
            case '*': {
                String line = readLineAscii();
                String s = line.trim();
                if ("?".equals(s)) {
                    return parseStreamedArray(nestingDepth);
                }
                int count = Integer.parseInt(s);
                if (count == -1) {
                    return RespArray.nullArray();
                }
                if (count < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid array length");
                }
                if (count > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: array length too large");
                }
                List<RespObject> items = new ArrayList<>(Math.min(count, 16));
                for (int i = 0; i < count; i++) {
                    items.add(parseOne(nestingDepth + 1));
                }
                return RespArray.of(items);
            }
            case '%': {
                String line = readLineAscii();
                String s = line.trim();
                if ("?".equals(s)) {
                    return parseStreamedMap(nestingDepth);
                }
                int pairs = Integer.parseInt(s);
                if (pairs < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid map length");
                }
                if (pairs > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: map length too large");
                }
                List<RespMap.Entry> entries = new ArrayList<>(Math.min(pairs, 16));
                for (int i = 0; i < pairs; i++) {
                    RespObject key = parseOne(nestingDepth + 1);
                    RespObject value = parseOne(nestingDepth + 1);
                    entries.add(new RespMap.Entry(key, value));
                }
                return RespMap.of(entries);
            }
            case '~': {
                String line = readLineAscii();
                String s = line.trim();
                if ("?".equals(s)) {
                    return parseStreamedSet(nestingDepth);
                }
                int count = Integer.parseInt(s);
                if (count < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid set length");
                }
                if (count > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: set length too large");
                }
                List<RespObject> items = new ArrayList<>(Math.min(count, 16));
                for (int i = 0; i < count; i++) {
                    items.add(parseOne(nestingDepth + 1));
                }
                return RespSet.of(items);
            }
            case '#': {
                String line = readLineAscii();
                if ("t".equals(line)) {
                    return RespBoolean.of(true);
                }
                if ("f".equals(line)) {
                    return RespBoolean.of(false);
                }
                throw new IllegalArgumentException("Protocol error: invalid boolean value");
            }
            case ',': {
                String line = readLineAscii();
                double v;
                try {
                    v = Double.parseDouble(line.trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Protocol error: invalid double value");
                }
                if (Double.isNaN(v) || Double.isInfinite(v)) {
                    throw new IllegalArgumentException("Protocol error: invalid double value");
                }
                return RespDouble.of(v);
            }
            case '(': {
                String line = readLineAscii();
                String s = line.trim();
                if (s.isEmpty()) {
                    throw new IllegalArgumentException("Protocol error: invalid big number");
                }
                return RespBigNumber.of(s);
            }
            case '!': {
                int len = readBulkLen();
                if (len < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid blob error length");
                }
                byte[] data = readFixedBytes(len);
                expectCrlf();
                return RespBlobError.ofBytes(data);
            }
            case '=': {
                int len = readBulkLen();
                if (len < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid verbatim string length");
                }
                byte[] raw = readFixedBytes(len);
                expectCrlf();
                if (raw.length < 4 || raw[3] != (byte) ':') {
                    throw new IllegalArgumentException("Protocol error: invalid verbatim string payload");
                }
                String format = new String(raw, 0, 3, StandardCharsets.US_ASCII);
                byte[] data = new byte[raw.length - 4];
                if (data.length > 0) {
                    System.arraycopy(raw, 4, data, 0, data.length);
                }
                return RespVerbatimString.ofBytes(format, data);
            }
            case '>': {
                String line = readLineAscii();
                int count = Integer.parseInt(line.trim());
                if (count < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid push length");
                }
                if (count > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: push length too large");
                }
                List<RespObject> items = new ArrayList<>(Math.min(count, 16));
                for (int i = 0; i < count; i++) {
                    items.add(parseOne(nestingDepth + 1));
                }
                return RespPush.of(items);
            }
            case '|': {
                String line = readLineAscii();
                int pairs = Integer.parseInt(line.trim());
                if (pairs < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid attribute length");
                }
                if (pairs > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: attribute length too large");
                }
                List<RespMap.Entry> entries = new ArrayList<>(Math.min(pairs, 16));
                for (int i = 0; i < pairs; i++) {
                    RespObject key = parseOne(nestingDepth + 1);
                    RespObject value = parseOne(nestingDepth + 1);
                    entries.add(new RespMap.Entry(key, value));
                }
                RespMap attrs = RespMap.of(entries);
                RespObject value = parseOne(nestingDepth + 1);
                return RespAttribute.of(attrs, value);
            }
            case '_': {
                // RESP3 null: "_\r\n"
                expectCrlf();
                return RespNull.INSTANCE;
            }
            default:
                throw new IllegalArgumentException("Protocol error: unknown RESP prefix: " + (char) prefix);
        }
    }

    private RespBulkString parseStreamedBlobString() {
        // Streamed blob string: "$?\r\n" + ( ";"<len>\r\n<payload>\r\n )* + ";0\r\n"
        int total = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(256, maxBulkBytes));
        for (; ; ) {
            if (index >= frame.length()) {
                throw new IllegalArgumentException("Protocol error: truncated streamed blob string");
            }
            byte chunkPrefix = frame.getByte(index++);
            if (chunkPrefix != ';') {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk prefix");
            }
            String line = readLineAscii();
            int len;
            try {
                len = Integer.parseInt(line.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            if (len < 0) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            if (len == 0) {
                break;
            }
            if (total > maxBulkBytes - len) {
                throw new IllegalArgumentException("Protocol error: bulk length too large");
            }
            byte[] data = readFixedBytes(len);
            expectCrlf();
            out.write(data, 0, data.length);
            total += len;
        }
        return RespBulkString.ofBytes(out.toByteArray());
    }

    private RespArray parseStreamedArray(int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }
        List<RespObject> items = new ArrayList<>();
        for (; ; ) {
            if (consumeStreamedEndMarker()) {
                break;
            }
            if (items.size() >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: array length too large");
            }
            items.add(parseOne(nestingDepth + 1));
        }
        return RespArray.of(items);
    }

    private RespSet parseStreamedSet(int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested sets too deep");
        }
        List<RespObject> items = new ArrayList<>();
        for (; ; ) {
            if (consumeStreamedEndMarker()) {
                break;
            }
            if (items.size() >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: set length too large");
            }
            items.add(parseOne(nestingDepth + 1));
        }
        return RespSet.of(items);
    }

    private RespMap parseStreamedMap(int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested maps too deep");
        }
        List<RespMap.Entry> entries = new ArrayList<>();
        for (; ; ) {
            if (consumeStreamedEndMarker()) {
                break;
            }
            if (entries.size() >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: map length too large");
            }
            RespObject key = parseOne(nestingDepth + 1);
            if (consumeStreamedEndMarker()) {
                throw new IllegalArgumentException("Protocol error: missing map value before end marker");
            }
            RespObject value = parseOne(nestingDepth + 1);
            entries.add(new RespMap.Entry(key, value));
        }
        return RespMap.of(entries);
    }

    private boolean consumeStreamedEndMarker() {
        if (index >= frame.length()) {
            throw new IllegalArgumentException("Protocol error: truncated frame");
        }
        if (frame.getByte(index) != '.') {
            return false;
        }
        index++;
        expectCrlf();
        return true;
    }

    private String readLineUtf8() {
        int end = indexOfCrlf(index);
        int len = end - index;
        byte[] buf = new byte[len];
        if (len > 0) {
            frame.getBytes(index, buf, 0, len);
        }
        index = end + 2;
        return new String(buf, StandardCharsets.UTF_8);
    }

    private String readLineAscii() {
        int end = indexOfCrlf(index);
        int len = end - index;
        byte[] buf = new byte[len];
        if (len > 0) {
            frame.getBytes(index, buf, 0, len);
        }
        index = end + 2;
        return new String(buf, StandardCharsets.US_ASCII);
    }

    private int indexOfCrlf(int start) {
        int maxCrlfStart = start + maxLineBytes;
        int scanLimit = Math.min(frame.length() - 1, maxCrlfStart + 1);
        for (int i = start; i < scanLimit; i++) {
            if (frame.getByte(i) == '\r' && frame.getByte(i + 1) == '\n') {
                return i;
            }
        }
        if (frame.length() - start > maxLineBytes + 2) {
            throw new IllegalArgumentException("Protocol error: line too long");
        }
        throw new IllegalArgumentException("Protocol error: truncated line");
    }

    private void expectCrlf() {
        if (index + 2 > frame.length()) {
            throw new IllegalArgumentException("Protocol error: truncated CRLF");
        }
        if (frame.getByte(index) != '\r' || frame.getByte(index + 1) != '\n') {
            throw new IllegalArgumentException("Protocol error: bad CRLF");
        }
        index += 2;
    }

    private int readBulkLen() {
        String line = readLineAscii();
        int len;
        try {
            len = Integer.parseInt(line.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Protocol error: invalid bulk length");
        }
        if (len > maxBulkBytes) {
            throw new IllegalArgumentException("Protocol error: bulk length too large");
        }
        return len;
    }

    private byte[] readFixedBytes(int len) {
        if (len < 0) {
            throw new IllegalArgumentException("Protocol error: invalid bulk length");
        }
        if (index + len + 2 > frame.length()) {
            throw new IllegalArgumentException("Protocol error: truncated bulk string");
        }
        byte[] data = new byte[len];
        if (len > 0) {
            frame.getBytes(index, data, 0, len);
        }
        index += len;
        return data;
    }
}
