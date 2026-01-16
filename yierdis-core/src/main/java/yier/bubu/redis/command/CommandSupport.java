package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisBulkStringOutput;
import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.RandomAccess;

/**
 * Shared parsing helpers and request-scoped scratch buffers for low-allocation command handlers.
 * <p>
 * This object is <b>not</b> thread-safe and is intended to be used from the single command executor thread.
 */
final class CommandSupport {
    private final YierdisDb db;

    private final ByteArraySliceList slice = new ByteArraySliceList();
    private byte[][] argvScratch = new byte[16][];
    private final RespCommandArgBytesView argView = new RespCommandArgBytesView();
    private final WriterBulkStringOutput bulkOut = new WriterBulkStringOutput();

    CommandSupport(YierdisDb db) {
        this.db = db;
    }

    YierdisDb db() {
        return db;
    }

    YierdisBytesView argView(RespCommand cmd, int argIndex) {
        return argView.reset(cmd, argIndex);
    }

    YierdisBulkStringOutput bulkOut(RespWriter writer) {
        bulkOut.reset(writer);
        return bulkOut;
    }

    java.util.List<byte[]> slice() {
        return slice;
    }

    void sliceResetFromCommand(RespCommand cmd, int argStart, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be non-negative");
        }
        if (len == 0) {
            slice.reset(argvScratch, 0, 0);
            return;
        }
        ensureScratchCapacity(len);
        for (int i = 0; i < len; i++) {
            argvScratch[i] = cmd.toByteArray(argStart + i);
        }
        slice.reset(argvScratch, 0, len);
    }

    void clearScratch(int len) {
        if (len <= 0) {
            slice.reset(argvScratch, 0, 0);
            return;
        }
        Arrays.fill(argvScratch, 0, Math.min(len, argvScratch.length), null);
        slice.reset(argvScratch, 0, 0);
    }

    private void ensureScratchCapacity(int desired) {
        if (argvScratch.length >= desired) {
            return;
        }
        int next = argvScratch.length;
        while (next < desired) {
            next <<= 1;
        }
        argvScratch = Arrays.copyOf(argvScratch, next);
    }

    static void wrongArity(RespWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }

    static String utf8(RespCommand cmd, int argIndex) {
        return utf8(cmd.toByteArray(argIndex));
    }

    static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
    }

    static boolean asciiEqualsIgnoreCase(RespCommand cmd, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (cmd.isNull(argIndex)) {
            return false;
        }
        int len = cmd.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = cmd.byteAt(argIndex, i) & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    static boolean asciiEqualsIgnoreCase(byte[] raw, String literal) {
        if (raw == null || literal == null) {
            return false;
        }
        int len = raw.length;
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = raw[i] & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static boolean asciiSliceEqualsIgnoreCase(byte[] raw, int off, int len, String literal) {
        if (raw == null || literal == null) {
            return false;
        }
        if (off < 0 || len < 0 || off + len > raw.length) {
            return false;
        }
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = raw[off + i] & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    static long parseLong(RespCommand cmd, int argIndex, String label) {
        return parseLong(cmd.toByteArray(argIndex), label);
    }

    static long parseNonNegativeLong(RespCommand cmd, int argIndex, String label) {
        long v = parseLong(cmd, argIndex, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
        return v;
    }

    static int parseIntClamped(RespCommand cmd, int argIndex, String label) {
        long v = parseLong(cmd, argIndex, label);
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) v;
    }

    static long parseLong(byte[] s, String label) {
        if (s == null || s.length == 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == s.length) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            if (result < multMin) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    static long parseNonNegativeLong(byte[] s, String label) {
        long v = parseLong(s, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
        return v;
    }

    static int parseIntClamped(byte[] s, String label) {
        long v = parseLong(s, label);
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) v;
    }

    static ScoreBound parseScoreBound(byte[] raw) {
        if (raw == null || raw.length == 0) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }

        int start = 0;
        boolean exclusive = false;
        byte first = raw[0];
        if (first == '(') {
            exclusive = true;
            start = 1;
        } else if (first == '[') {
            start = 1;
        }
        if (start >= raw.length) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }

        int len = raw.length - start;
        if (len == 4 && raw[start] == '-' && asciiSliceEqualsIgnoreCase(raw, start + 1, 3, "INF")) {
            return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive);
        }
        if (len == 4 && raw[start] == '+' && asciiSliceEqualsIgnoreCase(raw, start + 1, 3, "INF")) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }
        if (len == 3 && asciiSliceEqualsIgnoreCase(raw, start, 3, "INF")) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }

        String s = new String(raw, start, len, StandardCharsets.US_ASCII);
        double v;
        try {
            v = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }
        return new ScoreBound(v, exclusive);
    }

    static final class ScoreBound {
        final double value;
        final boolean exclusive;

        private ScoreBound(double value, boolean exclusive) {
            this.value = value;
            this.exclusive = exclusive;
        }
    }

    private static final class WriterBulkStringOutput implements YierdisBulkStringOutput {
        private RespWriter writer;

        void reset(RespWriter writer) {
            this.writer = writer;
        }

        @Override
        public void bulkString(byte[] buf, int off, int len) {
            writer.bulkString(buf, off, len);
        }

        @Override
        public void bulkString(YierdisOffHeapSlice slice) {
            writer.bulkString(slice);
        }

        @Override
        public void bulkStringNull() {
            writer.bulkString((byte[]) null);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            writer.bulkStringLongAscii(value);
        }
    }

    private static final class ByteArraySliceList extends AbstractList<byte[]> implements RandomAccess {
        private byte[][] argv;
        private int offset;
        private int len;

        void reset(byte[][] argv, int offset, int len) {
            this.argv = argv;
            this.offset = offset;
            this.len = len;
        }

        @Override
        public byte[] get(int index) {
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return argv[offset + index];
        }

        @Override
        public int size() {
            return len;
        }
    }

    private static final class RespCommandArgBytesView implements YierdisBytesView {
        private RespCommand cmd;
        private int argIndex;

        RespCommandArgBytesView reset(RespCommand cmd, int argIndex) {
            this.cmd = cmd;
            this.argIndex = argIndex;
            return this;
        }

        @Override
        public int len() {
            return cmd.len(argIndex);
        }

        @Override
        public byte byteAt(int index) {
            return cmd.byteAt(argIndex, index);
        }
    }
}

