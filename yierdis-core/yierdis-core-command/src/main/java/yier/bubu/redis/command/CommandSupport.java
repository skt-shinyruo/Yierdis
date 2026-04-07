package yier.bubu.redis.command;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.DbIndexProvider;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

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
    private final YierdisDbRouter dbRouter;
    private final ServerInfoProvider infoProvider;
    private final SlowCommandGovernor slowGovernor;

    private final ByteArraySliceList slice = new ByteArraySliceList();
    private byte[][] argvScratch = new byte[16][];
    private final CommandArgBytesView argView = new CommandArgBytesView();
    private final CommandArgBytesSlice argSlice = new CommandArgBytesSlice();

    CommandSupport(DbEngine engine) {
        this(singleDbRouter(engine), null, SlowCommandGovernor.DEFAULT);
    }

    CommandSupport(DbEngine engine, ServerInfoProvider infoProvider) {
        this(singleDbRouter(engine), infoProvider, SlowCommandGovernor.DEFAULT);
    }

    CommandSupport(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider) {
        this(dbRouter, infoProvider, SlowCommandGovernor.DEFAULT);
    }

    CommandSupport(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor) {
        this.dbRouter = java.util.Objects.requireNonNull(dbRouter, "dbRouter");
        this.infoProvider = infoProvider;
        this.slowGovernor = slowGovernor == null ? SlowCommandGovernor.DEFAULT : slowGovernor;
    }

    DbEngine db(CommandContext ctx) {
        java.util.Objects.requireNonNull(ctx, "ctx");
        return dbRouter.dbFor(ctx.dbIndexProviderOrNull());
    }

    DbReads dbReads(CommandContext ctx) {
        return db(ctx).reads();
    }

    DbWrites dbWrites(CommandContext ctx) {
        return db(ctx).writes();
    }

    int databases() {
        return dbRouter.databases();
    }

    ServerInfoProvider infoProvider() {
        return infoProvider;
    }

    SlowCommandGovernor slowGovernor() {
        return slowGovernor;
    }

    BytesView argView(ExecutionRequest request, int argIndex) {
        return argView.reset(request, argIndex);
    }

    BytesSlice argSlice(ExecutionRequest request, int argIndex) {
        return argSlice.reset(request, argIndex);
    }

    java.util.List<byte[]> slice() {
        return slice;
    }

    void sliceResetFromRequest(ExecutionRequest request, int argStart, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be non-negative");
        }
        if (len == 0) {
            slice.reset(argvScratch, 0, 0);
            return;
        }
        ensureScratchCapacity(len);
        for (int i = 0; i < len; i++) {
            argvScratch[i] = request.readOnlyByteArray(argStart + i);
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

    private static YierdisDbRouter singleDbRouter(DbEngine engine) {
        DbEngine fixed = java.util.Objects.requireNonNull(engine, "engine");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexProvider dbIndexProvider) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }

    static void wrongArity(ReplyWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }

    static String utf8(ExecutionRequest request, int argIndex) {
        return utf8(request.readOnlyByteArray(argIndex));
    }

    static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
    }

    static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (request.isNull(argIndex)) {
            return false;
        }
        int len = request.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
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

    static long parseLong(ExecutionRequest request, int argIndex, String label) {
        return parseLong(request.readOnlyByteArray(argIndex), label);
    }

    static long parseNonNegativeLong(ExecutionRequest request, int argIndex, String label) {
        long v = parseLong(request, argIndex, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
        return v;
    }

    static int parseIntClamped(ExecutionRequest request, int argIndex, String label) {
        long v = parseLong(request, argIndex, label);
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
            throw new IllegalArgumentException("value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == s.length) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            if (result < multMin) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    static long parseNonNegativeLong(byte[] s, String label) {
        long v = parseLong(s, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
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
            throw new YierdisCommandException("ERR min or max is not a float");
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
            throw new YierdisCommandException("ERR min or max is not a float");
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
            throw new YierdisCommandException("ERR min or max is not a float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisCommandException("ERR min or max is not a float");
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

    private static final class CommandArgBytesView implements BytesView {
        private ExecutionRequest request;
        private int argIndex;

        CommandArgBytesView reset(ExecutionRequest request, int argIndex) {
            this.request = request;
            this.argIndex = argIndex;
            return this;
        }

        @Override
        public int length() {
            return request.len(argIndex);
        }

        @Override
        public byte getByte(int index) {
            return request.byteAt(argIndex, index);
        }
    }

    private static final class CommandArgBytesSlice implements BytesSlice {
        private static final int WRITE_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_WRITE_BUF =
                ThreadLocal.withInitial(() -> new byte[WRITE_CHUNK_BYTES]);

        private ExecutionRequest request;
        private int argIndex;
        private BytesSource frame;
        private int frameOffset;

        CommandArgBytesSlice reset(ExecutionRequest request, int argIndex) {
            this.request = request;
            this.argIndex = argIndex;
            // temporary compatibility seam for zero-copy/frame-backed Command producers
            // until the remaining producer migration lands in later tasks.
            if (request instanceof yier.bubu.redis.contract.Command cmd) {
                this.frame = cmd.frame();
                this.frameOffset = frame == null ? -1 : cmd.argOffset(argIndex);
            } else {
                this.frame = null;
                this.frameOffset = -1;
            }
            return this;
        }

        @Override
        public int length() {
            if (request == null) {
                return 0;
            }
            int len = request.len(argIndex);
            return Math.max(0, len);
        }

        @Override
        public byte getByte(int index) {
            int len = length();
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException();
            }
            if (frame != null && frameOffset >= 0) {
                return frame.getByte(frameOffset + index);
            }
            return request.byteAt(argIndex, index);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            int l = length();
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            if (index < 0 || index + len > l) {
                throw new IndexOutOfBoundsException();
            }
            if (dst == null) {
                throw new IllegalArgumentException("dst must not be null");
            }
            if (dstOff < 0 || dstOff + len > dst.length) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                return;
            }

            if (frame != null && frameOffset >= 0) {
                frame.getBytes(frameOffset + index, dst, dstOff, len);
                return;
            }

            if (index == 0 && len == l) {
                request.copyToByteArray(argIndex, dst, dstOff);
                return;
            }
            for (int i = 0; i < len; i++) {
                dst[dstOff + i] = request.byteAt(argIndex, index + i);
            }
        }

        @Override
        public void writeTo(BytesSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            int len = length();
            if (len <= 0) {
                return;
            }
            byte[] scratch = TL_WRITE_BUF.get();
            int index = 0;
            while (index < len) {
                int chunk = Math.min(len - index, scratch.length);
                getBytes(index, scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                index += chunk;
            }
        }

        @Override
        public boolean hasMemoryAddress() {
            return frame != null && frameOffset >= 0 && frame.hasMemoryAddress();
        }

        @Override
        public long memoryAddress() {
            if (!hasMemoryAddress()) {
                throw new UnsupportedOperationException("memoryAddress not supported");
            }
            return frame.memoryAddress() + frameOffset;
        }
    }
}
