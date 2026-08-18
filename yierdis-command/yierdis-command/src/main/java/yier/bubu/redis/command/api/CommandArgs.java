package yier.bubu.redis.command.api;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class CommandArgs {
    private static final String INTEGER_ERROR = "ERR value is not an integer or out of range";

    private final ExecutionRequest request;

    public CommandArgs(ExecutionRequest request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    public ExecutionRequest request() {
        return request;
    }

    public int argc() {
        return request.argc();
    }

    public boolean isNull(int index) {
        return request.isNull(index);
    }

    public int length(int index) {
        return request.len(index);
    }

    public BytesSlice slice(int index) {
        return new RequestSlice(request, index);
    }

    public byte[] bytes(int index) {
        return request.readOnlyByteArray(index);
    }

    public String utf8(int index) {
        byte[] value = bytes(index);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public boolean is(int index, String asciiLiteral) {
        if (request.isNull(index) || asciiLiteral == null) {
            return false;
        }
        int length = request.len(index);
        if (length != asciiLiteral.length()) {
            return false;
        }
        for (int offset = 0; offset < length; offset++) {
            int actual = request.byteAt(index, offset) & 0xff;
            int expected = asciiLiteral.charAt(offset);
            if (expected > 0x7f || foldAsciiCase(actual) != foldAsciiCase(expected)) {
                return false;
            }
        }
        return true;
    }

    public long longAt(int index) {
        byte[] value = bytes(index);
        if (value == null) {
            throw integerFailure();
        }
        try {
            return Long.parseLong(new String(value, StandardCharsets.US_ASCII));
        } catch (NumberFormatException failure) {
            throw integerFailure();
        }
    }

    public long nonNegativeLongAt(int index) {
        long value = longAt(index);
        if (value < 0) {
            throw integerFailure();
        }
        return value;
    }

    public long positiveLongAt(int index) {
        long value = longAt(index);
        if (value <= 0) {
            throw integerFailure();
        }
        return value;
    }

    public int intClampedAt(int index) {
        return Math.clamp(longAt(index), Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public List<byte[]> byteArraysFrom(int firstIndex) {
        int argc = request.argc();
        if (firstIndex < 0 || firstIndex > argc) {
            throw new IndexOutOfBoundsException("firstIndex: " + firstIndex + ", argc: " + argc);
        }
        List<byte[]> values = new ArrayList<>(argc - firstIndex);
        for (int index = firstIndex; index < argc; index++) {
            values.add(bytes(index));
        }
        return Collections.unmodifiableList(values);
    }

    private static int foldAsciiCase(int value) {
        return value >= 'A' && value <= 'Z' ? value | 0x20 : value;
    }

    private static CommandParseException integerFailure() {
        return new CommandParseException(INTEGER_ERROR);
    }

    private record RequestSlice(ExecutionRequest request, int argumentIndex) implements BytesSlice {
        @Override
        public int length() {
            return request.len(argumentIndex);
        }

        @Override
        public byte getByte(int index) {
            return request.byteAt(argumentIndex, index);
        }

        @Override
        public void writeTo(BytesSink out) {
            Objects.requireNonNull(out, "out");
            byte[] value = request.readOnlyByteArray(argumentIndex);
            if (value == null) {
                throw new IllegalStateException("arg is null");
            }
            out.writeBytes(value, 0, value.length);
        }
    }
}
