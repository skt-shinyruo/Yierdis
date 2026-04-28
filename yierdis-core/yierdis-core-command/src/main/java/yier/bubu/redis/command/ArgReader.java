package yier.bubu.redis.command;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

final class ArgReader {
    private final ExecutionRequest request;

    private ArgReader(ExecutionRequest request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    static ArgReader of(ExecutionRequest request) {
        return new ArgReader(request);
    }

    ExecutionRequest request() {
        return request;
    }

    int argc() {
        return request.argc();
    }

    boolean isNull(int index) {
        return request.isNull(index);
    }

    int len(int index) {
        return request.len(index);
    }

    byte[] bytes(int index) {
        return request.readOnlyByteArray(index);
    }

    boolean is(int index, String literal) {
        return CommandSupport.asciiEqualsIgnoreCase(request, index, literal);
    }

    long longAt(int index) {
        return CommandSupport.parseLong(request, index, "value");
    }

    long nonNegativeLongAt(int index) {
        return CommandSupport.parseNonNegativeLong(request, index, "value");
    }

    long positiveLongAt(int index) {
        long v = longAt(index);
        if (v <= 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
        return v;
    }

    int intClampedAt(int index) {
        return CommandSupport.parseIntClamped(request, index, "value");
    }

    BytesView view(CommandSupport support, int index) {
        return support.argView(request, index);
    }

    BytesSlice slice(CommandSupport support, int index) {
        return support.argSlice(request, index);
    }
}
