package yier.bubu.redis.memory.foreign;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

public record YierdisFfmSpan(MemorySegment segment) {
    public YierdisFfmSpan {
        Objects.requireNonNull(segment, "segment");
    }

    public int size() {
        return Math.toIntExact(segment.byteSize());
    }

    public YierdisFfmSpan slice(int offset, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (offset < 0 || offset + length > size()) {
            throw new IndexOutOfBoundsException();
        }
        return new YierdisFfmSpan(segment.asSlice(offset, length));
    }
}
