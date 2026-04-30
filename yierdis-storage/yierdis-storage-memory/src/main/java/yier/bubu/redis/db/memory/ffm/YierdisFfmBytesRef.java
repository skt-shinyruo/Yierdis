package yier.bubu.redis.db.memory.ffm;

import yier.bubu.redis.db.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.db.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.db.memory.foreign.YierdisFfmSpan;

import java.util.Objects;

public record YierdisFfmBytesRef(YierdisFfmRegion region, int offset, int length) {
    public YierdisFfmBytesRef {
        Objects.requireNonNull(region, "region");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (offset + length > region.size()) {
            throw new IndexOutOfBoundsException();
        }
    }

    public YierdisFfmSpan span() {
        return region.span(offset, length);
    }

    public byte byteAt(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + length);
        }
        return YierdisFfmAccess.getByte(span(), index);
    }
}
