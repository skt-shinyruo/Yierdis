package yier.bubu.redis.storage.memory.internal.ffm;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSpan;

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
