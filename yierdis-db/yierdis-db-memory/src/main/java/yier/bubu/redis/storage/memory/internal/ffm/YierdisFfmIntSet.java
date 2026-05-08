package yier.bubu.redis.storage.memory.internal.ffm;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSpan;

public final class YierdisFfmIntSet implements AutoCloseable {
    private static final int LONG_BYTES = Long.BYTES;

    private final YierdisFfmMemoryRuntime memoryRuntime;
    private YierdisFfmRegion region;
    private int capacity;
    private int size;

    public YierdisFfmIntSet(YierdisFfmMemoryRuntime memoryRuntime) {
        if (memoryRuntime == null) {
            throw new IllegalArgumentException("memoryRuntime must not be null");
        }
        this.memoryRuntime = memoryRuntime;
    }

    public int size() {
        return size;
    }

    public boolean contains(long value) {
        return indexOf(value) >= 0;
    }

    public boolean add(long value) {
        ensureCapacity(size + 1);
        int index = indexOf(value);
        if (index >= 0) {
            return false;
        }
        int insertAt = -(index + 1);
        for (int i = size; i > insertAt; i--) {
            setLong(i, getLong(i - 1));
        }
        setLong(insertAt, value);
        size++;
        return true;
    }

    public boolean remove(long value) {
        int index = indexOf(value);
        if (index < 0) {
            return false;
        }
        for (int i = index; i + 1 < size; i++) {
            setLong(i, getLong(i + 1));
        }
        size--;
        return true;
    }

    public long get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        return getLong(index);
    }

    @Override
    public void close() {
        if (region != null) {
            region.close();
            region = null;
            capacity = 0;
            size = 0;
        }
    }

    private void ensureCapacity(int desired) {
        if (capacity >= desired) {
            return;
        }
        int next = Math.max(4, capacity);
        while (next < desired) {
            next <<= 1;
        }
        YierdisFfmRegion nextRegion = memoryRuntime.allocateRegion("intset", next * LONG_BYTES);
        if (region != null && size > 0) {
            YierdisFfmSpan src = region.span(0, size * LONG_BYTES);
            YierdisFfmSpan dst = nextRegion.span(0, size * LONG_BYTES);
            for (int i = 0; i < size; i++) {
                YierdisFfmAccess.setLong(dst, i * LONG_BYTES, YierdisFfmAccess.getLong(src, i * LONG_BYTES));
            }
            region.close();
        }
        region = nextRegion;
        capacity = next;
    }

    private int indexOf(long value) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long candidate = getLong(mid);
            if (candidate < value) {
                low = mid + 1;
            } else if (candidate > value) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private long getLong(int index) {
        return YierdisFfmAccess.getLong(region.span(0, capacity * LONG_BYTES), index * LONG_BYTES);
    }

    private void setLong(int index, long value) {
        YierdisFfmAccess.setLong(region.span(0, capacity * LONG_BYTES), index * LONG_BYTES, value);
    }
}
