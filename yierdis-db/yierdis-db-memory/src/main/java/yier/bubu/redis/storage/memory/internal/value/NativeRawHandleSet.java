package yier.bubu.redis.storage.memory.internal.value;

import java.util.function.LongConsumer;
import yier.bubu.redis.memory.api.NativeAllocator;

final class NativeRawHandleSet {
    private static final int MAX_CAPACITY = 1 << 30;

    private final long[] table;
    private int size;

    NativeRawHandleSet(int expectedHandles) {
        if (expectedHandles < 0) {
            throw new IllegalArgumentException("expectedHandles must be >= 0");
        }
        this.table = expectedHandles == 0 ? new long[0] : new long[capacity(expectedHandles)];
    }

    boolean add(long rawHandle) {
        if (rawHandle == 0L) {
            throw new IllegalArgumentException("rawHandle must not be null");
        }
        if (table.length == 0) {
            throw new IllegalStateException("native raw handle set has zero capacity");
        }
        int slot = slot(rawHandle);
        int mask = table.length - 1;
        for (int probes = 0; probes < table.length; probes++) {
            long current = table[slot];
            if (current == 0L) {
                table[slot] = rawHandle;
                size++;
                return true;
            }
            if (current == rawHandle) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
        throw new IllegalStateException("native raw handle set capacity exceeded");
    }

    boolean contains(long rawHandle) {
        if (rawHandle == 0L || table.length == 0) {
            return false;
        }
        int slot = slot(rawHandle);
        int mask = table.length - 1;
        for (int probes = 0; probes < table.length; probes++) {
            long current = table[slot];
            if (current == 0L) {
                return false;
            }
            if (current == rawHandle) {
                return true;
            }
            slot = (slot + 1) & mask;
        }
        return false;
    }

    int size() {
        return size;
    }

    void forEach(LongConsumer consumer) {
        for (long rawHandle : table) {
            if (rawHandle != 0L) {
                consumer.accept(rawHandle);
            }
        }
    }

    void pinAll(NativeAllocator allocator) {
        int tableIndex = 0;
        try {
            for (; tableIndex < table.length; tableIndex++) {
                long rawHandle = table[tableIndex];
                if (rawHandle != 0L) {
                    allocator.pinRaw(rawHandle);
                }
            }
        } catch (RuntimeException | Error failure) {
            for (int rollbackIndex = 0; rollbackIndex < tableIndex; rollbackIndex++) {
                long rawHandle = table[rollbackIndex];
                if (rawHandle == 0L) {
                    continue;
                }
                try {
                    allocator.unpinRaw(rawHandle);
                } catch (RuntimeException | Error unpinFailure) {
                    failure.addSuppressed(unpinFailure);
                }
            }
            throw failure;
        }
    }

    void unpinAll(NativeAllocator allocator) {
        Throwable failure = null;
        for (long rawHandle : table) {
            if (rawHandle == 0L) {
                continue;
            }
            try {
                allocator.unpinRaw(rawHandle);
            } catch (RuntimeException | Error next) {
                failure = addFailure(failure, next);
            }
        }
        rethrow(failure);
    }

    void freeAll(NativeAllocator allocator) {
        Throwable failure = null;
        for (long rawHandle : table) {
            if (rawHandle == 0L) {
                continue;
            }
            try {
                allocator.freeRaw(rawHandle);
            } catch (RuntimeException | Error next) {
                failure = addFailure(failure, next);
            }
        }
        rethrow(failure);
    }

    private int slot(long rawHandle) {
        long mixed = rawHandle;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdl;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53l;
        mixed ^= mixed >>> 33;
        return (int) mixed & (table.length - 1);
    }

    private static int capacity(int expectedHandles) {
        long required = Math.max(4L, (long) expectedHandles * 2L);
        if (required > MAX_CAPACITY) {
            throw new IllegalArgumentException("too many retained native handles");
        }
        int capacity = 4;
        while (capacity < required) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new AssertionError("unexpected native handle release failure", failure);
    }
}
