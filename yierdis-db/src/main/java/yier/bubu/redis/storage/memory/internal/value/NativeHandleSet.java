package yier.bubu.redis.storage.memory.internal.value;

import java.util.function.Consumer;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StableMemoryBackend;

final class NativeHandleSet {
    private static final int MAX_CAPACITY = 1 << 30;

    private final NativeHandle[] table;
    private int size;

    NativeHandleSet(int expectedHandles) {
        if (expectedHandles < 0) {
            throw new IllegalArgumentException("expectedHandles must be >= 0");
        }
        this.table = expectedHandles == 0 ? new NativeHandle[0] : new NativeHandle[capacity(expectedHandles)];
    }

    boolean add(NativeHandle handle) {
        if (handle == null || handle.isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }
        if (table.length == 0) {
            throw new IllegalStateException("native handle set has zero capacity");
        }
        int slot = slot(handle);
        int mask = table.length - 1;
        for (int probes = 0; probes < table.length; probes++) {
            NativeHandle current = table[slot];
            if (current == null) {
                table[slot] = handle;
                size++;
                return true;
            }
            if (current.equals(handle)) {
                return false;
            }
            slot = (slot + 1) & mask;
        }
        throw new IllegalStateException("native handle set capacity exceeded");
    }

    boolean contains(NativeHandle handle) {
        if (handle == null || handle.isNull() || table.length == 0) {
            return false;
        }
        int slot = slot(handle);
        int mask = table.length - 1;
        for (int probes = 0; probes < table.length; probes++) {
            NativeHandle current = table[slot];
            if (current == null) {
                return false;
            }
            if (current.equals(handle)) {
                return true;
            }
            slot = (slot + 1) & mask;
        }
        return false;
    }

    int size() {
        return size;
    }

    void forEach(Consumer<NativeHandle> consumer) {
        for (NativeHandle handle : table) {
            if (handle != null) {
                consumer.accept(handle);
            }
        }
    }

    void pinAll(StableMemoryBackend allocator) {
        int tableIndex = 0;
        try {
            for (; tableIndex < table.length; tableIndex++) {
                NativeHandle handle = table[tableIndex];
                if (handle != null) {
                    allocator.pin(handle);
                }
            }
        } catch (RuntimeException | Error failure) {
            for (int rollbackIndex = 0; rollbackIndex < tableIndex; rollbackIndex++) {
                NativeHandle handle = table[rollbackIndex];
                if (handle == null) {
                    continue;
                }
                try {
                    allocator.unpin(handle);
                } catch (RuntimeException | Error unpinFailure) {
                    failure.addSuppressed(unpinFailure);
                }
            }
            throw failure;
        }
    }

    void unpinAll(StableMemoryBackend allocator) {
        Throwable failure = null;
        for (NativeHandle handle : table) {
            if (handle == null) {
                continue;
            }
            try {
                allocator.unpin(handle);
            } catch (RuntimeException | Error next) {
                failure = addFailure(failure, next);
            }
        }
        rethrow(failure);
    }

    void freeAll(StableMemoryBackend allocator) {
        Throwable failure = null;
        for (NativeHandle handle : table) {
            if (handle == null) {
                continue;
            }
            try {
                allocator.free(handle);
            } catch (RuntimeException | Error next) {
                failure = addFailure(failure, next);
            }
        }
        rethrow(failure);
    }

    private int slot(NativeHandle handle) {
        long mixed = handle.allocatorId();
        mixed ^= handle.localRaw() + 0x9e3779b97f4a7c15L + (mixed << 6) + (mixed >>> 2);
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
