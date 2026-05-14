package yier.bubu.redis.memory.foreign;

import java.util.ArrayDeque;
import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisStableNativeAllocator implements NativeAllocator {
    private static final int INITIAL_GENERATION = 1;

    private final OffHeapAllocator payloadAllocator;
    private final Slot[] slots;
    private final ArrayDeque<Integer> freeSlots = new ArrayDeque<>();

    private boolean closed;
    private long logicalUsedBytes;
    private long liveObjects;
    private long staleHandleDetections;
    private long reallocInPlaceCount;
    private long reallocMovedCount;

    public YierdisStableNativeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        this(new YierdisForeignOffHeapAllocator(Objects.requireNonNull(runtime, "runtime"), 0), maxSlots);
    }

    public YierdisStableNativeAllocator(OffHeapAllocator payloadAllocator, int maxSlots) {
        this.payloadAllocator = Objects.requireNonNull(payloadAllocator, "payloadAllocator");
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        this.slots = new Slot[maxSlots + 1];
        for (int i = 1; i < slots.length; i++) {
            slots[i] = new Slot(i);
            freeSlots.addLast(i);
        }
    }

    @Override
    public synchronized NativeHandle allocate(NativeObjectKind kind, int size) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }

        Integer slotId = freeSlots.pollFirst();
        if (slotId == null) {
            throw new NativeMemoryException("native object slot limit exceeded");
        }

        Slot slot = slots[slotId];
        OffHeapBuf buffer = null;
        boolean allocated = false;
        try {
            buffer = payloadAllocator.allocate(size);
            slot.allocate(kind, buffer, size);
            logicalUsedBytes += size;
            liveObjects++;
            allocated = true;
            return NativeHandle.of(kind.domain(), kind, slotId, slot.generation, 0);
        } finally {
            if (!allocated) {
                if (buffer != null) {
                    buffer.close();
                }
                slot.resetAfterFailedAllocation();
                freeSlots.addFirst(slotId);
            }
        }
    }

    @Override
    public synchronized NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        ensureOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0");
        }

        Slot slot = requireLiveSlot(handle);
        int oldSize = slot.size;
        if (newSize <= oldSize) {
            slot.size = newSize;
            logicalUsedBytes += (long) newSize - oldSize;
            reallocInPlaceCount++;
            return handle;
        }

        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("native object cannot grow in place");
        }

        OffHeapBuf next = payloadAllocator.allocate(newSize);
        boolean moved = false;
        try {
            byte[] copy = new byte[oldSize];
            slot.buffer.getBytes(0, copy, 0, copy.length);
            next.setBytes(0, copy, 0, copy.length);

            OffHeapBuf previous = slot.buffer;
            slot.buffer = next;
            slot.size = newSize;
            slot.capacity = newSize;
            logicalUsedBytes += (long) newSize - oldSize;
            previous.close();
            reallocMovedCount++;
            moved = true;
            return handle;
        } finally {
            if (!moved) {
                next.close();
            }
        }
    }

    @Override
    public synchronized void free(NativeHandle handle) {
        ensureOpen();
        Slot slot = requireLiveSlot(handle);
        if (slot.pinCount > 0) {
            slot.quarantined = true;
            return;
        }
        releaseSlot(slot);
    }

    @Override
    public synchronized NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        Objects.requireNonNull(mode, "mode");
        Slot slot = requireLiveSlot(handle);
        return new StableObjectView(handle, slot, mode);
    }

    @Override
    public synchronized NativeAllocatorStats stats() {
        return new NativeAllocatorStats(
                logicalUsedBytes,
                payloadAllocator.usedBytes(),
                liveObjects,
                pinnedObjects(),
                quarantinedObjects(),
                staleHandleDetections,
                reallocInPlaceCount,
                reallocMovedCount
        );
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;

        RuntimeException failure = null;
        for (int i = 1; i < slots.length; i++) {
            Slot slot = slots[i];
            if (slot.live && slot.buffer != null) {
                try {
                    slot.buffer.close();
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                } finally {
                    slot.clear();
                }
            }
        }

        try {
            payloadAllocator.close();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private Slot requireLiveSlot(NativeHandle handle) {
        if (handle == null || handle.isNull()) {
            throw new IllegalArgumentException("native handle must not be null");
        }

        long slotId = handle.slotId();
        if (slotId <= 0 || slotId >= slots.length) {
            return stale("stale native handle: unknown slot " + slotId);
        }

        Slot slot = slots[(int) slotId];
        if (!slot.live || slot.generation != handle.generation()) {
            return stale("stale native handle: slot=" + slotId + " generation=" + handle.generation());
        }

        if (slot.kind.code() != handle.kindCode() || slot.kind.domain() != handle.domain()) {
            throw new NativeMemoryException("native handle kind/domain mismatch: " + handle.raw());
        }

        return slot;
    }

    private Slot stale(String message) {
        staleHandleDetections++;
        throw new StaleNativeHandleException(message);
    }

    private void releaseSlot(Slot slot) {
        OffHeapBuf buffer = slot.buffer;
        int size = slot.size;
        buffer.close();
        logicalUsedBytes -= size;
        liveObjects--;
        slot.clear();
        slot.generation = nextGeneration(slot.generation);
        freeSlots.addLast(slot.slotId);
    }

    private long pinnedObjects() {
        long count = 0;
        for (int i = 1; i < slots.length; i++) {
            if (slots[i].pinCount > 0) {
                count++;
            }
        }
        return count;
    }

    private long quarantinedObjects() {
        long count = 0;
        for (int i = 1; i < slots.length; i++) {
            if (slots[i].quarantined) {
                count++;
            }
        }
        return count;
    }

    private static int nextGeneration(int current) {
        int next = (current + 1) & 0x0fff;
        return next == 0 ? INITIAL_GENERATION : next;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("stable native allocator is closed");
        }
    }

    private static final class Slot {
        private final int slotId;
        private int generation = INITIAL_GENERATION;
        private NativeObjectKind kind;
        private OffHeapBuf buffer;
        private int size;
        private int capacity;
        private int pinCount;
        private boolean live;
        private boolean quarantined;

        private Slot(int slotId) {
            this.slotId = slotId;
        }

        private void allocate(NativeObjectKind kind, OffHeapBuf buffer, int size) {
            this.kind = kind;
            this.buffer = buffer;
            this.size = size;
            this.capacity = buffer.capacity();
            this.pinCount = 0;
            this.live = true;
            this.quarantined = false;
        }

        private void resetAfterFailedAllocation() {
            this.kind = null;
            this.buffer = null;
            this.size = 0;
            this.capacity = 0;
            this.pinCount = 0;
            this.live = false;
            this.quarantined = false;
        }

        private void clear() {
            this.kind = null;
            this.buffer = null;
            this.size = 0;
            this.capacity = 0;
            this.pinCount = 0;
            this.live = false;
            this.quarantined = false;
        }
    }

    private final class StableObjectView implements NativeObjectView {
        private final NativeHandle handle;
        private final Slot slot;
        private final NativeAccessMode mode;
        private boolean closedView;

        private StableObjectView(NativeHandle handle, Slot slot, NativeAccessMode mode) {
            this.handle = handle;
            this.slot = slot;
            this.mode = mode;
        }

        @Override
        public NativeHandle handle() {
            ensureViewOpen();
            return handle;
        }

        @Override
        public int size() {
            ensureLiveSlot();
            return slot.size;
        }

        @Override
        public int capacity() {
            ensureLiveSlot();
            return slot.capacity;
        }

        @Override
        public byte getByte(int index) {
            ensureLiveSlot();
            checkRange(index, 1);
            return slot.buffer.getByte(index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureWritable();
            checkRange(index, 1);
            slot.buffer.setByte(index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            ensureLiveSlot();
            checkRange(index, len);
            slot.buffer.getBytes(index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            ensureWritable();
            checkRange(index, len);
            slot.buffer.setBytes(index, src, srcOff, len);
        }

        @Override
        public void close() {
            closedView = true;
        }

        private void ensureWritable() {
            ensureLiveSlot();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new NativeMemoryException("resolved object is read-only");
            }
        }

        private void ensureViewOpen() {
            if (closedView) {
                throw new IllegalStateException("native object view is closed");
            }
        }

        private void ensureLiveSlot() {
            ensureViewOpen();
            requireLiveSlot(handle);
        }

        private void checkRange(int index, int len) {
            if (len < 0 || index < 0 || index + len > slot.size) {
                throw new IndexOutOfBoundsException();
            }
        }
    }
}
