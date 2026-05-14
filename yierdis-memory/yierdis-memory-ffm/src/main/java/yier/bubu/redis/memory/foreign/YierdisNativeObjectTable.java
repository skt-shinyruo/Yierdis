package yier.bubu.redis.memory.foreign;

import java.util.ArrayDeque;
import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisNativeObjectTable implements AutoCloseable {
    public static final int STATE_FREE = 0;
    public static final int STATE_ALLOCATED = 1;
    public static final int STATE_PINNED = 2;
    public static final int STATE_MOVING = 3;
    public static final int STATE_FREED_QUARANTINED = 4;
    public static final int STATE_CORRUPT = 5;

    public static final int META_BYTES = 72;

    private static final int ADDRESS_OFFSET = 0;
    private static final int SIZE_OFFSET = 8;
    private static final int CAPACITY_OFFSET = 12;
    private static final int SEGMENT_ID_OFFSET = 16;
    private static final int PAGE_CLASS_OFFSET = 20;
    private static final int GENERATION_OFFSET = 24;
    private static final int DOMAIN_OFFSET = 28;
    private static final int KIND_OFFSET = 32;
    private static final int FLAGS_OFFSET = 36;
    private static final int PIN_COUNT_OFFSET = 40;
    private static final int OWNER_SHARD_ID_OFFSET = 44;
    private static final int ALLOC_EPOCH_OFFSET = 48;
    private static final int FREE_EPOCH_OFFSET = 56;
    private static final int STATE_OFFSET = 64;

    private static final int INITIAL_GENERATION = 1;
    private static final int MAX_GENERATION = 0x0fff;

    private final int maxSlots;
    private final int ownerShardId;
    private final YierdisFfmRegion metadataRegion;
    private final ArrayDeque<Integer> freeSlots = new ArrayDeque<>();
    private final boolean[] retired;

    private boolean closed;

    public YierdisNativeObjectTable(YierdisFfmMemoryRuntime runtime, int maxSlots, int ownerShardId) {
        Objects.requireNonNull(runtime, "runtime");
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        this.maxSlots = maxSlots;
        this.ownerShardId = ownerShardId;
        this.metadataRegion = runtime.allocateRegion("native-object-table", Math.multiplyExact(maxSlots, META_BYTES));
        this.retired = new boolean[maxSlots + 1];
        for (int slotId = 1; slotId <= maxSlots; slotId++) {
            writeInt(slotId, GENERATION_OFFSET, INITIAL_GENERATION);
            writeInt(slotId, OWNER_SHARD_ID_OFFSET, ownerShardId);
            writeInt(slotId, STATE_OFFSET, STATE_FREE);
            freeSlots.addLast(slotId);
        }
    }

    public synchronized NativeHandle allocate(
            NativeObjectKind kind,
            int size,
            int capacity,
            long address,
            int pageClass,
            long allocEpoch
    ) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }

        Integer slotId = freeSlots.pollFirst();
        if (slotId == null) {
            throw new NativeMemoryException("native object slot limit exceeded");
        }
        if (retired[slotId]) {
            throw new NativeMemoryException("native object slot is retired");
        }

        int generation = readInt(slotId, GENERATION_OFFSET);
        writeLong(slotId, ADDRESS_OFFSET, address);
        writeInt(slotId, SIZE_OFFSET, size);
        writeInt(slotId, CAPACITY_OFFSET, capacity);
        writeInt(slotId, SEGMENT_ID_OFFSET, 0);
        writeInt(slotId, PAGE_CLASS_OFFSET, pageClass);
        writeInt(slotId, DOMAIN_OFFSET, kind.domain().code());
        writeInt(slotId, KIND_OFFSET, kind.code());
        writeInt(slotId, FLAGS_OFFSET, 0);
        writeInt(slotId, PIN_COUNT_OFFSET, 0);
        writeInt(slotId, OWNER_SHARD_ID_OFFSET, ownerShardId);
        writeLong(slotId, ALLOC_EPOCH_OFFSET, allocEpoch);
        writeLong(slotId, FREE_EPOCH_OFFSET, 0L);
        writeInt(slotId, STATE_OFFSET, STATE_ALLOCATED);
        return NativeHandle.of(kind.domain(), kind, slotId, generation, 0);
    }

    public synchronized YierdisNativeObjectMeta resolve(NativeHandle handle) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, false);
        return readMeta(ref.slotId);
    }

    public synchronized YierdisNativeObjectMeta snapshot(NativeHandle handle, boolean allowQuarantined) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, allowQuarantined);
        return readMeta(ref.slotId);
    }

    public synchronized void free(NativeHandle handle, long freeEpoch) {
        free(handle, freeEpoch, false);
    }

    public synchronized void free(NativeHandle handle, long freeEpoch, boolean forceQuarantine) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, false);
        int pinCount = readInt(ref.slotId, PIN_COUNT_OFFSET);
        if (forceQuarantine || pinCount > 0) {
            writeLong(ref.slotId, FREE_EPOCH_OFFSET, freeEpoch);
            writeInt(ref.slotId, STATE_OFFSET, STATE_FREED_QUARANTINED);
            return;
        }
        releaseSlot(ref.slotId, freeEpoch);
    }

    public synchronized void pin(NativeHandle handle) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, false);
        int pinCount = readInt(ref.slotId, PIN_COUNT_OFFSET);
        writeInt(ref.slotId, PIN_COUNT_OFFSET, pinCount + 1);
        writeInt(ref.slotId, STATE_OFFSET, STATE_PINNED);
    }

    public synchronized void unpin(NativeHandle handle) {
        unpin(handle, true);
    }

    public synchronized void unpin(NativeHandle handle, boolean releaseQuarantinedOnZero) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, true);
        int pinCount = readInt(ref.slotId, PIN_COUNT_OFFSET);
        if (pinCount <= 0) {
            throw new NativeMemoryException("native object is not pinned");
        }
        pinCount--;
        writeInt(ref.slotId, PIN_COUNT_OFFSET, pinCount);
        int state = readInt(ref.slotId, STATE_OFFSET);
        if (pinCount == 0 && state == STATE_FREED_QUARANTINED && releaseQuarantinedOnZero) {
            releaseSlot(ref.slotId, readLong(ref.slotId, FREE_EPOCH_OFFSET));
        } else if (pinCount == 0) {
            if (state != STATE_FREED_QUARANTINED) {
                writeInt(ref.slotId, STATE_OFFSET, STATE_ALLOCATED);
            }
        }
    }

    public synchronized void releaseQuarantined(NativeHandle handle) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, true);
        int state = readInt(ref.slotId, STATE_OFFSET);
        if (state != STATE_FREED_QUARANTINED) {
            throw new NativeMemoryException("native object is not quarantined");
        }
        if (readInt(ref.slotId, PIN_COUNT_OFFSET) != 0) {
            throw new NativeMemoryException("native object is still pinned");
        }
        releaseSlot(ref.slotId, readLong(ref.slotId, FREE_EPOCH_OFFSET));
    }

    public synchronized void updateLocation(NativeHandle handle, int size, int capacity, long address, int pageClass) {
        ensureOpen();
        SlotRef ref = requireLiveSlot(handle, false);
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }
        writeLong(ref.slotId, ADDRESS_OFFSET, address);
        writeInt(ref.slotId, SIZE_OFFSET, size);
        writeInt(ref.slotId, CAPACITY_OFFSET, capacity);
        writeInt(ref.slotId, PAGE_CLASS_OFFSET, pageClass);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        metadataRegion.close();
        freeSlots.clear();
    }

    private SlotRef requireLiveSlot(NativeHandle handle, boolean allowQuarantined) {
        if (handle == null) {
            return stale("stale native handle: null");
        }
        long slotIdLong = handle.slotId();
        if (slotIdLong <= 0 || slotIdLong > maxSlots) {
            return stale("stale native handle: unknown slot " + slotIdLong);
        }
        int slotId = (int) slotIdLong;
        int state = readInt(slotId, STATE_OFFSET);
        int generation = readInt(slotId, GENERATION_OFFSET);
        if (state == STATE_FREE || retired[slotId] || generation != handle.generation()) {
            return stale("stale native handle: slot=" + slotId + " generation=" + handle.generation());
        }
        if (state == STATE_FREED_QUARANTINED && !allowQuarantined) {
            return stale("stale native handle: quarantined slot=" + slotId);
        }
        NativeHandleDomain domain = NativeHandleDomain.fromCode(readInt(slotId, DOMAIN_OFFSET));
        int kindCode = readInt(slotId, KIND_OFFSET);
        if (domain != handle.domain() || kindCode != handle.kindCode()) {
            throw new NativeMemoryException("native handle kind/domain mismatch: " + handle.raw());
        }
        return new SlotRef(slotId);
    }

    private void releaseSlot(int slotId, long freeEpoch) {
        int generation = readInt(slotId, GENERATION_OFFSET);
        writeLong(slotId, ADDRESS_OFFSET, 0L);
        writeInt(slotId, SIZE_OFFSET, 0);
        writeInt(slotId, CAPACITY_OFFSET, 0);
        writeInt(slotId, SEGMENT_ID_OFFSET, 0);
        writeInt(slotId, PAGE_CLASS_OFFSET, 0);
        writeInt(slotId, DOMAIN_OFFSET, 0);
        writeInt(slotId, KIND_OFFSET, 0);
        writeInt(slotId, FLAGS_OFFSET, 0);
        writeInt(slotId, PIN_COUNT_OFFSET, 0);
        writeLong(slotId, FREE_EPOCH_OFFSET, freeEpoch);
        writeInt(slotId, STATE_OFFSET, STATE_FREE);
        if (generation >= MAX_GENERATION) {
            retired[slotId] = true;
            return;
        }
        writeInt(slotId, GENERATION_OFFSET, generation + 1);
        freeSlots.addLast(slotId);
    }

    private YierdisNativeObjectMeta readMeta(int slotId) {
        return new YierdisNativeObjectMeta(
                slotId,
                readLong(slotId, ADDRESS_OFFSET),
                readInt(slotId, SIZE_OFFSET),
                readInt(slotId, CAPACITY_OFFSET),
                readInt(slotId, SEGMENT_ID_OFFSET),
                readInt(slotId, PAGE_CLASS_OFFSET),
                readInt(slotId, GENERATION_OFFSET),
                NativeHandleDomain.fromCode(readInt(slotId, DOMAIN_OFFSET)),
                readInt(slotId, KIND_OFFSET),
                readInt(slotId, FLAGS_OFFSET),
                readInt(slotId, PIN_COUNT_OFFSET),
                readInt(slotId, OWNER_SHARD_ID_OFFSET),
                readLong(slotId, ALLOC_EPOCH_OFFSET),
                readLong(slotId, FREE_EPOCH_OFFSET),
                readInt(slotId, STATE_OFFSET)
        );
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native object table is closed");
        }
    }

    private SlotRef stale(String message) {
        throw new StaleNativeHandleException(message);
    }

    private long readLong(int slotId, int fieldOffset) {
        return YierdisFfmAccess.getLong(metaSpan(slotId), fieldOffset);
    }

    private void writeLong(int slotId, int fieldOffset, long value) {
        YierdisFfmAccess.setLong(metaSpan(slotId), fieldOffset, value);
    }

    private int readInt(int slotId, int fieldOffset) {
        return YierdisFfmAccess.getInt(metaSpan(slotId), fieldOffset);
    }

    private void writeInt(int slotId, int fieldOffset, int value) {
        YierdisFfmAccess.setInt(metaSpan(slotId), fieldOffset, value);
    }

    private YierdisFfmSpan metaSpan(int slotId) {
        if (slotId <= 0 || slotId > maxSlots) {
            throw new IndexOutOfBoundsException();
        }
        return metadataRegion.span((slotId - 1) * META_BYTES, META_BYTES);
    }

    private record SlotRef(int slotId) {
    }
}
