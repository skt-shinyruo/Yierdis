package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
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

    static final int ADDRESS_OFFSET = 0;
    static final int SIZE_OFFSET = 8;
    static final int CAPACITY_OFFSET = 12;
    static final int SEGMENT_ID_OFFSET = 16;
    static final int PAGE_CLASS_OFFSET = 20;
    static final int GENERATION_OFFSET = 24;
    static final int DOMAIN_OFFSET = 28;
    static final int KIND_OFFSET = 32;
    static final int FLAGS_OFFSET = 36;
    static final int PIN_COUNT_OFFSET = 40;
    static final int OWNER_SHARD_ID_OFFSET = 44;
    static final int ALLOC_EPOCH_OFFSET = 48;
    static final int FREE_EPOCH_OFFSET = 56;
    static final int STATE_OFFSET = 64;

    static final int INITIAL_GENERATION = 1;
    private static final int MAX_GENERATION = 0x0fff;
    private static final int STATE_COUNT = STATE_CORRUPT + 1;

    private final YierdisFfmMemoryRuntime runtime;
    private final int maxSlots;
    private final int maxSegments;
    private final int ownerShardId;
    private final long[] stateCounts = new long[STATE_COUNT];

    private YierdisNativeObjectSegment[] segments = new YierdisNativeObjectSegment[0];
    private int[] availableSegments = new int[0];
    private int activeSegments;
    private int availableSegmentCount;
    private long liveSlots;
    private long freeSlots;
    private long retiredSlots;
    private long peakLiveSlots;
    private boolean closed;

    public YierdisNativeObjectTable(YierdisFfmMemoryRuntime runtime, int maxSlots, int ownerShardId) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        this.maxSlots = maxSlots;
        this.maxSegments = (int) ((maxSlots + (long) YierdisNativeObjectSegment.SLOTS_PER_SEGMENT - 1L)
                / YierdisNativeObjectSegment.SLOTS_PER_SEGMENT);
        this.ownerShardId = ownerShardId;
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
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }

        SegmentSlot slot = allocateSlot();
        int generation = slot.segment.readInt(slot.offset, GENERATION_OFFSET);
        slot.segment.writeLong(slot.offset, ADDRESS_OFFSET, address);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, CAPACITY_OFFSET, capacity);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, 0);
        slot.segment.writeInt(slot.offset, PAGE_CLASS_OFFSET, pageClass);
        slot.segment.writeInt(slot.offset, DOMAIN_OFFSET, kind.domain().code());
        slot.segment.writeInt(slot.offset, KIND_OFFSET, kind.code());
        slot.segment.writeInt(slot.offset, FLAGS_OFFSET, 0);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, 0);
        slot.segment.writeInt(slot.offset, OWNER_SHARD_ID_OFFSET, ownerShardId);
        slot.segment.writeLong(slot.offset, ALLOC_EPOCH_OFFSET, allocEpoch);
        slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, 0L);
        transitionState(slot, STATE_ALLOCATED);
        liveSlots++;
        freeSlots--;
        peakLiveSlots = Math.max(peakLiveSlots, liveSlots);
        return NativeHandle.of(kind.domain(), kind, slot.slotId, generation, 0);
    }

    public synchronized YierdisNativeObjectMeta resolve(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        return readMeta(slot);
    }

    public synchronized YierdisNativeObjectMeta snapshot(NativeHandle handle, boolean allowQuarantined) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, allowQuarantined);
        return readMeta(slot);
    }

    public synchronized void free(NativeHandle handle, long freeEpoch) {
        free(handle, freeEpoch, false);
    }

    public synchronized void free(NativeHandle handle, long freeEpoch, boolean forceQuarantine) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (forceQuarantine || pinCount > 0) {
            slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, freeEpoch);
            transitionState(slot, STATE_FREED_QUARANTINED);
            return;
        }
        releaseSlot(slot, freeEpoch);
    }

    public synchronized void pin(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, pinCount + 1);
        transitionState(slot, STATE_PINNED);
    }

    public synchronized void unpin(NativeHandle handle) {
        unpin(handle, true);
    }

    public synchronized void unpin(NativeHandle handle, boolean releaseQuarantinedOnZero) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, true);
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (pinCount <= 0) {
            throw new NativeMemoryException("native object is not pinned");
        }
        pinCount--;
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, pinCount);
        int state = slot.segment.readInt(slot.offset, STATE_OFFSET);
        if (pinCount == 0 && state == STATE_FREED_QUARANTINED && releaseQuarantinedOnZero) {
            releaseSlot(slot, slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET));
        } else if (pinCount == 0 && state != STATE_FREED_QUARANTINED) {
            transitionState(slot, STATE_ALLOCATED);
        }
    }

    public synchronized void releaseQuarantined(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, true);
        int state = slot.segment.readInt(slot.offset, STATE_OFFSET);
        if (state != STATE_FREED_QUARANTINED) {
            throw new NativeMemoryException("native object is not quarantined");
        }
        if (slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET) != 0) {
            throw new NativeMemoryException("native object is still pinned");
        }
        releaseSlot(slot, slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET));
    }

    public synchronized void updateLocation(NativeHandle handle, int size, int capacity, long address, int pageClass) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }
        slot.segment.writeLong(slot.offset, ADDRESS_OFFSET, address);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, CAPACITY_OFFSET, capacity);
        slot.segment.writeInt(slot.offset, PAGE_CLASS_OFFSET, pageClass);
    }

    public synchronized YierdisNativeObjectMeta beginMove(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        int state = slot.segment.readInt(slot.offset, STATE_OFFSET);
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (state != STATE_ALLOCATED || pinCount != 0) {
            throw new NativeMemoryException("native object cannot move");
        }
        YierdisNativeObjectMeta meta = readMeta(slot);
        transitionState(slot, STATE_MOVING);
        return meta;
    }

    public synchronized void publishMoved(NativeHandle handle, int size, int capacity, long address, int pageClass) {
        ensureOpen();
        SegmentSlot slot = requireSlotInState(handle, STATE_MOVING);
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }
        slot.segment.writeLong(slot.offset, ADDRESS_OFFSET, address);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, CAPACITY_OFFSET, capacity);
        slot.segment.writeInt(slot.offset, PAGE_CLASS_OFFSET, pageClass);
        transitionState(slot, STATE_ALLOCATED);
    }

    public synchronized void abortMove(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireSlotInState(handle, STATE_MOVING);
        transitionState(slot, STATE_ALLOCATED);
    }

    public synchronized YierdisNativeObjectTableStats stats() {
        ensureOpen();
        return new YierdisNativeObjectTableStats(
                (long) activeSegments * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT * META_BYTES,
                activeSegments,
                liveSlots,
                freeSlots,
                retiredSlots,
                peakLiveSlots,
                stateCounts
        );
    }

    synchronized int estimateAdditionalSegments(int requestedObjects) {
        ensureOpen();
        if (requestedObjects < 0) {
            throw new IllegalArgumentException("requestedObjects must be >= 0");
        }
        long remaining = requestedObjects - Math.min((long) requestedObjects, freeSlots);
        int additionalSegments = 0;
        int segmentIndex = activeSegments;
        while (remaining > 0 && segmentIndex < maxSegments) {
            int validSlots = Math.min(
                    YierdisNativeObjectSegment.SLOTS_PER_SEGMENT,
                    maxSlots - segmentIndex * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT
            );
            remaining -= validSlots;
            additionalSegments++;
            segmentIndex++;
        }
        if (remaining > 0) {
            throw new NativeMemoryException("native object slot limit exceeded");
        }
        return additionalSegments;
    }

    synchronized long heapEstimatedBytes() {
        long bytes = 192L;
        bytes += 16L + (long) segments.length * 8L;
        bytes += 16L + (long) availableSegments.length * Integer.BYTES;
        bytes += 16L + (long) stateCounts.length * Long.BYTES;
        for (int i = 0; i < activeSegments; i++) {
            bytes += objectSegmentHeapBytes();
        }
        return bytes;
    }

    synchronized long estimateAdditionalHeapBytes(int requestedObjects) {
        int additionalSegments = estimateAdditionalSegments(requestedObjects);
        long bytes = (long) additionalSegments * objectSegmentHeapBytes();
        int requiredSegments = activeSegments + additionalSegments;
        int segmentCapacity = estimatedGrownCapacity(segments.length, requiredSegments);
        int availableCapacity = estimatedGrownCapacity(availableSegments.length, requiredSegments);
        bytes += (long) (segmentCapacity - segments.length) * 8L;
        bytes += (long) (availableCapacity - availableSegments.length) * Integer.BYTES;
        return bytes;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (int i = 0; i < activeSegments; i++) {
            segments[i].close();
        }
        segments = new YierdisNativeObjectSegment[0];
        availableSegments = new int[0];
        activeSegments = 0;
        availableSegmentCount = 0;
    }

    private SegmentSlot allocateSlot() {
        while (availableSegmentCount > 0) {
            int segmentIndex = availableSegments[--availableSegmentCount];
            YierdisNativeObjectSegment segment = segments[segmentIndex];
            segment.availableQueued(false);
            int offset = segment.allocateOffset();
            if (segment.hasFreeSlot()) {
                enqueueAvailable(segmentIndex, segment);
            }
            if (offset >= 0) {
                return new SegmentSlot(slotId(segmentIndex, offset), segmentIndex, offset, segment);
            }
        }
        if (activeSegments >= maxSegments) {
            throw new NativeMemoryException("native object slot limit exceeded");
        }
        int segmentIndex = activeSegments;
        int validSlots = Math.min(
                YierdisNativeObjectSegment.SLOTS_PER_SEGMENT,
                maxSlots - segmentIndex * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT
        );
        ensureSegmentCapacity(activeSegments + 1);
        YierdisNativeObjectSegment segment = new YierdisNativeObjectSegment(
                runtime,
                segmentIndex,
                validSlots,
                ownerShardId
        );
        segments[segmentIndex] = segment;
        activeSegments++;
        freeSlots += validSlots;
        stateCounts[STATE_FREE] += validSlots;
        int offset = segment.allocateOffset();
        if (segment.hasFreeSlot()) {
            enqueueAvailable(segmentIndex, segment);
        }
        return new SegmentSlot(slotId(segmentIndex, offset), segmentIndex, offset, segment);
    }

    private SegmentSlot requireLiveSlot(NativeHandle handle, boolean allowQuarantined) {
        if (handle == null) {
            return stale("stale native handle: null");
        }
        long slotId = handle.slotId();
        if (slotId <= 0 || slotId > maxSlots) {
            return stale("stale native handle: unknown slot " + slotId);
        }
        SegmentSlot slot = slotRef((int) slotId);
        if (slot.segmentIndex >= activeSegments) {
            return stale("stale native handle: unknown slot " + slotId);
        }
        int state = slot.segment.readInt(slot.offset, STATE_OFFSET);
        int generation = slot.segment.readInt(slot.offset, GENERATION_OFFSET);
        if (state == STATE_FREE || slot.segment.isRetired(slot.offset) || generation != handle.generation()) {
            return stale("stale native handle: slot=" + slotId + " generation=" + handle.generation());
        }
        if (state == STATE_FREED_QUARANTINED && !allowQuarantined) {
            return stale("stale native handle: quarantined slot=" + slotId);
        }
        NativeHandleDomain domain = NativeHandleDomain.fromCode(slot.segment.readInt(slot.offset, DOMAIN_OFFSET));
        int kindCode = slot.segment.readInt(slot.offset, KIND_OFFSET);
        if (domain != handle.domain() || kindCode != handle.kindCode()) {
            throw new NativeMemoryException("native handle kind/domain mismatch: " + handle.raw());
        }
        return slot;
    }

    private SegmentSlot requireSlotInState(NativeHandle handle, int expectedState) {
        SegmentSlot slot = requireLiveSlot(handle, false);
        int state = slot.segment.readInt(slot.offset, STATE_OFFSET);
        if (state != expectedState) {
            throw new NativeMemoryException("native object state mismatch: expected " + expectedState + " but was " + state);
        }
        return slot;
    }

    private void releaseSlot(SegmentSlot slot, long freeEpoch) {
        int generation = slot.segment.readInt(slot.offset, GENERATION_OFFSET);
        slot.segment.writeLong(slot.offset, ADDRESS_OFFSET, 0L);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, 0);
        slot.segment.writeInt(slot.offset, CAPACITY_OFFSET, 0);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, 0);
        slot.segment.writeInt(slot.offset, PAGE_CLASS_OFFSET, 0);
        slot.segment.writeInt(slot.offset, DOMAIN_OFFSET, 0);
        slot.segment.writeInt(slot.offset, KIND_OFFSET, 0);
        slot.segment.writeInt(slot.offset, FLAGS_OFFSET, 0);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, 0);
        slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, freeEpoch);
        transitionState(slot, STATE_FREE);
        liveSlots--;
        if (generation >= MAX_GENERATION) {
            slot.segment.retire(slot.offset);
            retiredSlots++;
            return;
        }
        slot.segment.writeInt(slot.offset, GENERATION_OFFSET, generation + 1);
        boolean wasFull = !slot.segment.hasFreeSlot();
        slot.segment.releaseOffset(slot.offset);
        freeSlots++;
        if (wasFull) {
            enqueueAvailable(slot.segmentIndex, slot.segment);
        }
    }

    private YierdisNativeObjectMeta readMeta(SegmentSlot slot) {
        return new YierdisNativeObjectMeta(
                slot.slotId,
                slot.segment.readLong(slot.offset, ADDRESS_OFFSET),
                slot.segment.readInt(slot.offset, SIZE_OFFSET),
                slot.segment.readInt(slot.offset, CAPACITY_OFFSET),
                slot.segment.readInt(slot.offset, SEGMENT_ID_OFFSET),
                slot.segment.readInt(slot.offset, PAGE_CLASS_OFFSET),
                slot.segment.readInt(slot.offset, GENERATION_OFFSET),
                NativeHandleDomain.fromCode(slot.segment.readInt(slot.offset, DOMAIN_OFFSET)),
                slot.segment.readInt(slot.offset, KIND_OFFSET),
                slot.segment.readInt(slot.offset, FLAGS_OFFSET),
                slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET),
                slot.segment.readInt(slot.offset, OWNER_SHARD_ID_OFFSET),
                slot.segment.readLong(slot.offset, ALLOC_EPOCH_OFFSET),
                slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET),
                slot.segment.readInt(slot.offset, STATE_OFFSET)
        );
    }

    private void transitionState(SegmentSlot slot, int newState) {
        int oldState = slot.segment.readInt(slot.offset, STATE_OFFSET);
        if (oldState == newState) {
            return;
        }
        stateCounts[oldState]--;
        stateCounts[newState]++;
        slot.segment.writeInt(slot.offset, STATE_OFFSET, newState);
    }

    private void enqueueAvailable(int segmentIndex, YierdisNativeObjectSegment segment) {
        if (segment.availableQueued() || !segment.hasFreeSlot()) {
            return;
        }
        ensureAvailableCapacity(availableSegmentCount + 1);
        availableSegments[availableSegmentCount++] = segmentIndex;
        segment.availableQueued(true);
    }

    private void ensureSegmentCapacity(int required) {
        if (required <= segments.length) {
            return;
        }
        int capacity = Math.min(maxSegments, growCapacity(segments.length, required));
        segments = Arrays.copyOf(segments, capacity);
    }

    private void ensureAvailableCapacity(int required) {
        if (required <= availableSegments.length) {
            return;
        }
        int capacity = Math.min(maxSegments, growCapacity(availableSegments.length, required));
        availableSegments = Arrays.copyOf(availableSegments, capacity);
    }

    private static int growCapacity(int current, int required) {
        int grown = current == 0 ? 1 : current + Math.max(1, current >>> 1);
        return Math.max(required, grown);
    }

    private int estimatedGrownCapacity(int current, int required) {
        int capacity = current;
        while (capacity < required) {
            capacity = Math.min(maxSegments, growCapacity(capacity, capacity + 1));
        }
        return capacity;
    }

    private static long objectSegmentHeapBytes() {
        return 160L
                + 16L + (long) YierdisNativeObjectSegment.SLOTS_PER_SEGMENT * Integer.BYTES
                + 16L + (long) (YierdisNativeObjectSegment.SLOTS_PER_SEGMENT / Long.SIZE) * Long.BYTES;
    }

    private SegmentSlot slotRef(int slotId) {
        int zeroBased = Math.toIntExact(slotId - 1L);
        int segmentIndex = zeroBased >>> 12;
        int segmentOffset = zeroBased & 0x0fff;
        YierdisNativeObjectSegment segment = segmentIndex < activeSegments ? segments[segmentIndex] : null;
        return new SegmentSlot(slotId, segmentIndex, segmentOffset, segment);
    }

    private static int slotId(int segmentIndex, int segmentOffset) {
        return segmentIndex * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT + segmentOffset + 1;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native object table is closed");
        }
    }

    private SegmentSlot stale(String message) {
        throw new StaleNativeHandleException(message);
    }

    private record SegmentSlot(
            int slotId,
            int segmentIndex,
            int offset,
            YierdisNativeObjectSegment segment
    ) {
    }
}
