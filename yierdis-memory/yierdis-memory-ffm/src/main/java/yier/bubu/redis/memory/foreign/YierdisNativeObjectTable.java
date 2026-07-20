package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisNativeObjectTable implements AutoCloseable {
    static final int AUTOMATIC_MAX_SLOTS = Integer.MAX_VALUE;

    public static final int STATE_FREE = 0;
    public static final int STATE_ALLOCATED = 1;
    public static final int STATE_PINNED = 2;
    public static final int STATE_MOVING = 3;
    public static final int STATE_FREED_QUARANTINED = 4;
    public static final int STATE_CORRUPT = 5;

    public static final int META_BYTES = 36;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long CHECKPOINT_OBJECT_BYTES = 40L;
    private static final long INT_BYTES = Integer.BYTES;
    private static final long REFERENCE_BYTES = 8L;

    static final int ADDRESS_OFFSET = 0;
    static final int SIZE_OFFSET = 4;
    static final int SEGMENT_ID_OFFSET = 8;
    static final int PACKED_METADATA_OFFSET = 12;
    static final int ALLOC_EPOCH_OFFSET = 16;
    static final int FREE_EPOCH_OFFSET = 24;
    static final int PIN_COUNT_OFFSET = 32;

    private static final int STATE_SHIFT = 0;
    private static final int STATE_MASK = 0x07;
    private static final int PAGE_CLASS_SHIFT = 3;
    private static final int PAGE_CLASS_MASK = 0x07;
    private static final int FLAGS_SHIFT = 6;
    private static final int FLAGS_MASK = 0x0f;
    private static final int KIND_SHIFT = 10;
    private static final int KIND_MASK = 0x0f;
    private static final int DOMAIN_SHIFT = 14;
    private static final int DOMAIN_MASK = 0x0f;
    private static final int GENERATION_SHIFT = 18;
    private static final int GENERATION_MASK = 0x0fff;

    static final int INITIAL_GENERATION = 1;
    private static final int MAX_GENERATION = 0x0fff;
    private static final int STATE_COUNT = STATE_CORRUPT + 1;

    private final YierdisFfmMemoryRuntime runtime;
    private final int maxSlots;
    private final int maxSegments;
    private final int ownerShardId;
    private final CapacityResolver capacityResolver;
    private final long[] stateCounts = new long[STATE_COUNT];

    private YierdisNativeObjectSegment[] segments = new YierdisNativeObjectSegment[0];
    private int[] availableSegments = new int[0];
    private int activeSegments;
    private int availableSegmentCount;
    private long liveSlots;
    private long freeSlots;
    private long retiredSlots;
    private long peakLiveSlots;
    private long retainedHeapBytes;
    private AllocationScopeCheckpoint activeAllocationScope;
    private boolean allocationScopeAbortInProgress;
    private boolean closed;
    private boolean heapIterationTrapForTesting;

    public YierdisNativeObjectTable(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            int ownerShardId,
            CapacityResolver capacityResolver
    ) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (maxSlots < 0) {
            throw new IllegalArgumentException("maxSlots must be >= 0");
        }
        this.maxSlots = maxSlots == 0 ? AUTOMATIC_MAX_SLOTS : maxSlots;
        this.maxSegments = (int) ((this.maxSlots + (long) YierdisNativeObjectSegment.SLOTS_PER_SEGMENT - 1L)
                / YierdisNativeObjectSegment.SLOTS_PER_SEGMENT);
        this.ownerShardId = ownerShardId;
        this.capacityResolver = Objects.requireNonNull(capacityResolver, "capacityResolver");
        this.retainedHeapBytes = baseHeapBytes();
    }

    public NativeHandle allocate(
            NativeObjectKind kind,
            int size,
            int capacity,
            int segmentId,
            long address,
            int pageClass,
            long allocEpoch
    ) {
        return NativeHandle.fromRaw(allocateRaw(kind, size, capacity, segmentId, address, pageClass, allocEpoch));
    }

    public long allocateRaw(
            NativeObjectKind kind,
            int size,
            int capacity,
            int segmentId,
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
        int pageOffset = checkedPageOffset(address);
        requirePackedField("pageClass", pageClass, PAGE_CLASS_MASK);
        validateResolvedCapacity(size, capacity, segmentId, pageOffset, pageClass);
        int packedFields = packMetadata(
                INITIAL_GENERATION,
                kind.domain().code(),
                kind.code(),
                0,
                pageClass,
                STATE_FREE
        );

        SegmentSlot slot = allocateSlot();
        int generation = generation(slot);
        slot.segment.writeInt(slot.offset, ADDRESS_OFFSET, pageOffset);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, segmentId);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, 0);
        slot.segment.writeLong(slot.offset, ALLOC_EPOCH_OFFSET, allocEpoch);
        slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, 0L);
        slot.segment.writeInt(
                slot.offset,
                PACKED_METADATA_OFFSET,
                replaceGeneration(packedFields, generation)
        );
        transitionState(slot, STATE_ALLOCATED);
        liveSlots++;
        freeSlots--;
        peakLiveSlots = Math.max(peakLiveSlots, liveSlots);
        return NativeHandle.rawOf(kind.domain(), kind, slot.slotId, generation, 0);
    }

    public YierdisNativeObjectMeta resolve(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        return readMeta(slot);
    }

    public YierdisNativeObjectMeta resolveRaw(long rawHandle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, false);
        return readMeta(slot);
    }

    public YierdisNativeObjectMeta snapshot(NativeHandle handle, boolean allowQuarantined) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, allowQuarantined);
        return readMeta(slot);
    }

    public YierdisNativeObjectMeta snapshotRaw(long rawHandle, boolean allowQuarantined) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, allowQuarantined);
        return readMeta(slot);
    }

    ResolvedSlot resolvedSlotRaw(long rawHandle, boolean allowQuarantined) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, allowQuarantined);
        return new ResolvedSlot(slot.segment, slot.offset);
    }

    void validateResolvedSlot(ResolvedSlot slot, boolean allowQuarantined) {
        ensureOpen();
        Objects.requireNonNull(slot, "slot");
        int state = state(slot.segment, slot.offset);
        if (state == STATE_FREE || slot.segment.isRetired(slot.offset)) {
            stale("stale native handle: resolved slot was released");
        }
        if (state == STATE_FREED_QUARANTINED && !allowQuarantined) {
            stale("stale native handle: quarantined resolved slot");
        }
        if (slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET) <= 0) {
            throw new NativeMemoryException("resolved native view lost its pin");
        }
    }

    public void free(NativeHandle handle, long freeEpoch) {
        free(handle, freeEpoch, false);
    }

    public void free(NativeHandle handle, long freeEpoch, boolean forceQuarantine) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        freeSlot(slot, freeEpoch, forceQuarantine);
    }

    public void freeRaw(long rawHandle, long freeEpoch, boolean forceQuarantine) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, false);
        freeSlot(slot, freeEpoch, forceQuarantine);
    }

    private void freeSlot(SegmentSlot slot, long freeEpoch, boolean forceQuarantine) {
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (forceQuarantine || pinCount > 0) {
            slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, freeEpoch);
            transitionState(slot, STATE_FREED_QUARANTINED);
            return;
        }
        releaseSlot(slot, freeEpoch);
    }

    public void pin(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        pinSlot(slot);
    }

    public void pinRaw(long rawHandle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, false);
        pinSlot(slot);
    }

    private void pinSlot(SegmentSlot slot) {
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, pinCount + 1);
        transitionState(slot, STATE_PINNED);
    }

    public void unpin(NativeHandle handle) {
        unpin(handle, true);
    }

    public void unpin(NativeHandle handle, boolean releaseQuarantinedOnZero) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, true);
        unpinSlot(slot, releaseQuarantinedOnZero);
    }

    public void unpinRaw(long rawHandle, boolean releaseQuarantinedOnZero) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, true);
        unpinSlot(slot, releaseQuarantinedOnZero);
    }

    private void unpinSlot(SegmentSlot slot, boolean releaseQuarantinedOnZero) {
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (pinCount <= 0) {
            throw new NativeMemoryException("native object is not pinned");
        }
        pinCount--;
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, pinCount);
        int state = state(slot);
        if (pinCount == 0 && state == STATE_FREED_QUARANTINED && releaseQuarantinedOnZero) {
            releaseSlot(slot, slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET));
        } else if (pinCount == 0 && state != STATE_FREED_QUARANTINED) {
            transitionState(slot, STATE_ALLOCATED);
        }
    }

    public void releaseQuarantined(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, true);
        int state = state(slot);
        if (state != STATE_FREED_QUARANTINED) {
            throw new NativeMemoryException("native object is not quarantined");
        }
        if (slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET) != 0) {
            throw new NativeMemoryException("native object is still pinned");
        }
        releaseSlot(slot, slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET));
    }

    public void updateLocation(
            NativeHandle handle,
            int size,
            int capacity,
            int segmentId,
            long address,
            int pageClass
    ) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        updateLocation(slot, size, capacity, segmentId, address, pageClass);
    }

    void updateLocationRaw(
            long rawHandle,
            int size,
            int capacity,
            int segmentId,
            long address,
            int pageClass
    ) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(rawHandle, false);
        updateLocation(slot, size, capacity, segmentId, address, pageClass);
    }

    private void updateLocation(
            SegmentSlot slot,
            int size,
            int capacity,
            int segmentId,
            long address,
            int pageClass
    ) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }
        int pageOffset = checkedPageOffset(address);
        requirePackedField("pageClass", pageClass, PAGE_CLASS_MASK);
        validateResolvedCapacity(size, capacity, segmentId, pageOffset, pageClass);
        slot.segment.writeInt(slot.offset, ADDRESS_OFFSET, pageOffset);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, segmentId);
        replacePageClass(slot, pageClass);
    }

    public YierdisNativeObjectMeta beginMove(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireLiveSlot(handle, false);
        int state = state(slot);
        int pinCount = slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET);
        if (state != STATE_ALLOCATED || pinCount != 0) {
            throw new NativeMemoryException("native object cannot move");
        }
        YierdisNativeObjectMeta meta = readMeta(slot);
        transitionState(slot, STATE_MOVING);
        return meta;
    }

    public void publishMoved(
            NativeHandle handle,
            int size,
            int capacity,
            int segmentId,
            long address,
            int pageClass
    ) {
        ensureOpen();
        SegmentSlot slot = requireSlotInState(handle, STATE_MOVING);
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        if (capacity < size) {
            throw new IllegalArgumentException("capacity must be >= size");
        }
        int pageOffset = checkedPageOffset(address);
        requirePackedField("pageClass", pageClass, PAGE_CLASS_MASK);
        validateResolvedCapacity(size, capacity, segmentId, pageOffset, pageClass);
        slot.segment.writeInt(slot.offset, ADDRESS_OFFSET, pageOffset);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, size);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, segmentId);
        replacePageClass(slot, pageClass);
        transitionState(slot, STATE_ALLOCATED);
    }

    public void abortMove(NativeHandle handle) {
        ensureOpen();
        SegmentSlot slot = requireSlotInState(handle, STATE_MOVING);
        transitionState(slot, STATE_ALLOCATED);
    }

    public YierdisNativeObjectTableStats stats() {
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

    int estimateAdditionalSegments(int requestedObjects) {
        ensureOpen();
        if (requestedObjects < 0) {
            throw new IllegalArgumentException("requestedObjects must be >= 0");
        }
        long remaining = requestedObjects - Math.min((long) requestedObjects, freeSlots);
        int additionalSegments = 0;
        int segmentIndex = activeSegments;
        while (remaining > 0 && segmentIndex < maxSegments) {
            int validSlots = validSlotsInSegment(maxSlots, segmentIndex);
            remaining -= validSlots;
            additionalSegments++;
            segmentIndex++;
        }
        if (remaining > 0) {
            throw new NativeCapacityExceededException("native object slot limit exceeded");
        }
        return additionalSegments;
    }

    long heapEstimatedBytes() {
        return retainedHeapBytes;
    }

    void armHeapIterationTrapForTesting() {
        heapIterationTrapForTesting = true;
    }

    void disarmHeapIterationTrapForTesting() {
        heapIterationTrapForTesting = false;
    }

    long estimateAdditionalHeapBytes(int requestedObjects) {
        int additionalSegments = estimateAdditionalSegments(requestedObjects);
        long bytes = (long) additionalSegments * objectSegmentHeapBytes();
        int requiredSegments = activeSegments + additionalSegments;
        int segmentCapacity = estimatedGrownCapacity(segments.length, requiredSegments);
        int availableCapacity = estimatedGrownCapacity(availableSegments.length, requiredSegments);
        bytes += (long) (segmentCapacity - segments.length) * 8L;
        bytes += (long) (availableCapacity - availableSegments.length) * Integer.BYTES;
        return bytes;
    }

    AllocationScopeCheckpoint allocationScopeCheckpoint() {
        ensureOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native object-table allocation scope is already active");
        }
        AllocationScopeCheckpoint checkpoint = new AllocationScopeCheckpoint(
                activeSegments,
                segments,
                availableSegments,
                retainedHeapBytes
        );
        activeAllocationScope = checkpoint;
        return checkpoint;
    }

    long allocationScopeCheckpointHeapEstimatedBytes() {
        ensureOpen();
        return allocationScopeCheckpointHeapEstimatedBytes(segments.length, availableSegments.length);
    }

    void promoteAllocationScope(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope == checkpoint) {
            activeAllocationScope = null;
            allocationScopeAbortInProgress = false;
            checkpoint.releaseReferences();
        }
    }

    void beginAllocationScopeAbort(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native object-table allocation scope is not active");
        }
        allocationScopeAbortInProgress = true;
    }

    void restoreAllocationScopeCheckpoint(AllocationScopeCheckpoint checkpoint) {
        ensureOpen();
        Objects.requireNonNull(checkpoint, "checkpoint");
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native object-table allocation scope is not active");
        }
        try {
            if (checkpoint.activeSegments > activeSegments) {
                throw new IllegalStateException("allocation scope checkpoint is ahead of the object table");
            }
            for (int segmentIndex = activeSegments - 1; segmentIndex >= checkpoint.activeSegments; segmentIndex--) {
                YierdisNativeObjectSegment segment = segments[segmentIndex];
                int reusableSlots = 0;
                int retired = 0;
                for (int offset = 0; offset < segment.validSlots(); offset++) {
                    int state = state(segment, offset);
                    if (state != STATE_FREE) {
                        throw new IllegalStateException("allocation scope left a live native object");
                    }
                    if (segment.isRetired(offset)) {
                        retired++;
                    } else {
                        reusableSlots++;
                    }
                }
                freeSlots -= reusableSlots;
                retiredSlots -= retired;
                stateCounts[STATE_FREE] -= segment.validSlots();
                segment.close();
                segments[segmentIndex] = null;
            }
            activeSegments = checkpoint.activeSegments;
            segments = checkpoint.segments;
            availableSegments = checkpoint.availableSegments;
            retainedHeapBytes = checkpoint.retainedHeapBytes;
            availableSegmentCount = 0;
            for (int segmentIndex = 0; segmentIndex < activeSegments; segmentIndex++) {
                YierdisNativeObjectSegment segment = segments[segmentIndex];
                segment.availableQueued(false);
                if (segment.hasFreeSlot()) {
                    enqueueAvailable(segmentIndex, segment);
                }
            }
        } finally {
            allocationScopeAbortInProgress = false;
            activeAllocationScope = null;
            checkpoint.releaseReferences();
        }
    }

    int firstOccupiedSlot() {
        return nextOccupiedSlot(0);
    }

    int nextOccupiedSlot(int afterSlotId) {
        ensureOpen();
        int last = materializedSlotUpperBound(maxSlots, activeSegments);
        if (afterSlotId >= last) {
            return 0;
        }
        int first = Math.max(1, afterSlotId + 1);
        for (long candidate = first; candidate <= last; candidate++) {
            int slotId = (int) candidate;
            SegmentSlot slot = slotRef(slotId);
            int state = state(slot);
            if (state != STATE_FREE && !slot.segment.isRetired(slot.offset)) {
                return slotId;
            }
        }
        return 0;
    }

    YierdisNativeObjectMeta occupiedMeta(int slotId) {
        ensureOpen();
        if (slotId <= 0 || slotId > maxSlots) {
            return null;
        }
        SegmentSlot slot = slotRef(slotId);
        if (slot.segmentIndex >= activeSegments) {
            return null;
        }
        int state = state(slot);
        if (state == STATE_FREE || slot.segment.isRetired(slot.offset)) {
            return null;
        }
        return readMeta(slot);
    }

    @Override
    public void close() {
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
        retainedHeapBytes = baseHeapBytes();
        activeAllocationScope = null;
        allocationScopeAbortInProgress = false;
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
            throw new NativeCapacityExceededException("native object slot limit exceeded");
        }
        int segmentIndex = activeSegments;
        int validSlots = validSlotsInSegment(maxSlots, segmentIndex);
        ensureSegmentCapacity(activeSegments + 1);
        YierdisNativeObjectSegment segment = new YierdisNativeObjectSegment(
                runtime,
                segmentIndex,
                validSlots
        );
        detachSegmentsForActiveAllocationScope();
        segments[segmentIndex] = segment;
        activeSegments++;
        retainedHeapBytes = MemoryUsageSnapshot.addSaturating(retainedHeapBytes, objectSegmentHeapBytes());
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
        return requireLiveSlot(handle.raw(), allowQuarantined);
    }

    private SegmentSlot requireLiveSlot(long rawHandle, boolean allowQuarantined) {
        NativeHandle.requireValidRaw(rawHandle);
        long slotId = NativeHandle.slotId(rawHandle);
        if (slotId <= 0 || slotId > maxSlots) {
            return stale("stale native handle: unknown slot " + slotId);
        }
        SegmentSlot slot = slotRef((int) slotId);
        if (slot.segmentIndex >= activeSegments) {
            return stale("stale native handle: unknown slot " + slotId);
        }
        int packedMetadata = packedMetadata(slot);
        int state = unpack(packedMetadata, STATE_SHIFT, STATE_MASK);
        int generation = unpack(packedMetadata, GENERATION_SHIFT, GENERATION_MASK);
        int handleGeneration = NativeHandle.generation(rawHandle);
        if (state == STATE_FREE || slot.segment.isRetired(slot.offset) || generation != handleGeneration) {
            return stale("stale native handle: slot=" + slotId + " generation=" + handleGeneration);
        }
        if (state == STATE_FREED_QUARANTINED && !allowQuarantined) {
            return stale("stale native handle: quarantined slot=" + slotId);
        }
        int domainCode = unpack(packedMetadata, DOMAIN_SHIFT, DOMAIN_MASK);
        int kindCode = unpack(packedMetadata, KIND_SHIFT, KIND_MASK);
        if (domainCode != NativeHandle.domainCode(rawHandle) || kindCode != NativeHandle.kindCode(rawHandle)) {
            throw new NativeMemoryException("native handle kind/domain mismatch: " + rawHandle);
        }
        return slot;
    }

    private SegmentSlot requireSlotInState(NativeHandle handle, int expectedState) {
        SegmentSlot slot = requireLiveSlot(handle, false);
        int state = state(slot);
        if (state != expectedState) {
            throw new NativeMemoryException("native object state mismatch: expected " + expectedState + " but was " + state);
        }
        return slot;
    }

    private void releaseSlot(SegmentSlot slot, long freeEpoch) {
        int generation = generation(slot);
        slot.segment.writeInt(slot.offset, ADDRESS_OFFSET, 0);
        slot.segment.writeInt(slot.offset, SIZE_OFFSET, 0);
        slot.segment.writeInt(slot.offset, SEGMENT_ID_OFFSET, 0);
        slot.segment.writeInt(slot.offset, PIN_COUNT_OFFSET, 0);
        slot.segment.writeLong(slot.offset, FREE_EPOCH_OFFSET, freeEpoch);
        transitionState(slot, STATE_FREE);
        slot.segment.writeInt(
                slot.offset,
                PACKED_METADATA_OFFSET,
                packMetadata(generation, 0, 0, 0, 0, STATE_FREE)
        );
        liveSlots--;
        if (generation >= MAX_GENERATION) {
            slot.segment.retire(slot.offset);
            retiredSlots++;
            return;
        }
        slot.segment.writeInt(
                slot.offset,
                PACKED_METADATA_OFFSET,
                packMetadata(generation + 1, 0, 0, 0, 0, STATE_FREE)
        );
        boolean wasFull = !slot.segment.hasFreeSlot();
        slot.segment.releaseOffset(slot.offset);
        freeSlots++;
        if (wasFull && !allocationScopeAbortInProgress) {
            enqueueAvailable(slot.segmentIndex, slot.segment);
        }
    }

    private YierdisNativeObjectMeta readMeta(SegmentSlot slot) {
        int packedMetadata = packedMetadata(slot);
        int pageOffset = slot.segment.readInt(slot.offset, ADDRESS_OFFSET);
        int size = slot.segment.readInt(slot.offset, SIZE_OFFSET);
        int segmentId = slot.segment.readInt(slot.offset, SEGMENT_ID_OFFSET);
        int pageClass = unpack(packedMetadata, PAGE_CLASS_SHIFT, PAGE_CLASS_MASK);
        // capacity 的权威来源是 page/span descriptor；slot 只保存定位信息，避免为每个 tiny object 重复 4 bytes。
        int capacity = capacityResolver.resolveCapacity(segmentId, pageOffset, pageClass);
        if (capacity <= 0 || capacity < size) {
            throw new NativeMemoryException("native page capacity is inconsistent with object metadata");
        }
        return new YierdisNativeObjectMeta(
                slot.slotId,
                pageOffset,
                size,
                capacity,
                segmentId,
                pageClass,
                unpack(packedMetadata, GENERATION_SHIFT, GENERATION_MASK),
                NativeHandleDomain.fromCode(unpack(packedMetadata, DOMAIN_SHIFT, DOMAIN_MASK)),
                unpack(packedMetadata, KIND_SHIFT, KIND_MASK),
                unpack(packedMetadata, FLAGS_SHIFT, FLAGS_MASK),
                slot.segment.readInt(slot.offset, PIN_COUNT_OFFSET),
                ownerShardId,
                slot.segment.readLong(slot.offset, ALLOC_EPOCH_OFFSET),
                slot.segment.readLong(slot.offset, FREE_EPOCH_OFFSET),
                unpack(packedMetadata, STATE_SHIFT, STATE_MASK)
        );
    }

    private void transitionState(SegmentSlot slot, int newState) {
        requirePackedField("state", newState, STATE_MASK);
        int packedMetadata = packedMetadata(slot);
        int oldState = unpack(packedMetadata, STATE_SHIFT, STATE_MASK);
        if (oldState == newState) {
            return;
        }
        stateCounts[oldState]--;
        stateCounts[newState]++;
        slot.segment.writeInt(
                slot.offset,
                PACKED_METADATA_OFFSET,
                replacePackedField(packedMetadata, STATE_SHIFT, STATE_MASK, newState)
        );
    }

    // generation、句柄类型、页类型和状态共享一个 word，避免 tiny object 为低基数字段支付整型槽位开销。
    static int initialPackedMetadata() {
        return packMetadata(INITIAL_GENERATION, 0, 0, 0, 0, STATE_FREE);
    }

    private static int packMetadata(
            int generation,
            int domainCode,
            int kindCode,
            int flags,
            int pageClass,
            int state
    ) {
        requirePackedField("generation", generation, GENERATION_MASK);
        requirePackedField("domainCode", domainCode, DOMAIN_MASK);
        requirePackedField("kindCode", kindCode, KIND_MASK);
        requirePackedField("flags", flags, FLAGS_MASK);
        requirePackedField("pageClass", pageClass, PAGE_CLASS_MASK);
        requirePackedField("state", state, STATE_MASK);
        return generation << GENERATION_SHIFT
                | domainCode << DOMAIN_SHIFT
                | kindCode << KIND_SHIFT
                | flags << FLAGS_SHIFT
                | pageClass << PAGE_CLASS_SHIFT
                | state << STATE_SHIFT;
    }

    private static int packedMetadata(SegmentSlot slot) {
        return slot.segment.readInt(slot.offset, PACKED_METADATA_OFFSET);
    }

    private static int state(SegmentSlot slot) {
        return state(slot.segment, slot.offset);
    }

    private static int state(YierdisNativeObjectSegment segment, int offset) {
        return unpack(segment.readInt(offset, PACKED_METADATA_OFFSET), STATE_SHIFT, STATE_MASK);
    }

    private static int generation(SegmentSlot slot) {
        return unpack(packedMetadata(slot), GENERATION_SHIFT, GENERATION_MASK);
    }

    private static int replaceGeneration(int packedMetadata, int generation) {
        return replacePackedField(packedMetadata, GENERATION_SHIFT, GENERATION_MASK, generation);
    }

    private static void replacePageClass(SegmentSlot slot, int pageClass) {
        int packedMetadata = packedMetadata(slot);
        slot.segment.writeInt(
                slot.offset,
                PACKED_METADATA_OFFSET,
                replacePackedField(packedMetadata, PAGE_CLASS_SHIFT, PAGE_CLASS_MASK, pageClass)
        );
    }

    private static int replacePackedField(int packedMetadata, int shift, int mask, int value) {
        requirePackedField("packed metadata field", value, mask);
        return (packedMetadata & ~(mask << shift)) | value << shift;
    }

    private static int unpack(int packedMetadata, int shift, int mask) {
        return packedMetadata >>> shift & mask;
    }

    private static void requirePackedField(String name, int value, int mask) {
        if (value < 0 || value > mask) {
            throw new IllegalArgumentException(name + " out of range: " + value);
        }
    }

    private static int checkedPageOffset(long address) {
        int pageOffset = Math.toIntExact(address);
        if (pageOffset < 0) {
            throw new IllegalArgumentException("address must be >= 0");
        }
        return pageOffset;
    }

    private void validateResolvedCapacity(
            int size,
            int declaredCapacity,
            int segmentId,
            int pageOffset,
            int pageClass
    ) {
        int resolvedCapacity = capacityResolver.resolveCapacity(segmentId, pageOffset, pageClass);
        if (resolvedCapacity <= 0) {
            throw new IllegalArgumentException("resolved capacity must be > 0");
        }
        if (resolvedCapacity != declaredCapacity) {
            throw new IllegalArgumentException(
                    "capacity does not match native page descriptor: declared="
                            + declaredCapacity + " resolved=" + resolvedCapacity
            );
        }
        if (resolvedCapacity < size) {
            throw new IllegalArgumentException("resolved capacity must be >= size");
        }
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
        failIfAllocationScopeAbortWouldAllocate();
        retainSegmentsForActiveAllocationScope();
        long previousHeapBytes = arrayHeapBytes(segments.length, REFERENCE_BYTES);
        int capacity = Math.min(maxSegments, growCapacity(segments.length, required));
        segments = Arrays.copyOf(segments, capacity);
        replaceRetainedHeapBytes(previousHeapBytes, arrayHeapBytes(segments.length, REFERENCE_BYTES));
    }

    private void ensureAvailableCapacity(int required) {
        if (required <= availableSegments.length) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        retainAvailableSegmentsForActiveAllocationScope();
        long previousHeapBytes = arrayHeapBytes(availableSegments.length, INT_BYTES);
        int capacity = Math.min(maxSegments, growCapacity(availableSegments.length, required));
        availableSegments = Arrays.copyOf(availableSegments, capacity);
        replaceRetainedHeapBytes(previousHeapBytes, arrayHeapBytes(availableSegments.length, INT_BYTES));
    }

    private void detachSegmentsForActiveAllocationScope() {
        if (activeAllocationScope == null || segments != activeAllocationScope.segments) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        segments = segments.clone();
        activeAllocationScope.retainSegments();
    }

    private void retainSegmentsForActiveAllocationScope() {
        if (activeAllocationScope != null && segments == activeAllocationScope.segments) {
            activeAllocationScope.retainSegments();
        }
    }

    private void retainAvailableSegmentsForActiveAllocationScope() {
        if (activeAllocationScope != null && availableSegments == activeAllocationScope.availableSegments) {
            activeAllocationScope.retainAvailableSegments();
        }
    }

    private void failIfAllocationScopeAbortWouldAllocate() {
        if (allocationScopeAbortInProgress) {
            throw new IllegalStateException("native object-table allocation scope abort must not allocate");
        }
    }

    private static int growCapacity(int current, int required) {
        int grown = current == 0 ? 1 : current + Math.max(1, current >>> 1);
        return Math.max(required, grown);
    }

    private static long arrayHeapBytes(int length, long elementBytes) {
        return ARRAY_HEADER_BYTES + (long) length * elementBytes;
    }

    private static long allocationScopeCheckpointHeapEstimatedBytes(int segmentsLength, int availableSegmentsLength) {
        return CHECKPOINT_OBJECT_BYTES
                + arrayHeapBytes(segmentsLength, REFERENCE_BYTES)
                + arrayHeapBytes(availableSegmentsLength, INT_BYTES);
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

    static int materializedSlotUpperBound(int maxSlots, int activeSegments) {
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        if (activeSegments < 0) {
            throw new IllegalArgumentException("activeSegments must be >= 0");
        }
        long materialized = (long) activeSegments * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT;
        return (int) Math.min((long) maxSlots, materialized);
    }

    static int validSlotsInSegment(int maxSlots, int segmentIndex) {
        if (maxSlots <= 0) {
            throw new IllegalArgumentException("maxSlots must be > 0");
        }
        if (segmentIndex < 0) {
            throw new IllegalArgumentException("segmentIndex must be >= 0");
        }
        long firstSlot = (long) segmentIndex * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT;
        long remaining = (long) maxSlots - firstSlot;
        if (remaining <= 0L) {
            return 0;
        }
        return (int) Math.min((long) YierdisNativeObjectSegment.SLOTS_PER_SEGMENT, remaining);
    }

    static int slotId(int segmentIndex, int segmentOffset) {
        long slotId = (long) segmentIndex * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT + segmentOffset + 1L;
        return Math.toIntExact(slotId);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native object table is closed");
        }
    }

    private long baseHeapBytes() {
        long bytes = 200L;
        bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(segments.length, REFERENCE_BYTES));
        bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(availableSegments.length, INT_BYTES));
        return MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(stateCounts.length, Long.BYTES));
    }

    private void replaceRetainedHeapBytes(long previousHeapBytes, long nextHeapBytes) {
        subtractRetainedHeapBytes(previousHeapBytes);
        retainedHeapBytes = MemoryUsageSnapshot.addSaturating(retainedHeapBytes, nextHeapBytes);
    }

    private void subtractRetainedHeapBytes(long bytes) {
        if (bytes < 0L || retainedHeapBytes < bytes) {
            throw new IllegalStateException("native object-table heap accounting underflow");
        }
        retainedHeapBytes -= bytes;
    }

    private SegmentSlot stale(String message) {
        throw new StaleNativeHandleException(message);
    }

    @FunctionalInterface
    public interface CapacityResolver {
        int resolveCapacity(int pageId, int pageOffset, int pageClass);
    }

    private record SegmentSlot(
            int slotId,
            int segmentIndex,
            int offset,
            YierdisNativeObjectSegment segment
    ) {
    }

    record ResolvedSlot(YierdisNativeObjectSegment segment, int offset) {
        ResolvedSlot {
            Objects.requireNonNull(segment, "segment");
        }
    }

    static final class AllocationScopeCheckpoint {
        private final int activeSegments;
        private YierdisNativeObjectSegment[] segments;
        private int[] availableSegments;
        private final long retainedHeapBytes;
        private boolean retainsSegments;
        private boolean retainsAvailableSegments;

        private AllocationScopeCheckpoint(
                int activeSegments,
                YierdisNativeObjectSegment[] segments,
                int[] availableSegments,
                long retainedHeapBytes
        ) {
            this.activeSegments = activeSegments;
            this.segments = segments;
            this.availableSegments = availableSegments;
            this.retainedHeapBytes = retainedHeapBytes;
        }

        private void retainSegments() {
            retainsSegments = true;
        }

        private void retainAvailableSegments() {
            retainsAvailableSegments = true;
        }

        private void releaseReferences() {
            segments = null;
            availableSegments = null;
            retainsSegments = false;
            retainsAvailableSegments = false;
        }

        long heapEstimatedBytes() {
            long bytes = CHECKPOINT_OBJECT_BYTES;
            if (retainsSegments) {
                bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(segments.length, REFERENCE_BYTES));
            }
            if (retainsAvailableSegments) {
                bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(availableSegments.length, INT_BYTES));
            }
            return bytes;
        }
    }
}
