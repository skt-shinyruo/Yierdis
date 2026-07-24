package yier.bubu.redis.memory.testkit;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorMetadataStats;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeHandleOwnershipException;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectKindCounts;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryBackendIds;
import yier.bubu.redis.memory.api.StableMemoryRegion;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class HeapStableMemoryBackend implements StableMemoryBackend {
    private final long allocatorId = StableMemoryBackendIds.nextId();
    private final MemoryOwner owner;
    private final int maxSlots;
    private final AtomicLong nextLocalRaw = new AtomicLong(1L);
    private final AtomicLong nextEpoch = new AtomicLong(1L);
    private final Map<Long, HeapObject> objects = new HashMap<>();
    private final Map<Long, Integer> pinCounts = new HashMap<>();
    private final Set<Long> quarantined = new HashSet<>();
    private final Set<HeapView> views = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<HeapRegion> regions = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<HeapEpochScope> epochs = Collections.newSetFromMap(new IdentityHashMap<>());

    private HeapAllocationScope activeAllocationScope;
    private long externalRegionBytes;
    private boolean closed;

    public HeapStableMemoryBackend(String name, int maxSlots, MemoryOwner owner) {
        Objects.requireNonNull(name, "name");
        if (maxSlots < 0) {
            throw new IllegalArgumentException("maxSlots must be non-negative");
        }
        this.maxSlots = maxSlots;
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public long allocatorId() {
        return allocatorId;
    }

    @Override
    public void bindToCurrentThread() {
        if (closed) {
            throw new IllegalStateException("heap stable memory backend is closed");
        }
        owner.bindToCurrentThread();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        checkOpen();
        Objects.requireNonNull(kind, "kind");
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        if (maxSlots > 0 && objects.size() >= maxSlots) {
            throw new NativeCapacityExceededException("heap stable memory slot capacity exceeded");
        }
        long localRaw = nextLocalRaw.getAndIncrement();
        if (localRaw <= 0L) {
            throw new NativeCapacityExceededException("heap stable memory handle space exhausted");
        }
        NativeHandle handle = new NativeHandle(allocatorId, localRaw);
        objects.put(localRaw, new HeapObject(kind, new byte[size]));
        if (activeAllocationScope != null) {
            activeAllocationScope.track(handle);
        }
        return handle;
    }

    @Override
    public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        checkOpen();
        Objects.requireNonNull(policy, "policy");
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize must be non-negative");
        }
        HeapObject object = requireLive(handle);
        long localRaw = handle.localRaw();
        if (isPinned(localRaw)) {
            throw new NativeMemoryException("native object is pinned");
        }
        if (hasLiveView(localRaw)) {
            throw new NativeMemoryException("native object has a live view");
        }
        if (newSize != object.bytes.length) {
            object.bytes = Arrays.copyOf(object.bytes, newSize);
        }
        return handle;
    }

    @Override
    public void free(NativeHandle handle) {
        checkOpen();
        requireLive(handle);
        long localRaw = handle.localRaw();
        if (isPinned(localRaw)) {
            // 预览结果可以在条目已替换后继续借用 pin 读取；最后一次 unpin 才真正释放块。
            quarantined.add(localRaw);
        } else {
            releaseObject(localRaw);
        }
        if (activeAllocationScope != null) {
            activeAllocationScope.forget(handle);
        }
    }

    @Override
    public void pin(NativeHandle handle) {
        checkOpen();
        requireLive(handle);
        pinCounts.merge(handle.localRaw(), 1, Integer::sum);
    }

    @Override
    public void unpin(NativeHandle handle) {
        checkOpen();
        requireRetained(handle);
        long localRaw = handle.localRaw();
        Integer count = pinCounts.get(localRaw);
        if (count == null) {
            throw new IllegalStateException("native object is not pinned");
        }
        if (count == 1) {
            pinCounts.remove(localRaw);
            if (quarantined.contains(localRaw)) {
                releaseObject(localRaw);
            }
            return;
        }
        pinCounts.put(localRaw, count - 1);
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        checkOpen();
        HeapEpochScope scope = new HeapEpochScope(
                Objects.requireNonNull(kind, "kind"),
                nextEpoch.getAndIncrement()
        );
        epochs.add(scope);
        return scope;
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        checkOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("heap stable memory allocation scope is already active");
        }
        activeAllocationScope = new HeapAllocationScope();
        return activeAllocationScope;
    }

    @Override
    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        checkOpen();
        if (expectedAllocationCount < 0) {
            throw new IllegalArgumentException("expectedAllocationCount must be non-negative");
        }
        return 0L;
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        checkOpen();
        requireLive(handle);
        pinCounts.merge(handle.localRaw(), 1, Integer::sum);
        HeapView view = new HeapView(handle, Objects.requireNonNull(mode, "mode"), true);
        views.add(view);
        return view;
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        checkOpen();
        Objects.requireNonNull(mode, "mode");
        if (mode != NativeAccessMode.READ_ONLY) {
            throw new NativeMemoryException("retained native views must be read-only");
        }
        requireRetained(handle);
        if (!isPinned(handle.localRaw())) {
            throw new NativeMemoryException("native object is not pinned");
        }
        HeapView view = new HeapView(handle, mode, false);
        views.add(view);
        return view;
    }

    @Override
    public StableMemoryRegion allocateRegion(String regionOwner, int bytes) {
        checkOpen();
        Objects.requireNonNull(regionOwner, "regionOwner");
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        HeapRegion region = new HeapRegion(new byte[bytes]);
        regions.add(region);
        externalRegionBytes = addSaturating(externalRegionBytes, bytes);
        return region;
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        checkOpen();
        if (maxMoveBytes < 0L) {
            throw new IllegalArgumentException("maxMoveBytes must be non-negative");
        }
        HeapObject object = requireLive(handle);
        if (isPinned(handle.localRaw())) {
            return NativeDefragResult.skippedPinnedObject();
        }
        if (maxMoveBytes < object.bytes.length) {
            return NativeDefragResult.skippedMoveBudget();
        }
        return NativeDefragResult.moved(object.bytes.length);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        checkOpen();
        Objects.requireNonNull(options, "options");
        return new NativeDefragReport(0L, 0L, 0L, 0L, 0L, 0L, false, false, false);
    }

    @Override
    public long logicalUsedBytes() {
        checkOpen();
        return objectBytes();
    }

    @Override
    public NativeAllocatorStats stats() {
        checkOpen();
        long objectBytes = objectBytes();
        return new NativeAllocatorStats(
                objectBytes,
                objectBytes,
                objectBytes,
                0L,
                0L,
                0L,
                0L,
                0L,
                objects.size(),
                pinCounts.size(),
                quarantined.size(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                objectKindCounts(),
                yier.bubu.redis.memory.api.NativeAllocationLatencyHistogram.empty()
        );
    }

    @Override
    public NativeAllocatorMetadataStats metadataStats() {
        checkOpen();
        long freeSlots = maxSlots == 0 ? 0L : Math.max(0L, (long) maxSlots - objects.size());
        return new NativeAllocatorMetadataStats(0L, freeSlots);
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        checkOpen();
        long dataBytes = addSaturating(objectBytes(), externalRegionBytes);
        return new MemoryUsageSnapshot(0L, 0L, dataBytes, dataBytes, 0L);
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        checkOpen();
        Objects.requireNonNull(budget, "budget");
        return MemoryReclaimResult.empty();
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        checkOpen();
        return new NativeAllocationGrowth(0L, 0L, requestedByteTotal(requestedBytes));
    }

    @Override
    public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        checkOpen();
        return new NativeAllocationGrowth(0L, 0L, requestedByteTotal(requestedBytes));
    }

    @Override
    public long liveRegionCount() {
        checkOpen();
        return regions.size();
    }

    @Override
    public void close() {
        owner.checkCurrentThreadForShutdown();
        if (closed) {
            return;
        }
        if (!views.isEmpty() || !pinCounts.isEmpty() || !epochs.isEmpty() || activeAllocationScope != null) {
            throw new IllegalStateException("heap stable memory backend still has active derived resources");
        }
        if (!regions.isEmpty()) {
            throw new IllegalStateException("heap stable memory backend still has live regions");
        }
        if (!objects.isEmpty()) {
            throw new IllegalStateException("heap stable memory backend still has live objects");
        }
        closed = true;
    }

    private void checkOpen() {
        owner.checkCurrentThread();
        if (closed) {
            throw new IllegalStateException("heap stable memory backend is closed");
        }
    }

    private HeapObject requireLive(NativeHandle handle) {
        HeapObject object = requireRetained(handle);
        if (quarantined.contains(handle.localRaw())) {
            throw new StaleNativeHandleException("native handle is quarantined");
        }
        return object;
    }

    private HeapObject requireRetained(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        if (handle.allocatorId() != allocatorId) {
            throw new NativeHandleOwnershipException(allocatorId, handle.allocatorId());
        }
        HeapObject object = objects.get(handle.localRaw());
        if (object == null) {
            throw new StaleNativeHandleException("native handle is not live");
        }
        return object;
    }

    private boolean isPinned(long localRaw) {
        return pinCounts.containsKey(localRaw);
    }

    private void releaseObject(long localRaw) {
        objects.remove(localRaw);
        quarantined.remove(localRaw);
        pinCounts.remove(localRaw);
    }

    private boolean hasLiveView(long localRaw) {
        for (HeapView view : views) {
            if (!view.closed && view.handle.localRaw() == localRaw) {
                return true;
            }
        }
        return false;
    }

    private long objectBytes() {
        long total = 0L;
        for (HeapObject object : objects.values()) {
            total = addSaturating(total, object.bytes.length);
        }
        return total;
    }

    private NativeObjectKindCounts objectKindCounts() {
        return new NativeObjectKindCounts(
                objectCount(NativeObjectKind.GENERIC),
                objectCount(NativeObjectKind.STRING_BYTES),
                objectCount(NativeObjectKind.LISTPACK_BYTES),
                objectCount(NativeObjectKind.HASH_FIELD_BYTES),
                objectCount(NativeObjectKind.HASH_VALUE_BYTES),
                objectCount(NativeObjectKind.SET_MEMBER_BYTES),
                objectCount(NativeObjectKind.ZSET_MEMBER_BYTES),
                objectCount(NativeObjectKind.SCORE_BYTES),
                objectCount(NativeObjectKind.ENTRY_RECORD),
                objectCount(NativeObjectKind.KEY_BYTES),
                objectCount(NativeObjectKind.LIST_ROOT),
                objectCount(NativeObjectKind.HASH_ROOT),
                objectCount(NativeObjectKind.SET_ROOT),
                objectCount(NativeObjectKind.ZSET_ROOT),
                objectCount(NativeObjectKind.LIST_NODE),
                objectCount(NativeObjectKind.HASH_TABLE),
                objectCount(NativeObjectKind.SET_TABLE),
                objectCount(NativeObjectKind.ZSET_TABLE),
                objectCount(NativeObjectKind.ZSET_NODE),
                objectCount(NativeObjectKind.INDEX_NODE),
                objectCount(NativeObjectKind.METADATA_RECORD)
        );
    }

    private long objectCount(NativeObjectKind kind) {
        long count = 0L;
        for (HeapObject object : objects.values()) {
            if (object.kind == kind) {
                count++;
            }
        }
        return count;
    }

    private static long requestedByteTotal(int[] requestedBytes) {
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        long total = 0L;
        for (int bytes : requestedBytes) {
            if (bytes < 0) {
                throw new IllegalArgumentException("requestedBytes must be non-negative");
            }
            total = addSaturating(total, bytes);
        }
        return total;
    }

    private static long addSaturating(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static void requireRange(int size, int offset, int length) {
        if (offset < 0 || length < 0 || offset > size - length) {
            throw new IndexOutOfBoundsException();
        }
    }

    private static int readIntLittleEndian(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static void writeIntLittleEndian(byte[] bytes, int offset, int value) {
        for (int index = 0; index < Integer.BYTES; index++) {
            bytes[offset + index] = (byte) (value >>> (index * 8));
        }
    }

    private static long readLongLittleEndian(byte[] bytes, int offset) {
        return ((long) bytes[offset] & 0xff)
                | (((long) bytes[offset + 1] & 0xff) << 8)
                | (((long) bytes[offset + 2] & 0xff) << 16)
                | (((long) bytes[offset + 3] & 0xff) << 24)
                | (((long) bytes[offset + 4] & 0xff) << 32)
                | (((long) bytes[offset + 5] & 0xff) << 40)
                | (((long) bytes[offset + 6] & 0xff) << 48)
                | (((long) bytes[offset + 7] & 0xff) << 56);
    }

    private static void writeLongLittleEndian(byte[] bytes, int offset, long value) {
        for (int index = 0; index < Long.BYTES; index++) {
            bytes[offset + index] = (byte) (value >>> (index * 8));
        }
    }

    private static final class HeapObject {
        private final NativeObjectKind kind;
        private byte[] bytes;

        private HeapObject(NativeObjectKind kind, byte[] bytes) {
            this.kind = kind;
            this.bytes = bytes;
        }
    }

    private final class HeapView implements NativeObjectView {
        private final NativeHandle handle;
        private final NativeAccessMode mode;
        private final boolean ownsPin;
        private boolean closed;

        private HeapView(NativeHandle handle, NativeAccessMode mode, boolean ownsPin) {
            this.handle = handle;
            this.mode = mode;
            this.ownsPin = ownsPin;
        }

        @Override
        public NativeHandle handle() {
            checkViewOpen();
            return handle;
        }

        @Override
        public int size() {
            return object().bytes.length;
        }

        @Override
        public int capacity() {
            return object().bytes.length;
        }

        @Override
        public byte getByte(int index) {
            byte[] bytes = object().bytes;
            requireRange(bytes.length, index, 1);
            return bytes[index];
        }

        @Override
        public void setByte(int index, byte value) {
            requireWritable();
            byte[] bytes = object().bytes;
            requireRange(bytes.length, index, 1);
            bytes[index] = value;
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            Objects.requireNonNull(dst, "dst");
            byte[] bytes = object().bytes;
            requireRange(bytes.length, index, len);
            requireRange(dst.length, dstOff, len);
            System.arraycopy(bytes, index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            requireWritable();
            Objects.requireNonNull(src, "src");
            byte[] bytes = object().bytes;
            requireRange(bytes.length, index, len);
            requireRange(src.length, srcOff, len);
            System.arraycopy(src, srcOff, bytes, index, len);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (!closed) {
                closed = true;
                views.remove(this);
                if (ownsPin) {
                    unpin(handle);
                }
            }
        }

        private HeapObject object() {
            checkViewOpen();
            return requireRetained(handle);
        }

        private void requireWritable() {
            checkViewOpen();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new IllegalStateException("native object view is read-only");
            }
        }

        private void checkViewOpen() {
            checkOpen();
            if (closed) {
                throw new IllegalStateException("native object view is closed");
            }
        }
    }

    private final class HeapRegion implements StableMemoryRegion {
        private final byte[] bytes;
        private boolean closed;

        private HeapRegion(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int size() {
            checkRegionOpen();
            return bytes.length;
        }

        @Override
        public byte getByte(int offset) {
            checkRegionOpen();
            requireRange(bytes.length, offset, 1);
            return bytes[offset];
        }

        @Override
        public void setByte(int offset, byte value) {
            checkRegionOpen();
            requireRange(bytes.length, offset, 1);
            bytes[offset] = value;
        }

        @Override
        public int getIntLittleEndian(int offset) {
            checkRegionOpen();
            requireRange(bytes.length, offset, Integer.BYTES);
            return readIntLittleEndian(bytes, offset);
        }

        @Override
        public void setIntLittleEndian(int offset, int value) {
            checkRegionOpen();
            requireRange(bytes.length, offset, Integer.BYTES);
            writeIntLittleEndian(bytes, offset, value);
        }

        @Override
        public long getLongLittleEndian(int offset) {
            checkRegionOpen();
            requireRange(bytes.length, offset, Long.BYTES);
            return readLongLittleEndian(bytes, offset);
        }

        @Override
        public void setLongLittleEndian(int offset, long value) {
            checkRegionOpen();
            requireRange(bytes.length, offset, Long.BYTES);
            writeLongLittleEndian(bytes, offset, value);
        }

        @Override
        public void getBytes(int offset, byte[] dst, int dstOffset, int length) {
            checkRegionOpen();
            Objects.requireNonNull(dst, "dst");
            requireRange(bytes.length, offset, length);
            requireRange(dst.length, dstOffset, length);
            System.arraycopy(bytes, offset, dst, dstOffset, length);
        }

        @Override
        public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            checkRegionOpen();
            Objects.requireNonNull(src, "src");
            requireRange(bytes.length, offset, length);
            requireRange(src.length, srcOffset, length);
            System.arraycopy(src, srcOffset, bytes, offset, length);
        }

        @Override
        public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) {
            checkRegionOpen();
            Objects.requireNonNull(target, "target");
            requireRange(bytes.length, sourceOffset, length);
            if (target instanceof HeapStableMemoryBackend.HeapRegion heapTarget) {
                heapTarget.checkRegionOpen();
                requireRange(heapTarget.bytes.length, targetOffset, length);
                System.arraycopy(bytes, sourceOffset, heapTarget.bytes, targetOffset, length);
                return;
            }
            byte[] copy = new byte[length];
            System.arraycopy(bytes, sourceOffset, copy, 0, length);
            target.setBytes(targetOffset, copy, 0, length);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (!closed) {
                closed = true;
                regions.remove(this);
                externalRegionBytes -= bytes.length;
            }
        }

        private void checkRegionOpen() {
            checkOpen();
            if (closed) {
                throw new IllegalStateException("stable memory region is closed");
            }
        }
    }

    private final class HeapEpochScope implements NativeEpochScope {
        private final NativeEpochKind kind;
        private final long epoch;
        private boolean closed;

        private HeapEpochScope(NativeEpochKind kind, long epoch) {
            this.kind = kind;
            this.epoch = epoch;
        }

        @Override
        public NativeEpochKind kind() {
            return kind;
        }

        @Override
        public long epoch() {
            return epoch;
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (!closed) {
                closed = true;
                epochs.remove(this);
            }
        }
    }

    private final class HeapAllocationScope implements NativeAllocationScope {
        private final Set<NativeHandle> tracked = new HashSet<>();
        private boolean completed;

        @Override
        public NativeAllocationGrowth growth() {
            checkOpen();
            long bytes = 0L;
            for (NativeHandle handle : tracked) {
                HeapObject object = objects.get(handle.localRaw());
                if (object != null) {
                    bytes = addSaturating(bytes, object.bytes.length);
                }
            }
            return new NativeAllocationGrowth(0L, 0L, bytes);
        }

        @Override
        public void promote() {
            checkOpen();
            complete();
        }

        @Override
        public void abort() {
            checkOpen();
            if (completed) {
                return;
            }
            for (NativeHandle handle : tracked) {
                long localRaw = handle.localRaw();
                if (objects.containsKey(localRaw)) {
                    if (isPinned(localRaw) || hasLiveView(localRaw)) {
                        throw new NativeMemoryException("allocation scope has active derived object state");
                    }
                    objects.remove(localRaw);
                }
            }
            complete();
        }

        private void track(NativeHandle handle) {
            tracked.add(handle);
        }

        private void forget(NativeHandle handle) {
            tracked.remove(handle);
        }

        private void complete() {
            tracked.clear();
            completed = true;
            if (activeAllocationScope == this) {
                activeAllocationScope = null;
            }
        }
    }
}
