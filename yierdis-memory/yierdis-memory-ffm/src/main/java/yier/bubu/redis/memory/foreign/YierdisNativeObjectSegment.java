package yier.bubu.redis.memory.foreign;

final class YierdisNativeObjectSegment implements AutoCloseable {
    static final int SLOTS_PER_SEGMENT = 4_096;
    private static final int RETIRED_WORDS = SLOTS_PER_SEGMENT / Long.SIZE;

    private final YierdisFfmRegion metadataRegion;
    private final int validSlots;
    private final int[] freeStack = new int[SLOTS_PER_SEGMENT];
    private final long[] retiredBitmap = new long[RETIRED_WORDS];

    private int freeCount;
    private boolean availableQueued;
    private boolean closed;

    YierdisNativeObjectSegment(
            YierdisFfmMemoryRuntime runtime,
            int segmentIndex,
            int validSlots,
            int ownerShardId
    ) {
        if (segmentIndex < 0) {
            throw new IllegalArgumentException("segmentIndex must be >= 0");
        }
        if (validSlots <= 0 || validSlots > SLOTS_PER_SEGMENT) {
            throw new IllegalArgumentException("validSlots must be between 1 and " + SLOTS_PER_SEGMENT);
        }
        this.metadataRegion = runtime.allocateRegion(
                "native-object-table-segment-" + segmentIndex,
                SLOTS_PER_SEGMENT * YierdisNativeObjectTable.META_BYTES
        );
        this.validSlots = validSlots;
        for (int offset = validSlots - 1; offset >= 0; offset--) {
            writeInt(offset, YierdisNativeObjectTable.GENERATION_OFFSET, YierdisNativeObjectTable.INITIAL_GENERATION);
            writeInt(offset, YierdisNativeObjectTable.OWNER_SHARD_ID_OFFSET, ownerShardId);
            writeInt(offset, YierdisNativeObjectTable.STATE_OFFSET, YierdisNativeObjectTable.STATE_FREE);
            freeStack[freeCount++] = offset;
        }
    }

    int validSlots() {
        return validSlots;
    }

    int allocateOffset() {
        ensureOpen();
        if (freeCount == 0) {
            return -1;
        }
        return freeStack[--freeCount];
    }

    void releaseOffset(int offset) {
        ensureValidOffset(offset);
        if (isRetired(offset)) {
            throw new IllegalStateException("retired slot cannot be released");
        }
        if (freeCount >= validSlots) {
            throw new IllegalStateException("segment free stack overflow");
        }
        freeStack[freeCount++] = offset;
    }

    void retire(int offset) {
        ensureValidOffset(offset);
        retiredBitmap[offset >>> 6] |= 1L << (offset & 63);
    }

    boolean isRetired(int offset) {
        ensureValidOffset(offset);
        return (retiredBitmap[offset >>> 6] & (1L << (offset & 63))) != 0;
    }

    boolean hasFreeSlot() {
        return freeCount > 0;
    }

    boolean availableQueued() {
        return availableQueued;
    }

    void availableQueued(boolean availableQueued) {
        this.availableQueued = availableQueued;
    }

    long readLong(int offset, int fieldOffset) {
        return YierdisFfmAccess.getLong(metaSpan(offset), fieldOffset);
    }

    void writeLong(int offset, int fieldOffset, long value) {
        YierdisFfmAccess.setLong(metaSpan(offset), fieldOffset, value);
    }

    int readInt(int offset, int fieldOffset) {
        return YierdisFfmAccess.getInt(metaSpan(offset), fieldOffset);
    }

    void writeInt(int offset, int fieldOffset, int value) {
        YierdisFfmAccess.setInt(metaSpan(offset), fieldOffset, value);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        metadataRegion.close();
    }

    private YierdisFfmSpan metaSpan(int offset) {
        ensureValidOffset(offset);
        return metadataRegion.span(offset * YierdisNativeObjectTable.META_BYTES, YierdisNativeObjectTable.META_BYTES);
    }

    private void ensureValidOffset(int offset) {
        ensureOpen();
        if (offset < 0 || offset >= validSlots) {
            throw new IndexOutOfBoundsException("invalid object segment offset: " + offset);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native object segment is closed");
        }
    }
}
