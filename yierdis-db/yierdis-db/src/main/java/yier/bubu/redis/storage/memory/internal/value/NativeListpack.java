package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class NativeListpack implements AutoCloseable {
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long INT_BYTES = Integer.BYTES;
    private static final long FIXED_HEAP_BYTES = 72L;
    private static final int COPY_BUFFER_BYTES = 8 * 1024;

    private final NativeByteStore byteStore;
    private int[] entryOffsets = new int[0];
    private NativeHandle blockHandle = NativeHandle.NULL;
    private int size;
    private int encodedBytes;
    private int storageBytes;
    private int allocatedBytes;
    private int rawBytes;

    public NativeListpack(NativeByteStore byteStore, NativeObjectKind valueKind) {
        this.byteStore = Objects.requireNonNull(byteStore, "byteStore");
        Objects.requireNonNull(valueKind, "valueKind");
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int encodedBytes() {
        return encodedBytes;
    }

    public int allocatedBytes() {
        return allocatedBytes;
    }

    public long estimatedBytes() {
        return allocatedBytes;
    }

    public long heapEstimatedBytes() {
        return FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES + (long) entryOffsets.length * INT_BYTES;
    }

    static long heapUpperBoundForEntries(long expectedEntries) {
        if (expectedEntries < 0L || expectedEntries > Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long capacity = 0L;
        while (capacity < expectedEntries) {
            capacity = capacity == 0L ? 10L : capacity + (capacity >>> 1);
            if (capacity > Integer.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
        }
        return addSaturating(
                FIXED_HEAP_BYTES + ARRAY_HEADER_BYTES,
                multiplySaturating(capacity, INT_BYTES)
        );
    }

    public int rawBytesSize() {
        return rawBytes;
    }

    // detached replacement 在发布前一次性申请最终 block，失败时 source 的容量和内容都不会被改动。
    void reserveForBuild(int finalEntryCount, int finalEncodedBytes) {
        if (finalEntryCount < 0) {
            throw new IllegalArgumentException("finalEntryCount must be >= 0");
        }
        if (finalEncodedBytes < 0) {
            throw new IllegalArgumentException("finalEncodedBytes must be >= 0");
        }
        if (size != 0 || encodedBytes != 0) {
            throw new IllegalStateException("build reservation requires an empty listpack");
        }
        if ((finalEntryCount == 0) != (finalEncodedBytes == 0)) {
            throw new IllegalArgumentException("empty entry and encoded byte counts must agree");
        }
        ensureEntryCapacity(finalEntryCount);
        if (finalEncodedBytes > 0) {
            ensureNativeStorage(finalEncodedBytes);
        }
    }

    public void clear() {
        if (!blockHandle.isNull()) {
            byteStore.release(blockHandle);
        }
        resetState();
    }

    public void addLast(byte[] value) {
        insertAt(size, value);
    }

    public void addLast(byte[] value, NativeObjectKind kind) {
        insertAt(size, value, kind);
    }

    void addBorrowed(NativeListEntryRef entry) {
        Objects.requireNonNull(entry, "entry");
        NativeHandle handle = entry.handle();
        if (handle == null) {
            addLast(null);
            return;
        }
        addLast(byteStore.toByteArray(handle, entry.payloadOffset(), entry.payloadLength()));
    }

    public void addFirst(byte[] value) {
        insertAt(0, value);
    }

    public void addFirst(byte[] value, NativeObjectKind kind) {
        insertAt(0, value, kind);
    }

    public void insertAt(int index, byte[] value) {
        insertAt(index, value, NativeObjectKind.LISTPACK_BYTES);
    }

    public void insertAt(int index, byte[] value, NativeObjectKind kind) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException();
        }
        Objects.requireNonNull(kind, "kind");
        int payloadLength = value == null ? -1 : value.length;
        int entryBytes = entryEncodedBytes(payloadLength);
        int nextEncodedBytes = addExact(encodedBytes, entryBytes);
        ensureEntryCapacityForAdd();
        ensureNativeStorage(nextEncodedBytes);

        int insertOffset = index == size ? encodedBytes : entryOffsets[index];
        try (NativeObjectView view = byteStore.backend().resolve(blockHandle, NativeAccessMode.READ_WRITE)) {
            int tailBytes = encodedBytes - insertOffset;
            if (tailBytes > 0) {
                view.copyBytes(insertOffset, insertOffset + entryBytes, tailBytes);
            }
            writeEntry(view, insertOffset, value);
        }

        if (index < size) {
            System.arraycopy(entryOffsets, index, entryOffsets, index + 1, size - index);
            for (int i = index + 1; i <= size; i++) {
                entryOffsets[i] += entryBytes;
            }
        }
        entryOffsets[index] = insertOffset;
        size++;
        encodedBytes = nextEncodedBytes;
        if (payloadLength >= 0) {
            rawBytes = addExact(rawBytes, payloadLength);
        }
    }

    public byte[] removeFirst() {
        return removeAt(0);
    }

    public byte[] removeLast() {
        return removeAt(size - 1);
    }

    public byte[] removeAt(int index) {
        return remove(index, true);
    }

    public void removeAtDiscard(int index) {
        remove(index, false);
    }

    public NativeListEntryRef entryRefAt(int index) {
        long slice = entrySliceAt(index);
        int payloadLength = sliceLength(slice);
        if (payloadLength < 0) {
            return NativeListEntryRef.nullValue();
        }
        return NativeListEntryRef.handle(
                blockHandle,
                sliceOffset(slice),
                payloadLength,
                allocatedBytes
        );
    }

    public byte[] get(int index) {
        return toByteArray(entrySliceAt(index));
    }

    public void writeAt(int index, ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        writeSlice(entrySliceAt(index), out);
    }

    int encodedEntryBytesAt(int index) {
        checkIndex(index);
        int entryEnd = index + 1 < size ? entryOffsets[index + 1] : encodedBytes;
        return entryEnd - entryOffsets[index];
    }

    int encodedBytesInRange(int fromIndex, int entryCount) {
        checkRange(fromIndex, entryCount);
        if (entryCount == 0) {
            return 0;
        }
        int startOffset = entryOffsets[fromIndex];
        int endIndex = fromIndex + entryCount;
        int endOffset = endIndex < size ? entryOffsets[endIndex] : encodedBytes;
        return endOffset - startOffset;
    }

    void appendRangeFrom(NativeListpack source, int fromIndex, int entryCount) {
        Objects.requireNonNull(source, "source");
        if (source == this) {
            throw new IllegalArgumentException("source must be a different listpack");
        }
        source.checkRange(fromIndex, entryCount);
        if (entryCount == 0) {
            return;
        }

        int copiedBytes = source.encodedBytesInRange(fromIndex, entryCount);
        int nextSize = Math.addExact(size, entryCount);
        int nextEncodedBytes = addExact(encodedBytes, copiedBytes);
        ensureEntryCapacity(nextSize);
        ensureNativeStorage(nextEncodedBytes);

        int sourceOffset = source.entryOffsets[fromIndex];
        byte[] transfer = new byte[Math.min(copiedBytes, COPY_BUFFER_BYTES)];
        int copiedRawBytes = 0;
        try (NativeObjectView sourceView = source.byteStore.backend().resolve(
                source.blockHandle,
                NativeAccessMode.READ_ONLY
        ); NativeObjectView targetView = byteStore.backend().resolve(
                blockHandle,
                NativeAccessMode.READ_WRITE
        )) {
            int transferredBytes = 0;
            while (transferredBytes < copiedBytes) {
                int chunkBytes = Math.min(transfer.length, copiedBytes - transferredBytes);
                sourceView.getBytes(sourceOffset + transferredBytes, transfer, 0, chunkBytes);
                targetView.setBytes(encodedBytes + transferredBytes, transfer, 0, chunkBytes);
                transferredBytes += chunkBytes;
            }

            for (int index = 0; index < entryCount; index++) {
                int sourceIndex = fromIndex + index;
                int entryOffset = source.entryOffsets[sourceIndex];
                int entryEnd = sourceIndex + 1 < source.size
                        ? source.entryOffsets[sourceIndex + 1]
                        : source.encodedBytes;
                long decoded = decodeHeader(sourceView, entryOffset);
                int payloadLength = decodedPayloadLength(decoded);
                validateEntryEnd(entryOffset + decodedHeaderBytes(decoded), payloadLength, entryEnd);
                entryOffsets[size + index] = encodedBytes + entryOffset - sourceOffset;
                if (payloadLength > 0) {
                    copiedRawBytes = addExact(copiedRawBytes, payloadLength);
                }
            }
        }
        size = nextSize;
        encodedBytes = nextEncodedBytes;
        rawBytes = addExact(rawBytes, copiedRawBytes);
    }

    public int nativePayloadCount() {
        return blockHandle.isNull() ? 0 : 1;
    }

    public int copyNativePayloadSizes(int[] target, int offset) {
        Objects.requireNonNull(target, "target");
        if (blockHandle.isNull()) {
            return offset;
        }
        target[offset] = Math.max(1, allocatedBytes);
        return offset + 1;
    }

    public boolean equalsAt(int index, byte[] other) {
        return equalsSlice(entrySliceAt(index), other);
    }

    public void set(int index, byte[] value) {
        set(index, value, NativeObjectKind.LISTPACK_BYTES);
    }

    public void set(int index, byte[] value, NativeObjectKind kind) {
        checkIndex(index);
        Objects.requireNonNull(kind, "kind");
        int nextPayloadLength = value == null ? -1 : value.length;
        int nextEntryBytes = entryEncodedBytes(nextPayloadLength);

        int entryOffset = entryOffsets[index];
        long oldSlice = entrySliceAt(index);
        int oldPayloadLength = sliceLength(oldSlice);
        int oldEntryEnd = index + 1 < size ? entryOffsets[index + 1] : encodedBytes;
        int oldEntryBytes = oldEntryEnd - entryOffset;
        int delta = nextEntryBytes - oldEntryBytes;
        int nextEncodedBytes = addExact(encodedBytes, delta);
        ensureNativeStorage(nextEncodedBytes);

        try (NativeObjectView view = byteStore.backend().resolve(blockHandle, NativeAccessMode.READ_WRITE)) {
            int tailBytes = encodedBytes - oldEntryEnd;
            if (tailBytes > 0 && delta != 0) {
                view.copyBytes(oldEntryEnd, oldEntryEnd + delta, tailBytes);
            }
            writeEntry(view, entryOffset, value);
        }

        if (delta != 0) {
            for (int i = index + 1; i < size; i++) {
                entryOffsets[i] += delta;
            }
        }
        encodedBytes = nextEncodedBytes;
        rawBytes = addExact(
                rawBytes,
                Math.max(0, nextPayloadLength) - Math.max(0, oldPayloadLength)
        );
    }

    void replaceBorrowedAt(int index, byte[] value, NativeObjectKind kind) {
        set(index, value, kind);
    }

    public int indexOf(byte[] needle) {
        for (int i = 0; i < size; i++) {
            if (equalsAt(i, needle)) {
                return i;
            }
        }
        return -1;
    }

    public Cursor cursor() {
        return new Cursor(this);
    }

    public void forEachNativeHandle(Consumer<NativeHandle> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (!blockHandle.isNull()) {
            consumer.accept(blockHandle);
        }
    }

    public void closeExcept(NativeHandle[] retained) {
        closeExcept(handle -> isRetained(handle, retained));
    }

    void closeExcept(Predicate<NativeHandle> retained) {
        Objects.requireNonNull(retained, "retained");
        if (blockHandle.isNull()) {
            resetState();
            return;
        }
        NativeHandle handle = blockHandle;
        if (retained.test(handle)) {
            // pop 响应接管整块 ownership 后，原 listpack 只移除自己的计量，不能提前 free。
            byteStore.forget(handle);
        } else {
            byteStore.release(handle);
        }
        resetState();
    }

    void closeExcept(NativeListpack retained) {
        Objects.requireNonNull(retained, "retained");
        if (blockHandle.isNull()) {
            resetState();
            return;
        }
        NativeHandle handle = blockHandle;
        if (handle.equals(retained.blockHandle)) {
            byteStore.forget(handle);
        } else {
            byteStore.release(handle);
        }
        resetState();
    }

    @Override
    public void close() {
        clear();
    }

    private byte[] remove(int index, boolean copyPayload) {
        checkIndex(index);
        int entryOffset = entryOffsets[index];
        int entryEnd = index + 1 < size ? entryOffsets[index + 1] : encodedBytes;
        int entryBytes = entryEnd - entryOffset;
        byte[] removed;
        int payloadLength;

        try (NativeObjectView view = byteStore.backend().resolve(blockHandle, NativeAccessMode.READ_WRITE)) {
            long decoded = decodeHeader(view, entryOffset);
            payloadLength = decodedPayloadLength(decoded);
            int payloadOffset = entryOffset + decodedHeaderBytes(decoded);
            validateEntryEnd(payloadOffset, payloadLength, entryEnd);
            if (!copyPayload || payloadLength < 0) {
                removed = null;
            } else if (payloadLength == 0) {
                removed = new byte[0];
            } else {
                removed = new byte[payloadLength];
                view.getBytes(payloadOffset, removed, 0, payloadLength);
            }

            int tailBytes = encodedBytes - entryEnd;
            if (tailBytes > 0) {
                view.copyBytes(entryEnd, entryOffset, tailBytes);
            }
        }

        for (int i = index + 1; i < size; i++) {
            entryOffsets[i - 1] = entryOffsets[i] - entryBytes;
        }
        entryOffsets[size - 1] = 0;
        size--;
        encodedBytes -= entryBytes;
        if (payloadLength >= 0) {
            rawBytes -= payloadLength;
        }
        return removed;
    }

    private void ensureEntryCapacityForAdd() {
        ensureEntryCapacity(addExact(size, 1));
    }

    private void ensureEntryCapacity(int expectedEntries) {
        if (expectedEntries <= entryOffsets.length) {
            return;
        }
        int next = entryOffsets.length == 0 ? 10 : entryOffsets.length;
        while (next < expectedEntries) {
            int grown = next + (next >>> 1);
            if (grown <= next) {
                next = Integer.MAX_VALUE;
                break;
            }
            next = grown;
        }
        entryOffsets = Arrays.copyOf(entryOffsets, next);
    }

    private void ensureNativeStorage(int requiredBytes) {
        if (requiredBytes <= 0) {
            throw new IllegalArgumentException("requiredBytes must be > 0");
        }
        if (blockHandle.isNull()) {
            blockHandle = byteStore.allocateBlock(NativeObjectKind.LISTPACK_BYTES, requiredBytes);
            storageBytes = requiredBytes;
            allocatedBytes = byteStore.allocatedBytes(blockHandle);
            return;
        }
        if (requiredBytes <= storageBytes) {
            return;
        }
        blockHandle = byteStore.reallocateBlock(blockHandle, requiredBytes, NativeReallocPolicy.PRESERVE_PREFIX);
        storageBytes = requiredBytes;
        allocatedBytes = byteStore.allocatedBytes(blockHandle);
    }

    private long entrySliceAt(int index) {
        checkIndex(index);
        int entryOffset = entryOffsets[index];
        int entryEnd = index + 1 < size ? entryOffsets[index + 1] : encodedBytes;
        try (NativeObjectView view = byteStore.backend().resolve(blockHandle, NativeAccessMode.READ_ONLY)) {
            long decoded = decodeHeader(view, entryOffset);
            int payloadLength = decodedPayloadLength(decoded);
            int payloadOffset = entryOffset + decodedHeaderBytes(decoded);
            validateEntryEnd(payloadOffset, payloadLength, entryEnd);
            return encodeSlice(payloadOffset, payloadLength);
        }
    }

    private byte[] toByteArray(long slice) {
        int payloadLength = sliceLength(slice);
        if (payloadLength < 0) {
            return null;
        }
        return byteStore.toByteArray(blockHandle, sliceOffset(slice), payloadLength);
    }

    private boolean equalsSlice(long slice, byte[] other) {
        int payloadLength = sliceLength(slice);
        if (payloadLength < 0) {
            return other == null;
        }
        if (other == null || other.length != payloadLength) {
            return false;
        }
        try (NativeObjectView view = byteStore.backend().resolve(blockHandle, NativeAccessMode.READ_ONLY)) {
            return view.contentEquals(sliceOffset(slice), other, 0, payloadLength);
        }
    }

    private void writeSlice(long slice, ByteValueSink out) {
        int payloadLength = sliceLength(slice);
        if (payloadLength < 0) {
            out.nullValue();
            return;
        }
        out.value(byteStore.slice(blockHandle, sliceOffset(slice), payloadLength));
    }

    private void resetState() {
        blockHandle = NativeHandle.NULL;
        size = 0;
        encodedBytes = 0;
        storageBytes = 0;
        allocatedBytes = 0;
        rawBytes = 0;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
    }

    private void checkRange(int fromIndex, int entryCount) {
        if (fromIndex < 0 || entryCount < 0 || fromIndex > size - entryCount) {
            throw new IndexOutOfBoundsException();
        }
    }

    private static void writeEntry(NativeObjectView view, int offset, byte[] value) {
        int headerValue = value == null ? 0 : addExact(value.length, 1);
        int next = writeVarInt(view, offset, headerValue);
        if (value != null && value.length > 0) {
            view.setBytes(next, value, 0, value.length);
        }
    }

    private static int writeVarInt(NativeObjectView view, int offset, int value) {
        int next = offset;
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            view.setByte(next++, (byte) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        view.setByte(next++, (byte) remaining);
        return next;
    }

    private static long decodeHeader(NativeObjectView view, int offset) {
        long value = 0L;
        int shift = 0;
        for (int bytes = 1; bytes <= 5; bytes++) {
            int current = view.getByte(offset + bytes - 1) & 0xFF;
            value |= (long) (current & 0x7F) << shift;
            if ((current & 0x80) == 0) {
                if (value > Integer.MAX_VALUE) {
                    throw new IllegalStateException("listpack entry length exceeds int range");
                }
                return ((long) bytes << 32) | value;
            }
            shift += 7;
        }
        throw new IllegalStateException("invalid listpack varint");
    }

    private static int decodedHeaderBytes(long decoded) {
        return (int) (decoded >>> 32);
    }

    private static int decodedPayloadLength(long decoded) {
        return (int) decoded - 1;
    }

    private static void validateEntryEnd(int payloadOffset, int payloadLength, int entryEnd) {
        int expectedEnd = payloadLength < 0 ? payloadOffset : addExact(payloadOffset, payloadLength);
        if (expectedEnd != entryEnd) {
            throw new IllegalStateException("listpack entry offsets do not match encoded payload");
        }
    }

    private static long encodeSlice(int payloadOffset, int payloadLength) {
        return ((long) payloadOffset << 32) | ((payloadLength + 1L) & 0xFFFF_FFFFL);
    }

    private static int sliceOffset(long slice) {
        return (int) (slice >>> 32);
    }

    private static int sliceLength(long slice) {
        return (int) slice - 1;
    }

    private static boolean isRetained(NativeHandle handle, NativeHandle[] retained) {
        if (retained == null || retained.length == 0) {
            return false;
        }
        for (NativeHandle candidate : retained) {
            if (candidate != null && candidate.equals(handle)) {
                return true;
            }
        }
        return false;
    }

    static int entryEncodedBytes(byte[] value) {
        return entryEncodedBytes(value == null ? -1 : value.length);
    }

    static int entryEncodedBytes(int payloadLength) {
        int headerValue = payloadLength < 0 ? 0 : addExact(payloadLength, 1);
        return addExact(varIntSize(headerValue), Math.max(0, payloadLength));
    }

    static int encodedBytesOf(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        int total = 0;
        for (byte[] value : values) {
            total = addExact(total, entryEncodedBytes(value));
        }
        return total;
    }

    private static int varIntSize(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }

    private static int addExact(int left, int right) {
        long sum = (long) left + right;
        if (sum < 0L || sum > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("listpack size exceeds int range");
        }
        return (int) sum;
    }

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    public static final class Cursor {
        private final NativeListpack owner;
        private int index = -1;
        private long slice;

        private Cursor(NativeListpack owner) {
            this.owner = owner;
        }

        public boolean next() {
            if (index + 1 >= owner.size) {
                return false;
            }
            index++;
            slice = owner.entrySliceAt(index);
            return true;
        }

        public boolean isNull() {
            ensurePositioned();
            return sliceLength(slice) < 0;
        }

        public int length() {
            ensurePositioned();
            return Math.max(0, sliceLength(slice));
        }

        public boolean equalsBytes(byte[] other) {
            ensurePositioned();
            return owner.equalsSlice(slice, other);
        }

        public byte[] toByteArray() {
            ensurePositioned();
            return owner.toByteArray(slice);
        }

        public void writeTo(ByteValueSink out) {
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            ensurePositioned();
            owner.writeSlice(slice, out);
        }

        private void ensurePositioned() {
            if (index < 0 || index >= owner.size) {
                throw new IllegalStateException("cursor is not positioned");
            }
        }
    }
}
