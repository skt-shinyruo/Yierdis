package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

public final class NativeKeyDirectory implements AutoCloseable {
    // Directory 只把 key bytes 放在 native allocator 中；探测数组留在 heap 上，tombstone 用来维持线性探测链。
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;

    private final NativeAllocator allocator;
    private final boolean ownsAllocator;
    private NativeHandle[] keyHandles;
    private EntryHandle[] handles;
    private int[] hashes;
    private byte[] states;
    private int size;
    private int tombstones;
    private boolean closed;

    public NativeKeyDirectory(YierdisFfmMemoryRuntime runtime) {
        this(new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
    }

    public NativeKeyDirectory(NativeAllocator allocator) {
        this(allocator, false);
    }

    private NativeKeyDirectory(NativeAllocator allocator, boolean ownsAllocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
        int capacity = MIN_CAPACITY;
        this.keyHandles = new NativeHandle[capacity];
        this.handles = new EntryHandle[capacity];
        this.hashes = new int[capacity];
        this.states = new byte[capacity];
    }

    public synchronized int size() {
        return size;
    }

    public synchronized long nativeBytes() {
        ensureOpen();
        return 0L;
    }

    public synchronized long estimatedInsertHeapGrowthBytes() {
        ensureOpen();
        if (size + tombstones + 1 <= threshold()) {
            return 0L;
        }
        return heapBytesForCapacity(keyHandles.length << 1);
    }

    public synchronized EntryHandle get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        return index < 0 ? null : handles[index];
    }

    public synchronized KeyHandle getKeyHandle(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        return index < 0 ? null : keyHandleAt(index);
    }

    public synchronized KeyHandle randomKeyHandle() {
        ensureOpen();
        if (size == 0) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(keyHandles.length);
        for (int step = 0; step < keyHandles.length; step++) {
            int index = (start + step) & (keyHandles.length - 1);
            if (states[index] == STATE_FILLED) {
                return keyHandleAt(index);
            }
        }
        return null;
    }

    public synchronized void forEachEntry(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        for (int i = 0; i < keyHandles.length; i++) {
            if (states[i] == STATE_FILLED) {
                consumer.accept(keyHandleAt(i), handles[i]);
            }
        }
    }

    public synchronized ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer consumer) {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        if (maxSteps <= 0 || size == 0) {
            return ScanCursorV2.start();
        }

        int index = Math.toIntExact(Math.min(cursor.position(), keyHandles.length));
        int visited = 0;
        while (index < keyHandles.length && visited < maxSteps) {
            if (states[index] == STATE_FILLED) {
                visited++;
                if (!consumer.accept(keyHandleAt(index), handles[index])) {
                    return nextCursor(index + 1);
                }
            }
            index++;
        }
        return nextCursor(index);
    }

    public synchronized EntryHandle compute(
            byte[] keyBytes,
            BiFunction<? super byte[], ? super EntryHandle, ? extends EntryHandle> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        ensureOpen();

        int hash = hash(keyBytes);
        int index = findIndex(keyBytes, hash);
        if (index >= 0) {
            EntryHandle oldHandle = handles[index];
            EntryHandle newHandle = remappingFunction.apply(keyBytes, oldHandle);
            if (newHandle == null) {
                removeAt(index);
                return null;
            }
            handles[index] = newHandle;
            return newHandle;
        }

        EntryHandle newHandle = remappingFunction.apply(keyBytes, null);
        if (newHandle == null) {
            return null;
        }

        ensureCapacityForInsert();
        int insertIndex = findInsertIndex(keyBytes, hash);
        if (states[insertIndex] == STATE_TOMBSTONE) {
            tombstones--;
        }
        keyHandles[insertIndex] = allocateKey(keyBytes);
        handles[insertIndex] = newHandle;
        hashes[insertIndex] = hash;
        states[insertIndex] = STATE_FILLED;
        size++;
        return newHandle;
    }

    public StagedInsert stageInsert(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        byte[] stableKeyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
        NativeHandle keyHandle;
        int hash = hash(stableKeyBytes);
        synchronized (this) {
            ensureOpen();
            if (findIndex(stableKeyBytes, hash) >= 0) {
                throw new IllegalStateException("native key already exists during staged insert");
            }
            keyHandle = allocateKey(stableKeyBytes);
        }
        StagedDirectory stagedDirectory = stageDirectoryForInsert();
        return new StagedInsert(stableKeyBytes, hash, keyHandle, stagedDirectory);
    }

    public synchronized void publishStagedInsert(StagedInsert staged, EntryHandle entryHandle) {
        Objects.requireNonNull(staged, "staged");
        Objects.requireNonNull(entryHandle, "entryHandle");
        ensureOpen();
        staged.ensureActive();
        if (findIndex(staged.keyBytes, staged.hash) >= 0) {
            throw new IllegalStateException("native key appeared during staged insert");
        }
        if (staged.directory != null) {
            publishStagedDirectory(staged.directory);
        }
        int insertIndex = findInsertIndex(staged.keyBytes, staged.hash);
        if (states[insertIndex] == STATE_TOMBSTONE) {
            tombstones--;
        }
        keyHandles[insertIndex] = staged.publish();
        handles[insertIndex] = entryHandle;
        hashes[insertIndex] = staged.hash;
        states[insertIndex] = STATE_FILLED;
        size++;
    }

    public synchronized EntryHandle remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        if (index < 0) {
            return null;
        }
        EntryHandle removed = handles[index];
        removeAt(index);
        return removed;
    }

    public synchronized boolean remove(byte[] keyBytes, EntryHandle expectedHandle) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(expectedHandle, "expectedHandle");
        ensureOpen();
        int index = findIndex(keyBytes, hash(keyBytes));
        if (index < 0 || !expectedHandle.equals(handles[index])) {
            return false;
        }
        removeAt(index);
        return true;
    }

    public synchronized boolean remove(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        int index = findHandleIndex(handle);
        if (index < 0) {
            return false;
        }
        removeAt(index);
        return true;
    }

    public synchronized void clear() {
        ensureOpen();
        clearInternal();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        Throwable failure = null;
        try {
            clearInternal();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        closed = true;
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native key directory is closed");
        }
    }

    private KeyHandle keyHandleAt(int index) {
        return KeyHandle.forNative(allocator, keyHandles[index], hashes[index]);
    }

    private ScanCursorV2 nextCursor(int nextIndex) {
        return nextIndex >= keyHandles.length ? ScanCursorV2.start() : ScanCursorV2.of(nextIndex);
    }

    private void clearInternal() {
        Throwable failure = null;
        for (int i = 0; i < keyHandles.length; i++) {
            if (states[i] == STATE_FILLED) {
                try {
                    allocator.free(keyHandles[i]);
                } catch (RuntimeException | Error e) {
                    failure = addFailure(failure, e);
                    continue;
                }
            }
            keyHandles[i] = null;
            handles[i] = null;
            hashes[i] = 0;
            states[i] = STATE_EMPTY;
        }
        size = 0;
        tombstones = 0;
        if (failure != null) {
            recomputeCounts();
            rethrow(failure);
        }
    }

    private void ensureCapacityForInsert() {
        if (size + tombstones + 1 <= threshold()) {
            return;
        }
        rehash(keyHandles.length << 1);
    }

    private StagedDirectory stageDirectoryForInsert() {
        if (size + tombstones + 1 <= threshold()) {
            return null;
        }
        int newCapacity = keyHandles.length << 1;
        NativeHandle[] stagedKeyHandles = new NativeHandle[newCapacity];
        EntryHandle[] stagedHandles = new EntryHandle[newCapacity];
        int[] stagedHashes = new int[newCapacity];
        byte[] stagedStates = new byte[newCapacity];
        for (int i = 0; i < keyHandles.length; i++) {
            if (states[i] != STATE_FILLED) {
                continue;
            }
            int insertIndex = findInsertIndexInStates(stagedStates, hashes[i]);
            stagedKeyHandles[insertIndex] = keyHandles[i];
            stagedHandles[insertIndex] = handles[i];
            stagedHashes[insertIndex] = hashes[i];
            stagedStates[insertIndex] = STATE_FILLED;
        }
        return new StagedDirectory(stagedKeyHandles, stagedHandles, stagedHashes, stagedStates);
    }

    private void publishStagedDirectory(StagedDirectory staged) {
        keyHandles = staged.keyHandles;
        handles = staged.handles;
        hashes = staged.hashes;
        states = staged.states;
        tombstones = 0;
    }

    private void rehash(int newCapacity) {
        NativeHandle[] oldKeyHandles = keyHandles;
        EntryHandle[] oldHandles = handles;
        int[] oldHashes = hashes;
        byte[] oldStates = states;

        keyHandles = new NativeHandle[newCapacity];
        handles = new EntryHandle[newCapacity];
        hashes = new int[newCapacity];
        states = new byte[newCapacity];
        tombstones = 0;

        for (int i = 0; i < oldKeyHandles.length; i++) {
            if (oldStates[i] != STATE_FILLED) {
                continue;
            }
            NativeHandle keyHandle = oldKeyHandles[i];
            EntryHandle handle = oldHandles[i];
            int hash = oldHashes[i];
            int index = findInsertIndex(keyHandle, hash);
            keyHandles[index] = keyHandle;
            handles[index] = handle;
            hashes[index] = hash;
            states[index] = STATE_FILLED;
        }
    }

    private int threshold() {
        return Math.max(1, (int) (keyHandles.length * LOAD_FACTOR));
    }

    private int findIndex(byte[] keyBytes, int hash) {
        int mask = keyHandles.length - 1;
        int index = hash & mask;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[index] == hash && equalsBytes(keyHandles[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findHandleIndex(EntryHandle handle) {
        for (int i = 0; i < handles.length; i++) {
            if (states[i] == STATE_FILLED && handle.equals(handles[i])) {
                return i;
            }
        }
        return -1;
    }

    private int findInsertIndex(byte[] keyBytes, int hash) {
        int mask = keyHandles.length - 1;
        int index = hash & mask;
        int firstTombstone = -1;
        while (true) {
            byte state = states[index];
            if (state == STATE_EMPTY) {
                return firstTombstone >= 0 ? firstTombstone : index;
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = index;
                }
            } else if (hashes[index] == hash && equalsBytes(keyHandles[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findInsertIndex(NativeHandle keyHandle, int hash) {
        int mask = keyHandles.length - 1;
        int index = hash & mask;
        while (true) {
            if (states[index] == STATE_EMPTY) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private int findInsertIndexInStates(byte[] targetStates, int hash) {
        int mask = targetStates.length - 1;
        int index = hash & mask;
        while (true) {
            if (targetStates[index] == STATE_EMPTY) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void removeAt(int index) {
        if (states[index] != STATE_FILLED) {
            return;
        }
        allocator.free(keyHandles[index]);
        keyHandles[index] = null;
        handles[index] = null;
        hashes[index] = 0;
        states[index] = STATE_TOMBSTONE;
        size--;
        tombstones++;
    }

    private void recomputeCounts() {
        int nextSize = 0;
        int nextTombstones = 0;
        for (byte state : states) {
            if (state == STATE_FILLED) {
                nextSize++;
            } else if (state == STATE_TOMBSTONE) {
                nextTombstones++;
            }
        }
        size = nextSize;
        tombstones = nextTombstones;
    }

    private static int hash(byte[] keyBytes) {
        int h = 1;
        for (byte keyByte : keyBytes) {
            h = 31 * h + keyByte;
        }
        return h;
    }

    private NativeHandle allocateKey(byte[] keyBytes) {
        NativeHandle handle = allocator.allocate(NativeObjectKind.KEY_BYTES, keyBytes.length);
        boolean ok = false;
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            if (keyBytes.length > 0) {
                view.setBytes(0, keyBytes, 0, keyBytes.length);
            }
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                allocator.free(handle);
            }
        }
    }

    private boolean equalsBytes(NativeHandle handle, byte[] keyBytes) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(keyBytes, "keyBytes");
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            if (view.size() != keyBytes.length) {
                return false;
            }
            for (int i = 0; i < keyBytes.length; i++) {
                if (view.getByte(i) != keyBytes[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }

    private static long heapBytesForCapacity(int capacity) {
        long keyHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long entryHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long hashArray = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long stateArray = ARRAY_HEADER_BYTES + capacity;
        return keyHandleArray + entryHandleArray + hashArray + stateArray;
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(KeyHandle keyHandle, EntryHandle entryHandle);
    }

    @FunctionalInterface
    public interface ScanConsumer {
        boolean accept(KeyHandle keyHandle, EntryHandle entryHandle);
    }

    public final class StagedInsert implements AutoCloseable {
        private final byte[] keyBytes;
        private final int hash;
        private final StagedDirectory directory;
        private NativeHandle keyHandle;
        private boolean terminal;

        private StagedInsert(byte[] keyBytes, int hash, NativeHandle keyHandle, StagedDirectory directory) {
            this.keyBytes = keyBytes;
            this.hash = hash;
            this.keyHandle = Objects.requireNonNull(keyHandle, "keyHandle");
            this.directory = directory;
        }

        public KeyHandle keyHandle() {
            ensureActive();
            return KeyHandle.forNative(allocator, keyHandle, hash);
        }

        public long stagedHeapBytes() {
            return directory == null ? 0L : heapBytesForCapacity(directory.keyHandles.length);
        }

        private NativeHandle publish() {
            ensureActive();
            NativeHandle published = keyHandle;
            keyHandle = null;
            terminal = true;
            return published;
        }

        private void ensureActive() {
            if (terminal || keyHandle == null) {
                throw new IllegalStateException("staged native key insert is closed");
            }
        }

        @Override
        public void close() {
            if (terminal) {
                return;
            }
            terminal = true;
            NativeHandle handle = keyHandle;
            keyHandle = null;
            if (handle != null) {
                allocator.free(handle);
            }
        }
    }

    private record StagedDirectory(
            NativeHandle[] keyHandles,
            EntryHandle[] handles,
            int[] hashes,
            byte[] states
    ) {
    }
}
