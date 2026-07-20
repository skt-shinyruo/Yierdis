package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;

final class RawPathRecordingAllocator implements NativeAllocator {
    private final NativeAllocator delegate;
    private int allocateRawCalls;
    private int reallocRawCalls;
    private int freeRawCalls;
    private int pinRawCalls;
    private int unpinRawCalls;
    private int resolveRawCalls;

    RawPathRecordingAllocator(NativeAllocator delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        throw boxedCall("allocate");
    }

    @Override
    public long allocateRaw(NativeObjectKind kind, int size) {
        allocateRawCalls++;
        return delegate.allocateRaw(kind, size);
    }

    @Override
    public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        throw boxedCall("realloc");
    }

    @Override
    public long reallocRaw(long rawHandle, int newSize, NativeReallocPolicy policy) {
        reallocRawCalls++;
        return delegate.reallocRaw(rawHandle, newSize, policy);
    }

    @Override
    public void free(NativeHandle handle) {
        throw boxedCall("free");
    }

    @Override
    public void freeRaw(long rawHandle) {
        freeRawCalls++;
        delegate.freeRaw(rawHandle);
    }

    @Override
    public void pin(NativeHandle handle) {
        throw boxedCall("pin");
    }

    @Override
    public void pinRaw(long rawHandle) {
        pinRawCalls++;
        delegate.pinRaw(rawHandle);
    }

    @Override
    public void unpin(NativeHandle handle) {
        throw boxedCall("unpin");
    }

    @Override
    public void unpinRaw(long rawHandle) {
        unpinRawCalls++;
        delegate.unpinRaw(rawHandle);
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        return delegate.beginEpoch(kind);
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        throw boxedCall("resolve");
    }

    @Override
    public NativeObjectView resolveRaw(long rawHandle, NativeAccessMode mode) {
        resolveRawCalls++;
        return delegate.resolveRaw(rawHandle, mode);
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        throw boxedCall("resolvePinned");
    }

    @Override
    public NativeObjectView resolvePinnedRaw(long rawHandle, NativeAccessMode mode) {
        return delegate.resolvePinnedRaw(rawHandle, mode);
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        return delegate.defragOne(handle, maxMoveBytes);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        return delegate.defragCycle(options);
    }

    @Override
    public NativeAllocatorStats stats() {
        return delegate.stats();
    }

    @Override
    public void close() {
        delegate.close();
    }

    int allocateRawCalls() {
        return allocateRawCalls;
    }

    int reallocRawCalls() {
        return reallocRawCalls;
    }

    int freeRawCalls() {
        return freeRawCalls;
    }

    int pinRawCalls() {
        return pinRawCalls;
    }

    int unpinRawCalls() {
        return unpinRawCalls;
    }

    int resolveRawCalls() {
        return resolveRawCalls;
    }

    private static AssertionError boxedCall(String operation) {
        return new AssertionError("boxed allocator path invoked: " + operation);
    }
}
