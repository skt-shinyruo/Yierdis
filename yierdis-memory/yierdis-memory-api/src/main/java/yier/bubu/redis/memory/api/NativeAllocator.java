package yier.bubu.redis.memory.api;

public interface NativeAllocator extends AutoCloseable {
    NativeHandle allocate(NativeObjectKind kind, int size);

    NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    void free(NativeHandle handle);

    void pin(NativeHandle handle);

    void unpin(NativeHandle handle);

    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes);

    NativeAllocatorStats stats();

    @Override
    void close();
}
