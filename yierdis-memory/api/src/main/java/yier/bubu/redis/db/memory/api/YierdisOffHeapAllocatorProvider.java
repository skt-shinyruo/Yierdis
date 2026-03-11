package yier.bubu.redis.db.memory.api;

/**
 * Service provider interface for off-heap allocator backends.
 * <p>
 * Implementations should be registered via {@code META-INF/services} so that {@link java.util.ServiceLoader}
 * can discover available backends at startup.
 */
public interface YierdisOffHeapAllocatorProvider {
    YierdisOffHeapBackend backend();

    YierdisOffHeapAllocator create(long maxBytes);
}

