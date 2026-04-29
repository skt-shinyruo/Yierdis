package yier.bubu.redis.offheap.api;

/**
 * Off-heap allocation failure (hard cap exceeded or backend cannot reserve).
 */
public class OffHeapOutOfMemoryException extends RuntimeException {
    public OffHeapOutOfMemoryException(String message) {
        super(message);
    }
}

