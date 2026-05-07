package yier.bubu.redis.memory.api;

/**
 * Off-heap allocation failure (hard cap exceeded or backend cannot reserve).
 */
public class OffHeapOutOfMemoryException extends RuntimeException {
    public OffHeapOutOfMemoryException(String message) {
        super(message);
    }
}

